use std::io::{Read, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Child;
use std::time::{Duration, Instant};

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers_modules::testcontainers::{GenericImage, ImageExt, runners::SyncRunner};

const BIN: &str = env!("CARGO_BIN_EXE_beyond-pg-sink");
const FORWARDER_BIN: &str = env!("CARGO_BIN_EXE_wal-forwarder");

struct Sink {
    process: Child,
    dir: PathBuf,
}

impl Drop for Sink {
    fn drop(&mut self) {
        // beyond-pg-sink was spawned as its own process group leader (pgid == pid).
        // Killing the negative pgid takes both it and its pg_receivewal child down
        // together, so no orphan keeps reconnecting and spamming stderr.
        #[cfg(unix)]
        unsafe {
            let pid = self.process.id() as i32;
            libc::kill(-pid, libc::SIGKILL);
        }
        #[cfg(not(unix))]
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct Forwarder {
    process: Child,
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            let pid = self.process.id() as i32;
            libc::kill(-pid, libc::SIGKILL);
        }
        #[cfg(not(unix))]
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn http_get(port: u16, path: &str) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();

    let header_end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header terminator in response");
    let headers = std::str::from_utf8(&response[..header_end]).unwrap();
    let status: u16 = headers
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (status, response[header_end + 4..].to_vec())
}

fn wait_http_ready(port: u16) {
    for _ in 0..200 {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ =
                s.write_all(b"GET /list HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            let mut buf = [0u8; 16];
            if s.read(&mut buf).is_ok() {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("beyond-pg-sink HTTP server did not become ready on port {port}");
}

#[test]
#[ignore = "requires Docker"]
fn wal_sink_streams_from_primary() {
    // postgres:18 with replication config. with_cmd replaces CMD entirely; the
    // postgres Docker entrypoint prepends `postgres` when the first arg is `-`,
    // so passing flags directly is correct. with_cmd also replaces the default
    // `["-c", "fsync=off"]` added by testcontainers-modules — omitting it
    // intentionally so the primary behaves like production.
    let container = Postgres::default()
        .with_tag("18")
        // Covers regular connections (used by our postgres::Client below).
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    // Rewrite pg_hba.conf to allow replication from any host, then reload.
    // POSTGRES_HOST_AUTH_METHOD=trust only adds `host all all all trust`, which
    // does NOT cover replication — pg_hba.conf "all" excludes replication.
    // On Mac, Docker Desktop also routes host→container via 192.168.65.1 (not
    // 127.0.0.1), so the default 127.0.0.1/32 entries don't cover pg_receivewal.
    {
        let mut setup = postgres::Client::connect(&pg_url, postgres::NoTls)
            .expect("failed to connect to Postgres for pg_hba.conf setup");
        setup
            .batch_execute(
                "DO $$ DECLARE p text; BEGIN \
                     SELECT current_setting('hba_file') INTO p; \
                     EXECUTE format( \
                         $q$COPY (SELECT line FROM (VALUES \
                             ('local all all trust'), \
                             ('host all all all trust'), \
                             ('host replication all all trust') \
                         ) AS t(line)) TO %L$q$, p); \
                 END; $$; \
                 SELECT pg_reload_conf();",
            )
            .expect("failed to rewrite pg_hba.conf");
    }

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-e2e-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();

    // Grab an OS-assigned port; TOCTOU window is acceptable in tests.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = probe.local_addr().unwrap().port();
    drop(probe);

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let mut cmd = std::process::Command::new(BIN);
    cmd.args([
        "--mode",
        "tcp",
        "--connstr",
        &connstr,
        "--dir",
        sink_dir.to_str().unwrap(),
        "--port",
        &http_port.to_string(),
        "--slot",
        "wal_sink",
    ]);
    // New process group so Sink::drop can SIGKILL the whole group, including
    // the pg_receivewal child, instead of orphaning it.
    #[cfg(unix)]
    cmd.process_group(0);
    let process = cmd.spawn().expect("failed to spawn beyond-pg-sink");

    // _sink is declared after container so it drops first — process killed
    // before the container is stopped.
    let _sink = Sink {
        process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port);

    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("failed to connect to Postgres");

    // Poll until the native receiver appears in pg_stat_replication (connected and streaming).
    // The native receiver connects with application_name = slot name ("wal_sink").
    let mut streaming = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        streaming,
        "native WAL receiver never appeared in pg_stat_replication — did not connect"
    );

    // INSERT blocks until the sink has flushed the WAL (synchronous_commit=remote_write).
    client
        .batch_execute(
            "CREATE TABLE e2e (id serial, v text);
             INSERT INTO e2e (v) SELECT 'row-' || g FROM generate_series(1, 500) g;",
        )
        .unwrap();

    // Capture a stable LSN immediately after commit. SELECT generates no WAL so
    // this won't advance past the committed position before we read flush_lsn.
    let commit_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    // flush_lsn >= commit_lsn proves the sink acknowledged every byte before INSERT returned.
    // flush_lsn is on pg_stat_replication (the live connection), not pg_replication_slots
    // (which only tracks restart_lsn for physical slots).
    // commit_lsn comes from the server as a pg_lsn value (HH/HHHHHHHH hex format only).
    let flushed: bool = client
        .query_one(
            &format!(
                "SELECT flush_lsn >= '{commit_lsn}'::pg_lsn
                 FROM pg_stat_replication
                 WHERE application_name = 'wal_sink'"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        flushed,
        "flush_lsn did not reach commit LSN {commit_lsn} — synchronous commit did not wait for sink"
    );

    // Force a WAL segment switch so the native receiver finalises the current segment:
    // WalWriter renames <name>.partial → <name> only at a segment boundary.
    client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    // Wait for at least one complete segment (no .partial suffix) to appear.
    let mut segments: Vec<String> = Vec::new();
    for _ in 0..60 {
        segments = std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .collect();
        if !segments.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    segments.sort_unstable();
    assert!(
        !segments.is_empty(),
        "no complete WAL segments in sink dir after pg_switch_wal()"
    );

    // /list must return exactly those segments in sorted order.
    let (status, body) = http_get(http_port, "/list");
    assert_eq!(status, 200, "/list returned non-200");
    let body_str = std::str::from_utf8(&body).unwrap();
    let listed: Vec<&str> = body_str.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        listed,
        segments.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        "/list content does not match sink dir"
    );

    // /<segment> must return bytes byte-for-byte identical to the on-disk file.
    // Snapshot the file before the HTTP request so both reads see the same moment.
    let first = &segments[0];
    let on_disk = std::fs::read(sink_dir.join(first)).unwrap();
    let (status, bytes) = http_get(http_port, &format!("/{first}"));
    assert_eq!(status, 200, "segment fetch returned non-200");
    assert_eq!(
        bytes.len(),
        on_disk.len(),
        "HTTP response length differs from on-disk file"
    );
    assert_eq!(
        bytes, on_disk,
        "HTTP response bytes do not match on-disk segment"
    );
}

/// Same correctness assertions as `wal_sink_streams_from_primary` but uses the
/// QUIC transport path: sink runs `--mode quic`, a standalone `wal-forwarder`
/// process bridges Postgres (TCP) → sink (QUIC).
///
/// Run with:
///   cargo test -p beyond-pg-sink -- --ignored --nocapture wal_sink_streams_via_quic
#[test]
#[ignore = "requires Docker"]
fn wal_sink_streams_via_quic() {
    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    {
        let mut setup = postgres::Client::connect(&pg_url, postgres::NoTls)
            .expect("failed to connect to Postgres for pg_hba.conf setup");
        setup
            .batch_execute(
                "DO $$ DECLARE p text; BEGIN \
                     SELECT current_setting('hba_file') INTO p; \
                     EXECUTE format( \
                         $q$COPY (SELECT line FROM (VALUES \
                             ('local all all trust'), \
                             ('host all all all trust'), \
                             ('host replication all all trust') \
                         ) AS t(line)) TO %L$q$, p); \
                 END; $$; \
                 SELECT pg_reload_conf();",
            )
            .expect("failed to rewrite pg_hba.conf");
    }

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-quic-e2e-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = probe.local_addr().unwrap().port();
    drop(probe);

    // Sink in QUIC mode: no --connstr, receives WAL over QUIC from the forwarder.
    let mut sink_cmd = std::process::Command::new(BIN);
    sink_cmd.args([
        "--mode",
        "quic",
        "--dir",
        sink_dir.to_str().unwrap(),
        "--port",
        &http_port.to_string(),
        "--slot",
        "wal_sink",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_process = sink_cmd.spawn().expect("failed to spawn beyond-pg-sink");
    let _sink = Sink {
        process: sink_process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port);

    // Forwarder: bridges Postgres TCP → sink QUIC.
    let sink_addr = format!("127.0.0.1:{http_port}");
    let mut fwd_cmd = std::process::Command::new(FORWARDER_BIN);
    fwd_cmd.args([
        "--pg-port",
        &pg_port.to_string(),
        "--sink-addr",
        &sink_addr,
        "--slot",
        "wal_sink",
    ]);
    #[cfg(unix)]
    fwd_cmd.process_group(0);
    let fwd_process = fwd_cmd.spawn().expect("failed to spawn wal-forwarder");
    let _forwarder = Forwarder {
        process: fwd_process,
    };

    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("failed to connect to Postgres");

    // Poll until forwarder appears in pg_stat_replication.
    let mut streaming = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        streaming,
        "wal-forwarder never appeared in pg_stat_replication"
    );

    client
        .batch_execute(
            "CREATE TABLE e2e_quic (id serial, v text);
             INSERT INTO e2e_quic (v) SELECT 'row-' || g FROM generate_series(1, 500) g;",
        )
        .unwrap();

    let commit_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    let flushed: bool = client
        .query_one(
            &format!(
                "SELECT flush_lsn >= '{commit_lsn}'::pg_lsn
                 FROM pg_stat_replication
                 WHERE application_name = 'wal_sink'"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        flushed,
        "flush_lsn did not reach commit LSN {commit_lsn} (QUIC path)"
    );

    client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let mut segments: Vec<String> = Vec::new();
    for _ in 0..60 {
        segments = std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .collect();
        if !segments.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    segments.sort_unstable();
    assert!(
        !segments.is_empty(),
        "no complete WAL segments after pg_switch_wal (QUIC path)"
    );

    let (status, body) = http_get(http_port, "/list");
    assert_eq!(status, 200, "/list returned non-200 (QUIC path)");
    let body_str = std::str::from_utf8(&body).unwrap();
    let listed: Vec<&str> = body_str.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        listed,
        segments.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        "/list content does not match sink dir (QUIC path)"
    );

    let first = &segments[0];
    let on_disk = std::fs::read(sink_dir.join(first)).unwrap();
    let (status, bytes) = http_get(http_port, &format!("/{first}"));
    assert_eq!(status, 200, "segment fetch returned non-200 (QUIC path)");
    assert_eq!(
        bytes.len(),
        on_disk.len(),
        "HTTP response length differs from on-disk (QUIC path)"
    );
    assert_eq!(
        bytes, on_disk,
        "HTTP response bytes do not match on-disk segment (QUIC path)"
    );
}

/// Measures commit latency for `synchronous_commit = remote_write` with the current
/// TCP + pg_receivewal path. Prints p50/p95/p99/max to stderr. No assertions —
/// purely diagnostic. Run with:
///   cargo test -p beyond-pg-sink -- --ignored --nocapture latency_baseline
#[test]
#[ignore = "requires Docker and pg_receivewal 18 on PATH"]
fn latency_baseline() {
    const N: usize = 2000;

    if std::process::Command::new("pg_receivewal")
        .arg("--version")
        .output()
        .is_err()
    {
        panic!("pg_receivewal not on PATH");
    }

    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    {
        let mut setup = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();
        setup
            .batch_execute(
                "DO $$ DECLARE p text; BEGIN \
                 SELECT current_setting('hba_file') INTO p; \
                 EXECUTE format( \
                     $q$COPY (SELECT line FROM (VALUES \
                         ('local all all trust'), \
                         ('host all all all trust'), \
                         ('host replication all all trust') \
                     ) AS t(line)) TO %L$q$, p); \
             END; $$; \
             SELECT pg_reload_conf();",
            )
            .unwrap();
    }

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-bench-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = probe.local_addr().unwrap().port();
    drop(probe);

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let mut cmd = std::process::Command::new(BIN);
    cmd.args([
        "--mode",
        "tcp",
        "--connstr",
        &connstr,
        "--dir",
        sink_dir.to_str().unwrap(),
        "--port",
        &http_port.to_string(),
        "--slot",
        "wal_sink",
    ]);
    #[cfg(unix)]
    cmd.process_group(0);
    let process = cmd.spawn().unwrap();
    let _sink = Sink {
        process,
        dir: sink_dir,
    };

    wait_http_ready(http_port);

    let mut client = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();

    // Wait for pg_receivewal to appear in pg_stat_replication.
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'pg_receivewal'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    client
        .batch_execute("CREATE TABLE bench (id serial, v text)")
        .unwrap();

    // Warm up: 50 commits before measuring.
    for _ in 0..50 {
        client
            .batch_execute("INSERT INTO bench (v) VALUES ('warmup')")
            .unwrap();
    }

    // Measure N sequential single-row commits.
    let mut latencies_us: Vec<u64> = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        client
            .batch_execute("INSERT INTO bench (v) VALUES ('x')")
            .unwrap();
        latencies_us.push(t.elapsed().as_micros() as u64);
    }

    // Also capture server-side lag from pg_stat_replication.
    let lag_row = client
        .query_opt(
            "SELECT \
                 extract(epoch from write_lag) * 1e6 AS write_lag_us, \
                 extract(epoch from flush_lag) * 1e6 AS flush_lag_us \
             FROM pg_stat_replication \
             WHERE application_name = 'pg_receivewal'",
            &[],
        )
        .unwrap();

    latencies_us.sort_unstable();
    let p50 = latencies_us[N * 50 / 100];
    let p95 = latencies_us[N * 95 / 100];
    let p99 = latencies_us[N * 99 / 100];
    let max = *latencies_us.last().unwrap();

    eprintln!("\n=== latency_baseline (N={N}, synchronous_commit=remote_write, TCP) ===");
    eprintln!("  client-side commit latency:");
    eprintln!("    p50  = {p50} µs");
    eprintln!("    p95  = {p95} µs");
    eprintln!("    p99  = {p99} µs");
    eprintln!("    max  = {max} µs");

    if let Some(row) = lag_row {
        let write_lag: f64 = row.get::<_, Option<f64>>(0).unwrap_or(0.0);
        let flush_lag: f64 = row.get::<_, Option<f64>>(1).unwrap_or(0.0);
        eprintln!("  server-side pg_stat_replication (last sample):");
        eprintln!("    write_lag = {:.0} µs", write_lag);
        eprintln!("    flush_lag = {:.0} µs", flush_lag);
    }
    eprintln!("======================================================");
}

/// Same as `latency_baseline` but uses the QUIC transport path (sink `--mode quic` +
/// `wal-forwarder`). Run alongside `latency_baseline` to compare TCP vs QUIC overhead.
///
///   cargo test -p beyond-pg-sink -- --ignored --nocapture latency_baseline_quic
#[test]
#[ignore = "requires Docker"]
fn latency_baseline_quic() {
    const N: usize = 2000;

    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    {
        let mut setup = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();
        setup
            .batch_execute(
                "DO $$ DECLARE p text; BEGIN \
                 SELECT current_setting('hba_file') INTO p; \
                 EXECUTE format( \
                     $q$COPY (SELECT line FROM (VALUES \
                         ('local all all trust'), \
                         ('host all all all trust'), \
                         ('host replication all all trust') \
                     ) AS t(line)) TO %L$q$, p); \
             END; $$; \
             SELECT pg_reload_conf();",
            )
            .unwrap();
    }

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-quic-bench-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = probe.local_addr().unwrap().port();
    drop(probe);

    let mut sink_cmd = std::process::Command::new(BIN);
    sink_cmd.args([
        "--mode",
        "quic",
        "--dir",
        sink_dir.to_str().unwrap(),
        "--port",
        &http_port.to_string(),
        "--slot",
        "wal_sink",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let _sink = Sink {
        process: sink_cmd.spawn().unwrap(),
        dir: sink_dir,
    };

    wait_http_ready(http_port);

    let sink_addr = format!("127.0.0.1:{http_port}");
    let mut fwd_cmd = std::process::Command::new(FORWARDER_BIN);
    fwd_cmd.args([
        "--pg-port",
        &pg_port.to_string(),
        "--sink-addr",
        &sink_addr,
        "--slot",
        "wal_sink",
    ]);
    #[cfg(unix)]
    fwd_cmd.process_group(0);
    let _forwarder = Forwarder {
        process: fwd_cmd.spawn().unwrap(),
    };

    let mut client = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();

    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    client
        .batch_execute("CREATE TABLE bench_quic (id serial, v text)")
        .unwrap();

    for _ in 0..50 {
        client
            .batch_execute("INSERT INTO bench_quic (v) VALUES ('warmup')")
            .unwrap();
    }

    let mut latencies_us: Vec<u64> = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        client
            .batch_execute("INSERT INTO bench_quic (v) VALUES ('x')")
            .unwrap();
        latencies_us.push(t.elapsed().as_micros() as u64);
    }

    // Cast to float8 explicitly — interval * float8 can come back as numeric
    // depending on server version, which the postgres crate can't coerce.
    let lag_row = client
        .query_opt(
            "SELECT \
                 (extract(epoch from write_lag) * 1e6)::float8, \
                 (extract(epoch from flush_lag) * 1e6)::float8 \
             FROM pg_stat_replication \
             WHERE application_name = 'wal_sink'",
            &[],
        )
        .unwrap();

    latencies_us.sort_unstable();
    let p50 = latencies_us[N * 50 / 100];
    let p95 = latencies_us[N * 95 / 100];
    let p99 = latencies_us[N * 99 / 100];
    let max = *latencies_us.last().unwrap();

    eprintln!("\n=== latency_baseline_quic (N={N}, synchronous_commit=remote_write, QUIC) ===");
    eprintln!("  client-side commit latency:");
    eprintln!("    p50  = {p50} µs");
    eprintln!("    p95  = {p95} µs");
    eprintln!("    p99  = {p99} µs");
    eprintln!("    max  = {max} µs");

    if let Some(row) = lag_row {
        let write_lag: f64 = row.get::<_, Option<f64>>(0).unwrap_or(0.0);
        let flush_lag: f64 = row.get::<_, Option<f64>>(1).unwrap_or(0.0);
        eprintln!("  server-side pg_stat_replication (last sample):");
        eprintln!("    write_lag = {:.0} µs", write_lag);
        eprintln!("    flush_lag = {:.0} µs", flush_lag);
    }
    eprintln!("======================================================");
}

// ---------------------------------------------------------------------------
// Real two-container benchmark
// ---------------------------------------------------------------------------
//
// `latency_baseline_quic` runs both the forwarder and the sink on the loopback
// interface — every QUIC packet travels through the same network namespace as
// the test process. Loopback hides the real cost of crossing a network: it has
// zero queuing, no MTU effects, and effectively infinite bandwidth.
//
// This test pins the sink inside an Alpine container on Docker's bridge
// network. The forwarder still runs on the host but reaches the sink via
// host-published ports (TCP for HTTP control, UDP for QUIC). On Docker Desktop
// (macOS) the host-to-container round trip is ~0.1-0.3 ms; on native Linux
// Docker the bridge adds tens of microseconds. Both are real, non-loopback
// measurements.
//
// Requires `x86_64-unknown-linux-musl` because the sink binary is mounted into
// a glibc-free Alpine image. The test skips with a clear message if the
// target isn't installed.

/// Verify that the sink can be crash-killed and restarted while reusing the same
/// replication slot and WAL directory.  The second run must:
///   1. Attach to the existing slot without erroring (slot `IF NOT EXISTS`).
///   2. Resume streaming from the last complete segment on disk via
///      `highest_local_lsn`, ignoring any `.partial` file left by the crash.
///   3. Acknowledge new commits so `flush_lsn >= commit_lsn`.
///   4. Leave the segments written during the first run intact on disk.
#[test]
#[ignore = "requires Docker"]
fn sink_restart_is_idempotent() {
    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    {
        let mut setup = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();
        setup
            .batch_execute(
                "DO $$ DECLARE p text; BEGIN \
                     SELECT current_setting('hba_file') INTO p; \
                     EXECUTE format( \
                         $q$COPY (SELECT line FROM (VALUES \
                             ('local all all trust'), \
                             ('host all all all trust'), \
                             ('host replication all all trust') \
                         ) AS t(line)) TO %L$q$, p); \
                 END; $$; \
                 SELECT pg_reload_conf();",
            )
            .unwrap();
    }

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-restart-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres");

    let spawn_sink = |http_port: u16| -> std::process::Child {
        let mut cmd = std::process::Command::new(BIN);
        cmd.args([
            "--mode",
            "tcp",
            "--connstr",
            &connstr,
            "--dir",
            sink_dir.to_str().unwrap(),
            "--port",
            &http_port.to_string(),
            "--slot",
            "wal_sink_restart",
        ]);
        #[cfg(unix)]
        cmd.process_group(0);
        cmd.spawn().expect("failed to spawn beyond-pg-sink")
    };

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port1 = probe.local_addr().unwrap().port();
    drop(probe);

    // ── First run ──────────────────────────────────────────────────────────
    let mut proc1 = spawn_sink(http_port1);
    wait_http_ready(http_port1);

    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("failed to connect to Postgres");

    let mut streaming = false;
    for _ in 0..60 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_restart'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        streaming,
        "first run: sink never appeared in pg_stat_replication"
    );

    client
        .batch_execute(
            "CREATE TABLE restart_test (id serial, v text); \
             INSERT INTO restart_test (v) SELECT 'row-' || g FROM generate_series(1, 200) g;",
        )
        .unwrap();
    client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let mut first_segments: Vec<String> = Vec::new();
    for _ in 0..60 {
        first_segments = std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .collect();
        if !first_segments.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        !first_segments.is_empty(),
        "no complete segments after first run + pg_switch_wal"
    );

    // SIGKILL simulates a crash: the process dies instantly, possibly leaving a
    // .partial file for the segment that was being written at the time.
    #[cfg(unix)]
    unsafe {
        libc::kill(-(proc1.id() as i32), libc::SIGKILL);
    }
    #[cfg(not(unix))]
    let _ = proc1.kill();
    let _ = proc1.wait();

    // ── Second run ─────────────────────────────────────────────────────────
    // Use a fresh ephemeral port so we don't race against the kernel's
    // TIME_WAIT or socket lingering from the first process.
    let probe2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port2 = probe2.local_addr().unwrap().port();
    drop(probe2);

    let proc2 = spawn_sink(http_port2);
    let _sink2 = Sink {
        process: proc2,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port2);

    let mut streaming2 = false;
    for _ in 0..60 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_restart'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            streaming2 = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        streaming2,
        "second run: sink did not reconnect after restart"
    );

    client
        .batch_execute(
            "INSERT INTO restart_test (v) SELECT 'restart-' || g FROM generate_series(1, 200) g;",
        )
        .unwrap();
    let commit_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);
    let flushed: bool = client
        .query_one(
            &format!(
                "SELECT flush_lsn >= '{commit_lsn}'::pg_lsn \
                 FROM pg_stat_replication \
                 WHERE application_name = 'wal_sink_restart'"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        flushed,
        "flush_lsn did not reach commit LSN {commit_lsn} after restart"
    );

    // Segments written during the first run must still be on disk.
    let segments_after: Vec<String> = std::fs::read_dir(&sink_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
        .collect();
    for seg in &first_segments {
        assert!(
            segments_after.contains(seg),
            "segment {seg} from first run is missing after restart"
        );
    }
}

/// Cross-compile a binary for the musl target and return the absolute path.
/// Returns `None` (with a stderr message) if the target isn't available so
/// the caller can skip the test.
fn build_musl(bin: &str) -> Option<PathBuf> {
    const TARGET: &str = "x86_64-unknown-linux-musl";

    let installed = std::process::Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .ok()?;
    let installed_list = std::str::from_utf8(&installed.stdout).ok()?;
    if !installed_list.lines().any(|l| l.trim() == TARGET) {
        eprintln!(
            "skipping latency_baseline_real: {TARGET} target not installed\n\
             run: rustup target add {TARGET}\n\
             on macOS also: brew install filosottile/musl-cross/musl-cross"
        );
        return None;
    }

    let status = std::process::Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "beyond-pg-sink",
            "--bin",
            bin,
            "--target",
            TARGET,
        ])
        .status()
        .ok()?;
    if !status.success() {
        eprintln!("skipping latency_baseline_real: musl build failed for {bin}");
        return None;
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.parent().unwrap_or(&manifest);
    let path = workspace
        .join("target")
        .join(TARGET)
        .join("release")
        .join(bin);
    if !path.exists() {
        eprintln!(
            "skipping latency_baseline_real: musl artifact not found at {}",
            path.display()
        );
        return None;
    }
    Some(path)
}

/// Same workload as `latency_baseline_quic`, but the sink runs in a Docker
/// container instead of on host loopback. Prints TCP and QUIC results
/// side-by-side so the operator can see the per-hop cost of QUIC across a
/// real network boundary.
///
/// Run with:
///   rustup target add x86_64-unknown-linux-musl
///   cargo test -p beyond-pg-sink -- --ignored --nocapture latency_baseline_real
#[test]
#[ignore = "requires Docker + x86_64-unknown-linux-musl target"]
fn latency_baseline_real() {
    const N: usize = 2000;

    let Some(sink_bin) = build_musl("beyond-pg-sink") else {
        return;
    };
    // wal-forwarder is also musl-built for parity, even though we run it on
    // the host. This catches musl-build regressions in CI.
    let Some(_fwd_bin) = build_musl("wal-forwarder") else {
        return;
    };

    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    {
        let mut setup = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();
        setup
            .batch_execute(
                "DO $$ DECLARE p text; BEGIN \
                 SELECT current_setting('hba_file') INTO p; \
                 EXECUTE format( \
                     $q$COPY (SELECT line FROM (VALUES \
                         ('local all all trust'), \
                         ('host all all all trust'), \
                         ('host replication all all trust') \
                     ) AS t(line)) TO %L$q$, p); \
             END; $$; \
             SELECT pg_reload_conf();",
            )
            .unwrap();
    }

    // --- Leg 1: QUIC, sink in a real container ---------------------------------
    let sink_bin_str = sink_bin.to_str().expect("musl path utf-8").to_owned();
    let sink_container = GenericImage::new("alpine", "3.20")
        .with_exposed_port(9000_u16.tcp()) // HTTP
        .with_exposed_port(9000_u16.udp()) // QUIC
        .with_wait_for(WaitFor::seconds(2))
        .with_entrypoint("/usr/local/bin/beyond-pg-sink")
        .with_mount(Mount::bind_mount(
            sink_bin_str,
            "/usr/local/bin/beyond-pg-sink",
        ))
        .with_cmd([
            "--mode", "quic", "--dir", "/tmp/wal", "--port", "9000", "--slot", "wal_sink",
        ])
        .start()
        .expect("failed to start sink container");

    let sink_http_port = sink_container.get_host_port_ipv4(9000_u16.tcp()).unwrap();
    let sink_udp_port = sink_container.get_host_port_ipv4(9000_u16.udp()).unwrap();
    let sink_addr = format!("127.0.0.1:{sink_udp_port}");
    wait_http_ready(sink_http_port);

    let mut fwd_cmd = std::process::Command::new(FORWARDER_BIN);
    fwd_cmd.args([
        "--pg-port",
        &pg_port.to_string(),
        "--sink-addr",
        &sink_addr,
        "--slot",
        "wal_sink",
    ]);
    #[cfg(unix)]
    fwd_cmd.process_group(0);
    let _forwarder = Forwarder {
        process: fwd_cmd.spawn().expect("failed to spawn wal-forwarder"),
    };

    let mut client = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();

    let mut streaming = false;
    for _ in 0..120 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        streaming,
        "QUIC leg: wal-forwarder never appeared in pg_stat_replication"
    );

    client
        .batch_execute("CREATE TABLE bench_real_quic (id serial, v text)")
        .unwrap();
    for _ in 0..50 {
        client
            .batch_execute("INSERT INTO bench_real_quic (v) VALUES ('warmup')")
            .unwrap();
    }

    let mut quic_us: Vec<u64> = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        client
            .batch_execute("INSERT INTO bench_real_quic (v) VALUES ('x')")
            .unwrap();
        quic_us.push(t.elapsed().as_micros() as u64);
    }

    // Tear down the QUIC leg so the slot is free for the TCP leg.
    drop(_forwarder);
    drop(sink_container);
    std::thread::sleep(Duration::from_secs(1));
    client
        .batch_execute("SELECT pg_drop_replication_slot('wal_sink')")
        .ok();

    // --- Leg 2: TCP, sink on the host for direct comparison --------------------
    let tcp_sink_dir = std::env::temp_dir().join(format!("wal-sink-real-tcp-{pg_port}"));
    let _ = std::fs::remove_dir_all(&tcp_sink_dir);
    std::fs::create_dir_all(&tcp_sink_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp_http_port = probe.local_addr().unwrap().port();
    drop(probe);

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let mut tcp_cmd = std::process::Command::new(BIN);
    tcp_cmd.args([
        "--mode",
        "tcp",
        "--connstr",
        &connstr,
        "--dir",
        tcp_sink_dir.to_str().unwrap(),
        "--port",
        &tcp_http_port.to_string(),
        "--slot",
        "wal_sink",
    ]);
    #[cfg(unix)]
    tcp_cmd.process_group(0);
    let _tcp_sink = Sink {
        process: tcp_cmd.spawn().unwrap(),
        dir: tcp_sink_dir,
    };
    wait_http_ready(tcp_http_port);

    for _ in 0..120 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    client
        .batch_execute("CREATE TABLE bench_real_tcp (id serial, v text)")
        .unwrap();
    for _ in 0..50 {
        client
            .batch_execute("INSERT INTO bench_real_tcp (v) VALUES ('warmup')")
            .unwrap();
    }

    let mut tcp_us: Vec<u64> = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        client
            .batch_execute("INSERT INTO bench_real_tcp (v) VALUES ('x')")
            .unwrap();
        tcp_us.push(t.elapsed().as_micros() as u64);
    }

    fn pct(v: &[u64], q: usize) -> u64 {
        v[(v.len() * q) / 100]
    }
    quic_us.sort_unstable();
    tcp_us.sort_unstable();

    eprintln!(
        "\n=== latency_baseline_real (N={N}, Docker network, synchronous_commit=remote_write) ==="
    );
    eprintln!("  TCP (native receiver, host loopback to Postgres container):");
    eprintln!(
        "    p50 = {} µs  p95 = {} µs  p99 = {} µs  max = {} µs",
        pct(&tcp_us, 50),
        pct(&tcp_us, 95),
        pct(&tcp_us, 99),
        tcp_us.last().copied().unwrap_or(0)
    );
    eprintln!("  QUIC (forwarder on host -> sink in Alpine container, real Docker network):");
    eprintln!(
        "    p50 = {} µs  p95 = {} µs  p99 = {} µs  max = {} µs",
        pct(&quic_us, 50),
        pct(&quic_us, 95),
        pct(&quic_us, 99),
        quic_us.last().copied().unwrap_or(0)
    );
    eprintln!("=====================================================================");
}
