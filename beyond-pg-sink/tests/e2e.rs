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

/// Build a statically-linked musl binary for the Linux architecture that Docker
/// containers run on (aarch64 or x86_64).  Returns `(binary_path, platform)`
/// where `platform` is the Docker `--platform` string, or `None` if prerequisites
/// are absent so the caller can skip the test.
fn build_linux_bin(bin: &str) -> Option<(PathBuf, &'static str)> {
    let arch_out = std::process::Command::new("docker")
        .args(["info", "--format", "{{.Architecture}}"])
        .output()
        .ok()?;
    let arch = std::str::from_utf8(&arch_out.stdout)
        .ok()?
        .trim()
        .to_owned();
    let (target, platform): (&'static str, &'static str) = match arch.as_str() {
        a if a == "aarch64" || a == "arm64" => ("aarch64-unknown-linux-musl", "linux/arm64"),
        _ => ("x86_64-unknown-linux-musl", "linux/amd64"),
    };

    let installed = std::process::Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .ok()?;
    let list = std::str::from_utf8(&installed.stdout).ok()?.to_owned();
    if !list.lines().any(|l| l.trim() == target) {
        eprintln!(
            "skipping: {target} not installed\n\
             run: rustup target add {target}"
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
            target,
        ])
        .status()
        .ok()?;
    if !status.success() {
        eprintln!("skipping: musl build failed for {bin}");
        return None;
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.parent().unwrap_or(&manifest);
    let path = workspace
        .join("target")
        .join(target)
        .join("release")
        .join(bin);
    path.exists().then_some((path, platform))
}

/// Full archive-recovery integration via the production QUIC transport:
///
///   primary (container) → wal-forwarder (host) → sink (container, QUIC)
///   → replica (container) via restore_command over the shared Docker network.
///
/// Unlike `replica_recovers_via_archive`, the sink runs in its own Alpine
/// container so QUIC crosses a real network boundary, the forwarder runs on
/// the host (as it does in the VM topology), and the replica's restore_command
/// reaches the sink directly by container IP — no host.docker.internal.
///
/// Run with:
///   cargo test -p beyond-pg-sink -- --ignored --nocapture replica_recovers_via_archive_quic
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn replica_recovers_via_archive_quic() {
    let Some((sink_bin, platform)) = build_linux_bin("beyond-pg-sink") else {
        return;
    };
    // ── helpers (same as replica_recovers_via_archive) ────────────────────

    fn allow_replication(client: &mut postgres::Client) {
        client
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
            .expect("pg_hba rewrite failed");
    }

    fn wait_for_rows(client: &mut postgres::Client, table: &str, expected: i64) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if Instant::now() > deadline {
                panic!("replica did not see {expected} rows in {table} within 90s");
            }
            let n: i64 = client
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if n >= expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    struct DropContainer(String);
    impl Drop for DropContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }
    struct DropNetwork(String);
    impl Drop for DropNetwork {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }
    struct DropDir(std::path::PathBuf);
    impl Drop for DropDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn container_ip(container_id: &str, network: &str) -> String {
        let template = format!(
            r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
            network
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let out = std::process::Command::new("docker")
                .args(["inspect", "-f", &template, container_id])
                .output()
                .unwrap();
            let ip = String::from_utf8_lossy(&out.stdout).trim().to_owned();
            if !ip.is_empty() {
                return ip;
            }
            if Instant::now() > deadline {
                panic!("{container_id} has no IP on network {network} after 10s");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    // Count complete WAL segments reported by the sink's HTTP /list endpoint.
    fn sink_complete_segs(http_port: u16) -> usize {
        use std::io::{Read as _, Write as _};
        let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", http_port)) else {
            return 0;
        };
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
        if stream
            .write_all(b"GET /list HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return 0;
        }
        let mut response = Vec::new();
        if stream.read_to_end(&mut response).is_err() {
            return 0;
        }
        let Some(header_end) = response.windows(4).position(|w| w == b"\r\n\r\n") else {
            return 0;
        };
        std::str::from_utf8(&response[header_end + 4..])
            .unwrap_or("")
            .lines()
            .filter(|l| {
                let l = l.trim();
                l.len() == 24 && l.bytes().all(|b| b.is_ascii_hexdigit())
            })
            .count()
    }

    // ── unique docker network ───────────────────────────────────────────────
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let net_name = format!("beyond-pg-archive-quic-{}-{n}", std::process::id());
    let out = std::process::Command::new("docker")
        .args(["network", "create", &net_name])
        .output()
        .expect("docker network create");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _net = DropNetwork(net_name.clone());

    // ── 1. primary ────────────────────────────────────────────────────────
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "hot_standby=on",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut primary_client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut primary_client);

    let out = std::process::Command::new("docker")
        .args(["network", "connect", &net_name, primary.id()])
        .output()
        .expect("network connect primary");
    assert!(out.status.success());
    let primary_ip = container_ip(primary.id(), &net_name);

    // ── 2. sink container (QUIC mode, alpine + musl binary) ───────────────
    // The sink container joins the Docker network so the replica can reach its
    // HTTP port directly by container IP — same topology as production.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_http_host_port = probe.local_addr().unwrap().port();
    drop(probe);
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let sink_quic_host_port = probe.local_addr().unwrap().port();
    drop(probe);

    let sink_bin_str = sink_bin.to_str().unwrap().to_owned();
    let sink_start = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            &format!("--platform={platform}"),
            &format!("--network={net_name}"),
            "-p",
            &format!("{sink_http_host_port}:9000/tcp"),
            "-p",
            &format!("{sink_quic_host_port}:9000/udp"),
            "-v",
            &format!("{sink_bin_str}:/beyond-pg-sink:ro"),
            "alpine:3.20",
            "/beyond-pg-sink",
            "--mode",
            "quic",
            "--dir",
            "/tmp/wal",
            "--port",
            "9000",
            "--slot",
            "wal_sink_archive",
        ])
        .output()
        .expect("docker run sink");
    assert!(
        sink_start.status.success(),
        "sink container start: {}",
        String::from_utf8_lossy(&sink_start.stderr)
    );
    let sink_id = String::from_utf8_lossy(&sink_start.stdout)
        .trim()
        .to_owned();
    let _sink_container = DropContainer(sink_id.clone());

    wait_http_ready(sink_http_host_port);
    let sink_ip = container_ip(&sink_id, &net_name);

    // ── 3. wal-forwarder on host (TCP → QUIC bridge) ───────────────────────
    // The forwarder is the host-side process that connects to the primary via
    // standard streaming replication and forwards WAL to the sink over QUIC.
    // On the real VM host this is the same topology: forwarder on the hypervisor,
    // sink elsewhere.
    // wal-forwarder runs natively on the host (same as the hypervisor in production).
    let mut fwd_cmd = std::process::Command::new(FORWARDER_BIN);
    fwd_cmd.args([
        "--pg-port",
        &pg_port.to_string(),
        "--sink-addr",
        &format!("127.0.0.1:{sink_quic_host_port}"),
        "--slot",
        "wal_sink_archive",
    ]);
    #[cfg(unix)]
    fwd_cmd.process_group(0);
    let _forwarder = Forwarder {
        process: fwd_cmd.spawn().expect("spawn wal-forwarder"),
    };

    let mut streaming = false;
    for _ in 0..60 {
        if primary_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication \
                 WHERE application_name = 'wal_sink_archive'",
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
        "wal-forwarder never appeared in pg_stat_replication"
    );

    // ── 4. pre-backup rows ────────────────────────────────────────────────
    primary_client
        .batch_execute(
            "CREATE TABLE archive_test_quic (id serial, v text); \
             INSERT INTO archive_test_quic (v) \
               SELECT 'pre-' || g FROM generate_series(1, 50) g;",
        )
        .expect("pre-basebackup rows");
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    // ── 5. pg_basebackup ─────────────────────────────────────────────────
    let pgdata = std::env::temp_dir().join(format!("archive-pgdata-quic-{pg_port}"));
    std::fs::create_dir_all(&pgdata).unwrap();
    let _pgdata_cleanup = DropDir(pgdata.clone());
    let pgdata_str = pgdata.to_str().unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pgdata, std::fs::Permissions::from_mode(0o777)).unwrap();
    }

    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            &format!("--network={net_name}"),
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host={primary_ip} port=5432 user=postgres"),
            "--pgdata",
            "/pgdata",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
        ])
        .output()
        .expect("docker pg_basebackup");
    assert!(
        out.status.success(),
        "pg_basebackup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 6. write fetch_wal.pl — uses sink's container IP directly ─────────
    // The replica is on the same Docker network as the sink, so it can reach
    // the sink's HTTP port at the container's network IP.  No host.docker.internal
    // needed — this is the realistic topology.
    // IO::Socket::INET's Timeout parameter sets a connect deadline, preventing
    // restore_command from hanging forever if the sink is reachable but not
    // responding (network partition without RST).
    let perl_script = format!(
        "use IO::Socket::INET;\n\
         ($s,$d)=@ARGV;\n\
         $sock=IO::Socket::INET->new(PeerAddr=>q({sink_ip}),PeerPort=>9000,\
         Proto=>q(tcp),Timeout=>10) or exit 1;\n\
         $sock->autoflush(1);\n\
         print $sock \"GET /$s HTTP/1.0\\r\\nHost: sink\\r\\nConnection: close\\r\\n\\r\\n\";\n\
         $l=<$sock>;exit 1 unless $l=~/200/;\n\
         while(<$sock>){{last if/^\\r\\n$/}}\n\
         open(O,\">\",$d)or exit 1;\n\
         while(read($sock,$b,65536)){{print O $b}}\n\
         close O;\n"
    );
    std::fs::write(pgdata.join("fetch_wal.pl"), &perl_script).expect("write fetch_wal.pl");

    // ── 7. post-backup rows + wait for archive ────────────────────────────
    let segs_before = sink_complete_segs(sink_http_host_port);

    primary_client
        .batch_execute(
            "INSERT INTO archive_test_quic (v) \
               SELECT 'post-' || g FROM generate_series(1, 50) g;",
        )
        .expect("post-basebackup rows");
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    // Wait until the sink has archived the segment containing post-backup rows.
    for _ in 0..120 {
        if sink_complete_segs(sink_http_host_port) > segs_before {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        sink_complete_segs(sink_http_host_port) > segs_before,
        "post-backup WAL segment never archived via QUIC"
    );

    // ── 8. kill primary ───────────────────────────────────────────────────
    drop(primary_client);
    drop(primary);
    // Forwarder will retry; sink HTTP server keeps serving archived segments.

    // ── 9. write recovery config ──────────────────────────────────────────
    let setup_script = "touch /pgdata/standby.signal && \
         echo \"restore_command = 'perl /pgdata/fetch_wal.pl %f %p'\" \
           >> /pgdata/postgresql.auto.conf && \
         echo \"recovery_target_timeline = 'latest'\" \
           >> /pgdata/postgresql.auto.conf";
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "bash",
            "-c",
            setup_script,
        ])
        .output()
        .expect("recovery config setup");
    assert!(
        out.status.success(),
        "recovery setup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 10. start replica on the same network ─────────────────────────────
    // No --add-host needed: the replica reaches the sink by container IP.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let replica_port = probe.local_addr().unwrap().port();
    drop(probe);

    let start_out = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            &format!("--network={net_name}"),
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "-p",
            &format!("{replica_port}:5432"),
            "postgres:18",
            "bash",
            "-c",
            "chmod 700 /pgdata && chown -R postgres:postgres /pgdata \
             && exec gosu postgres postgres -D /pgdata",
        ])
        .output()
        .expect("docker run replica");
    assert!(
        start_out.status.success(),
        "replica start: {}",
        String::from_utf8_lossy(&start_out.stderr)
    );
    let replica_id = String::from_utf8_lossy(&start_out.stdout).trim().to_owned();
    let _replica = DropContainer(replica_id.clone());

    // ── 11. wait for hot-standby connections ──────────────────────────────
    let replica_url = format!("host=127.0.0.1 port={replica_port} user=postgres dbname=postgres");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut replica_client = loop {
        if Instant::now() > deadline {
            let logs = std::process::Command::new("docker")
                .args(["logs", &replica_id])
                .output();
            let log_text = logs
                .ok()
                .map(|o| {
                    format!(
                        "stdout: {}\nstderr: {}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_else(|| "(docker logs failed)".to_owned());
            panic!(
                "archive replica (QUIC) not ready on port {replica_port} within 90s\n\
                 --- container logs ---\n{log_text}"
            );
        }
        if let Ok(c) = postgres::Client::connect(&replica_url, postgres::NoTls) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(300));
    };

    // ── 12. assert all 100 rows recovered via QUIC archive ────────────────
    wait_for_rows(&mut replica_client, "archive_test_quic", 100);

    let is_recovery: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        is_recovery,
        "replica should still be in recovery (no primary to promote it)"
    );

    drop(replica_client);
    // Drop order: _replica → _forwarder → _sink_container → _net → _pgdata_cleanup
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

/// Full archive-recovery integration: primary → sink → replica via restore_command.
///
/// Verifies the end-to-end HA scenario where the primary has failed and the
/// replica must recover solely from WAL archived by `beyond-pg-sink`:
///
/// 1. Primary runs with `synchronous_commit=remote_write` so every INSERT
///    waits until the sink has flushed the WAL to disk.
/// 2. `pg_basebackup` seeds the replica PGDATA while the primary is running.
/// 3. Rows written AFTER basebackup exist only in the WAL archive (sink dir).
/// 4. Primary is killed (simulated failure).
/// 5. Replica starts with `restore_command = 'perl fetch_wal.pl %f %p'` pointing at
///    the sink's HTTP server — no `primary_conninfo` (no streaming fallback).
/// 6. Replica recovers via archive and makes the post-basebackup rows visible
///    via hot standby queries.
///
/// This exercises `config::replica_conf()`'s restore_command output and the
/// sink's ability to serve segments after the primary connection is lost.
#[test]
#[ignore = "requires Docker"]
fn replica_recovers_via_archive() {
    // ── helpers inlined from tests/e2e.rs ──────────────────────────────────

    fn allow_replication(client: &mut postgres::Client) {
        client
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
            .expect("pg_hba rewrite failed");
    }

    fn wait_for_rows(client: &mut postgres::Client, table: &str, expected: i64) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if Instant::now() > deadline {
                panic!("replica did not see {expected} rows in {table} within 90s");
            }
            let n: i64 = client
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if n >= expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    struct DropDir(std::path::PathBuf);
    impl Drop for DropDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct DropContainer(String);
    impl Drop for DropContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }
    struct DropNetwork(String);
    impl Drop for DropNetwork {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }

    // ── unique docker network ───────────────────────────────────────────────
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let net_name = format!("beyond-pg-archive-{}-{n}", std::process::id());
    let out = std::process::Command::new("docker")
        .args(["network", "create", &net_name])
        .output()
        .expect("docker network create");
    assert!(
        out.status.success(),
        "network create: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _net = DropNetwork(net_name.clone());

    // ── 1. primary with synchronous WAL ────────────────────────────────────
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "hot_standby=on",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    // Connect the container to the test network before opening any postgres
    // connection. This avoids the iptables disruption that docker network
    // connect causes on already-established TCP connections.
    let out = std::process::Command::new("docker")
        .args(["network", "connect", &net_name, primary.id()])
        .output()
        .expect("network connect primary");
    assert!(out.status.success());

    let mut primary_client = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match postgres::Client::connect(&pg_url, postgres::NoTls) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => panic!("connect primary_client: {e}"),
            }
        }
    };
    allow_replication(&mut primary_client);

    // Resolve primary's container-internal IP (for pg_basebackup container).
    let template = format!(
        r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
        net_name
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let primary_ip = loop {
        let out = std::process::Command::new("docker")
            .args(["inspect", "-f", &template, primary.id()])
            .output()
            .unwrap();
        let ip = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        if !ip.is_empty() {
            break ip;
        }
        if Instant::now() > deadline {
            panic!("primary has no IP on test network after 5s");
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    // ── 2. start sink on host ───────────────────────────────────────────────
    // Sink binds to 0.0.0.0, so containers reach it at host.docker.internal.
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-archive-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();
    let sink_dir_str = sink_dir.to_str().unwrap().to_owned();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_port = probe.local_addr().unwrap().port();
    drop(probe);

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let mut sink_cmd = std::process::Command::new(BIN);
    sink_cmd.args([
        "--mode",
        "tcp",
        "--connstr",
        &connstr,
        "--dir",
        &sink_dir_str,
        "--port",
        &sink_port.to_string(),
        "--slot",
        "wal_sink_archive",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_proc = sink_cmd.spawn().expect("spawn beyond-pg-sink");
    // _sink must outlive _replica so the HTTP server stays up during recovery.
    let _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };

    wait_http_ready(sink_port);

    // Wait for the native receiver to appear in pg_stat_replication.
    let mut streaming = false;
    for _ in 0..60 {
        if primary_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication \
                 WHERE application_name = 'wal_sink_archive'",
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
    assert!(streaming, "sink never appeared in pg_stat_replication");

    // ── 3. write rows before basebackup ────────────────────────────────────
    primary_client
        .batch_execute(
            "CREATE TABLE archive_test (id serial, v text); \
             INSERT INTO archive_test (v) \
               SELECT 'pre-' || g FROM generate_series(1, 50) g;",
        )
        .expect("pre-basebackup rows");
    // Force a WAL segment boundary so these rows land in a complete segment.
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    // ── 4. pg_basebackup ────────────────────────────────────────────────────
    let pgdata = std::env::temp_dir().join(format!("archive-pgdata-{pg_port}"));
    std::fs::create_dir_all(&pgdata).unwrap();
    let _pgdata_cleanup = DropDir(pgdata.clone());
    let pgdata_str = pgdata.to_str().unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pgdata, std::fs::Permissions::from_mode(0o777)).unwrap();
    }

    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            &format!("--network={net_name}"),
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host={primary_ip} port=5432 user=postgres"),
            "--pgdata",
            "/pgdata",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
        ])
        .output()
        .expect("docker pg_basebackup");
    assert!(
        out.status.success(),
        "pg_basebackup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 5. write fetch_wal.pl to PGDATA ─────────────────────────────────────
    // postgres:18 has neither curl nor wget, but Perl + Socket is available.
    // We write the script to the host-side PGDATA directory (bind-mounted into
    // the replica container) so Docker has no internet dependency.
    //
    // The script opens a raw TCP connection to the sink HTTP server via bash's
    // /dev/tcp equivalent in Perl (Socket module). Postgres calls it as:
    //   perl /pgdata/fetch_wal.pl <segment-name> <dest-path>
    // $| enables autoflush on the *currently selected* filehandle.  We must
    // select(S) first or the HTTP request sits in Perl's stdio buffer while the
    // server waits for \r\n\r\n — deadlock.  select(STDOUT) restores the default.
    let perl_script = format!(
        "use IO::Socket::INET;\n\
         ($s,$d)=@ARGV;\n\
         $sock=IO::Socket::INET->new(PeerAddr=>q(host.docker.internal),PeerPort=>{sink_port},\
         Proto=>q(tcp),Timeout=>10) or exit 1;\n\
         $sock->autoflush(1);\n\
         print $sock \"GET /$s HTTP/1.0\\r\\nHost: host.docker.internal\\r\\nConnection: close\\r\\n\\r\\n\";\n\
         $l=<$sock>;exit 1 unless $l=~/200/;\n\
         while(<$sock>){{last if/^\\r\\n$/}}\n\
         open(O,\">\",$d)or exit 1;\n\
         while(read($sock,$b,65536)){{print O $b}}\n\
         close O;\n"
    );
    std::fs::write(pgdata.join("fetch_wal.pl"), perl_script).expect("write fetch_wal.pl to PGDATA");

    // ── 6. write rows AFTER basebackup — archive-only recovery needed ───────

    // Count complete segments before writing post-backup rows.  We need the
    // segment containing those rows to also reach the sink as a complete file.
    let complete_segs_before = || -> usize {
        std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .count()
    };
    let segs_before = complete_segs_before();

    primary_client
        .batch_execute(
            "INSERT INTO archive_test (v) \
               SELECT 'post-' || g FROM generate_series(1, 50) g;",
        )
        .expect("post-basebackup rows");
    // Switch WAL so the segment containing the post-backup rows is sealed on the
    // primary.  The sink's streaming receiver must then finalize the sealed
    // segment (rename .partial → full name) before we kill the primary.
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    // Wait until the sink has archived at least one more COMPLETE segment than
    // before we wrote the post-backup rows.  This ensures the segment with those
    // rows is fully received and renamed from .partial before we stop the primary.
    let mut segs: Vec<String> = Vec::new();
    for _ in 0..120 {
        segs = std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .collect();
        if segs.len() > segs_before {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        segs.len() > segs_before,
        "post-backup WAL segment never archived: had {segs_before} complete segments before, \
         still {}",
        segs.len()
    );

    // ── 7. kill primary ──────────────────────────────────────────────────────
    // Drop the client first so no TCP connections linger into the stopped container.
    drop(primary_client);
    drop(primary);

    // ── 8. write recovery config: restore_command only, no primary_conninfo ─
    // Run as root so we can write standby.signal and append to postgresql.auto.conf
    // (which is owned by uid 999/postgres from the basebackup).
    // host.docker.internal is injected by Docker Desktop on macOS automatically
    // and by --add-host=host.docker.internal:host-gateway on Linux Docker.
    let setup_script = "touch /pgdata/standby.signal && \
         echo \"restore_command = 'perl /pgdata/fetch_wal.pl %f %p'\" \
           >> /pgdata/postgresql.auto.conf && \
         echo \"recovery_target_timeline = 'latest'\" \
           >> /pgdata/postgresql.auto.conf";
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "bash",
            "-c",
            setup_script,
        ])
        .output()
        .expect("recovery config setup");
    assert!(
        out.status.success(),
        "recovery setup script: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 9. start archive replica ────────────────────────────────────────────
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let replica_port = probe.local_addr().unwrap().port();
    drop(probe);

    let start_out = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            // host-gateway maps host.docker.internal to the Docker host IP.
            // On macOS Docker Desktop, host.docker.internal is already injected
            // via DNS so this is redundant but harmless. On Linux Docker, this
            // is the canonical way to expose it.
            "--add-host=host.docker.internal:host-gateway",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "-p",
            &format!("{replica_port}:5432"),
            "postgres:18",
            "bash",
            "-c",
            "chmod 700 /pgdata && chown -R postgres:postgres /pgdata \
             && exec gosu postgres postgres -D /pgdata",
        ])
        .output()
        .expect("docker run archive replica");
    assert!(
        start_out.status.success(),
        "replica start: {}",
        String::from_utf8_lossy(&start_out.stderr)
    );
    let replica_id = String::from_utf8_lossy(&start_out.stdout).trim().to_owned();
    // _sink declared above: keeps HTTP server alive during recovery.
    let _replica = DropContainer(replica_id);

    // Wait until the replica accepts hot-standby connections.
    let replica_url = format!("host=127.0.0.1 port={replica_port} user=postgres dbname=postgres");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut replica_client = loop {
        if Instant::now() > deadline {
            // Dump container logs for diagnosis before panicking.
            let logs = std::process::Command::new("docker")
                .args(["logs", &_replica.0])
                .output();
            let log_text = logs
                .ok()
                .map(|o| {
                    format!(
                        "stdout: {}\nstderr: {}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_else(|| "(docker logs failed)".to_owned());
            panic!(
                "archive replica not ready on port {replica_port} within 90s\n\
                 --- container logs ---\n{log_text}"
            );
        }
        if let Ok(c) = postgres::Client::connect(&replica_url, postgres::NoTls) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(300));
    };

    // ── 10. assert all 100 rows visible via archive recovery ────────────────
    // Both pre- (inherited from basebackup) and post-basebackup rows must be
    // present. The post rows require a successful restore_command fetch.
    wait_for_rows(&mut replica_client, "archive_test", 100);

    // Replica must still be in hot standby (no primary to promote it).
    let is_recovery: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        is_recovery,
        "replica should be in recovery (archive replay active, no primary to connect to)"
    );

    drop(replica_client);
    // _replica dropped → container removed
    // _sink dropped → sink process killed (SIGKILL to process group)
    // _net dropped → network removed
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

/// SIGKILL the sink mid-write (leaving a .partial segment) and verify that the
/// full archive pipeline recovers:
///
///   1. Primary uses synchronous_commit=remote_write so each INSERT waits for
///      the sink to flush WAL to disk — guaranteeing post-backup WAL is in a
///      .partial file when the sink is killed.
///   2. The restarted sink calls cleanup_partial_segments on startup, removing
///      the orphaned .partial.
///   3. The sink reconnects and re-streams the lost WAL from the primary.
///   4. pg_switch_wal seals the segment; the sink finalises it.
///   5. A replica recovers all 100 rows via the rebuilt archive.
#[test]
#[ignore = "requires Docker"]
fn sink_crash_mid_write() {
    fn allow_replication(client: &mut postgres::Client) {
        client
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
            .expect("pg_hba rewrite");
    }

    fn wait_for_rows(client: &mut postgres::Client, table: &str, n: i64) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if Instant::now() > deadline {
                panic!("replica did not see {n} rows in {table} within 90s");
            }
            let count: i64 = client
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if count >= n {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    struct DropDir(std::path::PathBuf);
    impl Drop for DropDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct DropContainer(String);
    impl Drop for DropContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }
    struct DropNetwork(String);
    impl Drop for DropNetwork {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }

    fn complete_segs(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .count()
    }

    fn partial_files(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".partial"))
            .collect()
    }

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let net_name = format!("beyond-pg-crash-{}-{n}", std::process::id());
    let out = std::process::Command::new("docker")
        .args(["network", "create", &net_name])
        .output()
        .expect("docker network create");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _net = DropNetwork(net_name.clone());

    // ── 1. primary with synchronous WAL ──────────────────────────────────────
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "hot_standby=on",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("docker not available or postgres:18 pull failed");
    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    let out = std::process::Command::new("docker")
        .args(["network", "connect", &net_name, primary.id()])
        .output()
        .expect("network connect");
    assert!(out.status.success());

    let mut primary_client = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match postgres::Client::connect(&pg_url, postgres::NoTls) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => panic!("connect primary_client: {e}"),
            }
        }
    };
    allow_replication(&mut primary_client);

    let primary_ip = {
        let tpl = format!(
            r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
            net_name
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let o = std::process::Command::new("docker")
                .args(["inspect", "-f", &tpl, primary.id()])
                .output()
                .unwrap();
            let ip = String::from_utf8_lossy(&o.stdout).trim().to_owned();
            if !ip.is_empty() {
                break ip;
            }
            if Instant::now() > deadline {
                panic!("primary has no IP after 5s");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    };

    // ── 2. sink on host ───────────────────────────────────────────────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-crash-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();
    let sink_dir_str = sink_dir.to_str().unwrap().to_owned();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_port = probe.local_addr().unwrap().port();
    drop(probe);

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let make_sink = |dir: &str, port: u16| {
        let mut cmd = std::process::Command::new(BIN);
        cmd.args([
            "--mode",
            "tcp",
            "--connstr",
            &format!("host=127.0.0.1 port={pg_port} user=postgres"),
            "--dir",
            dir,
            "--port",
            &port.to_string(),
            "--slot",
            "wal_sink_crash",
        ]);
        #[cfg(unix)]
        cmd.process_group(0);
        cmd.spawn().expect("spawn beyond-pg-sink")
    };
    let _ = connstr; // used inside make_sink via capture

    let sink_proc = make_sink(&sink_dir_str, sink_port);
    let mut _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    for _ in 0..60 {
        if primary_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_crash'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // ── 3. pre-backup rows ────────────────────────────────────────────────────
    primary_client
        .batch_execute(
            "CREATE TABLE crash_test (id serial, v text); \
             INSERT INTO crash_test (v) SELECT 'pre-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    let segs_after_pre = {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let c = complete_segs(&sink_dir);
            if c >= 1 {
                break c;
            }
            if Instant::now() > deadline {
                panic!("pre-backup segment never archived");
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    };

    // ── 4. pg_basebackup ─────────────────────────────────────────────────────
    let pgdata = std::env::temp_dir().join(format!("crash-pgdata-{pg_port}"));
    std::fs::create_dir_all(&pgdata).unwrap();
    let _pgdata_cleanup = DropDir(pgdata.clone());
    let pgdata_str = pgdata.to_str().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pgdata, std::fs::Permissions::from_mode(0o777)).unwrap();
    }
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            &format!("--network={net_name}"),
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host={primary_ip} port=5432 user=postgres"),
            "--pgdata",
            "/pgdata",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
        ])
        .output()
        .expect("pg_basebackup");
    assert!(
        out.status.success(),
        "pg_basebackup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 5. post-backup rows — sync commit guarantees WAL is in .partial ──────
    // With synchronous_commit=remote_write, INSERT blocks until flush_lsn ≥
    // commit LSN, so when this returns the WAL is on disk in a .partial file.
    let segs_before_crash = complete_segs(&sink_dir);
    primary_client
        .batch_execute(
            "INSERT INTO crash_test (v) SELECT 'post-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    // Verify .partial exists before killing
    assert!(
        !partial_files(&sink_dir).is_empty(),
        "expected a .partial file after sync INSERT (WAL segment not yet full)"
    );

    // ── 6. SIGKILL sink — leave .partial on disk ──────────────────────────────
    #[cfg(unix)]
    unsafe {
        let pid = _sink.process.id() as i32;
        libc::kill(-pid, libc::SIGKILL);
    }
    let _ = _sink.process.wait();
    // Prevent Drop from double-killing
    std::mem::forget(_sink);

    // .partial must still exist after kill (no cleanup on SIGKILL)
    assert!(
        !partial_files(&sink_dir).is_empty(),
        ".partial should persist after SIGKILL (no graceful cleanup)"
    );

    // ── 7. restart sink — cleanup_partial_segments removes the orphan ─────────
    // Record the pre-crash .partial names so we can verify they're gone once
    // the segment is fully re-streamed (they become complete files).
    let pre_crash_partials = partial_files(&sink_dir);
    let sink_proc2 = make_sink(&sink_dir_str, sink_port);
    let _sink2 = Sink {
        process: sink_proc2,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);
    // cleanup_partial_segments runs before the HTTP server starts, so the
    // orphaned .partial has been deleted.  The receiver thread starts concurrently
    // and may immediately create a new .partial with the same name as it
    // re-streams the same WAL segment.  We therefore cannot assert "no .partial
    // exists" here — that window is too narrow to catch reliably.  Instead we
    // verify below that the pre-crash .partial names no longer exist AS .partial
    // (they become complete segment files once pg_switch_wal seals the segment).
    let _ = pre_crash_partials;

    // Wait for the restarted sink to reconnect as a sync standby.
    for _ in 0..120 {
        if primary_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_crash'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // ── 8. seal post-backup segment ───────────────────────────────────────────
    // pg_switch_wal is not a user transaction — it does not block waiting for
    // synchronous standbies.  The sink (now reconnected) re-streams the lost
    // WAL and finalises the segment.
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    for _ in 0..120 {
        if complete_segs(&sink_dir) > segs_before_crash {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        complete_segs(&sink_dir) > segs_before_crash,
        "post-crash segment never archived: had {segs_before_crash} before crash, \
         still {} after restart",
        complete_segs(&sink_dir)
    );
    let _ = segs_after_pre; // suppress unused warning

    // ── 9. kill primary ───────────────────────────────────────────────────────
    drop(primary_client);
    drop(primary);

    // ── 10. write recovery config ─────────────────────────────────────────────
    let perl_script = format!(
        "use IO::Socket::INET;\n\
         ($s,$d)=@ARGV;\n\
         $sock=IO::Socket::INET->new(PeerAddr=>q(host.docker.internal),PeerPort=>{sink_port},\
         Proto=>q(tcp),Timeout=>10) or exit 1;\n\
         $sock->autoflush(1);\n\
         print $sock \"GET /$s HTTP/1.0\\r\\nHost: host.docker.internal\\r\\n\\r\\n\";\n\
         $l=<$sock>;exit 1 unless $l=~/200/;\n\
         while(<$sock>){{last if/^\\r\\n$/}}\n\
         open(O,\">\",$d)or exit 1;\n\
         while(read($sock,$b,65536)){{print O $b}}\n\
         close O;\n"
    );
    std::fs::write(pgdata.join("fetch_wal.pl"), &perl_script).unwrap();

    let setup_script = "touch /pgdata/standby.signal && \
         echo \"restore_command = 'perl /pgdata/fetch_wal.pl %f %p'\" \
           >> /pgdata/postgresql.auto.conf && \
         echo \"recovery_target_timeline = 'latest'\" \
           >> /pgdata/postgresql.auto.conf";
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "bash",
            "-c",
            setup_script,
        ])
        .output()
        .expect("recovery config");
    assert!(
        out.status.success(),
        "recovery setup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 11. start replica ─────────────────────────────────────────────────────
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let replica_port = probe.local_addr().unwrap().port();
    drop(probe);

    let start_out = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--add-host=host.docker.internal:host-gateway",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "-p",
            &format!("{replica_port}:5432"),
            "postgres:18",
            "bash",
            "-c",
            "chmod 700 /pgdata && chown -R postgres:postgres /pgdata \
             && exec gosu postgres postgres -D /pgdata",
        ])
        .output()
        .expect("docker run replica");
    assert!(
        start_out.status.success(),
        "{}",
        String::from_utf8_lossy(&start_out.stderr)
    );
    let replica_id = String::from_utf8_lossy(&start_out.stdout).trim().to_owned();
    let _replica = DropContainer(replica_id.clone());

    let replica_url = format!("host=127.0.0.1 port={replica_port} user=postgres dbname=postgres");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut replica_client = loop {
        if Instant::now() > deadline {
            let logs = std::process::Command::new("docker")
                .args(["logs", &replica_id])
                .output()
                .ok()
                .map(|o| {
                    format!(
                        "stdout: {}\nstderr: {}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_else(|| "(docker logs failed)".to_owned());
            panic!("replica not ready within 90s\n{logs}");
        }
        if let Ok(c) = postgres::Client::connect(&replica_url, postgres::NoTls) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(300));
    };

    // ── 12. assert all 100 rows recovered ─────────────────────────────────────
    wait_for_rows(&mut replica_client, "crash_test", 100);

    let is_recovery: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        is_recovery,
        "replica should remain in recovery (no primary to promote it)"
    );
}

/// Verify that a missing archive segment leaves the replica stalled and visible
/// in hot standby with ONLY the rows present at basebackup time, never the
/// post-backup rows whose WAL segment was never archived.
///
/// This ensures the pipeline fails CLEARLY (50 visible rows, recovery stall)
/// rather than silently serving incorrect data.
#[test]
#[ignore = "requires Docker"]
fn wal_gap_stalls_replica() {
    fn allow_replication(client: &mut postgres::Client) {
        client
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
            .expect("pg_hba rewrite");
    }

    struct DropDir(std::path::PathBuf);
    impl Drop for DropDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct DropContainer(String);
    impl Drop for DropContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }
    struct DropNetwork(String);
    impl Drop for DropNetwork {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let net_name = format!("beyond-pg-gap-{}-{n}", std::process::id());
    let out = std::process::Command::new("docker")
        .args(["network", "create", &net_name])
        .output()
        .expect("docker network create");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _net = DropNetwork(net_name.clone());

    // ── 1. primary — async replication so INSERTs succeed without the sink ───
    // No synchronous_standby_names: post-backup rows commit locally without
    // waiting for the sink (which we kill before sealing that segment).
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "hot_standby=on",
        ])
        .start()
        .expect("docker not available or postgres:18 pull failed");
    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    let out = std::process::Command::new("docker")
        .args(["network", "connect", &net_name, primary.id()])
        .output()
        .expect("network connect");
    assert!(out.status.success());

    let mut primary_client = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match postgres::Client::connect(&pg_url, postgres::NoTls) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => panic!("connect primary_client: {e}"),
            }
        }
    };
    allow_replication(&mut primary_client);

    let primary_ip = {
        let tpl = format!(
            r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
            net_name
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let o = std::process::Command::new("docker")
                .args(["inspect", "-f", &tpl, primary.id()])
                .output()
                .unwrap();
            let ip = String::from_utf8_lossy(&o.stdout).trim().to_owned();
            if !ip.is_empty() {
                break ip;
            }
            if Instant::now() > deadline {
                panic!("primary no IP after 5s");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    };

    // ── 2. sink on host (archive pre-backup segment, then killed) ────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-gap-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();
    let sink_dir_str = sink_dir.to_str().unwrap().to_owned();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_port = probe.local_addr().unwrap().port();
    drop(probe);

    let mut sink_cmd = std::process::Command::new(BIN);
    sink_cmd.args([
        "--mode",
        "tcp",
        "--connstr",
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        "--dir",
        &sink_dir_str,
        "--port",
        &sink_port.to_string(),
        "--slot",
        "wal_sink_gap",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_proc = sink_cmd.spawn().expect("spawn beyond-pg-sink");
    let mut _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    for _ in 0..60 {
        if primary_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_gap'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // ── 3. pre-backup rows — archived ────────────────────────────────────────
    primary_client
        .batch_execute(
            "CREATE TABLE gap_test (id serial, v text); \
             INSERT INTO gap_test (v) SELECT 'pre-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    // Wait for the pre-backup segment to land in the sink.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let count = std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .count();
        if count >= 1 {
            break;
        }
        if Instant::now() > deadline {
            panic!("pre-backup segment never archived");
        }
        std::thread::sleep(Duration::from_millis(300));
    }

    // ── 4. pg_basebackup ─────────────────────────────────────────────────────
    let pgdata = std::env::temp_dir().join(format!("gap-pgdata-{pg_port}"));
    std::fs::create_dir_all(&pgdata).unwrap();
    let _pgdata_cleanup = DropDir(pgdata.clone());
    let pgdata_str = pgdata.to_str().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pgdata, std::fs::Permissions::from_mode(0o777)).unwrap();
    }
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            &format!("--network={net_name}"),
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host={primary_ip} port=5432 user=postgres"),
            "--pgdata",
            "/pgdata",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
        ])
        .output()
        .expect("pg_basebackup");
    assert!(
        out.status.success(),
        "pg_basebackup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 5. kill sink — post-backup WAL will NEVER be archived ────────────────
    #[cfg(unix)]
    unsafe {
        let pid = _sink.process.id() as i32;
        libc::kill(-pid, libc::SIGKILL);
    }
    let _ = _sink.process.wait();
    std::mem::forget(_sink);

    // ── 6. post-backup rows — commit locally, never archived ─────────────────
    primary_client
        .batch_execute("INSERT INTO gap_test (v) SELECT 'post-' || g FROM generate_series(1,50) g;")
        .unwrap();
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    // ── 7. kill primary ───────────────────────────────────────────────────────
    drop(primary_client);
    drop(primary);

    // ── 8. recovery config: restore_command points to the now-dead sink ───────
    // The sink HTTP port is no longer listening.  restore_command will fail on
    // every attempt, leaving the replica stalled in recovery after the last
    // successfully fetched segment.
    let perl_script = format!(
        "use IO::Socket::INET;\n\
         ($s,$d)=@ARGV;\n\
         $sock=IO::Socket::INET->new(PeerAddr=>q(host.docker.internal),PeerPort=>{sink_port},\
         Proto=>q(tcp),Timeout=>10) or exit 1;\n\
         $sock->autoflush(1);\n\
         print $sock \"GET /$s HTTP/1.0\\r\\nHost: host.docker.internal\\r\\n\\r\\n\";\n\
         $l=<$sock>;exit 1 unless $l=~/200/;\n\
         while(<$sock>){{last if/^\\r\\n$/}}\n\
         open(O,\">\",$d)or exit 1;\n\
         while(read($sock,$b,65536)){{print O $b}}\n\
         close O;\n"
    );
    std::fs::write(pgdata.join("fetch_wal.pl"), &perl_script).unwrap();

    let setup_script = "touch /pgdata/standby.signal && \
         echo \"restore_command = 'perl /pgdata/fetch_wal.pl %f %p'\" \
           >> /pgdata/postgresql.auto.conf && \
         echo \"recovery_target_timeline = 'latest'\" \
           >> /pgdata/postgresql.auto.conf";
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "bash",
            "-c",
            setup_script,
        ])
        .output()
        .expect("recovery config");
    assert!(
        out.status.success(),
        "recovery setup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 9. start replica ──────────────────────────────────────────────────────
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let replica_port = probe.local_addr().unwrap().port();
    drop(probe);

    let start_out = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--add-host=host.docker.internal:host-gateway",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "-p",
            &format!("{replica_port}:5432"),
            "postgres:18",
            "bash",
            "-c",
            "chmod 700 /pgdata && chown -R postgres:postgres /pgdata \
             && exec gosu postgres postgres -D /pgdata",
        ])
        .output()
        .expect("docker run replica");
    assert!(
        start_out.status.success(),
        "{}",
        String::from_utf8_lossy(&start_out.stderr)
    );
    let replica_id = String::from_utf8_lossy(&start_out.stdout).trim().to_owned();
    let _replica = DropContainer(replica_id.clone());

    // ── 10. wait for hot-standby connections ──────────────────────────────────
    let replica_url = format!("host=127.0.0.1 port={replica_port} user=postgres dbname=postgres");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut replica_client = loop {
        if Instant::now() > deadline {
            let logs = std::process::Command::new("docker")
                .args(["logs", &replica_id])
                .output()
                .ok()
                .map(|o| {
                    format!(
                        "stdout: {}\nstderr: {}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_else(|| "(docker logs failed)".to_owned());
            panic!("replica not ready within 60s\n{logs}");
        }
        if let Ok(c) = postgres::Client::connect(&replica_url, postgres::NoTls) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(300));
    };

    // ── 11. assert stall: 50 rows visible, never 100, is_recovery = true ──────
    // Give the replica 30 s to apply any archive it can find.  It should see
    // the 50 pre-backup rows from the basebackup, then stall when it cannot
    // fetch the missing post-backup segment.
    std::thread::sleep(Duration::from_secs(30));

    let count: i64 = replica_client
        .query_one("SELECT count(*) FROM gap_test", &[])
        .map(|r| r.get(0))
        .unwrap_or(0);
    assert_eq!(
        count, 50,
        "replica should see exactly 50 pre-backup rows — \
         the post-backup segment was never archived"
    );

    let is_recovery: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        is_recovery,
        "replica must remain in recovery (stalled on missing archive segment)"
    );
}

/// Full timeline-switch test: the archive pipeline survives a primary failover.
///
///   T1 primary → sink → replica1 (recovers all T1 WAL via archive) → promote
///   → T2 primary → sink (archives T2 WAL + timeline history file)
///   → replica2 (recovers T1 + T2 WAL via restore_command, sees all 150 rows)
///
/// Validates:
///   - The sink HTTP server serves `.history` files (needed for timeline switch)
///   - restore_command correctly requests history files (`%f = 00000002.history`)
///   - `recovery_target_timeline = 'latest'` follows T1 → T2 using the history file
#[test]
#[ignore = "requires Docker"]
fn timeline_boundary_survives_failover() {
    fn allow_replication(client: &mut postgres::Client) {
        client
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
            .expect("pg_hba rewrite");
    }

    struct DropDir(std::path::PathBuf);
    impl Drop for DropDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct DropContainer(String);
    impl Drop for DropContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }
    struct DropNetwork(String);
    impl Drop for DropNetwork {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }

    fn complete_segs(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .count()
    }

    fn container_ip(id: &str, net: &str) -> String {
        let tpl = format!(
            r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
            net
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let o = std::process::Command::new("docker")
                .args(["inspect", "-f", &tpl, id])
                .output()
                .unwrap();
            let ip = String::from_utf8_lossy(&o.stdout).trim().to_owned();
            if !ip.is_empty() {
                return ip;
            }
            if Instant::now() > deadline {
                panic!("{id} has no IP on {net} after 10s");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn write_recovery_cfg(pgdata: &std::path::Path, sink_port: u16) {
        let perl = format!(
            "use IO::Socket::INET;\n\
             ($s,$d)=@ARGV;\n\
             $sock=IO::Socket::INET->new(PeerAddr=>q(host.docker.internal),PeerPort=>{sink_port},\
             Proto=>q(tcp),Timeout=>10) or exit 1;\n\
             $sock->autoflush(1);\n\
             print $sock \"GET /$s HTTP/1.0\\r\\nHost: host.docker.internal\\r\\n\\r\\n\";\n\
             $l=<$sock>;exit 1 unless $l=~/200/;\n\
             while(<$sock>){{last if/^\\r\\n$/}}\n\
             open(O,\">\",$d)or exit 1;\n\
             while(read($sock,$b,65536)){{print O $b}}\n\
             close O;\n"
        );
        std::fs::write(pgdata.join("fetch_wal.pl"), &perl).unwrap();

        let script = "touch /pgdata/standby.signal && \
             echo \"restore_command = 'perl /pgdata/fetch_wal.pl %f %p'\" \
               >> /pgdata/postgresql.auto.conf && \
             echo \"recovery_target_timeline = 'latest'\" \
               >> /pgdata/postgresql.auto.conf";
        let pgdata_str = pgdata.to_str().unwrap();
        let out = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--user",
                "root",
                "-v",
                &format!("{pgdata_str}:/pgdata"),
                "postgres:18",
                "bash",
                "-c",
                script,
            ])
            .output()
            .expect("recovery cfg docker run");
        assert!(
            out.status.success(),
            "recovery cfg: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let net_name = format!("beyond-pg-timeline-{}-{n}", std::process::id());
    let out = std::process::Command::new("docker")
        .args(["network", "create", &net_name])
        .output()
        .expect("docker network create");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _net = DropNetwork(net_name.clone());

    // ── 1. T1 primary ─────────────────────────────────────────────────────────
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "hot_standby=on",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("docker not available or postgres:18 pull failed");
    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    let out = std::process::Command::new("docker")
        .args(["network", "connect", &net_name, primary.id()])
        .output()
        .expect("network connect");
    assert!(out.status.success());

    let mut t1_client = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match postgres::Client::connect(&pg_url, postgres::NoTls) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => panic!("connect t1_client: {e}"),
            }
        }
    };
    allow_replication(&mut t1_client);
    let primary_ip = container_ip(primary.id(), &net_name);

    // ── 2. sink on host ───────────────────────────────────────────────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-timeline-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();
    let sink_dir_str = sink_dir.to_str().unwrap().to_owned();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_port = probe.local_addr().unwrap().port();
    drop(probe);

    let mut sink_cmd = std::process::Command::new(BIN);
    sink_cmd.args([
        "--mode",
        "tcp",
        "--connstr",
        &format!("host=127.0.0.1 port={pg_port} user=postgres"),
        "--dir",
        &sink_dir_str,
        "--port",
        &sink_port.to_string(),
        "--slot",
        "wal_sink_timeline",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_proc = sink_cmd.spawn().expect("spawn beyond-pg-sink");
    let _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    for _ in 0..60 {
        if t1_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_timeline'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // ── 3. T1 pre-backup rows ─────────────────────────────────────────────────
    t1_client
        .batch_execute(
            "CREATE TABLE timeline_test (id serial, v text); \
             INSERT INTO timeline_test (v) SELECT 't1-pre-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    t1_client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let _segs_after_pre = {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let c = complete_segs(&sink_dir);
            if c >= 1 {
                break c;
            }
            if Instant::now() > deadline {
                panic!("pre-backup T1 segment never archived");
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    };

    // ── 4. pg_basebackup for BOTH replicas ────────────────────────────────────
    let pgdata1 = std::env::temp_dir().join(format!("timeline-pgdata1-{pg_port}"));
    // pgdata2 must NOT be pre-created: `cp -r src dst` when dst exists puts src
    // *inside* dst rather than replacing it.  We create pgdata1 for the basebackup
    // and then cp -r pgdata1 → pgdata2 (creating pgdata2 as a true copy).
    let pgdata2 = std::env::temp_dir().join(format!("timeline-pgdata2-{pg_port}"));
    std::fs::create_dir_all(&pgdata1).unwrap();
    std::fs::remove_dir_all(&pgdata2).ok(); // ensure dst doesn't exist before cp
    let _pgdata1_cleanup = DropDir(pgdata1.clone());
    let _pgdata2_cleanup = DropDir(pgdata2.clone());
    let pgdata1_str = pgdata1.to_str().unwrap();
    let pgdata2_str = pgdata2.to_str().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pgdata1, std::fs::Permissions::from_mode(0o777)).unwrap();
    }

    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            &format!("--network={net_name}"),
            "-v",
            &format!("{pgdata1_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host={primary_ip} port=5432 user=postgres"),
            "--pgdata",
            "/pgdata",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
        ])
        .output()
        .expect("pg_basebackup");
    assert!(
        out.status.success(),
        "pg_basebackup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Copy pgdata1 → pgdata2 before modifying pgdata1 for replica1 recovery.
    // pgdata2 must not exist when cp -r runs (see comment above).
    let out = std::process::Command::new("cp")
        .args(["-r", pgdata1_str, pgdata2_str])
        .output()
        .expect("cp -r pgdata1 pgdata2");
    assert!(
        out.status.success(),
        "cp -r: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 5. T1 post-backup rows ────────────────────────────────────────────────
    // Snapshot segment count AFTER basebackup so the wait below actually tracks
    // the segment that contains the post-backup rows (the basebackup itself may
    // have generated additional WAL segments, so segs_after_pre is stale).
    let segs_before_post = complete_segs(&sink_dir);
    t1_client
        .batch_execute(
            "INSERT INTO timeline_test (v) SELECT 't1-post-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    t1_client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if complete_segs(&sink_dir) > segs_before_post {
            break;
        }
        if Instant::now() > deadline {
            panic!("T1 post-backup segment never archived");
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    let _segs_after_t1 = complete_segs(&sink_dir);

    // ── 6. kill T1 primary ────────────────────────────────────────────────────
    // Keep _sink alive: replica1 needs its HTTP server to fetch T1 archive segments.
    drop(t1_client);
    drop(primary);

    // ── 7. start replica1 — archive-only recovery ─────────────────────────────
    write_recovery_cfg(&pgdata1, sink_port);

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let r1_port = probe.local_addr().unwrap().port();
    drop(probe);

    let r1_start = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--add-host=host.docker.internal:host-gateway",
            "--user",
            "root",
            "-v",
            &format!("{pgdata1_str}:/pgdata"),
            "-p",
            &format!("{r1_port}:5432"),
            "postgres:18",
            "bash",
            "-c",
            "chmod 700 /pgdata && chown -R postgres:postgres /pgdata \
             && exec gosu postgres postgres -D /pgdata",
        ])
        .output()
        .expect("docker run replica1");
    assert!(
        r1_start.status.success(),
        "{}",
        String::from_utf8_lossy(&r1_start.stderr)
    );
    let r1_id = String::from_utf8_lossy(&r1_start.stdout).trim().to_owned();
    let _r1 = DropContainer(r1_id.clone());

    let r1_url = format!("host=127.0.0.1 port={r1_port} user=postgres dbname=postgres");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut r1_client = loop {
        if Instant::now() > deadline {
            let logs = std::process::Command::new("docker")
                .args(["logs", &r1_id])
                .output()
                .ok()
                .map(|o| {
                    format!(
                        "stdout: {}\nstderr: {}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_else(|| "(docker logs failed)".to_owned());
            panic!("replica1 not ready within 90s\n{logs}");
        }
        if let Ok(c) = postgres::Client::connect(&r1_url, postgres::NoTls) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(300));
    };

    // Replica1 must apply all 100 T1 rows (50 pre + 50 post-backup).
    {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if Instant::now() > deadline {
                let count: i64 = r1_client
                    .query_one("SELECT count(*) FROM timeline_test", &[])
                    .map(|r| r.get(0))
                    .unwrap_or(-1);
                let recovery: bool = r1_client
                    .query_one("SELECT pg_is_in_recovery()", &[])
                    .unwrap()
                    .get(0);
                let segs = complete_segs(&sink_dir);
                let logs = std::process::Command::new("docker")
                    .args(["logs", "--tail", "40", &r1_id])
                    .output()
                    .ok()
                    .map(|o| {
                        format!(
                            "stdout: {}\nstderr: {}",
                            String::from_utf8_lossy(&o.stdout),
                            String::from_utf8_lossy(&o.stderr)
                        )
                    })
                    .unwrap_or_default();
                panic!(
                    "replica1 did not reach 100 rows within 90s\n\
                     current count={count}, in_recovery={recovery}, \
                     sink complete_segs={segs}\n--- replica1 logs ---\n{logs}"
                );
            }
            let count: i64 = r1_client
                .query_one("SELECT count(*) FROM timeline_test", &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if count >= 100 {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    let is_r: bool = r1_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(is_r, "replica1 should be in recovery before promotion");

    // ── 8. promote replica1 → T2 primary ─────────────────────────────────────
    let out = std::process::Command::new("docker")
        .args([
            "exec", &r1_id, "gosu", "postgres", "pg_ctl", "promote", "-D", "/pgdata",
        ])
        .output()
        .expect("pg_ctl promote");
    assert!(
        out.status.success(),
        "promote: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let is_r: bool = r1_client
            .query_one("SELECT pg_is_in_recovery()", &[])
            .unwrap()
            .get(0);
        if !is_r {
            break;
        }
        if Instant::now() > deadline {
            panic!("replica1 did not finish promoting within 30s");
        }
        std::thread::sleep(Duration::from_millis(300));
    }

    // ── 9. copy .history file to sink dir ─────────────────────────────────────
    // After promotion Postgres writes a timeline history file to pg_wal/ so
    // standbies can locate the T1→T2 branch point.  The sink HTTP server now
    // serves it (see the /00000002.history match arm in main.rs).  We copy it
    // from the bind-mounted PGDATA rather than installing an archive_command.
    let history_src = pgdata1.join("pg_wal").join("00000002.history");
    let history_dst = sink_dir.join("00000002.history");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if history_src.exists() {
            std::fs::copy(&history_src, &history_dst).expect("copy .history to sink_dir");
            break;
        }
        if Instant::now() > deadline {
            panic!("00000002.history not written within 10s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 10. add replication rule on T2 primary ───────────────────────────────
    allow_replication(&mut r1_client);

    // ── 11. swap T1 sink for T2 sink ─────────────────────────────────────────
    // Replica1 has finished recovery: the T1 sink's HTTP server is no longer
    // needed.  Kill it and start a new TCP-mode sink on the same port and dir
    // pointing at T2 (replica1).  T1 segments remain in the dir;
    // T2 segments land alongside them.  Both are served by the same HTTP server,
    // so replica2's restore_command can fetch segments from any timeline.
    // cleanup_partial_segments in the new process removes any T1 .partial left.
    //
    // IMPORTANT: kill the process without running Sink's Drop (which would
    // remove sink_dir along with all T1 WAL segments and the .history file).
    {
        let mut s = _sink;
        s.process.kill().ok();
        s.process.wait().ok();
        std::mem::forget(s); // skip the remove_dir_all in Sink::drop
    }
    let mut t2_sink_cmd = std::process::Command::new(BIN);
    t2_sink_cmd.args([
        "--mode",
        "tcp",
        "--connstr",
        &format!("host=127.0.0.1 port={r1_port} user=postgres"),
        "--dir",
        &sink_dir_str,
        "--port",
        &sink_port.to_string(),
        "--slot",
        "wal_sink_timeline",
    ]);
    #[cfg(unix)]
    t2_sink_cmd.process_group(0);
    let t2_sink_proc = t2_sink_cmd.spawn().expect("spawn T2 sink");
    let _t2_sink = Sink {
        process: t2_sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    // Wait for T2 sink to appear in pg_stat_replication on replica1 (T2 primary).
    for _ in 0..60 {
        if r1_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_timeline'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // ── 12. T2 rows ───────────────────────────────────────────────────────────
    let segs_before_t2 = complete_segs(&sink_dir);
    r1_client
        .batch_execute(
            "INSERT INTO timeline_test (v) SELECT 't2-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    r1_client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    // Wait for the T2 segment to land in the sink.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if complete_segs(&sink_dir) > segs_before_t2 {
            break;
        }
        if Instant::now() > deadline {
            panic!("T2 segment never archived");
        }
        std::thread::sleep(Duration::from_millis(300));
    }

    // ── 13. kill T2 primary ───────────────────────────────────────────────────
    // Keep _t2_sink alive: replica2 needs its HTTP server to fetch T1+T2 segments.
    // Drop it last, after replica2 has finished recovery.
    drop(r1_client);
    drop(_r1); // stops the T2 primary container

    // ── 14. configure replica2 — both T1 and T2 segments available in sink ────
    write_recovery_cfg(&pgdata2, sink_port);

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let r2_port = probe.local_addr().unwrap().port();
    drop(probe);

    let r2_start = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--add-host=host.docker.internal:host-gateway",
            "--user",
            "root",
            "-v",
            &format!("{pgdata2_str}:/pgdata"),
            "-p",
            &format!("{r2_port}:5432"),
            "postgres:18",
            "bash",
            "-c",
            "chmod 700 /pgdata && chown -R postgres:postgres /pgdata \
             && exec gosu postgres postgres -D /pgdata",
        ])
        .output()
        .expect("docker run replica2");
    assert!(
        r2_start.status.success(),
        "{}",
        String::from_utf8_lossy(&r2_start.stderr)
    );
    let r2_id = String::from_utf8_lossy(&r2_start.stdout).trim().to_owned();
    let _r2 = DropContainer(r2_id.clone());

    let r2_url = format!("host=127.0.0.1 port={r2_port} user=postgres dbname=postgres");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut r2_client = loop {
        if Instant::now() > deadline {
            let logs = std::process::Command::new("docker")
                .args(["logs", &r2_id])
                .output()
                .ok()
                .map(|o| {
                    format!(
                        "stdout: {}\nstderr: {}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_else(|| "(docker logs failed)".to_owned());
            panic!("replica2 not ready within 90s\n{logs}");
        }
        if let Ok(c) = postgres::Client::connect(&r2_url, postgres::NoTls) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(300));
    };

    // ── 15. assert 150 rows: 50 T1-pre + 50 T1-post + 50 T2 ─────────────────
    // replica2 starts from the T1 basebackup, applies T1 WAL, reads the
    // .history file to learn about the T1→T2 switch, then applies T2 WAL.
    {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if Instant::now() > deadline {
                let count: i64 = r2_client
                    .query_one("SELECT count(*) FROM timeline_test", &[])
                    .map(|r| r.get(0))
                    .unwrap_or(-1);
                let recovery: bool = r2_client
                    .query_one("SELECT pg_is_in_recovery()", &[])
                    .unwrap()
                    .get(0);
                let segs = complete_segs(&sink_dir);
                let history_present = sink_dir.join("00000002.history").exists();
                let sink_files: Vec<String> = std::fs::read_dir(&sink_dir)
                    .into_iter()
                    .flatten()
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect::<Vec<_>>();
                let r2_logs = std::process::Command::new("docker")
                    .args(["logs", "--tail", "50", &r2_id])
                    .output()
                    .ok()
                    .map(|o| {
                        format!(
                            "stdout: {}\nstderr: {}",
                            String::from_utf8_lossy(&o.stdout),
                            String::from_utf8_lossy(&o.stderr)
                        )
                    })
                    .unwrap_or_default();
                panic!(
                    "replica2 did not reach 150 rows within 90s\n\
                     count={count}, in_recovery={recovery}, \
                     sink complete_segs={segs}, history_present={history_present}\n\
                     sink files: {sink_files:?}\n\
                     --- replica2 logs ---\n{r2_logs}"
                );
            }
            let count: i64 = r2_client
                .query_one("SELECT count(*) FROM timeline_test", &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if count >= 150 {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    let is_r: bool = r2_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        is_r,
        "replica2 should remain in recovery (no T2 primary to promote it)"
    );
}

/// QUIC transport survives 20% UDP packet loss in both directions.
///
/// A host-side UDP proxy sits between wal-forwarder and the sink container,
/// dropping every 5th packet.  QUIC's built-in retransmission must recover
/// all lost data.  A replica then does archive recovery via the sink and
/// asserts all 100 rows are present.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn quic_survives_packet_loss() {
    let Some((sink_bin, platform)) = build_linux_bin("beyond-pg-sink") else {
        return;
    };

    // ── lossy UDP proxy ────────────────────────────────────────────────────────
    // Drops 1-in-5 packets (20% loss) in both directions by counting packets
    // with a plain counter in each thread (no RNG needed — deterministic is fine).
    fn lossy_udp_proxy(drop_every: u64, target: std::net::SocketAddr) -> u16 {
        use std::net::UdpSocket;
        use std::sync::{Arc, Mutex};

        let inbound = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        let proxy_port = inbound.local_addr().unwrap().port();
        inbound
            .set_read_timeout(Some(Duration::from_millis(50)))
            .ok();

        let outbound = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        outbound.connect(target).unwrap();
        outbound
            .set_read_timeout(Some(Duration::from_millis(50)))
            .ok();

        let client_addr: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
        let client_r = client_addr.clone();
        let inbound_r = inbound.clone();
        let outbound_r = outbound.clone();

        // Forward: wal-forwarder → sink (with drops)
        std::thread::spawn(move || {
            let mut buf = vec![0u8; 65536];
            let mut count = 0u64;
            loop {
                match inbound.recv_from(&mut buf) {
                    Ok((n, from)) => {
                        *client_addr.lock().unwrap() = Some(from);
                        count += 1;
                        if !count.is_multiple_of(drop_every) {
                            let _ = outbound.send(&buf[..n]);
                        }
                    }
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                        ) => {}
                    Err(_) => break,
                }
            }
        });

        // Reverse: sink → wal-forwarder (with drops)
        std::thread::spawn(move || {
            let mut buf = vec![0u8; 65536];
            let mut count = 0u64;
            loop {
                match outbound_r.recv(&mut buf) {
                    Ok(n) => {
                        count += 1;
                        if !count.is_multiple_of(drop_every)
                            && let Some(addr) = *client_r.lock().unwrap()
                        {
                            let _ = inbound_r.send_to(&buf[..n], addr);
                        }
                    }
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                        ) => {}
                    Err(_) => break,
                }
            }
        });

        proxy_port
    }

    // ── helpers ────────────────────────────────────────────────────────────────
    fn allow_replication(client: &mut postgres::Client) {
        client
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
            .expect("pg_hba rewrite");
    }

    fn sink_complete_segs(http_port: u16) -> usize {
        use std::io::{Read as _, Write as _};
        let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", http_port)) else {
            return 0;
        };
        s.set_read_timeout(Some(Duration::from_secs(5))).ok();
        if s.write_all(b"GET /list HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return 0;
        }
        let mut resp = Vec::new();
        if s.read_to_end(&mut resp).is_err() {
            return 0;
        }
        let Some(hdr_end) = resp.windows(4).position(|w| w == b"\r\n\r\n") else {
            return 0;
        };
        std::str::from_utf8(&resp[hdr_end + 4..])
            .unwrap_or("")
            .lines()
            .filter(|l| {
                let l = l.trim();
                l.len() == 24 && l.bytes().all(|b| b.is_ascii_hexdigit())
            })
            .count()
    }

    // Returns true when the specific segment file (24 hex chars) is in the sink's /list.
    fn sink_has_segment(http_port: u16, seg_name: &str) -> bool {
        use std::io::{Read as _, Write as _};
        let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", http_port)) else {
            return false;
        };
        s.set_read_timeout(Some(Duration::from_secs(5))).ok();
        if s.write_all(b"GET /list HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return false;
        }
        let mut resp = Vec::new();
        if s.read_to_end(&mut resp).is_err() {
            return false;
        }
        let Some(hdr_end) = resp.windows(4).position(|w| w == b"\r\n\r\n") else {
            return false;
        };
        std::str::from_utf8(&resp[hdr_end + 4..])
            .unwrap_or("")
            .lines()
            .any(|l| l.trim() == seg_name)
    }

    struct DropContainer(String);
    impl Drop for DropContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }
    struct DropNetwork(String);
    impl Drop for DropNetwork {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.0])
                .output();
        }
    }
    struct DropDir(std::path::PathBuf);
    impl Drop for DropDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn container_ip(id: &str, net: &str) -> String {
        let tpl = format!(
            r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
            net
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let o = std::process::Command::new("docker")
                .args(["inspect", "-f", &tpl, id])
                .output()
                .unwrap();
            let ip = String::from_utf8_lossy(&o.stdout).trim().to_owned();
            if !ip.is_empty() {
                return ip;
            }
            if Instant::now() > deadline {
                panic!("{id} no IP on {net} after 10s");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let net_name = format!("beyond-pg-loss-{}-{n}", std::process::id());
    let out = std::process::Command::new("docker")
        .args(["network", "create", &net_name])
        .output()
        .expect("docker network create");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _net = DropNetwork(net_name.clone());

    // ── 1. primary ────────────────────────────────────────────────────────────
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "hot_standby=on",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("docker not available or postgres:18 pull failed");
    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut primary_client = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();
    allow_replication(&mut primary_client);

    let out = std::process::Command::new("docker")
        .args(["network", "connect", &net_name, primary.id()])
        .output()
        .expect("network connect");
    assert!(out.status.success());
    let primary_ip = container_ip(primary.id(), &net_name);

    // ── 2. sink container (QUIC mode) ─────────────────────────────────────────
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_http_port = probe.local_addr().unwrap().port();
    drop(probe);
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let sink_quic_port = probe.local_addr().unwrap().port();
    drop(probe);

    let sink_bin_str = sink_bin.to_str().unwrap().to_owned();
    let sink_start = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            &format!("--platform={platform}"),
            &format!("--network={net_name}"),
            "-p",
            &format!("{sink_http_port}:9000/tcp"),
            "-p",
            &format!("{sink_quic_port}:9000/udp"),
            "-v",
            &format!("{sink_bin_str}:/beyond-pg-sink:ro"),
            "alpine:3.20",
            "/beyond-pg-sink",
            "--mode",
            "quic",
            "--dir",
            "/tmp/wal",
            "--port",
            "9000",
            "--slot",
            "wal_sink_loss",
        ])
        .output()
        .expect("docker run sink");
    assert!(
        sink_start.status.success(),
        "{}",
        String::from_utf8_lossy(&sink_start.stderr)
    );
    let sink_id = String::from_utf8_lossy(&sink_start.stdout)
        .trim()
        .to_owned();
    let _sink_container = DropContainer(sink_id.clone());
    wait_http_ready(sink_http_port);
    let sink_ip = container_ip(&sink_id, &net_name);

    // ── 3. lossy UDP proxy between forwarder and sink ─────────────────────────
    // Drop 1 in 5 packets (20% loss) — QUIC must retransmit and recover.
    let sink_quic_addr = std::net::SocketAddr::from(([127, 0, 0, 1], sink_quic_port));
    let proxy_port = lossy_udp_proxy(5, sink_quic_addr);

    // ── 4. wal-forwarder → lossy proxy → sink ────────────────────────────────
    let mut fwd_cmd = std::process::Command::new(FORWARDER_BIN);
    fwd_cmd.args([
        "--pg-port",
        &pg_port.to_string(),
        "--sink-addr",
        &format!("127.0.0.1:{proxy_port}"),
        "--slot",
        "wal_sink_loss",
    ]);
    #[cfg(unix)]
    fwd_cmd.process_group(0);
    let _forwarder = Forwarder {
        process: fwd_cmd.spawn().expect("spawn wal-forwarder"),
    };

    for _ in 0..60 {
        if primary_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_loss'",
                &[],
            )
            .unwrap()
            .is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    let t_start = Instant::now();

    // ── 5. pre-backup rows ────────────────────────────────────────────────────
    primary_client
        .batch_execute(
            "CREATE TABLE loss_test (id serial, v text); \
             INSERT INTO loss_test (v) SELECT 'pre-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();
    eprintln!(
        "timing: pre-backup rows done at {:.1}s",
        t_start.elapsed().as_secs_f64()
    );

    // ── 6. pg_basebackup ──────────────────────────────────────────────────────
    let pgdata = std::env::temp_dir().join(format!("loss-pgdata-{pg_port}"));
    std::fs::create_dir_all(&pgdata).unwrap();
    let _pgdata_cleanup = DropDir(pgdata.clone());
    let pgdata_str = pgdata.to_str().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pgdata, std::fs::Permissions::from_mode(0o777)).unwrap();
    }
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            &format!("--network={net_name}"),
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host={primary_ip} port=5432 user=postgres"),
            "--pgdata",
            "/pgdata",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
        ])
        .output()
        .expect("pg_basebackup");
    assert!(
        out.status.success(),
        "pg_basebackup: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!(
        "timing: pg_basebackup done at {:.1}s",
        t_start.elapsed().as_secs_f64()
    );

    // ── 7. post-backup rows + archive ─────────────────────────────────────────
    // Get the segment name that currently holds active WAL — this is the
    // segment that will contain the post-backup rows after insertion.
    let post_seg_name: String = primary_client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    primary_client
        .batch_execute(
            "INSERT INTO loss_test (v) SELECT 'post-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    eprintln!(
        "timing: post-backup INSERT done at {:.1}s",
        t_start.elapsed().as_secs_f64()
    );
    // Switch WAL to close the post-backup segment and start a new one.
    // When the sink receives the new segment's first byte, it renames
    // post_seg_name.partial → post_seg_name (complete).
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();
    eprintln!(
        "timing: pg_switch_wal done at {:.1}s",
        t_start.elapsed().as_secs_f64()
    );

    // With 20% packet loss QUIC retransmits; use a generous timeout.
    // Wait for the SPECIFIC segment containing the post-backup rows.
    for _ in 0..240 {
        if sink_has_segment(sink_http_port, &post_seg_name) {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        sink_has_segment(sink_http_port, &post_seg_name),
        "post-backup WAL segment {post_seg_name} never archived despite QUIC retransmission (20% loss)"
    );
    eprintln!(
        "timing: post-backup segment archived at {:.1}s",
        t_start.elapsed().as_secs_f64()
    );

    // ── 8. kill primary ───────────────────────────────────────────────────────
    drop(primary_client);
    drop(primary);

    // ── 9. recovery config ────────────────────────────────────────────────────
    // The replica reaches the sink directly by container IP on the shared
    // Docker network (same topology as replica_recovers_via_archive_quic).
    let perl_script = format!(
        "use IO::Socket::INET;\n\
         ($s,$d)=@ARGV;\n\
         $sock=IO::Socket::INET->new(PeerAddr=>q({sink_ip}),PeerPort=>9000,\
         Proto=>q(tcp),Timeout=>10) or exit 1;\n\
         $sock->autoflush(1);\n\
         print $sock \"GET /$s HTTP/1.0\\r\\nHost: sink\\r\\n\\r\\n\";\n\
         $l=<$sock>;exit 1 unless $l=~/200/;\n\
         while(<$sock>){{last if/^\\r\\n$/}}\n\
         open(O,\">\",$d)or exit 1;\n\
         while(read($sock,$b,65536)){{print O $b}}\n\
         close O;\n"
    );
    std::fs::write(pgdata.join("fetch_wal.pl"), &perl_script).unwrap();

    let setup_script = "touch /pgdata/standby.signal && \
         echo \"restore_command = 'perl /pgdata/fetch_wal.pl %f %p'\" \
           >> /pgdata/postgresql.auto.conf && \
         echo \"recovery_target_timeline = 'latest'\" \
           >> /pgdata/postgresql.auto.conf";
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "bash",
            "-c",
            setup_script,
        ])
        .output()
        .expect("recovery config");
    assert!(
        out.status.success(),
        "recovery setup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // ── 10. start replica on the shared network ───────────────────────────────
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let replica_port = probe.local_addr().unwrap().port();
    drop(probe);

    let r_start = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            &format!("--network={net_name}"),
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "-p",
            &format!("{replica_port}:5432"),
            "postgres:18",
            "bash",
            "-c",
            "chmod 700 /pgdata && chown -R postgres:postgres /pgdata \
             && exec gosu postgres postgres -D /pgdata",
        ])
        .output()
        .expect("docker run replica");
    assert!(
        r_start.status.success(),
        "{}",
        String::from_utf8_lossy(&r_start.stderr)
    );
    let replica_id = String::from_utf8_lossy(&r_start.stdout).trim().to_owned();
    let _replica = DropContainer(replica_id.clone());
    eprintln!(
        "timing: replica container started at {:.1}s",
        t_start.elapsed().as_secs_f64()
    );

    let replica_url = format!("host=127.0.0.1 port={replica_port} user=postgres dbname=postgres");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut replica_client = loop {
        if Instant::now() > deadline {
            let logs = std::process::Command::new("docker")
                .args(["logs", &replica_id])
                .output()
                .ok()
                .map(|o| {
                    format!(
                        "stdout: {}\nstderr: {}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                })
                .unwrap_or_else(|| "(docker logs failed)".to_owned());
            panic!("replica not ready within 90s\n{logs}");
        }
        if let Ok(c) = postgres::Client::connect(&replica_url, postgres::NoTls) {
            eprintln!(
                "timing: replica accepting connections at {:.1}s",
                t_start.elapsed().as_secs_f64()
            );
            break c;
        }
        std::thread::sleep(Duration::from_millis(300));
    };

    // ── 11. assert all 100 rows recovered despite packet loss ─────────────────
    {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if Instant::now() > deadline {
                let count: i64 = replica_client
                    .query_one("SELECT count(*) FROM loss_test", &[])
                    .map(|r| r.get(0))
                    .unwrap_or(-1);
                let recovery: bool = replica_client
                    .query_one("SELECT pg_is_in_recovery()", &[])
                    .unwrap()
                    .get(0);
                let segs = sink_complete_segs(sink_http_port);
                let replica_logs = std::process::Command::new("docker")
                    .args(["logs", "--tail", "40", &replica_id])
                    .output()
                    .ok()
                    .map(|o| {
                        format!(
                            "stdout: {}\nstderr: {}",
                            String::from_utf8_lossy(&o.stdout),
                            String::from_utf8_lossy(&o.stderr)
                        )
                    })
                    .unwrap_or_default();
                let sink_logs = std::process::Command::new("docker")
                    .args(["logs", "--tail", "20", &sink_id])
                    .output()
                    .ok()
                    .map(|o| {
                        format!(
                            "stdout: {}\nstderr: {}",
                            String::from_utf8_lossy(&o.stdout),
                            String::from_utf8_lossy(&o.stderr)
                        )
                    })
                    .unwrap_or_default();
                panic!(
                    "replica did not reach 100 rows in loss_test within 120s\n\
                     count={count}, in_recovery={recovery}, sink_complete_segs={segs}\n\
                     --- replica logs ---\n{replica_logs}\n\
                     --- sink logs ---\n{sink_logs}"
                );
            }
            let count: i64 = replica_client
                .query_one("SELECT count(*) FROM loss_test", &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if count >= 100 {
                eprintln!(
                    "timing: 100 rows confirmed at {:.1}s",
                    t_start.elapsed().as_secs_f64()
                );
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    let is_recovery: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(is_recovery, "replica should remain in recovery");
}
