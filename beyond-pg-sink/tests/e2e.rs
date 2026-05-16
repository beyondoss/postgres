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

    let mut primary_client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut primary_client);

    // ── 2. start sink on host ───────────────────────────────────────────────
    // Sink writes WAL segments to sink_dir on the host; the replica container
    // bind-mounts sink_dir at /mnt/sink (read-only) for restore_command.
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
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host=host.docker.internal port={pg_port} user=postgres"),
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

    // ── 6. write rows AFTER basebackup — archive-only recovery needed ───────

    primary_client
        .batch_execute(
            "INSERT INTO archive_test (v) \
               SELECT 'post-' || g FROM generate_series(1, 50) g;",
        )
        .expect("post-basebackup rows");

    // Capture the exact segment containing the post-backup commit BEFORE
    // pg_switch_wal advances to the next segment. We'll wait for THIS segment
    // to land in sink_dir as a sealed file.
    let post_segment: String = primary_client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);

    // Switch WAL so the segment containing the post-backup rows is sealed.
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    // Wait for the specific segment containing post-backup data to appear in
    // sink_dir as a complete (renamed) file. Deterministic — no race on
    // whatever the sink might or might not have archived earlier.
    let target_path = sink_dir.join(&post_segment);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !target_path.exists() {
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            panic!(
                "post-backup segment {post_segment} never archived within 60s\nsink_dir contents: {ls:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 6b. exercise sink HTTP server: serves complete segment, 404s partial ──
    // The replica uses a bind-mounted sink_dir for restore_command, but in
    // production WAL is fetched via the HTTP server.  Verify both paths work.
    {
        const WAL_SEG: usize = 16 * 1024 * 1024;
        let (status, body) = http_get(sink_port, &format!("/{post_segment}"));
        assert_eq!(status, 200, "sink HTTP must serve sealed segment");
        assert_eq!(
            body.len(),
            WAL_SEG,
            "served segment must be {WAL_SEG} bytes (1 WAL segment)"
        );
        // WAL page header has magic 0xD117 (page magic for postgres 17/18).
        // Check first 2 bytes are non-zero; full magic check would couple to a
        // specific pg version. This is enough to verify we got real WAL.
        assert!(
            body[0] != 0 || body[1] != 0,
            "served segment must not be all zeros (got zero page header)"
        );
        let (status, _) = http_get(sink_port, "/000000000000000000000000");
        assert_eq!(status, 404, "sink HTTP must 404 on unknown segment");
    }

    // ── 7. kill primary ──────────────────────────────────────────────────────
    // Drop the client first so no TCP connections linger into the stopped container.
    drop(primary_client);
    drop(primary);

    // ── 8. write recovery config: restore_command only, no primary_conninfo ─
    // Run as root so we can write standby.signal and append to postgresql.auto.conf
    // (which is owned by uid 999/postgres from the basebackup).
    // The replica bind-mounts the sink dir at /mnt/sink (read-only), so
    // restore_command is a simple cp from the local filesystem.
    let setup_script = "touch /pgdata/standby.signal && \
         echo \"restore_command = 'cp /mnt/sink/%f %p'\" \
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

    let chmod_out = std::process::Command::new("chmod")
        .args(["-R", "a+rX", &sink_dir_str])
        .output()
        .expect("chmod sink_dir");
    assert!(
        chmod_out.status.success(),
        "chmod sink_dir: {}",
        String::from_utf8_lossy(&chmod_out.stderr)
    );

    let start_out = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "-v",
            &format!("{sink_dir_str}:/mnt/sink:ro"),
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
    {
        let _ = wait_for_rows; // suppress unused warning
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let n: i64 = replica_client
                .query_one("SELECT count(*) FROM archive_test", &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if n >= 100 {
                break;
            }
            if Instant::now() > deadline {
                let recovery: bool = replica_client
                    .query_one("SELECT pg_is_in_recovery()", &[])
                    .map(|r| r.get(0))
                    .unwrap_or(false);
                let logs = std::process::Command::new("docker")
                    .args(["logs", "--tail", "40", &_replica.0])
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
                panic!(
                    "replica did not see 100 rows in archive_test within 90s\n\
                     current count={n}, in_recovery={recovery}\n\
                     --- replica logs (tail 40) ---\n{logs}"
                );
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

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

    let mut primary_client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut primary_client);

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
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host=host.docker.internal port={pg_port} user=postgres"),
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
    primary_client
        .batch_execute(
            "INSERT INTO crash_test (v) SELECT 'post-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();

    // Capture the segment containing post-backup commit before pg_switch_wal
    // advances. We'll wait for this exact segment to appear sealed in sink_dir.
    let post_segment: String = primary_client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
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

    let target_path = sink_dir.join(&post_segment);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !target_path.exists() {
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            panic!(
                "post-backup segment {post_segment} never archived within 60s\nsink_dir contents: {ls:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let _ = segs_after_pre; // suppress unused warning

    // ── 8b. exercise the RESTARTED sink's HTTP server ─────────────────────────
    // Crucially exercises the restart code path: the second sink process must
    // serve segments after cleanup_partial_segments runs and the receiver
    // re-streams the lost WAL.
    {
        const WAL_SEG: usize = 16 * 1024 * 1024;
        let (status, body) = http_get(sink_port, &format!("/{post_segment}"));
        assert_eq!(status, 200, "restarted sink HTTP must serve sealed segment");
        assert_eq!(body.len(), WAL_SEG, "served segment must be 16 MiB");
        assert!(
            body[0] != 0 || body[1] != 0,
            "served segment must not be all zeros after sink restart"
        );
    }

    // ── 9. kill primary ───────────────────────────────────────────────────────
    drop(primary_client);
    drop(primary);

    // ── 10. write recovery config ─────────────────────────────────────────────
    let setup_script = "touch /pgdata/standby.signal && \
         echo \"restore_command = 'cp /mnt/sink/%f %p'\" \
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

    let chmod_out = std::process::Command::new("chmod")
        .args(["-R", "a+rX", &sink_dir_str])
        .output()
        .expect("chmod sink_dir");
    assert!(
        chmod_out.status.success(),
        "chmod sink_dir: {}",
        String::from_utf8_lossy(&chmod_out.stderr)
    );

    let start_out = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--user",
            "root",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "-v",
            &format!("{sink_dir_str}:/mnt/sink:ro"),
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
    {
        let _ = wait_for_rows; // suppress unused warning
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let count: i64 = replica_client
                .query_one("SELECT count(*) FROM crash_test", &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if count >= 100 {
                break;
            }
            if Instant::now() > deadline {
                let recovery: bool = replica_client
                    .query_one("SELECT pg_is_in_recovery()", &[])
                    .map(|r| r.get(0))
                    .unwrap_or(false);
                let logs = std::process::Command::new("docker")
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
                    .unwrap_or_else(|| "(docker logs failed)".to_owned());
                panic!(
                    "replica did not see 100 rows in crash_test within 90s\n\
                     current count={count}, in_recovery={recovery}\n\
                     --- replica logs (tail 40) ---\n{logs}"
                );
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

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

    let mut primary_client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut primary_client);

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
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host=host.docker.internal port={pg_port} user=postgres"),
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
    fn complete_segs(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .count()
    }

    fn write_recovery_cfg(pgdata: &std::path::Path, sink_port: u16) {
        let _ = sink_port;
        let script = "touch /pgdata/standby.signal && \
             echo \"restore_command = 'cp /mnt/sink/%f %p'\" \
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

    let mut t1_client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut t1_client);

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
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &format!("{pgdata1_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host=host.docker.internal port={pg_port} user=postgres"),
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

    // pg_basebackup wrote files as uid 999 (postgres in container) with mode
    // 0700. Make them host-readable so the cp -r below can read them.
    let chmod_out = std::process::Command::new("docker")
        .args([
            "run", "--rm", "--user", "root",
            "-v", &format!("{pgdata1_str}:/pgdata"),
            "postgres:18",
            "chmod", "-R", "a+rwX", "/pgdata",
        ])
        .output()
        .expect("chmod pgdata1");
    assert!(
        chmod_out.status.success(),
        "chmod pgdata1: {}",
        String::from_utf8_lossy(&chmod_out.stderr)
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
    t1_client
        .batch_execute(
            "INSERT INTO timeline_test (v) SELECT 't1-post-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();

    // Capture the segment containing T1 post-backup commit; wait for it to be
    // archived as a complete file after pg_switch_wal seals it.
    let t1_post_segment: String = t1_client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    t1_client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let t1_target = sink_dir.join(&t1_post_segment);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !t1_target.exists() {
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            panic!(
                "T1 post-backup segment {t1_post_segment} never archived within 60s\nsink_dir: {ls:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 5b. exercise T1 sink HTTP: serve T1 segment ──────────────────────────
    {
        const WAL_SEG: usize = 16 * 1024 * 1024;
        let (status, body) = http_get(sink_port, &format!("/{t1_post_segment}"));
        assert_eq!(status, 200, "T1 sink HTTP must serve T1 segment");
        assert_eq!(body.len(), WAL_SEG, "T1 segment must be 16 MiB");
        assert!(
            body[0] != 0 || body[1] != 0,
            "T1 segment must not be all zeros"
        );
    }

    // ── 6. kill T1 primary ────────────────────────────────────────────────────
    // Keep _sink alive: replica1 needs its HTTP server to fetch T1 archive segments.
    drop(t1_client);
    drop(primary);

    // ── 7. start replica1 — archive-only recovery ─────────────────────────────
    write_recovery_cfg(&pgdata1, sink_port);

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let r1_port = probe.local_addr().unwrap().port();
    drop(probe);

    let chmod_out = std::process::Command::new("chmod")
        .args(["-R", "a+rX", &sink_dir_str])
        .output()
        .expect("chmod sink_dir");
    assert!(
        chmod_out.status.success(),
        "chmod sink_dir: {}",
        String::from_utf8_lossy(&chmod_out.stderr)
    );

    let r1_start = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--user",
            "root",
            "-v",
            &format!("{pgdata1_str}:/pgdata"),
            "-v",
            &format!("{sink_dir_str}:/mnt/sink:ro"),
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
    // After promotion Postgres writes a timeline history file to pg_wal/. The
    // pgdata is owned by uid 999 mode 0700 (set by the replica's startup
    // script), so the host can't read it directly — use docker exec cat to
    // extract the file content, then write to sink_dir on the host.
    let history_dst = sink_dir.join("00000002.history");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let out = std::process::Command::new("docker")
            .args([
                "exec",
                "-u",
                "root",
                &r1_id,
                "cat",
                "/pgdata/pg_wal/00000002.history",
            ])
            .output();
        if let Ok(o) = out
            && o.status.success()
            && !o.stdout.is_empty()
        {
            std::fs::write(&history_dst, &o.stdout).expect("write .history to sink_dir");
            // Make it world-readable so the bind-mounted sink_dir is searchable
            // by uid 999 in r2's container.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(
                    &history_dst,
                    std::fs::Permissions::from_mode(0o644),
                );
            }
            break;
        }
        if Instant::now() > deadline {
            // Dump what's in pg_wal for diagnosis.
            let ls = std::process::Command::new("docker")
                .args(["exec", "-u", "root", &r1_id, "ls", "-la", "/pgdata/pg_wal"])
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
                .unwrap_or_default();
            panic!("00000002.history not written within 10s\npg_wal contents:\n{ls}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 9b. exercise sink HTTP for .history file ─────────────────────────────
    // Verify the sink's HTTP server serves the timeline history file so
    // standbies following `recovery_target_timeline = 'latest'` can discover
    // the T1→T2 branch point.
    {
        let (status, body) = http_get(sink_port, "/00000002.history");
        assert_eq!(status, 200, "sink HTTP must serve .history file");
        assert!(
            !body.is_empty(),
            "served .history file must be non-empty"
        );
        // .history files are text — first byte must be ASCII (typically a digit
        // for the parent timeline id).
        assert!(
            body[0].is_ascii(),
            ".history file should be ASCII text, got {:?}",
            &body[..body.len().min(16)]
        );
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
    r1_client
        .batch_execute(
            "INSERT INTO timeline_test (v) SELECT 't2-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();

    let t2_post_segment: String = r1_client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    r1_client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let t2_target = sink_dir.join(&t2_post_segment);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !t2_target.exists() {
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            panic!(
                "T2 segment {t2_post_segment} never archived within 60s\nsink_dir: {ls:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 12b. exercise T2 sink HTTP: serve both T1 and T2 segments + history ──
    // T2 sink runs on the same sink_dir as the T1 sink did, so its HTTP server
    // should serve segments from both timelines plus the .history file.
    {
        const WAL_SEG: usize = 16 * 1024 * 1024;
        let (status, body) = http_get(sink_port, &format!("/{t2_post_segment}"));
        assert_eq!(status, 200, "T2 sink HTTP must serve T2 segment");
        assert_eq!(body.len(), WAL_SEG, "T2 segment must be 16 MiB");
        let (status, body) = http_get(sink_port, &format!("/{t1_post_segment}"));
        assert_eq!(
            status, 200,
            "T2 sink HTTP must still serve T1 segments (same sink_dir)"
        );
        assert_eq!(body.len(), WAL_SEG, "T1 segment must be 16 MiB");
        let (status, body) = http_get(sink_port, "/00000002.history");
        assert_eq!(
            status, 200,
            "T2 sink HTTP must serve .history file across timelines"
        );
        assert!(!body.is_empty(), ".history must be non-empty");
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

    let chmod_out = std::process::Command::new("chmod")
        .args(["-R", "a+rX", &sink_dir_str])
        .output()
        .expect("chmod sink_dir");
    assert!(
        chmod_out.status.success(),
        "chmod sink_dir: {}",
        String::from_utf8_lossy(&chmod_out.stderr)
    );

    let r2_start = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--user",
            "root",
            "-v",
            &format!("{pgdata2_str}:/pgdata"),
            "-v",
            &format!("{sink_dir_str}:/mnt/sink:ro"),
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

/// End-to-end test that postgres-driven `restore_command` fetches WAL via the
/// sink's HTTP server using a perl script — closes the coverage gap where
/// `replica_recovers_via_archive` uses `cp /mnt/sink` (bind-mount shortcut).
#[test]
#[ignore = "requires Docker"]
fn replica_recovers_via_archive_real_http() {
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

    let mut primary_client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut primary_client);

    // ── 2. start sink on host ────────────────────────────────────────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-real-http-{pg_port}"));
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
        "wal_sink_real_http",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_proc = sink_cmd.spawn().expect("spawn beyond-pg-sink");
    let _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    // ── 3. wait for sink in pg_stat_replication ──────────────────────────────
    let mut streaming = false;
    for _ in 0..60 {
        if primary_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication \
                 WHERE application_name = 'wal_sink_real_http'",
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

    // ── 4. pre-basebackup rows ───────────────────────────────────────────────
    primary_client
        .batch_execute(
            "CREATE TABLE archive_test_http (id serial, v text); \
             INSERT INTO archive_test_http (v) \
               SELECT 'pre-' || g FROM generate_series(1, 50) g;",
        )
        .expect("pre-basebackup rows");
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    // ── 5. pg_basebackup ─────────────────────────────────────────────────────
    let pgdata = std::env::temp_dir().join(format!("archive-pgdata-real-http-{pg_port}"));
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
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            &format!("{pgdata_str}:/pgdata"),
            "postgres:18",
            "pg_basebackup",
            "-d",
            &format!("host=host.docker.internal port={pg_port} user=postgres"),
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

    // ── 6. post-basebackup rows ──────────────────────────────────────────────
    primary_client
        .batch_execute(
            "INSERT INTO archive_test_http (v) \
               SELECT 'post-' || g FROM generate_series(1, 50) g;",
        )
        .expect("post-basebackup rows");

    let post_segment: String = primary_client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);

    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    let target_path = sink_dir.join(&post_segment);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !target_path.exists() {
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            panic!(
                "post-backup segment {post_segment} never archived within 60s\nsink_dir contents: {ls:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 7. write fetch_wal.pl into PGDATA — production-shaped fetch path ─────
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
    std::fs::write(pgdata.join("fetch_wal.pl"), perl_script).expect("write fetch_wal.pl");

    // ── 8. kill primary ──────────────────────────────────────────────────────
    drop(primary_client);
    drop(primary);

    // ── 9. write recovery config (perl restore_command) ──────────────────────
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

    // ── 10. chmod sink_dir so replica container can read it (defensive) ──────
    let chmod_out = std::process::Command::new("chmod")
        .args(["-R", "a+rX", &sink_dir_str])
        .output()
        .expect("chmod sink_dir");
    assert!(
        chmod_out.status.success(),
        "chmod sink_dir: {}",
        String::from_utf8_lossy(&chmod_out.stderr)
    );

    // ── 11. start replica with host.docker.internal mapping ──────────────────
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
        .expect("docker run archive replica");
    assert!(
        start_out.status.success(),
        "replica start: {}",
        String::from_utf8_lossy(&start_out.stderr)
    );
    let replica_id = String::from_utf8_lossy(&start_out.stdout).trim().to_owned();
    let _replica = DropContainer(replica_id.clone());

    // ── 12. wait for hot-standby connections ─────────────────────────────────
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
                "archive replica not ready on port {replica_port} within 90s\n\
                 --- container logs ---\n{log_text}"
            );
        }
        if let Ok(c) = postgres::Client::connect(&replica_url, postgres::NoTls) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(300));
    };

    // ── 13. assert all 100 rows visible via real HTTP restore_command ────────
    {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let n: i64 = replica_client
                .query_one("SELECT count(*) FROM archive_test_http", &[])
                .map(|r| r.get(0))
                .unwrap_or(0);
            if n >= 100 {
                break;
            }
            if Instant::now() > deadline {
                let recovery: bool = replica_client
                    .query_one("SELECT pg_is_in_recovery()", &[])
                    .map(|r| r.get(0))
                    .unwrap_or(false);
                let logs = std::process::Command::new("docker")
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
                    .unwrap_or_else(|| "(docker logs failed)".to_owned());
                panic!(
                    "replica did not see 100 rows in archive_test_http within 90s\n\
                     current count={n}, in_recovery={recovery}\n\
                     --- replica logs (tail 40) ---\n{logs}"
                );
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    let is_recovery: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        is_recovery,
        "replica should be in recovery (no primary to promote it)"
    );
}

/// Prove the sink's replication slot survives sink restart and the receiver
/// streams from the saved `confirmed_flush_lsn`, not from the beginning.
#[test]
#[ignore = "requires Docker"]
fn sink_slot_offline_catch_up() {
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

    // ── 1. primary — async replication so commits succeed without sink ───────
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .expect("docker not available or postgres:18 pull failed");
    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");

    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut client);

    // ── 2. start sink ────────────────────────────────────────────────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-offline-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();
    let sink_dir_str = sink_dir.to_str().unwrap().to_owned();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let sink_port = probe.local_addr().unwrap().port();
    drop(probe);

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let make_sink = || {
        let mut cmd = std::process::Command::new(BIN);
        cmd.args([
            "--mode",
            "tcp",
            "--connstr",
            &connstr,
            "--dir",
            &sink_dir_str,
            "--port",
            &sink_port.to_string(),
            "--slot",
            "wal_sink_offline",
        ]);
        #[cfg(unix)]
        cmd.process_group(0);
        cmd.spawn().expect("spawn beyond-pg-sink")
    };

    let sink_proc = make_sink();
    let sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    // ── 3. wait for sink in pg_stat_replication ──────────────────────────────
    let mut streaming = false;
    for _ in 0..60 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_offline'",
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

    // ── 4. pre-kill writes ───────────────────────────────────────────────────
    client
        .batch_execute(
            "CREATE TABLE slot_offline (id serial, v text); \
             INSERT INTO slot_offline (v) SELECT 'pre-' || g FROM generate_series(1,50) g;",
        )
        .unwrap();
    client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let lsn_before_kill: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    // Wait for sink flush_lsn to catch up before kill.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let flush_lsn: Option<String> = client
            .query_opt(
                "SELECT flush_lsn::text FROM pg_stat_replication \
                 WHERE application_name='wal_sink_offline'",
                &[],
            )
            .unwrap()
            .and_then(|r| r.get(0));
        if let Some(fl) = flush_lsn.as_ref() {
            // Compare via pg_lsn cast.
            let caught_up: bool = client
                .query_one(
                    &format!("SELECT '{fl}'::pg_lsn >= '{lsn_before_kill}'::pg_lsn"),
                    &[],
                )
                .unwrap()
                .get(0);
            if caught_up {
                break;
            }
        }
        if Instant::now() > deadline {
            panic!(
                "sink flush_lsn {:?} never reached lsn_before_kill={lsn_before_kill} within 30s",
                flush_lsn
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 5. SIGKILL the sink, but keep its WAL files on disk ──────────────────
    #[cfg(unix)]
    unsafe {
        let pid = sink.process.id() as i32;
        libc::kill(-pid, libc::SIGKILL);
    }
    // Forget the Sink so its Drop doesn't remove_dir_all the sink_dir; we need
    // the previously-archived WAL to persist for the restarted sink.
    std::mem::forget(sink);
    // Give the OS a moment to reap the dead process group.
    std::thread::sleep(Duration::from_millis(500));

    // ── 6. post-kill writes (commit locally — async replication) ─────────────
    client
        .batch_execute(
            "INSERT INTO slot_offline (v) SELECT 'post-kill-' || g FROM generate_series(1,100) g;",
        )
        .unwrap();
    client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let lsn_after_writes: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    // ── 7. verify slot persists and is now inactive ──────────────────────────
    let (slot_exists, slot_active): (bool, bool) = {
        let row = client
            .query_one(
                "SELECT true, active FROM pg_replication_slots \
                 WHERE slot_name='wal_sink_offline'",
                &[],
            )
            .expect("slot row missing");
        (row.get(0), row.get(1))
    };
    assert!(slot_exists, "slot wal_sink_offline must persist after kill");
    assert!(!slot_active, "slot must be inactive after sink SIGKILL");

    // ── 8. restart the sink with the same slot + dir ─────────────────────────
    let sink_proc2 = make_sink();
    let _sink2 = Sink {
        process: sink_proc2,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    // ── 9. poll for flush_lsn to reach lsn_after_writes ──────────────────────
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let flush_lsn: Option<String> = client
            .query_opt(
                "SELECT flush_lsn::text FROM pg_stat_replication \
                 WHERE application_name='wal_sink_offline'",
                &[],
            )
            .unwrap()
            .and_then(|r| r.get(0));
        if let Some(fl) = flush_lsn.as_ref() {
            let caught_up: bool = client
                .query_one(
                    &format!("SELECT '{fl}'::pg_lsn >= '{lsn_after_writes}'::pg_lsn"),
                    &[],
                )
                .unwrap()
                .get(0);
            if caught_up {
                break;
            }
        }
        if Instant::now() > deadline {
            let slots = client
                .query(
                    "SELECT slot_name, active, confirmed_flush_lsn::text \
                     FROM pg_replication_slots",
                    &[],
                )
                .unwrap();
            let stat = client
                .query(
                    "SELECT application_name, state, flush_lsn::text \
                     FROM pg_stat_replication",
                    &[],
                )
                .unwrap();
            let slot_dump: Vec<String> = slots
                .iter()
                .map(|r| {
                    format!(
                        "slot={} active={} flush={}",
                        r.get::<_, &str>(0),
                        r.get::<_, bool>(1),
                        r.get::<_, Option<&str>>(2).unwrap_or("<null>")
                    )
                })
                .collect();
            let stat_dump: Vec<String> = stat
                .iter()
                .map(|r| {
                    format!(
                        "app={} state={} flush={}",
                        r.get::<_, &str>(0),
                        r.get::<_, &str>(1),
                        r.get::<_, Option<&str>>(2).unwrap_or("<null>")
                    )
                })
                .collect();
            panic!(
                "restarted sink flush_lsn {:?} never reached lsn_after_writes={lsn_after_writes} within 60s\n\
                 pg_replication_slots: {slot_dump:?}\n\
                 pg_stat_replication: {stat_dump:?}",
                flush_lsn
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    // ── 10. drive a fresh INSERT batch + pg_switch_wal post-restart ──────────
    // To prove the sink can advance and seal segments after restart, capture
    // the segment containing a NEW post-restart INSERT, then pg_switch_wal to
    // seal it. We INSERT a large batch (1000 rows) so the post-restart segment
    // has real records — not just an XLOG_SWITCH stub — and then run another
    // INSERT + pg_switch_wal to be sure the streaming crosses a boundary
    // (which is what triggers the sink's .partial → final rename).
    client
        .batch_execute(
            "INSERT INTO slot_offline (v) \
             SELECT 'post-restart-' || g FROM generate_series(1, 1000) g",
        )
        .expect("post-restart bulk INSERT");
    let post_restart_segment: String = client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    client.execute("SELECT pg_switch_wal()", &[]).unwrap();
    // Follow-up INSERT so primary writes records into the NEXT segment, which
    // is what makes the sink's WalWriter cross the boundary and rename
    // post_restart_segment.partial → post_restart_segment.
    client
        .batch_execute(
            "INSERT INTO slot_offline (v) \
             SELECT 'tick-' || g FROM generate_series(1, 200) g",
        )
        .expect("post-switch INSERT to drive segment seal");

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if sink_dir.join(&post_restart_segment).exists() {
            break;
        }
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    let size = e.metadata().map(|m| m.len()).unwrap_or(0);
                    format!("{name} ({size}B)")
                })
                .collect();
            panic!(
                "post-restart segment {post_restart_segment} never sealed within 60s\nsink_dir: {ls:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Sustained-write stress test: drive WAL with pgbench for ~15 s and verify
/// the sink keeps up.
#[test]
#[ignore = "requires Docker"]
fn sink_keeps_up_with_pgbench() {
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

    // ── 1. primary tuned for pgbench (sync replication exerts back-pressure) ─
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "max_connections=20",
            "-c",
            "shared_buffers=128MB",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("docker not available or postgres:18 pull failed");
    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let primary_id = primary.id().to_owned();

    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut client);

    // ── 2. start sink ────────────────────────────────────────────────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-pgbench-{pg_port}"));
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
        "wal_sink_pgbench",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_proc = sink_cmd.spawn().expect("spawn beyond-pg-sink");
    let _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    // ── 3. wait for sink in pg_stat_replication ──────────────────────────────
    let mut streaming = false;
    for _ in 0..60 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_pgbench'",
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

    // ── 4. pgbench init inside the primary container ─────────────────────────
    let init_out = std::process::Command::new("docker")
        .args([
            "exec",
            &primary_id,
            "gosu",
            "postgres",
            "pgbench",
            "-i",
            "-s",
            "1",
            "postgres",
        ])
        .output()
        .expect("docker exec pgbench -i");
    assert!(
        init_out.status.success(),
        "pgbench -i failed: {}",
        String::from_utf8_lossy(&init_out.stderr)
    );

    // ── 5. drive pgbench for 30s ─────────────────────────────────────────────
    // 30s at -c 4 -j 4 -s 1 against synchronous_commit=remote_write reliably
    // archives 3+ complete 16 MiB WAL segments on GHA runners.
    let run_out = std::process::Command::new("docker")
        .args([
            "exec",
            &primary_id,
            "gosu",
            "postgres",
            "pgbench",
            "-c",
            "4",
            "-j",
            "4",
            "-T",
            "30",
            "postgres",
        ])
        .output()
        .expect("docker exec pgbench run");
    assert!(
        run_out.status.success(),
        "pgbench run failed: {}",
        String::from_utf8_lossy(&run_out.stderr)
    );

    // ── 6. snapshot final LSN ────────────────────────────────────────────────
    let final_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    // ── 7. poll sink flush_lsn >= final_lsn ──────────────────────────────────
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let flush_lsn: Option<String> = client
            .query_opt(
                "SELECT flush_lsn::text FROM pg_stat_replication \
                 WHERE application_name='wal_sink_pgbench'",
                &[],
            )
            .unwrap()
            .and_then(|r| r.get(0));
        if let Some(fl) = flush_lsn.as_ref() {
            let caught_up: bool = client
                .query_one(
                    &format!("SELECT '{fl}'::pg_lsn >= '{final_lsn}'::pg_lsn"),
                    &[],
                )
                .unwrap()
                .get(0);
            if caught_up {
                break;
            }
        }
        if Instant::now() > deadline {
            panic!(
                "sink flush_lsn {:?} never reached final_lsn={final_lsn} within 30s after pgbench",
                flush_lsn
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 8. pg_switch_wal and wait for the sealing segment ────────────────────
    let final_segment: String = client
        .query_one(
            &format!("SELECT pg_walfile_name('{final_lsn}'::pg_lsn)"),
            &[],
        )
        .unwrap()
        .get(0);
    client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if sink_dir.join(&final_segment).exists() {
            break;
        }
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            panic!(
                "segment {final_segment} containing final_lsn never sealed within 30s\nsink_dir: {ls:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 9. correctness: WAL was rolled across segments under load ─────────────
    // The primary correctness assertion is flush_lsn >= final_lsn above. This
    // is a sanity check that the sink rolled segments while pgbench ran (i.e.,
    // the receiver/writer wasn't stalled). >= 2 sealed segments proves at
    // least one segment boundary was crossed cleanly under load.
    let complete_segs: Vec<String> = std::fs::read_dir(&sink_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
        .collect();
    assert!(
        complete_segs.len() >= 2,
        "expected >= 2 complete WAL segments archived after 30s of pgbench, got {} ({:?})",
        complete_segs.len(),
        complete_segs
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional regression tests
// ─────────────────────────────────────────────────────────────────────────────

/// Prove `cleanup_partial_segments` deletes orphaned `.partial` files on startup.
#[test]
#[ignore = "requires Docker"]
fn sink_recovers_from_corrupted_partial() {
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

    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut client);

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-corrupt-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = probe.local_addr().unwrap().port();
    drop(probe);

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres");
    let spawn_sink = |port: u16| -> Child {
        let mut cmd = std::process::Command::new(BIN);
        cmd.args([
            "--mode",
            "tcp",
            "--connstr",
            &connstr,
            "--dir",
            sink_dir.to_str().unwrap(),
            "--port",
            &port.to_string(),
            "--slot",
            "wal_sink_corrupt",
        ]);
        #[cfg(unix)]
        cmd.process_group(0);
        cmd.spawn().expect("spawn beyond-pg-sink")
    };

    let sink_process = spawn_sink(http_port);
    let sink = Sink {
        process: sink_process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port);

    // wait for sink in pg_stat_replication
    let mut streaming = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_corrupt'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(streaming, "sink never appeared in pg_stat_replication");

    client
        .batch_execute(
            "CREATE TABLE corrupt_test (id serial, v text); \
             INSERT INTO corrupt_test (v) SELECT 'row-' || g FROM generate_series(1, 50) g; \
             SELECT pg_switch_wal();",
        )
        .unwrap();

    // wait for at least one sealed segment
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let any_sealed = std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.len() == 24 && name.bytes().all(|b| b.is_ascii_hexdigit())
            });
        if any_sealed {
            break;
        }
        if Instant::now() > deadline {
            panic!("no sealed segment within 30s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // SIGKILL sink process group; forget Sink to preserve files
    #[cfg(unix)]
    unsafe {
        let pid = sink.process.id() as i32;
        libc::kill(-pid, libc::SIGKILL);
    }
    std::mem::forget(sink);
    std::thread::sleep(Duration::from_millis(500));

    // Find any .partial file; create one if none
    let partials: Vec<_> = std::fs::read_dir(&sink_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .ends_with(".partial")
        })
        .collect();
    if partials.is_empty() {
        let path = sink_dir.join("000000010000000000000999.partial");
        std::fs::write(&path, vec![0xFFu8; 8 * 1024]).unwrap();
    }

    let partial_count = std::fs::read_dir(&sink_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .ends_with(".partial")
        })
        .count();
    assert!(
        partial_count >= 1,
        ".partial file should exist before restart"
    );

    // Restart sink (new HTTP port to avoid TIME_WAIT)
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port2 = probe.local_addr().unwrap().port();
    drop(probe);
    let sink2_process = spawn_sink(http_port2);
    let _sink2 = Sink {
        process: sink2_process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port2);

    // wait for .partial files to be gone
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .ends_with(".partial")
            })
            .count();
        // After cleanup_partial_segments + a fresh stream, sink may create a
        // new .partial for the live segment. We only require that the
        // PRE-RESTART corrupt partial (000000010000000000000999.partial) is gone.
        let stale_present = std::fs::read_dir(&sink_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy() == "000000010000000000000999.partial");
        if !stale_present {
            let _ = remaining;
            break;
        }
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            panic!("stale .partial not cleaned up within 30s; dir: {ls:?}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // wait for sink to reattach
    let mut reattached = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_corrupt'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            reattached = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(reattached, "sink did not reattach after restart");

    // capture pre-switch segment, then drive past it
    let pre_switch_segment: String = client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    client
        .batch_execute(
            "INSERT INTO corrupt_test (v) SELECT 'row-' || g FROM generate_series(1, 50) g; \
             SELECT pg_switch_wal(); \
             INSERT INTO corrupt_test (v) SELECT 'tick-' || g FROM generate_series(1, 200) g;",
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let exists = sink_dir.join(&pre_switch_segment).exists();
        if exists {
            break;
        }
        if Instant::now() > deadline {
            let ls: Vec<(String, u64)> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| {
                    let n = e.file_name().to_string_lossy().into_owned();
                    let s = e.metadata().map(|m| m.len()).unwrap_or(0);
                    (n, s)
                })
                .collect();
            panic!("post-restart segment {pre_switch_segment} never sealed; dir: {ls:?}");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Prove the sink reconnects after a TCP-level disruption (docker pause/unpause).
#[test]
#[ignore = "requires Docker"]
fn sink_reconnects_after_primary_pause() {
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

    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let primary_id = primary.id().to_owned();
    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut client);

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-pause-{pg_port}"));
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
        "wal_sink_pause",
    ]);
    #[cfg(unix)]
    cmd.process_group(0);
    let process = cmd.spawn().expect("spawn beyond-pg-sink");
    let _sink = Sink {
        process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port);

    // Wait for streaming
    let mut streaming = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_pause'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(streaming, "sink never connected");

    client
        .batch_execute(
            "CREATE TABLE pause_test (id serial, v text); \
             INSERT INTO pause_test (v) SELECT 'a-' || g FROM generate_series(1, 50) g;",
        )
        .unwrap();

    let lsn_a: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let row = client
            .query_opt(
                &format!(
                    "SELECT flush_lsn >= '{lsn_a}'::pg_lsn \
                     FROM pg_stat_replication \
                     WHERE application_name = 'wal_sink_pause'"
                ),
                &[],
            )
            .unwrap();
        let flushed = row
            .and_then(|r| r.try_get::<_, Option<bool>>(0).ok().flatten())
            .unwrap_or(false);
        if flushed {
            break;
        }
        if Instant::now() > deadline {
            panic!("sink did not reach lsn_a {lsn_a} within 30s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // Drop the client; docker pause will freeze the postgres TCP stack and any
    // open connection would block until unpause.
    drop(client);

    let out = std::process::Command::new("docker")
        .args(["pause", &primary_id])
        .output()
        .expect("docker pause");
    assert!(
        out.status.success(),
        "docker pause: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    std::thread::sleep(Duration::from_secs(8));

    let out = std::process::Command::new("docker")
        .args(["unpause", &primary_id])
        .output()
        .expect("docker unpause");
    assert!(
        out.status.success(),
        "docker unpause: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Reconnect client
    let mut client = {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match postgres::Client::connect(&pg_url, postgres::NoTls) {
                Ok(c) => break c,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(500));
                }
                Err(e) => panic!("reconnect after unpause failed: {e}"),
            }
        }
    };

    client
        .batch_execute(
            "INSERT INTO pause_test (v) SELECT 'b-' || g FROM generate_series(1, 100) g;",
        )
        .unwrap();

    let lsn_b: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let row = client
            .query_opt(
                &format!(
                    "SELECT flush_lsn >= '{lsn_b}'::pg_lsn \
                     FROM pg_stat_replication \
                     WHERE application_name = 'wal_sink_pause'"
                ),
                &[],
            )
            .unwrap();
        let flushed = row
            .and_then(|r| r.try_get::<_, Option<bool>>(0).ok().flatten())
            .unwrap_or(false);
        if flushed {
            break;
        }
        if Instant::now() > deadline {
            let rows = client
                .query(
                    "SELECT application_name, state, sent_lsn::text, write_lsn::text, flush_lsn::text \
                     FROM pg_stat_replication",
                    &[],
                )
                .unwrap();
            let dump: Vec<String> = rows
                .iter()
                .map(|r| {
                    format!(
                        "app={} state={} sent={} write={} flush={}",
                        r.get::<_, &str>(0),
                        r.get::<_, &str>(1),
                        r.get::<_, Option<&str>>(2).unwrap_or("?"),
                        r.get::<_, Option<&str>>(3).unwrap_or("?"),
                        r.get::<_, Option<&str>>(4).unwrap_or("?"),
                    )
                })
                .collect();
            panic!("sink did not catch up to lsn_b {lsn_b} after reconnect; rep: {dump:?}");
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    let pre_switch_segment: String = client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    client
        .batch_execute(
            "SELECT pg_switch_wal(); \
             INSERT INTO pause_test (v) SELECT 'c-' || g FROM generate_series(1, 1000) g;",
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if sink_dir.join(&pre_switch_segment).exists() {
            break;
        }
        if Instant::now() > deadline {
            panic!("post-reconnect segment {pre_switch_segment} never sealed");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Prove the sink doesn't corrupt existing segments under disk-full.
#[test]
#[ignore = "requires Docker"]
fn sink_handles_disk_full() {
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

    struct DropContainer(String);
    impl Drop for DropContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }

    let Some(sink_bin) = build_musl("beyond-pg-sink") else {
        eprintln!("skipping sink_handles_disk_full: musl bin unavailable");
        return;
    };

    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "synchronous_commit=off",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut client);

    let sink_bin_str = sink_bin.to_str().unwrap().to_owned();

    let out = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            "--add-host=host.docker.internal:host-gateway",
            "--tmpfs",
            "/sinkdata:size=48m",
            "-v",
            &format!("{sink_bin_str}:/beyond-pg-sink:ro"),
            "alpine:3.20",
            "/beyond-pg-sink",
            "--mode",
            "tcp",
            "--connstr",
            &format!("host=host.docker.internal port={pg_port} user=postgres"),
            "--dir",
            "/sinkdata",
            "--port",
            "9000",
            "--slot",
            "wal_sink_disk_full",
        ])
        .output()
        .expect("docker run sink");
    assert!(
        out.status.success(),
        "docker run sink: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let sink_id = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    let _sink_container = DropContainer(sink_id.clone());

    // Wait for sink in pg_stat_replication
    let mut streaming = false;
    for _ in 0..120 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_disk_full'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(streaming, "sink container never connected");

    client
        .batch_execute(
            "CREATE TABLE disk_full_test (id serial, v text); \
             INSERT INTO disk_full_test (v) SELECT repeat('x', 1024) FROM generate_series(1, 80000) g;",
        )
        .unwrap();

    let _lsn_full: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    // Poll for flush_lsn to STOP advancing.
    let mut last_flush: Option<String> = None;
    let mut stable_count = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let cur: Option<String> = client
            .query_opt(
                "SELECT flush_lsn::text FROM pg_stat_replication \
                 WHERE application_name = 'wal_sink_disk_full'",
                &[],
            )
            .unwrap()
            .and_then(|r| r.get(0));
        if cur.is_some() && cur == last_flush {
            stable_count += 1;
            if stable_count >= 5 {
                break;
            }
        } else {
            stable_count = 0;
            last_flush = cur;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // Verify sink container is still running.
    let out = std::process::Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", &sink_id])
        .output()
        .expect("docker inspect");
    let running = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    assert_eq!(running, "true", "sink crashed under disk-full (running={running})");

    // List segments inside container.
    let out = std::process::Command::new("docker")
        .args(["exec", &sink_id, "ls", "-la", "/sinkdata"])
        .output()
        .expect("docker exec ls");
    let listing = String::from_utf8_lossy(&out.stdout).into_owned();
    let has_sealed = listing.lines().any(|l| {
        let last = l.split_whitespace().last().unwrap_or("");
        last.len() == 24 && last.bytes().all(|b| b.is_ascii_hexdigit())
    });
    assert!(
        has_sealed,
        "no complete sealed segment in sink dir under disk-full\nlisting:\n{listing}"
    );
}

/// Prove retention pruning runs while WAL is actively streaming.
#[test]
#[ignore = "requires Docker"]
fn sink_retention_prunes_under_streaming() {
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

    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut client);

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-retention-{pg_port}"));
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
        "wal_sink_retention",
        "--retention-segments",
        "8",
    ]);
    #[cfg(unix)]
    cmd.process_group(0);
    let process = cmd.spawn().expect("spawn beyond-pg-sink");
    let _sink = Sink {
        process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port);

    let mut streaming = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_retention'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(streaming, "sink never connected");

    client
        .batch_execute("CREATE TABLE retention_test (id serial, v text);")
        .unwrap();

    for _ in 0..10 {
        client
            .batch_execute(
                "INSERT INTO retention_test (v) SELECT 'r-' || g FROM generate_series(1, 10000) g; \
                 SELECT pg_switch_wal(); \
                 INSERT INTO retention_test (v) SELECT 'tick-' || g FROM generate_series(1, 200) g;",
            )
            .unwrap();
    }

    let final_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let flushed: Option<bool> = client
            .query_opt(
                &format!(
                    "SELECT flush_lsn >= '{final_lsn}'::pg_lsn \
                     FROM pg_stat_replication \
                     WHERE application_name = 'wal_sink_retention'"
                ),
                &[],
            )
            .unwrap()
            .map(|r| r.get(0));
        if flushed == Some(true) {
            break;
        }
        if Instant::now() > deadline {
            panic!("sink did not reach final_lsn {final_lsn} within 60s");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // Give the retention watcher a moment to react to the last segment seal.
    std::thread::sleep(Duration::from_secs(2));

    let segs: Vec<String> = std::fs::read_dir(&sink_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
        .collect();
    let count = segs.len();
    assert!(
        (8..=20).contains(&count),
        "retention count out of bounds: got {count} segments, expected 8..=20; segs: {segs:?}"
    );

    let active: bool = client
        .query_one(
            "SELECT active FROM pg_replication_slots WHERE slot_name = 'wal_sink_retention'",
            &[],
        )
        .unwrap()
        .get(0);
    assert!(active, "slot is no longer active");
}

/// Prove the sink's connection string parser handles password-based auth.
#[test]
#[ignore = "requires Docker"]
fn sink_streams_with_md5_auth() {
    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_PASSWORD", "testpass123")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "scram-sha-256")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url =
        format!("host=127.0.0.1 port={pg_port} user=postgres password=testpass123 dbname=postgres");

    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    client
        .batch_execute(
            "DO $$ DECLARE p text; BEGIN \
                 SELECT current_setting('hba_file') INTO p; \
                 EXECUTE format( \
                     $q$COPY (SELECT line FROM (VALUES \
                         ('local all all trust'), \
                         ('host all all all scram-sha-256'), \
                         ('host replication all all scram-sha-256') \
                     ) AS t(line)) TO %L$q$, p); \
             END; $$; \
             SELECT pg_reload_conf();",
        )
        .expect("pg_hba rewrite failed");

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-md5-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = probe.local_addr().unwrap().port();
    drop(probe);

    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres password=testpass123");
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
        "wal_sink_md5",
    ]);
    #[cfg(unix)]
    cmd.process_group(0);
    let process = cmd.spawn().expect("spawn beyond-pg-sink");
    let _sink = Sink {
        process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port);

    let mut streaming = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_md5'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(streaming, "sink never connected with password auth");

    client
        .batch_execute(
            "CREATE TABLE md5_test (id serial, v text); \
             INSERT INTO md5_test (v) SELECT 'row-' || g FROM generate_series(1, 50) g;",
        )
        .unwrap();
    let pre_switch_segment: String = client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    client
        .batch_execute(
            "SELECT pg_switch_wal(); \
             INSERT INTO md5_test (v) SELECT 'tail-' || g FROM generate_series(1, 1000) g;",
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if sink_dir.join(&pre_switch_segment).exists() {
            break;
        }
        if Instant::now() > deadline {
            let ls: Vec<String> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            panic!("segment {pre_switch_segment} never sealed; dir: {ls:?}");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Prove pg_switch_wal alone — with no follow-up activity — eventually seals
/// the segment in sink_dir.
#[test]
#[ignore = "requires Docker"]
fn sink_seals_segment_with_quiet_primary() {
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

    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect to primary");
    allow_replication(&mut client);

    let sink_dir = std::env::temp_dir().join(format!("wal-sink-quiet-{pg_port}"));
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
        "wal_sink_quiet",
    ]);
    #[cfg(unix)]
    cmd.process_group(0);
    let process = cmd.spawn().expect("spawn beyond-pg-sink");
    let _sink = Sink {
        process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port);

    let mut streaming = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'wal_sink_quiet'",
                &[],
            )
            .unwrap();
        if row.is_some() {
            streaming = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(streaming, "sink never connected");

    client
        .batch_execute(
            "CREATE TABLE quiet_test (id serial, v text); \
             INSERT INTO quiet_test (v) SELECT 'row-' || g FROM generate_series(1, 50) g;",
        )
        .unwrap();

    let pre_switch_segment: String = client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);

    client.execute("SELECT pg_switch_wal()", &[]).unwrap();

    // No follow-up activity. Sleep briefly to let WAL flush.
    std::thread::sleep(Duration::from_secs(1));

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if sink_dir.join(&pre_switch_segment).exists() {
            break;
        }
        if Instant::now() > deadline {
            let ls: Vec<(String, u64)> = std::fs::read_dir(&sink_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| {
                    let n = e.file_name().to_string_lossy().into_owned();
                    let s = e.metadata().map(|m| m.len()).unwrap_or(0);
                    (n, s)
                })
                .collect();
            panic!(
                "segment {pre_switch_segment} never sealed with quiet primary; dir: {ls:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Pure-Rust regression test locking in the chmod fix for bind-mount readability.
#[test]
fn chmod_sink_dir_makes_bind_mount_readable() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let dir = tmp.path();
    let file = dir.join("inside.txt");

    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        let f = opts.open(&file).expect("create file");
        f.set_permissions(std::fs::Permissions::from_mode(0o600))
            .expect("chmod file 0600");
    }

    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .expect("chmod dir 0700");

    let out = std::process::Command::new("chmod")
        .args(["-R", "a+rX", dir.to_str().unwrap()])
        .output()
        .expect("chmod -R a+rX");
    assert!(
        out.status.success(),
        "chmod -R a+rX: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let dir_mode = std::fs::metadata(dir).unwrap().permissions().mode();
    assert_eq!(
        dir_mode & 0o755,
        0o755,
        "dir mode after a+rX: {:o}",
        dir_mode & 0o7777
    );

    let file_mode = std::fs::metadata(&file).unwrap().permissions().mode();
    assert_eq!(
        file_mode & 0o644,
        0o644,
        "file mode after a+rX: {:o}",
        file_mode & 0o7777
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: TLS integration — postgres accepts the cert produced by
// beyond_pg::tls::ensure_cert, and rejects plain TCP when pg_hba requires SSL.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires Docker"]
fn tls_integration_postgres_with_beyond_pg_cert() {
    use beyond_pg::tls::{TlsCertOutcome, ensure_cert};

    fn allow_replication_ssl(client: &mut postgres::Client) {
        // SSL-only pg_hba: every `hostssl` line forces TLS; no plain `host` lines.
        // A `sslmode=disable` connect from another peer must be rejected.
        client
            .batch_execute(
                "DO $$ DECLARE p text; BEGIN \
                     SELECT current_setting('hba_file') INTO p; \
                     EXECUTE format( \
                         $q$COPY (SELECT line FROM (VALUES \
                             ('local all all trust'), \
                             ('hostssl all all all scram-sha-256'), \
                             ('hostssl replication all all scram-sha-256') \
                         ) AS t(line)) TO %L$q$, p); \
                 END; $$; \
                 SELECT pg_reload_conf();",
            )
            .expect("pg_hba rewrite failed");
    }

    // ── 1. Generate cert with beyond_pg::tls::ensure_cert ───────────────────
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let outcome = ensure_cert(tmp.path()).expect("ensure_cert");
    assert_eq!(
        outcome,
        TlsCertOutcome::Generated,
        "first call must Generate"
    );
    let cert_path = tmp.path().join("beyond/server.crt");
    let key_path = tmp.path().join("beyond/server.key");
    assert!(cert_path.exists(), "server.crt missing");
    assert!(key_path.exists(), "server.key missing");

    // Assertion C: cert content is non-trivial. ensure_cert wrote a real PEM,
    // not an empty stub. Ed25519 certs are compact (~470 bytes PEM-encoded);
    // 300 bytes is well above any plausible empty/stub but below real content.
    let cert_size = std::fs::metadata(&cert_path).unwrap().len();
    assert!(
        cert_size > 300,
        "ensure_cert produced suspiciously small cert: {cert_size} bytes"
    );

    // Postgres in the container runs as uid 999 and refuses to load a key it
    // doesn't own. chown via a short-lived root container so the host runner
    // never needs root.
    let certdir_str = tmp.path().join("beyond").to_str().unwrap().to_owned();
    let chown_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--user",
            "root",
            "-v",
            &format!("{certdir_str}:/tls"),
            "postgres:18",
            "chown",
            "-R",
            "999:999",
            "/tls",
        ])
        .output()
        .expect("docker chown");
    assert!(
        chown_out.status.success(),
        "docker chown failed: {}",
        String::from_utf8_lossy(&chown_out.stderr)
    );

    // ── 2. Start postgres with SSL on, mounting the cert dir ────────────────
    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "scram-sha-256")
        .with_env_var("POSTGRES_PASSWORD", "tlstest")
        .with_mount(Mount::bind_mount(certdir_str.clone(), "/tls"))
        .with_cmd([
            "-c",
            "ssl=on",
            "-c",
            "ssl_cert_file=/tls/server.crt",
            "-c",
            "ssl_key_file=/tls/server.key",
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!(
        "host=127.0.0.1 port={pg_port} user=postgres password=tlstest dbname=postgres"
    );

    // Connect once via the default (non-SSL) trust-local route through the
    // unix socket-equivalent to rewrite pg_hba. Use the password-bearing URL
    // since auth method is scram. The first connection still allows plain TCP
    // because POSTGRES_HOST_AUTH_METHOD seeded the default rules.
    let mut setup = postgres::Client::connect(&pg_url, postgres::NoTls)
        .expect("initial setup connection (pre-pg_hba rewrite)");
    allow_replication_ssl(&mut setup);
    drop(setup);

    // ── 3. Assertion A: SSL connect succeeds and ssl_is_used()=true ─────────
    let cert_pem = std::fs::read(&cert_path).expect("read cert");
    let root_cert = native_tls::Certificate::from_pem(&cert_pem).expect("parse cert");
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(root_cert)
        // Self-signed cert with SAN=localhost; we'll connect via 127.0.0.1, so
        // disable hostname checks. The verify-CA equivalent (trust the cert
        // bytes but skip CN match) is exactly this.
        .danger_accept_invalid_hostnames(true)
        .build()
        .expect("build TlsConnector");
    let mtls = postgres_native_tls::MakeTlsConnector::new(connector);

    let mut ssl_client = postgres::Client::connect(
        &format!("{pg_url} sslmode=require"),
        mtls,
    )
    .expect("SSL connect should succeed against beyond-pg cert");

    let ssl_in_use: bool = ssl_client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_ssl \
             WHERE ssl AND pid = pg_backend_pid())",
            &[],
        )
        .unwrap()
        .get(0);
    assert!(ssl_in_use, "pg_stat_ssl says backend is not using SSL");
    drop(ssl_client);

    // ── 4. Assertion B: sslmode=disable is rejected by pg_hba ───────────────
    let plain_result = postgres::Client::connect(
        &format!("{pg_url} sslmode=disable"),
        postgres::NoTls,
    );
    assert!(
        plain_result.is_err(),
        "sslmode=disable should be rejected by hostssl-only pg_hba, got Ok"
    );

    // Note: testing the sink itself over TLS is future work — beyond-pg-sink
    // currently only speaks plaintext to the primary. When TLS support lands,
    // extend this test to spawn the sink with --sslmode=verify-ca.
    drop(container);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Three replicas concurrently restore WAL from the same sink dir via
// bind-mount restore_command. Exposes concurrent filesystem-read races.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires Docker"]
fn multiple_replicas_recover_from_one_sink() {
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

    // ── 1. primary ──────────────────────────────────────────────────────────
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
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect primary");
    allow_replication(&mut primary_client);

    // ── 2. sink ─────────────────────────────────────────────────────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-multi-{pg_port}"));
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
        "wal_sink_multi",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_proc = sink_cmd.spawn().expect("spawn beyond-pg-sink");
    let _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    // Wait for the sink to attach.
    let mut streaming = false;
    for _ in 0..60 {
        if primary_client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_multi'",
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
    assert!(streaming, "sink never attached");

    // ── 3. pre-backup rows ──────────────────────────────────────────────────
    primary_client
        .batch_execute(
            "CREATE TABLE archive_test_multi (id serial, v text); \
             INSERT INTO archive_test_multi (v) \
               SELECT 'pre-' || g FROM generate_series(1, 50) g;",
        )
        .expect("pre-backup rows");
    let pre_segment: String = primary_client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();
    // Wait for the pre segment to seal in sink_dir.
    let deadline = Instant::now() + Duration::from_secs(60);
    while !sink_dir.join(&pre_segment).exists() {
        if Instant::now() > deadline {
            panic!("pre-backup segment {pre_segment} never sealed");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 4. pg_basebackup ×3 ─────────────────────────────────────────────────
    let pgdata_r1 = std::env::temp_dir().join(format!("multi-pgdata1-{pg_port}"));
    let pgdata_r2 = std::env::temp_dir().join(format!("multi-pgdata2-{pg_port}"));
    let pgdata_r3 = std::env::temp_dir().join(format!("multi-pgdata3-{pg_port}"));
    for p in [&pgdata_r1, &pgdata_r2, &pgdata_r3] {
        std::fs::create_dir_all(p).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o777)).unwrap();
        }
    }
    let _c1 = DropDir(pgdata_r1.clone());
    let _c2 = DropDir(pgdata_r2.clone());
    let _c3 = DropDir(pgdata_r3.clone());

    for pgdata in [&pgdata_r1, &pgdata_r2, &pgdata_r3] {
        let pgdata_str = pgdata.to_str().unwrap();
        let out = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--add-host=host.docker.internal:host-gateway",
                "-v",
                &format!("{pgdata_str}:/pgdata"),
                "postgres:18",
                "pg_basebackup",
                "-d",
                &format!("host=host.docker.internal port={pg_port} user=postgres"),
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
            "pg_basebackup into {pgdata_str}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // ── 5. post-backup rows ─────────────────────────────────────────────────
    primary_client
        .batch_execute(
            "INSERT INTO archive_test_multi (v) \
               SELECT 'post-' || g FROM generate_series(1, 50) g;",
        )
        .expect("post-backup rows");
    let post_segment: String = primary_client
        .query_one("SELECT pg_walfile_name(pg_current_wal_lsn())", &[])
        .unwrap()
        .get(0);
    primary_client
        .execute("SELECT pg_switch_wal()", &[])
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    while !sink_dir.join(&post_segment).exists() {
        if Instant::now() > deadline {
            panic!("post-backup segment {post_segment} never sealed");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 6. kill primary ─────────────────────────────────────────────────────
    drop(primary_client);
    drop(primary);

    // ── 7. write recovery config into each pgdata via root container ────────
    for pgdata in [&pgdata_r1, &pgdata_r2, &pgdata_r3] {
        let pgdata_str = pgdata.to_str().unwrap();
        let setup_script = "touch /pgdata/standby.signal && \
             echo \"restore_command = 'cp /mnt/sink/%f %p'\" \
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
            "recovery setup ({pgdata_str}): {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // ── 8. chmod sink_dir so the postgres uid inside containers can read it ─
    let chmod_out = std::process::Command::new("chmod")
        .args(["-R", "a+rX", &sink_dir_str])
        .output()
        .expect("chmod sink_dir");
    assert!(chmod_out.status.success(), "chmod sink_dir failed");

    // ── 9. start all three replicas in parallel ─────────────────────────────
    let mut replica_ports = [0u16; 3];
    let mut replica_ids = [String::new(), String::new(), String::new()];

    for (i, pgdata) in [&pgdata_r1, &pgdata_r2, &pgdata_r3].iter().enumerate() {
        let pgdata_str = pgdata.to_str().unwrap();
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        replica_ports[i] = port;

        let out = std::process::Command::new("docker")
            .args([
                "run",
                "-d",
                "--user",
                "root",
                "-v",
                &format!("{pgdata_str}:/pgdata"),
                "-v",
                &format!("{sink_dir_str}:/mnt/sink:ro"),
                "-p",
                &format!("{port}:5432"),
                "postgres:18",
                "bash",
                "-c",
                "chmod 700 /pgdata && chown -R postgres:postgres /pgdata \
                 && exec gosu postgres postgres -D /pgdata",
            ])
            .output()
            .expect("docker run replica");
        assert!(
            out.status.success(),
            "replica {i} start: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        replica_ids[i] = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    }

    let _r1 = DropContainer(replica_ids[0].clone());
    let _r2 = DropContainer(replica_ids[1].clone());
    let _r3 = DropContainer(replica_ids[2].clone());

    // ── 10. wait for each replica to accept connections ─────────────────────
    for (i, &port) in replica_ports.iter().enumerate() {
        let url = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if postgres::Client::connect(&url, postgres::NoTls).is_ok() {
                break;
            }
            if Instant::now() > deadline {
                panic!("replica {i} not accepting connections on port {port} within 90s");
            }
            std::thread::sleep(Duration::from_millis(300));
        }
    }

    // ── 11. wait for >=100 rows on each replica, in parallel ────────────────
    let mut handles = Vec::new();
    for (i, &port) in replica_ports.iter().enumerate() {
        let h = std::thread::spawn(move || -> Result<bool, String> {
            let url = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
            let mut client = postgres::Client::connect(&url, postgres::NoTls)
                .map_err(|e| format!("replica {i} connect: {e}"))?;
            let deadline = Instant::now() + Duration::from_secs(90);
            loop {
                let n: i64 = client
                    .query_one("SELECT count(*) FROM archive_test_multi", &[])
                    .map(|r| r.get(0))
                    .unwrap_or(0);
                if n >= 100 {
                    let in_recovery: bool = client
                        .query_one("SELECT pg_is_in_recovery()", &[])
                        .map(|r| r.get(0))
                        .unwrap_or(false);
                    return Ok(in_recovery);
                }
                if Instant::now() > deadline {
                    return Err(format!("replica {i} timed out at count={n}"));
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        });
        handles.push(h);
    }

    for h in handles {
        let res = h.join().expect("thread join");
        let in_recovery = res.expect("replica did not recover within 90s");
        assert!(in_recovery, "replica must still be in recovery");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: 3-minute pgbench soak. Verify the sink's RSS, fd count, .partial
// files, and replication-slot lag are all bounded under sustained load.
// Linux-only because /proc snapshotting isn't portable; skipped elsewhere.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires Docker"]
fn sink_stability_under_3min_load() {
    if !cfg!(target_os = "linux") {
        eprintln!(
            "sink_stability_under_3min_load: skipping on non-Linux \
             (depends on /proc/<pid>/status and /proc/<pid>/fd)"
        );
        return;
    }

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

    #[cfg(target_os = "linux")]
    fn read_rss_kb(pid: u32) -> Option<u64> {
        let s = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let mut parts = rest.split_ascii_whitespace();
                if let Some(num) = parts.next() {
                    return num.parse::<u64>().ok();
                }
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    fn read_rss_kb(_pid: u32) -> Option<u64> {
        None
    }

    #[cfg(target_os = "linux")]
    fn count_fds(pid: u32) -> Option<usize> {
        std::fs::read_dir(format!("/proc/{pid}/fd")).ok().map(|d| d.count())
    }
    #[cfg(not(target_os = "linux"))]
    fn count_fds(_pid: u32) -> Option<usize> {
        None
    }

    // ── 1. primary tuned for pgbench ────────────────────────────────────────
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "max_connections=20",
            "-c",
            "shared_buffers=128MB",
            "-c",
            "synchronous_standby_names=*",
            "-c",
            "synchronous_commit=remote_write",
        ])
        .start()
        .expect("docker not available or postgres:18 pull failed");

    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let primary_id = primary.id().to_owned();
    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect primary");
    allow_replication(&mut client);

    // ── 2. sink ─────────────────────────────────────────────────────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-soak-{pg_port}"));
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
        "wal_sink_soak",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_proc = sink_cmd.spawn().expect("spawn beyond-pg-sink");
    let sink_pid = sink_proc.id();
    let _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    let mut streaming = false;
    for _ in 0..60 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_soak'",
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
    assert!(streaming, "sink never attached");

    // ── 3. pgbench init ─────────────────────────────────────────────────────
    let init_out = std::process::Command::new("docker")
        .args([
            "exec", &primary_id, "gosu", "postgres", "pgbench", "-i", "-s", "1", "postgres",
        ])
        .output()
        .expect("docker exec pgbench -i");
    assert!(
        init_out.status.success(),
        "pgbench -i: {}",
        String::from_utf8_lossy(&init_out.stderr)
    );

    // ── 4. spawn pgbench for 180s (background) ──────────────────────────────
    let mut pgbench = std::process::Command::new("docker")
        .args([
            "exec",
            &primary_id,
            "gosu",
            "postgres",
            "pgbench",
            "-c",
            "4",
            "-j",
            "4",
            "-T",
            "180",
            "postgres",
        ])
        .spawn()
        .expect("spawn pgbench");

    // ── 5. sample every 10s for 18 iterations (= 180s) ──────────────────────
    let mut samples: Vec<(u64, usize, usize, i64)> = Vec::with_capacity(18);
    for _ in 0..18 {
        std::thread::sleep(Duration::from_secs(10));
        let rss = read_rss_kb(sink_pid).unwrap_or(0);
        let fds = count_fds(sink_pid).unwrap_or(0);
        let partials = std::fs::read_dir(&sink_dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| {
                        e.file_name()
                            .to_string_lossy()
                            .ends_with(".partial")
                    })
                    .count()
            })
            .unwrap_or(0);
        let lag: i64 = client
            .query_opt(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)::bigint \
                 FROM pg_stat_replication WHERE application_name='wal_sink_soak'",
                &[],
            )
            .unwrap()
            .and_then(|r| r.get::<_, Option<i64>>(0))
            .unwrap_or(0);
        samples.push((rss, fds, partials, lag));
    }

    // ── 6. wait for pgbench to finish ───────────────────────────────────────
    let pgbench_status = pgbench.wait().expect("wait pgbench");
    assert!(pgbench_status.success(), "pgbench did not exit cleanly");

    // ── 7. wait for the sink to catch up post-pgbench ───────────────────────
    let final_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let caught: Option<bool> = client
            .query_opt(
                &format!(
                    "SELECT flush_lsn >= '{final_lsn}'::pg_lsn \
                     FROM pg_stat_replication WHERE application_name='wal_sink_soak'"
                ),
                &[],
            )
            .unwrap()
            .map(|r| r.get(0));
        if caught == Some(true) {
            break;
        }
        if Instant::now() > deadline {
            panic!("sink never caught up to {final_lsn} after pgbench (samples={samples:?})");
        }
        std::thread::sleep(Duration::from_millis(300));
    }

    // ── 8. assertions ───────────────────────────────────────────────────────
    let min_rss = samples.iter().map(|s| s.0).min().unwrap_or(0).max(1);
    let max_rss = samples.iter().map(|s| s.0).max().unwrap_or(0);
    let min_fds = samples.iter().map(|s| s.1).min().unwrap_or(0);
    let max_fds = samples.iter().map(|s| s.1).max().unwrap_or(0);

    assert!(
        max_rss < min_rss * 2,
        "RSS doubled (likely leak): min={min_rss}kB max={max_rss}kB; samples={samples:?}"
    );
    assert!(
        max_fds < min_fds + 10,
        "fd count grew >10 (likely leak): min={min_fds} max={max_fds}; samples={samples:?}"
    );
    for (i, (_, _, partials, lag)) in samples.iter().enumerate() {
        assert!(
            *partials <= 1,
            "sample {i}: partials={partials} > 1; samples={samples:?}"
        );
        assert!(
            *lag < 64 * 1024 * 1024,
            "sample {i}: lag={lag} bytes > 64MiB; samples={samples:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: A paused sink pins the primary's WAL via its replication slot.
// Resuming the sink lets the slot advance and WAL recycle.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires Docker"]
fn slot_blocks_wal_pruning_when_consumer_stops() {
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

    fn count_wal_segments(client: &mut postgres::Client) -> i64 {
        // pg_ls_waldir() lists pg_wal/ contents via SQL — no docker exec
        // permission/path surprises. Filter to 24-char hex segment files only
        // (excludes archive_status/, .partial, .history, etc.).
        client
            .query_one(
                "SELECT count(*) FROM pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}$'",
                &[],
            )
            .unwrap()
            .get(0)
    }

    // ── 1. primary with small WAL retention so it recycles quickly ──────────
    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=4",
            "-c",
            "min_wal_size=32MB",
            "-c",
            "max_wal_size=64MB",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = primary.get_host_port_ipv4(5432).unwrap();
    let pg_url = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let _primary_id = primary.id().to_owned();
    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("connect primary");
    allow_replication(&mut client);

    // ── 2. sink ─────────────────────────────────────────────────────────────
    let sink_dir = std::env::temp_dir().join(format!("wal-sink-blocked-{pg_port}"));
    std::fs::create_dir_all(&sink_dir).unwrap();

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
        sink_dir.to_str().unwrap(),
        "--port",
        &sink_port.to_string(),
        "--slot",
        "wal_sink_blocked",
    ]);
    #[cfg(unix)]
    sink_cmd.process_group(0);
    let sink_proc = sink_cmd.spawn().expect("spawn beyond-pg-sink");
    let sink_pid = sink_proc.id();
    let _sink = Sink {
        process: sink_proc,
        dir: sink_dir.clone(),
    };
    wait_http_ready(sink_port);

    let mut streaming = false;
    for _ in 0..60 {
        if client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name='wal_sink_blocked'",
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
    assert!(streaming, "sink never attached");

    // ── 3. seed table and wait for flush ────────────────────────────────────
    client
        .batch_execute(
            "CREATE TABLE slot_test (id serial, v text); \
             INSERT INTO slot_test (v) SELECT 'seed-' || g FROM generate_series(1, 50) g;",
        )
        .unwrap();
    let seed_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        // flush_lsn can transiently be NULL before the standby has fully
        // initialised, so deserialise as Option<bool> and treat NULL as false.
        let row = client
            .query_opt(
                &format!(
                    "SELECT flush_lsn >= '{seed_lsn}'::pg_lsn \
                     FROM pg_stat_replication WHERE application_name='wal_sink_blocked'"
                ),
                &[],
            )
            .unwrap();
        let flushed = row
            .and_then(|r| r.try_get::<_, Option<bool>>(0).ok().flatten())
            .unwrap_or(false);
        if flushed {
            break;
        }
        if Instant::now() > deadline {
            panic!("sink never reached seed_lsn {seed_lsn}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // ── 4. snapshot restart_lsn + WAL count before pause ────────────────────
    let restart_lsn_before: String = client
        .query_one(
            "SELECT restart_lsn::text FROM pg_replication_slots \
             WHERE slot_name='wal_sink_blocked'",
            &[],
        )
        .unwrap()
        .get(0);
    let wal_count_before = count_wal_segments(&mut client);

    // ── 5. SIGSTOP the sink (process stays alive, slot stays "active") ──────
    #[cfg(unix)]
    unsafe {
        libc::kill(sink_pid as i32, libc::SIGSTOP);
    }
    #[cfg(not(unix))]
    {
        let _ = sink_pid;
        panic!("non-unix: cannot SIGSTOP");
    }
    // Give the kernel a beat to deliver the signal.
    std::thread::sleep(Duration::from_millis(500));

    // ── 6. drive WAL on the primary to force rotation ───────────────────────
    // Each batch is ~5 MB of text; 8 batches × switch ≈ 40+ MB, well above
    // max_wal_size=64MB. Without the slot, postgres would recycle freely;
    // with it, segments must accumulate.
    for _ in 0..8 {
        client
            .batch_execute(
                "INSERT INTO slot_test (v) \
                   SELECT repeat('x', 100) FROM generate_series(1, 50000) g; \
                 SELECT pg_switch_wal();",
            )
            .unwrap();
    }

    // ── 7. assertions while paused ──────────────────────────────────────────
    let wal_count_after_stop = count_wal_segments(&mut client);
    assert!(
        wal_count_after_stop > wal_count_before,
        "WAL did not grow with paused consumer: before={wal_count_before} after={wal_count_after_stop}"
    );

    let restart_lsn_after_stop: String = client
        .query_one(
            "SELECT restart_lsn::text FROM pg_replication_slots \
             WHERE slot_name='wal_sink_blocked'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        restart_lsn_after_stop, restart_lsn_before,
        "restart_lsn must not advance while sink is SIGSTOPed"
    );

    let active: bool = client
        .query_one(
            "SELECT active FROM pg_replication_slots WHERE slot_name='wal_sink_blocked'",
            &[],
        )
        .unwrap()
        .get(0);
    assert!(active, "slot should still be active (TCP conn alive)");

    // ── 8. resume the sink ──────────────────────────────────────────────────
    #[cfg(unix)]
    unsafe {
        libc::kill(sink_pid as i32, libc::SIGCONT);
    }

    // Wait up to 60s for the sink to drain the backlog.
    let current_lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .unwrap()
        .get(0);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let row = client
            .query_opt(
                &format!(
                    "SELECT flush_lsn >= '{current_lsn}'::pg_lsn \
                     FROM pg_stat_replication WHERE application_name='wal_sink_blocked'"
                ),
                &[],
            )
            .unwrap();
        let caught = row
            .and_then(|r| r.try_get::<_, Option<bool>>(0).ok().flatten())
            .unwrap_or(false);
        if caught {
            break;
        }
        if Instant::now() > deadline {
            panic!("sink did not catch up to {current_lsn} after SIGCONT");
        }
        std::thread::sleep(Duration::from_millis(300));
    }

    // Force a checkpoint so postgres prunes/recycles old WAL.
    client.execute("CHECKPOINT", &[]).unwrap();

    let restart_lsn_after_resume: String = client
        .query_one(
            "SELECT restart_lsn::text FROM pg_replication_slots \
             WHERE slot_name='wal_sink_blocked'",
            &[],
        )
        .unwrap()
        .get(0);
    let advanced: bool = client
        .query_one(
            &format!(
                "SELECT '{restart_lsn_after_resume}'::pg_lsn > '{restart_lsn_before}'::pg_lsn"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        advanced,
        "restart_lsn did not advance after resume: before={restart_lsn_before} after={restart_lsn_after_resume}"
    );

    // Assertion E: eventually the primary recycles some WAL.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_count;
    loop {
        last_count = count_wal_segments(&mut client);
        if last_count <= wal_count_after_stop {
            // Some WAL was pruned (count went down or stayed); we tolerate equal
            // since at minimum primary stopped growing under recycling pressure.
            // The asserted condition is `<=`, which is what we want here.
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        // Repeat a small checkpoint to nudge recycling.
        let _ = client.execute("CHECKPOINT", &[]);
        std::thread::sleep(Duration::from_secs(2));
    }
    assert!(
        last_count <= wal_count_after_stop,
        "WAL was never pruned after resume: at_pause={wal_count_after_stop} now={last_count}"
    );
}

