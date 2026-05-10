use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Child;
use std::time::Duration;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt, runners::SyncRunner};

const BIN: &str = env!("CARGO_BIN_EXE_beyond-pg-sink");

struct Sink {
    process: Child,
    dir: PathBuf,
}

impl Drop for Sink {
    fn drop(&mut self) {
        // SIGTERM lets beyond-pg-sink forward the signal to pg_receivewal and
        // wait for it to exit, avoiding an orphaned child that retries after drop.
        #[cfg(unix)]
        unsafe {
            libc::kill(self.process.id() as libc::pid_t, libc::SIGTERM);
        }
        #[cfg(not(unix))]
        let _ = self.process.kill();
        let _ = self.process.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
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
#[ignore = "requires Docker and pg_receivewal 18 on PATH"]
fn wal_sink_streams_from_primary() {
    if std::process::Command::new("pg_receivewal")
        .arg("--version")
        .output()
        .is_err()
    {
        panic!("pg_receivewal not on PATH — run `mise install` to get postgres 18 client tools");
    }

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
    let process = std::process::Command::new(BIN)
        .args([
            "--connstr",
            &connstr,
            "--dir",
            sink_dir.to_str().unwrap(),
            "--port",
            &http_port.to_string(),
            "--slot",
            "wal_sink",
        ])
        .spawn()
        .expect("failed to spawn beyond-pg-sink");

    // _sink is declared after container so it drops first — process killed
    // before the container is stopped.
    let _sink = Sink {
        process,
        dir: sink_dir.clone(),
    };

    wait_http_ready(http_port);

    let mut client =
        postgres::Client::connect(&pg_url, postgres::NoTls).expect("failed to connect to Postgres");

    // Poll until pg_receivewal appears in pg_stat_replication (connected and streaming).
    let mut streaming = false;
    for _ in 0..60 {
        let row = client
            .query_opt(
                "SELECT 1 FROM pg_stat_replication WHERE application_name = 'pg_receivewal'",
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
        "pg_receivewal never appeared in pg_stat_replication — did not connect"
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
                 WHERE application_name = 'pg_receivewal'"
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        flushed,
        "flush_lsn did not reach commit LSN {commit_lsn} — synchronous commit did not wait for sink"
    );

    // Force a WAL segment switch so pg_receivewal finalises the current segment:
    // it renames <name>.partial → <name> only at a segment boundary.
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
