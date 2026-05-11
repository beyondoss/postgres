use std::io::Write;
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt, runners::SyncRunner};

const BIN: &str = env!("CARGO_BIN_EXE_beyond-pg-cdc");

struct CdcProcess(Child);

impl Drop for CdcProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_cdc(pg_host: &str, pg_port: u16, port: u16) -> CdcProcess {
    let child = Command::new(BIN)
        .args([
            "--socket-dir",
            pg_host,
            "--pg-port",
            &pg_port.to_string(),
            "--user",
            "replicator",
            "--dbname",
            "postgres",
            "--port",
            &port.to_string(),
            "--slot",
            "cdc",
            "--publication",
            "cdc",
        ])
        .spawn()
        .expect("failed to spawn beyond-pg-cdc");
    CdcProcess(child)
}

/// Probe the RESP3 listener with a PING until it answers PONG, or panic after 30s.
fn wait_ready(port: u16) {
    use std::io::Read;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(1)));
            if s.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 7];
                if s.read_exact(&mut buf).is_ok() && &buf == b"+PONG\r\n" {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("beyond-pg-cdc RESP server did not become ready on port {port}");
}

/// Connect, send WATCH, and block until the server sends the "ready" push frame
/// (confirming the subscriber is registered). Then forward subsequent JSON change
/// events through the returned channel. Skips "heartbeat" control payloads.
///
/// Blocking on "ready" before returning is essential: it guarantees the subscriber
/// is in the broadcast list before the caller inserts rows, preventing the race
/// where events are broadcast to an empty subscriber list and silently dropped.
fn spawn_resp_reader(port: u16) -> std::sync::mpsc::Receiver<serde_json::Value> {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<()>(0);
    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        use std::io::{BufRead, BufReader, Read};

        let mut stream = match TcpStream::connect(("127.0.0.1", port)) {
            Ok(s) => s,
            Err(_) => return,
        };
        if stream.write_all(b"*1\r\n$5\r\nWATCH\r\n").is_err() {
            return;
        }
        stream
            .set_read_timeout(Some(Duration::from_secs(60)))
            .unwrap();
        let mut reader = BufReader::new(stream);
        let mut signaled_ready = false;

        loop {
            let mut header = Vec::new();
            if reader.read_until(b'\n', &mut header).is_err() || header.is_empty() {
                return;
            }
            while matches!(header.last(), Some(b'\n' | b'\r')) {
                header.pop();
            }
            let count = match header.first() {
                Some(b'>') => match std::str::from_utf8(&header[1..])
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(n) => n,
                    None => return,
                },
                _ => continue,
            };

            let mut elems: Vec<Vec<u8>> = Vec::with_capacity(count);
            for _ in 0..count {
                let mut len_line = Vec::new();
                if reader.read_until(b'\n', &mut len_line).is_err() {
                    return;
                }
                while matches!(len_line.last(), Some(b'\n' | b'\r')) {
                    len_line.pop();
                }
                if len_line.first() != Some(&b'$') {
                    return;
                }
                let len: usize = match std::str::from_utf8(&len_line[1..])
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(n) => n,
                    None => return,
                };
                let mut buf = vec![0u8; len + 2];
                if reader.read_exact(&mut buf).is_err() {
                    return;
                }
                buf.truncate(len);
                elems.push(buf);
            }

            if elems.len() == 2 && elems[0] == b"change" {
                let payload = &elems[1];
                if payload == b"ready" {
                    if !signaled_ready {
                        signaled_ready = true;
                        let _ = ready_tx.send(());
                    }
                    continue;
                }
                if payload == b"heartbeat" {
                    continue;
                }
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) {
                    if tx.send(v).is_err() {
                        return;
                    }
                }
            }
        }
    });

    // Block until the server confirms the subscriber is registered.
    ready_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("timed out waiting for WATCH ready signal");
    rx
}

fn recv_n(
    rx: &std::sync::mpsc::Receiver<serde_json::Value>,
    n: usize,
    timeout: Duration,
) -> Vec<serde_json::Value> {
    (0..n)
        .map(|i| {
            rx.recv_timeout(timeout)
                .unwrap_or_else(|_| panic!("timed out waiting for CDC event {i}"))
        })
        .collect()
}

fn lsn_u64(s: &str) -> u64 {
    let mut parts = s.splitn(2, '/');
    let hi = u64::from_str_radix(parts.next().unwrap_or("0"), 16).unwrap_or(0);
    let lo = u64::from_str_radix(parts.next().unwrap_or("0"), 16).unwrap_or(0);
    (hi << 32) | lo
}

#[test]
#[ignore = "requires Docker"]
fn cdc_streams_dml_events() {
    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut client = postgres::Client::connect(&connstr, postgres::NoTls)
        .expect("failed to connect to Postgres");

    // Provision replication infrastructure (mirrors what beyond-pg does in post_start).
    client
        .batch_execute(
            "DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
                 CREATE ROLE replicator LOGIN REPLICATION;
               END IF;
             END
             $$",
        )
        .expect("create replicator role");

    client
        .batch_execute(
            "DO $$
             BEGIN
               IF NOT EXISTS (
                 SELECT FROM pg_replication_slots WHERE slot_name = 'cdc'
               ) THEN
                 PERFORM pg_create_logical_replication_slot('cdc', 'pgoutput');
               END IF;
             END
             $$;
             DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'cdc') THEN
                 EXECUTE 'CREATE PUBLICATION cdc';
               END IF;
             END
             $$;
             CREATE TABLE IF NOT EXISTS e2e(id int PRIMARY KEY, val text);
             ALTER PUBLICATION cdc ADD TABLE e2e;",
        )
        .expect("create slot, publication, and test table");

    let port = 9091u16;
    let _cdc = spawn_cdc("127.0.0.1", pg_port, port);
    wait_ready(port);

    let rx = spawn_resp_reader(port);

    // 3 inserts + 1 update + 1 delete = 5 DML events.
    client
        .execute("INSERT INTO e2e VALUES (1, 'alpha')", &[])
        .unwrap();
    client
        .execute("INSERT INTO e2e VALUES (2, 'beta')", &[])
        .unwrap();
    client
        .execute("INSERT INTO e2e VALUES (3, 'gamma')", &[])
        .unwrap();
    client
        .execute("UPDATE e2e SET val = 'alpha2' WHERE id = 1", &[])
        .unwrap();
    client.execute("DELETE FROM e2e WHERE id = 2", &[]).unwrap();

    let events = recv_n(&rx, 5, Duration::from_secs(30));

    // --- INSERT (id=1) -------------------------------------------------------
    let ins = events
        .iter()
        .find(|e| e["op"] == "insert" && e["new"]["id"] == 1)
        .expect("insert(id=1) event");
    assert_eq!(ins["schema"], "public");
    assert_eq!(ins["table"], "e2e");
    assert_eq!(ins["new"]["val"], "alpha");
    assert!(ins["lsn"].is_string(), "lsn field missing");

    // --- UPDATE (id=1, REPLICA IDENTITY DEFAULT → no old tuple) --------------
    let upd = events
        .iter()
        .find(|e| e["op"] == "update")
        .expect("update event");
    assert_eq!(upd["new"]["id"], 1);
    assert_eq!(upd["new"]["val"], "alpha2");

    // --- DELETE (id=2, REPLICA IDENTITY DEFAULT → PK-only old tuple) ---------
    let del = events
        .iter()
        .find(|e| e["op"] == "delete")
        .expect("delete event");
    assert_eq!(del["old"]["id"], 2);
    assert!(del.get("new").is_none(), "delete must not have 'new'");

    // --- LSN ordering --------------------------------------------------------
    let lsns: Vec<u64> = events
        .iter()
        .map(|e| lsn_u64(e["lsn"].as_str().unwrap()))
        .collect();
    let mut sorted = lsns.clone();
    sorted.sort_unstable();
    assert_eq!(lsns, sorted, "events not in LSN order");

    drop(container);
}

#[test]
#[ignore = "requires Docker"]
fn cdc_slot_preserves_wal_across_restart() {
    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut client = postgres::Client::connect(&connstr, postgres::NoTls).unwrap();

    client
        .batch_execute(
            "DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
                 CREATE ROLE replicator LOGIN REPLICATION;
               END IF;
             END
             $$",
        )
        .unwrap();

    client
        .batch_execute(
            "DO $$
             BEGIN
               IF NOT EXISTS (
                 SELECT FROM pg_replication_slots WHERE slot_name = 'cdc'
               ) THEN
                 PERFORM pg_create_logical_replication_slot('cdc', 'pgoutput');
               END IF;
             END
             $$;
             DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'cdc') THEN
                 EXECUTE 'CREATE PUBLICATION cdc';
               END IF;
             END
             $$;
             CREATE TABLE IF NOT EXISTS restart_test(id int PRIMARY KEY, val text);
             ALTER PUBLICATION cdc ADD TABLE restart_test;",
        )
        .unwrap();

    let port = 9092u16;

    // Phase 1: start CDC, deliver row 1, then kill the process.
    // After delivery, CDC reports flush_lsn = LSN(row 1), so the slot's
    // confirmed_flush_lsn advances past row 1. On restart Postgres will
    // NOT replay row 1 — only WAL received after this point.
    {
        let _cdc = spawn_cdc("127.0.0.1", pg_port, port);
        wait_ready(port);
        let rx = spawn_resp_reader(port);
        client
            .execute("INSERT INTO restart_test VALUES (1, 'first')", &[])
            .unwrap();
        let events = recv_n(&rx, 1, Duration::from_secs(30));
        assert_eq!(events[0]["op"], "insert");
        assert_eq!(events[0]["new"]["id"], 1);
        // _cdc drops → process killed; rx drops → subscriber pruned on next broadcast
    }

    // Phase 2: restart CDC and verify slot durability.
    //
    // We subscribe *before* inserting to avoid the replay race: CDC may start
    // replaying buffered WAL before a late subscriber connects. By registering
    // first, every broadcast lands in our channel.
    //
    // Slot durability is proven by the absence of row 1: if flush_lsn were
    // not advanced correctly (the pre-fix bug), the slot would replay from an
    // earlier position and row 1 would re-appear. Instead we assert that only
    // the two new rows arrive — the slot remembered exactly where it left off.
    {
        let _cdc = spawn_cdc("127.0.0.1", pg_port, port);
        wait_ready(port);
        let rx = spawn_resp_reader(port);

        client
            .execute(
                "INSERT INTO restart_test VALUES (2, 'after_restart_a')",
                &[],
            )
            .unwrap();
        client
            .execute(
                "INSERT INTO restart_test VALUES (3, 'after_restart_b')",
                &[],
            )
            .unwrap();

        let events = recv_n(&rx, 2, Duration::from_secs(30));

        // Row 1 must NOT be replayed — the slot advanced past it.
        assert!(
            events.iter().all(|e| e["new"]["id"] != 1),
            "slot must not replay already-acknowledged row 1 (flush_lsn regression)"
        );
        assert!(
            events.iter().any(|e| e["new"]["id"] == 2),
            "post-restart insert id=2 not received"
        );
        assert!(
            events.iter().any(|e| e["new"]["id"] == 3),
            "post-restart insert id=3 not received"
        );
    }

    drop(container);
}

#[test]
#[ignore = "requires Docker"]
fn cdc_publication_gates_events() {
    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let pg_port = container.get_host_port_ipv4(5432).unwrap();
    let connstr = format!("host=127.0.0.1 port={pg_port} user=postgres dbname=postgres");
    let mut client = postgres::Client::connect(&connstr, postgres::NoTls).unwrap();

    client
        .batch_execute(
            "DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
                 CREATE ROLE replicator LOGIN REPLICATION;
               END IF;
             END
             $$",
        )
        .unwrap();

    client
        .batch_execute(
            "DO $$
             BEGIN
               IF NOT EXISTS (
                 SELECT FROM pg_replication_slots WHERE slot_name = 'cdc'
               ) THEN
                 PERFORM pg_create_logical_replication_slot('cdc', 'pgoutput');
               END IF;
             END
             $$;
             DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'cdc') THEN
                 EXECUTE 'CREATE PUBLICATION cdc';
               END IF;
             END
             $$;
             CREATE TABLE IF NOT EXISTS gated(id int PRIMARY KEY, val text);
             CREATE TABLE IF NOT EXISTS included(id int PRIMARY KEY, val text);
             ALTER PUBLICATION cdc ADD TABLE included;",
        )
        .unwrap();

    let port = 9093u16;
    let _cdc = spawn_cdc("127.0.0.1", pg_port, port);
    wait_ready(port);
    let rx = spawn_resp_reader(port);

    // Step 2: insert into gated (not in publication) — expect no events.
    client
        .execute("INSERT INTO gated VALUES (1, 'gated_pre')", &[])
        .unwrap();
    let res = rx.recv_timeout(Duration::from_secs(3));
    assert!(
        res.is_err(),
        "expected no event for ungated table insert, got {res:?}"
    );

    // Step 3: insert into included — expect exactly one event with table name.
    client
        .execute("INSERT INTO included VALUES (1, 'included_a')", &[])
        .unwrap();
    let events = recv_n(&rx, 1, Duration::from_secs(30));
    assert_eq!(events[0]["op"], "insert");
    assert_eq!(events[0]["table"], "included");
    assert_eq!(events[0]["new"]["id"], 1);

    // Step 4 + 5: add gated to publication, then insert again — expect event.
    client
        .batch_execute("ALTER PUBLICATION cdc ADD TABLE gated")
        .unwrap();
    client
        .execute("INSERT INTO gated VALUES (2, 'gated_post')", &[])
        .unwrap();
    let events = recv_n(&rx, 1, Duration::from_secs(30));
    assert_eq!(events[0]["op"], "insert");
    assert_eq!(events[0]["table"], "gated");
    assert_eq!(events[0]["new"]["id"], 2);

    drop(container);
}
