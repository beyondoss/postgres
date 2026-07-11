//! PgBouncer so_reuseport peering end-to-end test: cross-worker query cancellation.
//!
//! When beyond-pg runs more than one so_reuseport pooler worker (see
//! `src/config.rs::pgbouncer_ini` / `PgbScaler`), a Postgres CancelRequest arrives
//! on a *fresh* TCP connection that the kernel may route to a different worker than
//! the one holding the session. Without peering that cancel is silently dropped;
//! with peering the receiving worker forwards it to the peer that owns the session.
//!
//! This test proves the mechanism against a real PgBouncer using the exact config
//! keys `pgbouncer_ini` emits (`peer_id`, per-worker `unix_socket_dir`, and the
//! shared `[peers]` map — asserted in the `config` unit tests). To make the routing
//! deterministic in CI, the two workers listen on *distinct* ports and the cancel is
//! sent to the "wrong" one on purpose; so_reuseport port-sharing is orthogonal to the
//! forwarding logic under test and is already exercised in production.
//!
//!   - [`peering_forwards_misrouted_cancel`] — peering ON  → misrouted cancel cancels the query
//!   - [`no_peering_drops_misrouted_cancel`] — peering OFF → misrouted cancel is dropped (control)
//!
//! # Prerequisites
//!   Test image: `mise run build:test-image`
//!   (`beyond-pg-test:latest` = postgres:18 + pgbouncer 1.25 + …)
//!
//! Run: `cargo test --test pgbouncer_peering -- --ignored --nocapture`

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::{Duration, Instant};

// Serialize Docker launches (matches the other e2e suites on macOS Docker Desktop).
static DOCKER: Mutex<()> = Mutex::new(());

const IMAGE: &str = "beyond-pg-test:latest";
const CANCEL_CODE: u32 = 80877102;
const PROTO_3: u32 = 196608;

// ---------------------------------------------------------------------------
// Minimal Docker RAII helper
// ---------------------------------------------------------------------------

struct Container(String);

impl Container {
    /// Start postgres (trust auth) with the two pooler ports published to the host.
    /// Returns None if the test image is absent so the test skips instead of failing.
    fn start() -> Option<Self> {
        let present = std::process::Command::new("docker")
            .args(["image", "inspect", IMAGE])
            .output()
            .ok()?
            .status
            .success();
        if !present {
            eprintln!("skipping: {IMAGE} not found — run `mise run build:test-image`");
            return None;
        }
        let out = std::process::Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e",
                "POSTGRES_PASSWORD=x",
                "-p",
                "127.0.0.1::6432",
                "-p",
                "127.0.0.1::6433",
                IMAGE,
            ])
            .output()
            .expect("docker run");
        assert!(
            out.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Some(Self(
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
        ))
    }

    fn exec(&self, args: &[&str]) -> std::process::Output {
        let mut a = vec!["exec", &self.0];
        a.extend_from_slice(args);
        std::process::Command::new("docker")
            .args(&a)
            .output()
            .expect("docker exec")
    }

    fn exec_u(&self, user: &str, args: &[&str]) -> std::process::Output {
        let mut a = vec!["exec", "-u", user, &self.0];
        a.extend_from_slice(args);
        std::process::Command::new("docker")
            .args(&a)
            .output()
            .expect("docker exec -u")
    }

    /// Host port mapped to the container's `container_port`.
    fn host_port(&self, container_port: u16) -> u16 {
        let out = std::process::Command::new("docker")
            .args(["port", &self.0, &format!("{container_port}/tcp")])
            .output()
            .expect("docker port");
        let s = String::from_utf8_lossy(&out.stdout);
        let line = s.lines().next().unwrap_or_default();
        line.rsplit(':')
            .next()
            .and_then(|p| p.trim().parse().ok())
            .unwrap_or_else(|| panic!("no host port for {container_port}: {s:?}"))
    }

    fn wait_postgres_ready(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.exec(&["pg_isready", "-q"]).status.success() {
                return true;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        false
    }

    fn logs(&self) -> String {
        let out = std::process::Command::new("docker")
            .args(["logs", &self.0])
            .output()
            .expect("docker logs");
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .status();
    }
}

/// Write the two worker configs and (re)start both pgbouncers. `peering` toggles
/// the `peer_id` + `[peers]` keys; both cases keep distinct `unix_socket_dir`.
fn configure_and_start(c: &Container, peering: bool) {
    // trust auth still requires the user to exist in auth_file (pgbouncer 1.25).
    let mk = c.exec_u(
        "postgres",
        &["bash", "-c", "echo '\"postgres\" \"\"' > /tmp/userlist.txt"],
    );
    assert!(
        mk.status.success(),
        "write userlist: {}",
        String::from_utf8_lossy(&mk.stderr)
    );

    for id in 1..=2u16 {
        let port = 6431 + id;
        let peers = if peering {
            "\n[peers]\n1 = host=/tmp/pgb1\n2 = host=/tmp/pgb2"
        } else {
            ""
        };
        let ident = if peering {
            format!("\nunix_socket_dir = /tmp/pgb{id}\npeer_id = {id}")
        } else {
            format!("\nunix_socket_dir = /tmp/pgb{id}")
        };
        let ini = format!(
            "[databases]\n\
             * = host=/var/run/postgresql port=5432\n\
             [pgbouncer]\n\
             listen_addr = 0.0.0.0\n\
             listen_port = {port}\n\
             auth_type = trust\n\
             auth_file = /tmp/userlist.txt\n\
             pool_mode = transaction\n\
             logfile = /tmp/pgb{id}.log\n\
             pidfile = /tmp/pgb{id}.pid{ident}{peers}\n"
        );
        let script = format!("mkdir -p /tmp/pgb{id} && cat > /tmp/pgb{id}.ini <<'INI'\n{ini}INI");
        let w = c.exec_u("postgres", &["bash", "-c", &script]);
        assert!(
            w.status.success(),
            "write ini {id}: {}",
            String::from_utf8_lossy(&w.stderr)
        );
    }

    // Stop any prior workers, then start fresh.
    let _ = c.exec(&[
        "bash",
        "-c",
        "kill $(cat /tmp/pgb1.pid /tmp/pgb2.pid 2>/dev/null) 2>/dev/null; sleep 1; true",
    ]);
    for id in 1..=2u16 {
        let s = c.exec_u(
            "postgres",
            &["bash", "-c", &format!("pgbouncer -d /tmp/pgb{id}.ini")],
        );
        assert!(
            s.status.success(),
            "start pgbouncer {id}: {}\n{}",
            String::from_utf8_lossy(&s.stderr),
            c.logs()
        );
    }
    std::thread::sleep(Duration::from_secs(2));
}

// ---------------------------------------------------------------------------
// Raw pgwire probe
// ---------------------------------------------------------------------------

fn read_exact(s: &mut TcpStream, n: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    s.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read one backend message: (tag, body-without-length-prefix).
fn read_msg(s: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let tag = read_exact(s, 1)?[0];
    let len = u32::from_be_bytes(read_exact(s, 4)?.try_into().unwrap()) as usize;
    let body = read_exact(s, len - 4)?;
    Ok((tag, body))
}

/// Send StartupMessage (user/database=postgres) and return this session's
/// (backend pid, secret key) from BackendKeyData. Trust auth → no password.
fn startup(s: &mut TcpStream) -> (u32, u32) {
    let params = b"user\x00postgres\x00database\x00postgres\x00\x00";
    let mut msg = Vec::new();
    msg.extend_from_slice(&((8 + params.len()) as u32).to_be_bytes());
    msg.extend_from_slice(&PROTO_3.to_be_bytes());
    msg.extend_from_slice(params);
    s.write_all(&msg).unwrap();

    let (mut pid, mut secret) = (0u32, 0u32);
    loop {
        let (tag, body) = read_msg(s).expect("startup read");
        match tag {
            b'R' => {
                let kind = u32::from_be_bytes(body[..4].try_into().unwrap());
                assert_eq!(
                    kind, 0,
                    "expected trust auth (AuthenticationOk), got kind {kind}"
                );
            }
            b'K' => {
                pid = u32::from_be_bytes(body[..4].try_into().unwrap());
                secret = u32::from_be_bytes(body[4..8].try_into().unwrap());
            }
            b'Z' => return (pid, secret),
            b'E' => panic!("startup error: {}", String::from_utf8_lossy(&body)),
            _ => {}
        }
    }
}

fn send_query(s: &mut TcpStream, sql: &str) {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&((4 + body.len()) as u32).to_be_bytes());
    msg.extend_from_slice(&body);
    s.write_all(&msg).unwrap();
}

/// Send a CancelRequest carrying (pid, secret) to `port` — a fresh connection,
/// exactly as libpq's PQcancel does.
fn send_cancel(port: u16, pid: u32, secret: u32) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect cancel port");
    let mut msg = Vec::new();
    msg.extend_from_slice(&16u32.to_be_bytes());
    msg.extend_from_slice(&CANCEL_CODE.to_be_bytes());
    msg.extend_from_slice(&pid.to_be_bytes());
    msg.extend_from_slice(&secret.to_be_bytes());
    s.write_all(&msg).unwrap();
    let _ = s.read(&mut [0u8; 1]); // server closes after handling the cancel
}

/// Start `SELECT pg_sleep(30)` on the worker at `query_port`, then fire a
/// CancelRequest for that session at `cancel_port`. Returns true iff the query
/// was cancelled within `wait` (i.e. the cancel was forwarded to the right worker).
fn misrouted_cancel_hits(query_port: u16, cancel_port: u16, wait: Duration) -> bool {
    let mut conn = TcpStream::connect(("127.0.0.1", query_port)).expect("connect query port");
    let (pid, secret) = startup(&mut conn);
    eprintln!("  worker gave BackendKeyData pid={pid} secret={secret:#010x}");
    send_query(&mut conn, "SELECT pg_sleep(30)");

    // Give pgbouncer a moment to hand the query to a server before cancelling.
    std::thread::sleep(Duration::from_millis(300));
    eprintln!("  sending CancelRequest to the OTHER worker (port {cancel_port})");
    send_cancel(cancel_port, pid, secret);

    conn.set_read_timeout(Some(wait)).unwrap();
    loop {
        match read_msg(&mut conn) {
            Ok((b'E', body)) => {
                let text = String::from_utf8_lossy(&body);
                let cancelled = text.contains("57014") || text.contains("canceling statement");
                eprintln!("  ErrorResponse: {}", text.replace('\0', " "));
                return cancelled;
            }
            Ok((b'C', _)) => {
                eprintln!("  CommandComplete — pg_sleep ran to completion, cancel was dropped");
                return false;
            }
            Ok(_) => continue,
            Err(e) => {
                eprintln!("  no reply within {wait:?} ({e}) — cancel was dropped");
                return false;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker + beyond-pg-test:latest image"]
fn peering_forwards_misrouted_cancel() {
    let _g = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(c) = Container::start() else { return };
    assert!(
        c.wait_postgres_ready(Duration::from_secs(90)),
        "postgres not ready\n{}",
        c.logs()
    );
    configure_and_start(&c, /* peering = */ true);

    let qport = c.host_port(6432);
    let cport = c.host_port(6433);
    assert!(
        misrouted_cancel_hits(qport, cport, Duration::from_secs(10)),
        "peering ON: a cancel routed to the wrong worker must be forwarded and cancel the query"
    );
}

#[test]
#[ignore = "requires Docker + beyond-pg-test:latest image"]
fn no_peering_drops_misrouted_cancel() {
    // Control: proves the test actually discriminates — without peering the same
    // misrouted cancel is dropped, which is the behavior this feature fixes.
    let _g = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(c) = Container::start() else { return };
    assert!(
        c.wait_postgres_ready(Duration::from_secs(90)),
        "postgres not ready\n{}",
        c.logs()
    );
    configure_and_start(&c, /* peering = */ false);

    let qport = c.host_port(6432);
    let cport = c.host_port(6433);
    assert!(
        !misrouted_cancel_hits(qport, cport, Duration::from_secs(6)),
        "peering OFF: a cancel routed to the wrong worker has no peer to forward to and must be dropped"
    );
}
