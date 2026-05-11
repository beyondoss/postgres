//! beyond-pg-cdc — in-VM CDC consumer.
//!
//! Connects to Postgres over a unix socket as a logical replication client,
//! decodes pgoutput, and fans the resulting JSON change events out to RESP3
//! TCP subscribers (matching the platform KV service's watch transport).
//! Threads + blocking I/O only — no async runtime.

mod decode;
mod lsn;
mod proto;
mod resp;

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::decode::Decoder;
use crate::lsn::Lsn;
use crate::proto::{Conn, WalMsg};
use crate::resp::Subscribers;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn handle_signal(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Release);
}

struct Args {
    socket_dir: String,
    pg_port: u16,
    user: String,
    dbname: String,
    port: u16,
    slot: String,
    publication: String,
}

impl Args {
    fn defaults() -> Self {
        Self {
            socket_dir: "/var/run/postgresql".to_owned(),
            pg_port: 5433,
            user: "replicator".to_owned(),
            dbname: "postgres".to_owned(),
            port: 9001,
            slot: "cdc".to_owned(),
            publication: "cdc".to_owned(),
        }
    }
}

fn parse_args() -> Args {
    let mut args = Args::defaults();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--socket-dir" => args.socket_dir = require(&mut it, "--socket-dir"),
            "--pg-port" => args.pg_port = parse(require(&mut it, "--pg-port"), "--pg-port"),
            "--user" => args.user = require(&mut it, "--user"),
            "--dbname" => args.dbname = require(&mut it, "--dbname"),
            "--port" => args.port = parse(require(&mut it, "--port"), "--port"),
            "--slot" => args.slot = require(&mut it, "--slot"),
            "--publication" => args.publication = require(&mut it, "--publication"),
            other => {
                eprintln!("error: unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }
    args
}

fn require(it: &mut impl Iterator<Item = String>, name: &str) -> String {
    match it.next() {
        Some(v) => v,
        None => {
            eprintln!("error: {name} requires a value");
            std::process::exit(1);
        }
    }
}

fn parse<T: std::str::FromStr>(v: String, name: &str) -> T {
    v.parse().unwrap_or_else(|_| {
        eprintln!("error: {name} could not be parsed");
        std::process::exit(1);
    })
}

fn main() {
    let args = parse_args();

    install_signal_handlers();

    let subs: Subscribers = Arc::new(Mutex::new(Vec::new()));

    let resp_subs = Arc::clone(&subs);
    let resp_port = args.port;
    std::thread::spawn(move || resp::serve(resp_port, resp_subs));

    let mut delay_ms: u64 = 100;
    while !SHUTDOWN.load(Ordering::Acquire) {
        match run_replication(&args, &subs) {
            Ok(()) => break,
            Err(e) => {
                eprintln!("cdc replication error: {e}; reconnecting in {delay_ms}ms");
                sleep_interruptible(Duration::from_millis(delay_ms));
                delay_ms = (delay_ms * 2).min(30_000);
            }
        }
    }
}

fn install_signal_handlers() {
    #[cfg(unix)]
    // SAFETY: handle_signal only flips a static AtomicBool, which is async-signal-safe.
    // sa is fully initialized via zeroed() + explicit assignments. No SA_RESTART so
    // blocking reads return EINTR promptly on shutdown.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handle_signal as libc::sighandler_t;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = 0;
        for sig in [libc::SIGTERM, libc::SIGINT] {
            if libc::sigaction(sig, &sa, std::ptr::null_mut()) < 0 {
                eprintln!(
                    "warn: sigaction({sig}) failed: {} — graceful shutdown disabled",
                    io::Error::last_os_error()
                );
            }
        }
    }
}

fn sleep_interruptible(total: Duration) {
    let deadline = Instant::now() + total;
    while Instant::now() < deadline {
        if SHUTDOWN.load(Ordering::Acquire) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100).min(deadline - Instant::now()));
    }
}

fn run_replication(args: &Args, subs: &Subscribers) -> Result<(), String> {
    let mut conn: Conn = proto::connect(
        &args.socket_dir,
        args.pg_port,
        &args.user,
        &args.dbname,
        true,
    )?;
    proto::start_replication(&mut conn, &args.slot, &args.publication, Lsn::ZERO)?;
    conn.set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| format!("set_read_timeout: {e}"))?;

    let mut decoder = Decoder::new();
    let mut last_lsn = Lsn::ZERO;
    let mut last_status = Instant::now();

    loop {
        if SHUTDOWN.load(Ordering::Acquire) {
            return Ok(());
        }

        match proto::recv_wal(&mut conn) {
            Ok(WalMsg::XLogData { lsn, data }) => {
                if lsn.0 > last_lsn.0 {
                    last_lsn = lsn;
                }
                if let Some(json) = decoder.decode(lsn, &data) {
                    let msg: Arc<[u8]> = Arc::from(json.into_boxed_slice());
                    broadcast(subs, msg);
                }
            }
            Ok(WalMsg::Keepalive { reply_needed }) => {
                if reply_needed {
                    proto::send_status(&mut conn, last_lsn)
                        .map_err(|e| format!("send_status: {e}"))?;
                    last_status = Instant::now();
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                proto::send_status(&mut conn, last_lsn)
                    .map_err(|e| format!("send_status (timeout path): {e}"))?;
                last_status = Instant::now();
            }
            Err(e) => return Err(format!("recv_wal: {e}")),
        }

        if last_status.elapsed() >= Duration::from_secs(10) {
            proto::send_status(&mut conn, last_lsn)
                .map_err(|e| format!("send_status (proactive): {e}"))?;
            last_status = Instant::now();
        }
    }
}

fn broadcast(subs: &Subscribers, msg: Arc<[u8]>) {
    let mut guard = match subs.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    // try_send drops on a full channel — push delivery is best-effort.
    guard.retain(|tx| tx.try_send(Arc::clone(&msg)).is_ok());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;

    #[test]
    fn broadcast_fans_out_to_multiple_subscribers() {
        let subs: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let (tx1, rx1) = sync_channel::<Arc<[u8]>>(8);
        let (tx2, rx2) = sync_channel::<Arc<[u8]>>(8);
        subs.lock().unwrap().push(tx1);
        subs.lock().unwrap().push(tx2);

        broadcast(&subs, Arc::from(b"event" as &[u8]));

        let m1 = rx1.recv_timeout(Duration::from_secs(1)).unwrap();
        let m2 = rx2.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(&*m1, b"event");
        assert_eq!(&*m2, b"event");
        assert_eq!(subs.lock().unwrap().len(), 2);
    }

    #[test]
    fn broadcast_prunes_dead_receiver() {
        let subs: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let (tx1, rx1) = sync_channel::<Arc<[u8]>>(8);
        let (tx2, rx2) = sync_channel::<Arc<[u8]>>(8);
        drop(rx2);
        subs.lock().unwrap().push(tx1);
        subs.lock().unwrap().push(tx2);

        broadcast(&subs, Arc::from(b"event" as &[u8]));

        assert_eq!(subs.lock().unwrap().len(), 1);
        let m1 = rx1.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(&*m1, b"event");
    }

    #[test]
    fn broadcast_prunes_full_channel() {
        let subs: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = sync_channel::<Arc<[u8]>>(1);
        subs.lock().unwrap().push(tx);

        broadcast(&subs, Arc::from(b"first" as &[u8]));
        broadcast(&subs, Arc::from(b"second" as &[u8]));

        assert_eq!(subs.lock().unwrap().len(), 0);
        let m = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(&*m, b"first");
        assert!(rx.try_recv().is_err());
    }
}
