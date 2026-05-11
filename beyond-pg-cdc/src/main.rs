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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::decode::Decoder;
use crate::lsn::Lsn;
use crate::proto::{CdcError, Conn, WalMsg};
use crate::resp::Subscribers;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn handle_signal(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Release);
}

/// Shared runtime counters exposed via the RESP3 STATS command.
pub struct Stats {
    pub events_total: AtomicU64,
    pub reconnects_total: AtomicU64,
    pub last_flush_lsn: Mutex<Lsn>,
}

impl Stats {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events_total: AtomicU64::new(0),
            reconnects_total: AtomicU64::new(0),
            last_flush_lsn: Mutex::new(Lsn::ZERO),
        })
    }
}

struct Args {
    socket_dir: String,
    pg_port: u16,
    user: String,
    dbname: String,
    bind: String,
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
            bind: "127.0.0.1".to_owned(),
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
            "--bind" => args.bind = require(&mut it, "--bind"),
            "--port" => args.port = parse(require(&mut it, "--port"), "--port"),
            "--slot" => args.slot = require(&mut it, "--slot"),
            "--publication" => args.publication = require(&mut it, "--publication"),
            "--help" => {
                eprintln!("Usage: beyond-pg-cdc [OPTIONS]");
                eprintln!();
                eprintln!("Options:");
                eprintln!(
                    "  --socket-dir DIR    Postgres Unix socket directory (default: /var/run/postgresql)"
                );
                eprintln!("  --pg-port PORT      Postgres port in socket filename (default: 5433)");
                eprintln!("  --user USER         Postgres replication user (default: replicator)");
                eprintln!("  --dbname DB         Database name (default: postgres)");
                eprintln!("  --bind ADDR         RESP3 listener bind address (default: 127.0.0.1)");
                eprintln!("  --port PORT         RESP3 listener port (default: 9001)");
                eprintln!("  --slot NAME         Replication slot name (default: cdc)");
                eprintln!("  --publication NAME  Publication name (default: cdc)");
                eprintln!("  --help              Print this help message");
                std::process::exit(0);
            }
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
    let stats = Stats::new();

    let resp_subs = Arc::clone(&subs);
    let resp_stats = Arc::clone(&stats);
    let resp_bind = args.bind.clone();
    let resp_port = args.port;
    // resp::serve is `-> !`: it calls process::exit(1) on bind failure. Storing
    // the handle means a future panic here surfaces on join rather than being
    // silently discarded as it would be if the handle were immediately dropped.
    let _resp =
        std::thread::spawn(move || resp::serve(&resp_bind, resp_port, resp_subs, resp_stats));

    let mut delay_ms: u64 = 100;
    while !SHUTDOWN.load(Ordering::Acquire) {
        let started = Instant::now();
        match run_replication(&args, &subs, &stats) {
            Ok(()) => break,
            Err(CdcError::Config(e)) => {
                eprintln!("cdc fatal config error: {e}; exiting");
                std::process::exit(1);
            }
            Err(e) => {
                // Reset backoff if the connection was stable for at least 60s so
                // a transient drop after a long healthy run doesn't inherit a
                // stale 30s delay from an earlier flap.
                if started.elapsed() >= Duration::from_secs(60) {
                    delay_ms = 100;
                }
                eprintln!("cdc replication error: {e}; reconnecting in {delay_ms}ms");
                stats.reconnects_total.fetch_add(1, Ordering::Relaxed);
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

fn run_replication(args: &Args, subs: &Subscribers, stats: &Arc<Stats>) -> Result<(), CdcError> {
    let mut conn: Conn = proto::connect(
        &args.socket_dir,
        args.pg_port,
        &args.user,
        &args.dbname,
        true,
    )?;
    proto::start_replication(&mut conn, &args.slot, &args.publication, Lsn::ZERO)?;
    conn.set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| CdcError::Protocol(format!("set_read_timeout: {e}")))?;

    let mut decoder = Decoder::new();
    // last_write_lsn: highest XLogData LSN received from the server.
    // last_flush_lsn: highest LSN whose event was successfully offered to subscribers.
    // Postgres uses flush_lsn to advance confirmed_flush_lsn and recycle WAL,
    // so we must not advance it ahead of what has actually been delivered.
    let mut last_write_lsn = Lsn::ZERO;
    let mut last_flush_lsn = Lsn::ZERO;
    let mut last_status = Instant::now();

    loop {
        if SHUTDOWN.load(Ordering::Acquire) {
            return Ok(());
        }

        match proto::recv_wal(&mut conn) {
            Ok(WalMsg::XLogData { lsn, data }) => {
                if lsn > last_write_lsn {
                    last_write_lsn = lsn;
                }
                if let Some(json) = decoder.decode(lsn, &data) {
                    let msg: Arc<[u8]> = Arc::from(json.into_boxed_slice());
                    broadcast(subs, msg);
                    stats.events_total.fetch_add(1, Ordering::Relaxed);
                }
                // Advance flush only after delivering the event (best-effort).
                // If the process crashes between recv and here, the slot replays.
                if lsn > last_flush_lsn {
                    last_flush_lsn = lsn;
                    if let Ok(mut g) = stats.last_flush_lsn.lock() {
                        *g = last_flush_lsn;
                    }
                }
            }
            Ok(WalMsg::Keepalive {
                server_lsn,
                reply_needed,
            }) => {
                // The server's keepalive LSN covers WAL with no DML events.
                // Advancing flush to server_lsn here is safe: no data to deliver.
                if server_lsn > last_write_lsn {
                    last_write_lsn = server_lsn;
                }
                if server_lsn > last_flush_lsn {
                    last_flush_lsn = server_lsn;
                    if let Ok(mut g) = stats.last_flush_lsn.lock() {
                        *g = last_flush_lsn;
                    }
                }
                if reply_needed {
                    proto::send_status(&mut conn, last_write_lsn, last_flush_lsn)
                        .map_err(CdcError::Io)?;
                    last_status = Instant::now();
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                // Read timed out — fall through to the proactive status interval below.
            }
            Err(e) => return Err(CdcError::Io(e)),
        }

        if last_status.elapsed() >= Duration::from_secs(10) {
            proto::send_status(&mut conn, last_write_lsn, last_flush_lsn).map_err(CdcError::Io)?;
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
