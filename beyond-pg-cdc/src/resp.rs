//! RESP3 TCP server for streaming CDC change events.
//!
//! Wire protocol (subset of RESP3):
//!
//! Client commands (RESP3 arrays of bulk strings):
//! - `HELLO 3` — handshake; server responds with a minimal map
//! - `PING` — server replies `+PONG\r\n`
//! - `WATCH [SINCE <lsn>]` — register subscriber and stream push frames;
//!   SINCE is parsed and ignored (slot replays from confirmed_flush_lsn server-side)
//! - `UNWATCH` — server replies `+OK\r\n` and closes the connection
//! - `STATS` — server replies with a RESP3 map of runtime counters
//!
//! Server pushes (`>` prefix, 2-element arrays of bulk strings):
//! - `["change", "ready"]` — sent immediately on WATCH
//! - `["change", <json>]` — one per decoded change event
//! - `["change", "heartbeat"]` — emitted after 30s of channel idleness

use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::Stats;

/// Bounded per-subscriber channel capacity. Slow consumers exceeding this drop
/// events; the broadcaster prunes their senders on the next failed `try_send`.
pub const SUBSCRIBER_CAPACITY: usize = 64;

/// Maximum number of elements accepted in a RESP3 array command.
const MAX_ARRAY_ELEMS: usize = 16;

/// Maximum bulk-string byte length accepted from a client (16 MiB).
const MAX_BULK_LEN: usize = 16 * 1024 * 1024;

/// Maximum concurrent connections accepted by the RESP server.
const MAX_CONNECTIONS: usize = 64;

/// Idle interval after which a heartbeat push is emitted to keep the connection
/// (and any intermediate proxy) warm.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

pub type Subscribers = Arc<Mutex<Vec<SyncSender<Arc<[u8]>>>>>;

/// Bind `addr:port` and accept connections forever, spawning one OS thread per
/// accepted connection up to `MAX_CONNECTIONS`. Exits the process on bind
/// failure (callers treat the listener as fatal infrastructure).
pub fn serve(addr: &str, port: u16, subs: Subscribers, stats: Arc<Stats>) -> ! {
    let listener = match TcpListener::bind((addr, port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: failed to bind {addr}:{port}: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("cdc resp listening on {addr}:{port}");

    let conn_count = Arc::new(AtomicUsize::new(0));

    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                let prev = conn_count.fetch_add(1, Ordering::Relaxed);
                if prev >= MAX_CONNECTIONS {
                    conn_count.fetch_sub(1, Ordering::Relaxed);
                    eprintln!("cdc: connection limit ({MAX_CONNECTIONS}) reached, dropping {peer}");
                    drop(stream);
                    continue;
                }
                let subs = Arc::clone(&subs);
                let stats = Arc::clone(&stats);
                let conn_count = Arc::clone(&conn_count);
                std::thread::spawn(move || {
                    handle_conn(stream, subs, stats);
                    conn_count.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(e) => eprintln!("cdc accept: {e}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Cmd {
    Hello,
    Ping,
    Watch { since_requested: bool },
    Unwatch,
    Stats,
    Unknown(String),
}

fn handle_conn(stream: TcpStream, subs: Subscribers, stats: Arc<Stats>) {
    // Per-command read deadline; cleared before entering the streaming loop so
    // long-idle subscribers aren't dropped between events.
    if let Err(e) = stream.set_read_timeout(Some(Duration::from_secs(30))) {
        eprintln!("cdc: set_read_timeout failed: {e}");
        return;
    }

    // try_clone so the reader's BufReader and the writer share the same fd.
    // BufReader buffers ahead but we never multiplex reads with writes on a
    // streaming connection, so this is safe.
    let writer = match stream.try_clone() {
        Ok(w) => w,
        Err(e) => {
            eprintln!("cdc: try_clone failed: {e}");
            return;
        }
    };
    let mut reader = BufReader::new(stream);
    let mut writer = writer;

    loop {
        let cmd = match read_cmd(&mut reader) {
            Ok(c) => c,
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::WouldBlock => {}
                    _ => eprintln!("cdc: command read error: {e}"),
                }
                return;
            }
        };
        match cmd {
            Cmd::Hello => {
                if write_hello(&mut writer).is_err() {
                    return;
                }
            }
            Cmd::Ping => {
                if writer.write_all(b"+PONG\r\n").is_err() {
                    return;
                }
            }
            Cmd::Watch { since_requested } => {
                if since_requested {
                    let _ = writer.write_all(
                        b"-ERR SINCE is not supported; replay starts from confirmed_flush_lsn\r\n",
                    );
                    return;
                }
                stream_events(writer, subs);
                return;
            }
            Cmd::Unwatch => {
                if let Err(e) = writer.write_all(b"+OK\r\n") {
                    eprintln!("cdc: failed to write UNWATCH response: {e}");
                }
                return;
            }
            Cmd::Stats => {
                if write_stats(&mut writer, &subs, &stats).is_err() {
                    return;
                }
            }
            Cmd::Unknown(name) => {
                let msg = format!("-ERR unknown command '{name}'\r\n");
                if writer.write_all(msg.as_bytes()).is_err() {
                    return;
                }
            }
        }
    }
}

/// Write a RESP3 map with runtime counters.
fn write_stats(w: &mut impl Write, subs: &Subscribers, stats: &Stats) -> io::Result<()> {
    let events = stats.events_total.load(Ordering::Relaxed);
    let reconnects = stats.reconnects_total.load(Ordering::Relaxed);
    let last_lsn = stats
        .last_flush_lsn
        .lock()
        .map(|g| g.to_string())
        .unwrap_or_else(|_| "0/00000000".to_owned());
    let sub_count = subs.lock().map(|g| g.len()).unwrap_or(0);

    // RESP3 map with 4 entries.
    write!(w, "%4\r\n")?;
    write_bulk_pair(w, "events_total", &events.to_string())?;
    write_bulk_pair(w, "reconnects_total", &reconnects.to_string())?;
    write_bulk_pair(w, "last_flush_lsn", &last_lsn)?;
    write_bulk_pair(w, "subscribers", &sub_count.to_string())?;
    w.flush()
}

fn write_bulk_pair(w: &mut impl Write, key: &str, val: &str) -> io::Result<()> {
    write!(
        w,
        "${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        val.len(),
        val
    )
}

/// Register a subscriber and pump events from the broadcaster's channel out as
/// RESP3 push frames until the receiver closes or a write fails.
fn stream_events(writer: TcpStream, subs: Subscribers) {
    if let Err(e) = writer.set_write_timeout(Some(Duration::from_secs(30))) {
        eprintln!("cdc: set_write_timeout failed: {e}");
        return;
    }
    // BufWriter coalesces the N writes per push frame into a single syscall per event.
    let mut writer = io::BufWriter::new(writer);

    let (tx, rx) = sync_channel::<Arc<[u8]>>(SUBSCRIBER_CAPACITY);
    {
        let mut g = match subs.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        g.push(tx);
    }

    // Ready signal so clients know the subscription is live and they can begin
    // accepting events. Mirrors the KV watch handshake.
    if write_push(&mut writer, &[b"change", b"ready"]).is_err() || writer.flush().is_err() {
        return;
    }

    loop {
        match rx.recv_timeout(HEARTBEAT_INTERVAL) {
            Ok(msg) => {
                if write_push(&mut writer, &[b"change", &msg]).is_err() || writer.flush().is_err() {
                    return;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if write_push(&mut writer, &[b"change", b"heartbeat"]).is_err()
                    || writer.flush().is_err()
                {
                    return;
                }
            }
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
    // The broadcaster's retain() prunes our SyncSender on its next failed
    // try_send (after we drop the receiver), so no explicit dereg needed.
}

/// Parse one RESP3 array command: `*N\r\n` followed by N bulk strings.
/// Returns the decoded `Cmd`; unknown verbs are surfaced via `Cmd::Unknown` so
/// the caller can reply with an error and keep the connection alive.
fn read_cmd(r: &mut BufReader<TcpStream>) -> io::Result<Cmd> {
    let header = read_line(r)?;
    let n = match header.first() {
        Some(b'*') => parse_len(&header[1..])?,
        // Some clients (e.g. nc) send inline commands; we don't support them
        // and there's no reasonable framing fallback for streaming.
        _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "expected '*'")),
    };
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty array"));
    }
    if n > MAX_ARRAY_ELEMS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "array too large",
        ));
    }

    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(n);
    for _ in 0..n {
        parts.push(read_bulk(r)?);
    }

    let verb = parts[0].to_ascii_uppercase();
    Ok(match verb.as_slice() {
        b"HELLO" => Cmd::Hello,
        b"PING" => Cmd::Ping,
        b"WATCH" => {
            let since_requested = parts.len() >= 3 && parts[1].eq_ignore_ascii_case(b"SINCE");
            Cmd::Watch { since_requested }
        }
        b"UNWATCH" => Cmd::Unwatch,
        b"STATS" => Cmd::Stats,
        _ => Cmd::Unknown(String::from_utf8_lossy(&verb).into_owned()),
    })
}

/// Read one CRLF-terminated line. Returns the bytes without the trailing CRLF.
fn read_line(r: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(16);
    r.read_until(b'\n', &mut buf)?;
    if buf.last() != Some(&b'\n') {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no newline"));
    }
    buf.pop(); // \n
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    Ok(buf)
}

/// Read a `$len\r\n<bytes>\r\n` bulk string.
fn read_bulk(r: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let header = read_line(r)?;
    if header.first() != Some(&b'$') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected '$'"));
    }
    let len = parse_len(&header[1..])?;
    if len > MAX_BULK_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bulk string too large",
        ));
    }
    let buf_len = len
        .checked_add(2)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "length overflow"))?;
    let mut data = vec![0u8; buf_len];
    use std::io::Read;
    r.read_exact(&mut data)?;
    if &data[len..] != b"\r\n" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "missing CRLF"));
    }
    data.truncate(len);
    Ok(data)
}

fn parse_len(s: &[u8]) -> io::Result<usize> {
    std::str::from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad length"))
}

/// Write a RESP3 push frame (`>N\r\n`) carrying `items.len()` bulk strings.
fn write_push(w: &mut impl Write, items: &[&[u8]]) -> io::Result<()> {
    write!(w, ">{}\r\n", items.len())?;
    for item in items {
        write!(w, "${}\r\n", item.len())?;
        w.write_all(item)?;
        w.write_all(b"\r\n")?;
    }
    Ok(())
}

/// Minimal RESP3 map response to HELLO. Three entries: server name, protocol
/// version (an integer reply), and version string. Sufficient for clients that
/// just want a handshake ack.
fn write_hello(w: &mut impl Write) -> io::Result<()> {
    const SERVER: &[u8] = b"beyond-pg-cdc";
    const VERSION: &[u8] = b"0.1.0";
    write!(w, "%3\r\n")?;
    write!(w, "$6\r\nserver\r\n${}\r\n", SERVER.len())?;
    w.write_all(SERVER)?;
    w.write_all(b"\r\n")?;
    write!(w, "$5\r\nproto\r\n:3\r\n")?;
    write!(w, "$7\r\nversion\r\n${}\r\n", VERSION.len())?;
    w.write_all(VERSION)?;
    w.write_all(b"\r\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::atomic::AtomicU64;

    fn make_stats() -> Arc<Stats> {
        Arc::new(Stats {
            events_total: AtomicU64::new(0),
            reconnects_total: AtomicU64::new(0),
            last_flush_lsn: Mutex::new(crate::lsn::Lsn::ZERO),
        })
    }

    // Helper: drive read_cmd against an in-memory TcpStream pair.
    fn parse(input: &[u8]) -> io::Result<Cmd> {
        // Use a real loopback pair so we can construct a BufReader<TcpStream>.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        client.write_all(input).unwrap();
        drop(client);
        let mut r = BufReader::new(server);
        read_cmd(&mut r)
    }

    #[test]
    fn parses_hello() {
        assert_eq!(
            parse(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n").unwrap(),
            Cmd::Hello
        );
    }

    #[test]
    fn parses_ping() {
        assert_eq!(parse(b"*1\r\n$4\r\nPING\r\n").unwrap(), Cmd::Ping);
    }

    #[test]
    fn parses_watch() {
        assert_eq!(
            parse(b"*1\r\n$5\r\nWATCH\r\n").unwrap(),
            Cmd::Watch {
                since_requested: false
            }
        );
    }

    #[test]
    fn parses_watch_since_sets_flag() {
        // SINCE arg is detected and surfaces as since_requested=true so the
        // handler can return an error instead of silently ignoring it.
        assert_eq!(
            parse(b"*3\r\n$5\r\nWATCH\r\n$5\r\nSINCE\r\n$10\r\n1/23456780\r\n").unwrap(),
            Cmd::Watch {
                since_requested: true
            }
        );
    }

    #[test]
    fn parses_unwatch() {
        assert_eq!(parse(b"*1\r\n$7\r\nUNWATCH\r\n").unwrap(), Cmd::Unwatch);
    }

    #[test]
    fn parses_stats() {
        assert_eq!(parse(b"*1\r\n$5\r\nSTATS\r\n").unwrap(), Cmd::Stats);
    }

    #[test]
    fn unknown_command_classified() {
        match parse(b"*1\r\n$3\r\nFOO\r\n").unwrap() {
            Cmd::Unknown(s) => assert_eq!(s, "FOO"),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn verbs_are_case_insensitive() {
        assert_eq!(parse(b"*1\r\n$4\r\nping\r\n").unwrap(), Cmd::Ping);
        assert_eq!(
            parse(b"*1\r\n$5\r\nWatch\r\n").unwrap(),
            Cmd::Watch {
                since_requested: false
            }
        );
    }

    #[test]
    fn write_push_frames_change_event() {
        let mut buf = Cursor::new(Vec::new());
        write_push(&mut buf, &[b"change", b"hello"]).unwrap();
        assert_eq!(
            buf.into_inner(),
            b">2\r\n$6\r\nchange\r\n$5\r\nhello\r\n".to_vec()
        );
    }

    #[test]
    fn write_push_frames_heartbeat() {
        let mut buf = Cursor::new(Vec::new());
        write_push(&mut buf, &[b"change", b"heartbeat"]).unwrap();
        assert_eq!(
            buf.into_inner(),
            b">2\r\n$6\r\nchange\r\n$9\r\nheartbeat\r\n".to_vec()
        );
    }

    #[test]
    fn write_hello_emits_minimal_map() {
        let mut buf = Cursor::new(Vec::new());
        write_hello(&mut buf).unwrap();
        let out = buf.into_inner();
        // %3 + server/beyond-pg-cdc + proto/:3 + version/0.1.0
        assert!(out.starts_with(b"%3\r\n"));
        assert!(
            out.windows(b"beyond-pg-cdc".len())
                .any(|w| w == b"beyond-pg-cdc")
        );
        assert!(out.windows(4).any(|w| w == b":3\r\n"));
        assert!(out.windows(b"0.1.0".len()).any(|w| w == b"0.1.0"));
    }

    #[test]
    fn write_stats_emits_map() {
        let stats = make_stats();
        stats.events_total.store(42, Ordering::Relaxed);
        stats.reconnects_total.store(1, Ordering::Relaxed);
        let subs: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let mut buf = Cursor::new(Vec::new());
        write_stats(&mut buf, &subs, &stats).unwrap();
        let out = std::str::from_utf8(&buf.into_inner()).unwrap().to_owned();
        assert!(out.starts_with("%4\r\n"), "expected RESP3 map: {out:?}");
        assert!(out.contains("events_total"));
        assert!(out.contains("42"));
        assert!(out.contains("reconnects_total"));
        assert!(out.contains("subscribers"));
    }

    fn connect_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    #[test]
    fn hello_then_ping_handshake() {
        use std::io::Read;
        let (mut client, server) = connect_pair();
        let empty_subs: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let stats = make_stats();
        let handle = std::thread::spawn(move || handle_conn(server, empty_subs, stats));

        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        client
            .write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
            .unwrap();

        // Read until we have the full HELLO response (one read may not be enough).
        let mut acc: Vec<u8> = Vec::new();
        let mut tmp = [0u8; 128];
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match client.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => {
                    acc.extend_from_slice(&tmp[..n]);
                    if acc
                        .windows(b"beyond-pg-cdc".len())
                        .any(|w| w == b"beyond-pg-cdc")
                        && acc.windows(b"0.1.0".len()).any(|w| w == b"0.1.0")
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        assert!(!acc.is_empty());
        assert_eq!(acc[0], b'%');
        assert!(
            acc.windows(b"beyond-pg-cdc".len())
                .any(|w| w == b"beyond-pg-cdc")
        );

        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut pong = [0u8; 7];
        client.read_exact(&mut pong).unwrap();
        assert_eq!(&pong, b"+PONG\r\n");

        drop(client);
        let _ = handle.join();
    }

    #[test]
    fn unknown_command_returns_error() {
        use std::io::Read;
        let (mut client, server) = connect_pair();
        let empty_subs: Subscribers = Arc::new(Mutex::new(Vec::new()));
        let stats = make_stats();
        let handle = std::thread::spawn(move || handle_conn(server, empty_subs, stats));

        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client.write_all(b"*1\r\n$3\r\nFOO\r\n").unwrap();

        let mut buf = [0u8; 128];
        let n = client.read(&mut buf).unwrap();
        assert!(n > 0);
        assert_eq!(buf[0], b'-');

        drop(client);
        let _ = handle.join();
    }

    #[test]
    fn write_push_round_trip_with_arbitrary_bytes() {
        // Binary-safe: bulk strings carry their length, so embedded \r\n is fine.
        let payload = b"{\"lsn\":\"1/23\",\"op\":\"insert\"}\r\nweird";
        let mut buf = Cursor::new(Vec::new());
        write_push(&mut buf, &[b"change", payload]).unwrap();
        let bytes = buf.into_inner();
        let header = format!(">2\r\n$6\r\nchange\r\n${}\r\n", payload.len());
        assert!(bytes.starts_with(header.as_bytes()));
        assert!(bytes.ends_with(b"\r\n"));
    }
}
