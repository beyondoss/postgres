//! Standalone WAL forwarder binary for testing and standalone deployment.
//!
//! Connects to a local Postgres primary via physical replication (TCP) and
//! forwards WAL bytes to a `beyond-pg-sink --mode quic` instance over QUIC.
//!
//! Architecture: fully synchronous, no Tokio.
//!   * Thread 1: Postgres TCP reader (physical replication).
//!   * Thread 2: QUIC UDP pump (sync `quinn-proto` + `mio`).
//!   * `std::sync::mpsc` channels between them carry frames and acks.
//!
//! Usage:
//!   wal-forwarder --pg-port 5432 --sink-addr 127.0.0.1:9000 [--slot wal_sink]

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use beyond_pg_sink::send_one;
use bytes::{Buf, BufMut, BytesMut};
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token};
use quinn_proto::{
    ClientConfig, Connection, DatagramEvent, Dir, EndpointConfig, Event, ServerConfig, StreamEvent,
    StreamId,
};
use wal_proto::{
    Lsn, RecvError, WalMsg, connect, create_slot_if_not_exists, identify_system, recv_wal,
    send_status, start_replication,
};

const UDP_TOKEN: Token = Token(0);
const WAKE_TOKEN: Token = Token(1);
const MAX_DATAGRAMS: usize = 16;

// ---------------------------------------------------------------------------
// TLS: skip cert verification (private overlay network)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct AcceptAnyCert;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer,
        _intermediates: &[rustls::pki_types::CertificateDer],
        _server_name: &rustls::pki_types::ServerName,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _msg: &[u8],
        _cert: &rustls::pki_types::CertificateDer,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _msg: &[u8],
        _cert: &rustls::pki_types::CertificateDer,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn make_client_config() -> ClientConfig {
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert))
        .with_no_client_auth();
    let quic =
        quinn_proto::crypto::rustls::QuicClientConfig::try_from(tls).expect("quic client config");
    let mut cfg = ClientConfig::new(Arc::new(quic));
    let mut tc = quinn_proto::TransportConfig::default();
    tc.keep_alive_interval(Some(Duration::from_secs(5)));
    cfg.transport_config(Arc::new(tc));
    cfg
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut pg_port: u16 = 5432;
    let mut sink_addr = String::new();
    let mut slot = "wal_sink".to_owned();

    let mut args = std::env::args().skip(1);
    loop {
        match args.next().as_deref() {
            None => break,
            Some("--pg-port") => match args.next().as_deref() {
                Some(v) => match v.parse() {
                    Ok(n) => pg_port = n,
                    Err(_) => {
                        eprintln!("error: --pg-port must be a number");
                        std::process::exit(1);
                    }
                },
                None => {
                    eprintln!("error: --pg-port requires a value");
                    std::process::exit(1);
                }
            },
            Some("--sink-addr") => match args.next() {
                Some(v) => sink_addr = v,
                None => {
                    eprintln!("error: --sink-addr requires a value");
                    std::process::exit(1);
                }
            },
            Some("--slot") => match args.next() {
                Some(v) => slot = v,
                None => {
                    eprintln!("error: --slot requires a value");
                    std::process::exit(1);
                }
            },
            Some(other) => {
                eprintln!("error: unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }
    if sink_addr.is_empty() {
        eprintln!("error: --sink-addr is required");
        std::process::exit(1);
    }

    run_forever(pg_port, sink_addr, slot);
}

// ---------------------------------------------------------------------------
// Forwarder loop
// ---------------------------------------------------------------------------

fn run_forever(pg_port: u16, sink_addr: String, slot: String) {
    let mut backoff = Duration::from_millis(500);
    loop {
        match forward_once(pg_port, &sink_addr, &slot) {
            Ok(()) => {
                eprintln!("wal-forwarder: connection closed, reconnecting");
                backoff = Duration::from_millis(500);
            }
            Err(e) => {
                eprintln!("wal-forwarder: {e}");
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

fn forward_once(
    pg_port: u16,
    sink_addr: &str,
    slot: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Build poll + waker first so we can wake the QUIC pump from the
    // pg_reader thread the instant a frame is queued.
    let addr: std::net::SocketAddr = sink_addr.parse()?;
    let mut poll = Poll::new()?;
    let waker = Arc::new(mio::Waker::new(poll.registry(), WAKE_TOKEN)?);

    let (wal_tx, wal_rx) = mpsc::sync_channel::<(Lsn, Vec<u8>)>(8);
    let (ack_tx, ack_rx) = mpsc::sync_channel::<Lsn>(8);
    // Capacity 1: pg reader sends the timeline right after IDENTIFY_SYSTEM,
    // before the pump has necessarily connected; buffered send never blocks.
    let (timeline_tx, timeline_rx) = mpsc::sync_channel::<u32>(1);

    let slot_owned = slot.to_owned();
    let pump_waker = waker.clone();
    let pg_handle = thread::spawn(move || {
        pg_reader_thread(
            pg_port,
            &slot_owned,
            wal_tx,
            ack_rx,
            pump_waker,
            timeline_tx,
        )
    });

    quic_pump(addr, wal_rx, ack_tx, timeline_rx, &mut poll)?;

    match pg_handle.join() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string().into()),
        Err(_) => Err("pg reader thread panicked".into()),
    }
}

// ---------------------------------------------------------------------------
// Sync QUIC client pump
// ---------------------------------------------------------------------------

/// Drive a single QUIC connection to the sink:
///   * Open one bidi stream.
///   * Send the hello preamble frame `[u32 BE 5][b'h'][u32 BE timeline]`.
///   * For each `(lsn, payload)` from `wal_rx`, send `[u32 BE len][payload]`.
///   * Parse 8-byte BE ACKs from the recv side; forward each to `ack_tx`.
///
/// Returns when `wal_rx` is closed or an unrecoverable error occurs.
fn quic_pump(
    sink_addr: std::net::SocketAddr,
    wal_rx: mpsc::Receiver<(Lsn, Vec<u8>)>,
    ack_tx: mpsc::SyncSender<Lsn>,
    timeline_rx: mpsc::Receiver<u32>,
    poll: &mut Poll,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Bind ephemeral local socket matching the sink address family.
    let local: std::net::SocketAddr = if sink_addr.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    let mut socket = UdpSocket::bind(local)?;
    poll.registry()
        .register(&mut socket, UDP_TOKEN, Interest::READABLE)?;

    // Build endpoint as client-only.
    let endpoint_config = Arc::new(EndpointConfig::default());
    let server_config: Option<Arc<ServerConfig>> = None;
    let mut endpoint = quinn_proto::Endpoint::new(endpoint_config, server_config, true, None);

    let client_config = make_client_config();
    let now = Instant::now();
    let (handle, mut conn) = endpoint.connect(now, client_config, sink_addr, "beyond-pg-sink")?;

    let mut stream: Option<StreamId> = None;
    // Outgoing frames pending stream write (each is one full framed payload).
    let mut pending_writes: VecDeque<Vec<u8>> = VecDeque::new();
    // Inbound bytes assembled from the recv stream — parsed into 8-byte ACKs.
    let mut rx_buf = BytesMut::with_capacity(64 * 1024);
    let mut udp_recv = vec![0u8; 64 * 1024];
    let mut send_buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut events = Events::with_capacity(8);
    // Timeline received from pg_reader_thread. Consumed once to build the
    // hello preamble frame that must be sent before any WAL frames.
    let mut timeline_rx_opt: Option<mpsc::Receiver<u32>> = Some(timeline_rx);
    let mut hello_queued = false;

    eprintln!("wal-forwarder: QUIC connecting to {sink_addr}");

    loop {
        // Try to queue the hello preamble as soon as the stream is open and
        // the pg reader has sent the timeline.
        if stream.is_some()
            && !hello_queued
            && let Some(rx) = &timeline_rx_opt
        {
            match rx.try_recv() {
                Ok(tl) => {
                    // Hello frame: [u32 BE 5][b'h'][u32 BE timeline]
                    let mut frame = Vec::with_capacity(9);
                    frame.extend_from_slice(&5u32.to_be_bytes());
                    frame.push(b'h');
                    frame.extend_from_slice(&tl.to_be_bytes());
                    // push_front so it is sent before any WAL frames.
                    pending_writes.push_front(frame);
                    hello_queued = true;
                    timeline_rx_opt = None;
                }
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err("timeline channel closed before hello sent".into());
                }
            }
        }

        // Pull WAL frames only after the hello has been queued, ensuring the
        // sink always receives the preamble first.
        if hello_queued {
            for _ in 0..32 {
                match wal_rx.try_recv() {
                    Ok((_lsn, payload)) => {
                        let mut frame = Vec::with_capacity(4 + payload.len());
                        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                        frame.extend_from_slice(&payload);
                        pending_writes.push_back(frame);
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
                }
            }
        }

        // Try to write any pending frames if we have a stream.
        if let Some(id) = stream {
            try_drain_writes(&mut conn, id, &mut pending_writes)?;
        }

        // Drain quinn-proto outbound packets onto the socket.
        loop {
            let now = Instant::now();
            send_buf.clear();
            match conn.poll_transmit(now, MAX_DATAGRAMS, &mut send_buf) {
                Some(t) => send_one(&socket, &send_buf, &t),
                None => break,
            }
        }

        // Compute timeout: min(conn timer, 1ms if writes pending).
        let now = Instant::now();
        let conn_to = conn
            .poll_timeout()
            .map(|t| t.saturating_duration_since(now));
        let work_pending = !pending_writes.is_empty();
        let timeout = match (conn_to, work_pending) {
            (Some(c), true) => Some(c.min(Duration::from_millis(1))),
            (Some(c), false) => Some(c),
            (None, true) => Some(Duration::from_millis(1)),
            (None, false) => None, // Waker will wake us when wal_rx has data.
        };

        poll.poll(&mut events, timeout)?;

        let now = Instant::now();
        conn.handle_timeout(now);

        // Drain UDP.
        loop {
            match socket.recv_from(&mut udp_recv) {
                Ok((n, addr)) => {
                    let data = BytesMut::from(&udp_recv[..n]);
                    send_buf.clear();
                    if let Some(ev) = endpoint.handle(now, addr, None, None, data, &mut send_buf) {
                        match ev {
                            DatagramEvent::ConnectionEvent(h, e) if h == handle => {
                                conn.handle_event(e);
                            }
                            DatagramEvent::ConnectionEvent(_, _) => {}
                            DatagramEvent::NewConnection(_) => {
                                // Client-only endpoint: should never see this.
                            }
                            DatagramEvent::Response(t) => {
                                let _ = socket.send_to(&send_buf[..t.size], t.destination);
                            }
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Box::new(e)),
            }
        }

        // Pump connection-side events.
        while let Some(ev) = conn.poll() {
            match ev {
                Event::Connected => {
                    // Open our bidi stream now that the handshake is done.
                    if stream.is_none() {
                        let mut streams = conn.streams();
                        if let Some(id) = streams.open(Dir::Bi) {
                            stream = Some(id);
                            eprintln!("wal-forwarder: QUIC connected to {sink_addr}");
                        }
                    }
                }
                Event::ConnectionLost { reason } => {
                    return Err(format!("connection lost: {reason}").into());
                }
                Event::Stream(StreamEvent::Readable { id }) => {
                    if stream == Some(id) {
                        read_acks(&mut conn, id, &mut rx_buf, &ack_tx)?;
                    }
                }
                Event::Stream(StreamEvent::Writable { id }) => {
                    if stream == Some(id) {
                        try_drain_writes(&mut conn, id, &mut pending_writes)?;
                    }
                }
                Event::HandshakeDataReady => {}
                _ => {}
            }
        }

        // Pump endpoint <-> connection events.
        while let Some(ee) = conn.poll_endpoint_events() {
            if let Some(ce) = endpoint.handle_event(handle, ee) {
                conn.handle_event(ce);
            }
        }
    }
}

/// Try to drain pending payloads onto the stream. Stops when `Blocked` or
/// the queue is empty. Partial chunk writes are handled by trimming the
/// remainder. Returns `Err` on any non-`Blocked` write error.
fn try_drain_writes(
    conn: &mut Connection,
    id: StreamId,
    pending: &mut VecDeque<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use quinn_proto::WriteError;

    while let Some(buf) = pending.front_mut() {
        let mut send = conn.send_stream(id);
        match send.write(buf) {
            Ok(n) if n == buf.len() => {
                pending.pop_front();
            }
            Ok(n) => {
                buf.drain(..n);
                break;
            }
            Err(WriteError::Blocked) => break,
            Err(e) => {
                return Err(format!("stream write: {e:?}").into());
            }
        }
    }
    Ok(())
}

/// Read whatever's available on the ack stream and dispatch 8-byte ACKs.
fn read_acks(
    conn: &mut Connection,
    id: StreamId,
    rx_buf: &mut BytesMut,
    ack_tx: &mpsc::SyncSender<Lsn>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use quinn_proto::{ReadError, ReadableError};

    let mut recv = conn.recv_stream(id);
    let mut chunks = match recv.read(true) {
        Ok(c) => c,
        Err(ReadableError::ClosedStream) => return Ok(()),
        Err(e) => return Err(format!("read open: {e:?}").into()),
    };
    loop {
        match chunks.next(usize::MAX) {
            Ok(Some(chunk)) => rx_buf.put_slice(&chunk.bytes),
            Ok(None) => break,
            Err(ReadError::Blocked) => break,
            Err(ReadError::Reset(_)) => break,
        }
    }
    let _ = chunks.finalize();

    let mut consumed = 0usize;
    while rx_buf.len() - consumed >= 8 {
        let ack = Lsn::from_be_bytes(rx_buf[consumed..consumed + 8].try_into().unwrap());
        if ack_tx.send(ack).is_err() {
            return Err("ack channel closed".into());
        }
        consumed += 8;
    }
    if consumed > 0 {
        rx_buf.advance(consumed);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Postgres reader thread
// ---------------------------------------------------------------------------

fn pg_reader_thread(
    pg_port: u16,
    slot: &str,
    wal_tx: mpsc::SyncSender<(Lsn, Vec<u8>)>,
    ack_rx: mpsc::Receiver<Lsn>,
    waker: Arc<mio::Waker>,
    timeline_tx: mpsc::SyncSender<u32>,
) -> Result<(), RecvError> {
    let mut conn = connect("127.0.0.1", pg_port, "postgres", slot, None)?;
    conn.set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(RecvError::Io)?;

    // Identify the server to get the replication timeline. Send it to the
    // QUIC pump immediately so it can build the preamble hello frame before
    // WAL streaming begins. The sync_channel(1) means this send never blocks.
    let (timeline, sys_lsn) = identify_system(&mut conn)?;
    let _ = timeline_tx.send(timeline);

    let start_lsn = match create_slot_if_not_exists(&mut conn, slot)? {
        Some(lsn) if lsn != Lsn::ZERO => lsn,
        _ => sys_lsn,
    };
    start_replication(&mut conn, slot, start_lsn)?;
    eprintln!("wal-forwarder: replication started from {start_lsn}");

    let mut write_lsn = start_lsn;
    let mut flush_lsn = start_lsn;
    let mut last_status = Instant::now();

    loop {
        // Drain any backlog of acks first.
        while let Ok(lsn) = ack_rx.try_recv() {
            flush_lsn = lsn;
            write_lsn = lsn;
        }

        match recv_wal(&mut conn) {
            Ok(WalMsg::XLogData {
                start_lsn,
                end_lsn,
                wal_data,
            }) => {
                // Reconstruct the QUIC frame body: b'w' + start_lsn + end_lsn + zeroed_ts + data.
                // The timestamp is zeroed; the sink uses its own clock.
                let mut body = Vec::with_capacity(1 + 8 + 8 + 8 + wal_data.len());
                body.push(b'w');
                body.extend_from_slice(&start_lsn.to_be_bytes());
                body.extend_from_slice(&end_lsn.to_be_bytes());
                body.extend_from_slice(&[0u8; 8]);
                body.extend_from_slice(&wal_data);
                if wal_tx.send((start_lsn, body)).is_err() {
                    return Ok(());
                }
                // Kick the pump thread out of mio.poll() so the new frame
                // is picked up immediately rather than after the idle timeout.
                let _ = waker.wake();
                // Block on the corresponding ACK before reading the next WAL
                // chunk. This keeps the forwarder strictly request/reply per
                // frame, providing back-pressure all the way to Postgres.
                match ack_rx.recv() {
                    Ok(lsn) => {
                        flush_lsn = lsn;
                        write_lsn = lsn;
                        send_status(&mut conn, write_lsn, flush_lsn)?;
                        last_status = Instant::now();
                    }
                    Err(_) => return Ok(()),
                }
            }
            Ok(WalMsg::Keepalive { reply_needed, .. }) => {
                if reply_needed || last_status.elapsed() > Duration::from_secs(10) {
                    send_status(&mut conn, write_lsn, flush_lsn)?;
                    last_status = Instant::now();
                }
            }
            Err(RecvError::Io(e))
                if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock =>
            {
                send_status(&mut conn, write_lsn, flush_lsn)?;
                last_status = Instant::now();
            }
            Err(e) => return Err(e),
        }
    }
}
