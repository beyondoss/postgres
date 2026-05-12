//! Phase 2 QUIC transport for WAL streaming (sync, sans-I/O `quinn-proto`).
//!
//! Single OS thread, single mio-registered UDP socket, single
//! `quinn_proto::Endpoint`. We accept exactly one QUIC connection at a time;
//! when a new connection arrives all existing connections are closed first.
//! On that connection we accept the first bidirectional stream and use it to
//! receive length-prefixed frames and reply with 8-byte ACKs (flush LSN).
//!
//! Protocol on the bidi stream:
//!   first frame: `[u32 BE 5][0x68 'h'][u32 BE timeline]`  (preamble)
//!   subsequent:  `[u32 BE len][0x77 'w' + 8 start_lsn + 8 end_lsn + 8 ts + data]`
//!   send:        `[u64 BE flush_lsn]`

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token};
use quinn_proto::{
    Connection, ConnectionHandle, DatagramEvent, Dir, EndpointConfig, Event, ServerConfig,
    StreamEvent, StreamId, VarInt,
};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::send_one;
use crate::wal_recv::{Lsn, WalWriter};

const UDP_TOKEN: Token = Token(0);
const MAX_DATAGRAMS: usize = 16;
/// Maximum allowed frame payload: one full WAL segment plus header overhead.
/// A peer claiming more than this is misbehaving; reject and close.
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024 + 256;

pub fn make_server_config() -> Result<ServerConfig, Box<dyn std::error::Error + Send + Sync>> {
    let cert = generate_simple_self_signed(vec!["beyond-pg-sink".to_string()])?;
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(cert.signing_key.serialize_der())
        .map_err(|e| format!("private key: {e}"))?;
    let mut sc = ServerConfig::with_single_cert(vec![cert_der], key_der)?;
    let mut tc = quinn_proto::TransportConfig::default();
    // Keep the connection alive across short application stalls (fsync). The
    // peer's idle timeout default is 30s; we issue a PING every 5s.
    tc.keep_alive_interval(Some(std::time::Duration::from_secs(5)));
    sc.transport_config(Arc::new(tc));
    Ok(sc)
}

/// Per-connection bookkeeping: stream state and a partial frame buffer.
struct ConnState {
    /// The single bidi stream we use for the protocol (None until accepted).
    stream: Option<StreamId>,
    /// Buffered, not-yet-consumed bytes from the recv side of `stream`.
    /// Used to assemble length-prefixed frames across multiple reads.
    rx_buf: BytesMut,
    /// WAL writer scoped to this connection.
    writer: WalWriter,
    /// Set when the preamble hello frame has been received and the timeline
    /// applied to `writer`. WAL frames are rejected until this is true.
    timeline_ready: bool,
    /// Connection is closed/lost; remove on next sweep.
    dead: bool,
}

impl ConnState {
    fn new(dir: &std::path::Path) -> Self {
        Self {
            stream: None,
            rx_buf: BytesMut::with_capacity(64 * 1024),
            // Timeline 0 is a sentinel; set_timeline() is called from the
            // hello preamble frame before any WAL writes occur.
            writer: WalWriter::new(dir, 0),
            timeline_ready: false,
            dead: false,
        }
    }
}

/// Block until an unrecoverable error: accept QUIC connections and stream WAL into `dir`.
pub fn run_quic_server(
    port: u16,
    dir: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // rustls needs a default crypto provider installed. Idempotent — error means
    // someone else already installed one.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let server_config = Arc::new(make_server_config()?);
    let endpoint_config = Arc::new(EndpointConfig::default());
    let mut endpoint = quinn_proto::Endpoint::new(endpoint_config, Some(server_config), true, None);

    let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse()?;
    let mut socket = UdpSocket::bind(bind_addr)?;
    let mut poll = Poll::new()?;
    poll.registry()
        .register(&mut socket, UDP_TOKEN, Interest::READABLE)?;

    eprintln!("quic: listening on udp 0.0.0.0:{port}");

    let mut events = Events::with_capacity(16);
    let mut conns: HashMap<ConnectionHandle, (Connection, ConnState)> = HashMap::new();
    let mut recv_buf = vec![0u8; 64 * 1024];
    let mut send_buf: Vec<u8> = Vec::with_capacity(64 * 1024);

    loop {
        // Compute the next deadline from per-connection poll_timeout values.
        let now = Instant::now();
        let timeout = conns
            .values_mut()
            .filter_map(|(c, _)| c.poll_timeout())
            .min()
            .map(|t| t.saturating_duration_since(now));

        if let Err(e) = poll.poll(&mut events, timeout) {
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(Box::new(e));
        }

        let now = Instant::now();

        // 1) Drain UDP socket.
        loop {
            match socket.recv_from(&mut recv_buf) {
                Ok((n, addr)) => {
                    let data = BytesMut::from(&recv_buf[..n]);
                    send_buf.clear();
                    let ev = endpoint.handle(now, addr, None, None, data, &mut send_buf);
                    match ev {
                        Some(DatagramEvent::ConnectionEvent(handle, ev)) => {
                            if let Some((conn, _)) = conns.get_mut(&handle) {
                                conn.handle_event(ev);
                            }
                        }
                        Some(DatagramEvent::NewConnection(incoming)) => {
                            // Enforce single-connection semantics: close all
                            // existing connections before accepting the new one.
                            // Their CLOSE frames are queued into Connection state
                            // and sent during the transmit drain in step 3 of
                            // this same event loop iteration.
                            for (_, (conn, st)) in conns.iter_mut() {
                                conn.close(
                                    now,
                                    VarInt::from_u32(0),
                                    Bytes::from_static(b"replaced"),
                                );
                                st.dead = true;
                            }
                            send_buf.clear();
                            match endpoint.accept(incoming, now, &mut send_buf, None) {
                                Ok((handle, conn)) => {
                                    eprintln!("quic: connection from {}", conn.remote_address());
                                    conns.insert(handle, (conn, ConnState::new(&dir)));
                                }
                                Err(e) => {
                                    eprintln!("quic: accept error: {}", e.cause);
                                    if !send_buf.is_empty()
                                        && let Some(resp) = e.response
                                    {
                                        let _ = socket
                                            .send_to(&send_buf[..resp.size], resp.destination);
                                    }
                                }
                            }
                        }
                        Some(DatagramEvent::Response(t)) => {
                            let _ = socket.send_to(&send_buf[..t.size], t.destination);
                        }
                        None => {}
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Box::new(e)),
            }
        }

        // 2) Drive timers.
        for (_, (conn, _)) in conns.iter_mut() {
            conn.handle_timeout(now);
        }

        // 3) Process events on every connection, then drain transmits.
        //    Looped until nothing changes so that an app-level write triggers
        //    further endpoint events that need re-pumping.
        loop {
            let mut made_progress = false;

            // App events
            let handles: Vec<ConnectionHandle> = conns.keys().copied().collect();
            for handle in handles {
                process_conn_events(&mut conns, handle, &mut made_progress);
            }

            // Endpoint <-> connection events
            for (handle, (conn, st)) in conns.iter_mut() {
                while let Some(ee) = conn.poll_endpoint_events() {
                    if ee.is_drained() {
                        st.dead = true;
                    }
                    if let Some(ce) = endpoint.handle_event(*handle, ee) {
                        conn.handle_event(ce);
                        made_progress = true;
                    }
                }
            }

            // Drain transmits
            for (_, (conn, _)) in conns.iter_mut() {
                loop {
                    send_buf.clear();
                    match conn.poll_transmit(now, MAX_DATAGRAMS, &mut send_buf) {
                        Some(t) => {
                            send_one(&socket, &send_buf, &t);
                            made_progress = true;
                        }
                        None => break,
                    }
                }
            }

            if !made_progress {
                break;
            }
        }

        // 4) Drop closed connections.
        conns.retain(|_, (_, st)| !st.dead);
    }
}

/// Pull events from a single connection and act on them. Sets `made_progress`
/// when any state mutation happens so the outer loop knows to re-run.
fn process_conn_events(
    conns: &mut HashMap<ConnectionHandle, (Connection, ConnState)>,
    handle: ConnectionHandle,
    made_progress: &mut bool,
) {
    let Some((conn, st)) = conns.get_mut(&handle) else {
        return;
    };

    while let Some(ev) = conn.poll() {
        *made_progress = true;
        match ev {
            Event::Connected => {}
            Event::HandshakeDataReady => {}
            Event::ConnectionLost { reason } => {
                eprintln!("quic: connection lost: {reason}");
                st.dead = true;
                return;
            }
            Event::Stream(StreamEvent::Opened { dir: Dir::Bi }) => {
                // Accept the (single) bidi stream the peer opens.
                if st.stream.is_none() {
                    let mut streams = conn.streams();
                    if let Some(id) = streams.accept(Dir::Bi) {
                        st.stream = Some(id);
                    }
                }
            }
            Event::Stream(StreamEvent::Readable { id }) => {
                if st.stream == Some(id) {
                    if let Err(e) = read_and_handle(conn, st, id) {
                        eprintln!("quic: stream handler: {e}");
                        st.dead = true;
                        return;
                    }
                }
            }
            // Writable / Finished / Stopped / Available / Opened(Uni): ignore.
            _ => {}
        }
    }
}

/// Drain all available bytes from `id` into `st.rx_buf`, then parse and
/// process every complete framed payload. Each WAL frame triggers an ACK
/// write back onto the same stream.
fn read_and_handle(conn: &mut Connection, st: &mut ConnState, id: StreamId) -> io::Result<()> {
    use quinn_proto::{ReadError, ReadableError};

    let mut recv = conn.recv_stream(id);
    let mut chunks = match recv.read(true) {
        Ok(c) => c,
        Err(ReadableError::ClosedStream) => return Ok(()),
        Err(e) => {
            return Err(io::Error::other(format!("read open: {e:?}")));
        }
    };
    let mut got_eof = false;
    loop {
        match chunks.next(usize::MAX) {
            Ok(Some(chunk)) => {
                st.rx_buf.put_slice(&chunk.bytes);
                // Guard: reject before the buffer grows beyond one max-size
                // frame plus a length header. The per-frame size check in the
                // parse loop catches oversized frames early, but a flooding
                // peer could still fill memory up to this bound.
                if st.rx_buf.len() > MAX_FRAME_SIZE + 4 {
                    return Err(io::Error::other(format!(
                        "rx_buf overrun ({} bytes): peer is flooding",
                        st.rx_buf.len()
                    )));
                }
            }
            Ok(None) => {
                got_eof = true;
                break;
            }
            Err(ReadError::Blocked) => break,
            Err(ReadError::Reset(_)) => {
                got_eof = true;
                break;
            }
        }
    }
    let _ = chunks.finalize();
    if got_eof {
        st.stream = None;
    }

    // Parse as many complete frames as possible from the buffer.
    let mut consumed = 0usize;
    while st.rx_buf.len() - consumed >= 4 {
        let len =
            u32::from_be_bytes(st.rx_buf[consumed..consumed + 4].try_into().unwrap()) as usize;
        // Reject before buffering: a peer claiming an impossibly large frame
        // would cause unbounded rx_buf growth if we just waited for the data.
        if len > MAX_FRAME_SIZE {
            return Err(io::Error::other(format!(
                "frame too large: {len} bytes (max {MAX_FRAME_SIZE})"
            )));
        }
        if st.rx_buf.len() - consumed - 4 < len {
            break;
        }
        let payload = &st.rx_buf[consumed + 4..consumed + 4 + len];
        match payload.first().copied() {
            Some(b'h') if payload.len() >= 5 => {
                // Preamble hello frame: [b'h'][u32 BE timeline].
                // Must arrive before any WAL frames; sets the segment timeline.
                let tl = u32::from_be_bytes(payload[1..5].try_into().unwrap());
                st.writer.set_timeline(tl);
                st.timeline_ready = true;
            }
            Some(b'w') if payload.len() >= 25 => {
                if !st.timeline_ready {
                    return Err(io::Error::other("WAL frame received before hello preamble"));
                }
                let start_lsn = Lsn::from_be_bytes(payload[1..9].try_into().unwrap());
                st.writer.write(start_lsn, &payload[25..])?;
                // ACK: 8-byte BE flush LSN.
                let mut send = conn.send_stream(id);
                let ack = st.writer.flush_lsn.to_be_bytes();
                match send.write(&ack) {
                    Ok(n) if n == ack.len() => {}
                    Ok(short) => {
                        return Err(io::Error::other(format!(
                            "short ACK write: {short}/{}",
                            ack.len()
                        )));
                    }
                    Err(e) => {
                        return Err(io::Error::other(format!("ack write: {e:?}")));
                    }
                }
            }
            _ => {
                eprintln!(
                    "quic: unexpected payload type {:02x}",
                    payload.first().copied().unwrap_or(0)
                );
            }
        }
        consumed += 4 + len;
    }
    if consumed > 0 {
        st.rx_buf.advance(consumed);
    }
    Ok(())
}
