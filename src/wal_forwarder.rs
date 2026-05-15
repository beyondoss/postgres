//! Forwards WAL from the local Postgres primary to a remote QUIC sink.
//!
//! Architecture:
//!   - A blocking thread (spawn_blocking) holds the physical replication
//!     TCP connection to Postgres. It reads XLogData frames and forwards
//!     them to the async pump via `wal_tx`. It receives flush acks back
//!     via `ack_rx` and turns them into StandbyStatusUpdate messages.
//!   - The async pump owns the QUIC stream. It reads frames from `wal_rx`,
//!     writes them length-prefixed onto the bidi stream, reads the 8-byte
//!     flush-LSN ack, and forwards it to `ack_tx`.

use std::sync::Arc;

use tracing::{info, warn};

use wal_proto::{
    Lsn, RecvError, WalMsg, connect, create_slot_if_not_exists, identify_system, recv_wal,
    send_status, start_replication,
};

// ---------------------------------------------------------------------------
// TLS: accept any cert (private overlay network)
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

fn make_client_config() -> quinn::ClientConfig {
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert))
        .with_no_client_auth();
    quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("quic client config"),
    ))
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run forever: connect to Postgres, connect to sink via QUIC, forward WAL.
pub async fn run(sink_url: String, slot: String, pg_port: u16) {
    // Install ring as the default rustls provider. Idempotent across modules.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut backoff = std::time::Duration::from_secs(1);
    loop {
        match forward_once(&sink_url, &slot, pg_port).await {
            Ok(()) => {
                info!("wal forwarder: connection closed");
                backoff = std::time::Duration::from_secs(1);
            }
            Err(e) => {
                warn!("wal forwarder: {e}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
            }
        }
    }
}

async fn forward_once(
    sink_url: &str,
    slot: &str,
    pg_port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (sink_host, sink_port) = parse_sink_addr(sink_url)?;

    // pg→async (WAL frames) and async→pg (flush LSN acks).
    let (wal_tx, mut wal_rx) = tokio::sync::mpsc::channel::<(Lsn, Vec<u8>)>(8);
    let (ack_tx, ack_rx) = tokio::sync::mpsc::channel::<Lsn>(8);

    // Spawn the Postgres reader thread (sync TCP I/O).
    let slot_owned = slot.to_owned();
    let pg_handle =
        tokio::task::spawn_blocking(move || pg_reader_thread(pg_port, &slot_owned, wal_tx, ack_rx));

    // Resolve sink host. Prefer the first IPv4/IPv6 address.
    let addr = tokio::net::lookup_host((sink_host.as_str(), sink_port))
        .await?
        .next()
        .ok_or("sink host did not resolve")?;
    let client_config = make_client_config();
    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);
    let conn = endpoint.connect(addr, "beyond-pg-sink")?.await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    info!("wal forwarder: QUIC connected to {addr}");

    // Pump: WAL → QUIC → ACK.
    loop {
        let Some((_start_lsn, payload)) = wal_rx.recv().await else {
            break;
        };

        let len_bytes = (payload.len() as u32).to_be_bytes();
        send.write_all(&len_bytes).await?;
        send.write_all(&payload).await?;

        let mut ack_buf = [0u8; 8];
        recv.read_exact(&mut ack_buf).await?;
        let flush_lsn = Lsn::from_be_bytes(ack_buf);

        if ack_tx.send(flush_lsn).await.is_err() {
            break;
        }
    }

    match pg_handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(Box::new(e)),
        Err(e) => Err(Box::new(e)),
    }
}

// ---------------------------------------------------------------------------
// Postgres reader thread (sync)
// ---------------------------------------------------------------------------

fn pg_reader_thread(
    pg_port: u16,
    slot: &str,
    wal_tx: tokio::sync::mpsc::Sender<(Lsn, Vec<u8>)>,
    mut ack_rx: tokio::sync::mpsc::Receiver<Lsn>,
) -> Result<(), RecvError> {
    let mut conn = connect("127.0.0.1", pg_port, "postgres", slot, None)?;
    conn.set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .map_err(RecvError::Io)?;

    let start_lsn = match create_slot_if_not_exists(&mut conn, slot)? {
        Some(lsn) if lsn != Lsn::ZERO => lsn,
        _ => identify_system(&mut conn).map(|(_, lsn)| lsn)?,
    };
    start_replication(&mut conn, slot, start_lsn)?;

    let mut write_lsn = start_lsn;
    let mut flush_lsn = start_lsn;
    let mut last_status = std::time::Instant::now();

    loop {
        // Drain any pending ACKs that arrived during prior recvs.
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
                if wal_tx.blocking_send((start_lsn, body)).is_err() {
                    return Ok(());
                }

                // Wait for the matching ACK.
                match ack_rx.blocking_recv() {
                    Some(lsn) => {
                        flush_lsn = lsn;
                        write_lsn = lsn;
                        send_status(&mut conn, write_lsn, flush_lsn)?;
                        last_status = std::time::Instant::now();
                    }
                    None => return Ok(()),
                }
            }
            Ok(WalMsg::Keepalive { reply_needed, .. }) => {
                if reply_needed || last_status.elapsed() > std::time::Duration::from_secs(10) {
                    send_status(&mut conn, write_lsn, flush_lsn)?;
                    last_status = std::time::Instant::now();
                }
                // Exit if the async pump loop has been dropped (task cancelled or
                // supervisor shut down).  Without this check the blocking thread loops
                // on keepalives indefinitely, preventing the tokio runtime from
                // completing its shutdown and causing a SIGKILL instead of a clean exit.
                if wal_tx.is_closed() {
                    return Ok(());
                }
            }
            Err(RecvError::Io(e))
                if e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::WouldBlock =>
            {
                send_status(&mut conn, write_lsn, flush_lsn)?;
                last_status = std::time::Instant::now();
                if wal_tx.is_closed() {
                    return Ok(());
                }
            }
            Err(e) => return Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// URL parsing
// ---------------------------------------------------------------------------

fn parse_sink_addr(url: &str) -> Result<(String, u16), Box<dyn std::error::Error + Send + Sync>> {
    let url = url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let (host, port_str) = url.rsplit_once(':').ok_or("wal_sink URL missing port")?;
    let port_str = port_str.split('/').next().unwrap_or(port_str);
    let port: u16 = port_str.parse()?;
    Ok((host.to_owned(), port))
}
