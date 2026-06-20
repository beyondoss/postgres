//! Substrate vsock "guest ready" handshake for `beyond-pg-init`.
//!
//! After spawning Firecracker, instd waits ~30s for the guest to report ready
//! over vsock; without it the VM's create never completes ("guest not ready:
//! timeout"). The contract is a single self-described frame on the host vsock
//! channel — we speak it directly here so this repo builds standalone (no
//! dependency on the Beyond workspace):
//!
//! ```text
//! connect: AF_VSOCK  cid=2 (host)  port=52
//! frame:   [len: u32 BE = 1 + payload][type: u8 = 0x81 Ready][payload: MessagePack]
//!          payload = rmp_serde::to_vec_named(ReadyPayload)   // string keys
//! ```
//!
//! instd marks the guest ready the moment it reads the `Ready` frame; we then
//! hold the connection open for the VM's lifetime so the host keeps seeing the
//! guest as present. The frame layout mirrors `rustlib/vsock-protocol` in the
//! Beyond repo — kept honest by `tests::ready_frame_is_stable` below; if instd
//! ever changes the wire, that fixture must change in lockstep.
//!
//! Soft-fail throughout: no AF_VSOCK (e.g. a Docker test box) or a failed
//! connect is logged and the thread exits — never fatal. The supervise loop
//! owns shutdown via signalfd (instd also sends SIGTERM), so this thread never
//! powers the VM off.

use serde::Serialize;

/// Host vsock context id (`VMADDR_CID_HOST`).
const HOST_CID: u32 = 2;
/// Substrate vsock port instd listens on (`vsock_protocol::VSOCK_PORT`).
const SUBSTRATE_PORT: u32 = 52;
/// `Ready` message discriminator (`vsock_protocol::MessageType::Ready`).
const MSG_READY: u8 = 0x81;

/// Agent → host "ready after boot" payload. A field-compatible subset of
/// `vsock_protocol::ReadyPayload` — only the always-present fields; the rest are
/// `skip_serializing_if`/`default` on the host side and so may be omitted.
#[derive(Serialize)]
struct ReadyPayload {
    agent_version: String,
    boot_time_ms: u64,
    reconnect: bool,
}

/// Encode the `Ready` frame: `[len: u32 BE][type][MessagePack payload]`.
/// Length covers the type byte + payload (not the 4 length bytes themselves).
fn encode_ready_frame(payload: &ReadyPayload) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let body = rmp_serde::to_vec_named(payload)?;
    let mut frame = ((body.len() as u32) + 1).to_be_bytes().to_vec();
    frame.push(MSG_READY);
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Spawn the dedicated `substrate-vsock` thread that performs the guest-ready
/// handshake and then keeps the connection alive for the VM's lifetime.
///
/// Returns immediately; everything happens on the spawned thread. Soft-fail
/// throughout: a failed spawn or a failed connect is logged, never fatal.
pub fn spawn_handshake() {
    let builder = std::thread::Builder::new().name("substrate-vsock".to_string());
    if let Err(e) = builder.spawn(run) {
        eprintln!("[init] WARNING: failed to spawn substrate-vsock thread: {e}");
    }
}

fn run() {
    // Current-thread runtime: this thread only hosts the single vsock
    // connection + keep-alive read, so a multi-thread scheduler would just
    // waste workers.
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[init] WARNING: substrate-vsock tokio runtime build failed: {e}");
            return;
        }
    };
    runtime.block_on(handshake());
}

async fn handshake() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_vsock::{VsockAddr, VsockStream};

    let payload = ReadyPayload {
        agent_version: format!("beyond-pg-init/{}", env!("CARGO_PKG_VERSION")),
        boot_time_ms: read_uptime_ms(),
        reconnect: false,
    };
    let frame = match encode_ready_frame(&payload) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[init] WARNING: encode Ready frame failed: {e}");
            return;
        }
    };

    let mut conn = match VsockStream::connect(VsockAddr::new(HOST_CID, SUBSTRATE_PORT)).await {
        Ok(c) => c,
        Err(e) => {
            // Soft-fail: no AF_VSOCK (Docker tests) or no host listener just
            // means there's no substrate to report to. Let the thread exit.
            eprintln!(
                "[init] WARNING: substrate vsock connect failed; guest-ready unreported: {e}"
            );
            return;
        }
    };
    if let Err(e) = conn.write_all(&frame).await {
        eprintln!("[init] WARNING: substrate Ready write failed: {e}");
        return;
    }
    let _ = conn.flush().await;
    eprintln!("[init] substrate vsock handshake complete; guest reported ready");

    // Hold the connection open for the VM's lifetime so the host keeps seeing
    // the guest as present. We don't serve admin exec/ping here (the Postgres VM
    // is reached over the VPC via pgbouncer, not the admin channel); inbound
    // bytes (ReadyAck, heartbeats) are read and ignored. EOF/err → thread exits.
    let mut buf = [0u8; 512];
    loop {
        match conn.read(&mut buf).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
}

/// Milliseconds since kernel boot, from `/proc/uptime`. Best-effort (0 on any
/// read/parse failure) — it's only telemetry in the Ready payload.
fn read_uptime_ms() -> u64 {
    std::fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_owned))
        .and_then(|s| s.parse::<f64>().ok())
        .map(|secs| (secs * 1000.0) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the on-wire `Ready` frame so it can't silently drift from instd's
    /// `rustlib/vsock-protocol` decoder: a 4-byte BE length covering type +
    /// payload, the `0x81` type byte, then a MessagePack *map* (string keys)
    /// carrying the payload fields.
    #[test]
    fn ready_frame_is_stable() {
        let p = ReadyPayload {
            agent_version: "beyond-pg-init/0.1.0".to_string(),
            boot_time_ms: 1234,
            reconnect: false,
        };
        let frame = encode_ready_frame(&p).unwrap();

        let len = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4, "length covers type + payload");
        assert_eq!(frame[4], MSG_READY, "type byte is Ready (0x81)");

        // Body round-trips to the named fields (string-keyed map = to_vec_named).
        let body = &frame[5..];
        let v: serde_json::Value = rmp_serde::from_slice(body).unwrap();
        let obj = v.as_object().expect("Ready payload must be a map");
        assert!(obj.contains_key("agent_version"));
        assert!(obj.contains_key("boot_time_ms"));
        assert!(obj.contains_key("reconnect"));
    }
}
