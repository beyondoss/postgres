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
/// Message discriminators (`vsock_protocol::MessageType`).
const MSG_READY: u8 = 0x81; // Agent → host: ready after boot.
const MSG_HEARTBEAT: u8 = 0x02; // Host → agent: liveness probe.
const MSG_HEARTBEAT_RESP: u8 = 0x82; // Agent → host: heartbeat reply.
const MSG_SHUTDOWN: u8 = 0x04; // Host → agent: shutdown requested.
/// Frame length ceiling — sanity bound so a corrupt length can't allocate wild.
const MAX_FRAME: u32 = 16 * 1024 * 1024;

/// Agent → host "ready after boot" payload. A field-compatible subset of
/// `vsock_protocol::ReadyPayload` — only the always-present fields; the rest are
/// `skip_serializing_if`/`default` on the host side and so may be omitted.
#[derive(Serialize)]
struct ReadyPayload {
    agent_version: String,
    boot_time_ms: u64,
    reconnect: bool,
}

/// `vsock_protocol::HeartbeatPayload` — echoed back in our heartbeat reply.
#[derive(Serialize)]
struct HeartbeatPayload {
    timestamp: u64,
}

/// Frame a message: `[len: u32 BE = 1 + payload][type][MessagePack payload]`.
fn encode_frame(msg_type: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = ((body.len() as u32) + 1).to_be_bytes().to_vec();
    frame.push(msg_type);
    frame.extend_from_slice(body);
    frame
}

/// Encode the `Ready` frame.
fn encode_ready_frame(payload: &ReadyPayload) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    Ok(encode_frame(MSG_READY, &rmp_serde::to_vec_named(payload)?))
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

    // Keep the connection alive for the VM's lifetime AND answer instd's
    // heartbeats — instd pings every 10s and marks the VM `Degraded (guest
    // disconnected)` after 30s of silence, which disrupts VPC routing to the
    // guest. We parse frames and reply HeartbeatResp to each Heartbeat; other
    // messages (ReadyAck, admin/session — we don't serve those here) are
    // ignored; Shutdown ends the loop (beyond-pg-init's supervise owns the
    // actual poweroff via signalfd). EOF/err → thread exits.
    let mut len_buf = [0u8; 4];
    loop {
        if conn.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf);
        if len == 0 || len > MAX_FRAME {
            eprintln!("[init] substrate vsock: bad frame length {len}; closing");
            break;
        }
        let mut frame = vec![0u8; len as usize];
        if conn.read_exact(&mut frame).await.is_err() {
            break;
        }
        match frame[0] {
            MSG_HEARTBEAT => {
                let body =
                    rmp_serde::to_vec_named(&HeartbeatPayload { timestamp: 0 }).unwrap_or_default();
                if conn
                    .write_all(&encode_frame(MSG_HEARTBEAT_RESP, &body))
                    .await
                    .is_err()
                {
                    break;
                }
                let _ = conn.flush().await;
            }
            MSG_SHUTDOWN => {
                eprintln!("[init] substrate requested shutdown; vsock loop exiting");
                break;
            }
            _ => {} // ReadyAck etc. — nothing to do here.
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
