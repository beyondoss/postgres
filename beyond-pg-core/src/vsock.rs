//! Vsock port + frame type definitions.
//!
//! Keep in sync with `beyond/boxes/vsock-protocol/src/lib.rs`. Only constants
//! and serde type definitions live here so this module pulls in no
//! framing-codec dependency.

/// Substrate vsock port `instd` listens on (`vsock_protocol::VSOCK_PORT`).
///
/// `instd` accepts exactly one connection here per VM and runs it for the VM's
/// lifetime; PID 1 (`beyond-pg-init`) owns it (guest-ready handshake +
/// heartbeats). Workload log frames are multiplexed onto this same connection
/// by PID 1 after the supervisor hands them over via [`LOG_SINK_UNIX_PATH`].
pub const VSOCK_PORT: u32 = 52;

/// Host CID — guests connect to this CID to reach the host.
pub const HOST_CID: u32 = 2;

/// `beyond-pg` supervisor control RPC port (host → guest).
pub const RPC_PORT: u32 = 5430;

/// `beyond-pg-init` lifecycle port (host → guest): upgrade / shutdown / status.
/// Owned by init and not handed off across supervisor swaps.
pub const LIFECYCLE_PORT: u32 = 5429;

/// Max bytes of a single supervised-process log line before truncation.
pub const MAX_USER_PROCESS_LINE_BYTES: usize = 256 * 1024;

/// In-VM unix socket the supervisor (`beyond-pg`) connects to in order to hand
/// already-framed `AppMessage` log frames to PID 1 (`beyond-pg-init`).
///
/// # Why this exists
///
/// `instd` accepts exactly **one** vsock connection per VM on the substrate
/// port ([`VSOCK_PORT`]) and runs it for the VM's lifetime. PID 1 owns that
/// connection (guest-ready handshake + heartbeats). A *second* connection from
/// the supervisor straight to port 52 is never `accept()`ed by `instd`, so its
/// frames are silently dropped — which is exactly why workload logs used to
/// vanish. The supervisor therefore forwards log frames to PID 1 over this
/// local unix socket, and PID 1 relays them, verbatim, onto the single live
/// substrate connection (multiplexed with heartbeat replies).
///
/// Frames on this socket are the complete substrate wire frames produced by
/// `encode_log_frame` — `[len: u32 BE][0x20 AppMessage][msgpack payload]` — so
/// PID 1 is a dumb relay and the AppMessage encoding lives in exactly one place.
pub const LOG_SINK_UNIX_PATH: &str = "/run/beyond-pg/logsink.sock";

/// Which output stream a log line came from.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecStream {
    Stdout,
    Stderr,
}

/// Wire payload for a single log line from a supervised process.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct UserProcessStreamDataPayload {
    pub stream: ExecStream,
    pub line: String,
    pub truncated: bool,
    /// Zero UUID for long-running supervised processes.
    #[serde(default)]
    pub execution_id: String,
}
