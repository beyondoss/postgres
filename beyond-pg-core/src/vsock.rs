//! Vsock port + frame type definitions.
//!
//! Keep in sync with `beyond/boxes/vsock-protocol/src/lib.rs`. Only constants
//! and serde type definitions live here so this module pulls in no
//! framing-codec dependency.

/// Host log pipeline port (agent → host).
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
