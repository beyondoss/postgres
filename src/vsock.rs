//! Minimal vsock wire format — inlined from vsock-protocol to avoid a cross-repo
//! path dependency. Values are copied verbatim; keep in sync with
//! `beyond/boxes/vsock-protocol/src/lib.rs`.
//!
//! All items here are used on Linux; the dead_code lint fires on macOS/dev builds.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

/// Host log pipeline port (agent → host).
pub const VSOCK_PORT: u32 = 52;

/// Host CID — guests connect to this CID to reach the host.
pub const HOST_CID: u32 = 2;

/// Beyond-pg control RPC port (host → agent). PG-themed, clearly not DNS.
pub const RPC_PORT: u32 = 5430;

/// Maximum size of a single user-process log line before truncation.
pub const MAX_USER_PROCESS_LINE_BYTES: usize = 256 * 1024;

/// Type byte for `UserProcessStreamData` frames (Agent → Host: supervised process output).
const USER_PROCESS_STREAM_DATA: u8 = 0xA1;

/// Which output stream a log line came from.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecStream {
    Stdout,
    Stderr,
}

/// Wire payload for a single log line from a supervised process.
/// Must match `UserProcessStreamDataPayload` in vsock-protocol exactly.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct UserProcessStreamDataPayload {
    pub stream: ExecStream,
    pub line: String,
    pub truncated: bool,
    /// Zero UUID for long-running supervised processes.
    #[serde(default)]
    pub execution_id: String,
}

/// Encode one log frame: `[4-byte BE length][0xA1][msgpack payload]`.
///
/// Length covers the type byte + payload (not the 4-byte length field itself),
/// matching the vsock-protocol framing contract.
pub fn encode_log_frame(payload: &UserProcessStreamDataPayload) -> Vec<u8> {
    let msgpack = rmp_serde::to_vec_named(payload)
        .expect("UserProcessStreamDataPayload serialization is infallible");
    let len = (1 + msgpack.len()) as u32;
    let mut buf = Vec::with_capacity(5 + msgpack.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.push(USER_PROCESS_STREAM_DATA);
    buf.extend_from_slice(&msgpack);
    buf
}
