//! Vsock framing helpers for `beyond-pg` (supervisor side).
//!
//! Port constants and type definitions live in [`beyond_pg_core::vsock`].
//! This module adds the rmp-serde-based encode helpers that we don't want
//! linked into PID 1 (`beyond-pg-init`).
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

pub use beyond_pg_core::vsock::{
    ExecStream, HOST_CID, MAX_USER_PROCESS_LINE_BYTES, RPC_PORT, UserProcessStreamDataPayload,
    VSOCK_PORT,
};

/// Type byte for `UserProcessStreamData` frames (Agent → Host: supervised process output).
const USER_PROCESS_STREAM_DATA: u8 = 0xA1;

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
