//! Vsock framing helpers for `beyond-pg` (supervisor side).
//!
//! Port constants and type definitions live in [`beyond_pg_core::vsock`].
//! This module adds the rmp-serde-based encode helpers that we don't want
//! linked into PID 1 (`beyond-pg-init`).
//!
//! # Log forwarding wire contract
//!
//! Supervised-process log lines ride the Beyond substrate's opaque
//! **`AppMessage`** envelope (substrate `MessageType` `0x20`) exactly like the
//! first-party primitives (repo / service). The substrate (`instd`) is
//! primitive-blind: it frames/deframes the vsock packet, strips the `0x20`
//! wrapper, and forwards the inner opaque bytes to whichever host-side
//! **driver** subscribed for this VM's `beyond.dev/primitive` label. The driver
//! decodes the inner `AppKind` byte and writes the structured `service.log`
//! JSONL that `logfwd` tails into ClickHouse.
//!
//! The full frame this module emits on vsock port 52 is:
//!
//! ```text
//! [len: u32 BE][0x20][ msgpack( AppMessagePayload{ bytes } ) ]
//!                              └─ bytes = [AppKind=46][ msgpack_named(UserProcessStreamDataPayload) ]
//! ```
//!
//! `len` covers the type byte + payload (not the 4-byte length field itself),
//! matching the substrate framing contract
//! (`[length: u32 BE][type: u8][payload]`).
//!
//! Everything here is vendored — `beyond-pg` has **zero** dependency on the
//! Beyond workspace. The wire shapes mirror, byte-for-byte:
//!   * substrate frame header — `rustlib/vsock-protocol` `write_message`
//!   * `MessageType::AppMessage = 0x20` — `rustlib/vsock-protocol`
//!   * `AppMessagePayload { bytes: <serde_bytes / msgpack bin> }` — ditto
//!   * `AppKind::UserProcessStreamData = 46` — `primitives/repo/protocol`
//!   * the inner `[AppKind][msgpack_named]` envelope — `repo_protocol::encode_app`
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use serde::Serialize;

pub use beyond_pg_core::vsock::{
    ExecStream, LOG_SINK_UNIX_PATH, MAX_USER_PROCESS_LINE_BYTES, RPC_PORT,
    UserProcessStreamDataPayload,
};

/// Substrate `MessageType::AppMessage`. Opaque workload-protocol envelope;
/// `instd` forwards its inner bytes to a driver without decoding them.
/// Mirrors `vsock_protocol::MessageType::AppMessage`.
const APP_MESSAGE: u8 = 0x20;

/// Inner workload-protocol discriminator for a supervised-process log line.
/// Mirrors `repo_protocol::AppKind::UserProcessStreamData = 46`. The driver
/// dispatches on this byte; the substrate never reads it.
const APP_KIND_USER_PROCESS_STREAM_DATA: u8 = 46;

/// Outer substrate envelope payload: the opaque inner workload bytes.
///
/// `serde_bytes` makes `bytes` serialize as a MessagePack **bin** (not an
/// array of integers), matching `vsock_protocol::AppMessagePayload`. The field
/// name (`bytes`) is part of the wire contract because the substrate decodes
/// this with `rmp_serde::to_vec_named` (string keys).
#[derive(Serialize)]
struct AppMessagePayload {
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
}

/// Encode one log line as a substrate `AppMessage` (`0x20`) frame.
///
/// Replaces the former bogus `0xA1` raw frame, which was not a real substrate
/// `MessageType` and was dropped by `instd` (`unknown message type: 0xa1`).
///
/// Returns the complete framed bytes ready to `write_all` to the vsock stream:
/// `[len: u32 BE][0x20][ msgpack(AppMessagePayload{ bytes }) ]` where
/// `bytes = [46][ msgpack_named(UserProcessStreamDataPayload) ]`.
pub fn encode_log_frame(payload: &UserProcessStreamDataPayload) -> Vec<u8> {
    // Inner workload envelope: [AppKind=46][msgpack_named(payload)].
    let inner_payload = rmp_serde::to_vec_named(payload)
        .expect("UserProcessStreamDataPayload serialization is infallible");
    let mut inner = Vec::with_capacity(1 + inner_payload.len());
    inner.push(APP_KIND_USER_PROCESS_STREAM_DATA);
    inner.extend_from_slice(&inner_payload);

    // Outer substrate envelope: msgpack(AppMessagePayload{ bytes: inner }).
    let envelope = rmp_serde::to_vec_named(&AppMessagePayload { bytes: inner })
        .expect("AppMessagePayload serialization is infallible");

    // Substrate frame: [len: u32 BE = 1 + envelope.len()][0x20][envelope].
    let len = (1 + envelope.len()) as u32;
    let mut buf = Vec::with_capacity(5 + envelope.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.push(APP_MESSAGE);
    buf.extend_from_slice(&envelope);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The frame must decode back through the exact substrate path: strip the
    /// 4-byte length, assert the `0x20` type byte, msgpack-decode the outer
    /// `AppMessagePayload`, then split the inner `[AppKind][payload]` envelope
    /// and msgpack-decode the `UserProcessStreamDataPayload` — proving the
    /// bytes are what a Beyond driver receives after `instd` strips the wrapper.
    #[test]
    fn frame_round_trips_through_substrate_shape() {
        let payload = UserProcessStreamDataPayload {
            stream: ExecStream::Stdout,
            line: "database system is ready to accept connections".to_string(),
            truncated: false,
            execution_id: String::new(),
        };
        let frame = encode_log_frame(&payload);

        // 1. Frame header: [len: u32 BE][type].
        let len = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4, "length covers type + payload");
        assert_eq!(frame[4], APP_MESSAGE, "type byte must be AppMessage 0x20");

        // 2. Outer AppMessagePayload — what `instd` deframes and forwards.
        #[derive(serde::Deserialize)]
        struct OuterIn {
            #[serde(with = "serde_bytes")]
            bytes: Vec<u8>,
        }
        let outer: OuterIn = rmp_serde::from_slice(&frame[5..]).expect("decode AppMessagePayload");

        // 3. Inner workload envelope: [AppKind=46][msgpack_named(payload)].
        let (kind, rest) = outer.bytes.split_first().expect("non-empty inner bytes");
        assert_eq!(
            *kind, APP_KIND_USER_PROCESS_STREAM_DATA,
            "inner AppKind must be UserProcessStreamData (46)"
        );

        // 4. Payload decodes back to the original (named-field msgpack).
        let decoded: UserProcessStreamDataPayload =
            rmp_serde::from_slice(rest).expect("decode UserProcessStreamDataPayload");
        assert!(matches!(decoded.stream, ExecStream::Stdout));
        assert_eq!(decoded.line, payload.line);
        assert!(!decoded.truncated);
        assert_eq!(decoded.execution_id, "");
    }

    /// `AppMessagePayload.bytes` must be a MessagePack **bin** family marker
    /// (0xc4/0xc5/0xc6), never an array — `instd` decodes it via `serde_bytes`.
    #[test]
    fn inner_bytes_encoded_as_msgpack_bin() {
        let payload = UserProcessStreamDataPayload {
            stream: ExecStream::Stderr,
            line: "x".to_string(),
            truncated: true,
            execution_id: "abc".to_string(),
        };
        let frame = encode_log_frame(&payload);
        // Outer is a fixmap of 1 entry: 0x81, then fixstr "bytes" (0xa5 b y t e s),
        // then the bin marker for the value.
        let env = &frame[5..];
        assert_eq!(env[0], 0x81, "outer is a 1-entry fixmap");
        assert_eq!(env[1], 0xa5, "key is a 5-char fixstr");
        assert_eq!(&env[2..7], b"bytes", "key name is `bytes`");
        let bin_marker = env[7];
        assert!(
            matches!(bin_marker, 0xc4 | 0xc5 | 0xc6),
            "value must be a msgpack bin (got {bin_marker:#x})"
        );
    }
}
