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

/// Inner workload-protocol discriminator for the **readiness** signal. Mirrors
/// `service_protocol::ServiceKind::Ready = 12` (the generic service primitive's
/// readiness kind — reused verbatim so no new wire type is needed). The host
/// `postgres-driver` translates this into `service.ready.{instance}`, the event
/// the orchestrator's readiness gate waits on. The bespoke postgres primitive
/// has no other readiness emitter, so without this frame a deploy hangs forever.
const SERVICE_KIND_READY: u8 = 12;

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

/// Readiness payload. Mirror of `service_protocol::ReadyPayload`. beyond-pg has
/// no HTTP port — readiness is TCP pgbouncer:5432 over the VPC overlay — so
/// `http_port` is always omitted (`skip_serializing_if` → empty msgpack map).
/// The field is kept for wire-shape parity with the driver's decoder.
#[derive(Serialize, Default)]
struct ReadyPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    http_port: Option<u16>,
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
    wrap_app_message(&inner)
}

/// Encode the **readiness** signal as a substrate `AppMessage` (`0x20`) frame.
///
/// Same outer envelope as [`encode_log_frame`]; only the inner discriminator
/// differs — `ServiceKind::Ready (12)` instead of `UserProcessStreamData (46)`.
/// The host `postgres-driver` dispatches on that byte and publishes
/// `service.ready.{instance}`. Emit this once Postgres/pgbouncer is accepting.
///
/// Inner bytes: `[12][ msgpack_named(ReadyPayload{}) ]` (empty map, since
/// `http_port` is omitted for postgres).
pub fn encode_ready_frame() -> Vec<u8> {
    let inner_payload = rmp_serde::to_vec_named(&ReadyPayload::default())
        .expect("ReadyPayload serialization is infallible");
    let mut inner = Vec::with_capacity(1 + inner_payload.len());
    inner.push(SERVICE_KIND_READY);
    inner.extend_from_slice(&inner_payload);
    wrap_app_message(&inner)
}

/// Wrap opaque inner workload bytes `[discriminator][payload]` in the substrate
/// `AppMessage` (`0x20`) frame: `[len: u32 BE = 1 + envelope][0x20][envelope]`,
/// where `envelope = msgpack(AppMessagePayload{ bytes: inner })`. `len` covers
/// the type byte + payload, matching the substrate framing contract.
fn wrap_app_message(inner: &[u8]) -> Vec<u8> {
    let envelope = rmp_serde::to_vec_named(&AppMessagePayload {
        bytes: inner.to_vec(),
    })
    .expect("AppMessagePayload serialization is infallible");

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

    /// The readiness frame must decode through the same substrate path as logs,
    /// but carry the `ServiceKind::Ready (12)` inner discriminator the host
    /// `postgres-driver` translates into `service.ready`. Pins the wire shape so
    /// it can't drift from the driver's `service_protocol` decoder: outer
    /// AppMessage (0x20), inner `[12][ msgpack map ]`, with `http_port` omitted
    /// (empty map) — matching `ReadyPayload { http_port: None }`.
    #[test]
    fn ready_frame_round_trips_through_substrate_shape() {
        let frame = encode_ready_frame();

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

        // 3. Inner envelope: [ServiceKind::Ready=12][msgpack_named(ReadyPayload)].
        let (kind, rest) = outer.bytes.split_first().expect("non-empty inner bytes");
        assert_eq!(
            *kind, SERVICE_KIND_READY,
            "inner discriminator must be ServiceKind::Ready (12)"
        );

        // 4. Payload is an EMPTY msgpack map (the single byte 0x80) — http_port
        //    omitted, so the driver's `ReadyPayload` decode yields
        //    `http_port: None`.
        assert_eq!(
            rest,
            &[0x80],
            "ReadyPayload must serialize as an empty msgpack map (http_port omitted)"
        );
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
