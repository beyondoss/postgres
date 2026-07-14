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
//! FIRST connect is logged and the thread exits — never fatal. The supervise loop
//! owns shutdown via signalfd (instd also sends SIGTERM), so this thread never
//! powers the VM off.
//!
//! # Reconnect
//!
//! Once connected, a dropped connection is redialled rather than ending the
//! thread. "Connection open" is literally how the host knows this guest exists:
//! instd pings every ~10s and marks the VM `Degraded (guest disconnected)` after
//! 30s of silence. So a thread that exited on EOF left the VM running but
//! permanently invisible to the host — no ready, no heartbeat replies, no PSI
//! reports, no workload logs — and nothing ever restored it. Any instd restart
//! (every `deploy:compute:local`) did exactly that.
//!
//! Only a *first* connect failure is still soft-fail: that means there is no
//! substrate here at all (Docker tests, no AF_VSOCK), so there is nothing to
//! redial. Having connected once proves a substrate exists, and a host that went
//! away is a host that is coming back — so we retry with capped backoff, forever.
//! Reconnects re-send `Ready` with `reconnect: true`.
//!
//! An explicit `Shutdown` frame still ends the loop (it is not a disconnect), and
//! poweroff remains the supervise loop's job.

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
const MSG_GUEST_RESOURCE_STATS: u8 = 0xA2; // Agent → host: periodic resource stats.
/// Frame length ceiling — sanity bound so a corrupt length can't allocate wild.
const MAX_FRAME: u32 = 16 * 1024 * 1024;
/// How often to report guest memory pressure to the host.
const RESOURCE_STATS_PERIOD: std::time::Duration = std::time::Duration::from_secs(30);
/// First delay before redialling a dropped substrate connection.
const INITIAL_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_millis(250);
/// Ceiling for the reconnect backoff. instd marks the guest disconnected after
/// ~30s of silence, so keep retries well inside that window — a VM that comes
/// back within one host restart should never be seen as gone.
const MAX_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

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

/// Agent → host periodic resource stats. A field-compatible subset of
/// `vsock_protocol::GuestResourceStatsPayload`: we report only PSI memory
/// pressure, so the host's memory controller can right-size this VM. `seq` and
/// `disk_used_bytes` are required by the host struct (sent as 0); we omit
/// `disk_total_bytes` so the host skips disk billing for this report (Postgres
/// disk usage is not tracked here). Keys must match the host decoder exactly.
#[derive(Serialize)]
struct GuestResourceStatsPayload {
    seq: u64,
    disk_used_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    psi_mem_some_avg10: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    psi_mem_full_avg10: Option<f64>,
    /// Cumulative `workingset_refault_file` from `/proc/vmstat`: file pages
    /// evicted from the page cache and then read back in.
    ///
    /// Reported so the host can right-size this VM's memory. Postgres is
    /// page-cache-bound and reads with `read()`, not `mmap`, so when its working
    /// set outgrows RAM it does not swap, takes no major faults and never stalls
    /// on reclaim — measured on a real VM (1.8 GB working set, 735 MiB RAM):
    /// PSI 0.00, swap 0, major faults 0/tick. None of the usual memory-pressure
    /// signals move. Refaults are the one counter that does: re-reading a page we
    /// just evicted is precisely "the working set does not fit".
    #[serde(skip_serializing_if = "Option::is_none")]
    workingset_refault_file: Option<u64>,
}

/// Read cumulative `workingset_refault_file` from `/proc/vmstat`.
fn read_workingset_refault_file() -> Option<u64> {
    let raw = std::fs::read_to_string("/proc/vmstat").ok()?;
    parse_workingset_refault_file(&raw)
}

/// Parse `workingset_refault_file` from `/proc/vmstat` text. Split out from the
/// file read so it's testable without `/proc`.
fn parse_workingset_refault_file(raw: &str) -> Option<u64> {
    raw.lines().find_map(|l| {
        l.strip_prefix("workingset_refault_file ")?
            .trim()
            .parse()
            .ok()
    })
}

/// Read Linux PSI memory pressure `(some.avg10, full.avg10)` from
/// `/proc/pressure/memory`. `None` if PSI is unavailable (kernel without
/// `CONFIG_PSI` / not booted with `psi=1`) or the file can't be read.
fn read_memory_pressure() -> Option<(f64, f64)> {
    let raw = std::fs::read_to_string("/proc/pressure/memory").ok()?;
    parse_memory_pressure(&raw)
}

/// Parse `some.avg10` / `full.avg10` from `/proc/pressure/memory` text. Split
/// out from the file read so it's testable without `/proc`.
fn parse_memory_pressure(raw: &str) -> Option<(f64, f64)> {
    let mut some = None;
    let mut full = None;
    for line in raw.lines() {
        let mut fields = line.split_ascii_whitespace();
        let kind = fields.next();
        let avg10 = fields.find_map(|f| f.strip_prefix("avg10=")?.parse::<f64>().ok());
        match kind {
            Some("some") => some = avg10,
            Some("full") => full = avg10,
            _ => {}
        }
    }
    Some((some?, full.unwrap_or(0.0)))
}

/// Encode a `GuestResourceStats` frame carrying current PSI memory pressure, or
/// `None` if PSI is unavailable this tick.
fn encode_resource_stats_frame() -> Option<Vec<u8>> {
    let (some, full) = read_memory_pressure()?;
    let payload = GuestResourceStatsPayload {
        seq: 0,
        disk_used_bytes: 0,
        psi_mem_some_avg10: Some(some),
        psi_mem_full_avg10: Some(full),
        workingset_refault_file: read_workingset_refault_file(),
    };
    let body = rmp_serde::to_vec_named(&payload).ok()?;
    Some(encode_frame(MSG_GUEST_RESOURCE_STATS, &body))
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

/// Why [`keep_alive`] gave the connection back.
enum ConnEnd {
    /// EOF / read / write error — the host went away. Redial.
    Disconnected,
    /// Host sent an explicit `Shutdown`. Not a disconnect: stop for good and let
    /// the supervise loop own the poweroff.
    Shutdown,
}

async fn handshake() {
    use tokio::io::AsyncWriteExt;
    use tokio_vsock::{VsockAddr, VsockStream};

    // Bound ONCE, outside the reconnect loop: the log sink owns a unix listener,
    // and rebinding it per reconnect would unlink the socket out from under the
    // previous accept task and race it. The receiver simply carries over — the
    // supervisor keeps relaying into the same channel across a host bounce.
    let (log_tx, mut log_rx) = tokio::sync::mpsc::channel::<LogFrameBytes>(1024);
    spawn_log_sink(log_tx);

    // False until we have connected at least once. It gates BOTH the `reconnect`
    // flag in the Ready payload and the soft-fail: a first-connect failure means
    // there is no substrate here (Docker tests, no AF_VSOCK) and there is nothing
    // to redial, so keep the original behaviour and let the thread exit.
    let mut connected_once = false;
    let mut backoff = INITIAL_RECONNECT_DELAY;

    loop {
        let mut conn = match VsockStream::connect(VsockAddr::new(HOST_CID, SUBSTRATE_PORT)).await {
            Ok(c) => c,
            Err(e) => {
                if !connected_once {
                    eprintln!(
                        "[init] WARNING: substrate vsock connect failed; guest-ready unreported: {e}"
                    );
                    return;
                }
                eprintln!(
                    "[init] substrate vsock redial failed ({e}); retrying in {}ms",
                    backoff.as_millis()
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
                continue;
            }
        };

        let payload = ReadyPayload {
            agent_version: format!("beyond-pg-init/{}", env!("CARGO_PKG_VERSION")),
            boot_time_ms: read_uptime_ms(),
            reconnect: connected_once,
        };
        let frame = match encode_ready_frame(&payload) {
            Ok(f) => f,
            // Encoding cannot start working on a retry — this is a bug, not an
            // outage. Bail rather than spin.
            Err(e) => {
                eprintln!("[init] WARNING: encode Ready frame failed: {e}");
                return;
            }
        };

        if let Err(e) = conn.write_all(&frame).await {
            if !connected_once {
                eprintln!("[init] WARNING: substrate Ready write failed: {e}");
                return;
            }
            eprintln!("[init] substrate Ready write failed on redial ({e}); retrying");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_RECONNECT_DELAY);
            continue;
        }
        let _ = conn.flush().await;

        if connected_once {
            eprintln!("[init] substrate vsock reconnected; guest re-reported ready");
        } else {
            eprintln!("[init] substrate vsock handshake complete; guest reported ready");
        }
        connected_once = true;
        backoff = INITIAL_RECONNECT_DELAY;

        match keep_alive(&mut conn, &mut log_rx).await {
            ConnEnd::Shutdown => return,
            ConnEnd::Disconnected => {
                eprintln!("[init] substrate vsock connection lost; redialling");
            }
        }
    }
}

/// Bytes received from the supervisor's log sink: one already-framed substrate
/// `AppMessage` wire frame (`[len][0x20][payload]`) to relay onto the vsock.
type LogFrameBytes = Vec<u8>;

/// Own the single substrate connection for the VM's lifetime: answer instd's
/// heartbeats AND relay workload log frames the supervisor hands us over
/// [`LOG_SINK_UNIX_PATH`], multiplexed onto this one connection.
///
/// Why multiplex here: instd accepts exactly one vsock connection per VM (PID 1
/// holds it). The supervisor cannot open its own connection to port 52 — it
/// would never be `accept()`ed and its frames would be dropped. So the
/// supervisor forwards frames to us; we are the only writer to the vsock.
///
/// instd pings every ~10s and marks the VM `Degraded (guest disconnected)`
/// after 30s of silence, so heartbeat replies must never be starved. The
/// `select!` keeps both inbound-vsock and inbound-logs serviced; all vsock
/// writes happen on this one task so there's no write interleaving.
async fn keep_alive<S>(
    conn: &mut S,
    log_rx: &mut tokio::sync::mpsc::Receiver<LogFrameBytes>,
) -> ConnEnd
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Periodic guest memory-pressure (PSI) report for the host memory
    // controller. Fire-and-forget like heartbeats; a write error ends this
    // connection and the caller redials (same as any vsock failure).
    let mut psi_interval = tokio::time::interval(RESOURCE_STATS_PERIOD);
    psi_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut len_buf = [0u8; 4];
    loop {
        tokio::select! {
            // Periodic PSI report.
            _ = psi_interval.tick() => {
                if let Some(frame) = encode_resource_stats_frame() {
                    if conn.write_all(&frame).await.is_err() {
                        return ConnEnd::Disconnected;
                    }
                    let _ = conn.flush().await;
                }
            }
            // Inbound substrate frames (heartbeat / shutdown / ignored).
            read = conn.read_exact(&mut len_buf) => {
                if read.is_err() {
                    return ConnEnd::Disconnected; // EOF / error → redial.
                }
                let len = u32::from_be_bytes(len_buf);
                if len == 0 || len > MAX_FRAME {
                    eprintln!("[init] substrate vsock: bad frame length {len}; closing");
                    return ConnEnd::Disconnected;
                }
                let mut frame = vec![0u8; len as usize];
                if conn.read_exact(&mut frame).await.is_err() {
                    return ConnEnd::Disconnected;
                }
                match frame[0] {
                    MSG_HEARTBEAT => {
                        let body = rmp_serde::to_vec_named(&HeartbeatPayload { timestamp: 0 })
                            .unwrap_or_default();
                        if conn.write_all(&encode_frame(MSG_HEARTBEAT_RESP, &body)).await.is_err() {
                            return ConnEnd::Disconnected;
                        }
                        let _ = conn.flush().await;
                    }
                    MSG_SHUTDOWN => {
                        eprintln!("[init] substrate requested shutdown; vsock loop exiting");
                        return ConnEnd::Shutdown;
                    }
                    _ => {} // ReadyAck etc. — nothing to do here.
                }
            }
            // Workload log frames relayed from the supervisor. Already a complete
            // substrate frame; write verbatim. `None` = sink task ended (only on
            // listener bind failure); we simply stop relaying and keep heartbeats.
            maybe_frame = log_rx.recv() => {
                match maybe_frame {
                    Some(bytes) => {
                        if conn.write_all(&bytes).await.is_err() {
                            return ConnEnd::Disconnected;
                        }
                        let _ = conn.flush().await;
                    }
                    None => {
                        // Drain side closed; nothing more to relay. Keep the
                        // connection alive purely for heartbeats by parking this
                        // arm — re-loop and let the vsock read drive liveness.
                        std::future::pending::<()>().await;
                    }
                }
            }
        }
    }
}

/// Bind [`LOG_SINK_UNIX_PATH`] and forward every framed message a connecting
/// supervisor writes into `log_tx`. Multiple sequential supervisor connections
/// are supported (a handoff swaps the supervisor process); each is drained until
/// it closes, then we accept the next. Soft-fail: a bind error logs and ends the
/// task (heartbeats keep working; logs just won't flow until next boot).
fn spawn_log_sink(log_tx: tokio::sync::mpsc::Sender<LogFrameBytes>) {
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        use tokio::net::UnixListener;

        let path = beyond_pg_core::vsock::LOG_SINK_UNIX_PATH;
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        // Remove any stale socket from a prior boot so bind() succeeds.
        let _ = std::fs::remove_file(path);
        let listener = match UnixListener::bind(path) {
            Ok(l) => l,
            Err(e) => {
                eprintln!(
                    "[init] WARNING: log sink bind {path} failed: {e}; workload logs disabled"
                );
                return;
            }
        };
        eprintln!("[init] log sink listening on {path}");

        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    eprintln!("[init] log sink accept failed: {e}");
                    continue;
                }
            };
            // Drain one supervisor connection: read complete substrate frames
            // (`[len: u32 BE][type][payload]`) and relay them verbatim. On EOF or
            // error, loop back to accept the next supervisor (post-handoff).
            let mut len_buf = [0u8; 4];
            loop {
                if stream.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let len = u32::from_be_bytes(len_buf);
                if len == 0 || len > MAX_FRAME {
                    eprintln!("[init] log sink: bad frame length {len}; dropping connection");
                    break;
                }
                let mut frame = Vec::with_capacity(4 + len as usize);
                frame.extend_from_slice(&len_buf);
                let start = frame.len();
                frame.resize(start + len as usize, 0);
                if stream.read_exact(&mut frame[start..]).await.is_err() {
                    break;
                }
                // Drop on a full channel rather than block the reader — the
                // supervisor rate-limits upstream and a dropped line is
                // acceptable; stalling the relay is not.
                if log_tx.try_send(frame).is_err() {
                    // Channel full or closed: shed this line, keep reading.
                    continue;
                }
            }
        }
    });
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

    /// Pins the `GuestResourceStats` (0xA2) frame so it can't drift from instd's
    /// `vsock_protocol::GuestResourceStatsPayload` decoder: BE length, the type
    /// byte, then a MessagePack map whose keys match the host struct. `seq` and
    /// `disk_used_bytes` are required host fields; `disk_total_bytes` is omitted
    /// so the host skips disk billing; the PSI and refault keys carry the signal.
    #[test]
    fn resource_stats_frame_is_stable() {
        let payload = GuestResourceStatsPayload {
            seq: 0,
            disk_used_bytes: 0,
            psi_mem_some_avg10: Some(12.34),
            psi_mem_full_avg10: Some(0.5),
            workingset_refault_file: Some(123_456),
        };
        let body = rmp_serde::to_vec_named(&payload).unwrap();
        let frame = encode_frame(MSG_GUEST_RESOURCE_STATS, &body);

        let len = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4, "length covers type + payload");
        assert_eq!(frame[4], MSG_GUEST_RESOURCE_STATS, "type byte is 0xA2");

        let v: serde_json::Value = rmp_serde::from_slice(&frame[5..]).unwrap();
        let obj = v.as_object().expect("payload must be a map");
        assert!(obj.contains_key("seq"));
        assert!(obj.contains_key("disk_used_bytes"));
        assert!(obj.contains_key("psi_mem_some_avg10"));
        assert!(obj.contains_key("psi_mem_full_avg10"));
        assert!(
            obj.contains_key("workingset_refault_file"),
            "the refault counter must reach the host — it is the ONLY signal that \
             sees this VM outgrow its RAM (PSI, swap and major faults all read \
             zero on a page-cache-bound workload)"
        );
        assert!(
            !obj.contains_key("disk_total_bytes"),
            "disk_total omitted so the host skips disk billing"
        );
    }

    #[test]
    fn refault_frame_omits_the_key_when_unavailable() {
        // The field is Option: a kernel without the counter must simply omit it,
        // not send a bogus 0 that the host would read as "no refaults".
        let payload = GuestResourceStatsPayload {
            seq: 0,
            disk_used_bytes: 0,
            psi_mem_some_avg10: Some(1.0),
            psi_mem_full_avg10: Some(0.0),
            workingset_refault_file: None,
        };
        let body = rmp_serde::to_vec_named(&payload).unwrap();
        let v: serde_json::Value = rmp_serde::from_slice(&body).unwrap();
        assert!(
            !v.as_object()
                .unwrap()
                .contains_key("workingset_refault_file")
        );
    }

    #[test]
    fn parses_workingset_refault_file() {
        let raw = "nr_free_pages 12345\nworkingset_refault_anon 0\nworkingset_refault_file 297848\npgmajfault 8\n";
        assert_eq!(parse_workingset_refault_file(raw), Some(297_848));
        // Absent on kernels that don't export it — must be None, not 0.
        assert_eq!(parse_workingset_refault_file("nr_free_pages 1\n"), None);
        assert_eq!(parse_workingset_refault_file(""), None);
    }

    /// The whole point of the reconnect work: EOF is NOT a shutdown.
    ///
    /// If these two ever collapse into the same answer again, a host restart
    /// takes the guest's substrate channel down permanently — the VM keeps
    /// running while instd marks it `Degraded (guest disconnected)` and nothing
    /// ever redials.
    #[tokio::test]
    async fn eof_is_a_disconnect_not_a_shutdown() {
        let (mine, theirs) = tokio::io::duplex(1024);
        drop(theirs); // host went away mid-connection

        let (_tx, mut log_rx) = tokio::sync::mpsc::channel::<LogFrameBytes>(1);
        let mut conn = mine;

        assert!(
            matches!(
                keep_alive(&mut conn, &mut log_rx).await,
                ConnEnd::Disconnected
            ),
            "a dropped connection must ask the caller to redial, not end the thread"
        );
    }

    /// An explicit Shutdown frame must still stop for good — it is not a
    /// disconnect, and redialling through it would fight the supervise loop's
    /// poweroff.
    #[tokio::test]
    async fn explicit_shutdown_frame_stops_for_good() {
        use tokio::io::AsyncWriteExt;

        let (mine, mut theirs) = tokio::io::duplex(1024);
        let body = rmp_serde::to_vec_named(&HeartbeatPayload { timestamp: 0 }).unwrap();
        theirs
            .write_all(&encode_frame(MSG_SHUTDOWN, &body))
            .await
            .unwrap();

        let (_tx, mut log_rx) = tokio::sync::mpsc::channel::<LogFrameBytes>(1);
        let mut conn = mine;

        assert!(
            matches!(keep_alive(&mut conn, &mut log_rx).await, ConnEnd::Shutdown),
            "an explicit Shutdown must not be retried as if the host had merely bounced"
        );
    }

    /// A redial re-reports readiness with `reconnect: true` so the host can tell
    /// a returning guest from a freshly booted one.
    #[test]
    fn redial_marks_the_ready_frame_as_a_reconnect() {
        let p = ReadyPayload {
            agent_version: "beyond-pg-init/0.1.0".to_string(),
            boot_time_ms: 0,
            reconnect: true,
        };
        let frame = encode_ready_frame(&p).unwrap();
        let v: serde_json::Value = rmp_serde::from_slice(&frame[5..]).unwrap();
        assert_eq!(v.as_object().unwrap()["reconnect"], serde_json::json!(true));
    }

    #[test]
    fn parses_psi_memory() {
        let raw = "\
some avg10=12.34 avg60=5.00 avg300=1.20 total=461476658
full avg10=0.50 avg60=0.10 avg300=0.00 total=422631474
";
        assert_eq!(parse_memory_pressure(raw), Some((12.34, 0.50)));
        assert_eq!(parse_memory_pressure(""), None);
    }
}
