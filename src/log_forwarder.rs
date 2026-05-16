//! Async reader tasks that forward supervised process stdout/stderr to the host
//! via a bounded `mpsc` channel.
//!
//! Copied from `beyond/boxes/guest-agent/src/supervisor/log_forwarder.rs` with
//! two changes:
//! - Import source changed to `crate::vsock` instead of `vsock_protocol`.
//! - `stderr_capture` ring-buffer removed (unused here).
//! - `spawn_reader_task` (raw-fd variant) removed; callers use tokio process pipes directly.
//!
//! Each `spawn_reader_task` call consumes one stdio pipe, reads lines, applies
//! per-stream token-bucket rate limiting, and sends `LogFrame`s to the shared
//! channel. The channel is bounded (capacity 1024); when full, new frames are
//! dropped with a counter that synthesizes a single "[beyond: dropped N log
//! lines]" message before the next successful send.
//!
//! The child process is never blocked — all pipe I/O is non-blocking and reads
//! continue regardless of channel pressure.

use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::vsock::{ExecStream, MAX_USER_PROCESS_LINE_BYTES};

/// One line of output from a supervised process.
// Fields are read by log_writer_task on Linux; dead_code fires on macOS dev builds.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct LogFrame {
    pub stream: ExecStream,
    pub line: String,
    pub truncated: bool,
    /// Zero UUID for long-running supervised processes.
    pub execution_id: Arc<str>,
}

/// Zero UUID used for the long-running postgres and pgbouncer processes.
pub fn zero_execution_id() -> Arc<str> {
    Arc::from("00000000-0000-0000-0000-000000000000")
}

/// Sustained line rate per stream (tokens / second).
pub const LOG_RATE_LINES_PER_SEC: f64 = 500.0;
/// Maximum burst depth per stream (tokens).
pub const LOG_BURST_LINES: f64 = 1000.0;

pub(super) struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new() -> Self {
        Self {
            tokens: LOG_BURST_LINES,
            last_refill: Instant::now(),
        }
    }

    /// Returns `true` if a token was consumed (line may proceed).
    fn consume(&mut self) -> bool {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        self.tokens = (self.tokens + elapsed * LOG_RATE_LINES_PER_SEC).min(LOG_BURST_LINES);
        self.last_refill = Instant::now();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Spawn an async task that reads lines from an already-async `pipe` and sends
/// `LogFrame`s to `log_tx`. Same semantics as [`spawn_reader_task`] but for
/// tokio-process pipes that are already `AsyncRead`.
pub fn spawn_async_reader_task(
    stream: ExecStream,
    pipe: impl AsyncRead + Unpin + Send + 'static,
    log_tx: mpsc::Sender<LogFrame>,
    execution_id: Arc<str>,
) -> JoinHandle<()> {
    spawn_reader_loop(
        stream,
        BufReader::with_capacity(64 * 1024, pipe),
        log_tx,
        execution_id,
    )
}

fn spawn_reader_loop<R: AsyncRead + Unpin + Send + 'static>(
    stream: ExecStream,
    mut reader: BufReader<R>,
    log_tx: mpsc::Sender<LogFrame>,
    execution_id: Arc<str>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf: Vec<u8> = Vec::with_capacity(MAX_USER_PROCESS_LINE_BYTES);
        let mut bucket = TokenBucket::new();
        let mut dropped: u64 = 0;

        loop {
            buf.clear();
            match read_line_bounded(&mut reader, &mut buf).await {
                Ok(0) => break, // EOF
                Ok(total) => {
                    let truncated = total > MAX_USER_PROCESS_LINE_BYTES;
                    if buf.ends_with(b"\n") {
                        buf.pop();
                    }
                    if buf.ends_with(b"\r") {
                        buf.pop();
                    }
                    send_frame(
                        stream,
                        &buf,
                        truncated,
                        &log_tx,
                        &mut bucket,
                        &mut dropped,
                        &execution_id,
                    )
                    .await;
                }
                Err(e) => {
                    warn!(error = %e, stream = ?stream, "pipe read error, stopping log forwarder");
                    break;
                }
            }
        }
    })
}

/// Read one line from `reader` into `buf`, storing at most
/// `MAX_USER_PROCESS_LINE_BYTES`. Excess bytes are consumed but discarded so the
/// stream stays in sync. Returns the total bytes consumed; `Ok(0)` means EOF.
async fn read_line_bounded<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let mut total = 0usize;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(total);
        }
        let newline_pos = available.iter().position(|&b| b == b'\n');
        let consume_len = newline_pos.map_or(available.len(), |p| p + 1);

        let can_store = MAX_USER_PROCESS_LINE_BYTES.saturating_sub(buf.len());
        if can_store > 0 {
            buf.extend_from_slice(&available[..consume_len.min(can_store)]);
        }
        total += consume_len;
        reader.consume(consume_len);

        if newline_pos.is_some() {
            return Ok(total);
        }
    }
}

async fn send_frame(
    stream: ExecStream,
    raw: &[u8],
    truncated: bool,
    log_tx: &mpsc::Sender<LogFrame>,
    bucket: &mut TokenBucket,
    dropped: &mut u64,
    execution_id: &Arc<str>,
) {
    let line = std::str::from_utf8(raw)
        .map(|s| s.to_owned())
        .unwrap_or_else(|_| String::from_utf8_lossy(raw).into_owned());

    if !bucket.consume() {
        *dropped += 1;
        return;
    }

    if *dropped > 0 {
        let notice = LogFrame {
            stream,
            line: format!("[beyond: dropped {} log lines]", *dropped),
            truncated: false,
            execution_id: Arc::clone(execution_id),
        };
        if log_tx.try_send(notice).is_ok() {
            *dropped = 0;
        }
    }

    let frame = LogFrame {
        stream,
        line,
        truncated,
        execution_id: Arc::clone(execution_id),
    };
    if log_tx.try_send(frame).is_err() {
        *dropped += 1;
    }
}

#[cfg(test)]
impl TokenBucket {
    fn set_tokens(&mut self, n: f64) {
        self.tokens = n;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::sync::mpsc;

    async fn collect_frames(data: &[u8], stream: ExecStream) -> Vec<(String, bool)> {
        let (tx, mut rx) = mpsc::channel::<LogFrame>(256);
        let cursor = Cursor::new(data.to_vec());
        let handle = spawn_async_reader_task(stream, cursor, tx, Arc::from(""));
        handle.await.unwrap();
        let mut frames = Vec::new();
        while let Ok(f) = rx.try_recv() {
            frames.push((f.line, f.truncated));
        }
        frames
    }

    #[tokio::test]
    async fn single_line_stdout() {
        let frames = collect_frames(b"hello world\n", ExecStream::Stdout).await;
        assert_eq!(frames, vec![("hello world".to_string(), false)]);
    }

    #[tokio::test]
    async fn crlf_endings() {
        let frames = collect_frames(b"line one\r\nline two\r\n", ExecStream::Stdout).await;
        assert_eq!(
            frames,
            vec![
                ("line one".to_string(), false),
                ("line two".to_string(), false),
            ]
        );
    }

    #[tokio::test]
    async fn unterminated_eof() {
        let frames = collect_frames(b"no newline at end", ExecStream::Stderr).await;
        assert_eq!(frames, vec![("no newline at end".to_string(), false)]);
    }

    #[tokio::test]
    async fn oversized_line_truncated() {
        let big = vec![b'x'; MAX_USER_PROCESS_LINE_BYTES + 100];
        let mut data = big.clone();
        data.push(b'\n');
        let frames = collect_frames(&data, ExecStream::Stdout).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].0.len(), MAX_USER_PROCESS_LINE_BYTES);
        assert!(frames[0].1, "truncated flag should be true");
    }

    #[tokio::test]
    async fn invalid_utf8_lossy() {
        let frames = collect_frames(b"valid \xFF invalid\n", ExecStream::Stdout).await;
        assert_eq!(frames.len(), 1);
        assert!(frames[0].0.contains("valid"));
    }

    #[tokio::test]
    async fn token_bucket_rate_limits_after_burst() {
        let mut bucket = TokenBucket::new();
        let burst = LOG_BURST_LINES as u64;
        for _ in 0..burst {
            assert!(bucket.consume(), "should succeed within burst");
        }
        // On a fast machine the burst loop completes in microseconds and the
        // bucket is empty here; on a heavily loaded CI runner the time-based
        // refill can put a fraction of a token back. Consume aggressively past
        // burst and assert that the limiter rejects at LEAST as much as it
        // accepts — the rate limit is engaged either way.
        let mut accepted = 0u64;
        let mut rejected = 0u64;
        for _ in 0..burst {
            if bucket.consume() {
                accepted += 1;
            } else {
                rejected += 1;
            }
        }
        assert!(
            rejected >= accepted,
            "post-burst rejections ({rejected}) must outnumber accepts ({accepted}) — rate limit not engaged"
        );
    }

    #[tokio::test]
    async fn drop_notice_emitted_before_next_real_frame() {
        let (tx, mut rx) = mpsc::channel::<LogFrame>(2);
        let execution_id: Arc<str> = Arc::from("test-id");
        let mut bucket = TokenBucket::new();
        let mut dropped: u64 = 0;

        send_frame(
            ExecStream::Stdout,
            b"first",
            false,
            &tx,
            &mut bucket,
            &mut dropped,
            &execution_id,
        )
        .await;
        send_frame(
            ExecStream::Stdout,
            b"second",
            false,
            &tx,
            &mut bucket,
            &mut dropped,
            &execution_id,
        )
        .await;
        assert_eq!(dropped, 0);

        send_frame(
            ExecStream::Stdout,
            b"third",
            false,
            &tx,
            &mut bucket,
            &mut dropped,
            &execution_id,
        )
        .await;
        assert_eq!(dropped, 1);

        let f1 = rx.recv().await.unwrap();
        assert_eq!(f1.line, "first");
        let f2 = rx.recv().await.unwrap();
        assert_eq!(f2.line, "second");

        send_frame(
            ExecStream::Stdout,
            b"fourth",
            false,
            &tx,
            &mut bucket,
            &mut dropped,
            &execution_id,
        )
        .await;
        assert_eq!(dropped, 0, "counter should be reset after notice is sent");

        let notice = rx.recv().await.unwrap();
        assert!(
            notice.line.contains("dropped 1 log lines"),
            "unexpected notice: {}",
            notice.line
        );
        let f4 = rx.recv().await.unwrap();
        assert_eq!(f4.line, "fourth");
    }
}
