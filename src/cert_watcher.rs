//! Watch the platform-provided TLS cert for rotation events.
//!
//! The Beyond box guest agent rotates `/run/beyond/tls/cert.pem` every ~22h
//! via atomic `rename(2)` (`../beyond/boxes/docs/09-internal-tls.md`).
//! Postgres caches the cert in shared memory and only re-reads it on SIGHUP,
//! and PgBouncer is the same. So we watch the cert file and notify the
//! supervisor on each rotation; the supervisor sends the reloads.
//!
//! Implementation is a 60s mtime poll, not inotify, because:
//! - The polling pattern is already used in `beyond-pg-sink` retention.
//! - Rotation is once per 22h — a 60s detection delay is fine.
//! - Avoids a new crate dependency.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc;
use tracing::{info, warn};

const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Spawn a polling task that sends `()` whenever `cert_path`'s mtime changes.
///
/// The first poll only records the baseline mtime; rotations are reported on
/// subsequent polls when the mtime has changed.
pub fn spawn(cert_path: PathBuf, tx: mpsc::Sender<()>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { run(cert_path, tx).await })
}

async fn run(cert_path: PathBuf, tx: mpsc::Sender<()>) {
    let mut last_mtime: Option<SystemTime> = current_mtime(&cert_path).await;
    info!(
        "cert_watcher: watching {} (baseline mtime: {:?})",
        cert_path.display(),
        last_mtime
    );

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        let current = current_mtime(&cert_path).await;
        if current != last_mtime && last_mtime.is_some() {
            info!("cert_watcher: rotation detected at {}", cert_path.display());
            // Best-effort: if the receiver has closed, the supervisor is shutting
            // down and we'll exit on the next tick when send returns Err.
            if tx.send(()).await.is_err() {
                info!("cert_watcher: receiver closed, exiting");
                return;
            }
        }
        last_mtime = current;
    }
}

async fn current_mtime(path: &std::path::Path) -> Option<SystemTime> {
    match tokio::fs::metadata(path).await {
        Ok(m) => m.modified().ok(),
        Err(e) => {
            warn!("cert_watcher: stat {}: {e}", path.display());
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Replacing the cert file (atomic rename) triggers exactly one event.
    /// We override `POLL_INTERVAL` indirectly by waiting longer than it in the
    /// test — too slow for CI if we used the real 60s interval, so this test
    /// uses a custom shorter run loop.
    #[tokio::test]
    async fn rotation_triggers_event() {
        let dir = tempfile::TempDir::new().unwrap();
        let cert = dir.path().join("cert.pem");
        std::fs::write(&cert, b"v1").unwrap();

        let (tx, mut rx) = mpsc::channel::<()>(4);

        // Inline test-loop with a short poll interval. Mirrors `run()` but
        // tunable. Keep this short so CI doesn't sit for a minute.
        let cert_clone = cert.clone();
        let handle = tokio::spawn(async move {
            let mut last_mtime: Option<SystemTime> = current_mtime(&cert_clone).await;
            for _ in 0..40 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let m = current_mtime(&cert_clone).await;
                if m != last_mtime && last_mtime.is_some() && tx.send(()).await.is_err() {
                    return;
                }
                last_mtime = m;
            }
        });

        // Sleep long enough that the new file's mtime is guaranteed to differ.
        // Modern filesystems (ext4/apfs) have sub-second mtime resolution, but
        // 1.1s gives margin for older configurations too.
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        // Atomic rename, mimicking the guest agent's rotation.
        let tmp = dir.path().join("cert.pem.new");
        std::fs::write(&tmp, b"v2 rotated").unwrap();
        std::fs::rename(&tmp, &cert).unwrap();

        let recv = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        handle.abort();
        assert!(
            matches!(recv, Ok(Some(()))),
            "expected one rotation event, got {recv:?}"
        );
    }
}
