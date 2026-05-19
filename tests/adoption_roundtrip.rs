//! Full round-trip of the successor adoption path, exercised as a single
//! process. Spawns a long-running child, persists its pid+starttime to
//! `children.json`, drops the `Child` handle without killing, reloads the
//! state file, adopts via `pidfd_open`, wraps in `AsyncFd`, sends SIGTERM
//! via `pidfd_send_signal`, and asserts the readability wake-up arrives.
//!
//! This is what `beyond-pg supervisor`'s successor branch does (less the
//! handoff handshake) — the chain of file-writes + libc syscalls is
//! exhaustively covered without needing a real handoff or postgres.

#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use beyond_pg::children::{
    AdoptResult, PersistedChildren, adopt, pidfd_send_signal, read_starttime,
};

#[tokio::test]
async fn round_trip_persist_and_adopt() {
    let dir = tempfile::tempdir().expect("tempdir");

    // ----- spawn (simulates the original supervisor) -----
    let child = tokio::process::Command::new("sleep")
        .arg("10")
        .spawn()
        .expect("spawn sleep");
    let pid = child.id().expect("pid");
    let starttime = read_starttime(pid).expect("starttime");

    let mut persisted = PersistedChildren::empty();
    persisted
        .record_with_starttime("test-child", pid, starttime)
        .expect("record");
    persisted.save(dir.path()).expect("save");

    // Drop the Child handle without killing. This models the original
    // supervisor exiting cleanly during handoff seal: the child keeps
    // running, reparented to init.
    drop(child);

    // ----- adopt (simulates the new successor) -----
    let prior = PersistedChildren::load(dir.path()).expect("load");
    let record = prior.get("test-child").expect("record present");
    assert_eq!(record.pid, pid);
    assert_eq!(record.starttime, starttime);

    let fd = match adopt(record).expect("adopt") {
        AdoptResult::Adopted(fd) => fd,
        other => panic!("expected Adopted, got {other:?}"),
    };
    let async_fd = tokio::io::unix::AsyncFd::with_interest(fd, tokio::io::Interest::READABLE)
        .expect("AsyncFd");

    // Verify the adopted fd is not yet readable (process still alive).
    let immediate = tokio::time::timeout(Duration::from_millis(50), async_fd.readable()).await;
    assert!(
        immediate.is_err(),
        "pidfd should not be readable while child is alive"
    );

    // Kill via pidfd and wait for the kernel to notify.
    {
        // Use the AsyncFd's inner OwnedFd reference; we don't own it
        // directly anymore, so we re-open one for the signal.
        // (Alternative: the pidfd inside async_fd is what we'd use, but
        // we can't move out of AsyncFd. pidfd_open again is fine — it
        // returns a new fd referencing the same process.)
        let fd2 = match adopt(record).expect("re-adopt") {
            AdoptResult::Adopted(fd) => fd,
            other => panic!("expected Adopted on re-open, got {other:?}"),
        };
        pidfd_send_signal(&fd2, libc::SIGTERM).expect("pidfd_send_signal");
    }

    let started = Instant::now();
    let _guard = tokio::time::timeout(Duration::from_secs(3), async_fd.readable())
        .await
        .expect("readable timeout");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "AsyncFd readable wake-up after SIGTERM should be fast (got {elapsed:?})"
    );
}

#[tokio::test]
async fn round_trip_handles_recycled_pid() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Spawn a process that exits immediately, persist its pid before the
    // OS reaps. Then load the file and try to adopt; the live starttime
    // will either be ESRCH (Dead) or some other process (Recycled). Both
    // are non-Adopted outcomes and *must* be returned correctly so the
    // supervisor can fall through to a fresh spawn.
    let child = tokio::process::Command::new("true").spawn().expect("spawn");
    let pid = child.id().expect("pid");
    let starttime_at_spawn = read_starttime(pid).ok();

    let mut persisted = PersistedChildren::empty();
    if let Some(st) = starttime_at_spawn {
        persisted
            .record_with_starttime("ephemeral", pid, st)
            .expect("record");
        persisted.save(dir.path()).expect("save");
    } else {
        // If we couldn't read starttime in time, the process exited too
        // fast for us to record. Skip this assertion for that case.
        let _ = child.wait_with_output().await;
        return;
    }
    let _ = child.wait_with_output().await;
    // Give the kernel a moment to fully reap.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let prior = PersistedChildren::load(dir.path()).expect("load");
    let record = prior.get("ephemeral").expect("record present");
    let result = adopt(record).expect("adopt");
    match result {
        AdoptResult::Dead | AdoptResult::Recycled { .. } => {}
        AdoptResult::Adopted(_) => panic!("adopted a dead pid — pid-recycling guard failed!"),
    }
}
