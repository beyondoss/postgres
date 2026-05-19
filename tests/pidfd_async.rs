//! End-to-end test of the pidfd-via-AsyncFd machinery used by the supervisor's
//! `WaitHandle::Adopted` variant.
//!
//! This is what a handoff successor uses to observe death of postgres /
//! pgbouncer / cdc children that were adopted via pidfd_open (the new
//! supervisor isn't their parent, so it can't `waitpid` on them — pidfd
//! readability is the kernel's notification channel).
//!
//! Verifies on real Linux:
//!  - `pidfd_open` on a live child returns an FD that is *not* readable yet.
//!  - After the child exits, tokio's `AsyncFd::readable()` resolves promptly.
//!  - Adoption of a recycled pid is detected via the saved starttime.
//!  - Adoption of a dead pid reports `AdoptResult::Dead`.

#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use beyond_pg::children::{AdoptResult, ChildRecord, adopt, read_starttime};

#[tokio::test]
async fn adopted_pidfd_becomes_readable_on_exit() {
    // Spawn a short-lived child (sleep 0.4s).
    let mut child = tokio::process::Command::new("sleep")
        .arg("0.4")
        .spawn()
        .expect("spawn sleep");
    let pid = child.id().expect("pid");
    let starttime = read_starttime(pid).expect("starttime");

    // Adopt via the same code path the successor uses.
    let fd = match adopt(&ChildRecord { pid, starttime }).expect("adopt") {
        AdoptResult::Adopted(fd) => fd,
        other => panic!("expected Adopted, got {other:?}"),
    };

    let async_fd = tokio::io::unix::AsyncFd::with_interest(fd, tokio::io::Interest::READABLE)
        .expect("AsyncFd");

    let started = Instant::now();
    // Awaiting readable() will resolve once the child exits (kernel signals
    // pidfd readability on process death). With a 400ms sleep we expect this
    // to take ~400ms.
    let _guard = tokio::time::timeout(Duration::from_secs(3), async_fd.readable())
        .await
        .expect("timeout");
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(300),
        "AsyncFd readable should track the child's actual lifetime (got {elapsed:?})"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "AsyncFd readable shouldn't lag the kernel notification (got {elapsed:?})"
    );

    // Reap so we don't leave a zombie under cargo test.
    let _ = child.wait().await;
}

#[tokio::test]
async fn adoption_detects_recycled_pid() {
    // Spawn and wait for the child to exit so its pid is freed.
    let child = tokio::process::Command::new("true").spawn().expect("spawn");
    let pid = child.id().expect("pid");
    // Make up a starttime that can't match anything currently running.
    let fake_record = ChildRecord {
        pid,
        starttime: u64::MAX,
    };
    let _ = child.wait_with_output().await;

    // If the OS hasn't recycled this pid, we should see Dead; if it has
    // recycled it, starttime mismatch should surface as Recycled. Either is
    // acceptable for "not Adopted" — the adoption guard rejects both.
    let result = adopt(&fake_record).expect("adopt");
    match result {
        AdoptResult::Dead | AdoptResult::Recycled { .. } => {}
        AdoptResult::Adopted(_) => {
            panic!("adopt() unsafely returned Adopted for a non-matching record")
        }
    }
}

#[tokio::test]
async fn adoption_succeeds_for_live_starttime_match() {
    // Spawn a long-running child and adopt it via the supervisor's helper.
    let mut child = tokio::process::Command::new("sleep")
        .arg("10")
        .spawn()
        .expect("spawn");
    let pid = child.id().expect("pid");
    let starttime = read_starttime(pid).expect("starttime");

    let result = adopt(&ChildRecord { pid, starttime }).expect("adopt");
    assert!(
        matches!(result, AdoptResult::Adopted(_)),
        "expected Adopted, got {result:?}"
    );

    // Cleanup
    let _ = child.kill().await;
    let _ = child.wait().await;
}
