//! Per-WAL-segment archive hook.
//!
//! Invoked by Postgres as:
//!   archive_command = '/usr/local/bin/beyond-pg archive %p %f'
//!
//! CRITICAL: must exit 0 even when archiving is not possible.
//! A non-zero exit causes Postgres to not recycle the WAL segment —
//! pg_wal/ grows unbounded until disk fills. See DESIGN.md failure modes.
//!
//! When no archive target is configured: silent no-op (exit 0).
//! When a target is configured: ships the segment to S3 via `aws s3 cp`.
//! On S3 failure: exits non-zero so Postgres retries and keeps the segment.

use crate::mmds::MMDS_PATH;

pub fn run(path: &str, filename: &str) {
    let mmds_path = std::env::var("BEYOND_PG_MMDS_PATH").unwrap_or_else(|_| MMDS_PATH.to_owned());
    let code = run_inner(path, filename, "aws", &mmds_path);
    if code != 0 {
        std::process::exit(code);
    }
}

/// Testable core: returns the exit code rather than calling process::exit directly.
/// `aws_cmd` is the name/path of the `aws` binary (injectable for tests).
fn run_inner(path: &str, filename: &str, aws_cmd: &str, mmds_path: &str) -> i32 {
    let data = std::fs::read_to_string(mmds_path).unwrap_or_default();
    let json: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();

    let target = json
        .pointer("/latest/meta-data/BEYOND_PG_ARCHIVE_TARGET")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    match target {
        None => 0,
        Some(target) => {
            let dest = format!("{}/{}", target.trim_end_matches('/'), filename);
            match spawn_aws_with_etxtbsy_retry(aws_cmd, path, &dest) {
                Ok(s) if s.success() => 0,
                Ok(s) => {
                    eprintln!(
                        "[beyond-pg] archive: aws s3 cp exited {s} — segment {filename} not archived"
                    );
                    1
                }
                Err(e) => {
                    eprintln!(
                        "[beyond-pg] archive: failed to invoke aws: {e} — segment {filename} not archived"
                    );
                    1
                }
            }
        }
    }
}

/// Run `aws s3 cp` with a bounded retry on `ETXTBSY`.
///
/// `archive_command` runs in a fresh subprocess that postgres `fork()`s on
/// every WAL segment. If the host is concurrently writing the aws binary
/// or one of its libraries (e.g. an in-place upgrade of awscli, or an
/// overlay-fs commit), Linux can return `ETXTBSY` from `execve`. The
/// retry is also why this same path passes under `cargo test`'s
/// multi-threaded executor when other tests in the workspace are
/// `fork()`-ing arbitrary commands at the same instant — those forks
/// capture in-flight writer FDs which the kernel then sees on execve.
fn spawn_aws_with_etxtbsy_retry(
    aws_cmd: &str,
    src: &str,
    dest: &str,
) -> std::io::Result<std::process::ExitStatus> {
    let mut attempt: u32 = 0;
    loop {
        let result = std::process::Command::new(aws_cmd)
            .args(["s3", "cp", src, dest, "--no-progress"])
            .status();
        match result {
            Ok(s) => return Ok(s),
            Err(e) if e.raw_os_error() == Some(libc::ETXTBSY) && attempt < 3 => {
                attempt += 1;
                // 10ms, 20ms, 40ms — total < 100ms cap before giving up.
                std::thread::sleep(std::time::Duration::from_millis(10 << (attempt - 1)));
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn write_mmds(archive_target: Option<&str>) -> tempfile::NamedTempFile {
        let f = tempfile::NamedTempFile::new().unwrap();
        let meta: serde_json::Value = if let Some(t) = archive_target {
            json!({ "latest": { "meta-data": { "BEYOND_PG_ARCHIVE_TARGET": t } } })
        } else {
            json!({ "latest": { "meta-data": {} } })
        };
        std::fs::write(f.path(), meta.to_string()).unwrap();
        f
    }

    /// Write a 0o755 script and return the path. Single-step open-write-
    /// close-via-drop with the mode set at creation: avoids the
    /// `write → set_permissions → execve` window that races against the
    /// kernel's `i_writecount` decrement (the cause of intermittent
    /// `ETXTBSY` flakes under parallel `cargo test`).
    #[cfg(unix)]
    fn write_script(path: &std::path::Path, body: &str) {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o755)
            .open(path)
            .unwrap();
        f.write_all(body.as_bytes()).unwrap();
        f.sync_all().unwrap();
        drop(f);
    }

    fn stub_aws(exit_code: i32) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("aws");
        #[cfg(unix)]
        write_script(&stub, &format!("#!/bin/sh\nexit {exit_code}\n"));
        let path_str = stub.to_str().unwrap().to_owned();
        (dir, path_str)
    }

    fn recording_aws() -> (tempfile::TempDir, tempfile::NamedTempFile, String) {
        let dir = tempfile::tempdir().unwrap();
        let log = tempfile::NamedTempFile::new().unwrap();
        let log_path = log.path().display().to_string();
        let stub = dir.path().join("aws");
        #[cfg(unix)]
        write_script(&stub, &format!("#!/bin/sh\necho \"$@\" >> {log_path}\n"));
        let path_str = stub.to_str().unwrap().to_owned();
        (dir, log, path_str)
    }

    #[test]
    fn no_archive_target_is_noop() {
        let mmds = write_mmds(None);
        // stub that always fails — should never be called
        let (_dir, aws) = stub_aws(1);
        let code = run_inner(
            "/fake/path",
            "000000010000000000000001",
            &aws,
            mmds.path().to_str().unwrap(),
        );
        assert_eq!(code, 0, "should exit 0 when no archive target configured");
    }

    #[test]
    fn archives_to_correct_s3_path() {
        let mmds = write_mmds(Some("s3://my-bucket/wal"));
        let (_dir, log, aws) = recording_aws();
        let segment = "000000010000000000000001";
        let code = run_inner(
            "/var/lib/postgresql/18/wal/000000010000000000000001",
            segment,
            &aws,
            mmds.path().to_str().unwrap(),
        );
        assert_eq!(code, 0, "should exit 0 when aws succeeds");
        let recorded = std::fs::read_to_string(log.path()).unwrap();
        assert!(
            recorded.contains("s3 cp"),
            "aws s3 cp not invoked: {recorded}"
        );
        assert!(
            recorded.contains("s3://my-bucket/wal/000000010000000000000001"),
            "wrong S3 destination: {recorded}"
        );
        assert!(
            recorded.contains("--no-progress"),
            "--no-progress missing from aws invocation: {recorded}"
        );
    }

    #[test]
    fn trailing_slash_on_target_not_doubled() {
        let mmds = write_mmds(Some("s3://my-bucket/wal/"));
        let (_dir, log, aws) = recording_aws();
        let segment = "000000010000000000000002";
        run_inner("/path", segment, &aws, mmds.path().to_str().unwrap());
        let recorded = std::fs::read_to_string(log.path()).unwrap();
        assert!(
            recorded.contains("s3://my-bucket/wal/000000010000000000000002"),
            "double slash in S3 path: {recorded}"
        );
        assert!(
            !recorded.contains("wal//"),
            "double slash present: {recorded}"
        );
    }

    #[test]
    fn aws_failure_returns_exit_1() {
        let mmds = write_mmds(Some("s3://my-bucket/wal"));
        let (_dir, aws) = stub_aws(2); // aws exits 2
        let code = run_inner(
            "/path",
            "000000010000000000000001",
            &aws,
            mmds.path().to_str().unwrap(),
        );
        assert_eq!(code, 1, "should propagate failure as exit code 1");
    }

    #[test]
    fn missing_mmds_treated_as_no_target() {
        let (_dir, aws) = stub_aws(1);
        let code = run_inner(
            "/path",
            "000000010000000000000001",
            &aws,
            "/nonexistent/mmds.json",
        );
        assert_eq!(
            code, 0,
            "missing MMDS should be treated as no archive target"
        );
    }
}
