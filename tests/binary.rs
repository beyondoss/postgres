//! Integration tests that exercise the actual `beyond-pg` binary via
//! `CARGO_BIN_EXE_beyond-pg`. Each test spawns the binary as a subprocess so
//! we prove the real artifact behaves correctly, not just library internals.
//!
//! `archive` subcommand tests run without Docker: they control the MMDS path
//! via `BEYOND_PG_MMDS_PATH` and swap in a fake `aws` script by prepending a
//! temp directory to `PATH`.

#[cfg(unix)]
mod archive {
    use std::os::unix::fs::PermissionsExt;

    const BIN: &str = env!("CARGO_BIN_EXE_beyond-pg");

    fn mmds_json(archive_target: Option<&str>) -> String {
        match archive_target {
            None => r#"{"latest":{"meta-data":{"POSTGRES_PASSWORD":"x"}}}"#.to_owned(),
            Some(t) => format!(
                r#"{{"latest":{{"meta-data":{{"POSTGRES_PASSWORD":"x","BEYOND_PG_ARCHIVE_TARGET":"{t}"}}}}}}"#
            ),
        }
    }

    /// Write a fake `aws` script into `dir/aws`. The script records all
    /// arguments to `capture_file` and exits with `exit_code`.
    fn write_fake_aws(dir: &std::path::Path, capture_file: &std::path::Path, exit_code: i32) {
        let script = format!(
            "#!/bin/sh\necho \"$@\" > '{}'\nexit {exit_code}\n",
            capture_file.display()
        );
        let aws = dir.join("aws");
        std::fs::write(&aws, script).unwrap();
        std::fs::set_permissions(&aws, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn prepend_path(dir: &std::path::Path) -> String {
        format!(
            "{}:{}",
            dir.display(),
            std::env::var("PATH").unwrap_or_default()
        )
    }

    /// No archive target in MMDS → binary exits 0 silently. WAL segment is
    /// recycled normally by Postgres. This is the expected state until S3 is
    /// configured.
    #[test]
    fn archive_noop_when_no_target() {
        let tmp = tempfile::tempdir().unwrap();
        let mmds = tmp.path().join("metadata.json");
        std::fs::write(&mmds, mmds_json(None)).unwrap();

        let status = std::process::Command::new(BIN)
            .args(["archive", "/dev/null", "000000010000000000000001"])
            .env("BEYOND_PG_MMDS_PATH", &mmds)
            .status()
            .expect("failed to spawn beyond-pg");

        assert!(status.success(), "archive no-op must exit 0, got: {status}");
    }

    /// Archive target present → binary must invoke `aws s3 cp <path>
    /// <target>/<filename> --no-progress` and exit 0 on success.
    #[test]
    fn archive_calls_aws_s3_cp_with_correct_args() {
        let tmp = tempfile::tempdir().unwrap();

        let mmds = tmp.path().join("metadata.json");
        std::fs::write(&mmds, mmds_json(Some("s3://my-bucket/wal/"))).unwrap();

        let capture = tmp.path().join("captured.txt");
        let bin_dir = tmp.path().join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        write_fake_aws(&bin_dir, &capture, 0);

        // A real WAL segment path (content irrelevant — the binary just passes it to aws).
        let wal_path = tmp.path().join("000000010000000000000001");
        std::fs::write(&wal_path, b"fake-wal-segment").unwrap();

        let status = std::process::Command::new(BIN)
            .args([
                "archive",
                wal_path.to_str().unwrap(),
                "000000010000000000000001",
            ])
            .env("BEYOND_PG_MMDS_PATH", &mmds)
            .env("PATH", prepend_path(&bin_dir))
            .status()
            .expect("failed to spawn beyond-pg");

        assert!(
            status.success(),
            "archive with reachable aws must exit 0, got: {status}"
        );

        let recorded = std::fs::read_to_string(&capture)
            .expect("fake aws did not write capture file — was it invoked?");
        let recorded = recorded.trim();

        assert!(
            recorded.contains("s3 cp"),
            "aws not called with 's3 cp': {recorded}"
        );
        assert!(
            recorded.contains("s3://my-bucket/wal/000000010000000000000001"),
            "aws destination wrong (expected s3://my-bucket/wal/000000010000000000000001): {recorded}"
        );
        assert!(
            recorded.contains(wal_path.to_str().unwrap()),
            "aws source path missing: {recorded}"
        );
        assert!(
            recorded.contains("--no-progress"),
            "aws missing --no-progress flag: {recorded}"
        );
    }

    /// When `aws s3 cp` exits non-zero the binary must exit non-zero so
    /// Postgres keeps the WAL segment and retries archiving on the next
    /// checkpoint cycle. A zero exit here would silently lose WAL.
    #[test]
    fn archive_exits_nonzero_when_aws_fails() {
        let tmp = tempfile::tempdir().unwrap();

        let mmds = tmp.path().join("metadata.json");
        std::fs::write(&mmds, mmds_json(Some("s3://bucket/wal"))).unwrap();

        let capture = tmp.path().join("captured.txt");
        let bin_dir = tmp.path().join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        write_fake_aws(&bin_dir, &capture, 1);

        let status = std::process::Command::new(BIN)
            .args(["archive", "/dev/null", "000000010000000000000001"])
            .env("BEYOND_PG_MMDS_PATH", &mmds)
            .env("PATH", prepend_path(&bin_dir))
            .status()
            .expect("failed to spawn beyond-pg");

        assert!(
            !status.success(),
            "archive must exit non-zero when aws fails — got success, which would silently lose WAL"
        );
    }

    /// Trailing slash on the archive target must be stripped: destination must
    /// be `s3://bucket/wal/000000010000000000000001`, not
    /// `s3://bucket/wal//000000010000000000000001`.
    #[test]
    fn archive_strips_trailing_slash_from_target() {
        let tmp = tempfile::tempdir().unwrap();

        // Target has multiple trailing slashes — all must be stripped.
        let mmds = tmp.path().join("metadata.json");
        std::fs::write(&mmds, mmds_json(Some("s3://bucket/wal///"))).unwrap();

        let capture = tmp.path().join("captured.txt");
        let bin_dir = tmp.path().join("bin");
        std::fs::create_dir_all(&bin_dir).unwrap();
        write_fake_aws(&bin_dir, &capture, 0);

        let status = std::process::Command::new(BIN)
            .args(["archive", "/dev/null", "000000010000000000000001"])
            .env("BEYOND_PG_MMDS_PATH", &mmds)
            .env("PATH", prepend_path(&bin_dir))
            .status()
            .expect("failed to spawn beyond-pg");

        assert!(status.success());

        let recorded = std::fs::read_to_string(&capture).unwrap();
        let recorded = recorded.trim();
        assert!(
            recorded.contains("s3://bucket/wal/000000010000000000000001"),
            "double slash in destination: {recorded}"
        );
        assert!(
            !recorded.contains("wal//"),
            "double slash in destination: {recorded}"
        );
    }
}
