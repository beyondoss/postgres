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
    let data = std::fs::read_to_string(&mmds_path).unwrap_or_default();
    let json: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();

    let target = json
        .pointer("/latest/meta-data/BEYOND_PG_ARCHIVE_TARGET")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    match target {
        None => {
            // No target configured — WAL segment will be recycled normally.
            // This is the expected MVP state.
        }
        Some(target) => {
            let dest = format!("{}/{}", target.trim_end_matches('/'), filename);
            let result = std::process::Command::new("aws")
                .args(["s3", "cp", path, &dest, "--no-progress"])
                .status();
            match result {
                Ok(s) if s.success() => {}
                Ok(s) => {
                    eprintln!(
                        "[beyond-pg] archive: aws s3 cp exited {s} — segment {filename} not archived"
                    );
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!(
                        "[beyond-pg] archive: failed to invoke aws: {e} — segment {filename} not archived"
                    );
                    std::process::exit(1);
                }
            }
        }
    }
    // Always exit 0 (implicit return from main via this fn)
}
