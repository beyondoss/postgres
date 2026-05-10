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
//! When a target is configured but not yet implemented: loud warning (exit 0).

use crate::mmds::MMDS_PATH;

pub fn run(path: &str, filename: &str) {
    let data = std::fs::read_to_string(MMDS_PATH).unwrap_or_default();
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
            // Target is set but archiving is not yet implemented.
            // Exit 0 is intentional — see module-level doc comment.
            eprintln!(
                "[beyond-pg] WARNING: archive target {target:?} is configured \
                 but WAL archiving is not yet implemented. \
                 Segment {filename} ({path}) was NOT archived. \
                 Do not set BEYOND_PG_ARCHIVE_TARGET until a backup service is available."
            );
            // TODO: implement s3:// shipping when backup service ships
        }
    }
    // Always exit 0 (implicit return from main via this fn)
}
