//! MMDS metadata reader.
//!
//! Reads config directly from the Firecracker MMDS endpoint at
//! `169.254.169.254` via raw HTTP. Falls back to environment variables
//! when MMDS is unavailable (local dev, Docker).
//!
//! Retries with backoff until MMDS is available. Called by `init::run()`
//! after the MMDS route has been added to the routing table.

use std::time::Duration;

use serde_json::Value;
use tracing::{debug, warn};

pub const MMDS_PATH: &str = "/run/mmds/metadata.json";
const MAX_ATTEMPTS: u32 = 5;
const RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// Configuration derived from MMDS at boot time.
#[derive(Debug, Clone)]
#[allow(dead_code)] // all fields used on Linux; some reserved for future features
pub struct MmdsConfig {
    pub pg_tier: PgTier,
    pub ephemeral: bool,
    /// Required. `beyond-pg` fails closed if absent.
    pub postgres_password: String,
    pub postgres_database: String,
    pub archive_target: Option<String>,
    /// HTTP base URL of the WAL sink (e.g. `http://10.0.0.5:9000`).
    /// Set via MMDS key `BEYOND_PG_WAL_SINK`. Absent or empty → `None`.
    pub wal_sink: Option<String>,
    /// When true, a `cdc` logical replication slot and empty publication are created on boot.
    /// Set via MMDS key `BEYOND_PG_CDC_ENABLED`. Absent or `false` → `false`.
    pub cdc_enabled: bool,
    /// libpq connection string to the primary. Required when `pg_tier = Replica`.
    /// Set via MMDS key `BEYOND_PG_PRIMARY_CONNINFO`.
    pub primary_conninfo: Option<String>,
    /// Host RAM in bytes (cgroup-aware).
    pub ram_bytes: u64,
    /// Logical CPU count (cgroup-aware).
    pub vcpus: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTier {
    Single,
    Primary,
    Replica,
}

#[derive(Debug, thiserror::Error)]
pub enum MmdsError {
    #[error("POSTGRES_PASSWORD is required but not set in MMDS")]
    MissingPassword,
    #[error("POSTGRES_PASSWORD contains the reserved dollar-quote tag '$_beyond_$'")]
    InvalidPassword,
    #[error("BEYOND_PG_PRIMARY_CONNINFO is required when BEYOND_PG_TIER=replica")]
    MissingPrimaryConninfo,
    #[error("MMDS metadata not available after {MAX_ATTEMPTS} attempts: {0}")]
    Unavailable(String),
}

/// Read and parse MMDS configuration. Retries on transient failures.
///
/// Fails closed if `POSTGRES_PASSWORD` is absent or empty.
pub async fn read() -> Result<MmdsConfig, MmdsError> {
    let json = load_json().await?;
    // Hardware detection reads /proc and /sys — blocking syscalls. Move them off
    // the async executor so they don't stall signal delivery during boot.
    tokio::task::spawn_blocking(move || parse(json))
        .await
        .map_err(|e| MmdsError::Unavailable(format!("parse task panicked: {e}")))?
}

async fn load_json() -> Result<Value, MmdsError> {
    let mut last_err = String::new();
    for attempt in 1..=MAX_ATTEMPTS {
        match tokio::fs::read_to_string(MMDS_PATH).await {
            Ok(raw) => match serde_json::from_str::<Value>(&raw) {
                Ok(v) => {
                    debug!(attempt, "MMDS metadata loaded");
                    return Ok(v);
                }
                Err(e) => {
                    last_err = format!("JSON parse error: {e}");
                    warn!(attempt, error = %e, "MMDS JSON malformed, retrying");
                }
            },
            Err(e) => {
                last_err = format!("read error: {e}");
                if attempt < MAX_ATTEMPTS {
                    warn!(attempt, error = %e, "MMDS file not readable, retrying");
                }
            }
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
    Err(MmdsError::Unavailable(last_err))
}

pub(crate) fn parse(json: Value) -> Result<MmdsConfig, MmdsError> {
    let meta = &json["latest"]["meta-data"];

    let postgres_password = meta["POSTGRES_PASSWORD"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or(MmdsError::MissingPassword)?
        .to_owned();

    // The dollar-quote tag used in set_superuser_password. If the password itself
    // contains this string the ALTER ROLE statement becomes malformed SQL.
    if postgres_password.contains("$_beyond_$") {
        return Err(MmdsError::InvalidPassword);
    }

    let pg_tier = match meta["BEYOND_PG_TIER"].as_str().unwrap_or("single") {
        "primary" => PgTier::Primary,
        "replica" => PgTier::Replica,
        _ => PgTier::Single,
    };

    let ephemeral = meta["BEYOND_VOLUME_EPHEMERAL"]
        .as_str()
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let postgres_database = meta["POSTGRES_DATABASE"]
        .as_str()
        .unwrap_or("postgres")
        .to_owned();

    let archive_target = meta["BEYOND_PG_ARCHIVE_TARGET"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned());

    let wal_sink = meta["BEYOND_PG_WAL_SINK"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_end_matches('/').to_owned());

    let cdc_enabled = meta["BEYOND_PG_CDC_ENABLED"]
        .as_str()
        .map(|s| s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let primary_conninfo = meta["BEYOND_PG_PRIMARY_CONNINFO"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned());

    if pg_tier == PgTier::Replica && primary_conninfo.is_none() {
        return Err(MmdsError::MissingPrimaryConninfo);
    }

    let ram_bytes = read_ram_bytes();
    let vcpus = read_vcpus();

    Ok(MmdsConfig {
        pg_tier,
        ephemeral,
        postgres_password,
        postgres_database,
        archive_target,
        wal_sink,
        cdc_enabled,
        primary_conninfo,
        ram_bytes,
        vcpus,
    })
}

// ---------------------------------------------------------------------------
// Hardware detection — cgroup-aware
// ---------------------------------------------------------------------------

fn read_ram_bytes() -> u64 {
    // cgroup v2 memory.max — correct when running under a memory limit (e.g. Docker dev)
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let s = s.trim();
        // "max" means no limit — fall through to /proc/meminfo
        if s != "max"
            && let Ok(n) = s.parse::<u64>()
        {
            return n;
        }
    }
    // Fallback: /proc/meminfo MemTotal
    read_proc_meminfo_kib("MemTotal").unwrap_or(4 * 1024 * 1024) * 1024
}

fn read_vcpus() -> u32 {
    // cgroup v2 cpu.max: "quota period" or "max period"
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() == 2
            && parts[0] != "max"
            && let (Ok(quota), Ok(period)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>())
            && period > 0
        {
            return ((quota / period) as u32).max(1);
        }
    }
    // Fallback: count processor entries in /proc/cpuinfo
    std::fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .filter(|l| l.starts_with("processor"))
        .count() as u32
}

fn read_proc_meminfo_kib(field: &str) -> Option<u64> {
    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        if line.starts_with(field) {
            let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
            return Some(kib);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_meta(extra: &[(&str, &str)]) -> serde_json::Value {
        let mut map = serde_json::json!({
            "POSTGRES_PASSWORD": "hunter2",
        });
        for (k, v) in extra {
            map["latest"]["meta-data"][k] = serde_json::Value::String(v.to_string());
        }
        // Rebuild with correct nesting
        let mut meta = serde_json::Map::new();
        meta.insert(
            "POSTGRES_PASSWORD".into(),
            serde_json::Value::String("hunter2".into()),
        );
        for (k, v) in extra {
            meta.insert(k.to_string(), serde_json::Value::String(v.to_string()));
        }
        serde_json::json!({ "latest": { "meta-data": meta } })
    }

    #[test]
    fn replica_requires_primary_conninfo() {
        let json = base_meta(&[("BEYOND_PG_TIER", "replica")]);
        let err = parse(json).unwrap_err();
        assert!(
            matches!(err, MmdsError::MissingPrimaryConninfo),
            "expected MissingPrimaryConninfo, got: {err}"
        );
    }

    #[test]
    fn replica_with_conninfo_parses() {
        let json = base_meta(&[
            ("BEYOND_PG_TIER", "replica"),
            (
                "BEYOND_PG_PRIMARY_CONNINFO",
                "host=10.0.0.1 user=replicator",
            ),
        ]);
        let cfg = parse(json).expect("should parse");
        assert_eq!(cfg.pg_tier, PgTier::Replica);
        assert_eq!(
            cfg.primary_conninfo.as_deref(),
            Some("host=10.0.0.1 user=replicator")
        );
    }

    #[test]
    fn single_tier_has_no_conninfo() {
        let json = base_meta(&[]);
        let cfg = parse(json).expect("should parse");
        assert_eq!(cfg.pg_tier, PgTier::Single);
        assert!(cfg.primary_conninfo.is_none());
    }

    #[test]
    fn empty_conninfo_string_treated_as_missing() {
        let json = base_meta(&[
            ("BEYOND_PG_TIER", "replica"),
            ("BEYOND_PG_PRIMARY_CONNINFO", ""),
        ]);
        let err = parse(json).unwrap_err();
        assert!(
            matches!(err, MmdsError::MissingPrimaryConninfo),
            "empty conninfo should fail: {err}"
        );
    }
}
