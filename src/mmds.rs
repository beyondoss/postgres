//! MMDS file reader (async wrapper around `beyond_pg_core::mmds::parse`).
//!
//! `beyond-pg-init` (PID 1) does the HTTP fetch and writes the JSON file.
//! This module reads that file on supervisor start with a short retry loop
//! in case the file isn't fully visible yet (atomic-rename race).

use std::time::Duration;

use serde_json::Value;
use tracing::{debug, warn};

pub use beyond_pg_core::mmds::{MMDS_PATH, MmdsConfig, MmdsError, PgTier};

const MAX_ATTEMPTS: u32 = 5;
const RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// Read and parse MMDS configuration. Retries on transient read failures.
pub async fn read() -> Result<MmdsConfig, MmdsError> {
    let json = load_json().await?;
    tokio::task::spawn_blocking(move || beyond_pg_core::mmds::parse(json))
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
