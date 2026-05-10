//! Thin wrappers around Postgres CLI tools (`pg_isready`, `psql`, `pg_ctl`,
//! `initdb`). All I/O goes through `tokio::process::Command`.

use std::time::{Duration, Instant};

use tokio::process::Command;
use tracing::{debug, warn};

pub const PGDATA: &str = "/var/lib/postgresql/18/main";
pub const PG_SOCKET_DIR: &str = "/var/run/postgresql";
pub const PG_PORT: u16 = 5433; // Postgres direct; PgBouncer is 5432 (separate process)
pub const POSTGRES_USER: &str = "postgres";

#[derive(Debug, thiserror::Error)]
pub enum PgError {
    #[error("pg command failed (exit {code}): {stderr}")]
    NonZeroExit { code: i32, stderr: String },
    #[error("pg command I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("pg command killed by signal")]
    Signal,
}

// ---------------------------------------------------------------------------
// Readiness
// ---------------------------------------------------------------------------

/// Returns `true` if Postgres is accepting connections on the unix socket.
pub async fn is_ready() -> bool {
    Command::new("pg_isready")
        .args(["-h", PG_SOCKET_DIR, "-p", &PG_PORT.to_string(), "-q"])
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Poll `is_ready()` every 100 ms until it returns `true` or `timeout` elapses.
pub async fn wait_until_ready(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if is_ready().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

// ---------------------------------------------------------------------------
// psql
// ---------------------------------------------------------------------------

/// Execute a SQL statement via `psql` on the unix socket.
pub async fn psql(sql: &str) -> Result<(), PgError> {
    psql_env(sql, &[]).await
}

/// Execute a SQL statement with extra environment variables.
pub async fn psql_env(sql: &str, env: &[(&str, &str)]) -> Result<(), PgError> {
    let mut cmd = Command::new("psql");
    cmd.args([
        "-U",
        POSTGRES_USER,
        "-h",
        PG_SOCKET_DIR,
        "-p",
        &PG_PORT.to_string(),
        "-d",
        "postgres",
        "-c",
        sql,
    ])
    .stdout(std::process::Stdio::null())
    .stderr(std::process::Stdio::piped());

    for (k, v) in env {
        cmd.env(k, v);
    }

    let out = cmd.output().await?;
    if out.status.success() {
        return Ok(());
    }
    let code = out.status.code().ok_or(PgError::Signal)?;
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    Err(PgError::NonZeroExit { code, stderr })
}

/// Set the superuser password using dollar-quoting to avoid SQL injection.
///
/// Dollar-quoting avoids single-quote injection. The password is MMDS-controlled
/// but correct quoting is still the right habit.
pub async fn set_superuser_password(pw: &str) -> Result<(), PgError> {
    // Dollar-quote tag chosen to be unlikely to appear in a password.
    // If the password itself contains `$_beyond_$`, this breaks — document that
    // as an unsupported edge case in the MMDS field.
    psql(&format!(
        "ALTER ROLE postgres WITH PASSWORD $_beyond_${pw}$_beyond_$"
    ))
    .await
}

// ---------------------------------------------------------------------------
// pg_ctl
// ---------------------------------------------------------------------------

/// Send `SIGHUP` to Postgres (reload config without restart).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub async fn reload() -> Result<(), PgError> {
    let out = Command::new("pg_ctl")
        .args(["reload", "-D", PGDATA])
        .stderr(std::process::Stdio::piped())
        .output()
        .await?;

    if out.status.success() {
        return Ok(());
    }
    let code = out.status.code().ok_or(PgError::Signal)?;
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    Err(PgError::NonZeroExit { code, stderr })
}

// ---------------------------------------------------------------------------
// initdb
// ---------------------------------------------------------------------------

/// Run `initdb` to initialize a new database cluster.
///
/// `pwfile_path` must point to a `0o600` file containing only the superuser
/// password (created by the caller as a `tempfile::NamedTempFile`).
pub async fn initdb(pgdata: &str, pwfile_path: &str) -> Result<(), PgError> {
    debug!("running initdb in {pgdata}");
    let out = Command::new("initdb")
        .args([
            "-D",
            pgdata,
            "--waldir",
            "/var/lib/postgresql/18/wal",
            "--auth=scram-sha-256",
            "--encoding=UTF8",
            "--locale=en_US.UTF-8",
            &format!("--pwfile={pwfile_path}"),
        ])
        .stderr(std::process::Stdio::piped())
        .output()
        .await?;

    if out.status.success() {
        debug!("initdb complete");
        return Ok(());
    }
    let code = out.status.code().ok_or(PgError::Signal)?;
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    warn!("initdb failed (exit {code}): {stderr}");
    Err(PgError::NonZeroExit { code, stderr })
}

// ---------------------------------------------------------------------------
// pg_basebackup (stub)
// ---------------------------------------------------------------------------

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub async fn basebackup(_target: &str) -> Result<(), PgError> {
    Err(PgError::NonZeroExit {
        code: -1,
        stderr: "pg_basebackup not yet implemented".into(),
    })
}
