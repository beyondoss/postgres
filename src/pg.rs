//! Thin wrappers around Postgres CLI tools (`pg_isready`, `psql`, `pg_ctl`,
//! `initdb`). All I/O goes through `tokio::process::Command`.

use std::time::{Duration, Instant};

use tokio::process::Command;
use tracing::{debug, warn};

/// Set uid/gid to the postgres OS user on Linux.
/// pg_hba.conf uses peer auth for Unix socket connections; all psql calls must
/// run as the postgres OS user so peer auth matches the "postgres" database user.
/// No-op on non-Linux (macOS dev uses host postgres which handles auth itself).
fn drop_to_postgres_user(
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] cmd: &mut Command,
) {
    #[cfg(target_os = "linux")]
    {
        let name = std::ffi::CString::new("postgres").expect("CString");
        // SAFETY: getpwnam is not thread-safe but this is called before any
        // concurrent thread could call it.  Pointer is valid until next call.
        let pw = unsafe { libc::getpwnam(name.as_ptr()) };
        if !pw.is_null() {
            let (uid, gid) = unsafe { ((*pw).pw_uid, (*pw).pw_gid) };
            cmd.uid(uid).gid(gid);
        }
    }
}

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
    #[error("PGDATA has PG_VERSION but no standby.signal — this is a primary data dir")]
    AlreadyPrimary,
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

    // pg_hba.conf uses peer auth for Unix socket connections.  The supervisor
    // runs as root (PID 1); we must drop to the postgres OS user so peer auth
    // matches the database user "postgres".
    drop_to_postgres_user(&mut cmd);

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
    set_role_password("postgres", pw).await
}

/// Set a role's password using dollar-quoting (same injection guard as
/// `set_superuser_password`). `role` is a fixed internal identifier (never user
/// input); `pw` is MMDS-controlled and dollar-quoted.
pub async fn set_role_password(role: &str, pw: &str) -> Result<(), PgError> {
    // Dollar-quote tag chosen to be unlikely to appear in a password. If the
    // password itself contains `$_beyond_$`, this breaks — documented as an
    // unsupported edge case in the MMDS field.
    psql(&format!(
        "ALTER ROLE {role} WITH PASSWORD $_beyond_${pw}$_beyond_$"
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
    let mut cmd = Command::new("initdb");
    cmd.args([
        "-D",
        pgdata,
        "--waldir",
        "/var/lib/postgresql/18/wal",
        "--auth=scram-sha-256",
        "--encoding=UTF8",
        "--locale=en_US.UTF-8",
        &format!("--pwfile={pwfile_path}"),
    ])
    .stderr(std::process::Stdio::piped());
    // initdb refuses to run as root; run it as the postgres OS user (the same
    // user that will own the cluster and that postgres/psql drop to). Required
    // when PGDATA is a fresh durable volume initialized at runtime (not baked
    // into the image as ephemeral data). The caller chowns the tree first.
    drop_to_postgres_user(&mut cmd);
    let out = cmd.output().await?;

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
// pg_basebackup
// ---------------------------------------------------------------------------

/// Seed a replica PGDATA from the primary via `pg_basebackup`.
///
/// Idempotent:
/// - `PG_VERSION` + `standby.signal` present → already seeded, returns `Ok(())`.
/// - `PG_VERSION` without `standby.signal` → `Err(AlreadyPrimary)` (refuse to overwrite).
/// - Neither present → runs `pg_basebackup`.
///
/// Does not pass `-R`; the caller writes `04-replica.conf` and touches
/// `standby.signal` so those files are owned by beyond-pg, not pg_basebackup.
///
/// `--waldir` is passed so pg_basebackup creates `pg_wal/` as a symlink to
/// `/var/lib/postgresql/18/wal`, matching the layout that `initdb --waldir`
/// produces and that `verify_wal_symlink()` expects.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub async fn basebackup(conninfo: &str) -> Result<(), PgError> {
    let pg_version = format!("{PGDATA}/PG_VERSION");
    let standby_signal = format!("{PGDATA}/standby.signal");

    if std::path::Path::new(&pg_version).exists() {
        return if std::path::Path::new(&standby_signal).exists() {
            tracing::info!(
                "basebackup: already seeded (PG_VERSION + standby.signal present), skipping"
            );
            Ok(())
        } else {
            Err(PgError::AlreadyPrimary)
        };
    }

    tracing::info!("basebackup: seeding replica PGDATA from primary");
    let out = Command::new("pg_basebackup")
        .args([
            "-d",
            conninfo,
            "--pgdata",
            PGDATA,
            "--waldir",
            "/var/lib/postgresql/18/wal",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
            "--progress",
        ])
        .stderr(std::process::Stdio::piped())
        .output()
        .await?;

    if out.status.success() {
        tracing::info!("basebackup: complete");
        return Ok(());
    }
    let code = out.status.code().ok_or(PgError::Signal)?;
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    tracing::warn!("basebackup failed (exit {code}): {stderr}");
    Err(PgError::NonZeroExit { code, stderr })
}

// ---------------------------------------------------------------------------
// pg_ctl promote
// ---------------------------------------------------------------------------

/// Promote this standby to primary via `pg_ctl promote`.
///
/// Blocks until Postgres has exited recovery mode (`-w`), up to 55 seconds.
/// Returns `Ok(())` only after promotion is complete — callers can rely on
/// `ok: true` from the RPC meaning the node is now a writable primary.
///
/// The 55 s internal limit matches the 60 s RPC_TIMEOUT in rpc.rs; promotion
/// under normal conditions completes in under 1 second.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub async fn promote() -> Result<(), PgError> {
    tracing::info!("pg_ctl: promoting standby to primary");
    let out = Command::new("pg_ctl")
        .args(["promote", "-w", "-t", "55", "-D", PGDATA])
        .stderr(std::process::Stdio::piped())
        .output()
        .await?;

    if out.status.success() {
        tracing::info!("pg_ctl: promotion complete — node is now primary");
        return Ok(());
    }
    let code = out.status.code().ok_or(PgError::Signal)?;
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    Err(PgError::NonZeroExit { code, stderr })
}
