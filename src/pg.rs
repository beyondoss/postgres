//! Thin wrappers around Postgres CLI tools (`pg_isready`, `psql`, `pg_ctl`,
//! `initdb`). All I/O goes through `tokio::process::Command`.

use std::time::{Duration, Instant};

use tokio::process::Command;
use tracing::{debug, warn};

/// Set uid/gid to the postgres OS user on Linux.
/// pg_hba.conf uses peer auth for Unix socket connections; all psql calls must
/// run as the postgres OS user so peer auth matches the "postgres" database user.
/// No-op on non-Linux (macOS dev uses host postgres which handles auth itself).
pub(crate) fn drop_to_postgres_user(
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
/// Runtime WAL directory. `initdb --waldir` points here and the cluster's
/// `main/pg_wal` symlink targets it. Kept off `PGDATA` so the WAL has its own
/// path on the data volume (matched by `boot::PG_WAL_TARGET`).
pub const PG_WALDIR: &str = "/var/lib/postgresql/18/wal";
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

/// PgBouncer's client-facing TCP port. Clients (and wave-1 services like
/// auth/queue dialing `postgres.<vpc>.internal:5432`) connect here, NOT to
/// Postgres direct (`PG_PORT` = 5433). PgBouncer is a separate process the
/// supervisor spawns after Postgres is ready.
pub const PGBOUNCER_PORT: u16 = 5432;

/// Returns `true` if PgBouncer is accepting TCP connections on its client port.
/// A successful connect means the pooler's listener is up — which is what
/// "ready for traffic" means for clients. Postgres-direct readiness
/// (`is_ready`, unix socket :5433) is necessary but not sufficient: a client
/// that sees `service.ready` and dials :5432 before PgBouncer binds gets
/// connection-refused.
pub async fn pgbouncer_accepting() -> bool {
    tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, PGBOUNCER_PORT))
        .await
        .is_ok()
}

/// Poll `pgbouncer_accepting()` every 100 ms until it returns `true` or
/// `timeout` elapses.
pub async fn wait_until_pgbouncer_ready(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pgbouncer_accepting().await {
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

/// Number of client backends currently running a query (excluding the caller).
///
/// Used to observe when PgBouncer has actually finished pausing: `PAUSE` releases
/// each server connection as it goes idle, so once no client backend is executing
/// anything, there is no in-flight work for a fast shutdown to abort.
///
/// `None` if the query fails (postgres already down, or unreachable) — the caller
/// treats that as "nothing in flight", which is the safe reading: there is no
/// live transaction to protect.
pub async fn active_backends() -> Option<u32> {
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
        "-tAc",
        "SELECT count(*) FROM pg_stat_activity \
         WHERE backend_type = 'client backend' \
           AND pid <> pg_backend_pid() \
           AND state <> 'idle'",
    ])
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::null());
    drop_to_postgres_user(&mut cmd);

    let out = cmd.output().await.ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// Wait until no client backend is running a query, up to `timeout`.
///
/// Returns the time actually waited. This replaces a blind fixed sleep after
/// signalling PgBouncer to PAUSE: in the common case the pool drains in
/// milliseconds, and the retune's client-visible stall shrinks accordingly.
pub async fn wait_quiesced(timeout: Duration) -> Duration {
    let start = Instant::now();
    loop {
        match active_backends().await {
            // 0 = drained. None = postgres unreachable ⇒ nothing to drain.
            Some(0) | None => return start.elapsed(),
            Some(_) if start.elapsed() >= timeout => return start.elapsed(),
            Some(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}

/// Build the dollar-quoted `ALTER ROLE … WITH PASSWORD` statement (no trailing
/// semicolon, so it composes into a larger script). Dollar-quoting guards against
/// single-quote injection; the `$_beyond_$` tag is unlikely to appear in a
/// password (if it does, this breaks — a documented unsupported MMDS edge case).
pub fn alter_role_password_sql(role: &str, pw: &str) -> String {
    format!("ALTER ROLE {role} WITH PASSWORD $_beyond_${pw}$_beyond_$")
}

/// Run a multi-statement SQL script in a SINGLE `psql` invocation, aborting on
/// the first error (`ON_ERROR_STOP=1`). Collapses the per-statement process
/// spawn + reconnect overhead of calling [`psql`] once per statement — used by
/// post-start setup, which is a fixed batch of idempotent statements.
pub async fn psql_script(sql: &str) -> Result<(), PgError> {
    use tokio::io::AsyncWriteExt as _;
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
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        "-",
    ])
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::null())
    .stderr(std::process::Stdio::piped());
    // Same peer-auth requirement as `psql`: drop to the postgres OS user.
    drop_to_postgres_user(&mut cmd);

    let mut child = cmd.spawn()?;
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| PgError::Io(std::io::Error::other("psql stdin unavailable")))?;
        stdin.write_all(sql.as_bytes()).await?;
        stdin.shutdown().await?;
    }
    let out = child.wait_with_output().await?;
    if out.status.success() {
        return Ok(());
    }
    let code = out.status.code().ok_or(PgError::Signal)?;
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    Err(PgError::NonZeroExit { code, stderr })
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
/// password (created by the caller as a `tempfile::NamedTempFile`). `waldir` is
/// where the WAL lives (runtime: [`PG_WALDIR`]; the template builder targets a
/// template-relative dir so the baked `pg_wal` symlink is repointed on copy).
///
/// This is the single source of the canonical `initdb` flag set — both the
/// runtime first-boot path and the build-time template builder call it, so the
/// pre-baked cluster can never drift from what the runtime would have produced.
pub async fn initdb(pgdata: &str, waldir: &str, pwfile_path: &str) -> Result<(), PgError> {
    debug!("running initdb in {pgdata} (waldir {waldir})");
    let mut cmd = Command::new("initdb");
    cmd.args([
        "-D",
        pgdata,
        "--waldir",
        waldir,
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
