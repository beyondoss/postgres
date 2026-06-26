//! Build-time PGDATA template builder (`beyond-pg build-template <dir>`).
//!
//! Produces an already-initialized PostgreSQL cluster — `initdb` output PLUS the
//! full `CREATE EXTENSION` suite — baked into the rootfs at image-build time. At
//! first boot, [`crate::boot::maybe_initdb`] copies this template onto the fresh
//! data volume instead of running `initdb` + `CREATE EXTENSION` in the guest,
//! removing both from the cold-boot critical path.
//!
//! The template layout mirrors what the runtime expects on the data volume:
//!
//! ```text
//! <dir>/main   → PGDATA contents (becomes /var/lib/postgresql/18/main)
//! <dir>/wal    → WAL contents    (becomes /var/lib/postgresql/18/wal)
//! ```
//!
//! `<dir>/main/pg_wal` is an absolute symlink to `<dir>/wal` here; the materialize
//! step repoints it to the runtime [`crate::pg::PG_WALDIR`] after copying.
//!
//! Per-instance state is intentionally NOT baked in: the superuser/replicator
//! passwords and the pgbouncer/replicator roles are applied on every boot by the
//! supervisor's `post_start`, so a shared build-time password is never exposed.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use tokio::process::Command;
use tracing::{info, warn};

use crate::pg;

/// Where the baked template lives in the rootfs. Outside `/var/lib/postgresql`
/// (which the data volume shadows at runtime) so it survives into the image.
pub const TEMPLATE_DIR: &str = "/usr/local/share/beyond-pg/pgdata-template";

/// Throwaway superuser password used only while building the template. The
/// runtime resets the superuser password from the MMDS secret on every boot
/// (`supervisor::post_start` → `pg::set_superuser_password`), so this value
/// never reaches a running instance.
const BUILD_PASSWORD: &str = "beyond-build-template";

#[derive(Debug, thiserror::Error)]
pub enum TemplateError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("initdb failed: {0}")]
    InitDb(String),
    #[error("postgres did not become ready within {0:?}")]
    NotReady(Duration),
    #[error("required extension {ext} failed: {source}")]
    RequiredExtension { ext: String, source: pg::PgError },
    #[error("postgres shutdown failed: {0}")]
    Shutdown(String),
}

/// Entry point for `beyond-pg build-template <dir>`.
pub async fn run(dir: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    if let Err(e) = build(dir).await {
        eprintln!("[build-template] FATAL: {e}");
        std::process::exit(1);
    }
    info!("build-template complete: {dir}");
}

/// Build the PGDATA template at `dir`. Idempotent: removes any prior contents
/// and rebuilds from scratch.
pub async fn build(dir: &str) -> Result<(), TemplateError> {
    let main = format!("{dir}/main");
    let wal = format!("{dir}/wal");

    // Clean rebuild — initdb refuses a non-empty -D / --waldir.
    if Path::new(dir).exists() {
        std::fs::remove_dir_all(dir)?;
    }
    std::fs::create_dir_all(dir)?;
    chown_postgres(dir);

    // initdb (and the build-time postgres) run as the postgres OS user; the
    // socket dir must exist and be postgres-writable, same as runtime boot.
    ensure_socket_dir();

    info!("build-template: initdb → {main} (waldir {wal})");
    run_initdb(&main, &wal).await?;
    // initdb creates the cluster as postgres; keep the whole tree postgres-owned.
    chown_postgres(dir);

    info!("build-template: starting postgres to create extensions");
    let mut child = spawn_build_postgres(&main)?;

    let extensions_result = create_extensions().await;

    // Always attempt a clean (fast) shutdown so the template carries a
    // shutdown checkpoint, even if extension creation failed.
    let shutdown_result = shutdown_postgres(&mut child).await;

    extensions_result?;
    shutdown_result?;

    chown_postgres(dir);
    info!("build-template: cluster + extensions baked at {dir}");
    Ok(())
}

async fn run_initdb(main: &str, wal: &str) -> Result<(), TemplateError> {
    let pwfile = tempfile::Builder::new()
        .prefix("pg-build-pwfile-")
        .tempfile_in("/run/")
        .map_err(TemplateError::Io)?;
    std::fs::set_permissions(pwfile.path(), std::fs::Permissions::from_mode(0o600))?;
    std::fs::write(pwfile.path(), BUILD_PASSWORD)?;
    let _ = std::process::Command::new("chown")
        .args(["postgres:postgres", pwfile.path().to_str().unwrap_or("")])
        .status();

    let path_str = pwfile
        .path()
        .to_str()
        .ok_or_else(|| TemplateError::Io(std::io::Error::other("pwfile path is not UTF-8")))?;

    // Same canonical flag set as the runtime first-boot path (single source in
    // pg::initdb), but WAL is template-relative — the symlink is repointed on copy.
    pg::initdb(main, wal, path_str)
        .await
        .map_err(|e| TemplateError::InitDb(e.to_string()))
}

/// Spawn a build-time postgres on the unix socket only, with the same filtered
/// `shared_preload_libraries` the runtime uses (so `pg_cron` / `beyond_queue`
/// `CREATE EXTENSION` succeed). TLS is off (no certs at build time) and there is
/// no TCP listener — this postgres is reachable only via the local socket that
/// `pg::psql` targets.
fn spawn_build_postgres(main: &str) -> Result<tokio::process::Child, TemplateError> {
    let preload = crate::config::preload_libraries();
    info!("build-template: shared_preload_libraries='{preload}'");
    let port = pg::PG_PORT.to_string();

    let mut cmd = Command::new("postgres");
    cmd.arg("-D").arg(main);
    for (k, v) in [
        ("shared_preload_libraries", preload.as_str()),
        ("listen_addresses", ""),
        ("ssl", "off"),
        ("cron.database_name", "postgres"),
        ("logging_collector", "off"),
        ("port", port.as_str()),
        ("unix_socket_directories", pg::PG_SOCKET_DIR),
        // `initdb --auth=scram-sha-256` made the template's own pg_hba require a
        // password for local socket connections; point at the shipped hba (same
        // file the runtime uses) whose `local all all peer` lets the build-time
        // `psql` authenticate as the postgres OS user without a password.
        ("hba_file", crate::config::PG_HBA_PATH),
    ] {
        cmd.arg("-c").arg(format!("{k}={v}"));
    }
    cmd.stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .stdin(std::process::Stdio::null());
    // postgres refuses to run as root; drop to the postgres OS user.
    pg::drop_to_postgres_user(&mut cmd);
    cmd.spawn().map_err(TemplateError::Io)
}

async fn create_extensions() -> Result<(), TemplateError> {
    let timeout = Duration::from_secs(60);
    if !pg::wait_until_ready(timeout).await {
        return Err(TemplateError::NotReady(timeout));
    }

    // Mirror supervisor::post_start exactly: required extensions are fatal when
    // their `.so` is installed; optional extensions are best-effort. Reusing the
    // same lists + `extension_installed` filter keeps the baked set identical to
    // what the runtime would auto-create (so post_start's IF-NOT-EXISTS calls
    // become no-ops on first boot).
    for ext in crate::supervisor::REQUIRED_EXTENSIONS {
        if !crate::supervisor::extension_installed(ext) {
            warn!("build-template: required extension {ext} not installed; skipping");
            continue;
        }
        pg::psql(&format!("CREATE EXTENSION IF NOT EXISTS {ext}"))
            .await
            .map_err(|e| TemplateError::RequiredExtension {
                ext: (*ext).to_string(),
                source: e,
            })?;
        info!("build-template: created extension {ext}");
    }

    for ext in crate::supervisor::OPTIONAL_EXTENSIONS {
        if !crate::supervisor::extension_installed(ext) {
            continue;
        }
        match pg::psql(&format!("CREATE EXTENSION IF NOT EXISTS {ext}")).await {
            Ok(()) => info!("build-template: created extension {ext}"),
            Err(e) => warn!("build-template: optional extension {ext} failed: {e}"),
        }
    }
    Ok(())
}

/// Fast-shutdown the build postgres (SIGINT → checkpoint + clean stop) and wait
/// for it to exit, so the template carries a clean shutdown state.
async fn shutdown_postgres(child: &mut tokio::process::Child) -> Result<(), TemplateError> {
    if let Some(pid) = child.id() {
        // SIGINT = "fast" shutdown: roll back open txns, checkpoint, exit cleanly.
        // SAFETY: kill(2) with a known child pid and a fixed signal number.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGINT);
        }
    }
    let status = child.wait().await.map_err(TemplateError::Io)?;
    if !status.success() {
        return Err(TemplateError::Shutdown(format!("exit status {status}")));
    }
    Ok(())
}

/// `mkdir -p` + `chown postgres` the unix-socket dir (same as `boot::ensure_socket_dir`).
fn ensure_socket_dir() {
    let dir = pg::PG_SOCKET_DIR;
    if let Err(e) = std::fs::create_dir_all(dir) {
        warn!("build-template: create socket dir {dir}: {e}");
        return;
    }
    chown_postgres(dir);
}

fn chown_postgres(path: &str) {
    let _ = std::process::Command::new("chown")
        .args(["-R", "postgres:postgres", path])
        .status();
}
