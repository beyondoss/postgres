//! Idempotent every-boot setup for the Postgres data volume.
//!
//! Runs at every boot (image swap, resize, first boot, fork). Detects state
//! from the data volume and takes the minimum action needed. Safe to re-run
//! any number of times — same result as running once.
//!
//! Exposed as `beyond-pg boot` for operator re-execution and called inline by
//! `beyond-pg supervisor` before starting Postgres.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tokio::process::Command;
use tracing::{error, info, warn};

use crate::config::{self, PG_HBA_PATH, PGBOUNCER_INI_PATH, SYSCTL_CONF, SYSCTL_PATH, THP_PATH};
use crate::mmds::{MmdsConfig, MmdsError, PgTier};
use crate::pg::{self, PGDATA};

const PG_WAL_LINK: &str = "/var/lib/postgresql/18/main/pg_wal";
const PG_WAL_TARGET: &str = crate::pg::PG_WALDIR;
const HOOKS_PRE_START: &str = "/etc/postgresql/18/hooks/pre-start.d";

#[derive(Debug, thiserror::Error)]
pub enum BootError {
    #[error("MMDS error: {0}")]
    Mmds(#[from] MmdsError),
    #[error("pg_wal exists as a plain directory — operator must convert to symlink")]
    WalIsDirectory,
    #[error("pg_wal symlink points to wrong target: expected {expected}, got {actual}")]
    WalWrongTarget { expected: String, actual: String },
    #[error("initdb failed: {0}")]
    InitDb(String),
    #[error("pg_basebackup failed: {0}")]
    BaseBackup(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS cert error: {0}")]
    Tls(#[from] crate::tls::TlsError),
    #[error("hook script {path} failed with exit code {code}")]
    HookFailed { path: String, code: i32 },
}

/// Entry point for `beyond-pg boot` subcommand.
pub async fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cfg = match crate::mmds::read().await {
        Ok(c) => c,
        Err(e) => {
            error!("boot: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = do_boot(&cfg).await {
        error!("boot failed: {e}");
        std::process::exit(1);
    }

    info!("boot complete");
}

/// Idempotent boot-time setup. Called by `supervisor` before spawning Postgres.
pub async fn do_boot(cfg: &MmdsConfig) -> Result<(), BootError> {
    ensure_socket_dir();
    match cfg.pg_tier {
        PgTier::Single | PgTier::Primary => do_boot_primary(cfg).await,
        PgTier::Replica => do_boot_replica(cfg).await,
    }
}

/// Ensure the Postgres unix-socket directory exists and is owned by postgres.
///
/// `/run/postgresql` (= `/var/run/postgresql`, [`pg::PG_SOCKET_DIR`]) is normally
/// materialized by systemd-tmpfiles, but the Beyond VM has no systemd — PID 1 is
/// `beyond-pg-init`. Without it, Postgres dies at startup with
/// `could not create lock file "/var/run/postgresql/.s.PGSQL.<port>.lock"`.
/// Idempotent: a no-op when the dir already exists with the right owner.
fn ensure_socket_dir() {
    let dir = pg::PG_SOCKET_DIR;
    if let Err(e) = std::fs::create_dir_all(dir) {
        warn!("ensure_socket_dir: create {dir}: {e}");
        return;
    }
    match std::process::Command::new("chown")
        .args(["postgres:postgres", dir])
        .status()
    {
        Ok(s) if s.success() => info!("socket dir {dir} ready (owner postgres)"),
        Ok(s) => warn!("chown {dir} exited {s}; continuing"),
        Err(e) => warn!("chown {dir} failed to spawn: {e}; continuing"),
    }
}

/// Create a per-worker PgBouncer peer socket directory owned by `postgres`.
/// Idempotent: create_dir_all is a no-op when it exists, and re-chowning is safe.
/// The socket inside (`.s.PGSQL.5432`) is created by pgbouncer after it drops to
/// the postgres user, so the directory must be writable by that user.
fn ensure_peer_socket_dir(peer_id: usize) {
    let dir = config::pgb_peer_socket_dir(peer_id);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        warn!("ensure_peer_socket_dir: create {dir}: {e}");
        return;
    }
    match std::process::Command::new("chown")
        .args(["postgres:postgres", &dir])
        .status()
    {
        Ok(s) if s.success() => info!("pgbouncer peer socket dir {dir} ready (owner postgres)"),
        Ok(s) => warn!("chown {dir} exited {s}; continuing"),
        Err(e) => warn!("chown {dir} failed to spawn: {e}; continuing"),
    }
}

async fn do_boot_primary(cfg: &MmdsConfig) -> Result<(), BootError> {
    info!("boot: step 1/8 maybe_initdb");
    maybe_initdb(cfg).await?;
    info!("boot: step 2/8 ensure_conf_d");
    ensure_conf_d()?;
    info!("boot: step 3/8 verify_wal_symlink");
    verify_wal_symlink()?;
    info!("boot: step 4/8 fetch_wal_gap");
    fetch_wal_gap(cfg).await?;

    info!("boot: step 5/8 tls::provision");
    let tls = crate::tls::provision(Path::new(PGDATA))?;
    info!("tls: source={:?} cert={}", tls.source, tls.cert.display());

    info!("boot: step 6/8 write_config_files");
    write_config_files(cfg, &tls)?;
    info!("boot: step 7/8 write_pitr_config");
    write_pitr_config(cfg)?;
    let shared_buffers_mb = (cfg.ram_bytes / (1024 * 1024) / 4).max(128);
    apply_kernel_settings(shared_buffers_mb);

    info!("boot: step 8/8 run_hook_scripts");
    run_hook_scripts(HOOKS_PRE_START).await?;

    info!("boot: do_boot_primary complete");
    Ok(())
}

async fn do_boot_replica(cfg: &MmdsConfig) -> Result<(), BootError> {
    // primary_conninfo is guaranteed Some for Replica by mmds::parse().
    let conninfo = cfg
        .primary_conninfo
        .as_deref()
        .expect("primary_conninfo required for replica — guaranteed by mmds::parse");

    // Seed PGDATA from the primary (idempotent — skips if already done).
    // ensure_conf_d must come AFTER basebackup: pg_basebackup refuses a non-empty
    // target directory, and create_dir_all(PGDATA/conf.d) would make it non-empty.
    pg::basebackup(conninfo)
        .await
        .map_err(|e| BootError::BaseBackup(e.to_string()))?;

    // Touch standby.signal (idempotent — pg_basebackup does not write it since
    // we don't pass -R; we own this file explicitly for auditability).
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(format!("{PGDATA}/standby.signal"))?;
    info!("standby.signal: present");

    // conf.d must exist before write_config_files; safe to create now that
    // pg_basebackup has populated PGDATA (creating it beforehand would cause
    // pg_basebackup to refuse the non-empty target directory).
    ensure_conf_d()?;
    verify_wal_symlink()?;

    let tls = crate::tls::provision(Path::new(PGDATA))?;
    info!("tls: source={:?} cert={}", tls.source, tls.cert.display());

    // Standard config files (tuning, memory, hba, pgbouncer, etc.)
    write_config_files(cfg, &tls)?;

    // Replica-specific config: primary_conninfo + optional restore_command.
    let replica_conf = config::replica_conf(conninfo, cfg.wal_sink.as_deref());
    config::write_atomic(Path::new(&config::replica_conf_path()), &replica_conf)?;
    info!("wrote {}", config::replica_conf_path());

    // pg_basebackup runs as root, so PGDATA + everything we just wrote is
    // root-owned; postgres refuses to start on a non-postgres-owned data dir.
    chown_data_tree();

    let shared_buffers_mb = (cfg.ram_bytes / (1024 * 1024) / 4).max(128);
    apply_kernel_settings(shared_buffers_mb);

    run_hook_scripts(HOOKS_PRE_START).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// conf.d directory
// ---------------------------------------------------------------------------

fn ensure_conf_d() -> Result<(), BootError> {
    let dir = config::conf_d_dir();
    std::fs::create_dir_all(&dir)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// initdb (first boot only)
// ---------------------------------------------------------------------------

async fn maybe_initdb(cfg: &MmdsConfig) -> Result<(), BootError> {
    let pg_version = format!("{PGDATA}/PG_VERSION");

    if Path::new(&pg_version).exists() {
        info!("PGDATA already initialized, skipping initdb");
        return Ok(());
    }

    // Detect partial PGDATA (initdb failed partway through on a previous boot):
    // PG_VERSION absent but directory non-empty → clean up and retry.
    if Path::new(PGDATA).exists() {
        let mut entries = std::fs::read_dir(PGDATA)?;
        if entries.next().is_some() {
            warn!("partial PGDATA detected (no PG_VERSION but directory non-empty), cleaning up");
            std::fs::remove_dir_all(PGDATA)?;
            std::fs::create_dir_all(PGDATA)?;
            info!("partial PGDATA removed, will re-run initdb");
        }
    } else {
        std::fs::create_dir_all(PGDATA)?;
    }

    chown_data_tree();

    // Fast path: a pre-baked PGDATA template shipped in the rootfs (image build
    // ran `beyond-pg build-template`). Copy it onto the fresh volume instead of
    // running initdb (+ first-boot CREATE EXTENSION) in the guest — both come off
    // the cold-boot critical path. The supervisor's `post_start` still resets the
    // per-instance superuser password and runs idempotent `CREATE EXTENSION IF NOT
    // EXISTS` (no-ops against the baked extensions), so the result is identical to
    // a runtime initdb.
    if template_available() {
        info!("materializing PGDATA from template, skipping initdb");
        materialize_template()?;
        return Ok(());
    }

    run_initdb(&cfg.postgres_password).await?;
    Ok(())
}

/// True iff a complete pre-baked PGDATA template is present in the rootfs.
fn template_available() -> bool {
    template_available_in(crate::template::TEMPLATE_DIR)
}

fn template_available_in(template_dir: &str) -> bool {
    Path::new(&format!("{template_dir}/main/PG_VERSION")).exists()
}

/// Copy the baked PGDATA template onto the fresh data volume, then chown it.
fn materialize_template() -> Result<(), BootError> {
    materialize_template_into(crate::template::TEMPLATE_DIR, PGDATA, PG_WAL_TARGET)?;
    chown_data_tree();
    info!("materialized PGDATA from template");
    Ok(())
}

/// Copy the template at `template_dir` (`main/` + `wal/`) onto `pgdata` + `wal_target`.
///
/// Atomic + idempotent (per CLAUDE.md): the template is copied into staging dirs
/// alongside the destinations, the `pg_wal` symlink is repointed to `wal_target`,
/// the staged data is flushed, and only then are the staging dirs renamed into
/// place. `pgdata/PG_VERSION` therefore becomes visible only once a complete,
/// correct PGDATA is published — so `maybe_initdb`'s skip-on-`PG_VERSION` is safe
/// and an interrupted materialize is simply redone on the next boot.
fn materialize_template_into(
    template_dir: &str,
    pgdata: &str,
    wal_target: &str,
) -> Result<(), BootError> {
    let tmpl_main = format!("{template_dir}/main");
    let tmpl_wal = format!("{template_dir}/wal");
    let main_staging = format!("{pgdata}.staging");
    let wal_staging = format!("{wal_target}.staging");

    // Idempotent clean slate. `pgdata` may exist empty (partial-PGDATA cleanup
    // recreated it above); a prior interrupted materialize may have left a wal
    // dir or *.staging dirs.
    for p in [&main_staging, &wal_staging, &pgdata.to_string(), &wal_target.to_string()] {
        rm_rf(p)?;
    }

    // `cp -a` preserves the pg_wal symlink, file modes, and postgres ownership.
    cp_a(&tmpl_wal, &wal_staging)?;
    cp_a(&tmpl_main, &main_staging)?;

    // Repoint the baked `pg_wal` symlink (template-relative) to the runtime WAL
    // dir BEFORE publishing, so a crash after the rename never leaves a wrong
    // target that `verify_wal_symlink` would reject.
    let staged_link = format!("{main_staging}/pg_wal");
    match std::fs::symlink_metadata(&staged_link) {
        Ok(_) => std::fs::remove_file(&staged_link)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(BootError::Io(e)),
    }
    std::os::unix::fs::symlink(wal_target, &staged_link)?;

    // Flush the staged tree (file contents + dir entries) so the data is durable
    // BEFORE it is published. Then rename atomically — WAL first, then PGDATA, so
    // `PG_VERSION` only appears once `main` is renamed, by which point both the
    // symlink and the data are correct and durable. Finally fsync the parent dir
    // to make the renames themselves durable. Targeted fsync (not a global
    // sync(2)) keeps this fast and non-blocking under concurrent I/O.
    fsync_tree(Path::new(&wal_staging))?;
    fsync_tree(Path::new(&main_staging))?;
    std::fs::rename(&wal_staging, wal_target)?;
    std::fs::rename(&main_staging, pgdata)?;
    if let Some(parent) = Path::new(pgdata).parent() {
        fsync_dir(parent)?;
    }

    Ok(())
}

/// Recursively fsync every file and directory under `path` (depth-first, so each
/// directory is fsynced after its entries). Symlinks are not followed — the
/// containing directory's fsync makes the symlink entry durable.
fn fsync_tree(path: &Path) -> Result<(), BootError> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.is_dir() {
        for entry in std::fs::read_dir(path)? {
            fsync_tree(&entry?.path())?;
        }
        fsync_dir(path)?;
    } else if meta.is_file() {
        std::fs::File::open(path)?.sync_all()?;
    }
    Ok(())
}

/// fsync a directory (durably commit its entries). On Linux a directory can be
/// opened read-only and `fsync`'d via `sync_all`.
fn fsync_dir(path: &Path) -> Result<(), BootError> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

/// `rm -rf` a path (file, symlink, or directory). Idempotent — a missing path is
/// not an error.
fn rm_rf(path: &str) -> Result<(), BootError> {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(BootError::Io(e)),
        Ok(meta) if meta.is_dir() => Ok(std::fs::remove_dir_all(path)?),
        Ok(_) => Ok(std::fs::remove_file(path)?),
    }
}

/// `cp -a src dst` — recursive copy preserving symlinks, modes, and ownership.
fn cp_a(src: &str, dst: &str) -> Result<(), BootError> {
    let status = std::process::Command::new("cp")
        .args(["-a", src, dst])
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(BootError::Io(std::io::Error::other(format!(
            "cp -a {src} {dst} failed: {status}"
        ))))
    }
}


/// chown `/var/lib/postgresql` → postgres recursively. A fresh durable volume
/// mounts root-owned over the image dir, and a root-run `pg_basebackup` (replica
/// seeding) writes a root-owned PGDATA — either way postgres dies at startup with
/// `could not access directory "…/main": Permission denied` unless the data dir
/// is postgres-owned. Idempotent / harmless when already postgres-owned.
fn chown_data_tree() {
    match std::process::Command::new("chown")
        .args(["-R", "postgres:postgres", "/var/lib/postgresql"])
        .status()
    {
        Ok(s) if s.success() => info!("chowned /var/lib/postgresql → postgres"),
        Ok(s) => warn!("chown /var/lib/postgresql exited {s}; continuing"),
        Err(e) => warn!("chown /var/lib/postgresql failed to spawn: {e}; continuing"),
    }
}

async fn run_initdb(password: &str) -> Result<(), BootError> {
    // Write password to a 0600 tempfile under /run/. The NamedTempFile is a
    // Drop guard — file is removed even if initdb fails.
    let pwfile = tempfile::Builder::new()
        .prefix("pg-pwfile-")
        .tempfile_in("/run/")
        .map_err(BootError::Io)?;

    // Set 0600 before writing the password.
    std::fs::set_permissions(pwfile.path(), std::fs::Permissions::from_mode(0o600))?;
    std::fs::write(pwfile.path(), password)?;
    // initdb runs as the postgres user (see pg::initdb); the pwfile is created
    // root-owned 0600, so chown it to postgres so initdb can read it.
    let _ = std::process::Command::new("chown")
        .args(["postgres:postgres", pwfile.path().to_str().unwrap_or("")])
        .status();

    let path_str = pwfile
        .path()
        .to_str()
        .ok_or_else(|| BootError::Io(std::io::Error::other("tempfile path is not UTF-8")))?;
    info!("running initdb");
    pg::initdb(PGDATA, PG_WAL_TARGET, path_str)
        .await
        .map_err(|e| BootError::InitDb(e.to_string()))
    // pwfile is dropped here — tempfile removes it from disk
}

// ---------------------------------------------------------------------------
// pg_wal symlink verification
// ---------------------------------------------------------------------------

fn verify_wal_symlink() -> Result<(), BootError> {
    let link = Path::new(PG_WAL_LINK);

    match link.symlink_metadata() {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // initdb --waldir should have created the symlink. If it's missing,
            // something is wrong with the initdb step.
            return Err(BootError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("pg_wal symlink missing at {PG_WAL_LINK} after initdb"),
            )));
        }
        Err(e) => return Err(BootError::Io(e)),
        Ok(meta) => {
            if meta.file_type().is_dir() {
                // Pre-symlink layout or corrupted PGDATA — do not blindly overwrite.
                return Err(BootError::WalIsDirectory);
            }
            if meta.file_type().is_symlink() {
                let target = std::fs::read_link(link)?;
                let expected = Path::new(PG_WAL_TARGET);
                if target != expected {
                    return Err(BootError::WalWrongTarget {
                        expected: expected.display().to_string(),
                        actual: target.display().to_string(),
                    });
                }
                // Symlink is correct — nothing to do.
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// PITR config and recovery.signal
// ---------------------------------------------------------------------------

fn write_pitr_config(cfg: &MmdsConfig) -> Result<(), BootError> {
    write_pitr_config_into(cfg, Path::new(PGDATA))
}

fn write_pitr_config_into(cfg: &MmdsConfig, pgdata: &Path) -> Result<(), BootError> {
    let pitr_path = pgdata.join("conf.d/05-pitr.conf");
    let recovery_signal = pgdata.join("recovery.signal");

    // PITR mode = a recovery target is set. The WAL is replayed from the sink
    // (mmds::parse guarantees wal_sink is present whenever recovery_target_time
    // is). We write 05-pitr.conf (sink restore_command + recovery target) and
    // recovery.signal so Postgres replays to the target on the cloned snapshot
    // volume and then promotes.
    if cfg.recovery_target_time.is_some() {
        let sink = cfg
            .wal_sink
            .as_deref()
            .expect("wal_sink required for PITR — guaranteed by mmds::parse");
        let conf = config::pitr_conf(sink, cfg.recovery_target_time.as_deref());
        config::write_atomic(&pitr_path, &conf)?;
        info!("wrote {}", pitr_path.display());
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&recovery_signal)?;
        info!(
            "recovery.signal: created for PITR recovery to {:?}",
            cfg.recovery_target_time
        );
    } else {
        for p in [&pitr_path, &recovery_signal] {
            match std::fs::remove_file(p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(BootError::Io(e)),
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mmds::{MmdsConfig, PgTier};

    fn test_cfg(wal_sink: Option<&str>, recovery_target_time: Option<&str>) -> MmdsConfig {
        MmdsConfig {
            pg_tier: PgTier::Single,
            ephemeral: false,
            postgres_password: "test".into(),
            postgres_database: "postgres".into(),
            wal_sink: wal_sink.map(str::to_owned),
            cdc_enabled: false,
            recovery_target_time: recovery_target_time.map(str::to_owned),
            primary_conninfo: None,
            replication_password: None,
            ram_bytes: 4 * 1024 * 1024 * 1024,
            vcpus: 2,
        }
    }

    #[test]
    fn hugepages_nr_calculation() {
        // nr_hugepages = shared_buffers_mb / 2 + 32
        // shared_buffers_mb = (ram_bytes / (1024*1024) / 4).max(128)

        // 4 GB → shared_buffers = 1024 MB → nr = 544
        let ram = 4u64 * 1024 * 1024 * 1024;
        let sb = (ram / (1024 * 1024) / 4).max(128);
        assert_eq!(sb, 1024);
        assert_eq!(sb / 2 + 32, 544);

        // 512 MB → clamped to 128 MB → nr = 96
        let ram = 512u64 * 1024 * 1024;
        let sb = (ram / (1024 * 1024) / 4).max(128);
        assert_eq!(sb, 128);
        assert_eq!(sb / 2 + 32, 96);

        // 16 GB → shared_buffers = 4096 MB → nr = 2080
        let ram = 16u64 * 1024 * 1024 * 1024;
        let sb = (ram / (1024 * 1024) / 4).max(128);
        assert_eq!(sb, 4096);
        assert_eq!(sb / 2 + 32, 2080);
    }

    #[test]
    fn pitr_config_not_written_without_recovery_target() {
        let pgdata = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pgdata.path().join("conf.d")).unwrap();

        // A sink alone (no recovery target) is not PITR — a primary generates WAL,
        // it doesn't restore. No 05-pitr.conf / recovery.signal.
        let cfg = test_cfg(Some("http://10.0.0.5:9000"), None);
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();
        assert!(!pgdata.path().join("conf.d/05-pitr.conf").exists());
        assert!(!pgdata.path().join("recovery.signal").exists());
    }

    #[test]
    fn pitr_config_and_signal_written_in_pitr_mode() {
        let pgdata = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pgdata.path().join("conf.d")).unwrap();

        let cfg = test_cfg(Some("http://10.0.0.5:9000"), Some("2026-05-14 03:00:00"));
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();

        let content = std::fs::read_to_string(pgdata.path().join("conf.d/05-pitr.conf")).unwrap();
        assert!(
            content.contains("restore_command = 'curl -f -s http://10.0.0.5:9000/%f -o %p'"),
            "restore_command wrong: {content}"
        );
        assert!(
            content.contains("recovery_target_time = '2026-05-14 03:00:00'"),
            "{content}"
        );
        assert!(
            content.contains("recovery_target_action = promote"),
            "{content}"
        );
        assert!(
            content.contains("recovery_target_inclusive = true"),
            "{content}"
        );
        assert!(
            pgdata.path().join("recovery.signal").exists(),
            "recovery.signal must exist in PITR mode"
        );
    }

    #[test]
    fn pitr_config_removed_when_recovery_target_cleared() {
        let pgdata = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pgdata.path().join("conf.d")).unwrap();

        // First boot: PITR (sink + recovery target).
        let cfg = test_cfg(Some("http://10.0.0.5:9000"), Some("2026-05-14 03:00:00"));
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();
        assert!(pgdata.path().join("conf.d/05-pitr.conf").exists());
        assert!(pgdata.path().join("recovery.signal").exists());

        // Second boot: recovery completed / target cleared (the sink may remain
        // for the replica path) → PITR artifacts removed.
        let cfg = test_cfg(Some("http://10.0.0.5:9000"), None);
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();
        assert!(
            !pgdata.path().join("conf.d/05-pitr.conf").exists(),
            "05-pitr.conf should be removed when recovery target cleared"
        );
        assert!(
            !pgdata.path().join("recovery.signal").exists(),
            "recovery.signal should be removed when not in PITR mode"
        );
    }

    // -----------------------------------------------------------------------
    // PGDATA template materialize unit tests
    // -----------------------------------------------------------------------

    /// Build a minimal fake template at `dir`: `main/` with PG_VERSION + a
    /// template-relative `pg_wal` symlink, and `wal/` with a segment file.
    fn make_fake_template(dir: &Path) {
        let tmain = dir.join("main");
        let twal = dir.join("wal");
        std::fs::create_dir_all(&tmain).unwrap();
        std::fs::create_dir_all(&twal).unwrap();
        std::fs::write(tmain.join("PG_VERSION"), "18\n").unwrap();
        std::fs::write(tmain.join("postgresql.conf"), "# initdb default\n").unwrap();
        std::fs::write(twal.join("000000010000000000000001"), b"wal-seg").unwrap();
        // Baked symlink points template-relative — WRONG for runtime; the
        // materialize must repoint it to the real wal target.
        std::os::unix::fs::symlink(&twal, tmain.join("pg_wal")).unwrap();
    }

    #[test]
    fn template_available_detects_pg_version() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("template");
        assert!(!template_available_in(dir.to_str().unwrap()));
        std::fs::create_dir_all(dir.join("main")).unwrap();
        assert!(
            !template_available_in(dir.to_str().unwrap()),
            "empty main/ is not a usable template"
        );
        std::fs::write(dir.join("main/PG_VERSION"), "18").unwrap();
        assert!(template_available_in(dir.to_str().unwrap()));
    }

    #[test]
    fn materialize_copies_template_and_repoints_wal_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let template = tmp.path().join("template");
        make_fake_template(&template);

        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let pgdata = data.join("main");
        let wal_target = data.join("wal");

        materialize_template_into(
            template.to_str().unwrap(),
            pgdata.to_str().unwrap(),
            wal_target.to_str().unwrap(),
        )
        .unwrap();

        // PGDATA + WAL contents present.
        assert!(pgdata.join("PG_VERSION").exists());
        assert!(pgdata.join("postgresql.conf").exists());
        assert!(wal_target.join("000000010000000000000001").exists());
        // pg_wal repointed to the absolute runtime WAL target (what
        // verify_wal_symlink expects), not the template-relative path.
        assert_eq!(
            std::fs::read_link(pgdata.join("pg_wal")).unwrap(),
            wal_target
        );
        // No staging dirs left behind.
        assert!(!data.join("main.staging").exists());
        assert!(!data.join("wal.staging").exists());
    }

    #[test]
    fn materialize_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let template = tmp.path().join("template");
        make_fake_template(&template);
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let pgdata = data.join("main");
        let wal_target = data.join("wal");

        let run = || {
            materialize_template_into(
                template.to_str().unwrap(),
                pgdata.to_str().unwrap(),
                wal_target.to_str().unwrap(),
            )
        };
        run().unwrap();
        // A second materialize over a published PGDATA succeeds (clean-slate
        // removes the prior copy) and yields the same correct layout.
        run().unwrap();
        assert!(pgdata.join("PG_VERSION").exists());
        assert_eq!(
            std::fs::read_link(pgdata.join("pg_wal")).unwrap(),
            wal_target
        );
    }

    #[test]
    fn materialize_recovers_from_leftover_staging() {
        let tmp = tempfile::tempdir().unwrap();
        let template = tmp.path().join("template");
        make_fake_template(&template);
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let pgdata = data.join("main");
        let wal_target = data.join("wal");

        // Simulate an interrupted prior run: stale staging dirs + an empty PGDATA
        // (as partial-PGDATA cleanup would have recreated).
        std::fs::create_dir_all(data.join("main.staging")).unwrap();
        std::fs::write(data.join("main.staging/garbage"), b"x").unwrap();
        std::fs::create_dir_all(data.join("wal.staging")).unwrap();
        std::fs::create_dir_all(&pgdata).unwrap();

        materialize_template_into(
            template.to_str().unwrap(),
            pgdata.to_str().unwrap(),
            wal_target.to_str().unwrap(),
        )
        .unwrap();

        assert!(pgdata.join("PG_VERSION").exists());
        assert!(!pgdata.join("garbage").exists(), "stale staging must not leak in");
    }

    // -----------------------------------------------------------------------
    // run_hook_scripts unit tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn hook_missing_dir_returns_ok() {
        let result = run_hook_scripts("/nonexistent/beyond-pg-hook-test-dir").await;
        assert!(result.is_ok(), "missing dir should not error: {result:?}");
    }

    #[tokio::test]
    async fn hook_scripts_run_in_lexicographic_order() {
        let dir = tempfile::tempdir().unwrap();
        let order_file = tempfile::NamedTempFile::new().unwrap();
        let order_path = order_file.path().display().to_string();

        for (name, letter) in [("20-z.sh", "z"), ("10-a.sh", "a"), ("15-m.sh", "m")] {
            let script = format!("#!/bin/sh\necho {} >> {order_path}\n", letter);
            let path = dir.path().join(name);
            std::fs::write(&path, &script).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        run_hook_scripts(dir.path().to_str().unwrap())
            .await
            .unwrap();

        let output = std::fs::read_to_string(order_file.path()).unwrap();
        let letters: Vec<&str> = output.lines().collect();
        assert_eq!(
            letters,
            ["a", "m", "z"],
            "scripts not run in lexicographic order: {letters:?}"
        );
    }

    #[tokio::test]
    async fn hook_non_executable_files_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");

        // Non-executable script that would fail if run.
        let fail_path = dir.path().join("01-would-fail.sh");
        std::fs::write(&fail_path, "#!/bin/sh\nexit 1\n").unwrap();
        // Explicitly leave permissions at the default (non-executable).

        // Executable script that creates the marker — should run.
        let ok_path = dir.path().join("02-ok.sh");
        std::fs::write(&ok_path, format!("#!/bin/sh\ntouch {}\n", marker.display())).unwrap();
        std::fs::set_permissions(&ok_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        run_hook_scripts(dir.path().to_str().unwrap())
            .await
            .unwrap();
        assert!(
            marker.exists(),
            "02-ok.sh should have run and created the marker"
        );
    }

    #[tokio::test]
    async fn hook_nonzero_exit_returns_hook_failed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("01-fail.sh");
        std::fs::write(&path, "#!/bin/sh\nexit 42\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let result = run_hook_scripts(dir.path().to_str().unwrap()).await;
        match result {
            Err(BootError::HookFailed { code, .. }) => {
                assert_eq!(code, 42, "expected exit code 42, got {code}");
            }
            other => panic!("expected HookFailed, got {other:?}"),
        }
    }

    #[test]
    fn write_pitr_config_is_idempotent() {
        let pgdata = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pgdata.path().join("conf.d")).unwrap();

        let cfg = test_cfg(Some("s3://bucket/prefix"), Some("2026-05-14 03:00:00"));

        write_pitr_config_into(&cfg, pgdata.path()).unwrap();
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();

        // After three calls the files exist exactly once with correct content.
        let pitr = pgdata.path().join("conf.d/05-pitr.conf");
        assert!(pitr.exists());
        assert!(pgdata.path().join("recovery.signal").exists());
        let content = std::fs::read_to_string(&pitr).unwrap();
        assert!(content.contains("recovery_target_time = '2026-05-14 03:00:00'"));
    }
}

// ---------------------------------------------------------------------------
// WAL gap recovery from sink
// ---------------------------------------------------------------------------

/// Fetch WAL segments from the WAL sink that are missing from the local pg_wal
/// directory. Called after verify_wal_symlink and before write_config_files.
///
/// All network and pg_controldata errors are non-fatal: we warn and return Ok(())
/// so boot can proceed. If WAL is genuinely missing, Postgres will fail to start
/// and the supervisor will retry (which will re-run do_boot including this step).
async fn fetch_wal_gap(cfg: &MmdsConfig) -> Result<(), BootError> {
    let sink_url = match &cfg.wal_sink {
        Some(url) => url.clone(),
        None => return Ok(()),
    };

    // Parse the last needed WAL segment from pg_controldata.
    let redo_segment = match pg_controldata_redo_wal().await {
        Some(s) => s,
        None => {
            // PGDATA not initialized yet (first boot) or pg_controldata unavailable.
            return Ok(());
        }
    };

    info!(redo_segment, "wal-gap: fetching WAL list from sink");

    // Fetch the list of available segments from the sink.
    let list_body = match http_get(&format!("{sink_url}/list")).await {
        Ok(b) => b,
        Err(e) => {
            warn!("wal-gap: sink unreachable, skipping WAL fetch: {e}");
            return Ok(());
        }
    };

    let listing = match std::str::from_utf8(&list_body) {
        Ok(s) => s,
        Err(e) => {
            warn!("wal-gap: sink returned non-UTF-8 listing: {e}");
            return Ok(());
        }
    };

    let mut fetched = 0u32;

    for segment in listing.lines() {
        let segment = segment.trim();
        // WAL segment names are exactly 24 hex characters.
        if segment.len() != 24 || !segment.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        // Only fetch segments at or after the last checkpoint's REDO WAL file.
        // Lexicographic order is correct within a single timeline.
        if segment < redo_segment.as_str() {
            continue;
        }

        let dest = Path::new(PG_WAL_LINK).join(segment);
        if dest.exists() {
            continue;
        }

        match http_get(&format!("{sink_url}/{segment}")).await {
            Ok(bytes) => match config::write_atomic_bytes(&dest, &bytes) {
                Ok(()) => {
                    fetched += 1;
                    info!(segment, "wal-gap: fetched WAL segment");
                }
                Err(e) => warn!(segment, "wal-gap: failed to write segment: {e}"),
            },
            Err(e) => warn!(segment, "wal-gap: failed to fetch segment: {e}"),
        }
    }

    info!(fetched, "wal-gap: WAL gap recovery complete");
    Ok(())
}

/// Run `pg_controldata` and extract the `Latest checkpoint's REDO WAL file` value.
/// Returns `None` if PGDATA is not initialized or the output can't be parsed.
async fn pg_controldata_redo_wal() -> Option<String> {
    let output = tokio::process::Command::new("pg_controldata")
        .arg("-D")
        .arg(PGDATA)
        .output()
        .await
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = std::str::from_utf8(&output.stdout).ok()?;
    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix("Latest checkpoint's REDO WAL file:") {
            let segment = rest.trim().to_owned();
            if segment.len() == 24 && segment.chars().all(|c| c.is_ascii_hexdigit()) {
                return Some(segment);
            }
        }
    }

    None
}

/// Minimal HTTP/1.1 GET over a plain TCP connection. Returns the response body.
/// Only supports `http://` (no TLS needed — WAL sink is on a private overlay network).
async fn http_get(url: &str) -> Result<Vec<u8>, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let without_scheme = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("only http:// URLs are supported: {url}"))?;

    let (hostport, path) = match without_scheme.find('/') {
        Some(i) => (&without_scheme[..i], &without_scheme[i..]),
        None => (without_scheme, "/"),
    };

    let mut stream = TcpStream::connect(hostport)
        .await
        .map_err(|e| format!("connect {hostport}: {e}"))?;

    let request = format!("GET {path} HTTP/1.1\r\nHost: {hostport}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|e| format!("read: {e}"))?;

    // Split headers from body at \r\n\r\n.
    let header_end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "response missing header terminator".to_string())?;

    let header_section = std::str::from_utf8(&response[..header_end])
        .map_err(|e| format!("non-UTF-8 headers: {e}"))?;

    let status_line = header_section.lines().next().unwrap_or("");
    // Expect "HTTP/1.1 200 OK" or similar 2xx.
    let status_code: u32 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if status_code / 100 != 2 {
        return Err(format!("HTTP {status_line}"));
    }

    Ok(response[header_end + 4..].to_vec())
}

// ---------------------------------------------------------------------------
// Config file writes
// ---------------------------------------------------------------------------

fn write_config_files(cfg: &MmdsConfig, tls: &crate::tls::TlsConfig) -> Result<(), BootError> {
    use config::write_atomic;

    // 00-beyond.conf — image opinions, overwritten every boot. The
    // shared_preload_libraries list is filtered to the extensions actually
    // installed in this image so a missing module can't crash postgres at
    // startup (see config::beyond_conf).
    write_atomic(
        Path::new(&config::beyond_conf_path()),
        &config::beyond_conf(),
    )?;

    // 05-tls.conf — resolved cert paths, overrides 00-beyond.conf's defaults
    // via alpha order under conf.d/. Numbered 05 so it lands after 04-replica.
    write_atomic(Path::new(&config::tls_conf_path()), &config::tls_conf(tls))?;

    // 01-tuning.conf — postmaster-context params, written once at boot
    write_atomic(
        Path::new(&config::tuning_conf_path()),
        &config::tuning_conf_boot(cfg.ram_bytes, cfg.vcpus),
    )?;

    // 02-memory.conf — reload-safe params, also updated by memory watcher on hotplug
    write_atomic(
        Path::new(&config::memory_conf_path()),
        &config::tuning_conf_adaptive(cfg.ram_bytes, cfg.vcpus),
    )?;

    // 03-durability.conf — present only on ephemeral volumes
    let durability_path = config::durability_conf_path();
    if cfg.ephemeral {
        write_atomic(
            Path::new(&durability_path),
            config::DURABILITY_CONF_EPHEMERAL,
        )?;
    } else {
        match std::fs::remove_file(&durability_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(BootError::Io(e)),
        }
    }

    // 03-wal-sink.conf declares the sink as a SYNCHRONOUS standby
    // (synchronous_standby_names='wal_sink') — only valid on a node whose WAL a
    // sink consumes. A PITR restore node sets BEYOND_PG_WAL_SINK only as the
    // restore_command source (05-pitr.conf); it has no sink consuming from it, so
    // declaring a sync standby would hang its post-promote commits. Suppress it
    // whenever recovering.
    let wal_sink_path = config::wal_sink_conf_path();
    if cfg.wal_sink.is_some() && cfg.recovery_target_time.is_none() {
        write_atomic(Path::new(&wal_sink_path), &config::wal_sink_conf())?;
    } else {
        match std::fs::remove_file(&wal_sink_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(BootError::Io(e)),
        }
    }

    // pg_hba.conf — auth baseline, overwritten every boot. When CDC is enabled
    // we append a local-trust rule for the replicator role so beyond-pg-cdc
    // can connect over the unix socket without a password.
    let hba = if cfg.cdc_enabled {
        std::borrow::Cow::Owned(format!(
            "{}local replication replicator trust\n",
            config::PG_HBA_CONF
        ))
    } else {
        std::borrow::Cow::Borrowed(config::PG_HBA_CONF)
    };
    write_atomic(Path::new(PG_HBA_PATH), &hba)?;

    // pgbouncer.ini — one config per possible so_reuseport worker, overwritten every
    // boot, with client_tls_* keys pointing at the same cert Postgres uses. Workers
    // are spawned lazily by the scaler, but all their configs (and peer socket dirs)
    // are laid down up front so the shared [peers] map is stable and a scaled-up
    // worker never waits on config. peer_id 1 keeps the canonical pgbouncer.ini.
    if let Some(parent) = Path::new(PGBOUNCER_INI_PATH).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let max_workers = config::pgbouncer_max_workers(cfg.vcpus);
    for peer_id in 1..=max_workers {
        write_atomic(
            Path::new(&config::pgbouncer_ini_path(peer_id)),
            &config::pgbouncer_ini(tls, cfg.ram_bytes, cfg.vcpus, peer_id),
        )?;
        // Peer socket dir must exist and be writable by the postgres user pgbouncer
        // drops to (only created when peering is active, i.e. max_workers > 1).
        if max_workers > 1 {
            ensure_peer_socket_dir(peer_id);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Kernel settings
// ---------------------------------------------------------------------------

fn apply_kernel_settings(shared_buffers_mb: u64) {
    if let Err(e) = config::write_atomic(Path::new(SYSCTL_PATH), SYSCTL_CONF) {
        warn!("could not write {SYSCTL_PATH}: {e}");
    } else {
        match std::process::Command::new("sysctl")
            .args(["-p", SYSCTL_PATH])
            .status()
        {
            Ok(s) if s.success() => info!("sysctl applied {SYSCTL_PATH}"),
            Ok(s) => warn!("sysctl -p exited with {s}"),
            Err(e) => warn!("sysctl -p failed: {e}"),
        }
    }

    // Transparent hugepages — must be disabled before reserving static hugepages.
    // Best effort (may not be writable in containers).
    match std::fs::write(THP_PATH, "never\n") {
        Ok(()) => info!("transparent hugepages set to never"),
        Err(e) => warn!("could not set transparent hugepages: {e}"),
    }

    // Static hugepages: reserve enough 2 MB pages to back shared_buffers, plus
    // 32 pages (64 MB) of overhead for WAL buffers and other shared memory in
    // the same segment.
    // Percona benchmark: TLB faults drop from ~200k/s to near zero with hugepages.
    // Ref: Percona "Benchmark PostgreSQL with Linux HugePages";
    //      PostgreSQL docs §19.4 "Managing Kernel Resources"
    let nr_hugepages = shared_buffers_mb / 2 + 32;
    let hugepages_reserved =
        match std::fs::write("/proc/sys/vm/nr_hugepages", format!("{nr_hugepages}\n")) {
            Ok(()) => {
                info!(
                    "reserved {nr_hugepages} hugepages ({} MB)",
                    nr_hugepages * 2
                );
                true
            }
            Err(e) => {
                warn!("could not set nr_hugepages: {e}");
                false
            }
        };

    // The tuning conf hardcodes `huge_pages = on`. That's the right
    // production default — postgres fails fast if its hugepage reservation
    // wasn't successfully provisioned, rather than silently falling back
    // to regular pages and giving up the TLB win. In environments where
    // we *couldn't* reserve hugepages (unprivileged containers, dev hosts
    // with locked-down sysfs), forcing `on` would make postgres refuse to
    // start. Detect that and override with `try` via a high-numbered
    // conf.d file so postgres still boots cleanly.
    if !hugepages_reserved {
        let override_path = format!("{PGDATA}/conf.d/99-hugepages-fallback.conf");
        let body = "# Generated automatically when nr_hugepages reservation\n\
                    # failed at boot (apply_kernel_settings). Postgres tries\n\
                    # hugepages and falls back to anonymous shmem if unavailable.\n\
                    huge_pages = try\n";
        match config::write_atomic(Path::new(&override_path), body) {
            Ok(()) => info!("huge_pages override written to {override_path} (fallback to try)"),
            Err(e) => warn!("could not write huge_pages override: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Hook scripts
// ---------------------------------------------------------------------------

/// Run all executable scripts in `dir` in lexicographic order.
/// Uses `Command::new(script)` directly to honor shebangs.
pub async fn run_hook_scripts(dir: &str) -> Result<(), BootError> {
    let path = Path::new(dir);
    if !path.exists() {
        return Ok(());
    }

    let mut scripts: Vec<_> = std::fs::read_dir(path)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.metadata()
                    .map(|m| m.permissions().mode() & 0o111 != 0)
                    .unwrap_or(false)
        })
        .collect();

    scripts.sort();

    for script in &scripts {
        let script_str = script.display().to_string();
        info!("running hook: {script_str}");
        let status = spawn_hook_with_etxtbsy_retry(script).await?;
        if !status.success() {
            let code = status.code().unwrap_or(-1);
            return Err(BootError::HookFailed {
                path: script_str,
                code,
            });
        }
    }

    Ok(())
}

/// Spawn a hook script with a bounded retry on `ETXTBSY`.
///
/// Hook scripts may have been written by an in-flight deploy at the same
/// instant we try to `execve` them; Linux returns `ETXTBSY` until the
/// writer's inode reference is dropped. The same race can fire under
/// `cargo test`'s multi-threaded executor when concurrent tests `fork()`
/// while a sibling thread is mid-write to its own script. The kernel
/// shouldn't see writers on *this* inode in either case for long, so a
/// few short retries cover both with negligible production cost.
async fn spawn_hook_with_etxtbsy_retry(script: &Path) -> std::io::Result<std::process::ExitStatus> {
    let mut attempt: u32 = 0;
    loop {
        // Command::new — not sh -c — so the shebang is honored
        match Command::new(script).status().await {
            Ok(s) => return Ok(s),
            Err(e) if e.raw_os_error() == Some(libc::ETXTBSY) && attempt < 3 => {
                attempt += 1;
                tokio::time::sleep(std::time::Duration::from_millis(10 << (attempt - 1))).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
