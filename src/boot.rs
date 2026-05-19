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
const PG_WAL_TARGET: &str = "/var/lib/postgresql/18/wal";
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
    match cfg.pg_tier {
        PgTier::Single | PgTier::Primary => do_boot_primary(cfg).await,
        PgTier::Replica => do_boot_replica(cfg).await,
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

    run_initdb(&cfg.postgres_password).await?;
    Ok(())
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

    let path_str = pwfile
        .path()
        .to_str()
        .ok_or_else(|| BootError::Io(std::io::Error::other("tempfile path is not UTF-8")))?;
    info!("running initdb");
    pg::initdb(PGDATA, path_str)
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

    if let Some(target) = &cfg.archive_target {
        let conf = config::pitr_conf(target, cfg.recovery_target_time.as_deref());
        config::write_atomic(&pitr_path, &conf)?;
        info!("wrote {}", pitr_path.display());
    } else {
        match std::fs::remove_file(&pitr_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(BootError::Io(e)),
        }
    }

    // recovery.signal tells Postgres to enter recovery mode and replay archived
    // WAL up to recovery_target_time, then promote. Only written when both an
    // archive target and a recovery target time are present — a target time
    // without an archive to read from is a misconfiguration.
    let pitr_mode = cfg.recovery_target_time.is_some() && cfg.archive_target.is_some();
    if pitr_mode {
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
        match std::fs::remove_file(&recovery_signal) {
            Ok(()) => info!("recovery.signal: removed (not in PITR mode)"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(BootError::Io(e)),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mmds::{MmdsConfig, PgTier};

    fn test_cfg(archive_target: Option<&str>, recovery_target_time: Option<&str>) -> MmdsConfig {
        MmdsConfig {
            pg_tier: PgTier::Single,
            ephemeral: false,
            postgres_password: "test".into(),
            postgres_database: "postgres".into(),
            archive_target: archive_target.map(str::to_owned),
            wal_sink: None,
            cdc_enabled: false,
            recovery_target_time: recovery_target_time.map(str::to_owned),
            primary_conninfo: None,
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
    fn pitr_config_written_when_archive_target_set() {
        let pgdata = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pgdata.path().join("conf.d")).unwrap();

        let cfg = test_cfg(Some("s3://bucket/prefix"), None);
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();

        let pitr = pgdata.path().join("conf.d/05-pitr.conf");
        assert!(pitr.exists(), "05-pitr.conf should be written");

        let content = std::fs::read_to_string(&pitr).unwrap();
        assert!(
            content
                .contains("restore_command = 'aws s3 cp s3://bucket/prefix/%f %p --no-progress'"),
            "restore_command wrong: {content}"
        );
        assert!(
            !content.contains("recovery_target_time"),
            "recovery_target_time should be absent: {content}"
        );

        // recovery.signal must NOT be created — no target time set.
        assert!(
            !pgdata.path().join("recovery.signal").exists(),
            "recovery.signal should not exist without recovery_target_time"
        );
    }

    #[test]
    fn pitr_config_and_signal_written_in_pitr_mode() {
        let pgdata = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pgdata.path().join("conf.d")).unwrap();

        let cfg = test_cfg(Some("s3://bucket/prefix"), Some("2026-05-14 03:00:00"));
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();

        let pitr = pgdata.path().join("conf.d/05-pitr.conf");
        let content = std::fs::read_to_string(&pitr).unwrap();
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
    fn pitr_config_removed_when_archive_target_cleared() {
        let pgdata = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pgdata.path().join("conf.d")).unwrap();

        // First boot: archive target set.
        let cfg = test_cfg(Some("s3://bucket/prefix"), Some("2026-05-14 03:00:00"));
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();
        assert!(pgdata.path().join("conf.d/05-pitr.conf").exists());
        assert!(pgdata.path().join("recovery.signal").exists());

        // Second boot: archive target cleared (e.g. recovery completed, MMDS updated).
        let cfg = test_cfg(None, None);
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();
        assert!(
            !pgdata.path().join("conf.d/05-pitr.conf").exists(),
            "05-pitr.conf should be removed when archive_target cleared"
        );
        assert!(
            !pgdata.path().join("recovery.signal").exists(),
            "recovery.signal should be removed when not in PITR mode"
        );
    }

    #[test]
    fn pitr_signal_not_written_without_archive_target() {
        let pgdata = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(pgdata.path().join("conf.d")).unwrap();

        // recovery_target_time set but no archive_target — misconfiguration guard.
        let cfg = test_cfg(None, Some("2026-05-14 03:00:00"));
        write_pitr_config_into(&cfg, pgdata.path()).unwrap();

        assert!(
            !pgdata.path().join("recovery.signal").exists(),
            "recovery.signal must not be created without archive_target"
        );
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

    // 00-beyond.conf — image opinions, overwritten every boot
    write_atomic(Path::new(&config::beyond_conf_path()), config::BEYOND_CONF)?;

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

    // 03-wal-sink.conf — present only when BEYOND_PG_WAL_SINK is set
    let wal_sink_path = config::wal_sink_conf_path();
    if cfg.wal_sink.is_some() {
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

    // pgbouncer.ini — overwritten every boot, with client_tls_* keys pointing
    // at the same cert Postgres uses.
    if let Some(parent) = Path::new(PGBOUNCER_INI_PATH).parent() {
        std::fs::create_dir_all(parent)?;
    }
    write_atomic(Path::new(PGBOUNCER_INI_PATH), &config::pgbouncer_ini(tls))?;

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
