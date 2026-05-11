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
    #[error("Tier 2 not yet implemented (tier = {0:?})")]
    UnsupportedTier(PgTier),
    #[error("pg_wal exists as a plain directory — operator must convert to symlink")]
    WalIsDirectory,
    #[error("pg_wal symlink points to wrong target: expected {expected}, got {actual}")]
    WalWrongTarget { expected: String, actual: String },
    #[error("initdb failed: {0}")]
    InitDb(String),
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
    if cfg.pg_tier != PgTier::Single {
        return Err(BootError::UnsupportedTier(cfg.pg_tier));
    }

    ensure_conf_d()?;
    maybe_initdb(cfg).await?;
    verify_wal_symlink()?;
    fetch_wal_gap(cfg).await?;
    write_config_files(cfg)?;
    let shared_buffers_mb = (cfg.ram_bytes / (1024 * 1024) / 4).max(128);
    apply_kernel_settings(shared_buffers_mb);

    // Step 7: ensure TLS cert exists and is not near expiry.
    match crate::tls::ensure_cert(Path::new(PGDATA))? {
        crate::tls::TlsCertOutcome::Generated => info!("tls: generated new self-signed cert"),
        crate::tls::TlsCertOutcome::Renewed => {
            info!("tls: renewed self-signed cert (was near expiry)");
            // Best-effort reload; if postgres is not yet started this is a no-op.
            if let Err(e) = pg::reload().await {
                info!("tls: pg_ctl reload skipped (postgres not yet started): {e}");
            }
        }
        crate::tls::TlsCertOutcome::UserManaged => {
            info!("tls: skipping cert — .user-managed sentinel present")
        }
        crate::tls::TlsCertOutcome::StillValid => info!("tls: cert still valid"),
    }

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

fn write_config_files(cfg: &MmdsConfig) -> Result<(), BootError> {
    use config::write_atomic;

    // 00-beyond.conf — image opinions, overwritten every boot
    write_atomic(Path::new(&config::beyond_conf_path()), config::BEYOND_CONF)?;

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

    // pgbouncer.ini — overwritten every boot
    if let Some(parent) = Path::new(PGBOUNCER_INI_PATH).parent() {
        std::fs::create_dir_all(parent)?;
    }
    write_atomic(Path::new(PGBOUNCER_INI_PATH), config::PGBOUNCER_INI)?;

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
    // the same segment. postgres is started with huge_pages=on, so it will fail
    // to start rather than silently fall back if reservation is insufficient.
    // Percona benchmark: TLB faults drop from ~200k/s to near zero with hugepages.
    // Ref: Percona "Benchmark PostgreSQL with Linux HugePages";
    //      PostgreSQL docs §19.4 "Managing Kernel Resources"
    let nr_hugepages = shared_buffers_mb / 2 + 32;
    match std::fs::write("/proc/sys/vm/nr_hugepages", format!("{nr_hugepages}\n")) {
        Ok(()) => info!(
            "reserved {nr_hugepages} hugepages ({} MB)",
            nr_hugepages * 2
        ),
        Err(e) => warn!("could not set nr_hugepages: {e}"),
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
        // Command::new — not sh -c — so the shebang is honored
        let status = Command::new(script).status().await?;
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
