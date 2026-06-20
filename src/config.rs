//! Generates the Beyond-managed `conf.d/` files and embeds the static config
//! files that are written verbatim.
//!
//! The RAM-derived tuning is split across two files with different lifecycles:
//!
//! - `01-tuning.conf` — postmaster-context params (require restart to change):
//!   `shared_buffers`, `max_connections`, `wal_buffers`, `huge_pages`,
//!   `max_worker_processes`, `max_parallel_workers`. Written once at boot.
//!
//! - `02-memory.conf` — sighup/user-context params (take effect on reload):
//!   `effective_cache_size`, `work_mem`, `maintenance_work_mem`,
//!   `max_parallel_workers_per_gather`, `max_parallel_maintenance_workers`.
//!   Written at boot and updated by the memory watcher in `supervisor` when
//!   virtio-mem hotplug changes visible RAM.

use std::io::Write as _;
use std::path::Path;

use crate::pg::PGDATA;

// ---------------------------------------------------------------------------
// Static files (embedded at compile time — must be non-empty before cargo build)
// ---------------------------------------------------------------------------

pub const BEYOND_CONF: &str = include_str!("../packer/files/postgresql/00-beyond.conf");

/// Directory holding the PostgreSQL shared-object extension modules. Matches
/// the PG18 Debian-derived layout (`pg_config --pkglibdir`) used everywhere in
/// this image — sibling to the `/var/lib/postgresql/18/...` paths in `pg.rs`.
const PKGLIBDIR: &str = "/usr/lib/postgresql/18/lib";

/// `00-beyond.conf` with `shared_preload_libraries` filtered down to the
/// libraries actually installed in this image.
///
/// `00-beyond.conf` lists every extension the platform *wants* preloaded, but
/// the standalone postgres primitive ships without the auth/queue milestone
/// (`beyond_auth`, `beyond_queue`) or pgdg's `pg_cron` (dropped on a version
/// pin). preloading a missing module makes postgres die at startup with
/// `FATAL: could not access file "<lib>": No such file or directory`.
///
/// This makes the supervisor self-adapting: with the extensions installed it
/// preloads them; without, it drops them and postgres boots. `pg_stat_statements`
/// and `auto_explain` ship with core postgres so they always survive the filter.
pub fn beyond_conf() -> String {
    filter_shared_preload_libraries(BEYOND_CONF, PKGLIBDIR)
}

/// Returns true iff `{pkglibdir}/{lib}.so` exists. Core-postgres libraries
/// (`pg_stat_statements`, `auto_explain`) are present in any standard install.
fn library_installed(pkglibdir: &str, lib: &str) -> bool {
    Path::new(pkglibdir).join(format!("{lib}.so")).exists()
}

/// Post-process a `postgresql.conf` body: rewrite the
/// `shared_preload_libraries = '...'` line, keeping only libraries whose shared
/// object exists under `pkglibdir`. Lines without that key pass through
/// untouched. If every listed library is missing, the key is emitted empty
/// (`shared_preload_libraries = ''`) rather than dropped, so an operator can
/// still see the (now-empty) setting.
fn filter_shared_preload_libraries(conf: &str, pkglibdir: &str) -> String {
    const KEY: &str = "shared_preload_libraries";
    let mut out = String::with_capacity(conf.len());
    for line in conf.lines() {
        if let Some(filtered) = filter_preload_line(line, KEY, pkglibdir) {
            out.push_str(&filtered);
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

/// If `line` is a `shared_preload_libraries = '...'` assignment, return the
/// rewritten line with only installed libraries kept; otherwise `None`.
fn filter_preload_line(line: &str, key: &str, pkglibdir: &str) -> Option<String> {
    let trimmed = line.trim_start();
    // Don't touch comments.
    if trimmed.starts_with('#') {
        return None;
    }
    let rest = trimmed.strip_prefix(key)?;
    // The next non-space char after the key must be '=' (avoid matching e.g.
    // `shared_preload_libraries.foo`).
    let after_key = rest.trim_start();
    let value_part = after_key.strip_prefix('=')?;
    // Extract the single-quoted list value.
    let value = value_part.trim();
    let inner = value.strip_prefix('\'')?.strip_suffix('\'')?;

    let kept: Vec<&str> = inner
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .filter(|lib| library_installed(pkglibdir, lib))
        .collect();

    Some(format!("{key} = '{}'", kept.join(",")))
}

pub const PG_HBA_CONF: &str = include_str!("../packer/files/postgresql/pg_hba.conf");

pub const PGBOUNCER_INI_BASE: &str = include_str!("../packer/files/pgbouncer/pgbouncer.ini");

/// PgBouncer config with TLS termination wired to the same cert Postgres uses.
///
/// `client_tls_sslmode = allow` matches Postgres `pg_hba.conf` posture — TLS
/// is available but plaintext is still accepted. Flipping to `require` to
/// reject plaintext is a separate policy decision (and would need the
/// matching `host`→`hostssl` flip in pg_hba.conf).
///
/// `client_tls_ca_file` is set when a CA is available (platform). It enables
/// mTLS opt-in: an operator can add `client_tls_sslmode = verify-full` in a
/// custom override and clients chaining to the per-app CA are authenticated.
/// PgBouncer server-side pool size, scaled to the box. ~3 server connections per
/// vCPU is enough query concurrency for SSD/GlideFS-backed Postgres; floored at 16
/// for tiny boxes, capped at 256, and always well under `max_connections`.
///
/// The old hardcoded `default_pool_size = 20` was wrong for the vertical-scaling
/// target: a 64-vCPU box choked on a 20-deep pool. (The pooler *process* isn't the
/// bottleneck at small core counts — measured in bench/glidefs-pg's F-test, identical
/// tps for 1/2/6 so_reuseport workers on 6 vCPU — but the pool SIZE must track the box.
/// so_reuseport multi-worker is a separate, larger change that needs a many-core host
/// to justify; pool sizing is the clear, low-risk win.)
pub fn pgbouncer_pool_size(vcpus: u32) -> u64 {
    ((vcpus as u64) * 3).clamp(16, 256)
}

/// CEILING on PgBouncer so_reuseport worker processes. PgBouncer is single-threaded —
/// one process saturates one core — so on a many-vCPU box a single pooler caps
/// throughput; so_reuseport lets N processes share `:5432` with the kernel balancing
/// connections.
///
/// This is a *cap*, not a fixed count. The supervisor scales the LIVE worker count
/// reactively (supervisor.rs `PgbScaler`) from 1 when idle up to this ceiling under
/// sustained pooler-CPU saturation, and reaps back down when cool — so a scaled-to-
/// zero box runs exactly one pooler and only a genuinely busy box spends more cores.
/// That tracks *usage*, not the allotment (idle boxes don't carry peak-provisioned
/// processes into their Firecracker snapshot).
///
/// Cap ≈ 1 worker per 4 vCPU (clamp 1..8): at the ceiling the pooler tier draws ≤1/4
/// of the box, leaving ≥3/4 for Postgres so the pooler can't starve it. Sizing from
/// `bench/glidefs-pg` measurements: one pooler core tops at ~56-71k tps (persistent)
/// vs ~19k tps per Postgres core for cheap reads, so the pooler becomes the bottleneck
/// only around ~3-4 PG cores — hence roughly one pooler per few cores.
pub fn pgbouncer_max_workers(vcpus: u32) -> usize {
    ((vcpus as usize) / 4).clamp(1, 8)
}

/// pgbouncer.ini: static base + boot-computed pool sizing + TLS termination keys.
pub fn pgbouncer_ini(tls: &crate::tls::TlsConfig, ram_bytes: u64, vcpus: u32) -> String {
    let mut out = String::from(PGBOUNCER_INI_BASE);
    if !out.ends_with('\n') {
        out.push('\n');
    }
    // Pool sizing — scaled to VM resources (the base .ini no longer hardcodes these).
    let pool = pgbouncer_pool_size(vcpus);
    let ram_mb = ram_bytes / (1024 * 1024);
    // FULL pool per worker, NOT pool/max_workers. `default_pool_size` is a per-worker
    // CEILING, not a reservation: in steady state the total server connections track
    // *query* demand (~`pool`), independent of worker count, because the scaler adds
    // so_reuseport workers for *handshake/CPU* load (the single-threaded pooler core
    // saturates terminating TLS — measured ~2.4k conns/s/core) not for query concurrency.
    // Dividing by the cap under-provisioned the common scaled-to-zero case (1 live worker
    // on a 64-vCPU box got pool/8 = 24 instead of 192). `max_connections`
    // (`pool + vcpus*2 + 10` in tuning_conf_boot) is the real backstop, and pgbouncer
    // QUEUES clients (doesn't error) if the pool is ever exhausted. Floor already ≥16.
    let per_worker_pool = pool;
    // Client connections are cheap (a few KB each), so keep the per-worker client cap
    // GENEROUS and undivided — one live worker can absorb the clients while the scaler
    // (which reacts on a ~5s tick) brings up more workers to add CPU, not capacity.
    let max_client_conn = (pool * 25).clamp(200, (ram_mb * 4).max(200));
    out.push_str("\n# Pool sizing — generated by beyond-pg from VM resources.\n");
    out.push_str(&format!("default_pool_size = {per_worker_pool}\n"));
    out.push_str(&format!("max_client_conn = {max_client_conn}\n"));
    // so_reuseport ALWAYS on (even at 1 live worker) so the supervisor can add/reap
    // workers reactively with no config change; the kernel load-balances connections.
    out.push_str("so_reuseport = 1\n");
    out.push_str("\n# TLS — termination for the public 5432 port (generated by beyond-pg).\n");
    out.push_str("client_tls_sslmode = allow\n");
    out.push_str(&format!("client_tls_cert_file = {}\n", tls.cert.display()));
    out.push_str(&format!("client_tls_key_file = {}\n", tls.key.display()));
    if let Some(ca) = &tls.ca {
        out.push_str(&format!("client_tls_ca_file = {}\n", ca.display()));
    } else {
        // pgbouncer (1.25.x) requires client_tls_ca_file whenever a client cert is
        // set — even for sslmode=allow — and FATALs with "failed to load CA:
        // (null)" otherwise. A self-signed cert is its own issuer, so point the CA
        // at the cert itself. This satisfies the requirement without changing
        // client-cert behavior (sslmode=allow neither requests nor verifies them).
        out.push_str(&format!("client_tls_ca_file = {}\n", tls.cert.display()));
    }
    out
}

// ---------------------------------------------------------------------------
// Generated: 01-tuning.conf  (postmaster-context — restart required)
// ---------------------------------------------------------------------------

/// Postmaster-context parameters — require a Postgres restart to take effect.
/// Written once at boot; not touched by the memory watcher.
pub fn tuning_conf_boot(ram_bytes: u64, vcpus: u32) -> String {
    let ram_mb = ram_bytes / (1024 * 1024);
    let pool_size = pgbouncer_pool_size(vcpus); // server connections PgBouncer opens
    let vcpus = vcpus as u64;

    // max_connections: PgBouncer pool + direct-path headroom + reserved slots.
    // Components: pool_size (PgBouncer server connections) + vcpus*2 (optimal
    // active connections for SSD per empirical formula cores*2+spindles, spindles≈0)
    // + 10 reserved (superuser, monitoring, pg_dump).
    // Floor 100 so tiny VMs leave room for ETL on the direct :5433 path.
    // Ceiling ram_mb/50 (~10 MB/connection overhead) prevents OOM.
    // Ref: PostgreSQL wiki "Number Of Database Connections"; Mattermost PgBouncer study
    let max_connections = (pool_size + vcpus * 2 + 10).clamp(100, (ram_mb / 50).max(100));

    // shared_buffers: 25% of RAM, floor 128MB.
    // Ref: PostgreSQL docs §20.4; pganalyze shared_buffers benchmark (2024) —
    // 25% is optimal up to ~64 GB; gains plateau above 40% due to double-buffering.
    let shared_buffers_mb = (ram_mb / 4).max(128);

    // wal_buffers: replicates Postgres auto-tune (wal_buffers=-1 = shared_buffers/32).
    // 16 MB ceiling is the historical single-WAL-segment size and is sufficient for
    // typical OLTP. High-concurrency write workloads may benefit from 64 MB.
    // Ref: EDB "Tuning shared_buffers and wal_buffers"
    let wal_buffers_mb = (shared_buffers_mb / 32).clamp(1, 16);

    // vCPU-derived parallelism.
    // max_worker_processes: vcpus + 4 to leave slots for non-parallel background
    // workers (autovacuum launcher, archiver, WAL senders). vcpus*2 has no empirical
    // basis and wastes process-table slots.
    // max_parallel_workers: equal to vCPU count — setting above vCPU count causes
    // context-switch overhead with no throughput gain.
    // Ref: Crunchy Data "Parallel Queries in Postgres"; jamesguthrie.ch PG parallel benchmark
    let max_worker_processes = (vcpus + 4).max(8);
    let max_parallel_workers = vcpus.max(1);

    format!(
        "# Generated by beyond-pg at boot from VM resources. Do not edit.\n\
         # Regenerated on every boot so resized VMs pick up correct values.\n\
         \n\
         # RAM-derived\n\
         shared_buffers = {shared_buffers_mb}MB\n\
         max_connections = {max_connections}\n\
         wal_buffers = {wal_buffers_mb}MB\n\
         # huge_pages=on requires nr_hugepages to be reserved before postgres starts;\n\
         # apply_kernel_settings() does this earlier in do_boot().\n\
         # Ref: Percona \"Benchmark PostgreSQL with Linux HugePages\";\n\
         #      PostgreSQL docs §19.4\n\
         huge_pages = on\n\
         \n\
         # Replication slot safety cap.\n\
         # A stalled or dead slot (WAL sink, CDC, or otherwise) holds back WAL\n\
         # cleanup, growing pg_wal/ until the primary's disk fills and Postgres\n\
         # crashes.  This hard cap invalidates any slot that falls >4 GB behind\n\
         # so the primary can reclaim WAL instead.  4 GB ≈ 256 segments: enough\n\
         # headroom for a multi-hour sink outage on a moderately busy primary.\n\
         max_slot_wal_keep_size = 4096\n\
         \n\
         # vCPU-derived\n\
         max_worker_processes = {max_worker_processes}\n\
         max_parallel_workers = {max_parallel_workers}\n"
    )
}

// ---------------------------------------------------------------------------
// Generated: 02-memory.conf  (reload-safe — pg_reload_conf() suffices)
// ---------------------------------------------------------------------------

/// Reload-safe parameters — take effect via `pg_reload_conf()` without restart.
/// Written at boot and updated live by the memory watcher in `supervisor` when
/// virtio-mem hotplug changes the amount of visible RAM.
pub fn tuning_conf_adaptive(ram_bytes: u64, vcpus: u32) -> String {
    let ram_mb = ram_bytes / (1024 * 1024);
    let pool_size = pgbouncer_pool_size(vcpus); // keep work_mem in step with pool depth
    let vcpus = vcpus as u64;

    // effective_cache_size: 75% of RAM (shared_buffers + OS page cache estimate).
    // Planner hint only — no memory allocated. 75% is the consensus value for a
    // dedicated server where the OS retains the rest.
    // Ref: PostgreSQL wiki; CYBERTEC "effective_cache_size explained"
    let effective_cache_mb = ram_mb * 3 / 4;

    // maintenance_work_mem: 5% of RAM, cap 2GB.
    // 2 GB cap prevents autovacuum (default 3 workers) from consuming 6 GB.
    // Note: autovacuum workers are internally capped at 1 GB regardless of this
    // setting; the cap mainly governs manual VACUUM and CREATE INDEX.
    // Ref: Robert Haas "How Much maintenance_work_mem Do I Need?" (2019)
    let maintenance_work_mb = (ram_mb / 20).min(2048);

    // work_mem: half of RAM divided by (pool_size × avg sort/hash nodes per plan).
    // work_mem is per sort/hash *node*, not per connection — a query with 3 sorts
    // uses 3×work_mem. Avg 3 nodes/plan is the empirically observed baseline for
    // OLTP with analytical extensions (pgvector ANN, postgis, pg_search).
    // Floor 32MB so even tiny VMs get something useful for ANN searches.
    // Tune empirically: raise log_temp_files threshold and monitor spills.
    // Ref: pganalyze "The surprising logic of Postgres work_mem" (2024)
    let work_mem_mb = (ram_mb / 2 / (pool_size * 3)).max(32);

    // max_parallel_*_per_gather: vcpus/2 with ceiling 4 for OLTP safety; benchmarks
    // show gains up to ~8 workers on large tables before diminishing returns.
    // Ref: Crunchy Data "Parallel Queries in Postgres"; jamesguthrie.ch PG parallel benchmark
    let max_parallel_workers_per_gather = (vcpus / 2).clamp(1, 4);
    let max_parallel_maintenance_workers = (vcpus / 2).clamp(1, 4);

    format!(
        "# Generated by beyond-pg. Updated at boot and on virtio-mem hotplug.\n\
         # Safe to reload without restarting postgres (pg_reload_conf()).\n\
         \n\
         # RAM-derived (reload-safe)\n\
         effective_cache_size = {effective_cache_mb}MB\n\
         maintenance_work_mem = {maintenance_work_mb}MB\n\
         work_mem = {work_mem_mb}MB\n\
         \n\
         # vCPU-derived (reload-safe)\n\
         max_parallel_workers_per_gather = {max_parallel_workers_per_gather}\n\
         max_parallel_maintenance_workers = {max_parallel_maintenance_workers}\n"
    )
}

// ---------------------------------------------------------------------------
// Generated: 02-durability.conf (ephemeral mode)
// ---------------------------------------------------------------------------

pub const DURABILITY_CONF_EPHEMERAL: &str = "# Generated by beyond-pg. Present only on ephemeral volumes.\n\
     # synchronous_commit=off gives 5-10x faster commits; ~10ms data-loss\n\
     # window on crash is irrelevant on a throwaway volume.\n\
     synchronous_commit = off\n\
     # Throttle autovacuum on ephemeral/fork volumes. Measured on PG18-on-GlideFS\n\
     # (bench/glidefs-pg): aggressive autovacuum on an idle fork wrote 306 MB of CoW\n\
     # divergence in 90s vs 0.1 MB throttled (~2240x). A throwaway volume is gone\n\
     # before bloat matters, but every vacuumed page is a new block GlideFS uploads.\n\
     # Wraparound protection still runs (autovacuum stays on). Durable primaries keep\n\
     # the aggressive default (00-beyond.conf) where bloat control wins.\n\
     autovacuum_vacuum_scale_factor = 0.4\n\
     autovacuum_naptime = 5min\n\
     autovacuum_vacuum_cost_delay = 20ms\n\
     autovacuum_vacuum_cost_limit = 200\n";

// ---------------------------------------------------------------------------
// Atomic write helper
// ---------------------------------------------------------------------------

/// Write `content` to `path` atomically using a temp file and rename.
/// Both paths must be on the same filesystem (always true here: both
/// are under PGDATA or /etc on the same volume).
pub fn write_atomic(path: &Path, content: &str) -> std::io::Result<()> {
    write_atomic_bytes(path, content.as_bytes())
}

/// Binary variant of [`write_atomic`].
///
/// Writes with mode `0o644` so the postgres OS user can read configs that
/// were generated by the supervisor running as root. The `tempfile` crate
/// defaults to `0o600`; persist preserves that without an explicit set,
/// which historically caused
/// `could not open configuration file ...: Permission denied` once
/// postgres dropped to its unprivileged user. Security-sensitive files
/// (private keys, password files) go through
/// [`write_atomic_bytes_with_mode`] instead.
pub fn write_atomic_bytes(path: &Path, content: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        write_atomic_bytes_with_mode(path, content, 0o644)
    }
    #[cfg(not(unix))]
    {
        let dir = path.parent().unwrap_or(Path::new("."));
        let mut tmp = tempfile::Builder::new().prefix(".tmp.").tempfile_in(dir)?;
        tmp.write_all(content)?;
        tmp.flush()?;
        let tmp_path = tmp.into_temp_path();
        tmp_path.persist(path).map_err(|e| e.error)
    }
}

/// Atomic write with an explicit Unix mode set *before* any bytes are written.
///
/// Use for security-sensitive files (private keys, password files) where a
/// permission window between creation and `chmod(2)` would be observable.
/// `tempfile` defaults to `0o600`, but this helper makes the invariant
/// explicit and survives any future change to the default.
#[cfg(unix)]
pub fn write_atomic_bytes_with_mode(path: &Path, content: &[u8], mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = path.parent().unwrap_or(Path::new("."));
    let mut tmp = tempfile::Builder::new().prefix(".tmp.").tempfile_in(dir)?;
    // Set mode on the open fd before content is written, so the file is
    // never observable on disk with a wider mode than requested.
    tmp.as_file()
        .set_permissions(std::fs::Permissions::from_mode(mode))?;
    tmp.write_all(content)?;
    tmp.flush()?;
    let tmp_path = tmp.into_temp_path();
    tmp_path.persist(path).map_err(|e| e.error)
}

// ---------------------------------------------------------------------------
// Well-known paths
// ---------------------------------------------------------------------------

pub fn conf_d_dir() -> String {
    format!("{PGDATA}/conf.d")
}

/// Path to the primary PostgreSQL config file (replaces the initdb default).
///
/// BEYOND_CONF contains `include_dir = 'conf.d'` at the end, so all
/// conf.d/*.conf files (tuning, durability) are picked up from there.
/// Writing to postgresql.conf directly (not conf.d/) avoids circular includes.
pub fn beyond_conf_path() -> String {
    format!("{}/postgresql.conf", PGDATA)
}

pub fn tuning_conf_path() -> String {
    format!("{}/conf.d/01-tuning.conf", PGDATA)
}

pub fn memory_conf_path() -> String {
    format!("{}/conf.d/02-memory.conf", PGDATA)
}

pub fn durability_conf_path() -> String {
    format!("{}/conf.d/03-durability.conf", PGDATA)
}

pub fn wal_sink_conf_path() -> String {
    format!("{}/conf.d/03-wal-sink.conf", PGDATA)
}

pub fn tls_conf_path() -> String {
    format!("{}/conf.d/06-tls.conf", PGDATA)
}

/// Emit `05-tls.conf` from a resolved [`crate::tls::TlsConfig`].
///
/// Numbered `06` so it lands after `04-replica.conf` and `05-pitr.conf` and
/// overrides the baseline `ssl_*_file` lines in `00-beyond.conf` (alpha
/// order, last setting wins). Always emitted at boot so a regression to the
/// baseline never silently leaks.
///
/// `ssl_ca_file` is only set when the cert source provides one (platform
/// today). Self-signed has no CA to advertise; user-managed leaves CA wiring
/// to `99-user.conf` so the operator can opt into `clientcert=verify-full`
/// independently.
pub fn tls_conf(tls: &crate::tls::TlsConfig) -> String {
    let mut out = String::from(
        "# Generated by beyond-pg at boot. Do not edit — overwritten on every boot.\n\
         # Overrides ssl_cert_file / ssl_key_file from 00-beyond.conf.\n\n",
    );
    out.push_str(&format!("ssl_cert_file = '{}'\n", tls.cert.display()));
    out.push_str(&format!("ssl_key_file = '{}'\n", tls.key.display()));
    if let Some(ca) = &tls.ca {
        out.push_str(&format!("ssl_ca_file = '{}'\n", ca.display()));
    }
    out
}

#[allow(dead_code)]
pub fn pitr_conf_path() -> String {
    format!("{}/conf.d/05-pitr.conf", PGDATA)
}

/// Generates `05-pitr.conf` when `BEYOND_PG_ARCHIVE_TARGET` is set.
///
/// Always writes `restore_command` so Postgres can fetch archived WAL segments
/// during crash recovery and replica lag catch-up. When `recovery_target_time`
/// is also set, adds the recovery target parameters that trigger point-in-time
/// recovery on a forked volume — Postgres replays archived WAL up to the target
/// and then promotes.
pub fn pitr_conf(archive_target: &str, recovery_target_time: Option<&str>) -> String {
    let target = archive_target.trim_end_matches('/');
    let mut out =
        String::from("# Generated by beyond-pg. Do not edit — overwritten on every boot.\n\n");
    out.push_str(&format!(
        "restore_command = 'aws s3 cp {target}/%f %p --no-progress'\n"
    ));
    if let Some(t) = recovery_target_time {
        let escaped = t.replace('\'', "''");
        out.push_str(&format!("recovery_target_time = '{escaped}'\n"));
        out.push_str("recovery_target_action = promote\n");
        out.push_str("recovery_target_inclusive = true\n");
    }
    out
}

pub fn replica_conf_path() -> String {
    format!("{}/conf.d/04-replica.conf", PGDATA)
}

/// Generates `04-replica.conf`, written only on replica-tier boots.
///
/// Configures streaming replication (`primary_conninfo`) and archive recovery
/// (`restore_command`) so Postgres can fall back to the WAL sink if the
/// streaming connection drops between restarts.
pub fn replica_conf(primary_conninfo: &str, wal_sink_url: Option<&str>) -> String {
    let escaped = primary_conninfo.replace('\'', "''");
    let mut out =
        String::from("# Generated by beyond-pg. Do not edit — overwritten on every boot.\n\n");
    out.push_str(&format!("primary_conninfo = '{escaped}'\n"));
    out.push_str("recovery_target_timeline = 'latest'\n");
    if let Some(url) = wal_sink_url {
        // %f = WAL segment filename, %p = destination path — standard Postgres archive vars.
        // curl -f fails on HTTP errors (4xx/5xx) so Postgres retries; -s suppresses progress.
        out.push_str(&format!("restore_command = 'curl -f -s {url}/%f -o %p'\n"));
    }
    out
}

pub fn wal_sink_conf() -> String {
    "# Generated by beyond-pg when BEYOND_PG_WAL_SINK is set.\n\
     # Commits wait for WAL acknowledgment from the WAL sink before returning.\n\
     synchronous_commit = remote_write\n\
     synchronous_standby_names = 'wal_sink'\n"
        .to_string()
}

pub const PG_HBA_PATH: &str = "/etc/postgresql/18/main/pg_hba.conf";
pub const PGBOUNCER_INI_PATH: &str = "/etc/pgbouncer/pgbouncer.ini";
pub const SYSCTL_PATH: &str = "/etc/sysctl.d/99-postgres.conf";
pub const THP_PATH: &str = "/sys/kernel/mm/transparent_hugepage/enabled";

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::tls::{TlsConfig, TlsSource};

    #[test]
    fn filter_preload_keeps_only_installed_libs() {
        // Fake pkglibdir with only pg_stat_statements + auto_explain present.
        let dir = tempfile::tempdir().unwrap();
        for lib in ["pg_stat_statements", "auto_explain"] {
            std::fs::write(dir.path().join(format!("{lib}.so")), b"").unwrap();
        }
        let pkglibdir = dir.path().to_str().unwrap();
        let conf = "shared_preload_libraries = 'pg_stat_statements,auto_explain,pg_cron,beyond_auth,beyond_queue'\nfoo = 1\n";
        let out = filter_shared_preload_libraries(conf, pkglibdir);
        assert!(
            out.contains("shared_preload_libraries = 'pg_stat_statements,auto_explain'"),
            "missing libs should be dropped: {out}"
        );
        assert!(!out.contains("pg_cron"), "pg_cron not installed: {out}");
        assert!(!out.contains("beyond_auth"), "beyond_auth not installed: {out}");
        assert!(!out.contains("beyond_queue"), "beyond_queue not installed: {out}");
        // Other lines untouched.
        assert!(out.contains("foo = 1"));
    }

    #[test]
    fn filter_preload_all_present_is_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        for lib in ["pg_stat_statements", "auto_explain", "pg_cron"] {
            std::fs::write(dir.path().join(format!("{lib}.so")), b"").unwrap();
        }
        let pkglibdir = dir.path().to_str().unwrap();
        let conf = "shared_preload_libraries = 'pg_stat_statements,auto_explain,pg_cron'\n";
        let out = filter_shared_preload_libraries(conf, pkglibdir);
        assert!(out.contains("shared_preload_libraries = 'pg_stat_statements,auto_explain,pg_cron'"));
    }

    #[test]
    fn filter_preload_all_missing_emits_empty_value() {
        let dir = tempfile::tempdir().unwrap();
        let pkglibdir = dir.path().to_str().unwrap();
        let conf = "shared_preload_libraries = 'pg_cron,beyond_auth'\n";
        let out = filter_shared_preload_libraries(conf, pkglibdir);
        assert!(
            out.contains("shared_preload_libraries = ''"),
            "all missing → empty value (key retained): {out}"
        );
    }

    #[test]
    fn filter_preload_ignores_comments_and_other_keys() {
        let dir = tempfile::tempdir().unwrap();
        let pkglibdir = dir.path().to_str().unwrap();
        let conf = "# shared_preload_libraries = 'pg_cron'\nshared_preload_libraries.foo = 'bar'\n";
        let out = filter_shared_preload_libraries(conf, pkglibdir);
        // Commented line passes through verbatim.
        assert!(out.contains("# shared_preload_libraries = 'pg_cron'"));
        // A different key (dotted) is not the assignment we rewrite.
        assert!(out.contains("shared_preload_libraries.foo = 'bar'"));
    }

    #[test]
    fn filter_preload_handles_real_embedded_conf() {
        // The embedded 00-beyond.conf must contain the key; filtering against a
        // dir with only core libs must drop the milestone extensions.
        let dir = tempfile::tempdir().unwrap();
        for lib in ["pg_stat_statements", "auto_explain"] {
            std::fs::write(dir.path().join(format!("{lib}.so")), b"").unwrap();
        }
        let out = filter_shared_preload_libraries(BEYOND_CONF, dir.path().to_str().unwrap());
        assert!(out.contains("shared_preload_libraries = 'pg_stat_statements,auto_explain'"));
        assert!(!out.contains("'pg_stat_statements,auto_explain,pg_cron"));
    }

    #[test]
    fn tls_conf_platform_includes_ca() {
        let tls = TlsConfig {
            source: TlsSource::Platform,
            cert: PathBuf::from("/run/beyond/tls/cert.pem"),
            key: PathBuf::from("/run/beyond/tls/key.pem"),
            ca: Some(PathBuf::from("/run/beyond/tls/ca.pem")),
        };
        let out = tls_conf(&tls);
        assert!(out.contains("ssl_cert_file = '/run/beyond/tls/cert.pem'"));
        assert!(out.contains("ssl_key_file = '/run/beyond/tls/key.pem'"));
        assert!(out.contains("ssl_ca_file = '/run/beyond/tls/ca.pem'"));
    }

    #[test]
    fn tls_conf_self_signed_omits_ca() {
        let tls = TlsConfig {
            source: TlsSource::SelfSigned,
            cert: PathBuf::from("/var/lib/postgresql/18/main/beyond/server.crt"),
            key: PathBuf::from("/var/lib/postgresql/18/main/beyond/server.key"),
            ca: None,
        };
        let out = tls_conf(&tls);
        assert!(out.contains("ssl_cert_file"));
        assert!(!out.contains("ssl_ca_file"), "self-signed has no CA: {out}");
    }

    #[test]
    fn pgbouncer_ini_appends_tls_keys() {
        let tls = TlsConfig {
            source: TlsSource::Platform,
            cert: PathBuf::from("/run/beyond/tls/cert.pem"),
            key: PathBuf::from("/run/beyond/tls/key.pem"),
            ca: Some(PathBuf::from("/run/beyond/tls/ca.pem")),
        };
        let out = pgbouncer_ini(&tls, 8 * 1024 * 1024 * 1024, 4);
        assert!(
            out.starts_with(PGBOUNCER_INI_BASE),
            "must preserve base config"
        );
        assert!(out.contains("default_pool_size = 16")); // 4 vCPU -> floor 16
        assert!(out.contains("max_client_conn = "));
        assert!(out.contains("client_tls_sslmode = allow"));
        assert!(out.contains("client_tls_cert_file = /run/beyond/tls/cert.pem"));
        assert!(out.contains("client_tls_key_file = /run/beyond/tls/key.pem"));
        assert!(out.contains("client_tls_ca_file = /run/beyond/tls/ca.pem"));
    }

    #[test]
    fn pgbouncer_ini_self_signed_points_ca_at_cert() {
        // pgbouncer FATALs without client_tls_ca_file when a client cert is set, so
        // a self-signed cert (its own issuer) must point the CA at itself.
        let tls = TlsConfig {
            source: TlsSource::SelfSigned,
            cert: PathBuf::from("/var/lib/postgresql/18/main/beyond/server.crt"),
            key: PathBuf::from("/var/lib/postgresql/18/main/beyond/server.key"),
            ca: None,
        };
        let out = pgbouncer_ini(&tls, 8 * 1024 * 1024 * 1024, 4);
        assert!(out.contains("client_tls_sslmode = allow"));
        assert!(out.contains("client_tls_cert_file"));
        assert!(
            out.contains("client_tls_ca_file = /var/lib/postgresql/18/main/beyond/server.crt"),
            "self-signed CA points at the cert itself: {out}"
        );
    }

    #[test]
    fn pgbouncer_scales_with_box() {
        // worker CEILING (supervisor scales the live count 1..this): ~1 per 4 vCPU, cap 8.
        assert_eq!(pgbouncer_max_workers(2), 1);
        assert_eq!(pgbouncer_max_workers(8), 2);
        assert_eq!(pgbouncer_max_workers(16), 4);
        assert_eq!(pgbouncer_max_workers(64), 8);
        assert_eq!(pgbouncer_max_workers(256), 8); // capped

        let tls = TlsConfig {
            source: TlsSource::SelfSigned,
            cert: PathBuf::from("/c"),
            key: PathBuf::from("/k"),
            ca: None,
        };
        // so_reuseport is ALWAYS on now (so the supervisor can add/reap workers live).
        let small = pgbouncer_ini(&tls, 4 * 1024 * 1024 * 1024, 4);
        assert!(small.contains("so_reuseport = 1"));
        assert!(small.contains("default_pool_size = 16")); // pool 16 (4 vCPU * 3, floored)
        // Empty unix_socket_dir so multiple so_reuseport workers share only TCP :5432.
        assert!(
            small.contains("unix_socket_dir ="),
            "needs empty unix_socket_dir: {small}"
        );
        // Mid box: full pool 48 (16 vCPU * 3), not divided across workers.
        let mid = pgbouncer_ini(&tls, 32u64 * 1024 * 1024 * 1024, 16);
        assert!(
            mid.contains("default_pool_size = 48"),
            "16 vCPU * 3 = full 48: {mid}"
        );
        // Big box: FULL pool per worker (a single live worker gets the whole 192, not
        // 192/8=24 — the scaler adds workers for handshake CPU, not query concurrency).
        let big = pgbouncer_ini(&tls, 256u64 * 1024 * 1024 * 1024, 64);
        assert!(big.contains("so_reuseport = 1"));
        assert!(
            big.contains("default_pool_size = 192"),
            "64 vCPU * 3 = full 192 per worker: {big}"
        );
        // max_connections is the real backstop covering server-side query demand.
        let tuning = tuning_conf_boot(256u64 * 1024 * 1024 * 1024, 64);
        assert!(tuning.contains("max_connections = 330")); // 192 + 64*2 + 10
    }

    #[test]
    fn replica_conf_no_wal_sink() {
        let conf = replica_conf("host=10.0.0.1 port=5433 user=replicator", None);
        assert!(
            conf.contains("primary_conninfo = 'host=10.0.0.1 port=5433 user=replicator'"),
            "primary_conninfo missing or wrong: {conf}"
        );
        assert!(
            conf.contains("recovery_target_timeline = 'latest'"),
            "recovery_target_timeline missing: {conf}"
        );
        assert!(
            !conf.contains("restore_command"),
            "restore_command should be absent when wal_sink is None: {conf}"
        );
    }

    #[test]
    fn replica_conf_with_wal_sink() {
        let conf = replica_conf(
            "host=10.0.0.1 user=replicator",
            Some("http://10.0.0.5:9000"),
        );
        assert!(
            conf.contains("restore_command = 'curl -f -s http://10.0.0.5:9000/%f -o %p'"),
            "restore_command wrong or missing: {conf}"
        );
    }

    #[test]
    fn replica_conf_escapes_single_quotes() {
        let conf = replica_conf("host=10.0.0.1 password=it's'secret", None);
        assert!(
            conf.contains("password=it''s''secret"),
            "single quotes not escaped: {conf}"
        );
        assert!(
            !conf.contains("password=it's"),
            "unescaped single quote present: {conf}"
        );
    }

    #[test]
    fn replica_conf_path_is_04() {
        assert!(
            replica_conf_path().ends_with("/conf.d/04-replica.conf"),
            "unexpected path: {}",
            replica_conf_path()
        );
    }

    #[test]
    fn pitr_conf_restore_command_only() {
        let conf = pitr_conf("s3://bucket/prefix", None);
        assert!(
            conf.contains("restore_command = 'aws s3 cp s3://bucket/prefix/%f %p --no-progress'"),
            "restore_command wrong or missing: {conf}"
        );
        assert!(
            !conf.contains("recovery_target_time"),
            "recovery_target_time should be absent when not set: {conf}"
        );
    }

    #[test]
    fn pitr_conf_with_recovery_target() {
        let conf = pitr_conf("s3://bucket/prefix", Some("2026-05-14 03:00:00"));
        assert!(
            conf.contains("restore_command = 'aws s3 cp s3://bucket/prefix/%f %p --no-progress'"),
            "restore_command missing: {conf}"
        );
        assert!(
            conf.contains("recovery_target_time = '2026-05-14 03:00:00'"),
            "recovery_target_time wrong: {conf}"
        );
        assert!(
            conf.contains("recovery_target_action = promote"),
            "recovery_target_action missing: {conf}"
        );
        assert!(
            conf.contains("recovery_target_inclusive = true"),
            "recovery_target_inclusive missing: {conf}"
        );
    }

    #[test]
    fn pitr_conf_strips_trailing_slash() {
        let conf = pitr_conf("s3://bucket/prefix/", None);
        assert!(
            conf.contains("s3://bucket/prefix/%f"),
            "trailing slash not stripped: {conf}"
        );
        assert!(!conf.contains("prefix//%f"), "double slash present: {conf}");
    }

    #[test]
    fn pitr_conf_escapes_single_quotes_in_target_time() {
        let conf = pitr_conf("s3://b/p", Some("2026-05-14 03:00:00.000+00''00"));
        assert!(
            conf.contains("2026-05-14 03:00:00.000+00''''00"),
            "single quotes not escaped: {conf}"
        );
    }

    #[test]
    fn pitr_conf_path_is_05() {
        assert!(
            pitr_conf_path().ends_with("/conf.d/05-pitr.conf"),
            "unexpected path: {}",
            pitr_conf_path()
        );
    }
}

pub const SYSCTL_CONF: &str = "\
# vm.swappiness: reduce kernel tendency to swap anonymous pages (e.g. work_mem)
# under moderate memory pressure. Value 10 avoids paging hot database memory
# while leaving the OOM killer a pressure valve.
# Ref: Red Hat PostgreSQL tuning guide; Percona \"Out of Memory Killer\" post
vm.swappiness = 10

# vm.overcommit_memory=2: kernel refuses allocations that exceed CommitLimit
# instead of killing processes after the fact. Postmaster survives; the failing
# client gets ENOMEM instead of a cluster restart.
# vm.overcommit_ratio=80: CommitLimit = (80% * RAM) + swap. Default 50 is too
# tight once shared_buffers (25% RAM) + workers are counted on a dedicated host.
# Ref: PostgreSQL docs §19.4; Cybertec \"Memory Overcommit and PostgreSQL\"
vm.overcommit_memory = 2
vm.overcommit_ratio = 80

# vm.dirty_*_bytes: fix-byte writeback thresholds instead of ratio-based defaults.
# On large-RAM hosts, dirty_background_ratio=10% allows multi-GB dirty backlogs
# before flush starts, causing multi-second I/O stalls timed to checkpoints.
# Tradeoff: ~11-14% TPS reduction and ~50-70% slower vacuum on write-heavy
# workloads — test on your storage before adjusting further.
# Ref: Greg Smith / EDB \"Tuning Linux for Low PostgreSQL Latency\";
#      PostgreSQL mailing list 2010-04-12 (id 4BC796EF.5030902@2ndquadrant.com)
vm.dirty_background_bytes = 67108864
vm.dirty_bytes = 536870912

# vm.min_free_kbytes: kernel page reserve for atomic allocations. Too low and
# the OOM killer fires even when memory is technically available.
# 128 MB is a safe floor for large-RAM hosts.
vm.min_free_kbytes = 131072

# kernel.sched_migration_cost_ns: time the CFS scheduler treats a process as
# \"cache hot\" after migration, resisting re-migration. Default 500µs is far too
# short under a large process table — the scheduler thrashes backends across
# cores, burning system CPU. 5 ms stabilises placement.
# Measured: 5x TPS improvement at 900 connections (20% → 70% sys CPU reversed).
# Ref: Shaun Thomas, pgsql-performance 2012-12-31 (id 50E4AAB1.9040902@optionshouse.com);
#      confirmed on LKML (lkml.iu.edu/hypermail/linux/kernel/1507.1/00767.html)
kernel.sched_migration_cost_ns = 5000000

# kernel.sched_autogroup_enabled: groups tasks by TTY for desktop responsiveness,
# which starves long-running server daemons of CPU time. No benefit on a
# headless VM; disabling gives a standalone ~30% TPS boost.
# Ref: same Shaun Thomas benchmark above
kernel.sched_autogroup_enabled = 0

# net.core.somaxconn: kernel listen-backlog cap for TCP sockets. PostgreSQL's
# own listen_backlog is silently clamped to this. PgBouncer absorbs most
# connection pressure, but raising this prevents drops during restart storms.
# Ref: PostgreSQL docs §19.3; Red Hat RHEL PostgreSQL tuning profile
net.core.somaxconn = 1024

# fs.aio-max-nr: system-wide cap on in-flight async I/O requests.
# Required for PostgreSQL 18+ io_method=io_uring; exhausting the default (65536)
# causes allocation failures under high concurrency with direct I/O.
# Ref: pganalyze \"PostgreSQL 18 Async I/O Explainer\"; Red Hat tuning profile
fs.aio-max-nr = 1048576

# net.ipv4.tcp_syn_retries: SYN retransmit attempts before giving up on a new
# connection. Default 6 (~127s total). Value 2 (~7s) cuts connection setup
# latency when a peer is unreachable — acceptable on a private overlay network
# where packet loss is rare and fast failure is preferred.
net.ipv4.tcp_syn_retries = 2
";
