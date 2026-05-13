//! `beyond-pg supervisor` — PID 1 and the everything daemon.
//!
//! Called by `init::run()` after Linux init responsibilities are complete.
//! Responsibilities:
//!   1. Run boot-time setup (idempotent).
//!   2. Spawn Postgres; wait until ready; run post-start setup.
//!   3. Spawn PgBouncer (after post-start so auth infra exists).
//!   4. Supervise both children with restart-on-crash and exponential backoff.
//!   5. Forward their logs over vsock to the host log pipeline (pass-through
//!      to stderr when vsock is unavailable — local dev, Docker).
//!   6. Serve the vsock control RPC.
//!   7. On SIGTERM: drain children (SIGTERM → 10s → SIGKILL), then call
//!      reboot(LINUX_REBOOT_CMD_POWER_OFF) when running as PID 1.
//!
//! Signal handling:
//!   - SIGTERM/SIGINT are blocked in this process before any spawn.
//!   - Each child unblocks them in its pre_exec hook.
//!   - We receive them via tokio::signal.

use std::time::{Duration, Instant};

use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::boot;
use crate::config;
use crate::log_forwarder::{LogFrame, spawn_async_reader_task, zero_execution_id};
use crate::mmds::{MmdsConfig, PgTier};
use crate::pg;
use crate::vsock::ExecStream;
#[cfg(target_os = "linux")]
use crate::vsock::{HOST_CID, UserProcessStreamDataPayload, VSOCK_PORT, encode_log_frame};

const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const STABLE_RUNTIME: Duration = Duration::from_secs(60);
const MAX_BACKOFF_MS: u64 = 30_000;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub async fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    if let Err(e) = run_inner().await {
        error!("supervisor fatal: {e}");
        std::process::exit(1);
    }
}

async fn run_inner() -> Result<(), Box<dyn std::error::Error>> {
    // Block SIGTERM/SIGINT before spawning any children so signals are queued
    // for our tokio::signal handles and not delivered as SIG_DFL.
    // Each child unblocks them in its pre_exec hook (see spawn_*).
    #[cfg(target_os = "linux")]
    {
        use nix::sys::signal::{SigSet, SigmaskHow, Signal, sigprocmask};
        let mut set = SigSet::empty();
        set.add(Signal::SIGTERM);
        set.add(Signal::SIGINT);
        sigprocmask(SigmaskHow::SIG_BLOCK, Some(&set), None)?;
    }

    let cfg = crate::mmds::read().await?;

    // Boot setup — idempotent
    boot::do_boot(&cfg).await?;

    // Bounded channel for log frames from all child pipes
    let (log_tx, log_rx) = mpsc::channel::<LogFrame>(1024);

    // Spawn Postgres
    let mut pg_state = ChildState::new("postgres");
    spawn_postgres(&mut pg_state, &log_tx)?;

    // Wait for Postgres to be ready before post-start and PgBouncer
    if !pg::wait_until_ready(Duration::from_secs(60)).await {
        return Err("postgres did not become ready within 60s".into());
    }
    info!("postgres is ready");

    // Replicas are read-only hot standbys — DDL is rejected during recovery.
    // Extensions, roles, and slots are replicated from the primary.
    if cfg.pg_tier != PgTier::Replica {
        post_start(&cfg).await?;
    }

    // Spawn PgBouncer after post-start so the pgbouncer role and auth function exist.
    // PgBouncer is useful on replicas too for read-only connection pooling.
    let mut pgb_state = ChildState::new("pgbouncer");
    spawn_pgbouncer(&mut pgb_state, &log_tx)?;

    // WAL forwarder and CDC run only on the primary.
    if cfg.pg_tier != PgTier::Replica
        && let Some(wal_sink) = &cfg.wal_sink
    {
        let sink_url = wal_sink.clone();
        tokio::spawn(crate::wal_forwarder::run(
            sink_url,
            "wal_sink".to_owned(),
            crate::pg::PG_PORT,
        ));
    }

    let mut cdc_state = if cfg.pg_tier != PgTier::Replica && cfg.cdc_enabled {
        let mut s = ChildState::new("beyond-pg-cdc");
        spawn_cdc(&mut s, &log_tx)?;
        Some(s)
    } else {
        None
    };

    // Background: forward logs to host vsock
    let log_writer_handle = tokio::spawn(log_writer_task(log_rx));

    // Background: serve control RPC
    let rpc_handle = tokio::spawn(crate::rpc::serve());

    // Background: update reload-safe tuning params when virtio-mem changes RAM
    let memory_watcher_handle = tokio::spawn(memory_watcher_task(cfg.vcpus, cfg.ram_bytes));

    // Signal handles — must be set up after blocking the signals
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    info!("supervisor running");

    // Main supervision loop
    loop {
        // Both children must have a live Child handle to select on.
        // If a child exited and is in backoff, we create a dummy future that
        // never resolves (handled by the restart logic below).
        tokio::select! {
            _ = sigterm.recv() => {
                info!("received SIGTERM, draining children");
                drain_children(&mut pg_state, &mut pgb_state, cdc_state.as_mut()).await;
                shutdown_background_tasks(log_writer_handle, rpc_handle, memory_watcher_handle).await;
                return Ok(());
            }
            _ = sigint.recv() => {
                info!("received SIGINT, draining children");
                drain_children(&mut pg_state, &mut pgb_state, cdc_state.as_mut()).await;
                shutdown_background_tasks(log_writer_handle, rpc_handle, memory_watcher_handle).await;
                return Ok(());
            }
            status = async {
                if let Some(ref mut ch) = pg_state.child {
                    ch.wait().await
                } else {
                    std::future::pending().await
                }
            } => {
                let status = status?;
                pg_state.on_exit(status.code());
                maybe_restart(&mut pg_state, &log_tx).await?;
            }
            status = async {
                if let Some(ref mut ch) = pgb_state.child {
                    ch.wait().await
                } else {
                    std::future::pending().await
                }
            } => {
                let status = status?;
                pgb_state.on_exit(status.code());
                maybe_restart_pgbouncer(&mut pgb_state, &log_tx).await?;
            }
            status = async {
                match cdc_state.as_mut().and_then(|s| s.child.as_mut()) {
                    Some(ch) => ch.wait().await,
                    None => std::future::pending().await,
                }
            } => {
                let status = status?;
                if let Some(state) = cdc_state.as_mut() {
                    state.on_exit(status.code());
                    maybe_restart_cdc(state, &log_tx).await?;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Child state and restart logic
// ---------------------------------------------------------------------------

struct ChildState {
    name: &'static str,
    child: Option<tokio::process::Child>,
    restart_count: u32,
    last_start: Option<Instant>,
}

impl ChildState {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            child: None,
            restart_count: 0,
            last_start: None,
        }
    }

    /// Record exit. Resets backoff if the child was stable long enough.
    fn on_exit(&mut self, code: Option<i32>) {
        self.child = None;
        // Measure how long this instance was alive
        let stable = self
            .last_start
            .map(|t| t.elapsed() >= STABLE_RUNTIME)
            .unwrap_or(false);
        if stable {
            info!("{}: ran stably, resetting restart backoff", self.name);
            self.restart_count = 0;
        }
        match code {
            Some(0) => info!("{}: exited cleanly (code 0)", self.name),
            Some(c) => warn!("{}: exited with code {c}", self.name),
            None => warn!("{}: killed by signal", self.name),
        }
    }

    fn backoff_ms(&self) -> u64 {
        // 100 << min(restart_count, 8): max = 100 << 8 = 25_600 < 30_000; no overflow.
        (100u64 << self.restart_count.min(8)).min(MAX_BACKOFF_MS)
    }

    fn record_start(&mut self, child: tokio::process::Child) {
        self.child = Some(child);
        self.last_start = Some(Instant::now());
        self.restart_count = self.restart_count.saturating_add(1);
    }
}

async fn maybe_restart(
    state: &mut ChildState,
    log_tx: &mpsc::Sender<LogFrame>,
) -> Result<(), Box<dyn std::error::Error>> {
    let delay = Duration::from_millis(state.backoff_ms());
    warn!(
        "{}: restarting in {}ms (count={})",
        state.name,
        delay.as_millis(),
        state.restart_count
    );
    tokio::time::sleep(delay).await;
    spawn_postgres(state, log_tx)
}

async fn maybe_restart_pgbouncer(
    state: &mut ChildState,
    log_tx: &mpsc::Sender<LogFrame>,
) -> Result<(), Box<dyn std::error::Error>> {
    let delay = Duration::from_millis(state.backoff_ms());
    warn!(
        "{}: restarting in {}ms (count={})",
        state.name,
        delay.as_millis(),
        state.restart_count
    );
    tokio::time::sleep(delay).await;
    spawn_pgbouncer(state, log_tx)
}

async fn maybe_restart_cdc(
    state: &mut ChildState,
    log_tx: &mpsc::Sender<LogFrame>,
) -> Result<(), Box<dyn std::error::Error>> {
    let delay = Duration::from_millis(state.backoff_ms());
    warn!(
        "{}: restarting in {}ms (count={})",
        state.name,
        delay.as_millis(),
        state.restart_count
    );
    tokio::time::sleep(delay).await;
    spawn_cdc(state, log_tx)
}

// ---------------------------------------------------------------------------
// Process spawning
// ---------------------------------------------------------------------------

fn spawn_postgres(
    state: &mut ChildState,
    log_tx: &mpsc::Sender<LogFrame>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::new("postgres");
    cmd.arg("-D")
        .arg(pg::PGDATA)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(std::process::Stdio::null());

    unblock_signals_in_child(&mut cmd);
    protect_postmaster_from_oom(&mut cmd);

    let mut child = cmd.spawn()?;
    let execution_id = zero_execution_id();
    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");
    spawn_async_reader_task(
        ExecStream::Stdout,
        stdout,
        log_tx.clone(),
        execution_id.clone(),
    );
    spawn_async_reader_task(ExecStream::Stderr, stderr, log_tx.clone(), execution_id);

    info!("postgres spawned (pid={})", child.id().unwrap_or(0));
    state.record_start(child);
    Ok(())
}

fn spawn_pgbouncer(
    state: &mut ChildState,
    log_tx: &mpsc::Sender<LogFrame>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::new("pgbouncer");
    cmd.arg("/etc/pgbouncer/pgbouncer.ini")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(std::process::Stdio::null());

    unblock_signals_in_child(&mut cmd);

    let mut child = cmd.spawn()?;
    let execution_id = zero_execution_id();
    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");
    spawn_async_reader_task(
        ExecStream::Stdout,
        stdout,
        log_tx.clone(),
        execution_id.clone(),
    );
    spawn_async_reader_task(ExecStream::Stderr, stderr, log_tx.clone(), execution_id);

    info!("pgbouncer spawned (pid={})", child.id().unwrap_or(0));
    state.record_start(child);
    Ok(())
}

fn spawn_cdc(
    state: &mut ChildState,
    log_tx: &mpsc::Sender<LogFrame>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::new("beyond-pg-cdc");
    cmd.args([
        "--slot",
        "cdc",
        "--publication",
        "cdc",
        "--http-port",
        "9001",
    ])
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .stdin(std::process::Stdio::null());

    unblock_signals_in_child(&mut cmd);

    let mut child = cmd.spawn()?;
    let execution_id = zero_execution_id();
    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");
    spawn_async_reader_task(
        ExecStream::Stdout,
        stdout,
        log_tx.clone(),
        execution_id.clone(),
    );
    spawn_async_reader_task(ExecStream::Stderr, stderr, log_tx.clone(), execution_id);

    info!("beyond-pg-cdc spawned (pid={})", child.id().unwrap_or(0));
    state.record_start(child);
    Ok(())
}

/// Unblock SIGTERM and SIGINT in the child process.
/// Parent blocks them (for tokio::signal); children must receive them normally.
#[allow(unused_variables)]
fn unblock_signals_in_child(cmd: &mut Command) {
    #[cfg(target_os = "linux")]
    // SAFETY: The closure runs between fork() and exec() in the child process and
    // must be async-signal-safe — no allocation, no mutexes, no Rust runtime.
    // libc::sigemptyset, sigaddset, and sigprocmask are async-signal-safe per POSIX.
    // std::mem::zeroed() for sigset_t is valid: the C type has no invalid bit patterns.
    unsafe {
        cmd.pre_exec(|| {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGTERM);
            libc::sigaddset(&mut set, libc::SIGINT);
            libc::sigprocmask(libc::SIG_UNBLOCK, &set, std::ptr::null_mut());
            Ok(())
        });
    }
}

/// Set oom_score_adj = -1000 on the postmaster, exempting it from OOM killer
/// selection. Child backends inherit 0 (default), so a runaway backend can
/// still be killed without taking down the whole cluster.
/// Only applied to the postmaster — pgbouncer does not need this protection.
/// Ref: PostgreSQL docs §19.4; Crunchy Data "Deep PostgreSQL Thoughts: The Linux Assassin"
#[allow(unused_variables)]
fn protect_postmaster_from_oom(cmd: &mut Command) {
    #[cfg(target_os = "linux")]
    // SAFETY: The closure runs between fork() and exec() and must be async-signal-safe.
    // libc::open, write, close are async-signal-safe per POSIX.
    // Errors are silently ignored — if the write fails the process still starts.
    unsafe {
        cmd.pre_exec(|| {
            let path = b"/proc/self/oom_score_adj\0";
            let val = b"-1000\n";
            let fd = libc::open(
                path.as_ptr() as *const libc::c_char,
                libc::O_WRONLY | libc::O_CLOEXEC,
            );
            if fd >= 0 {
                libc::write(fd, val.as_ptr() as *const libc::c_void, val.len());
                libc::close(fd);
            }
            Ok(())
        });
    }
}

// ---------------------------------------------------------------------------
// Drain on shutdown
// ---------------------------------------------------------------------------

async fn drain_children(
    pg: &mut ChildState,
    pgb: &mut ChildState,
    mut cdc: Option<&mut ChildState>,
) {
    // Send SIGTERM to all live children (not kill() which is SIGKILL)
    sigterm_child(pg);
    sigterm_child(pgb);
    if let Some(c) = cdc.as_deref_mut() {
        sigterm_child(c);
    }

    let mut all: Vec<&mut ChildState> = vec![pg, pgb];
    if let Some(c) = cdc {
        all.push(c);
    }

    // Poll for all to exit, up to DRAIN_TIMEOUT
    let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
    loop {
        let all_done = all.iter_mut().all(|s| {
            s.child
                .as_mut()
                .is_none_or(|c| c.try_wait().ok().flatten().is_some())
        });
        if all_done {
            info!("all children drained cleanly");
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!("drain timeout, sending SIGKILL to stragglers");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // SIGKILL any stragglers — intentional SIGKILL here
    for state in all {
        if let Some(ref mut ch) = state.child
            && ch.try_wait().ok().flatten().is_none()
        {
            warn!("{}: sending SIGKILL", state.name);
            drop(ch.kill()); // kill() is async but we don't need to await its result
            let _ = ch.wait().await;
        }
    }
}

fn sigterm_child(state: &mut ChildState) {
    if let Some(ref ch) = state.child
        && let Some(pid) = ch.id()
    {
        #[cfg(target_os = "linux")]
        {
            use nix::sys::signal::{Signal, kill};
            use nix::unistd::Pid;
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
            info!("{}: sent SIGTERM (pid={pid})", state.name);
        }
        #[cfg(not(target_os = "linux"))]
        {
            warn!(
                "{}: SIGTERM not implemented on this platform (pid={pid})",
                state.name
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Background task lifecycle
// ---------------------------------------------------------------------------

async fn shutdown_background_tasks(
    log_writer: tokio::task::JoinHandle<()>,
    rpc_server: tokio::task::JoinHandle<()>,
    memory_watcher: tokio::task::JoinHandle<()>,
) {
    log_writer.abort();
    rpc_server.abort();
    memory_watcher.abort();
    for (name, result) in [
        ("log-writer", log_writer.await),
        ("rpc-server", rpc_server.await),
        ("memory-watcher", memory_watcher.await),
    ] {
        // is_cancelled() is the normal outcome after abort() — no log needed
        if let Err(e) = result
            && e.is_panic()
        {
            error!("{name} background task had panicked: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// Post-start setup
// ---------------------------------------------------------------------------

async fn post_start(cfg: &MmdsConfig) -> Result<(), Box<dyn std::error::Error>> {
    pg::set_superuser_password(&cfg.postgres_password)
        .await
        .map_err(|e| format!("failed to set postgres password: {e}"))?;

    setup_pgbouncer_auth()
        .await
        .map_err(|e| format!("failed to set up pgbouncer auth: {e}"))?;

    // Required extensions — fail the supervisor if any are missing; the process
    // manager will restart and retry rather than running in a degraded state.
    for ext in REQUIRED_EXTENSIONS {
        pg::psql(&format!("CREATE EXTENSION IF NOT EXISTS {ext}"))
            .await
            .map_err(|e| format!("required extension {ext} failed: {e}"))?;
    }

    for ext in OPTIONAL_EXTENSIONS {
        if let Err(e) = pg::psql(&format!("CREATE EXTENSION IF NOT EXISTS {ext}")).await {
            warn!("optional extension {ext} failed: {e}");
        }
    }

    if cfg.wal_sink.is_some() || cfg.cdc_enabled {
        pg::psql(
            "DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
                 CREATE ROLE replicator LOGIN REPLICATION PASSWORD NULL;
               END IF;
             END
             $$",
        )
        .await
        .map_err(|e| format!("failed to create replicator role: {e}"))?;
    }

    if cfg.cdc_enabled {
        warn!("CDC enabled: replication slot 'cdc' will accumulate WAL until a consumer connects");
        pg::psql(
            "DO $$
             BEGIN
               IF NOT EXISTS (
                 SELECT FROM pg_replication_slots WHERE slot_name = 'cdc'
               ) THEN
                 PERFORM pg_create_logical_replication_slot('cdc', 'pgoutput');
               END IF;
             END
             $$",
        )
        .await
        .map_err(|e| format!("failed to create CDC replication slot: {e}"))?;

        pg::psql(
            "DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'cdc') THEN
                 EXECUTE 'CREATE PUBLICATION cdc';
               END IF;
             END
             $$",
        )
        .await
        .map_err(|e| format!("failed to create CDC publication: {e}"))?;
    }

    boot::run_hook_scripts("/etc/postgresql/18/hooks/post-start.d")
        .await
        .map_err(|e| format!("post-start hook failed: {e}"))?;

    info!("post-start complete");
    Ok(())
}

async fn setup_pgbouncer_auth() -> Result<(), crate::pg::PgError> {
    // Create the pgbouncer role and the SECURITY DEFINER auth lookup function.
    // Idempotent: CREATE IF NOT EXISTS / CREATE OR REPLACE.
    pg::psql(
        "DO $$
         BEGIN
           IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgbouncer') THEN
             CREATE ROLE pgbouncer LOGIN PASSWORD NULL;
           END IF;
         END
         $$",
    )
    .await?;

    pg::psql("CREATE SCHEMA IF NOT EXISTS pgbouncer").await?;

    pg::psql(
        "CREATE OR REPLACE FUNCTION pgbouncer.get_auth(p_user text)
         RETURNS TABLE(username text, password text)
         SECURITY DEFINER LANGUAGE sql AS $$
           SELECT usename::text, passwd::text FROM pg_shadow WHERE usename = p_user
         $$",
    )
    .await?;

    pg::psql("GRANT EXECUTE ON FUNCTION pgbouncer.get_auth(text) TO pgbouncer").await?;

    Ok(())
}

const REQUIRED_EXTENSIONS: &[&str] = &["beyond_auth", "beyond_queue", "pg_cron"];

const OPTIONAL_EXTENSIONS: &[&str] = &[
    "pg_stat_statements",
    "auto_explain",
    "pg_trgm",
    "pgvector",
    "pgvectorscale",
    "pg_partman",
    "pg_jsonschema",
    "hypopg",
    "pg_repack",
    "postgis",
    "pg_search",
];

// ---------------------------------------------------------------------------
// Memory watcher — updates reload-safe tuning on virtio-mem hotplug
// ---------------------------------------------------------------------------

async fn memory_watcher_task(vcpus: u32, initial_ram_bytes: u64) {
    let mut last_ram_bytes = initial_ram_bytes;
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let ram_bytes = match read_memtotal_bytes() {
            Some(v) => v,
            None => continue,
        };
        // Skip if change is ≤ 5% — avoids churn from minor balloon adjustments.
        let delta = ram_bytes.abs_diff(last_ram_bytes);
        if delta * 20 <= last_ram_bytes {
            continue;
        }
        last_ram_bytes = ram_bytes;
        let content = config::tuning_conf_adaptive(ram_bytes, vcpus);
        match config::write_atomic(std::path::Path::new(&config::memory_conf_path()), &content) {
            Ok(()) => info!(
                "memory watcher: updated 02-memory.conf (ram_mb={})",
                ram_bytes / (1024 * 1024)
            ),
            Err(e) => {
                warn!("memory watcher: write failed: {e}");
                continue;
            }
        }
        match pg::reload().await {
            Ok(()) => info!("memory watcher: reloaded postgres config"),
            Err(e) => warn!("memory watcher: pg_reload_conf failed: {e}"),
        }
    }
}

fn read_memtotal_bytes() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
            return Some(kib * 1024);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Log forwarding to host vsock
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
async fn log_writer_task(mut log_rx: mpsc::Receiver<LogFrame>) {
    use tokio::io::AsyncWriteExt;

    loop {
        match tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(HOST_CID, VSOCK_PORT))
            .await
        {
            Ok(mut stream) => {
                info!("log forwarder connected to vsock {HOST_CID}:{VSOCK_PORT}");
                while let Some(frame) = log_rx.recv().await {
                    let payload = UserProcessStreamDataPayload {
                        stream: frame.stream,
                        line: frame.line,
                        truncated: frame.truncated,
                        execution_id: frame.execution_id.to_string(),
                    };
                    let encoded = encode_log_frame(&payload);
                    if stream.write_all(&encoded).await.is_err() {
                        warn!("log vsock write failed, reconnecting");
                        break;
                    }
                }
            }
            Err(e) => {
                warn!("log vsock connect failed: {e}, retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn log_writer_task(mut log_rx: mpsc::Receiver<LogFrame>) {
    // Drain without forwarding — vsock unavailable on this platform.
    while log_rx.recv().await.is_some() {}
}
