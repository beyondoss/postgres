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

    // WAL forwarder must start BEFORE post_start.
    //
    // `03-wal-sink.conf` sets `synchronous_commit = remote_write` and
    // `synchronous_standby_names = 'wal_sink'`.  Every commit in post_start
    // blocks until the 'wal_sink' streaming standby ACKs the WAL.  The
    // wal_forwarder connects to postgres with application_name='wal_sink',
    // making it that standby.  Spawning it after post_start causes a deadlock.
    //
    // The handle is retained so we can abort the task on shutdown.  Aborting
    // the task drops ack_tx, which unblocks the pg_reader_thread
    // (spawn_blocking).  Without the explicit abort, the tokio current_thread
    // runtime deadlocks on drop: it drains the blocking pool before dropping
    // tasks, so ack_tx is never dropped and blocking_recv() blocks forever.
    let mut wal_forwarder_handle: Option<tokio::task::JoinHandle<()>> = None;
    if cfg.pg_tier != PgTier::Replica
        && let Some(wal_sink) = &cfg.wal_sink
    {
        let sink_url = wal_sink.clone();
        wal_forwarder_handle = Some(tokio::spawn(crate::wal_forwarder::run(
            sink_url,
            "wal_sink".to_owned(),
            crate::pg::PG_PORT,
        )));
        wait_for_sync_standby("wal_sink", std::time::Duration::from_secs(30)).await;
    }

    // Replicas are read-only hot standbys — DDL is rejected during recovery.
    // Extensions, roles, and slots are replicated from the primary.
    if cfg.pg_tier != PgTier::Replica {
        post_start(&cfg).await?;
    }

    // Spawn PgBouncer after post-start so the pgbouncer role and auth function exist.
    // PgBouncer is useful on replicas too for read-only connection pooling.
    let mut pgb_state = ChildState::new("pgbouncer");
    spawn_pgbouncer(&mut pgb_state, &log_tx)?;

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

    // Background: watch the platform TLS cert for rotation, if applicable.
    // The guest agent atomically replaces /run/beyond/tls/cert.pem every ~22h.
    // Postgres and PgBouncer cache the cert in shared memory and only re-read
    // on SIGHUP, so we drive both reloads from the watcher.
    let platform_cert = std::path::PathBuf::from(crate::tls::PLATFORM_TLS_DIR).join("cert.pem");
    let (cert_reload_tx, mut cert_reload_rx) = mpsc::channel::<()>(4);
    let cert_watcher_handle = if platform_cert.exists() {
        Some(crate::cert_watcher::spawn(platform_cert, cert_reload_tx))
    } else {
        info!("cert_watcher: no platform cert, watcher disabled");
        None
    };

    // Signal handles — must be set up after blocking the signals
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    // Unblock SIGTERM/SIGINT now that tokio's sigaction handlers are installed.
    // The block was held during startup to prevent SIG_DFL from killing the
    // process before tokio was ready to receive signals.  Any signal that
    // arrived while blocked becomes pending and is delivered to the handler now.
    #[cfg(target_os = "linux")]
    {
        use nix::sys::signal::{SigSet, SigmaskHow, Signal, sigprocmask};
        let mut set = SigSet::empty();
        set.add(Signal::SIGTERM);
        set.add(Signal::SIGINT);
        sigprocmask(SigmaskHow::SIG_UNBLOCK, Some(&set), None)?;
    }

    info!("supervisor running");

    // Main supervision loop
    loop {
        // Both children must have a live Child handle to select on.
        // If a child exited and is in backoff, we create a dummy future that
        // never resolves (handled by the restart logic below).
        tokio::select! {
            _ = sigterm.recv() => {
                info!("received SIGTERM, draining children");
                abort_wal_forwarder(&mut wal_forwarder_handle).await;
                drain_children(&mut pg_state, &mut pgb_state, cdc_state.as_mut()).await;
                shutdown_background_tasks(log_writer_handle, rpc_handle, memory_watcher_handle, cert_watcher_handle).await;
                return Ok(());
            }
            _ = sigint.recv() => {
                info!("received SIGINT, draining children");
                abort_wal_forwarder(&mut wal_forwarder_handle).await;
                drain_children(&mut pg_state, &mut pgb_state, cdc_state.as_mut()).await;
                shutdown_background_tasks(log_writer_handle, rpc_handle, memory_watcher_handle, cert_watcher_handle).await;
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
            Some(()) = cert_reload_rx.recv() => {
                info!("cert rotated, reloading postgres and pgbouncer");
                if let Err(e) = pg::reload().await {
                    warn!("pg_ctl reload after cert rotation failed: {e}");
                }
                sighup_pgbouncer(&pgb_state);
            }
        }
    }
}

/// Send SIGHUP to PgBouncer to reload its config (used for cert rotation).
/// Best-effort: if pgbouncer is between exit and restart, the pid is absent
/// and the next spawn will pick up the rotated cert anyway.
fn sighup_pgbouncer(state: &ChildState) {
    let Some(child) = state.child.as_ref() else {
        info!("pgbouncer: no live child, SIGHUP skipped");
        return;
    };
    let Some(pid) = child.id() else {
        info!("pgbouncer: child has no pid (already reaped), SIGHUP skipped");
        return;
    };
    #[cfg(target_os = "linux")]
    {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;
        match kill(Pid::from_raw(pid as i32), Signal::SIGHUP) {
            Ok(()) => info!("pgbouncer: SIGHUP sent (pid={pid})"),
            Err(e) => warn!("pgbouncer: SIGHUP failed (pid={pid}): {e}"),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid; // suppress unused warning on non-Linux dev hosts
        info!("pgbouncer: SIGHUP skipped (non-Linux build)");
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
    // protect_postmaster_from_oom writes /proc/self/oom_score_adj=-1000 which
    // requires CAP_SYS_RESOURCE (held while still root). Must run before uid drop.
    protect_postmaster_from_oom(&mut cmd);
    // postgres refuses to execute as root — drop to the postgres OS user.
    drop_to_postgres_user(&mut cmd)?;

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
    // pgbouncer refuses to run as root; drop to postgres OS user.
    drop_to_postgres_user(&mut cmd)?;

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

/// Drop the postgres child process to the `postgres` OS user (uid/gid).
/// postgres(1) refuses to execute as root; the supervisor runs as root (PID 1).
/// uid/gid are resolved once via getpwnam; the lookup is not async-signal-safe
/// so it runs in the parent before fork, not in a pre_exec hook.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn drop_to_postgres_user(
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] cmd: &mut Command,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "linux")]
    {
        let name = std::ffi::CString::new("postgres")?;
        // SAFETY: getpwnam is not thread-safe, but this is called before we
        // spawn any threads that could call it concurrently.  The returned
        // pointer is valid until the next getpwnam call in this thread.
        let pw = unsafe { libc::getpwnam(name.as_ptr()) };
        if pw.is_null() {
            return Err("postgres OS user not found".into());
        }
        let (uid, gid) = unsafe { ((*pw).pw_uid, (*pw).pw_gid) };
        cmd.uid(uid).gid(gid);
    }
    Ok(())
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
    cert_watcher: Option<tokio::task::JoinHandle<()>>,
) {
    log_writer.abort();
    rpc_server.abort();
    memory_watcher.abort();
    if let Some(h) = &cert_watcher {
        h.abort();
    }
    let mut results: Vec<(&str, _)> = vec![
        ("log-writer", log_writer.await),
        ("rpc-server", rpc_server.await),
        ("memory-watcher", memory_watcher.await),
    ];
    if let Some(h) = cert_watcher {
        results.push(("cert-watcher", h.await));
    }
    for (name, result) in results {
        // is_cancelled() is the normal outcome after abort() — no log needed
        if let Err(e) = result
            && e.is_panic()
        {
            error!("{name} background task had panicked: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// WAL forwarder shutdown
// ---------------------------------------------------------------------------

/// Abort the wal_forwarder task and wait for it to be dropped.
///
/// This must run before `drain_children` because the tokio current_thread
/// runtime drains the blocking pool before dropping tasks.  The
/// pg_reader_thread (spawn_blocking) waits on `ack_rx.blocking_recv()`.
/// `ack_tx` lives in the wal_forwarder task's stack frame and is only
/// dropped when the task is cancelled.  Without an explicit abort here, the
/// runtime's drop deadlocks: blocking pool waits for pg_reader_thread;
/// pg_reader_thread waits for ack_tx; ack_tx never drops because tasks are
/// dropped after the blocking pool.
///
/// Aborting also closes the TCP connection to postgres's walsender, letting
/// postgres exit its smart shutdown within DRAIN_TIMEOUT instead of waiting
/// for wal_sender_timeout (60 s default).
async fn abort_wal_forwarder(handle: &mut Option<tokio::task::JoinHandle<()>>) {
    if let Some(h) = handle.take() {
        h.abort();
        let _ = h.await; // JoinError::Cancelled is expected and ignored
        info!("wal forwarder task aborted");
    }
}

// ---------------------------------------------------------------------------
// Sync-standby readiness
// ---------------------------------------------------------------------------

/// Poll `pg_stat_replication` until `application_name` appears as a sync
/// standby or `timeout` elapses.  Called after spawning the wal_forwarder so
/// post_start's commits are not blocked by a missing sync standby.
async fn wait_for_sync_standby(name: &str, timeout: std::time::Duration) {
    let deadline = std::time::Instant::now() + timeout;
    // Use a DO block so psql exits non-zero when no matching row exists.
    // A plain SELECT exits 0 even when it returns 0 rows, so is_ok() would
    // always succeed and we would never actually wait.
    let sql = format!(
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT 1 FROM pg_stat_replication \
                          WHERE application_name = '{name}' \
                          AND sync_state IN ('sync','quorum')) \
           THEN RAISE EXCEPTION '{name} not yet a sync standby'; \
           END IF; \
         END $$"
    );
    loop {
        if pg::psql(&sql).await.is_ok() {
            info!("sync standby '{name}' is ready");
            return;
        }
        if std::time::Instant::now() >= deadline {
            warn!("timeout waiting for sync standby '{name}'; proceeding anyway");
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
        pg::psql(crate::sql::REPLICATOR_ROLE_SQL)
            .await
            .map_err(|e| format!("failed to create replicator role: {e}"))?;
    }

    if cfg.cdc_enabled {
        warn!("CDC enabled: replication slot 'cdc' will accumulate WAL until a consumer connects");
        pg::psql(crate::sql::CDC_SLOT_SQL)
            .await
            .map_err(|e| format!("failed to create CDC replication slot: {e}"))?;

        pg::psql(crate::sql::CDC_PUBLICATION_SQL)
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
    parse_memtotal_kib(&content).map(|kib| kib * 1024)
}

fn parse_memtotal_kib(content: &str) -> Option<u64> {
    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
            return Some(kib);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_memtotal_standard_format() {
        let input = "MemTotal:       16384000 kB\nMemFree:        8192000 kB\n";
        assert_eq!(parse_memtotal_kib(input), Some(16_384_000));
    }

    #[test]
    fn parse_memtotal_missing_returns_none() {
        let input = "MemFree:  1000 kB\nSwapTotal:  0 kB\n";
        assert_eq!(parse_memtotal_kib(input), None);
    }

    #[test]
    fn parse_memtotal_empty_returns_none() {
        assert_eq!(parse_memtotal_kib(""), None);
    }

    #[test]
    fn memory_watcher_threshold_5pct() {
        // The watcher skips updates when delta * 20 <= last (i.e., delta ≤ 5%).
        let last = 4_096_000_000u64; // ~4 GB

        // 4% change — skip
        let delta = last * 4 / 100;
        assert!(delta * 20 <= last, "4% should be below threshold (skip)");

        // 5% change — still skip (condition is ≤)
        let delta = last / 20;
        assert!(delta * 20 <= last, "5% should be at threshold (skip)");

        // 6% change — trigger update
        let delta = last * 6 / 100;
        assert!(delta * 20 > last, "6% should exceed threshold (update)");
    }
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
