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

#[cfg(target_os = "linux")]
pub async fn run(role: handoff::Role) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    if let Err(e) = run_inner(role).await {
        error!("supervisor fatal: {e}");
        std::process::exit(1);
    }
}

#[cfg(not(target_os = "linux"))]
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

#[cfg(target_os = "linux")]
async fn run_inner(role: handoff::Role) -> Result<(), Box<dyn std::error::Error>> {
    run_inner_inner(Some(role)).await
}

#[cfg(not(target_os = "linux"))]
async fn run_inner() -> Result<(), Box<dyn std::error::Error>> {
    run_inner_inner(None::<()>).await
}

#[cfg(target_os = "linux")]
type MaybeRole = Option<handoff::Role>;
#[cfg(not(target_os = "linux"))]
type MaybeRole = Option<()>;

async fn run_inner_inner(role: MaybeRole) -> Result<(), Box<dyn std::error::Error>> {
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

    // Bounded channel for log frames from all child pipes
    let (log_tx, log_rx) = mpsc::channel::<LogFrame>(1024);

    // PersistedChildren is the supervisor's record of who it spawned, so a
    // post-handoff or post-crash successor can adopt those PIDs via pidfd.
    // Written atomically (tmp + rename) after each spawn and on exit.
    #[cfg(target_os = "linux")]
    let mut persisted = crate::children::PersistedChildren::empty();

    // Branch on role: cold start vs successor adoption. The successor path
    // skips boot setup, postgres spawn + readiness wait, and post_start
    // (postgres is already running with all DDL applied by the original
    // supervisor). On successor we adopt the persisted PIDs via pidfd and
    // resume supervision; otherwise we follow the original boot sequence.
    #[cfg(target_os = "linux")]
    let mut begun_successor: Option<handoff::BegunSuccessor> = None;
    // When invoked by beyond-pg-init, the cold-start path inherits the
    // "rpc" listener fd via LISTEN_FDS. Standalone invocations (no init)
    // have no inherited listeners; we bind fresh below.
    #[cfg(target_os = "linux")]
    let mut cold_inherited: Option<handoff::InheritedListeners> = None;
    #[cfg(target_os = "linux")]
    let mut role_successor: Option<handoff::Successor> = None;
    #[cfg(target_os = "linux")]
    if let Some(r) = role {
        match r {
            handoff::Role::ColdStart { inherited } => cold_inherited = Some(inherited),
            handoff::Role::Successor(s) => role_successor = Some(s),
        }
    }

    #[cfg(target_os = "linux")]
    let is_successor = role_successor.is_some();
    #[cfg(not(target_os = "linux"))]
    let is_successor = false;

    let mut pg_state: ChildState;
    let mut pgb_state: ChildState;
    let mut cdc_state: Option<ChildState>;
    let cold_start_wal_handle: Option<tokio::task::JoinHandle<()>>;

    if is_successor {
        cold_start_wal_handle = None;
        #[cfg(target_os = "linux")]
        {
            // Drive the handoff protocol. handshake + wait_for_begin are
            // sync; on current_thread tokio they briefly block, which is
            // fine during this startup-only window.
            let s = role_successor
                .take()
                .expect("is_successor guarded the take above");
            let build_id = env!("CARGO_PKG_VERSION").as_bytes().to_vec();
            let s = s.handshake(build_id)?;
            let s = s.wait_for_begin()?;
            begun_successor = Some(s);

            info!("successor: incumbent sealed; adopting children");

            // Children.json was flushed by the old supervisor right before
            // it released the data-dir lock during seal.
            let prior = crate::children::PersistedChildren::load(&crate::children::state_dir())?;
            pg_state = adopt_or_respawn_child("postgres", &prior, &log_tx, &mut persisted)?;
            pgb_state = adopt_or_respawn_child("pgbouncer", &prior, &log_tx, &mut persisted)?;

            // CDC is configured per-MMDS; adopt only if both the prior
            // supervisor had it AND we still want it. If config flipped
            // off, kill the adopted one (rare; just respawn-fresh logic
            // is reused later if needed).
            cdc_state = if cfg.pg_tier != PgTier::Replica
                && cfg.cdc_enabled
                && prior.get("beyond-pg-cdc").is_some()
            {
                Some(adopt_or_respawn_child(
                    "beyond-pg-cdc",
                    &prior,
                    &log_tx,
                    &mut persisted,
                )?)
            } else {
                None
            };
        }
        #[cfg(not(target_os = "linux"))]
        {
            unreachable!("is_successor is only true on linux");
        }
    } else {
        // Cold-start path: original boot sequence.
        boot::do_boot(&cfg).await?;

        let mut pg = ChildState::new("postgres");
        spawn_postgres(&mut pg, &log_tx)?;
        #[cfg(target_os = "linux")]
        persist_child(&mut persisted, &pg)?;
        pg_state = pg;

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
        let mut wfh: Option<tokio::task::JoinHandle<()>> = None;
        if cfg.pg_tier != PgTier::Replica
            && let Some(wal_sink) = &cfg.wal_sink
        {
            let sink_url = wal_sink.clone();
            wfh = Some(tokio::spawn(crate::wal_forwarder::run(
                sink_url,
                "wal_sink".to_owned(),
                crate::pg::PG_PORT,
            )));
            wait_for_sync_standby("wal_sink", std::time::Duration::from_secs(30)).await;
        }
        cold_start_wal_handle = wfh;

        // Replicas are read-only hot standbys — DDL is rejected during recovery.
        if cfg.pg_tier != PgTier::Replica {
            post_start(&cfg).await?;
        }

        let mut pgb = ChildState::new("pgbouncer");
        spawn_pgbouncer(&mut pgb, &log_tx)?;
        #[cfg(target_os = "linux")]
        persist_child(&mut persisted, &pgb)?;
        pgb_state = pgb;

        cdc_state = if cfg.pg_tier != PgTier::Replica && cfg.cdc_enabled {
            let mut s = ChildState::new("beyond-pg-cdc");
            spawn_cdc(&mut s, &log_tx)?;
            #[cfg(target_os = "linux")]
            persist_child(&mut persisted, &s)?;
            Some(s)
        } else {
            None
        };
    }

    // The cold-start branch above started the WAL forwarder before post_start
    // for sync-replication ordering. The successor branch starts it below
    // (postgres is already running, so reconnect time is the only window
    // commits wait for sync acks).
    let mut wal_forwarder_handle: Option<tokio::task::JoinHandle<()>> = cold_start_wal_handle;
    if is_successor
        && cfg.pg_tier != PgTier::Replica
        && let Some(wal_sink) = &cfg.wal_sink
    {
        let sink_url = wal_sink.clone();
        wal_forwarder_handle = Some(tokio::spawn(crate::wal_forwarder::run(
            sink_url,
            "wal_sink".to_owned(),
            crate::pg::PG_PORT,
        )));
        // Don't wait_for_sync_standby here — postgres is already running and
        // its commits will stall until our new forwarder ACKs (~250ms p99
        // measured). Waiting here would re-introduce a deadlock if there
        // were any holding-the-runtime DDL pending.
    }

    // Background: forward logs to host vsock
    let log_writer_handle = tokio::spawn(log_writer_task(log_rx));

    // Background: serve control RPC.
    //
    // The shared `SharedState` couples this accept loop to the handoff
    // `Drainable`: `accept_paused` pauses new accepts during drain,
    // `in_flight` tracks live handlers so drain can wait for them.
    //
    // On a fresh boot (cold start) we bind the listener ourselves. On the
    // successor path we instead inherit the FD from the previous incumbent
    // (transparently dup'd into slot 3 by `beyond-pg-init`'s handoff
    // supervisor). Inheriting preserves the accept queue — clients see no
    // socket close.
    #[cfg(target_os = "linux")]
    let rpc_state = crate::handoff_bridge::SharedState::new();
    #[cfg(target_os = "linux")]
    let rpc_listener = if is_successor {
        take_inherited_rpc_listener(begun_successor.as_mut().expect("successor"))?
    } else if let Some(mut inh) = cold_inherited.take()
        && let Some(tcp) = inh.take("rpc")
    {
        take_inherited_rpc_from_tcp(tcp)?
    } else {
        crate::rpc::bind_cold_start()?
    };
    #[cfg(target_os = "linux")]
    let rpc_handle = tokio::spawn(crate::rpc::serve(rpc_listener, rpc_state.clone()));
    #[cfg(not(target_os = "linux"))]
    let rpc_handle = tokio::spawn(crate::rpc::serve((), ()));

    // Background: handoff::Incumbent control thread.
    //
    // `Incumbent::serve` is the *sync* loop that handles the protocol from
    // the upper supervisor (beyond-pg-init). It blocks on a unix-socket
    // accept and drives drain/seal callbacks on the same OS thread. Running
    // it on a dedicated `std::thread` keeps the sync API isolated from the
    // tokio current_thread runtime — the drain handler then polls the
    // tokio-side `in_flight` atomic across the boundary.
    //
    // Cold start binds the unix socket via `bind_cold_start`. Successor
    // takes over the socket path atomically via `announce_and_bind` — the
    // bind happens after `Ready` is sent so the prior incumbent's socket
    // ownership is released first.
    #[cfg(target_os = "linux")]
    let _handoff_thread = if let Some(s) = begun_successor.take() {
        spawn_incumbent_thread_successor(s, rpc_state.clone())?
    } else {
        spawn_incumbent_thread(rpc_state.clone())?
    };

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
            code = async {
                if let Some(ref mut handle) = pg_state.handle {
                    handle.wait().await
                } else {
                    std::future::pending().await
                }
            } => {
                let code = code?;
                pg_state.on_exit(code);
                maybe_restart(&mut pg_state, &log_tx).await?;
                #[cfg(target_os = "linux")]
                persist_child(&mut persisted, &pg_state)?;
            }
            code = async {
                if let Some(ref mut handle) = pgb_state.handle {
                    handle.wait().await
                } else {
                    std::future::pending().await
                }
            } => {
                let code = code?;
                pgb_state.on_exit(code);
                maybe_restart_pgbouncer(&mut pgb_state, &log_tx).await?;
                #[cfg(target_os = "linux")]
                persist_child(&mut persisted, &pgb_state)?;
            }
            code = async {
                match cdc_state.as_mut().and_then(|s| s.handle.as_mut()) {
                    Some(handle) => handle.wait().await,
                    None => std::future::pending().await,
                }
            } => {
                let code = code?;
                if let Some(state) = cdc_state.as_mut() {
                    state.on_exit(code);
                    maybe_restart_cdc(state, &log_tx).await?;
                    #[cfg(target_os = "linux")]
                    persist_child(&mut persisted, state)?;
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
    let Some(pid) = state.pid() else {
        info!("pgbouncer: no live child, SIGHUP skipped");
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

/// How the supervisor observes a child's lifecycle.
///
/// `Fresh` — spawned by *this* supervisor process. We hold the
/// `tokio::process::Child` and reap on `.wait()` (we are its direct parent).
///
/// `Adopted` — inherited from a previous supervisor (post-handoff). The
/// child is now PID-1's direct child (reparented when the old supervisor
/// exited). We can't `waitpid` on it from here, so we observe death via a
/// `pidfd` wrapped in `AsyncFd`. PID 1 reaps the zombie.
#[allow(dead_code)] // adopted variant constructed by phase 5b adopt path
enum WaitHandle {
    Fresh(tokio::process::Child),
    #[cfg(target_os = "linux")]
    Adopted {
        pid: u32,
        fd: tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>,
    },
}

impl WaitHandle {
    /// Process id, or `None` if the underlying Child has been reaped.
    fn pid(&self) -> Option<u32> {
        match self {
            WaitHandle::Fresh(c) => c.id(),
            #[cfg(target_os = "linux")]
            WaitHandle::Adopted { pid, .. } => Some(*pid),
        }
    }

    /// Await exit. Returns the exit code for `Fresh`; for `Adopted` we
    /// only know *that* the process died (the kernel notifies via pidfd
    /// readability) — exit status is unobservable because we're not the
    /// parent. `None` therefore covers both "killed by signal" and
    /// "adopted child, status unknown".
    async fn wait(&mut self) -> std::io::Result<Option<i32>> {
        match self {
            WaitHandle::Fresh(c) => {
                let status = c.wait().await?;
                Ok(status.code())
            }
            #[cfg(target_os = "linux")]
            WaitHandle::Adopted { fd, .. } => {
                let _guard = fd.readable().await?;
                Ok(None)
            }
        }
    }
}

struct ChildState {
    name: &'static str,
    handle: Option<WaitHandle>,
    restart_count: u32,
    last_start: Option<Instant>,
}

impl ChildState {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            handle: None,
            restart_count: 0,
            last_start: None,
        }
    }

    /// Record exit. Resets backoff if the child was stable long enough.
    fn on_exit(&mut self, code: Option<i32>) {
        self.handle = None;
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
        self.handle = Some(WaitHandle::Fresh(child));
        self.last_start = Some(Instant::now());
        self.restart_count = self.restart_count.saturating_add(1);
    }

    /// Adopt an already-running pid via pidfd. Used on the successor path
    /// after handoff to pick up postgres/pgbouncer/cdc children that are
    /// now reparented to init (PID 1). The fd must already be wrapped in
    /// `AsyncFd` so tokio can observe readability on exit.
    #[cfg(target_os = "linux")]
    fn record_adopted(&mut self, pid: u32, fd: tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>) {
        self.handle = Some(WaitHandle::Adopted { pid, fd });
        self.last_start = Some(Instant::now());
        // Reset restart_count — adopted means previous supervisor was
        // running stably enough that handoff was attempted.
        self.restart_count = 0;
    }

    fn pid(&self) -> Option<u32> {
        self.handle.as_ref().and_then(|h| h.pid())
    }
}

/// Persist a single child's pid+starttime into the on-disk state file.
///
/// Called after every spawn (cold-start and restart-on-crash) so a
/// successor or post-crash supervisor can `pidfd_open` the saved pid.
#[cfg(target_os = "linux")]
fn persist_child(
    persisted: &mut crate::children::PersistedChildren,
    state: &ChildState,
) -> std::io::Result<()> {
    if let Some(pid) = state.pid() {
        persisted.record(state.name, pid)?;
        persisted.save(&crate::children::state_dir())?;
    }
    Ok(())
}

/// Path of the unix socket where this incumbent listens for upgrade requests
/// from `beyond-pg-init`. Matches the path init expects.
#[cfg(target_os = "linux")]
const HANDOFF_SOCKET_PATH: &str = "/var/lib/beyond-pg/state/handoff.sock";

/// Adopt the named child from a `PersistedChildren` snapshot via pidfd, or
/// respawn fresh if it's dead or its pid has been recycled.
///
/// Adoption verifies `(pid, starttime)` match the saved record before
/// wrapping a `pidfd` in `AsyncFd` for the supervision loop. On any failure
/// path we fall through to a fresh spawn — the postgres-side state is
/// durable, so a respawn is safe (just visible as a brief disconnect for
/// clients on the affected service).
#[cfg(target_os = "linux")]
fn adopt_or_respawn_child(
    name: &'static str,
    prior: &crate::children::PersistedChildren,
    log_tx: &mpsc::Sender<LogFrame>,
    persisted: &mut crate::children::PersistedChildren,
) -> Result<ChildState, Box<dyn std::error::Error>> {
    use crate::children::AdoptResult;
    let mut state = ChildState::new(name);

    if let Some(record) = prior.get(name) {
        match crate::children::adopt(record)? {
            AdoptResult::Adopted(fd) => {
                let async_fd =
                    tokio::io::unix::AsyncFd::with_interest(fd, tokio::io::Interest::READABLE)?;
                state.record_adopted(record.pid, async_fd);
                info!("{name}: adopted pid={} via pidfd", record.pid);
                // Re-record into the new supervisor's persisted state so a
                // subsequent crash/handoff finds the same pid.
                persisted.record(name, record.pid)?;
                persisted.save(&crate::children::state_dir())?;
                return Ok(state);
            }
            AdoptResult::Dead => {
                warn!("{name}: prior pid={} is dead; respawning", record.pid);
            }
            AdoptResult::Recycled { saved, live } => {
                warn!(
                    "{name}: prior pid={} starttime mismatch (saved={saved} live={live}); respawning",
                    record.pid
                );
                // We can't trust this pid; ignore it and respawn.
            }
        }
    } else {
        warn!("{name}: no prior record in children.json; spawning fresh");
    }

    // Fall through to a fresh spawn.
    match name {
        "postgres" => spawn_postgres(&mut state, log_tx)?,
        "pgbouncer" => spawn_pgbouncer(&mut state, log_tx)?,
        "beyond-pg-cdc" => spawn_cdc(&mut state, log_tx)?,
        other => return Err(format!("unknown child name in adoption: {other}").into()),
    };
    persist_child(persisted, &state)?;
    Ok(state)
}

/// Spawn the sync `Incumbent::serve` loop on its own OS thread (cold start).
///
/// The path here is the same one `beyond-pg-init` uses when constructing the
/// `handoff::Supervisor`; both ends agree on this filesystem rendezvous.
#[cfg(target_os = "linux")]
fn spawn_incumbent_thread(
    state: crate::handoff_bridge::SharedState,
) -> Result<std::thread::JoinHandle<()>, Box<dyn std::error::Error>> {
    use crate::children::state_dir;
    use std::path::Path;

    std::fs::create_dir_all(state_dir())?;
    let lock = handoff::DataDirLock::acquire_or_break_stale(&state_dir())?;
    let incumbent = handoff::Incumbent::bind_cold_start(Path::new(HANDOFF_SOCKET_PATH), lock)?;
    let drainable = crate::handoff_bridge::SupervisorDrainable::new(state);
    let handle = std::thread::Builder::new()
        .name("handoff-incumbent".into())
        .spawn(move || {
            if let Err(e) = incumbent.serve(drainable) {
                tracing::error!("handoff::Incumbent::serve exited with error: {e}");
            }
        })?;
    tracing::info!("handoff::Incumbent serving on {HANDOFF_SOCKET_PATH} (cold start)");
    Ok(handle)
}

/// Successor variant: send `Ready` and bind the handoff socket atomically.
///
/// The data-dir lock was released by the prior incumbent during seal, so
/// `acquire` here always succeeds. `announce_and_bind` orders Ready + bind
/// safely against the abort path: if we crash between Ready and bind, the
/// prior incumbent's `resume_after_abort` reacquires the lock and rebinds.
#[cfg(target_os = "linux")]
fn spawn_incumbent_thread_successor(
    s: handoff::BegunSuccessor,
    state: crate::handoff_bridge::SharedState,
) -> Result<std::thread::JoinHandle<()>, Box<dyn std::error::Error>> {
    use crate::children::state_dir;
    use std::path::Path;

    let lock = handoff::DataDirLock::acquire(&state_dir())?;
    let snapshot = handoff::drainable::ReadinessSnapshot {
        listening_on: vec![format!("vsock::{}", crate::vsock::RPC_PORT)],
        healthz_ok: true,
        advertised_revision_per_shard: Vec::new(),
    };
    let incumbent = s.announce_and_bind(snapshot, Path::new(HANDOFF_SOCKET_PATH), lock)?;
    let drainable = crate::handoff_bridge::SupervisorDrainable::new(state);
    let handle = std::thread::Builder::new()
        .name("handoff-incumbent".into())
        .spawn(move || {
            if let Err(e) = incumbent.serve(drainable) {
                tracing::error!("handoff::Incumbent::serve exited with error: {e}");
            }
        })?;
    tracing::info!("handoff::Incumbent serving on {HANDOFF_SOCKET_PATH} (successor)");
    Ok(handle)
}

/// Take the inherited "rpc" listener fd from a `BegunSuccessor` and wrap it
/// as the right async listener type.
///
/// Handoff's `take_listener` returns a `TcpListener`. Underneath that's just
/// a wrapper around the inherited raw fd — the kernel doesn't validate
/// socket family. We unwrap back to a `RawFd` via `into_raw_fd`, query the
/// actual socket family via `getsockname`, set the non-blocking flag the
/// tokio runtime expects, and reclaim ownership as either `VsockListener`
/// (production) or `tokio::net::UnixListener` (tests / local dev).
#[cfg(target_os = "linux")]
fn take_inherited_rpc_listener(
    s: &mut handoff::BegunSuccessor,
) -> Result<crate::rpc::RpcListener, Box<dyn std::error::Error>> {
    use std::os::fd::{FromRawFd, IntoRawFd};

    let tcp = s
        .take_listener("rpc")
        .ok_or("inherited listener 'rpc' missing from successor")?;
    let raw_fd = tcp.into_raw_fd();

    // SAFETY: `raw_fd` is owned (we just took it from TcpListener::into_raw_fd).
    // fcntl(F_GETFL/F_SETFL) only reads/writes the fd's status flags.
    let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if unsafe { libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }

    // Detect socket family via getsockname so we wrap as the matching
    // tokio listener type. AF_VSOCK (40) → vsock; AF_UNIX (1) → unix.
    let family = socket_family(raw_fd)?;
    match family {
        libc::AF_UNIX => {
            // SAFETY: `raw_fd` is owned by us; from_raw_fd takes that ownership.
            let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(raw_fd) };
            std_listener.set_nonblocking(true)?;
            let tokio_listener = tokio::net::UnixListener::from_std(std_listener)?;
            Ok(crate::rpc::RpcListener::Unix(tokio_listener))
        }
        // AF_VSOCK is 40 on Linux; libc may not expose it as a constant on
        // all platforms (it varies by libc version). Match it as the literal.
        40 => {
            // SAFETY: same — we just took ownership of raw_fd from TcpListener.
            let v = unsafe { tokio_vsock::VsockListener::from_raw_fd(raw_fd) };
            Ok(crate::rpc::RpcListener::Vsock(v))
        }
        other => Err(format!("inherited rpc fd has unexpected socket family: {other}").into()),
    }
}

/// Same as `take_inherited_rpc_listener` but for the cold-start case where
/// `handoff::detect_role` returned `ColdStart { inherited }` and we got the
/// `TcpListener` from `InheritedListeners::take("rpc")` directly.
#[cfg(target_os = "linux")]
fn take_inherited_rpc_from_tcp(
    tcp: std::net::TcpListener,
) -> Result<crate::rpc::RpcListener, Box<dyn std::error::Error>> {
    use std::os::fd::{FromRawFd, IntoRawFd};

    let raw_fd = tcp.into_raw_fd();
    let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if unsafe { libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    match socket_family(raw_fd)? {
        libc::AF_UNIX => {
            let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(raw_fd) };
            std_listener.set_nonblocking(true)?;
            let tokio_listener = tokio::net::UnixListener::from_std(std_listener)?;
            Ok(crate::rpc::RpcListener::Unix(tokio_listener))
        }
        40 => {
            let v = unsafe { tokio_vsock::VsockListener::from_raw_fd(raw_fd) };
            Ok(crate::rpc::RpcListener::Vsock(v))
        }
        other => Err(format!("inherited rpc fd has unexpected socket family: {other}").into()),
    }
}

#[cfg(target_os = "linux")]
fn socket_family(fd: std::os::fd::RawFd) -> std::io::Result<libc::c_int> {
    // sockaddr_storage is large enough for any socket family.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: storage is a stack buffer of correct size; `len` is initialized
    // to the buffer's capacity; getsockname writes `<= len` bytes and updates
    // `len` to the actual size.
    let r =
        unsafe { libc::getsockname(fd, &mut storage as *mut _ as *mut libc::sockaddr, &mut len) };
    if r < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(storage.ss_family as libc::c_int)
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

    // Poll for all to exit, up to DRAIN_TIMEOUT.
    //
    // For Fresh we have a Child and can call try_wait(); for Adopted we'd
    // need a non-blocking pidfd check. We use `kill(pid, 0)` as the
    // portable cross-shape "is this pid alive?" check.
    let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
    loop {
        let all_done = all.iter().all(|s| s.pid().map(is_pid_dead).unwrap_or(true));
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

    // SIGKILL any stragglers.
    for state in all {
        if let Some(pid) = state.pid()
            && !is_pid_dead(pid)
        {
            warn!("{}: sending SIGKILL (pid={pid})", state.name);
            send_signal_to_pid(pid, libc::SIGKILL);
        }
        // Reap if it's a Fresh handle we own.
        if let Some(WaitHandle::Fresh(ref mut ch)) = state.handle {
            let _ = ch.wait().await;
        }
    }
}

#[cfg(target_os = "linux")]
fn is_pid_dead(pid: u32) -> bool {
    // `kill(pid, 0)` returns 0 if the process exists, ESRCH if not.
    let r = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if r == 0 {
        false
    } else {
        std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    }
}

#[cfg(not(target_os = "linux"))]
fn is_pid_dead(_pid: u32) -> bool {
    true
}

#[cfg(target_os = "linux")]
fn send_signal_to_pid(pid: u32, sig: libc::c_int) {
    unsafe { libc::kill(pid as libc::pid_t, sig) };
}

#[cfg(not(target_os = "linux"))]
fn send_signal_to_pid(_pid: u32, _sig: libc::c_int) {}

fn sigterm_child(state: &mut ChildState) {
    if let Some(pid) = state.pid() {
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
        let ram_bytes = match read_memtotal_bytes().await {
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

async fn read_memtotal_bytes() -> Option<u64> {
    // `/proc/meminfo` is normally fast, but under memory pressure — exactly
    // when this watcher matters most — the kernel may block. Stay on tokio's
    // I/O reactor so we don't stall the executor.
    let content = tokio::fs::read_to_string("/proc/meminfo").await.ok()?;
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
