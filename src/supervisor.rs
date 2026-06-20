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

/// Stable, supervisor-persisted names for extra PgBouncer so_reuseport workers
/// (worker 0 keeps the name "pgbouncer" for handoff compatibility). Capped at 7
/// because `config::pgbouncer_workers` caps total workers at 8.
const PGB_EXTRA_NAMES: [&str; 7] = [
    "pgbouncer-1",
    "pgbouncer-2",
    "pgbouncer-3",
    "pgbouncer-4",
    "pgbouncer-5",
    "pgbouncer-6",
    "pgbouncer-7",
];

/// Wait for ANY child in the slice to exit; returns its exit code and index.
/// Pends forever on an empty slice. Hand-rolled `select_all` (no `futures` dep):
/// persistent per-child wait futures are built once and polled together, so no
/// progress is lost between polls.
async fn wait_any_child(workers: &mut [ChildState]) -> (std::io::Result<Option<i32>>, usize) {
    if workers.is_empty() {
        std::future::pending::<()>().await;
        unreachable!();
    }
    let mut futs: Vec<_> = workers
        .iter_mut()
        .map(|w| {
            let h = w.handle.as_mut();
            Box::pin(async move {
                match h {
                    Some(h) => h.wait().await,
                    None => std::future::pending::<std::io::Result<Option<i32>>>().await,
                }
            })
        })
        .collect();
    std::future::poll_fn(|cx| {
        for (i, f) in futs.iter_mut().enumerate() {
            if let std::task::Poll::Ready(r) = f.as_mut().poll(cx) {
                return std::task::Poll::Ready((r, i));
            }
        }
        std::task::Poll::Pending
    })
    .await
}

// ---------------------------------------------------------------------------
// PgBouncer reactive scaler
// ---------------------------------------------------------------------------
//
// The single-threaded pooler saturates one core terminating TLS under
// connection churn (~2.4k conns/s/core, measured in bench/glidefs-pg §F). When a
// box is genuinely pooler-CPU-bound we add so_reuseport workers — the kernel
// load-balances new connections across them (proven linear: pooler CPU 0.79→1.59
// cores for 2 workers) — and reap them when load falls, so an idle/scaled-to-zero
// box runs exactly ONE pooler and only a busy box spends extra cores.
//
// The decision is an inline select! arm (not a spawned task) because it mutates
// pgb_state/pgb_extra to spawn and SIGINT workers.

/// How often the scaler samples pooler CPU and reconsiders the worker count.
const SCALER_TICK: Duration = Duration::from_secs(5);
/// Per-worker CPU (cores) above which the tier is "hot". 0.75 ≈ nearly saturated
/// (a single pooler tops ~0.79–1.0 cores in the rig).
const SCALE_UP_CORES: f64 = 0.75;
/// Low-water per-worker CPU (cores) the tier must stay UNDER — computed as if one
/// worker were already removed — to be considered cold enough to reap.
const SCALE_DOWN_CORES: f64 = 0.5;
/// Consecutive hot ticks before scaling up (≈10s) — react fast.
const SCALE_UP_TICKS: u32 = 2;
/// Consecutive cold ticks before scaling down (≈30s) — react slow, to avoid
/// thrash and minimize the ~0.03% reconnect blip a reap causes.
const SCALE_DOWN_TICKS: u32 = 6;
/// Minimum gap between scale-UP actions.
const SCALE_UP_COOLDOWN: Duration = Duration::from_secs(30);
/// Minimum gap between scale-DOWN actions.
const SCALE_DOWN_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ScaleAction {
    Up,
    Down,
    Hold,
}

/// Linux USER_HZ (`sysconf(_SC_CLK_TCK)`): the ticks-per-second that
/// /proc/<pid>/stat utime+stime are denominated in. Effectively always 100, but
/// read it to be correct; fall back to 100.
fn clk_tck() -> f64 {
    #[cfg(unix)]
    {
        let v = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if v > 0 { v as f64 } else { 100.0 }
    }
    #[cfg(not(unix))]
    {
        100.0
    }
}

/// Pure scaling decision: given current per-worker load + streak/cooldown state,
/// return the action and the updated streak counters. Side-effect-free so it can
/// be unit-tested without spawning processes; the caller stamps `last_action`
/// and adopts the returned streaks.
fn decide_scale(
    per_worker_cores: f64,
    aggregate_cores: f64,
    live: usize,
    max: usize,
    up_streak: u32,
    down_streak: u32,
    since_last_action: Duration,
) -> (ScaleAction, u32, u32) {
    // Hot streak: per-worker is saturated.
    let up_streak = if per_worker_cores > SCALE_UP_CORES {
        up_streak.saturating_add(1)
    } else {
        0
    };
    // Cold streak: even after dropping one worker, the remaining set would stay
    // under the low-water mark — so the extra worker is dead weight.
    let cold = live > 1 && aggregate_cores / (live as f64 - 1.0) < SCALE_DOWN_CORES;
    let down_streak = if cold {
        down_streak.saturating_add(1)
    } else {
        0
    };

    if up_streak >= SCALE_UP_TICKS && live < max && since_last_action >= SCALE_UP_COOLDOWN {
        return (ScaleAction::Up, 0, 0);
    }
    if down_streak >= SCALE_DOWN_TICKS && live > 1 && since_last_action >= SCALE_DOWN_COOLDOWN {
        return (ScaleAction::Down, 0, 0);
    }
    (ScaleAction::Hold, up_streak, down_streak)
}

/// Sampling + decision state carried across ticks (impure half: reads /proc).
struct PgbScaler {
    clk_tck: f64,
    max_workers: usize,
    /// Per-pid cumulative CPU ticks from the previous sample, keyed by pid so a
    /// worker added/reaped between ticks can't corrupt the aggregate delta (only
    /// pids present in BOTH samples contribute).
    prev_ticks: std::collections::HashMap<u32, u64>,
    prev_at: Option<Instant>,
    up_streak: u32,
    down_streak: u32,
    last_action_at: Option<Instant>,
}

impl PgbScaler {
    fn new(max_workers: usize) -> Self {
        Self {
            clk_tck: clk_tck(),
            max_workers,
            prev_ticks: std::collections::HashMap::new(),
            prev_at: None,
            up_streak: 0,
            down_streak: 0,
            last_action_at: None,
        }
    }

    /// Production entry point: sample pooler CPU from `/proc`, decide, publish
    /// telemetry, return the action. Thin wrapper over [`Self::tick_with`] with
    /// the real `/proc` sampler injected.
    fn tick(
        &mut self,
        pids: &[u32],
        now: Instant,
        stats: &crate::handoff_bridge::PoolerStatsHandle,
    ) -> ScaleAction {
        self.tick_with(pids, now, stats, |pid| {
            crate::children::read_proc_cpu_ticks(pid).unwrap_or(0)
        })
    }

    /// Sample pooler CPU for `pids` via `sample` (cumulative ticks per pid),
    /// decide, publish telemetry to `stats`, and return the action for the
    /// caller to enact. The first call only establishes a CPU baseline (returns
    /// Hold). `sample` is injectable so the whole orchestration — the
    /// prev-ticks-across-worker-churn map, the cores math, telemetry, and the
    /// decision — is deterministically testable without real processes or load.
    fn tick_with(
        &mut self,
        pids: &[u32],
        now: Instant,
        stats: &crate::handoff_bridge::PoolerStatsHandle,
        sample: impl Fn(u32) -> u64,
    ) -> ScaleAction {
        let live = pids.len();
        let mut cur: std::collections::HashMap<u32, u64> =
            std::collections::HashMap::with_capacity(live);
        let mut delta_ticks: u64 = 0;
        for &pid in pids {
            let t = sample(pid);
            if let Some(&prev) = self.prev_ticks.get(&pid) {
                delta_ticks += t.saturating_sub(prev);
            }
            cur.insert(pid, t);
        }
        let elapsed = self
            .prev_at
            .map(|p| now.saturating_duration_since(p).as_secs_f64())
            .unwrap_or(0.0);
        self.prev_ticks = cur;
        self.prev_at = Some(now);

        if elapsed <= 0.0 || live == 0 {
            return ScaleAction::Hold; // baseline tick (or no pooler live)
        }

        let aggregate_cores = (delta_ticks as f64 / self.clk_tck) / elapsed;
        let per_worker = aggregate_cores / live as f64;
        let since = self
            .last_action_at
            .map(|t| now.saturating_duration_since(t))
            .unwrap_or(Duration::MAX);

        let (action, up, down) = decide_scale(
            per_worker,
            aggregate_cores,
            live,
            self.max_workers,
            self.up_streak,
            self.down_streak,
            since,
        );
        self.up_streak = up;
        self.down_streak = down;
        if action != ScaleAction::Hold {
            self.last_action_at = Some(now);
        }

        if let Ok(mut s) = stats.lock() {
            s.live_workers = live as u32;
            s.max_workers = self.max_workers as u32;
            s.aggregate_cpu_cores = (aggregate_cores * 100.0).round() / 100.0;
            s.per_worker_cores = (per_worker * 100.0).round() / 100.0;
            s.at_ceiling = live >= self.max_workers && per_worker > SCALE_UP_CORES;
            match action {
                ScaleAction::Up => s.last_action = "up".into(),
                ScaleAction::Down => s.last_action = "down".into(),
                ScaleAction::Hold => {}
            }
        }
        action
    }
}

/// What [`apply_scale_action`] actually did, so the caller can persist/log with
/// the right worker and count.
#[derive(Debug, PartialEq, Eq)]
enum ScaleOutcome {
    /// Spawned and pushed an extra worker with this stable name.
    SpawnedUp(&'static str),
    /// SIGINT'd the named worker for graceful drain (still in `pgb_extra` until
    /// it exits and the `wait_any_child` arm reaps it).
    ReapedDown(&'static str),
    /// Nothing changed (Hold, at the name cap, spawn failed, or nothing to reap).
    NoOp,
}

/// Enact a [`ScaleAction`] on the extra-worker set. Process I/O is injected via
/// `spawn` (build+start a worker; `None` = failed) and `reap` (signal one to
/// drain) so the name-selection, bounds, and set mutation are unit-testable
/// without forking real pgbouncers. The new worker's name index is exactly the
/// current extra count (worker 0 is `pgb_state`); `decide_scale` guarantees
/// `live < max ≤ 8`, and the explicit bound is belt-and-suspenders.
fn apply_scale_action(
    action: ScaleAction,
    pgb_extra: &mut Vec<ChildState>,
    spawn: impl FnOnce(&'static str) -> Option<ChildState>,
    reap: impl FnOnce(&ChildState),
) -> ScaleOutcome {
    match action {
        ScaleAction::Up => {
            let idx = pgb_extra.len();
            if idx >= PGB_EXTRA_NAMES.len() {
                return ScaleOutcome::NoOp; // already at the worker-name cap
            }
            let name = PGB_EXTRA_NAMES[idx];
            match spawn(name) {
                Some(w) => {
                    pgb_extra.push(w);
                    ScaleOutcome::SpawnedUp(name)
                }
                None => ScaleOutcome::NoOp,
            }
        }
        ScaleAction::Down => match pgb_extra.last() {
            Some(w) => {
                let name = w.name;
                reap(w);
                ScaleOutcome::ReapedDown(name)
            }
            None => ScaleOutcome::NoOp,
        },
        ScaleAction::Hold => ScaleOutcome::NoOp,
    }
}

/// How many extra pooler workers to warm-start on cold boot: the count that was
/// live before the restart (from the durable children.json), clamped to the
/// current box's worker ceiling minus worker 0 — so a box that was resized
/// smaller during the restart doesn't over-spawn.
fn warm_start_extra_count(prior_live_extras: usize, vcpus: u32) -> usize {
    prior_live_extras.min(crate::config::pgbouncer_max_workers(vcpus).saturating_sub(1))
}

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
    // Extra PgBouncer so_reuseport workers (worker 0 is pgb_state). Empty on boxes
    // ≤ 8 vCPU, so everything below is a no-op there and the common path is unchanged.
    let mut pgb_extra: Vec<ChildState> = Vec::new();
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
            // Adopt exactly the extra workers the predecessor had live (a busy box keeps
            // its scaled-up pooler across a zero-downtime upgrade). The scaler re-checks
            // load on its next tick and adjusts; we don't pre-spawn up to the cap.
            for &name in PGB_EXTRA_NAMES.iter() {
                if prior.get(name).is_some() {
                    pgb_extra.push(adopt_or_respawn_child(
                        name,
                        &prior,
                        &log_tx,
                        &mut persisted,
                    )?);
                }
            }

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
        //
        // Warm-start hint: children.json is durable across reboots (the rootfs is
        // GlideFS-backed), so it records how many extra pooler workers were live
        // before this restart. Capture that COUNT now — before our first persist
        // overwrites the file — so we can bring the pooler back up to its
        // pre-restart size instead of meeting the post-restart reconnect storm
        // (peak TLS-handshake churn) with a single worker and crawling back up
        // under the scaler's cooldowns. The pids in it are dead; only the count
        // matters, and the scaler corrects from there. Best-effort: a fresh box
        // has no prior file → 0. It's a hint, not source-of-truth — losing it just
        // falls back to starting at one worker.
        #[cfg(target_os = "linux")]
        let warm_start_extras =
            crate::children::PersistedChildren::load(&crate::children::state_dir())
                .map(|prior| {
                    PGB_EXTRA_NAMES
                        .iter()
                        .filter(|&&name| prior.get(name).is_some())
                        .count()
                })
                .unwrap_or(0);
        #[cfg(not(target_os = "linux"))]
        let warm_start_extras = 0usize;

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
        // Warm-start the extra so_reuseport workers to the pre-restart size (clamped
        // to the current max_workers in case the box was resized smaller). They share
        // :5432 with worker 0, so a reconnect storm is absorbed across cores from the
        // first second; the scaler reaps any excess within minutes if load doesn't
        // justify it. Without this, a busy box drops to ONE pooler at exactly the
        // moment of peak handshake churn and climbs back only under the up-cooldown.
        let want_extras = warm_start_extra_count(warm_start_extras, cfg.vcpus);
        for &name in PGB_EXTRA_NAMES.iter().take(want_extras) {
            let mut w = ChildState::new(name);
            match spawn_pgbouncer(&mut w, &log_tx) {
                Ok(()) => {
                    #[cfg(target_os = "linux")]
                    persist_child(&mut persisted, &w)?;
                    pgb_extra.push(w);
                }
                Err(e) => warn!("warm-start: failed to spawn pooler worker '{name}': {e}"),
            }
        }
        if want_extras > 0 {
            info!("warm-start: restored {want_extras} extra pooler worker(s) to pre-restart size");
        }
        // The PgbScaler grows/shrinks pgb_extra reactively from here (so_reuseport).

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
    // Pooler scaler telemetry handle — the scaler (run loop) writes it each tick,
    // the RPC `pooler` command reads it. Created cross-platform so the scaler can
    // populate it even where the (Linux-only) RPC server is a no-op.
    let pooler_stats = crate::handoff_bridge::new_pooler_stats();
    #[cfg(target_os = "linux")]
    let rpc_state = {
        let mut s = crate::handoff_bridge::SharedState::new();
        s.pooler = pooler_stats.clone();
        s
    };
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

    // Reactive PgBouncer scaler: sample pooler CPU every tick and grow/shrink the
    // so_reuseport worker set within [1, max_workers]. On boxes where max_workers
    // == 1 it only ever publishes telemetry (the justification signal for raising
    // the cap) and never acts.
    let mut pgb_scaler = PgbScaler::new(crate::config::pgbouncer_max_workers(cfg.vcpus));
    let mut scaler_tick = tokio::time::interval(SCALER_TICK);
    scaler_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

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
                drain_children(&mut pg_state, &mut pgb_state, &mut pgb_extra, cdc_state.as_mut()).await;
                shutdown_background_tasks(log_writer_handle, rpc_handle, memory_watcher_handle, cert_watcher_handle).await;
                return Ok(());
            }
            _ = sigint.recv() => {
                info!("received SIGINT, draining children");
                abort_wal_forwarder(&mut wal_forwarder_handle).await;
                drain_children(&mut pg_state, &mut pgb_state, &mut pgb_extra, cdc_state.as_mut()).await;
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
            // Extra so_reuseport pgbouncer workers (disabled when there are none).
            (res, idx) = wait_any_child(&mut pgb_extra), if !pgb_extra.is_empty() => {
                let _ = res?; // propagate I/O errors; exit code irrelevant for extras
                // Extra pooler workers are never individually restarted — the scaler
                // owns the live count. Whether this was a crash or a scale-down reap,
                // drop it from the set; the scaler re-adds on its next tick if load
                // still demands it (the kernel rebalances connections meanwhile).
                let gone = pgb_extra.remove(idx);
                info!("pooler worker '{}' exited; removed (scaler owns count)", gone.name);
                #[cfg(target_os = "linux")]
                {
                    persisted.remove(gone.name);
                    persisted.save(&crate::children::state_dir())?;
                }
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
                for w in &pgb_extra {
                    sighup_pgbouncer(w);
                }
            }
            // Reactive pooler scaling: sample CPU, grow/shrink so_reuseport workers.
            _ = scaler_tick.tick() => {
                let now = Instant::now();
                let mut pids: Vec<u32> = Vec::with_capacity(1 + pgb_extra.len());
                if let Some(p) = pgb_state.pid() {
                    pids.push(p);
                }
                for w in &pgb_extra {
                    if let Some(p) = w.pid() {
                        pids.push(p);
                    }
                }
                let action = pgb_scaler.tick(&pids, now, &pooler_stats);
                let outcome = apply_scale_action(
                    action,
                    &mut pgb_extra,
                    // spawn: build + start a real worker; None on failure (logged here).
                    |name| {
                        let mut w = ChildState::new(name);
                        match spawn_pgbouncer(&mut w, &log_tx) {
                            Ok(()) => Some(w),
                            Err(e) => {
                                warn!("pooler scaler: failed to spawn '{name}': {e}");
                                None
                            }
                        }
                    },
                    // reap: SIGINT for graceful drain; the wait_any_child arm
                    // removes it from pgb_extra + persisted once it exits.
                    sigint_pgbouncer,
                );
                match outcome {
                    ScaleOutcome::SpawnedUp(name) => {
                        let live = 1 + pgb_extra.len();
                        info!(
                            "pooler scaler: UP to {live} workers (per-worker CPU saturated; spawned '{name}')"
                        );
                        #[cfg(target_os = "linux")]
                        if let Some(w) = pgb_extra.last()
                            && let Err(e) = persist_child(&mut persisted, w)
                        {
                            warn!("pooler scaler: persist of '{name}' failed: {e}");
                        }
                    }
                    ScaleOutcome::ReapedDown(name) => {
                        // worker still draining (not yet reaped from the vec).
                        let live = 1 + pgb_extra.len();
                        info!(
                            "pooler scaler: DOWN from {live} workers (SIGINT graceful drain of '{name}')"
                        );
                    }
                    ScaleOutcome::NoOp => {}
                }
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

/// Send SIGINT to a PgBouncer worker for a *graceful* shutdown ("safe shutdown":
/// finish in-flight transactions, then exit). Used by the scaler to reap an extra
/// so_reuseport worker — measured ~0.03% reconnect blip as the kernel re-routes
/// its clients to the survivors. Best-effort: a missing pid means it's already gone.
fn sigint_pgbouncer(state: &ChildState) {
    let Some(pid) = state.pid() else {
        info!("pgbouncer reap: no live child, SIGINT skipped");
        return;
    };
    #[cfg(target_os = "linux")]
    {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;
        match kill(Pid::from_raw(pid as i32), Signal::SIGINT) {
            Ok(()) => info!("pgbouncer reap: SIGINT sent (pid={pid})"),
            Err(e) => warn!("pgbouncer reap: SIGINT failed (pid={pid}): {e}"),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        info!("pgbouncer reap: SIGINT skipped (non-Linux build)");
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
    pgb_extra: &mut [ChildState],
    mut cdc: Option<&mut ChildState>,
) {
    // Send SIGTERM to all live children (not kill() which is SIGKILL)
    sigterm_child(pg);
    sigterm_child(pgb);
    for w in pgb_extra.iter_mut() {
        sigterm_child(w);
    }
    if let Some(c) = cdc.as_deref_mut() {
        sigterm_child(c);
    }

    let mut all: Vec<&mut ChildState> = vec![pg, pgb];
    for w in pgb_extra.iter_mut() {
        all.push(w);
    }
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

    // Required extensions — fail the supervisor if CREATE EXTENSION fails for
    // an extension whose shared object IS installed; the process manager will
    // restart and retry rather than running in a degraded state.
    //
    // But the standalone postgres primitive ships without the auth/queue
    // milestone (beyond_auth/beyond_queue) and pgdg's pg_cron (version pin
    // drift). When an extension's .so isn't installed, treat it as a warning,
    // not a fatal — the same self-adapting posture as the
    // shared_preload_libraries filter (see config::beyond_conf). With the
    // extensions present, the behavior is unchanged (still hard-required).
    for ext in REQUIRED_EXTENSIONS {
        if !extension_installed(ext) {
            warn!(
                "required extension {ext} not installed (no {ext}.so in {EXTENSION_PKGLIBDIR}); \
                 skipping (auth/queue milestone not in this image)"
            );
            continue;
        }
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

    // USAGE on the schema is REQUIRED for the pgbouncer role to call get_auth at
    // all — without it auth_query fails with "permission denied for schema
    // pgbouncer" for every client, and no one can connect through the pooler.
    // (EXECUTE on the function is not enough; the caller also needs schema USAGE.)
    pg::psql("GRANT USAGE ON SCHEMA pgbouncer TO pgbouncer").await?;
    pg::psql("GRANT EXECUTE ON FUNCTION pgbouncer.get_auth(text) TO pgbouncer").await?;

    Ok(())
}

const REQUIRED_EXTENSIONS: &[&str] = &["beyond_auth", "beyond_queue", "pg_cron"];

/// Directory holding PostgreSQL extension shared objects (PG18 Debian layout,
/// `pg_config --pkglibdir`). Mirrors `config`'s PKGLIBDIR.
const EXTENSION_PKGLIBDIR: &str = "/usr/lib/postgresql/18/lib";

/// True iff the extension's shared object is present in the image. An extension
/// listed in [`REQUIRED_EXTENSIONS`] but with no installed `.so` (e.g. the
/// future auth/queue milestone, or a dropped pgdg `pg_cron`) is downgraded from
/// fatal to a warning so the standalone primitive still boots.
fn extension_installed(ext: &str) -> bool {
    std::path::Path::new(EXTENSION_PKGLIBDIR)
        .join(format!("{ext}.so"))
        .exists()
}

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

    // --- PgBouncer scaler decision (pure) ---

    const COOL: Duration = Duration::from_secs(3600); // past every cooldown
    const HOT: f64 = 0.9; // per-worker cores, above SCALE_UP_CORES
    const COLD_AGG: f64 = 0.4; // aggregate cores: removing a worker stays < 0.5

    #[test]
    fn scaler_holds_until_up_streak_met() {
        // One hot tick is not enough (needs SCALE_UP_TICKS = 2).
        let (a, up, _down) = decide_scale(HOT, HOT, 1, 4, 0, 0, COOL);
        assert_eq!(a, ScaleAction::Hold);
        assert_eq!(up, 1);
        // Second consecutive hot tick → Up, streak reset.
        let (a, up, _down) = decide_scale(HOT, HOT, 1, 4, up, 0, COOL);
        assert_eq!(a, ScaleAction::Up);
        assert_eq!(up, 0);
    }

    #[test]
    fn scaler_never_scales_past_ceiling() {
        // live == max: stay put no matter how hot or how long.
        let (a, _u, _d) = decide_scale(HOT, HOT * 4.0, 4, 4, 99, 0, COOL);
        assert_eq!(a, ScaleAction::Hold);
    }

    #[test]
    fn scaler_respects_up_cooldown() {
        // Hot streak met but action happened 1s ago → Hold.
        let (a, _u, _d) = decide_scale(HOT, HOT, 1, 4, 99, 0, Duration::from_secs(1));
        assert_eq!(a, ScaleAction::Hold);
    }

    #[test]
    fn scaler_scales_down_when_cold_and_streak_met() {
        // 2 live workers, aggregate so low that one worker would suffice.
        let (a, _u, down) = decide_scale(COLD_AGG / 2.0, COLD_AGG, 2, 4, 0, 0, COOL);
        assert_eq!(a, ScaleAction::Hold); // first cold tick
        assert_eq!(down, 1);
        // After SCALE_DOWN_TICKS consecutive cold ticks → Down.
        let (a, _u, _d) = decide_scale(
            COLD_AGG / 2.0,
            COLD_AGG,
            2,
            4,
            0,
            SCALE_DOWN_TICKS - 1,
            COOL,
        );
        assert_eq!(a, ScaleAction::Down);
    }

    #[test]
    fn scaler_never_scales_below_one() {
        // Single worker, ice cold, long streak → still Hold (never reap worker 0).
        let (a, _u, _d) = decide_scale(0.0, 0.0, 1, 4, 0, 99, COOL);
        assert_eq!(a, ScaleAction::Hold);
    }

    #[test]
    fn scaler_down_streak_resets_when_warm() {
        // 2 workers but each near-busy: removing one would overload → not cold.
        let (a, _u, down) = decide_scale(0.7, 1.4, 2, 4, 0, 5, COOL);
        assert_eq!(a, ScaleAction::Hold);
        assert_eq!(down, 0, "warm tick resets the down streak");
    }

    // --- PgbScaler::tick orchestration (sampling + decide + telemetry),
    //     deterministic via an injected CPU sampler + manual clock ---

    fn test_scaler(max: usize) -> PgbScaler {
        PgbScaler {
            clk_tck: 100.0, // fixed USER_HZ so the cores math is exact in tests
            max_workers: max,
            prev_ticks: std::collections::HashMap::new(),
            prev_at: None,
            up_streak: 0,
            down_streak: 0,
            last_action_at: None,
        }
    }

    #[test]
    fn tick_baseline_then_scales_up_and_publishes_telemetry() {
        let stats = crate::handoff_bridge::new_pooler_stats();
        let mut s = test_scaler(4);
        let cpu = std::cell::Cell::new(0u64); // cumulative ticks the sampler reports
        let sample = |_pid: u32| cpu.get();
        let t0 = Instant::now();
        let pids = [100u32];

        // 1st tick: no prior sample → baseline only, Hold.
        assert_eq!(s.tick_with(&pids, t0, &stats, sample), ScaleAction::Hold);
        // +1s, +90 ticks ⇒ 0.90 cores (1 worker). Hot, but needs 2 ticks.
        cpu.set(90);
        assert_eq!(
            s.tick_with(&pids, t0 + Duration::from_secs(1), &stats, sample),
            ScaleAction::Hold
        );
        // +1s, +90 ticks ⇒ 2nd hot tick ⇒ Up (no prior action → cooldown satisfied).
        cpu.set(180);
        assert_eq!(
            s.tick_with(&pids, t0 + Duration::from_secs(2), &stats, sample),
            ScaleAction::Up
        );

        let snap = stats.lock().unwrap().clone();
        assert_eq!(snap.live_workers, 1);
        assert_eq!(snap.max_workers, 4);
        assert!(
            (snap.per_worker_cores - 0.90).abs() < 0.01,
            "per_worker_cores={}",
            snap.per_worker_cores
        );
        assert_eq!(snap.last_action, "up");
        assert!(!snap.at_ceiling, "1 of 4 workers is not at the ceiling");
    }

    #[test]
    fn tick_scales_down_when_cold() {
        let stats = crate::handoff_bridge::new_pooler_stats();
        let mut s = test_scaler(4);
        s.down_streak = SCALE_DOWN_TICKS - 1; // one cold tick away from a reap
        let cpu = std::cell::Cell::new(0u64);
        let sample = |_pid: u32| cpu.get(); // same cumulative for both pids
        let t0 = Instant::now();
        let pids = [200u32, 201u32];

        // baseline
        assert_eq!(s.tick_with(&pids, t0, &stats, sample), ScaleAction::Hold);
        // +1s, each pid +5 ticks ⇒ aggregate 10 ticks = 0.10 cores over 2 workers.
        // Removing one leaves 0.10 < 0.5 ⇒ cold ⇒ down_streak hits the threshold.
        cpu.set(5);
        assert_eq!(
            s.tick_with(&pids, t0 + Duration::from_secs(1), &stats, sample),
            ScaleAction::Down
        );
        assert_eq!(stats.lock().unwrap().last_action, "down");
    }

    #[test]
    fn tick_at_ceiling_flag_set_when_max_and_saturated() {
        let stats = crate::handoff_bridge::new_pooler_stats();
        let mut s = test_scaler(2); // max == live below
        let cpu = std::cell::Cell::new(0u64);
        let sample = |_pid: u32| cpu.get();
        let t0 = Instant::now();
        let pids = [10u32, 11u32]; // 2 live == max 2

        assert_eq!(s.tick_with(&pids, t0, &stats, sample), ScaleAction::Hold);
        // each pid +90 ⇒ aggregate 180 ticks = 1.8 cores / 2 = 0.9 per worker (> 0.75).
        cpu.set(90);
        // Hot, but live == max ⇒ never Up; at_ceiling must flag the box wants more.
        assert_eq!(
            s.tick_with(&pids, t0 + Duration::from_secs(1), &stats, sample),
            ScaleAction::Hold
        );
        let snap = stats.lock().unwrap().clone();
        assert!(
            snap.at_ceiling,
            "live==max && saturated must set at_ceiling"
        );
    }

    #[test]
    fn tick_worker_churn_does_not_spike_aggregate() {
        // A pid appearing mid-window (no prior sample) contributes 0 this tick —
        // its large absolute counter must NOT be read as a delta. This is the
        // per-pid-keyed map's whole reason to exist.
        let stats = crate::handoff_bridge::new_pooler_stats();
        let mut s = test_scaler(4);
        let t0 = Instant::now();

        // baseline: only pid 100 (cumulative 1000).
        assert_eq!(
            s.tick_with(&[100u32], t0, &stats, |_| 1000),
            ScaleAction::Hold
        );
        // +1s: pid 100 advances 50 ticks; brand-new pid 200 shows up with a huge
        // absolute counter. Only pid 100's 50-tick delta counts ⇒ 0.5 cores agg.
        let act = s.tick_with(
            &[100u32, 200u32],
            t0 + Duration::from_secs(1),
            &stats,
            |pid| {
                if pid == 100 { 1050 } else { 9_999_999 }
            },
        );
        assert_eq!(act, ScaleAction::Hold);
        let snap = stats.lock().unwrap().clone();
        assert!(
            (snap.aggregate_cpu_cores - 0.50).abs() < 0.01,
            "new worker must not spike aggregate: agg={}",
            snap.aggregate_cpu_cores
        );
    }

    // --- apply_scale_action: spawn/reap mutation with injected process I/O ---

    #[test]
    fn warm_start_clamps_to_current_max_workers() {
        // Same-size box: restore exactly what was live (max_workers(64)=8 → cap 7).
        assert_eq!(warm_start_extra_count(4, 64), 4);
        // Downsized during restart (now 8 vCPU → max_workers 2 → ≤1 extra): clamp down.
        assert_eq!(warm_start_extra_count(7, 8), 1);
        // Tiny box (max_workers 1 → 0 extras): never warm-start past a single pooler.
        assert_eq!(warm_start_extra_count(3, 2), 0);
        // Fresh box / nothing prior: start at one worker.
        assert_eq!(warm_start_extra_count(0, 64), 0);
    }

    #[test]
    fn apply_up_spawns_workers_in_name_order() {
        let mut extra: Vec<ChildState> = Vec::new();
        let out = apply_scale_action(
            ScaleAction::Up,
            &mut extra,
            |name| Some(ChildState::new(name)),
            |_w| panic!("reap must not run on Up"),
        );
        assert_eq!(out, ScaleOutcome::SpawnedUp("pgbouncer-1"));
        assert_eq!(extra.len(), 1);
        assert_eq!(extra[0].name, "pgbouncer-1");

        let out = apply_scale_action(
            ScaleAction::Up,
            &mut extra,
            |name| Some(ChildState::new(name)),
            |_w| {},
        );
        assert_eq!(out, ScaleOutcome::SpawnedUp("pgbouncer-2"));
        assert_eq!(extra.len(), 2);
    }

    #[test]
    fn apply_down_reaps_last_but_keeps_it_pending() {
        let mut extra = vec![
            ChildState::new("pgbouncer-1"),
            ChildState::new("pgbouncer-2"),
        ];
        let reaped = std::cell::Cell::new("");
        let out = apply_scale_action(
            ScaleAction::Down,
            &mut extra,
            |_n| panic!("spawn must not run on Down"),
            |w| reaped.set(w.name),
        );
        assert_eq!(out, ScaleOutcome::ReapedDown("pgbouncer-2"));
        assert_eq!(reaped.get(), "pgbouncer-2");
        // Down does NOT remove from the vec — wait_any_child does, on actual exit.
        assert_eq!(extra.len(), 2);
    }

    #[test]
    fn apply_up_noops_at_name_cap() {
        let mut extra: Vec<ChildState> = PGB_EXTRA_NAMES
            .iter()
            .map(|&n| ChildState::new(n))
            .collect();
        assert_eq!(extra.len(), PGB_EXTRA_NAMES.len());
        let out = apply_scale_action(
            ScaleAction::Up,
            &mut extra,
            |_n| panic!("must not spawn past the name cap"),
            |_w| {},
        );
        assert_eq!(out, ScaleOutcome::NoOp);
        assert_eq!(extra.len(), PGB_EXTRA_NAMES.len());
    }

    /// Real-process integration: the production `tick()` (real `/proc` sampler,
    /// real clock) must turn a genuinely CPU-bound worker into a real `Up`
    /// decision. Covers the wiring the injected-sampler tests stub out. Ignored
    /// by default because it spawns a process and burns ~1.5s of CPU; run with
    /// `cargo test -p beyond-pg --bins -- --ignored`.
    #[test]
    #[ignore = "spawns a real CPU-burning process; run with --ignored"]
    fn scaler_tick_detects_real_cpu_load_and_decides_up() {
        use std::process::{Command, Stdio};

        // Cleanup guard: SIGKILL the burner even if an assertion panics.
        struct Kill(u32);
        impl Drop for Kill {
            fn drop(&mut self) {
                let _ = Command::new("kill")
                    .arg("-9")
                    .arg(self.0.to_string())
                    .status();
            }
        }

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("while :; do :; done")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn CPU burner");
        let pid = child.id();
        let _guard = Kill(pid);

        let stats = crate::handoff_bridge::new_pooler_stats();
        let mut s = PgbScaler::new(4); // real clk_tck + real /proc sampler via tick()
        let pids = [pid];

        s.tick(&pids, Instant::now(), &stats); // baseline (no prior sample)
        let mut scaled_up = false;
        let mut max_cores = 0.0f64;
        for _ in 0..4 {
            std::thread::sleep(Duration::from_millis(600));
            let action = s.tick(&pids, Instant::now(), &stats);
            max_cores = max_cores.max(stats.lock().unwrap().per_worker_cores);
            if action == ScaleAction::Up {
                scaled_up = true;
                break;
            }
        }
        let _ = child.kill();
        let _ = child.wait();

        assert!(
            max_cores > 0.7,
            "real /proc sampling should read ~1 core for a busy loop, saw {max_cores}"
        );
        assert!(
            scaled_up,
            "scaler must decide Up under sustained real CPU load (peak per-worker {max_cores} cores)"
        );
    }

    #[test]
    fn apply_spawn_failure_and_empty_down_are_noops() {
        let mut extra: Vec<ChildState> = Vec::new();
        // spawn returns None (e.g. fork failed) → no growth.
        assert_eq!(
            apply_scale_action(ScaleAction::Up, &mut extra, |_n| None, |_w| {}),
            ScaleOutcome::NoOp
        );
        assert!(extra.is_empty());
        // Down with no extras → nothing to reap.
        assert_eq!(
            apply_scale_action(ScaleAction::Down, &mut extra, |_n| None, |_w| {}),
            ScaleOutcome::NoOp
        );
    }

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
