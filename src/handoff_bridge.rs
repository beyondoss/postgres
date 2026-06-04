//! `handoff::Drainable` bridge for the `beyond-pg` supervisor.
//!
//! beyond-pg holds no on-disk state of its own — postgres is the source of
//! truth — so `seal()` is a no-op. The interesting work is `drain()`: stop
//! accepting new control RPCs and wait for in-flight handlers to finish
//! (e.g. `promote` can block up to 55s).
//!
//! Topology mirrors `auth/src/handoff_bridge.rs`: the bridge runs on the
//! dedicated handoff control thread (where `handoff::Incumbent::serve`
//! blocks). The accept loop runs on the tokio runtime. They communicate
//! through two atomics:
//!
//! - `accept_paused`: set by `drain()`, cleared by `resume_after_abort()`.
//!   The accept loop checks before each accept and parks if true.
//! - `in_flight`: incremented per connection, decremented on completion.
//!   `drain()` polls until zero (or the deadline).
//!
//! Auth also has a `DrainSignal` broadcast to per-connection tasks so they
//! call hyper's `graceful_shutdown`. Our handlers don't have a keep-alive
//! shape — they complete naturally — so we omit that piece. If we ever
//! grow a long-lived bidi-stream RPC, add it back the way auth did:
//! atomic flag + Notify with double-check (see `auth/src/handoff_bridge.rs`
//! `DrainSignal` for the pattern; Notify alone is wrong because late
//! waiters miss the broadcast).
//!
//! Linux-only (handoff crate). Module-level gating done at the
//! `mod handoff_bridge;` declaration sites in `main.rs` and `lib.rs`.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use handoff::{DrainReport, Drainable, SealReport, StateSnapshot};

/// Snapshot of the PgBouncer pooler tier, published by the supervisor's scaler
/// every tick and served over the `pooler` RPC command. This is the production
/// signal for "does a real box ever saturate a pooler": `at_ceiling` sustained
/// means even the maxed-out worker set is CPU-bound; `live_workers` stuck at 1
/// forever means the scaler was never needed.
///
/// Lives here (rather than in `rpc`) because `rpc`/`supervisor` are binary-only
/// modules while `handoff_bridge` is in the library — `SharedState` carries the
/// handle, so the type must be library-visible.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct PoolerStats {
    /// Currently-running pooler processes (worker 0 + scaled-up extras).
    pub live_workers: u32,
    /// Ceiling the scaler may grow to (`config::pgbouncer_max_workers`).
    pub max_workers: u32,
    /// Total pooler CPU across all live workers, in cores, over the last tick.
    pub aggregate_cpu_cores: f64,
    /// `aggregate_cpu_cores / live_workers` — the per-worker saturation signal.
    pub per_worker_cores: f64,
    /// `live == max` AND per-worker near-saturated: the box wants more pooler
    /// capacity than the cap allows.
    pub at_ceiling: bool,
    /// Last scaling action the scaler took: "up", "down", or "" (none yet).
    pub last_action: String,
}

/// Shared handle: scaler writes, RPC reads. Lock is held only for a struct copy.
pub type PoolerStatsHandle = Arc<Mutex<PoolerStats>>;

/// Construct a fresh, zeroed pooler-stats handle.
pub fn new_pooler_stats() -> PoolerStatsHandle {
    Arc::new(Mutex::new(PoolerStats::default()))
}

#[derive(Clone, Default)]
pub struct SharedState {
    pub accept_paused: Arc<AtomicBool>,
    pub in_flight: Arc<AtomicUsize>,
    /// PgBouncer scaler telemetry, published by the supervisor and served over
    /// the `pooler` RPC command. Shares the handle so the RPC handler reads what
    /// the scaler last wrote. `Default` = a zeroed handle (Arc<Mutex<…>>).
    pub pooler: PoolerStatsHandle,
}

impl SharedState {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct SupervisorDrainable {
    state: SharedState,
}

impl SupervisorDrainable {
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }
}

impl Drainable for SupervisorDrainable {
    fn drain(&self, deadline: Instant) -> handoff::Result<DrainReport> {
        self.state.accept_paused.store(true, Ordering::SeqCst);

        // Acquire pairs with the SeqCst fetch_sub on the handler side
        // (rpc.rs). With Relaxed, the drain thread could observe a stale
        // non-zero value forever — or, worse, observe zero before the
        // handler's writes to other state are globally visible.
        while self.state.in_flight.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(25));
        }

        let remaining_usize = self.state.in_flight.load(Ordering::Acquire);
        debug_assert!(remaining_usize <= u32::MAX as usize);
        let remaining = remaining_usize as u32;
        tracing::info!(
            open_conns_remaining = remaining,
            "supervisor drain complete"
        );
        Ok(DrainReport {
            open_conns_remaining: remaining,
            accept_closed: true,
        })
    }

    fn seal(&self) -> handoff::Result<SealReport> {
        // No on-disk supervisor state: postgres holds the durable record;
        // children.json was flushed on every spawn/exit during normal
        // operation. If `seal` ever grows real work (e.g. flushing an
        // embedded cache), expand the handoff integration tests at the
        // same time — the no-state assumption is load-bearing.
        Ok(SealReport::default())
    }

    fn resume_after_abort(&self) -> handoff::Result<()> {
        self.state.accept_paused.store(false, Ordering::SeqCst);
        tracing::info!("handoff aborted; supervisor accept loop resumed");
        Ok(())
    }

    fn snapshot_state(&self) -> StateSnapshot {
        let open = self.state.in_flight.load(Ordering::Acquire);
        debug_assert!(open <= u32::MAX as usize);
        StateSnapshot {
            shard_count: 0,
            open_conns: open as u32,
            last_revision_per_shard: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_returns_when_in_flight_is_zero() {
        let s = SharedState::new();
        let d = SupervisorDrainable::new(s.clone());
        let r = d.drain(Instant::now() + Duration::from_secs(1)).unwrap();
        assert_eq!(r.open_conns_remaining, 0);
        assert!(r.accept_closed);
        assert!(s.accept_paused.load(Ordering::SeqCst));
    }

    #[test]
    fn drain_waits_for_in_flight_to_drop() {
        let s = SharedState::new();
        s.in_flight.store(2, Ordering::SeqCst);
        let d = SupervisorDrainable::new(s.clone());

        // Spawn a thread that decrements after ~50ms.
        let s_clone = s.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            s_clone.in_flight.store(0, Ordering::SeqCst);
        });

        let started = Instant::now();
        let r = d.drain(Instant::now() + Duration::from_secs(2)).unwrap();
        let elapsed = started.elapsed();
        assert_eq!(r.open_conns_remaining, 0);
        assert!(elapsed >= Duration::from_millis(40), "elapsed: {elapsed:?}");
        assert!(elapsed < Duration::from_millis(200), "elapsed: {elapsed:?}");
    }

    #[test]
    fn drain_times_out_when_in_flight_persists() {
        let s = SharedState::new();
        s.in_flight.store(3, Ordering::SeqCst);
        let d = SupervisorDrainable::new(s.clone());
        let r = d.drain(Instant::now() + Duration::from_millis(50)).unwrap();
        assert_eq!(r.open_conns_remaining, 3);
        assert!(r.accept_closed);
    }

    #[test]
    fn resume_after_abort_clears_pause() {
        let s = SharedState::new();
        s.accept_paused.store(true, Ordering::SeqCst);
        let d = SupervisorDrainable::new(s.clone());
        d.resume_after_abort().unwrap();
        assert!(!s.accept_paused.load(Ordering::SeqCst));
    }
}
