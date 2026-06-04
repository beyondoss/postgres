# Substrate-tuning results (measured)

Environment: **real Postgres 18.4 (PGDG, lz4+zstd) on a real GlideFS NBD device, inside an
isolated QEMU/KVM VM** (Ubuntu 24.04, 6 vCPU / 8 GB). Object store: `memory://`. Zero contact with
the production homelab glidefs. Harness: `bench/glidefs-pg/run.sh`; signal = GlideFS flush log
(`bytes_uploaded`/`packs_uploaded` per flush, summed over the run window after a forced `drain`).

## Method note (important)

- **Work-normalized runs only.** Time-based (`-T`) runs confound throughput with efficiency (a faster
  config does more transactions → more bytes). All results below use **fixed transactions**
  (`pgbench -t`, 160k txns, scale 50) so S3-byte deltas reflect _efficiency at equal work_.
- **Trust absolute work-normalized S3 bytes, not write-amp.** write-amp = S3 bytes / guest bytes; a
  knob that changes guest bytes written (e.g. `wal_init_zero`) moves the denominator and makes the
  ratio misleading. The honest metric is S3 write bytes for the same work.

## Round 1 — knobs measurable in a 60–120 s window (160k txns, scale 50)

| config                                     |                                S3 write MB |     Δ vs base | packs |         tps | call                            |
| ------------------------------------------ | -----------------------------------------: | ------------: | ----: | ----------: | ------------------------------- |
| baseline ×3                                | 165.8 / 167.7 / 167.6 (μ **167.0**, ±0.7%) |             — | 62–67 |   2523–2645 | reference                       |
| **D1 `wal_compression=zstd`**              |                                  **187.9** |    **+12.5%** |    57 |        2509 | **NO-SHIP**                     |
| **A1 `wal_recycle=off,wal_init_zero=off`** |                                  **181.9** |     **+8.9%** |    89 | 2207 (−15%) | **NO-SHIP**                     |
| C2 `*_flush_after=0`                       |                                      165.9 | −0.7% (noise) |    72 |        2650 | neutral (no S3 win)             |
| G `compaction_cooldown=8`                  |                                      167.9 | +0.6% (noise) |    63 |        2567 | inconclusive — needs churn test |

Noise floor: S3 bytes **±0.7%**, write-amp **±0.001**. So +12.5% and +8.9% are real (≫ noise).

### Findings

- **zstd is worse, not better (−ship).** GlideFS already LZ4-compresses every block at flush.
  Pre-compressing WAL with zstd produces high-entropy blocks GlideFS can't compress further, so it
  stores _more_ bytes (+12.5% at equal work; write-amp 0.142 vs 0.122). The substrate already owns
  compression — don't double it. Keep `wal_compression = lz4` (cheap, and its output still has
  GlideFS headroom) — or even test `off` later.
- **`wal_recycle/wal_init_zero=off` is mildly harmful (−ship).** +8.9% S3 bytes **and** −15% tps.
  The ZFS playbook does not transfer: recycling reuses segment offsets (blocks GlideFS can coalesce);
  `recycle=off` allocates fresh segment files → more distinct blocks + ext4 inode/bitmap churn. This
  **confirms and strengthens plan §A** (was "≈ noise"; measurement says "slightly worse"). Leave PG
  defaults (both `on`).
- **`*_flush_after=0`: no S3 benefit** at this workload (the flush cadence here is driven by the
  500-dirty-block threshold, not OS writeback timing). Park it; revisit only as a latency knob.
- **`compaction_cooldown=8`: inconclusive** in a short non-churning run — compaction barely fires.
  Needs the dedicated overwrite-churn test to confirm/deny the −22% PUT claim from the volume study.

## Still to measure (need tailored designs, not a 60 s OLTP run)

| knob                             | why round-1 can't see it                  | design                                                                |
| -------------------------------- | ----------------------------------------- | --------------------------------------------------------------------- |
| C1 `checkpoint_timeout` 15/30/60 | no time-based checkpoint fires in <15 min | forced checkpoints over fixed-txn; vary cadence; measure FPW→S3 bytes |
| C3 bgwriter aggressiveness       | masked by cache at this scale             | larger-than-cache working set                                         |
| D2 `maintenance_io_concurrency`  | OLTP doesn't exercise cold reads          | VACUUM/index-build on a cold fork; measure wall-time + cache misses   |
| E autovacuum-on-CoW              | needs an idle fork                        | populate → fork → idle; measure fork's divergence bytes               |
| G `compaction_cooldown`          | needs compaction to fire                  | sustained overwrite churn over time                                   |
| F PgBouncer multi-worker         | not a GlideFS metric                      | `pgbench -c200` connection-scaling, 1 vs N workers                    |

## Round 2 — checkpoint cadence (C1) — the headline win

Fixed 300k txns, scale 50, only `checkpoint_timeout` varied (wal-triggered checkpoints
disabled via `max_wal_size=32GB`). Compressed cadence tests the same FPW-frequency mechanism
as the production 15→30→60 min change.

| checkpoint_timeout | S3 write MB | guest WAL MB | packs | write-amp |  tps |   vs rare |
| ------------------ | ----------: | -----------: | ----: | --------: | ---: | --------: |
| 45 s (frequent)    |     **635** |         2929 |   310 |     0.217 | 1605 |     +194% |
| 90 s               |         294 |         2138 |   123 |     0.137 | 2105 |      +36% |
| 300 s (rare)       |     **216** |         1839 |    65 |     0.117 | 2062 | reference |

**Frequent checkpoints write 2.9× more to S3 at identical work (635 vs 216 MB) and run
slower (1605 vs 2062 tps).** Each checkpoint forces a full-page image on the first touch of
every page → more checkpoints → more WAL → more S3 bytes (and more WAL-sink bandwidth).
**SHIP: longer `checkpoint_timeout`.** Applied 15 min → 30 min in `00-beyond.conf` (halves the
checkpoint rate; recovery stays bounded by `max_wal_size=8GB`). 60 min is a further option;
returns diminish past ~2× per the 90→300 s segment.

## Round 3 — the remaining knobs (re-run with guard-rails)

### C3 bgwriter — DISPROVEN

Corrected design (hot set FITS in shared_buffers: scale 20, sb 1 GB, 200k txns), vary bgwriter:

| bgwriter                  | S3 write MB |  tps |
| ------------------------- | ----------: | ---: |
| aggressive (50 ms / 1000) |       113.6 | 2162 |
| gentle (500 ms / 100)     |       113.5 | 1956 |
| off (maxpages 0)          |       113.5 | 1811 |

S3 bytes **identical** (0.1%). When the hot set fits in shared_buffers, coalescing happens
regardless of bgwriter timing — it's not an S3 lever. **No-ship; leave bgwriter as-is.** (My first
C3 design — working set ≫ shared_buffers on `memory://` — was both wrong for the hypothesis and
pathological: eviction churn floods the in-RAM store until the guest thrashes. Don't do that.)

### G `compaction_cooldown` — DISPROVEN (under OLTP)

Overwrite-churn (scale 10, 400k txns), cooldown 0 vs 8:

| cooldown | S3 write MB | packs |
| -------- | ----------: | ----: |
| 0        |       140.5 |    42 |
| 8        |       140.9 |    41 |

Identical. The volume-study's −22% **does not reproduce** under live pgbench — compaction barely
fires at this scale, so cooldown has nothing to defer. No measurable S3 benefit for this workload;
the −22% appears specific to the trace-replay methodology, not OLTP.

### E autovacuum-on-CoW divergence — VALIDATED (big)

Measured directly (no fork API): build 85k dead tuples with autovacuum OFF, drain, then turn
autovacuum ON under a profile and idle 90 s; S3 bytes flushed = the divergence an idle fork incurs.

| autovacuum profile                                              |  S3 written in 90 s idle | dead tuples after  |
| --------------------------------------------------------------- | -----------------------: | ------------------ |
| **aggressive** (scale_factor 0.01)                              | **305.7 MB** (236 packs) | 0 (fully vacuumed) |
| **throttled** (scale_factor 0.4, naptime 5min, cost_delay 20ms) |     **0.1 MB** (6 packs) | 85k (deferred)     |

**~2240×.** An idle fork under aggressive autovacuum diverges **306 MB in 90 s** — the "idle fork
diverges" cost made concrete. **SHIP: throttle autovacuum on ephemeral/fork volumes** (they're
short-lived; the bloat never matters, but every vacuumed page is a new uploaded block). On a
long-lived durable primary, keep autovacuum aggressive (bloat control wins there).

### F PgBouncer multi-worker (so_reuseport) — CONFIRMED: scales (12-vCPU VM)

The 6-vCPU INCONCLUSIVE result was a **self-imposed wall** — at 6 vCPU the VM/Postgres saturates
before a single pooler does, so workers can't show. Rebuilt the VM at **12 vCPU** and drove a
**pooler-bound** load (Ed25519 TLS termination + connection churn → ~314 µs/handshake, PG idle), with
N poolers pinned to N cores and N async TLS-churn drivers on the next N cores, PG on cores 8–11.
`exp-multiworker.sh` (driver-limited sweep) and the saturated follow-up (3 drivers/pooler):

| N poolers | total tps | pooler CPU (aggregate) | note                              |
| --------: | --------: | ---------------------: | --------------------------------- |
|         1 |       718 |             0.35 cores | 1 driver/pooler (driver-limited)  |
|         2 |      1272 |             0.67 cores | "                                 |
|         3 |      1755 |             1.01 cores | "                                 |
|         1 |      1877 |             0.79 cores | **3 drivers/pooler (saturating)** |
|         2 |      2853 |             1.59 cores | **3 drivers/pooler**              |

**Pooler CPU scales perfectly linearly** (0.35→0.67→1.01 ≈ 0.34/worker; 0.79→1.59 = exactly 2.0×) —
`so_reuseport` genuinely distributes new connections across workers, each doing independent work on
its own core. Throughput scales 1.5–1.8×/worker; sub-2× only because at full 12-core utilization the
_drivers_ and PG contend, not the poolers. **A single pooler caps at ~2.4k TLS-churn conns/s/core
(≈1877/0.79); each added worker linearly adds that much.** Multi-worker is a **real, working lever**
for handshake/churn-bound pooler load — and it's exactly the regime Beyond's serverless/edge profile
lives in (every cold invocation = one fresh TLS handshake). **SHIP the mechanism; gate the count on a
real signal (pooler core-bound).** Scripts: `exp-multiworker.sh`, `exp-pooler-ceiling.sh`.

### F′ TLS session resumption — MEASURED DEAD as a pooler lever

The tempting alternative to workers: make each reconnect skip the handshake. Measured the raw ceiling
with `openssl s_time -new` vs `-reuse` against our exact certs, then tested reachability:

| cert    | TLS |   full |   resumed |  speedup | actually resumed? |
| ------- | --- | -----: | --------: | -------: | ----------------- |
| Ed25519 | 1.2 | 314 µs | **40 µs** | **7.9×** | Y (session-ID)    |
| Ed25519 | 1.3 | 336 µs |    343 µs |     1.0× | **N**             |
| RSA2048 | 1.2 | 283 µs |     39 µs |     7.2× | Y                 |
| RSA2048 | 1.3 | 382 µs |    411 µs |     0.9× | **N**             |

The 8× ceiling is real **but unreachable in our world**, for two independently-fatal reasons:

1. **TLS 1.3 (the secure default) keeps ECDHE on resume** (`psk_dhe_ke` for forward secrecy) → ~1×,
   no win. Only TLS 1.2 session-ID resumption (no PFS) or 1.3 `psk_ke` (no PFS) reaches the 8× — a
   security regression we won't ship.
2. **Standard Postgres clients don't cache TLS sessions across connections.** Measured directly: a
   _cooperating_ Python client that shares its `SSLContext` + re-presents the session resumes
   (`[F,T,T,T,T,T]`); the _default_ pg-client pattern — fresh context per connect, exactly what
   libpq/asyncpg do — **never** resumes (`[F,F,F,F,F,F]`). Beyond's churn = a new client per
   invocation = a fresh context every time = always a full handshake.

**Verdict: resumption is not a pooler-side lever we can ship.** The only way to capture its win is to
_avoid the handshake entirely_ via **connection reuse** (persistent client→pooler connections / an
edge proxy that terminates TLS once and keeps warm backends) — which is a client/platform-architecture
decision, not pgbouncer config. So for the pooler tier, **multi-worker is the lever; resumption is
not.**

### F″ Scale dynamics — add is free, remove is graceful (prereq for a reactive scaler)

With `so_reuseport`, scaling worker count up/down under live load:

- **Scale up** — a newly-spawned pooler registers with the kernel's `so_reuseport` group and starts
  taking _new_ connections immediately; existing connections on other workers are untouched. Demonstrated
  in `exp-multiworker.sh` (added poolers take proportional load within the 2 s startup).
- **Scale down** — `SIGINT` (pgbouncer "safe shutdown") drains gracefully. Measured: 200 persistent
  TLS connections at ~38k qps across 2 poolers, `SIGINT` one at t=3 s → it finished in-flight
  transactions and exited within 2 s; driver saw **`ok=305340 err=102 reconnects=302`** — i.e. the
  ~100 connections that were on the victim each took _exactly one_ error and reconnected (kernel routed
  them to the survivor). **0.033% error blip, no stall.** Use `SIGINT` (never `SIGTERM`/`SIGKILL`) to
  shed a worker. Script: scale-dynamics run (persistent `pd.py` driver).

**Net pooler conclusion:** the reactive in-VM pooler scaler is empirically sound. The mechanism scales
(linear), the trigger is observable (per-worker CPU in `/proc`), scale-up is zero-disruption, and
scale-down is graceful at ~0.03% cost. The remaining open question is purely _workload_ — does a real
Beyond instance's connection-churn rate exceed ~2.4k/s/core often enough to need a 2nd worker — which
only production telemetry answers.

### D2 `maintenance_io_concurrency` (cold-from-MinIO VACUUM)

See `d2-coldfork.txt` — measured via glidefs-restart-with-fresh-cache (data persists in MinIO,
empty cache → cold reads). Result appended on completion.

## Net so far

Measurement has already paid for itself by **killing two speculative knobs** (zstd, wal_recycle) that
intuition/the-ZFS-playbook would have shipped. The high-value hypotheses (checkpoint frequency,
maintenance_io_concurrency, cooldown-under-churn) remain open and need the tailored experiments above.
