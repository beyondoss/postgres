# GlideFS × Postgres substrate-tuning harness

Measure-first rig for `plans/wal-recycle-off-*.md`. It puts a real Postgres data
dir on a real GlideFS block device backed by an in-memory / local-file / MinIO
object store (never real S3), runs a fixed pgbench workload, and scrapes
GlideFS's own per-export metrics before/after so each tuning knob is scored on
**S3 write cost and coalescing**, not intuition.

## Why this exists

The S3 cost of running Postgres on GlideFS is _distinct 128 KiB blocks flushed
per cycle_. Overwrite-before-flush coalesces for free. Every knob in the plan
(checkpoints, `*_flush_after`, bgwriter, `wal_compression`, `wal_recycle`,
autovacuum-on-CoW, `compaction_cooldown`) changes how many distinct blocks reach
the object store. This rig measures that directly via
`glidefs_s3_batches_written_total`, `glidefs_coalesce_ratio`,
`glidefs_write_amplification`.

## Prereqs (already present on the homelab box)

- `glidefs` binary, `nbd` + `ublk_drv` kernel modules loaded
- passwordless `sudo` (glidefs needs CAP_SYS_ADMIN for `/dev/nbdN`; mkfs/mount)
- Postgres 18 client tools (`initdb`, `pg_ctl`, `pgbench`, `psql`)

## Run

```sh
# one run (baseline)
mise run bench:substrate

# a single candidate knob
bench/glidefs-pg/run.sh --conf bench/glidefs-pg/conf/c1-checkpoint60.conf --label c1-60

# full A/B sweep (baseline 3x for the noise floor, then each overlay)
bench/glidefs-pg/sweep.sh
```

Knobs: `--backend file|memory|minio` (default `file` — same byte accounting as
`memory`, no RAM blowup on long runs), `--scale`, `--duration`, `--clients`,
`--cooldown N` (GlideFS compaction_cooldown, the G experiment), `--transport
nbd|ublk`, `--keep`.

## Output

`out/<ts>-<label>/`:

- `delta.txt` — the scoreboard (before / after / Δ per metric, plus pgbench TPS)
- `before.prom` / `after.prom` — raw Prometheus snapshots (export-scoped)
- `*.json` — per-export metric snapshots
- `glidefs.toml`, `harness.conf` — exact inputs for that run
- `postgres.log`, `pgbench.log`

## Reading the scoreboard

| Want                     | Watch                                                 | Direction      |
| ------------------------ | ----------------------------------------------------- | -------------- |
| less S3 write cost       | `s3_batches_written_total` / `s3_bytes_written_total` | ↓              |
| more coalescing          | `coalesce_ratio`                                      | ↑              |
| less write-amp           | `write_amplification`                                 | ↓              |
| cold-read pressure       | `cache_misses_total`, `s3_bytes_read_total`           | ↓ (read tests) |
| no throughput regression | pgbench `tps`                                         | flat/↑         |

**Ship a knob only if its delta clears the 3-run baseline noise floor.**

## Experiment → conf map

| Exp | Conf overlay                | Hypothesis                                                              |
| --- | --------------------------- | ----------------------------------------------------------------------- |
| C1  | `c1-checkpoint30/60.conf`   | fewer/larger checkpoints → fewer FPW bursts → fewer S3 bytes            |
| C2  | `c2-flushafter-2mb/0.conf`  | relax forced writeback → more coalescing                                |
| C3  | `c3-bgwriter-gentle.conf`   | gentler bgwriter → re-dirty in place → more coalescing                  |
| D1  | `d1-zstd.conf`              | zstd WAL → fewer S3/sink bytes                                          |
| D2  | `d-maintenance-io.conf`     | parallel cold-from-S3 maintenance scans (use with a fork/VACUUM mode)   |
| A1  | `a1-no-recycle.conf`        | CoW WAL hygiene — expected ≈ noise (see plan §A)                        |
| E1  | `e1-autovac-throttled.conf` | fork autovacuum profile → less CoW divergence (use with fork-idle mode) |
| G   | `--cooldown 8` vs `0`       | compaction_cooldown → −22% S3 PUT (justifies the cross-repo change)     |

D2, E1, and G need the fork/idle run modes in `sweep.sh`; the rest run directly
with `--conf`.
