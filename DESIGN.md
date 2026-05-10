# Postgres Image — Design Document

The official Postgres image for Beyond. A Firecracker rootfs that boots
Postgres 18 on a GlideFS-backed data volume, ships every extension we
care about, runs PgBouncer on the front, and forks with the substrate.
No SDK, no proprietary surface — the abstraction is `psql localhost`.

> **Status:** design decisions settled. Implementation in progress.

---

## Goals

- A drop-in Postgres that **just works** on Beyond. Connect to
  `localhost:5432`, get a database. Same command in dev, same in prod.
- **Forks with the platform.** All persistent state lives on a single
  GlideFS portable volume; `glide fork` carries it for free, and
  Postgres' own crash-recovery handles the consistency story.
- **Extensions the modern stack expects**: `pgvector`, `pg_trgm`,
  `postgis`, `pg_cron`, `pg_partman`, `pg_jsonschema`, `hypopg`,
  `pg_repack`, `pg_search`, `pg_stat_statements`, `auto_explain`,
  `pgvectorscale`. Plus Beyond's own `beyond_auth` and `beyond_queue`.
- **Lightweight**: assume the portable volume primitive. Update the
  image at any time; data persists across image swaps.
- **Extensible to HA without a rewrite.** MVP is single-instance.
  Tier 2 (sync replication, quorum durability) lands as a config flag
  and a control-plane orchestration, not a new image.

### Non-goals (v1)

- HA / sync replication / automatic failover. **Designed for, not
  built.** Tier 2 lands post-MVP without an image rewrite.
- Horizontal write scaling / sharding. Citus and Vitess-for-Postgres
  are separate primitives, not Postgres features. Vertical scaling
  plus read replicas covers the workload bar this image targets.
- Multi-master.
- Cross-region synchronous replication. Multi-region DR ships as
  async GlideFS volume replication, not at the Postgres layer.
- A Beyond-specific Postgres SDK. The abstraction is the wire protocol.
- `pg_upgrade` across major versions inside the image. Major upgrades
  are a control-plane operation against a maintenance VM.
- A query proxy / router (Vitess analog). PgBouncer is the only proxy.

---

## Where this lands

Closest in **scope** to Supabase's compute (single instance, vertically
scaled, EBS-style block durability + backups + async replicas later).
Closest in **ergonomics** to RDS (managed, opinionated, auto-tuned).
The wedge is the fork primitive: every other managed Postgres bolts
branching onto an existing stack; ours inherits it from the substrate.

Same durability bar as Supabase in MVP. Stronger than Supabase the day
Tier 2 lands — sync replication is a quorum primitive Supabase doesn't
ship.

---

## Trajectory

The platform commits to three answers. MVP ships #1 of each; the
seams in this image make #2 a config flag away.

### Durability — quorum-replicated WAL via Postgres sync replication

Every committed transaction on a production database lives on **≥2
independent failure domains** before the client gets an ack.
Mechanism: Postgres sync replication with
`synchronous_commit = remote_write` and
`synchronous_standby_names = 'ANY 1 (r1, r2)'`.

| Tier             | Environment | Mechanism                                        | Host-loss data loss         |
| ---------------- | ----------- | ------------------------------------------------ | --------------------------- |
| 1 — single (MVP) | durable     | GlideFS write-behind (S3 flush every 5 s)        | Up to 5 s / 64 MB acked WAL |
| 1 — single (MVP) | ephemeral   | GlideFS export `ephemeral=true` — local SSD only | Volume gone on host loss    |
| 2 — HA           | durable     | Sync replication, ≥2 replicas, quorum on ANY 1   | Zero (any single host loss) |

Ephemerality is a substrate property, not a Postgres feature.
Beyond marks preview, branch, and throwaway-fork environments as
ephemeral at the GlideFS export level; writes stay on local SSD and
never flush to S3 (zero storage cost, zero PUT/GET cost). The image
reads `BEYOND_VOLUME_EPHEMERAL` from MMDS and tunes for relaxed
durability — `synchronous_commit = off` for 5–10× faster commits,
bounded by a ~10 ms data-loss window that is irrelevant on a
throwaway volume. See decision **B-009**.

Tier 2 (durable) is stronger than Supabase's production tier (their
replicas are async). Stops short of Aurora-style storage-quorum-on-
every-write because GlideFS already provides 80 % of that — see
_Trade-offs_.

### Availability — warm standbys with automatic failover, leveraging volume portability

Production databases survive host loss with **seconds of downtime,
zero data loss**. Mechanism: warm standbys promote via
Patroni-style orchestration; the substrate's volume portability does
the rest.

| Tier             | Failover mechanism                                 | Downtime budget |
| ---------------- | -------------------------------------------------- | --------------- |
| 1 — single (MVP) | Box-manager rehomes VM; volume reattaches; recover | Minutes         |
| 2 — HA           | Standby promotes; PgBouncer reroutes               | Seconds         |

Tier 1 availability falls out of the substrate for free: GlideFS
volumes are host-independent, so a dead host doesn't lose the data —
it loses the _running compute_, which Beyond rebuilds. Tier 2 is
purely an optimization on the downtime budget.

### Scalability — vertical first, horizontal reads via replicas, no horizontal writes

| Lever                    | Mechanism                                           | Where                |
| ------------------------ | --------------------------------------------------- | -------------------- |
| Vertical scaling         | Beyond box resize; volume follows                   | MVP                  |
| Horizontal read scaling  | Async streaming replicas (`BEYOND_PG_TIER=replica`) | Post-MVP, same image |
| Horizontal write scaling | Out of scope — separate primitive                   | —                    |

Vertical scaling is the default lever and covers ~95 % of users up
to ~256 GB / 64 vCPU. Read replicas are the same image with a tier
flag flipped. Sharding is a different product.

---

## Trade-offs we're choosing

What we're explicitly **not** doing, and why each choice is the
right one given how Beyond operates.

### Not building Aurora/Neon-style log-disaggregated storage

The expensive 80 % of Aurora — content-addressed page storage with
S3 layering, CoW snapshots, local-SSD caching — is **already shipped
as GlideFS**. Aurora and Neon built pageservers because their
storage layer (EBS, local disks) couldn't do CoW. Ours can.

The remaining 20 % is quorum-durable WAL — exactly what Tier 2 sync
replication delivers. Forking Postgres to add a custom storage
manager (Neon's path) would buy stateless compute that fails over
without WAL replay. The operational gain over warm-standby failover
(seconds vs. milliseconds) is not worth a person-year of Postgres
fork maintenance.

### Not building multi-master

Postgres doesn't natively do it; bolt-ons (BDR, Bucardo) come with
conflict-resolution semantics that confuse users more than they
help. The audience this image serves wants standard Postgres.

### Not building horizontal write scaling

Sharding inverts the model: it requires a coordinator, query
rewriting, distributed transactions, and a reshape of the user's
schema design. **It's a different product**, not a Postgres
feature. If it ships on Beyond someday, it ships as a separate
primitive that runs Postgres VMs underneath, not as a config flag
on this image.

### Not building cross-region synchronous replication

Multi-region sync would put internet-RTT in the commit path —
unacceptable for OLTP. Cross-region DR is a problem the substrate
solves: GlideFS replicates volume backing to S3 in another region,
async, with a documented RPO. Region-local sync replication
(Tier 2) is what's in scope.

### Not building a Postgres-specific SDK

`localhost:5432` and the wire protocol are the abstraction. ORMs,
migration tools, every Postgres library on every platform — they
all work unchanged. A Beyond SDK would be a _worse_ surface than
the one users already have.

---

## Prerequisites

Cross-team dependencies the image cannot run without. Each is a small,
generic Beyond capability — none Postgres-specific, all useful for
sibling official images (queue, auth, kv).

| Prerequisite                                | Owner                     | Status    | What we need                                                                                                                                                                                                                                        |
| ------------------------------------------- | ------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pre-fork CHECKPOINT before snapshot         | box-manager + GlideFS API | Required  | Box-manager (or whichever component drives `glide fork`) calls our `checkpoint` vsock RPC on the source VM before `POST /snapshot`. Without it, fork-boot WAL replay can take 5–30 s for a hot source — undermines the substrate-thesis fork pitch. |
| GlideFS `ephemeral=true` per-export policy  | GlideFS                   | Confirmed | Beyond marks dev/preview/branch volumes ephemeral; writes never flush to S3 (B-009).                                                                                                                                                                |
| GlideFS `bless --hot-set` for fork prefetch | GlideFS                   | Confirmed | Used to prefetch `pg_class`/`pg_attribute`/recent WAL on fork boot.                                                                                                                                                                                 |

The image will not work in production without the first prerequisite.
The CHECKPOINT requirement is tracked alongside phase 3 (end-to-end boot)
and phase 4 (fork validation); see plans for the coordination cadence.

---

## Composition with the substrate

Postgres is a primitive on Beyond, not a service alongside it. Every
mechanism it relies on already exists.

| Postgres concept           | Beyond primitive                                                                           |
| -------------------------- | ------------------------------------------------------------------------------------------ |
| Persistent storage         | GlideFS portable volume mounted at `/var/lib/postgresql/18/main`                           |
| Crash-consistent fork      | `POST /api/exports/{vol}/snapshot` — block-level CoW                                       |
| Bootstrap config / secrets | MMDS — read at first boot, identical to rootfs pattern                                     |
| Log shipping               | `beyond-pg` pipes postgres+pgbouncer stderr → vsock `UserProcessStreamData` frames to host |
| Process supervision        | `beyond-pg` is PID 1; directly supervises postgres + pgbouncer                             |
| Auto-tuning                | MMDS metadata (RAM, vCPU) → `conf.d/01-tuning.conf`                                        |
| Connection ingress         | Beyond's network puts the right thing at `localhost:5432`                                  |
| Ext binaries               | Rootfs (content-addressed image) — shared blocks across every PG VM                        |

We don't add a single new Beyond primitive for MVP. Tier 2 will ask
box-manager for a local NVMe scratch device and a multi-VM placement
hint — both are additive to box-manager, not new subsystems.

---

## Volume topology

Three block devices. Two used in MVP, one reserved.

| Device | Backing                                | Lifetime | Holds                                        |
| ------ | -------------------------------------- | -------- | -------------------------------------------- |
| `vda`  | GlideFS content-addressed rootfs image | Image    | Postgres binaries, all extensions, system    |
| `vdb`  | GlideFS portable volume                | Database | `PGDATA` including `pg_wal`, `conf.d/`, logs |
| `vdc`  | (reserved — local NVMe scratch)        | Host     | Future Tier 2: `pg_wal` only                 |

**Why one volume in MVP, not two.** Putting WAL on the same volume as
data means forks atomically include WAL. `glide fork` produces a
crash-consistent block snapshot; Postgres' own recovery replays
uncheckpointed WAL on the fork the same way it would after a power
loss. No `pg_resetwal`, no pre-fork coordination, no special path.
This is the documented and tested Postgres recovery model.

The S3-PUT cost the GlideFS docs warn about
(`glidefs/README.md:312-335`) is a function of WAL throughput. For
dev, preview, and branch databases — the dominant Beyond use case —
WAL is in KB/s. The cost is negligible. For high-traffic primary
production, the answer is Tier 2 (replicas), not a different WAL
location.

**`pg_wal` is a symlink, not a directory.**
`/var/lib/postgresql/18/main/pg_wal → /var/lib/postgresql/18/wal`. In
MVP that target is a directory on `vdb`. In Tier 2 the bootstrap
creates the directory on a `vdc` mount instead. Postgres sees no
difference; the entire WAL-relocation story is a symlink swap.

### What lives where

```
/                                         (rootfs — vda)
├── usr/lib/postgresql/18/                # Postgres binaries
│   ├── bin/postgres                      # the server
│   └── lib/                              # shared_preload_libraries .so
├── usr/share/postgresql/18/              # SQL extension scripts
├── etc/postgresql/18/main/
│   ├── postgresql.conf                   # PGDG default; references include_dir below
│   ├── pg_hba.conf                       # baseline auth policy
│   └── hooks/                            # drop-in hook scripts (mostly empty in MVP)
│       ├── pre-start.d/
│       ├── post-start.d/
│       ├── pre-stop.d/
│       └── pre-fork.d/
└── usr/local/bin/
    └── beyond-pg                         # one binary; subcommands: boot, control, archive, backup

/var/lib/postgresql/18/                   (data — vdb)
├── main/                                 # PGDATA
│   ├── PG_VERSION
│   ├── postgresql.auto.conf
│   ├── pg_wal -> /var/lib/postgresql/18/wal
│   ├── conf.d/
│   │   ├── 00-beyond.conf                # image-managed, overwritten on boot
│   │   ├── 01-tuning.conf                # MMDS-RAM derived, regenerated on boot
│   │   ├── 02-durability.conf            # ephemeral-mode overrides; absent when durable
│   │   ├── 03-replication.conf           # (future Tier 2) — absent in MVP
│   │   └── 99-user.conf                  # user-owned, never touched
│   └── (PG-managed dirs)
└── wal/                                  # symlink target; on vdb in MVP, vdc in Tier 2
```

`postgresql.conf` is the unmodified PGDG default with one addition
near the top:

```ini
include_dir = '/var/lib/postgresql/18/main/conf.d'
```

Beyond owns `00-` and `01-`. Tier 2 owns `02-`. The user owns `99-`.
Higher numbers win. Image swaps replace `00-` and `01-`; user tweaks
in `99-` survive. This is the standard Postgres convention used by
Debian, RDS, and Crunchy.

---

## Lifecycle

Single-tier supervision. `beyond-pg` is PID 1 — no systemd, no intermediate
init, no two-tier indirection. The same binary runs in Firecracker and in
Docker for local dev; the boot path is identical in both environments.

`beyond-pg` handles every init responsibility and all Postgres-specific
behavior:

```
kernel
  └─► /sbin/init = /usr/local/bin/beyond-pg                  (PID 1)
        │   • mount /proc, /sys, /dev, /dev/pts, /run
        │   • IPv4 already up (kernel CONFIG_IP_PNP via ip= cmdline)
        │   • ip route add 169.254.169.254 dev eth0 (MMDS route)
        │   • IPv6 addr+route from ipv6=/ipv6_ext= cmdline params
        │   • write /etc/resolv.conf (IPv6 gw → 8.8.8.8 fallback)
        │   • set up zram swap
        │   • read MMDS (raw HTTP, retry+backoff; env var fallback)
        │   • reap zombies; clean shutdown on SIGTERM
        │
        │   1. run boot-time setup (`do_boot()`)
        │      - initdb if PGDATA empty
        │      - drop 00-beyond.conf, pg_hba.conf, 02-durability.conf
        │      - regenerate 01-tuning.conf from MMDS RAM
        │      - symlink pg_wal per BEYOND_PG_TIER
        │      - run pre-start.d/ scripts
        │   2. spawn + supervise children
        │      ├─► postgres        (restart on crash)
        │      └─► pgbouncer       (restart on crash)
        │   3. once Postgres healthy: run post-start
        │      - ALTER ROLE postgres WITH PASSWORD … (from MMDS)
        │      - CREATE EXTENSION IF NOT EXISTS …
        │      - apply per-extension config (e.g. cron.database_name)
        │   4. listen on vsock for control RPC
        │      (checkpoint, health, reload, backup)
        │   5. on SIGTERM: SIGTERM children, wait, power off
        ▼
       (kernel reboot(LINUX_REBOOT_CMD_POWER_OFF) on exit)
```

**Why `beyond-pg` as PID 1, not a two-tier model.**

1. **Composability.** `beyond-pg` reads MMDS directly (with env var fallback
   for local dev). The same binary works in Firecracker and in Docker — no
   platform-specific init binary in the image, no configuration differences
   between environments. `psql localhost` is the only abstraction.

2. **Open-source legibility.** This is a public repo. The boot path —
   `kernel → /sbin/init → beyond-pg → postgres` — is self-contained and
   requires no knowledge of Beyond's internal toolchain to understand.

3. **Minimum effective abstraction.** A two-tier model (outer init + inner
   binary) would save ~200 lines of init code at the cost of a binary
   dependency, an extra process slot, and inter-process coordination for MMDS
   data handoff. Not the right trade for a database image.

The tradeoff: `beyond-pg` owns zombie reaping, signal handling, and Linux
init responsibilities (~400 extra lines of Rust). Worth it for composability
and legibility.

### `beyond-pg` subcommands

One binary, three callable subcommands. All Postgres-image-specific
behavior lives here.

| Subcommand                | When it runs                                                | What it does                                                                                                                                                       |
| ------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `beyond-pg supervisor`    | Long-running. Exec'd as PID 1 (`/sbin/init → beyond-pg`).   | PID 1. Init responsibilities, boot setup, spawn/supervise postgres+pgbouncer, log forwarding (vsock when available), post-start, vsock RPC. The everything daemon. |
| `beyond-pg boot`          | Manual / debug. Internally called by `supervisor`.          | Idempotent boot-time setup as a standalone subcommand for ops re-execution.                                                                                        |
| `beyond-pg archive %p %f` | Per WAL segment. Invoked by Postgres via `archive_command`. | Reads `BEYOND_PG_ARCHIVE_TARGET` from MMDS. No-op if unset; otherwise ships the segment to the target.                                                             |

Backup is part of `supervisor` (triggered via the `backup` vsock RPC,
which runs `pg_basebackup` against the local Postgres). No separate
subcommand because there's no external invoker that needs a CLI surface.

### What `beyond-pg supervisor` does at boot

1. **Read MMDS** (direct HTTP to `169.254.169.254`; retry with backoff; env var fallback for local dev). Read:
   - `BEYOND_PG_TIER` (default `single`)
   - `BEYOND_VOLUME_EPHEMERAL` (default `false`)
   - `POSTGRES_PASSWORD` (required; fail closed if missing)
   - `POSTGRES_DATABASE` (default `postgres`)
   - `BEYOND_PG_ARCHIVE_TARGET` (optional)
2. If `PGDATA/PG_VERSION` exists, skip to step (5).
3. Run `initdb -D /var/lib/postgresql/18/main --auth=scram-sha-256
   --encoding=UTF8 --locale=en_US.UTF-8 --pwfile=<MMDS password>`.
4. Symlink `pg_wal` according to `BEYOND_PG_TIER`:
   - `single` → `/var/lib/postgresql/18/wal` on `vdb`
   - `primary` / `replica` → `/var/lib/postgresql/18/wal` on `vdc`
5. Drop `conf.d/00-beyond.conf` (overwriting). Drop `pg_hba.conf`
   (overwriting). If `BEYOND_VOLUME_EPHEMERAL=true`, drop
   `conf.d/02-durability.conf` with `synchronous_commit = off`;
   otherwise remove that file if it exists. User overrides in
   `99-user.conf` are never touched.
6. Regenerate `conf.d/01-tuning.conf` from VM resources. The
   supervisor reads `/sys/fs/cgroup/memory.max` first (cgroup-aware,
   correct under Docker for `beyond dev`) and falls back to
   `/proc/meminfo` only when no cgroup limit is set. Same for vCPU
   count: `/sys/fs/cgroup/cpu.max` first, then `/proc/cpuinfo`.
   ```ini
   # RAM-derived
   shared_buffers = {max(128, ram_mb / 4)}MB
   effective_cache_size = {ram_mb * 3 / 4}MB
   maintenance_work_mem = {min(ram_mb / 20, 2048)}MB
   work_mem = {max(32, ram_mb / 2 / (pool_size * 3))}MB
   max_connections = {clamp(pool_size + vcpus * 2 + 10, 100, ram_mb / 50)}
   wal_buffers = {clamp(shared_buffers_mb / 32, 1, 16)}MB

   # vCPU-derived
   max_worker_processes = {max(8, vcpus + 4)}
   max_parallel_workers = {max(1, vcpus)}
   max_parallel_workers_per_gather = {clamp(vcpus / 2, 1, 4)}
   max_parallel_maintenance_workers = {clamp(vcpus / 2, 1, 4)}
   ```
   Regenerated every boot so resized VMs pick up new defaults.

   **Why this `work_mem` formula** (instead of `ram * 0.01 / max_connections`):
   `work_mem` is allocated per sort/hash _node_ in the query plan, not per
   connection — a query with 3 sorts consumes 3 × work_mem simultaneously.
   Dividing ½ RAM by `pool_size × 3` (3 = avg sort/hash nodes per OLTP plan)
   keeps peak working-set memory under 50 % of RAM at full pool concurrency,
   while giving the bundled extensions (pgvector ANN, postgis, pg_search)
   enough headroom to avoid spilling. Floor 32 MB so small boxes stay usable.
   Tune with `log_temp_files = 0` and raise until spills disappear.

   **Why this `max_connections` formula** (instead of `vcpus * 25`):
   max_connections encodes three components explicitly — PgBouncer's server
   pool (20), optimal active connections for NVMe per the empirical formula
   `cores*2 + spindles` (spindles≈0 for SSD), and 10 reserved slots for
   superuser/monitoring/ETL on the direct `:5433` path. Floor 100 ensures
   small-box ETL headroom. The old `vcpus*25` formula produced 200–1600
   connections with no basis, wasting shared-memory structures proportional
   to `max_connections`.

   **Why cgroup-aware.** `/proc/meminfo` reports host RAM inside a
   container. If the container has a 512 MB cgroup limit and we set
   `shared_buffers = 25 % of host RAM`, Postgres allocates more than
   the cgroup allows and gets OOM-killed at startup. Reading the
   cgroup limit fixes this. In Firecracker the VM's view IS the
   actual VM RAM, so both paths give the same answer.
7. Ensure TLS cert exists at `PGDATA/beyond/server.{crt,key}`.
   - If absent, generate a self-signed cert (1-year validity,
     CN=hostname, SAN=DNS:localhost,DNS:*.beyond.dev,IP:127.0.0.1).
     `openssl` shelled out, or `rcgen` crate.
   - If present and within 30 days of expiry, regenerate.
   - Otherwise leave alone.
   - Set mode `0o600` on the key, `0o644` on the cert, owned by
     `postgres:postgres`.
   - Cert lives under PGDATA so it forks with the database (correct
     identity continuity on a fork).
   - Users override by replacing the files (or pointing to their own
     paths in `99-user.conf`); `beyond-pg supervisor` only writes if
     it generated the file itself (track via a sentinel).
8. Run scripts in `/etc/postgresql/18/hooks/pre-start.d/` (empty in
   MVP; future tier-specific setup lands here).
9. Spawn `postgres` and `pgbouncer` as supervised children.

### What `beyond-pg supervisor` does post-start

Runs once Postgres is healthy (`pg_isready` polling). Idempotent on
every boot.

1. `ALTER ROLE postgres WITH PASSWORD …` from MMDS.
2. `CREATE EXTENSION IF NOT EXISTS` for every preloaded extension.
3. Apply per-extension config (`cron.database_name = 'postgres'`,
   etc.).
4. Run `post-start.d/` hook scripts (empty in MVP).

### Vsock RPC surface (served by `beyond-pg supervisor`)

| Command      | Behavior                                                               |
| ------------ | ---------------------------------------------------------------------- |
| `checkpoint` | `psql -c CHECKPOINT;`. Used before snapshot for fast fork boot.        |
| `health`     | `pg_isready` plus `SELECT 1` round-trip.                               |
| `reload`     | `pg_ctl reload` after `99-user.conf` changes.                          |
| `backup`     | Runs `pg_basebackup`, ships the result via `BEYOND_PG_ARCHIVE_TARGET`. |

Tier 2 grows the surface (`pre-fork-prepare`, `promote`, `demote`,
`add-standby`, `drop-standby`) without changing the wire format.

---

## Configuration

`conf.d/00-beyond.conf` is the image's opinions. Every setting below
has a reason. Values are grouped by what they're solving for.

### Networking

```ini
listen_addresses = '127.0.0.1'
port = 5433
unix_socket_directories = '/var/run/postgresql'
```

**Why.** Postgres never speaks to the network directly. PgBouncer
fronts on `0.0.0.0:5432` (E-001). The user-facing port is PgBouncer.
Postgres on `127.0.0.1:5433` is for the things transaction pooling
can't do (DDL, ETL, `pg_dump`, advisory locks, `LISTEN/NOTIFY`).
Unix socket is for the local admin and bootstrap scripts.

### Logging

```ini
log_destination = 'stderr'
logging_collector = off
log_line_prefix = '%m [%p] %q[%a] db=%d,user=%u,client=%h '
log_min_duration_statement = 1000
log_statement = 'ddl'
log_connections = on
log_disconnections = on
log_lock_waits = on
log_temp_files = 10MB
```

**Why stderr only.** Log files in PGDATA fork with every branch and
bloat snapshots. `logging_collector = on` writes to files in PGDATA —
explicitly avoided. Stderr flows naturally from child processes to
`beyond-pg` (PID 1) and to the VM's console output without any
intermediate buffer or relay.

**Why these specific log knobs.** High-volume log settings generate
more output than is operationally useful and slow queries that hit
logging paths:

- `log_statement = 'ddl'` records schema changes always (cheap,
  high signal). `'all'` floods logs on any active DB — impractical to
  read and non-trivial CPU cost per statement.
- `log_min_duration_statement = 1000` catches slow queries without
  logging every one. Operators tune lower in dev.
- `log_connections / log_disconnections` are needed to debug pooler
  behavior. Cheap.
- `log_lock_waits` catches blocking. Indispensable for production
  triage.
- `log_temp_files = 10MB` flags queries that spill, which usually
  means a missing index or a wrong `work_mem`.
- `log_line_prefix` carries enough context (timestamp, pid, app,
  db, user, client) that a single grep on the host log pipeline
  identifies the offending session.

### Statistics

```ini
shared_preload_libraries = 'pg_stat_statements, auto_explain, pg_cron, beyond_auth, beyond_queue'
pg_stat_statements.max = 10000
pg_stat_statements.track = all
auto_explain.log_min_duration = 1000
auto_explain.log_analyze = on
auto_explain.log_buffers = on
```

**Why these in `shared_preload_libraries`.** `shared_preload_libraries`
requires a Postgres restart to change (P-001). Anything we might want
later goes in now. `pg_stat_statements` and `auto_explain` are the
two non-negotiable observability extensions in production. `pg_cron`
runs as a background worker and must be preloaded. `beyond_auth` and
`beyond_queue` install hooks at startup.

**Why `pg_stat_statements.track = all`.** Tracks nested statements
inside functions. Without this, slow queries called from a stored
procedure are invisible.

**Why `auto_explain.log_analyze = on`.** Logs the actual plan, not
just the predicted plan. The actual plan is what you need to debug
plan regressions. Costs ~5 % overhead on the queries that hit the
duration threshold, which is fine.

### pg_cron

```ini
cron.database_name = 'postgres'
cron.use_background_workers = on
```

**Why.** `pg_cron` requires `cron.database_name` to be set or it
won't start. Background workers (vs the older `pg_cron`-as-extension
process) avoid spawning per-job processes for short-running jobs.
Default to the modern path.

### Replication readiness (the Tier 2 seam)

```ini
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
wal_keep_size = 1GB
synchronous_commit = on
```

**Why all of these in MVP.** Each one requires a primary restart to
change. We pay the cost in MVP so adding replicas later doesn't
require bouncing production (B-005, B-007, P-001).

- `wal_level = logical` is the highest level. Enables logical
  decoding, CDC, future zero-downtime tier upgrades. Costs ~10 %
  more WAL volume than `replica`. Worth it.
- `max_wal_senders` and `max_replication_slots` at 10 leaves
  headroom for: 2 sync standbys + 2 async replicas + logical
  decoding consumers. Setting them higher costs nothing.
- `hot_standby = on` lets a node serve as a standby that allows
  reads. Default in PG 14+; explicit because changing it requires
  a restart.
- `wal_keep_size = 1GB` keeps enough WAL on the primary that a
  standby can usually catch up without a base backup after a brief
  network blip.
- `synchronous_commit = on` is the MVP default. Tier 2 flips to
  `remote_write` via `03-replication.conf` (a SIGHUP reload, not a
  restart) once `synchronous_standby_names` is set.

### Archiving

```ini
archive_mode = on
archive_command = '/usr/local/bin/beyond-pg archive %p %f'
```

**Why on with a stub.** `archive_mode` cannot be flipped from `off`
to `on` without restarting Postgres (B-008). The archive script
returns 0 when no `BEYOND_PG_ARCHIVE_TARGET` is set in MMDS, so the
WAL recycles normally. PITR ships later as a MMDS config change.

### WAL and checkpoints, tuned for GlideFS

GlideFS uses 128 KB blocks and a 5-second flush window. Postgres'
8 KB pages map 16:1 onto a GlideFS block. Two consequences worth
tuning around: write coalescing within the flush window is free, and
S3 PUT cost scales with the number of distinct dirty blocks per
flush. Tune toward fewer, larger flushes.

```ini
full_page_writes = on
wal_sync_method = fdatasync
wal_compression = lz4
checkpoint_timeout = 15min
max_wal_size = 4GB
min_wal_size = 1GB
checkpoint_completion_target = 0.9
bgwriter_delay = 200ms
bgwriter_lru_maxpages = 1000
```

**`full_page_writes = on`.** Required, do not disable. Local SSD
can still tear writes on power loss; without full-page images the
torn page is unrecoverable. The cost (a write amplification right
after each checkpoint) is real but unavoidable for correctness.

**`wal_sync_method = fdatasync`.** The Linux default. The
NBD/ublk path (`UBLK_ATTR_FUA`, `UBLK_ATTR_VOLATILE_CACHE`) handles
`fdatasync` correctly, hashing the block once and forcing the
device sync. Switching to `open_datasync` (`O_DSYNC` on every
write) interacts badly with the FUA path under load.

**`wal_compression = lz4`.** Note: GlideFS already LZ4-compresses
its packs at flush time, so on-disk savings _inside_ the volume are
near zero. The real benefits live elsewhere: streaming-replication
wire bytes (replicas catch up faster from the primary's `pg_wal/`),
WAL replay bytes during crash recovery and PITR, and archived WAL
shipped to S3 by `archive_command`. Worth on; don't expect smaller
files on the data volume.

**Checkpoint and bgwriter timing.** GlideFS coalesces writes within
its 5-second flush window: the same 128 KB block written 100 times
is hashed once. We want Postgres' write pattern to land in those
windows densely:

- `checkpoint_timeout = 15min` (default 5min). Larger checkpoints
  mean fewer full-page-write storms, which means less write
  amplification, which means fewer distinct dirty blocks per
  GlideFS flush, which means fewer S3 PUTs.
- `max_wal_size = 8GB`, `min_wal_size = 1GB`. `max_wal_size` is the
  WAL accumulation ceiling before Postgres fires a _requested_
  checkpoint, bypassing `checkpoint_timeout` entirely. Set high enough
  that `checkpoints_timed` always dominates `checkpoints_req` in
  `pg_stat_bgwriter`. If `checkpoints_req > 0`, raise it further.
- `checkpoint_completion_target = 0.9` spreads checkpoint writes
  over 90 % of the window. The IO becomes nearly steady-state
  instead of spiky, which gives GlideFS' coalescer the best chance
  to merge them.
- `bgwriter_delay = 100ms`, `bgwriter_lru_maxpages = 1000`. The
  bgwriter cleans dirty pages out of `shared_buffers` ahead of
  checkpoints. `bgwriter_lru_maxpages = 1000` (10× the default 100)
  sets the per-cycle page budget; `bgwriter_delay = 100ms` sets the
  cycle rate, sustaining ~80 MB/s of background cleaning and keeping
  `buffers_backend` near zero. Monitor: if `buffers_backend` is
  consistently low, the delay can be relaxed.

The pattern across all of these: prefer larger, less-frequent IO
events that ride the GlideFS flush window. Smaller, more frequent
checkpoints just multiply the work.

### Storage cost model

```ini
random_page_cost = 1.1
effective_io_concurrency = 200
```

**Why.** PG's defaults assume rotational storage. `random_page_cost = 4`
makes the planner prefer sequential scans where an index would actually
be faster. Local-SSD-backed GlideFS has random-I/O latency close to
sequential; `1.1` is the SSD-tuned community default. `effective_io_concurrency = 200`
enables aggressive prefetch (`posix_fadvise` on bitmap heap scans).
Default `1` is HDD-era; SSD-class storage handles ~1000 concurrent
I/Os easily. Cold S3 backfills are bounded by S3 latency, but those
are uncommon on the hot path.

### Defensive timeouts

```ini
idle_in_transaction_session_timeout = 5min
lock_timeout = 30s
```

**Why.** Idle transactions hold locks and prevent vacuum. A misbehaving
client (network blip, debugger paused, SIGSTOP'd process) can pin row
locks indefinitely; auto-rolling them back after 5 min is a defense,
not a guarantee. Long admin transactions can override in `99-user.conf`.

`lock_timeout = 30s` prevents DDL from blocking forever behind a
long-running transaction. The DDL fails fast; the operator sees it.

`statement_timeout` is intentionally unset — different workloads have
wildly different reasonable upper bounds. PgBouncer's `query_wait_timeout`
provides per-pool backpressure instead.

### Connection liveness (TCP keepalives)

```ini
tcp_keepalives_idle = 60
tcp_keepalives_interval = 10
tcp_keepalives_count = 6
```

**Why.** Detects dead connections within ~120 s. Default `0` means
"use OS default," which is 7200 s (2 hours) on Linux. PgBouncer-to-
Postgres connections through Beyond's private network can stay open
across VM reboots, network reconvergence, or partial failures. Without
keepalives, the pool fills with zombie connections that consume
`max_connections` slots until next use.

### Autovacuum

```ini
autovacuum_vacuum_scale_factor = 0.1
autovacuum_naptime = 30s
```

**Why.** Tighter than PG defaults (0.2 / 1min). pgvector and postgis
indexes get bloated quickly under heavy insert/update workloads. Vacuum
delay = bloat = degraded query performance. Tighter scale factors
trigger autovacuum on smaller table changes; shorter naptime means
autovacuum re-evaluates more often. Cost is a slight increase in
background CPU; in exchange the planner's stats stay fresh and the
indexes stay tight.

### Lock budget

```ini
max_locks_per_transaction = 256
```

**Why.** Default 64 is tight for `pg_partman`. Monthly partitions over
5 years = 60 partitions × multiple indexes each, which collides with
the default during cross-partition queries and DDL. 256 is conservative
headroom; per-connection memory cost (~~200 bytes per lock slot) is
negligible.

---

## Kernel sysctls and huge pages

The image drops `/etc/sysctl.d/99-postgres.conf`:

```
vm.swappiness = 10
vm.overcommit_memory = 2
vm.overcommit_ratio = 80
vm.dirty_background_bytes = 67108864
vm.dirty_bytes = 536870912
vm.min_free_kbytes = 131072
kernel.sched_migration_cost_ns = 5000000
kernel.sched_autogroup_enabled = 0
net.core.somaxconn = 1024
fs.aio-max-nr = 1048576
```

And on the kernel cmdline (set during the Packer build):

```
transparent_hugepage=never
```

**Why `vm.swappiness = 10`.** Linux defaults to 60, which means it
will swap out application pages — including Postgres `shared_buffers` —
under modest memory pressure to grow the page cache. For a database,
that's backwards: `shared_buffers` _is_ the cache you want to keep.
10 keeps it in RAM until things are genuinely tight.

**Why `vm.overcommit_memory = 2` and `vm.overcommit_ratio = 80`.**
With overcommit on (default), the OOM killer chooses victims when
memory runs out, which can mean killing the postmaster and dropping
every connection. With `overcommit = 2`, allocations beyond
`CommitLimit = RAM × (ratio/100) + swap` fail at malloc time, which
Postgres handles gracefully (returns ERROR, keeps running). The default
ratio of 50 sets CommitLimit to 50% of RAM — tight once shared_buffers
(25%) plus workers are counted. 80 gives the full Postgres footprint
room to allocate without hitting the ceiling prematurely.

**Why `vm.dirty_background_bytes` and `vm.dirty_bytes`.** The default
ratio-based dirty limits (10%/20% of RAM) allow multi-GB dirty-page
backlogs on large-RAM hosts before background flushing starts, causing
multi-second I/O stalls timed to checkpoint boundaries. Fixed byte
thresholds avoid this regardless of how much RAM the VM has. The
tradeoff is real: reducing dirty limits costs ~11–14% sustained write
throughput and slows vacuum by ~50–70% on write-heavy workloads — test
against your storage if vacuum latency matters.

**Why `kernel.sched_migration_cost_ns = 5000000` and
`kernel.sched_autogroup_enabled = 0`.** At high connection counts the
Linux CFS scheduler thrashes Postgres backends across CPU cores at the
default 500 µs migration cost, burning system CPU. Raising to 5 ms
lets placements stabilize. `sched_autogroup_enabled` groups tasks by
TTY for desktop responsiveness — it starves long-running server daemons
on headless VMs. Disabling it recovers CPU stolen from the Postgres
process group.

**Why `net.core.somaxconn = 1024`.** The kernel TCP listen backlog is
silently clamped to this value, limiting how many connection attempts
can queue during a restart storm before the OS starts refusing them.
PgBouncer absorbs steady-state pressure; this is a defensive floor for
burst scenarios.

**Why `fs.aio-max-nr = 1048576`.** PostgreSQL 18 introduces
`io_method = io_uring`, which issues many async I/O requests
concurrently. The default system-wide cap of 65,536 can be exhausted
under high concurrency with direct I/O. Setting it high now costs
nothing (it's a counter, not allocated memory) and avoids the failure
mode entirely.

**Why `transparent_hugepage=never`.** THP's `khugepaged` daemon
periodically defragments memory by promoting 4 KB pages into 2 MB
pages. This causes unpredictable latency spikes on busy databases —
well-known PG footgun. Disable it. Use explicit huge pages
(`vm.nr_hugepages` + `huge_pages = on`) for `shared_buffers` if we
want them; default `huge_pages = try` is fine without explicit setup.

---

## Authentication

`pg_hba.conf` is the image's auth baseline:

```
# TYPE  DATABASE  USER       ADDRESS        METHOD
local   all       all                       peer
host    all       all        127.0.0.1/32   scram-sha-256
host    all       all        ::1/128        scram-sha-256
host    all       all        all            scram-sha-256
```

**Why peer for local.** Unix socket connections from a process
running as the `postgres` user authenticate as the `postgres` role.
Standard PG idiom. Used by the bootstrap scripts and admin tooling.

**Why scram-sha-256 everywhere else.** The modern Postgres password
auth (default in PG 14+). `md5` is legacy. `trust` would be wrong.
There is no `trust` rule anywhere, even on the loopback interface,
so a misconfigured PgBouncer or runaway local process can't bypass
authentication.

**TLS is on by default.** `beyond-pg supervisor` generates a
self-signed cert on first boot, writes it under PGDATA (so it forks
with the database — the fork has the same identity), and `00-beyond.conf`
sets:

```ini
ssl = on
ssl_cert_file = '/var/lib/postgresql/18/main/beyond/server.crt'
ssl_key_file  = '/var/lib/postgresql/18/main/beyond/server.key'
```

PgBouncer terminates TLS for the public 5432 port using the same cert
(`client_tls_sslmode = allow`, `client_tls_cert_file`,
`client_tls_key_file`). The PgBouncer→Postgres hop runs over the unix
socket, no TLS needed there.

**Why TLS on, not off.** Defense in depth is nearly free if we
auto-provision. The cert lives on the data volume so it survives
image swaps and forks naturally. Once we ship TLS-off, turning it on
later is a breaking change for some clients (depending on driver
defaults). Pay it now.

Beyond's network is still the perimeter for non-Postgres concerns
(mTLS tunnel, private VXLAN, eBPF policy); PG TLS adds defense-in-
depth, not the perimeter itself. Customers with strict CA-chain
requirements override the cert in `99-user.conf` and supply their
own. The auto-provisioned cert is the floor, not the ceiling.

Cert rotation: supervisor regenerates if the cert is within 30 days
of expiry (1-year cert by default), then runs `pg_ctl reload`.
Idempotent.

**User override**: if `PGDATA/beyond/.user-managed` exists, cert generation
is skipped entirely — place your own `server.{crt,key}` in that directory
and touch `.user-managed` to prevent the supervisor from overwriting them.

---

## Extensions

All from PGDG apt unless noted. All `CREATE EXTENSION IF NOT EXISTS`'d
by the post-start hook.

| Extension            | Source                | Purpose                                    |
| -------------------- | --------------------- | ------------------------------------------ |
| `pg_stat_statements` | postgresql-contrib    | Query stats — preloaded                    |
| `auto_explain`       | postgresql-contrib    | Slow-query plans — preloaded               |
| `pg_trgm`            | postgresql-contrib    | Trigram fuzzy search                       |
| `pgvector`           | PGDG                  | Vector similarity                          |
| `pgvectorscale`      | PGDG (Timescale)      | StreamingDiskANN for pgvector              |
| `pg_cron`            | PGDG                  | In-DB cron — preloaded                     |
| `pg_partman`         | PGDG                  | Partition management                       |
| `pg_jsonschema`      | PGDG                  | JSON schema validation                     |
| `hypopg`             | PGDG                  | Hypothetical indexes                       |
| `pg_repack`          | PGDG                  | Online table reorg                         |
| `postgis`            | PGDG                  | GIS — ~250 MB, amortized via shared blocks |
| `pg_search`          | ParadeDB apt          | BM25 full-text                             |
| `beyond_auth`        | GitHub Release `.deb` | Authz BFS — preloaded                      |
| `beyond_queue`       | GitHub Release `.deb` | Queue/workflows — preloaded                |

`shared_preload_libraries` order: `pg_stat_statements, auto_explain,
pg_cron, beyond_auth, beyond_queue`. Order doesn't matter functionally
but stable order prevents config churn across image rebuilds.

### Sibling extensions (GitHub Release `.deb` consumption)

`beyond_auth` and `beyond_queue` are built and released by their own
repos. The image's Packer build downloads a pre-built `.deb` from the
corresponding GitHub Release at build time. `extensions.toml` pins the
GitHub repo URL and release tag for each; the build fails if the release
asset is absent.

The auth/queue repos' release pipelines must publish `.deb` assets for
both `amd64` and `arm64` matching PG 18, named
`postgresql-18-beyond-{auth,queue}_{version}_{arch}.deb`.

### PostGIS sizing rationale

PostGIS plus its dependency stack (libgeos, libproj, libgdal) adds
~200 MB to the rootfs image. This is content-addressed in GlideFS, so
the same blocks are shared across every Postgres VM globally — paid
once at image build, never duplicated. Cold-boot latency includes
demand-fetch of these blocks if a query touches them; otherwise they
stay cold and free. **Verdict: ship it.**

---

## Connection topology

PgBouncer co-locates with Postgres on the same VM. Same pattern
PlanetScale ships ("local PgBouncer"), same pattern Supabase ships.
This is the established norm for managed Postgres.

```
client (any)
  │
  ▼
0.0.0.0:5432  ──►  pgbouncer  ──►  /var/run/postgresql/.s.PGSQL.5433
                                  127.0.0.1:5433  (admin / direct)
                                       │
                                       ▼
                                   postgres
```

`pgbouncer.ini` defaults (per PlanetScale):

```ini
[databases]
* = host=/var/run/postgresql port=5433

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 5432
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt   # synced from PG by bootstrap
pool_mode = transaction
default_pool_size = 20
max_client_conn = 100
server_idle_timeout = 600
server_lifetime = 3600
max_prepared_statements = 200             # protocol-level prepared stmt support
ignore_startup_parameters = extra_float_digits, search_path
```

User-facing convention:

- **5432**: default. Transaction pooling. Use for app traffic.
- **5433**: direct. Use for migrations, `pg_dump`, ETL, advisory
  locks, `LISTEN/NOTIFY`, anything that doesn't survive transaction
  pooling.

The image documents this distinction. The platform documents this
distinction. ORMs target 5432.

---

## Logging

Both Postgres and PgBouncer write to stderr. `beyond-pg` spawns each
with a piped stderr, reads lines from both pipes via async Tokio tasks,
and forwards them over vsock to box-manager as `UserProcessStreamData`
frames — the same wire format `paraglide-agent` uses for user-app
supervised processes. Box-manager emits structured log events with full
context (`box_id`, `vm_id`, `execution_id`, stream type).

The rate-limit and wire format match `paraglide-agent`'s
`log_forwarder.rs` exactly: 500 lines/sec sustained, 1000 line burst;
lines truncated at `MAX_USER_PROCESS_LINE_BYTES`; a synthetic
`[beyond-pg: dropped N log lines]` frame inserted before the next
real line after any drops. The host receiver needs no changes.

No `logging_collector`, no log files, no journald. Logs in PGDATA
would fork with every branch and bloat snapshots — explicitly avoided.

**Log shipping mode** is auto-detected at startup: `beyond-pg` attempts
to connect to vsock; if the connection succeeds it pipes and forwards,
if it fails (no `/dev/vsock`, Docker, direct invocation) it lets child
stderr inherit directly and `docker logs` / the terminal captures it.
`BEYOND_LOG_VSOCK=false` forces pass-through even in Firecracker
(useful for debugging). Same binary, both environments, no flag needed
in the common case.

---

## Durability

The honest table.

| Failure                                  | What's lost                                               |
| ---------------------------------------- | --------------------------------------------------------- |
| Postgres crash                           | Nothing. WAL on local SSD survives, recovery replays.     |
| Guest VM crash / reboot                  | Nothing. WAL on local SSD survives.                       |
| GlideFS process restart (same host)      | Nothing. GlideFS WAL recovers dirty SSD state on restart. |
| **Host loss between GlideFS S3 flushes** | **Up to 64 MB / 5 s of acked-but-unflushed WAL.**         |
| Region / availability-zone loss          | Up to last completed S3 backup (see Backups below).       |

The host-loss bound is GlideFS' write-behind window
(`glidefs/ARCHITECTURE.md:909`). Same durability bar as Supabase's
default tier (single instance backed by EBS, with EBS itself doing
synchronous quorum within an AZ — Beyond gets a slightly weaker bound
because GlideFS S3-flush is async). MVP accepts this; Tier 2 (sync
replication) raises it to quorum.

This is documented user-facing. Honesty up front beats surprises.

### Backups are GlideFS snapshots

Backups for this image are not a separate product. They're GlideFS
snapshots, which we already have for forking. The substrate-thesis
plays out here the same way it does for storage: Beyond already has
the primitive; the image inherits it.

| Backup operation        | Implementation                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------------- |
| Daily / hourly backup   | `POST /api/exports/{vol}/snapshot` on a schedule. Atomic, CoW-cheap.                                    |
| Restore from backup     | `glide fork <snapshot-id>` → boots a Postgres VM at that snapshot's state.                              |
| PITR within an interval | `archive_command` ships WAL segments to S3 between snapshots; replay applies them up to the target LSN. |
| Cross-region DR         | GlideFS replicates volume backing across regions (substrate feature, async).                            |

What's better than `pg_basebackup`-shaped backups:

- **Atomic at the block layer.** No `pg_start_backup`/`pg_stop_backup`
  coordination needed. The snapshot is crash-consistent by construction
  (`glidefs/src/block/write_cache/flush.rs`); Postgres' standard
  recovery handles the rest.
- **CoW-cheap.** Each snapshot ships only the diff against the
  previous to S3. `pg_basebackup` always re-ships everything.
- **Restore is a fork.** The restore path is the same primitive as
  branching. Sub-second to the fork's first query (with pre-fork
  CHECKPOINT in place; see Prerequisites).

What the image ships:

- `archive_mode = on` and `archive_command = '/usr/local/bin/beyond-pg
  archive %p %f'`. The subcommand reads `BEYOND_PG_ARCHIVE_TARGET`
  from MMDS:
  - Empty / absent → exit 0 (no-op). WAL recycles normally.
  - `s3://...` → ship the WAL segment to the target.
- The `backup` vsock RPC on `beyond-pg supervisor` runs
  `pg_basebackup` against the local Postgres on demand, for the cases
  where `pg_basebackup` is required (logical replication setup,
  cross-major-version migration). Stub in MVP.

What lives outside the image:

- **Snapshot scheduling.** A control-plane cron that calls
  `POST /api/exports/{vol}/snapshot` on the desired cadence. Tens of
  lines of glue, not a service. Tracked separately.
- **Snapshot retention policy.** Same place.
- **PITR target/restore UX** (`glide pg restore --to-time T`). CLI
  surface, post-MVP.

---

## The fork story

`glide fork` snapshots `vdb`. The block-level snapshot is
crash-consistent (taken under GlideFS' write-cache rotation lock —
`glidefs/src/block/write_cache/flush.rs:441-568`) — equivalent to
yanking power from the source VM at the snapshot timestamp. ext4's
journal recovers; Postgres' WAL recovers; everything works.

```
Source VM                         Forked VM
─────────                         ─────────
postgres committing tx            ─────►  postgres starts
WAL fsynced to GlideFS                    sees crash-consistent state
GlideFS snapshot taken                    ext4 journal replay (fast)
data + pg_wal frozen at T                 postgres WAL replay from
                                          last checkpoint to T
                                          → ready to accept connections
```

**No `pg_start_backup`, no quiesce, no fsfreeze.** Postgres' crash
recovery is the consistency mechanism, the same way it works after a
power loss on bare metal. The substrate gives us crash-consistency
for free.

**Worst-case fork-boot latency** is bounded by `checkpoint_timeout`
(15 min default). A long-idle source forks instantly; a hot source
might take seconds to replay. The supervisor's `checkpoint` vsock RPC
lets a future pre-fork hook bound this. Drop a script in
`pre-fork.d/`, configure box-manager to call the RPC before snapshot,
done. Out of MVP scope but the seam exists.

**Hot-set bless.** GlideFS supports prefetching specific inode
patterns to local SSD before the fork starts serving I/O
(`glidefs/ARCHITECTURE.md:615`). For Postgres the right hot set is
`pg_class`, `pg_attribute`, `pg_proc`, `pg_index`, `pg_namespace`, the
last few WAL segments, and `pg_control`. Without bless, the first read
of a never-touched 128 KB region demand-fetches from S3 (50–300 ms).
With bless, fork boot is local-SSD-fast. Configured in the image
manifest, applied by the build pipeline.

---

## Extensibility seams

Every seam below is in the MVP image. Each is tagged with the
trajectory commitment it unlocks — durability (D), availability (A),
scalability (S). The seams are how Tier 2, async read replicas, PITR,
and cross-region DR drop in without an image rewrite.

### Durability seams

#### `wal_level = logical` from day one — D, S

Costs ~10 % WAL volume; can't be raised without a restart, so we pay
the cost now. Unlocks logical decoding, CDC consumers, zero-downtime
tier upgrades, and future read-replica-via-logical patterns.

#### Replication knobs preset — D, A, S

`max_wal_senders`, `max_replication_slots`, `hot_standby`,
`wal_keep_size` all sized in MVP. Adding replicas later (sync or
async) doesn't require a primary restart.
`synchronous_standby_names` is empty in MVP and flips on via SIGHUP
when Tier 2 enables.

#### WAL path indirection — D

`pg_wal → /var/lib/postgresql/18/wal`. MVP target is on `vdb`. Tier 2
target is a `vdc` (local NVMe) mount for sub-ms fsync latency. One
symlink swap, no Postgres reconfiguration.

#### `archive_command` wired with stub — D

`beyond-pg archive` runs on every WAL recycle. MVP no-ops when no
`BEYOND_PG_ARCHIVE_TARGET` is set. Tomorrow's PITR and cross-region
DR are a MMDS config change, not an image rebuild.

#### Backup-shipping stub — D

`beyond-pg backup` subcommand exists. Stub in MVP. Real
implementation lands when the backup service ships. Image is
already wired.

### Availability seams

#### Vsock control daemon — A

MVP ships `checkpoint`, `health`, `reload`. The wire format is
versioned. New commands grow the set as Tier 2 features ship:
`pre-fork-prepare`, `promote-to-primary`, `demote-to-replica`,
`add-standby`, `drop-standby`. The control plane drives failover
through this surface.

#### Hook directories — A, D

`pre-start.d/`, `post-start.d/`, `pre-stop.d/`, `pre-fork.d/`. Empty
in MVP except for the post-start `CREATE EXTENSION` script. Tier 2
features land as drop-in scripts: pre-fork CHECKPOINT (faster fork
boot), pre-stop graceful drain (clean failover), post-promotion
PgBouncer rewiring.

### Scalability and dispatch

#### `BEYOND_PG_TIER` in MMDS — D, A, S

Field exists in MVP. Valid values: `single`. Future: `primary`,
`replica`. Bootstrap branches on it. The dispatcher exists; the
branches grow. This single flag drives every tier-specific code
path in the image.

#### Layered config — D, A, S

`00-beyond.conf` and `01-tuning.conf` in MVP. `03-replication.conf`
written by bootstrap when `BEYOND_PG_TIER ≠ single` (carries
`primary_conninfo`, `synchronous_standby_names`, etc.). `99-user.conf`
always preserved across image swaps and tier changes.

### What Tier 2 looks like, concretely

```
control-plane: glide pg promote-to-ha myapp
  │
  ├─ provision 2 replica VMs (box-manager hint: "different host than primary")
  │  └─ each boots with BEYOND_PG_TIER=replica, primary_conninfo in MMDS
  │     └─ bootstrap drops standby.signal + 03-replication.conf, starts replica
  │
  ├─ wait for replicas to catch up (GET /v1/pg/replication/lag)
  │
  └─ on primary: vsock `set-sync-standbys r1,r2` → bootstrap rewrites
                  03-replication.conf, SIGHUP postgres
                  → synchronous_commit = remote_write,
                    synchronous_standby_names = 'ANY 1 (r1, r2)'
```

Zero image changes. The seams handle it.

---

## Image build pipeline

Mirrors `beyond/packer` — same Packer + Docker → tiered ext4 → bless
flow. The Postgres image is just another rootfs.

### Layout

```
postgres/
├── DESIGN.md                   # this file
├── README.md
├── ARCHITECTURE.md             # implementation reference once built
├── .mise.toml                  # image:postgres:build / :bless / :publish
├── extensions.toml             # pinned git URLs + tags for beyond_auth, beyond_queue; versions for PGDG/ParadeDB
├── packer/
│   ├── templates/
│   │   └── postgres-rootfs.pkr.hcl
│   ├── scripts/
│   │   ├── 01-base-packages.sh         # OS packages, locales, /sbin/init → beyond-pg
│   │   ├── 02-postgres-install.sh      # PGDG apt, postgresql-18, contrib
│   │   ├── 03-pgdg-extensions.sh       # vector, postgis, cron, partman, etc.
│   │   ├── 04-beyond-extensions.sh     # download beyond_{auth,queue} .deb from GitHub Releases
│   │   ├── 05-pgbouncer-install.sh     # pgbouncer apt
│   │   ├── 06-beyond-pg-install.sh     # build/install single beyond-pg binary + hooks/
│   │   ├── 07-config.sh                # drop conf.d/, pg_hba, postgresql.conf include
│   │   ├── 08-mmds.sh                  # parity with rootfs
│   │   ├── 09-cleanup.sh
│   │   └── post-process.sh             # ext4 + tier sizing + bless
│   └── files/
│       ├── postgresql/
│       │   ├── 00-beyond.conf
│       │   └── pg_hba.conf
│       ├── pgbouncer/
│       │   └── pgbouncer.ini
│       └── beyond-pg/                  # source for our binary
│           ├── Cargo.toml
│           └── src/
│               ├── main.rs             # subcommand dispatch
│               ├── supervisor.rs       # `beyond-pg supervisor` (the inner tier)
│               ├── boot.rs             # boot-time setup (called by supervisor; also exposed as `beyond-pg boot`)
│               ├── archive.rs          # `beyond-pg archive`
│               ├── rpc.rs              # vsock RPC server used by supervisor
│               └── log_forwarder.rs    # pipe-stdio → vsock UserProcessStreamData frames
```

No systemd. `beyond-pg` is PID 1 — `/sbin/init` symlinks to
`/usr/local/bin/beyond-pg`. The same binary runs in Firecracker and
in Docker for local dev. Box-manager injects the standard
`paraglide-init` and `paraglide-agent` binaries into the rootfs (as
it does for every image) but neither is started — `/sbin/init` points
to `beyond-pg`.

### Mise tasks

```
image:postgres:build [version] [--bless] [--tier 128g]
image:postgres:bless <image_path> <base_name>
image:postgres:publish [image_name]
```

Same shape as `image:build` in `beyond/.mise.toml`. The data volume
uses the existing `image:build-volume-blanks` task — initdb runs into
an empty ext4 on first boot. **No new volume builder.**

### Image versioning

`postgres-noble-{git_sha}.img`. Manifest pins:

```toml
# extensions.toml
[postgres]
version = "18.0"

[extensions.beyond_auth]
version = "0.4.2"

[extensions.beyond_queue]
version = "1.1.0"
```

A build with a missing pinned version fails fast, not at first boot.

---

## Trust boundaries

Same network model as the rest of Beyond. The image doesn't add a
security layer.

- Beyond's network is the perimeter (mTLS tunnel, private VXLAN, eBPF).
- TLS at the PG layer is on by default with an auto-provisioned
  self-signed cert under PGDATA. Customers with strict CA-chain
  requirements override the cert files in `99-user.conf`.
- `pg_hba.conf` defaults to `scram-sha-256` for everything except
  Unix socket peer auth.
- Superuser password is mandatory at first boot (from MMDS); no
  trust-mode anywhere.
- `beyond-pg supervisor` listens on a vsock port for control RPC.
  Vsock is host-local, not network-reachable.
- PgBouncer's `auth_file` is generated by `beyond-pg supervisor` from
  PG roles, not a static file.

---

## Failure modes

| Failure                                         | What happens                                                                                               | Recovery                                                 |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| Postgres crash                                  | `beyond-pg supervisor` restarts it; WAL replay on start.                                                   | Automatic.                                               |
| PgBouncer crash                                 | `beyond-pg supervisor` restarts it; in-flight transactions error.                                          | Automatic restart; client retries.                       |
| `beyond-pg` crash (PID 1)                       | Cannot recover. PID 1 crash is a kernel panic — Firecracker drops the VM. Box-manager rehomes from volume. | Box-manager.                                             |
| MMDS unreachable at first boot                  | `beyond-pg` fails closed (no superuser password), retries with backoff.                                    | Automatic once MMDS is up.                               |
| Volume mount empty                              | `beyond-pg supervisor` detects, runs initdb, applies config, creates extensions.                           | Automatic — first boot path.                             |
| Volume mount has data, image upgraded           | `beyond-pg supervisor` detects PG_VERSION, skips initdb, replaces `00-beyond.conf` and `01-tuning.conf`.   | Automatic — image-swap path.                             |
| Major version bump (PG 18→19)                   | `beyond-pg supervisor` detects PG_VERSION mismatch, exits with error. Maintenance VM runs `pg_upgrade`.    | Out of image scope; control-plane operation.             |
| Sibling extension `.deb` missing at build       | Packer build fails with explicit error (curl -f).                                                          | Cut a GitHub Release in the sibling repo, rebuild image. |
| `archive_command` target unreachable            | WAL archive fails; `pg_wal` accumulates segments.                                                          | Operator alert via metric; retry on next segment.        |
| GlideFS SSD utilization > 95 %                  | Writes rejected with ENOSPC. Postgres panics; supervisor restarts; same condition persists.                | Host-side capacity policy; out of image scope.           |
| Fork boots before parent's checkpoint completed | WAL replay from last checkpoint to fork timestamp. Slower boot.                                            | Pre-fork CHECKPOINT hook (post-MVP).                     |
| User sets bad value in `99-user.conf`           | `beyond-pg supervisor` ignores `99-`; PG fails to start; supervisor logs and retries.                      | User edits, calls `reload` over vsock RPC.               |

---

## Performance notes

- **Connection establishment**: PgBouncer transaction pooling — pool
  hit is ~100 µs, pool miss is one PG fork (~1 ms).
- **Steady-state writes**: bounded by GlideFS local-SSD IOPS, not S3.
  Local-SSD fdatasync is ~50 µs.
- **Steady-state reads**: hot pages in PG `shared_buffers` (RAM) —
  microseconds. Cold pages on local SSD — tens of microseconds.
  Cold pages on S3 (very-cold forks) — tens to hundreds of milliseconds
  for the first 128 KB block, then SSD-resident.
- **Fork boot**: dominated by WAL replay since last checkpoint. With
  `checkpoint_timeout = 15min` and a hot DB, worst case 5–30 s. Most
  forks are sub-second.
- **`POST /snapshot` latency**: 500 ms – 2 s (S3 manifest PUT). The
  user-facing "200 ms fork" is a lower-bound metadata fork; data
  becomes consistent after the manifest PUT.
- **WAL throughput**: `wal_compression = lz4` on top of GlideFS' own
  pack-level LZ4 saves replay-side bytes too. GlideFS coalesces 8 KB
  Postgres writes into 128 KB blocks within a 5 s flush window for
  free.

---

## Why this is the minimum effective abstraction

We add **one image, one supervisor binary (PID 1), one tuning script,
four hook directories, two stub subcommands**. Every durability,
replication, and fork mechanism is a Postgres or GlideFS primitive
that already exists.

The image is opinionated about what's bundled (the extensions modern
Postgres apps reach for) and minimal about what's invented (one
config snippet, one tuning rule, one bootstrap path).

Tier 2 (HA, quorum durability) ships without a new image — the seams
are in MVP. The control-plane spawns replica VMs, drops a config
snippet, and flips a SIGHUP. The image is ready for it on day one.

Same `psql localhost` everywhere.
