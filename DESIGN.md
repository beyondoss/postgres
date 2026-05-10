# Postgres Image — Design Document

The official Postgres image for Beyond. A Firecracker rootfs that boots
Postgres 18 on a GlideFS-backed data volume, ships every extension we
care about, runs PgBouncer on the front, and forks with the substrate.
No SDK, no proprietary surface — the abstraction is `psql localhost`.

> **Status:** design proposal. Not yet built.

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

## Composition with the substrate

Postgres is a primitive on Beyond, not a service alongside it. Every
mechanism it relies on already exists.

| Postgres concept           | Beyond primitive                                                                               |
| -------------------------- | ---------------------------------------------------------------------------------------------- |
| Persistent storage         | GlideFS portable volume mounted at `/var/lib/postgresql/18/main`                               |
| Crash-consistent fork      | `POST /api/exports/{vol}/snapshot` — block-level CoW                                           |
| Bootstrap config / secrets | MMDS — read at first boot, identical to rootfs pattern                                         |
| Log shipping               | `beyond-pg supervisor` pipes child stdio over vsock to host                                    |
| Process supervision        | `paraglide-init` supervises `beyond-pg supervisor`; supervisor supervises postgres + pgbouncer |
| Auto-tuning                | MMDS metadata (RAM, vCPU) → `conf.d/01-tuning.conf`                                            |
| Connection ingress         | Beyond's network puts the right thing at `localhost:5432`                                      |
| Ext binaries               | Rootfs (content-addressed image) — shared blocks across every PG VM                            |

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

Two-tier supervision. Same shape as every Beyond VM, with the inner
tier replaced by a Postgres-aware binary. No systemd anywhere on the
image.

**Outer tier — `paraglide-init`** (Beyond's generic init, unchanged).
PID 1. Mounts `/proc`, `/sys`, `/dev`, `/dev/pts`, `/run`. Configures
network and DNS. Adds the MMDS route. Fetches MMDS into
`/run/mmds/metadata.json`, `/etc/hostname`, `/etc/environment`. Sets
up zram. Reaps zombies. Powers off cleanly on SIGTERM. **Supervises
one child** at `/usr/local/bin/paraglide-agent` with restart-on-crash
plus a 10-second drain on shutdown. Zero Postgres-specific code.

**Inner tier — `beyond-pg supervisor`**. Sits in the agent slot.
This is our binary at `/usr/local/bin/paraglide-agent`. Replaces the
generic `paraglide-agent` for this image because we don't need its
user-app features (file watching, sync, MCP, lifecycle phases — all
designed for the iterative-development loop, none of which applies
to a database). Spawns and supervises `postgres` and `pgbouncer`,
ships their stdio over vsock to the host, listens on vsock for
control-plane RPC, runs boot-time setup before forking children.
Tightly scoped: ~few hundred lines of Rust.

The two layers compose because `paraglide-init`'s contract is
already "supervise one child; restart it on crash; drain it on
shutdown." We slot in. No changes to `paraglide-init` needed.

```
kernel
  └─► /sbin/init = paraglide-init                  (PID 1, generic, unchanged)
        │   • mount /proc, /sys, /dev, /dev/pts, /run
        │   • configure network + DNS, MMDS route
        │   • fetch MMDS → /run/mmds/{metadata.json,hostname,environment}
        │   • zram swap
        │   • reap zombies; clean shutdown on SIGTERM
        │
        └─► /usr/local/bin/paraglide-agent = `beyond-pg supervisor`   (our binary)
              │   1. run boot-time setup (`do_boot()` inline)
              │      - initdb if PGDATA empty
              │      - drop 00-beyond.conf, pg_hba.conf, 02-durability.conf
              │      - regenerate 01-tuning.conf from MMDS RAM
              │      - symlink pg_wal per BEYOND_PG_TIER
              │      - run pre-start.d/ scripts
              │   2. spawn + supervise children
              │      ├─► postgres        (restart on crash, log_forwarder → vsock)
              │      └─► pgbouncer       (restart on crash, log_forwarder → vsock)
              │   3. once Postgres healthy: run post-start
              │      - ALTER ROLE postgres WITH PASSWORD … (from MMDS)
              │      - CREATE EXTENSION IF NOT EXISTS …
              │      - apply per-extension config (e.g. cron.database_name)
              │   4. listen on vsock for control RPC
              │      (checkpoint, health, reload, backup)
              │   5. on SIGTERM from paraglide-init: drain children, exit 0
              ▼
       (paraglide-init reaps, calls reboot LINUX_REBOOT_CMD_POWER_OFF)
```

### Box-manager coordination

`paraglide-agent` is injected into user-app VMs by box-manager via
GlideFS derived snapshots, so each VM gets a fresh agent without
rebuilding the rootfs. For our image that injection would shadow our
binary at `/usr/local/bin/paraglide-agent` and break us.

Box-manager needs one generic capability: **skip agent injection for
self-supervised images**. A bit on the image manifest
(`self_supervised: true`) or a per-image flag on the VM provisioning
call. Not Postgres-specific — the queue, auth, and kv images all want
the same behavior. Small ask. Out of this image's scope but
prerequisite to running it.

### `beyond-pg` subcommands

One binary, three callable subcommands. All Postgres-image-specific
behavior lives here.

| Subcommand                | When it runs                                                         | What it does                                                                                                          |
| ------------------------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `beyond-pg supervisor`    | Long-running. Started by `paraglide-init` as the agent slot's child. | Inner supervision tier. Boot setup, spawn/supervise postgres+pgbouncer, post-start, vsock RPC. The everything daemon. |
| `beyond-pg boot`          | Manual / debug. Internally called by `supervisor`.                   | Idempotent boot-time setup as a standalone subcommand for ops re-execution.                                           |
| `beyond-pg archive %p %f` | Per WAL segment. Invoked by Postgres via `archive_command`.          | Reads `BEYOND_PG_ARCHIVE_TARGET` from MMDS. No-op if unset; otherwise ships the segment to the target.                |

Backup is part of `supervisor` (triggered via the `backup` vsock RPC,
which runs `pg_basebackup` against the local Postgres). No separate
subcommand because there's no external invoker that needs a CLI surface.

### What `beyond-pg supervisor` does at boot

1. **Wait for MMDS readiness** (parity with rootfs `mmds-setup`). Read:
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
6. Regenerate `conf.d/01-tuning.conf` from MMDS RAM:
   ```ini
   shared_buffers = {ram * 0.25}MB
   effective_cache_size = {ram * 0.75}MB
   maintenance_work_mem = {min(ram * 0.05, 2GB)}MB
   work_mem = {ram * 0.01 / max_connections}MB
   max_connections = {pgbouncer.default_pool_size * 2 + 50}
   wal_buffers = {min(shared_buffers / 32, 16MB)}MB
   ```
   Regenerated every boot so resized VMs pick up new defaults.
7. Run scripts in `/etc/postgresql/18/hooks/pre-start.d/` (empty in
   MVP; future tier-specific setup lands here).
8. Spawn `postgres` and `pgbouncer` as supervised children.

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

**Why stderr only.** `beyond-pg supervisor` spawns Postgres with
piped stdio and forwards lines over vsock to the host log pipeline.
`logging_collector = on` would divert lines to a file the supervisor
can't see. Log files in PGDATA would also fork with every branch and
bloat snapshots.

**Why these specific log knobs.** The host-side log pipeline is
rate-limited (~500 lines/sec sustained, 1000 burst per stream). The
defaults below stay comfortably under:

- `log_statement = 'ddl'` records schema changes always (cheap,
  high signal). `'all'` would trip the rate limit on any active DB.
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

**`wal_compression = lz4`.** GlideFS' packs are LZ4-compressed at
flush time anyway, but compressing WAL inside Postgres saves
replay-side bytes too: a smaller WAL means less to read on standby
recovery and PITR.

**Checkpoint and bgwriter timing.** GlideFS coalesces writes within
its 5-second flush window: the same 128 KB block written 100 times
is hashed once. We want Postgres' write pattern to land in those
windows densely:

- `checkpoint_timeout = 15min` (default 5min). Larger checkpoints
  mean fewer full-page-write storms, which means less write
  amplification, which means fewer distinct dirty blocks per
  GlideFS flush, which means fewer S3 PUTs.
- `max_wal_size = 4GB`, `min_wal_size = 1GB`. Sized to give the
  15-minute checkpoint window enough WAL headroom without forcing
  premature checkpoints under burst.
- `checkpoint_completion_target = 0.9` spreads checkpoint writes
  over 90 % of the window. The IO becomes nearly steady-state
  instead of spiky, which gives GlideFS' coalescer the best chance
  to merge them.
- `bgwriter_delay = 200ms`, `bgwriter_lru_maxpages = 1000`. The
  bgwriter cleans dirty pages out of `shared_buffers` ahead of
  checkpoints. Tuned to push more pages per cycle (1000 vs default
  100) so the checkpointer has less to do when its turn comes.

The pattern across all of these: prefer larger, less-frequent IO
events that ride the GlideFS flush window. Smaller, more frequent
checkpoints just multiply the work.

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

**Why TLS is off.** Beyond's network is the perimeter:

- The Beyond tunnel does mTLS for client traffic.
- VM-to-VM traffic runs over private VXLAN with eBPF policy.
- Postgres' connection is never on a public network.

Adding PG-level TLS in MVP costs CPU plus cert management for zero
marginal security gain. Users who need defense in depth or have
apps connecting outside Beyond's tunnel flip `ssl = on` in
`99-user.conf` and provide certs (I-001).

---

## Extensions

All from PGDG apt unless noted. All `CREATE EXTENSION IF NOT EXISTS`'d
by the post-start hook.

| Extension            | Source              | Purpose                                    |
| -------------------- | ------------------- | ------------------------------------------ |
| `pg_stat_statements` | postgresql-contrib  | Query stats — preloaded                    |
| `auto_explain`       | postgresql-contrib  | Slow-query plans — preloaded               |
| `pg_trgm`            | postgresql-contrib  | Trigram fuzzy search                       |
| `pgvector`           | PGDG                | Vector similarity                          |
| `pgvectorscale`      | PGDG (Timescale)    | StreamingDiskANN for pgvector              |
| `pg_cron`            | PGDG                | In-DB cron — preloaded                     |
| `pg_partman`         | PGDG                | Partition management                       |
| `pg_jsonschema`      | PGDG                | JSON schema validation                     |
| `hypopg`             | PGDG                | Hypothetical indexes                       |
| `pg_repack`          | PGDG                | Online table reorg                         |
| `postgis`            | PGDG                | GIS — ~250 MB, amortized via shared blocks |
| `pg_search`          | ParadeDB apt        | BM25 full-text                             |
| `beyond_auth`        | sibling repo `.deb` | Authz BFS — preloaded                      |
| `beyond_queue`       | sibling repo `.deb` | Queue/workflows — preloaded                |

`shared_preload_libraries` order: `pg_stat_statements, auto_explain,
pg_cron, beyond_auth, beyond_queue`. Order doesn't matter functionally
but stable order prevents config churn across image rebuilds.

### Sibling extensions (`.deb` consumption)

`beyond_auth` and `beyond_queue` are built and released by their own
repos. The image's Packer build pulls a versioned `.deb` from S3
(`s3://beyond-extensions/{auth,queue}/{version}/{arch}/*.deb`) at
build time. Decoupling release cadence: the image specifies version
ranges in a manifest (`extensions.toml`) and the build fails if the
required version isn't in S3.

The auth/queue repos' release pipelines must publish `.deb`s for
both `amd64` and `arm64` matching PG 18.

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

Both Postgres and PgBouncer write to stderr. `beyond-pg supervisor`
spawns each with piped stdio, reads lines from both pipes, and forwards
them over vsock to the host log pipeline. The wire format mirrors what
`paraglide-agent` uses for user apps so the host receiver doesn't need
to distinguish official-image traffic from user-app traffic.

No `logging_collector`, no log files, no journald. Logs in PGDATA
would fork with every branch and bloat snapshots — explicitly avoided.

Rate limits: ~500 lines/sec sustained per stream, 1000 burst (host
pipeline default). Postgres defaults are tuned to stay well under (no
`log_statement = 'all'`).

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

### Backups (out of image scope, but wired)

`archive_mode = on`, `archive_command = '/usr/local/bin/beyond-pg
archive %p %f'`. The subcommand reads `BEYOND_PG_ARCHIVE_TARGET` from
MMDS:

- Empty / absent → script returns 0, WAL is recycled normally. No
  archiving. (MVP default.)
- `s3://...` → script ships the WAL segment to the target.

A separate Beyond backup service (out of this repo) drives base
backups and PITR. The image just exposes the hook.

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
├── extensions.toml             # pinned versions of beyond_auth, beyond_queue
├── packer/
│   ├── templates/
│   │   └── postgres-rootfs.pkr.hcl
│   ├── scripts/
│   │   ├── 01-base-packages.sh         # OS packages, locales, /sbin/init → paraglide-init
│   │   ├── 02-postgres-install.sh      # PGDG apt, postgresql-18, contrib
│   │   ├── 03-pgdg-extensions.sh       # vector, postgis, cron, partman, etc.
│   │   ├── 04-beyond-extensions.sh     # pull beyond_{auth,queue} .deb from S3
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
│               └── log_forwarder.rs    # pipe-stdio → vsock log frames
```

No systemd. PID 1 is `paraglide-init` (Beyond's generic init, same
binary on every Beyond VM). `paraglide-init` supervises one child at
`/usr/local/bin/paraglide-agent`; in this image that path is our
`beyond-pg` binary entered via `beyond-pg supervisor`. The supervisor
spawns and supervises postgres + pgbouncer.

**Box-manager prerequisite.** Box-manager normally injects
`paraglide-agent` via a derived snapshot, which would shadow our
binary. The image manifest carries a `self_supervised: true` bit so
box-manager skips the injection. Generic feature, not Postgres-
specific. Other official images (queue, auth, kv) use the same flag.

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
- TLS at the PG layer is off in MVP; `99-user.conf` can flip it on.
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

| Failure                                                          | What happens                                                                                                                                             | Recovery                                           |
| ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| Postgres crash                                                   | `beyond-pg supervisor` restarts it; WAL replay on start.                                                                                                 | Automatic.                                         |
| PgBouncer crash                                                  | `beyond-pg supervisor` restarts it; in-flight transactions error.                                                                                        | Automatic restart; client retries.                 |
| `beyond-pg supervisor` crash                                     | `paraglide-init` restarts it (1s delay). Supervisor re-runs boot setup (idempotent), respawns postgres+pgbouncer, re-runs post-start.                    | Automatic.                                         |
| `paraglide-init` crash                                           | Cannot. PID 1 crash is a kernel panic — Firecracker drops the VM. Box-manager rehomes from volume.                                                       | Box-manager.                                       |
| MMDS unreachable at first boot                                   | `beyond-pg supervisor` fails closed (no superuser password). `paraglide-init` restarts on backoff.                                                       | Automatic once MMDS is up.                         |
| Volume mount empty                                               | `beyond-pg supervisor` detects, runs initdb, applies config, creates extensions.                                                                         | Automatic — first boot path.                       |
| Volume mount has data, image upgraded                            | `beyond-pg supervisor` detects PG_VERSION, skips initdb, replaces `00-beyond.conf` and `01-tuning.conf`.                                                 | Automatic — image-swap path.                       |
| Major version bump (PG 18→19)                                    | `beyond-pg supervisor` detects PG_VERSION mismatch, exits with error. Maintenance VM runs `pg_upgrade`.                                                  | Out of image scope; control-plane operation.       |
| Sibling extension `.deb` missing at build                        | Packer build fails with explicit error.                                                                                                                  | Republish from sibling repo, rebuild image.        |
| `archive_command` target unreachable                             | WAL archive fails; `pg_wal` accumulates segments.                                                                                                        | Operator alert via metric; retry on next segment.  |
| GlideFS SSD utilization > 95 %                                   | Writes rejected with ENOSPC. Postgres panics; supervisor restarts; same condition persists.                                                              | Host-side capacity policy; out of image scope.     |
| Fork boots before parent's checkpoint completed                  | WAL replay from last checkpoint to fork timestamp. Slower boot.                                                                                          | Pre-fork CHECKPOINT hook (post-MVP).               |
| User sets bad value in `99-user.conf`                            | `beyond-pg supervisor` ignores `99-`; PG fails to start; supervisor logs and retries.                                                                    | User edits, calls `reload` over vsock RPC.         |
| Box-manager forgets to set `self_supervised: true` for our image | Box-manager injects `paraglide-agent`, shadowing our binary at `/usr/local/bin/paraglide-agent`. The injected agent has no Postgres knowledge and idles. | Operator sets the manifest flag and re-provisions. |

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

We add **one image, one bootstrap script, one tuning script, one
vsock daemon, four hook directories, two stub binaries**. Every
durability, replication, and fork mechanism is a Postgres or GlideFS
primitive that already exists.

The image is opinionated about what's bundled (the extensions modern
Postgres apps reach for) and minimal about what's invented (one
config snippet, one tuning rule, one bootstrap path).

Tier 2 (HA, quorum durability) ships without a new image — the seams
are in MVP. The control-plane spawns replica VMs, drops a config
snippet, and flips a SIGHUP. The image is ready for it on day one.

Same `psql localhost` everywhere.
