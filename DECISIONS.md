# Postgres Image — Decisions and Rationale

Companion to `DESIGN.md`. Where DESIGN.md says _what we're building_,
this document says _why we chose it over the alternatives_. Anyone
asking "why did we put WAL on the same volume?" or "why aren't we
shipping HA in MVP?" should find the answer here with full reasoning.

Decisions are grouped by area and tagged with stable IDs (e.g. `B-003`)
so PRs and code comments can reference them.

---

## A — Substrate, scope, and trajectory

### A-001 — Build substrate-native, not greenfield

**Decision.** The image is a thin layer over GlideFS, the guest-agent,
and Beyond's network. It introduces no new Beyond primitive.

**Why.** Beyond's whole bet is that branching at the substrate makes
every primitive above it inherit the property for free
(`beyond/PLATFORM.md`). Building a Postgres-specific storage layer,
log shipper, or fork coordinator would invert that — we'd be doing
work the substrate already does, badly. Postgres' own crash recovery

- GlideFS' crash-consistent block snapshots = working forks with no
  extra code.

**Alternatives considered.**

- _Build a Neon-style pageserver._ Rejected: GlideFS already provides
  the 80% of pageserver functionality that matters (CoW, S3 layering,
  local-SSD caching). The remaining 20% is quorum WAL durability,
  which Tier 2 sync replication delivers as a config flag.
- _Build a custom Postgres fork with disaggregated storage._ Rejected:
  ~person-year of engineering for marginal operational gain over
  warm-standby failover, plus permanent maintenance burden.

### A-002 — Tier 1 (single-instance) is MVP scope

**Decision.** MVP ships a single Postgres VM with vertically-scaled
sizing. No replicas. No automatic failover. No backup scheduler.

**Why.** Supabase's standard tier is single-instance + async replicas
(`supabase.com/docs/guides/platform/read-replicas`). RDS single-AZ is
single-instance. This is a known-acceptable bar for managed Postgres,
and shipping less first lets us validate the substrate-native model
before adding multi-VM coordination complexity.

**Cost.** Tier 1 durability is bounded by GlideFS' write-behind window
(~5 s / 64 MB on host loss). Documented user-facing. Tier 2 closes
this for production.

### A-003 — Tier 2 (HA) is designed-for, not-built

**Decision.** Tier 2 (sync replication, automatic failover, local-NVMe
WAL) is not implemented in MVP. Every seam needed to ship it lands in
MVP — see L-series.

**Why.** "Design for tomorrow today." Adding the seams costs ~100 LoC
in the bootstrap plus four config knobs that can't be changed without
a restart later (`wal_level`, `max_wal_senders`, etc.). Skipping them
would mean a forced restart on every existing database the day we
turn HA on. Paying the cost now is cheap; paying it later is
operationally hostile.

---

## B — Storage and durability

### B-001 — PostgreSQL 18

**Decision.** Latest stable major version (18) at the time of build.
Newer majors get a new image build, not an in-place upgrade.

**Why.** Latest. `beyond_auth` already supports both PG 17 and 18
(`auth/beyond-auth-extension/Dockerfile.cross`); extension ecosystem
has caught up.

**Alternatives.** PG 17 (rejected: deliberately shipping last-year's
software). Both 17 and 18 (rejected: maintenance multiplier on a tiny
team).

### B-002 — PGDATA at `/var/lib/postgresql/18/main`

**Decision.** PGDG / Debian idiom path.

**Why.** Every tool, every guide, every recovery procedure expects
this path. Inventing a custom path costs nothing functionally and
costs every user something operationally.

### B-003 — Single GlideFS data volume holds data + WAL (MVP)

**Decision.** PGDATA and `pg_wal` both live on one GlideFS-backed
volume (`vdb`) in MVP. No separate WAL volume.

**Why.** Forks atomically include WAL. The fork is a crash-consistent
block snapshot of `vdb`; Postgres' standard crash recovery replays
uncheckpointed WAL on the fork the same way it would after a power
loss. **No `pg_resetwal`, no quiesce, no pre-fork coordination
required for correctness.**

**Alternatives considered.**

- _Split: WAL on a second GlideFS volume, not snapshotted with data._
  Rejected: forks would arrive without WAL → fork must run
  `pg_resetwal` and reset to last checkpoint → loses uncheckpointed
  transactions on fork. Trade-off only worth it if WAL needs separate
  flush cadence (it doesn't, at MVP scale).
- _Split: WAL on local NVMe._ Rejected for MVP: host loss = total WAL
  loss = data loss in single-instance tier. Acceptable only when sync
  replicas hold the WAL too — i.e. Tier 2.
- _WAL on a non-GlideFS volume that's separately replicated._
  Rejected: requires a custom durability protocol Beyond doesn't
  have and would have to invent.

**Cost.** GlideFS write-behind ships WAL segments to S3 as packs.
For high-traffic workloads the per-byte S3 PUT cost is non-trivial
(`glidefs/README.md:329` notes ~8 PUTs/s/VM at 100 MB/s WAL). For dev,
preview, and branch databases — the dominant Beyond use case — WAL is
in KB/s and the cost is negligible. Tier 2 moves WAL to local NVMe
specifically to avoid this for prod-scale traffic.

### B-004 — `pg_wal` is a symlink, not a directory

**Decision.** `PGDATA/pg_wal → /var/lib/postgresql/18/wal` (a symlink).
The target is a directory on `vdb` in MVP; will become a `vdc` mount
in Tier 2.

**Why.** Relocating WAL from GlideFS to local NVMe in Tier 2 is a
symlink swap with no Postgres reconfiguration. Without this seam,
Tier 2 would require either (a) a custom Postgres patch, or (b) an
in-place data migration to move `pg_wal/` — both bad. The symlink is
the cheap, idiomatic Linux answer.

### B-005 — `wal_level = logical` from day one

**Decision.** Image ships with `wal_level = logical`, not `replica`
or `minimal`.

**Why.** `wal_level` cannot be raised without a Postgres restart.
Setting it to `logical` in MVP means we never have to bounce a
production primary later to enable: logical replication, CDC
consumers (Debezium etc.), zero-downtime tier upgrades, future
Beyond-internal features (e.g. `beyond_queue` event sourcing).

**Cost.** ~10 % more WAL volume than `replica`. At MVP traffic
levels, immaterial.

### B-006 — Production durability via Postgres-native WAL quorum, not storage-layer quorum

**Decision.** Quorum-durable WAL is achieved via Postgres-native
mechanisms (`pg_receivewal --synchronous` for Tier 1.5; full sync
replication for Tier 2), not via a custom storage protocol underneath
Postgres. GlideFS remains write-behind.

**Why.** The reason Aurora and Neon built custom storage was to get
CoW + S3 + local-SSD caching. GlideFS already provides those. The
remaining gap is WAL landing on a second failure domain before a
commit is acked. Postgres already solves that — we use it, we don't
reinvent it.

**Alternatives considered.**

- _GlideFS adds synchronous WAL replication._ GlideFS is a general
  block device; WAL semantics don't belong there. Wrong layer.
- _Aurora-style: WAL is the only thing the primary writes; data
  reconstructed from WAL by storage nodes._ Requires forking Postgres
  (custom `smgr`); buys sub-second failover over seconds; not worth a
  person-year of fork maintenance for the delta.
- _Neon-style: Safekeepers (Paxos WAL log) + Pageserver._ Pageserver's
  job is GlideFS' job. Safekeepers' job is what `pg_receivewal
  --synchronous` does, without a Postgres fork.

### B-006a — Tier 1.5: WAL sink before full replicas

**Decision.** The first production durability tier is a WAL sink —
a small VM running `pg_receivewal --synchronous` — not a full
streaming replica. Full replicas (Tier 2) come later when the
availability SLA requires fast failover.

**Why not jump straight to full replicas.**

A full replica carries a complete copy of the data plus a running
Postgres process. For durability alone, none of that is necessary.
What's needed is the WAL on a second host's disk before the commit
returns. `pg_receivewal --synchronous` does exactly that: it
connects via the streaming replication protocol, receives WAL records
as they're flushed, and sends acknowledgment back to the primary. The
primary counts it in `synchronous_standby_names`. Storage cost is
proportional to WAL retention, not database size.

**Recovery without box-manager involvement.** On host failure,
box-manager rehomes the VM and the GlideFS volume reattaches —
same as today. The only new step is the WAL gap fill. `beyond-pg
boot` reads `BEYOND_PG_WAL_SINK` from MMDS, checks `pg_controldata`
for the last valid WAL location, fetches any missing segments from
the sink's HTTP endpoint, places them in `pg_wal/`, and proceeds.
Postgres sees no WAL gap and recovers normally. Box-manager is
unchanged.

**What Tier 2 adds on top.** A full replica is promotable. When the
primary fails, the replica promotes and PgBouncer reroutes — seconds
of downtime, not minutes. Tier 2 also provides read scaling. Tier 1.5
provides neither; it only closes the data-loss window. The right
choice depends on the availability SLA, not the durability requirement.

### B-007 — Generous replication knobs in MVP

**Decision.** `max_wal_senders = 10`, `max_replication_slots = 10`,
`hot_standby = on`, `wal_keep_size = 1GB` are all set in MVP even
though no replicas exist.

**Why.** Each of these requires a Postgres restart to change. Adding
replicas later without restarting the primary requires them to be
already-set to sufficient values. Trivial cost; large operational
benefit.

### B-008 — `archive_mode = on` from day one with stub command

**Decision.** `archive_command = '/usr/local/bin/beyond-pg archive %p
%f'`. The subcommand no-ops (returns 0) when no
`BEYOND_PG_ARCHIVE_TARGET` is set in MMDS.

**Why.** `archive_mode` cannot be flipped from `off` to `on` without
restarting Postgres. Same argument as B-005, B-007: pay the cost in
MVP, never restart for this reason later. Tomorrow's PITR is a MMDS
config change, not an image rebuild.

### B-009b — Backups are GlideFS snapshots; the image doesn't ship a backup service

> Numbering note: this decision was added after B-009 was already
> stable in references; promoted to a `b` suffix to avoid renumbering
> rather than placed at B-010 (which is reserved for a future entry).

**Decision.** Beyond's backup story for this image is GlideFS
snapshots, scheduled by the control plane. The image ships
`archive_mode = on` + `archive_command` for sub-snapshot-interval
PITR (WAL segments shipped to S3 between snapshots) and a `backup`
vsock RPC that runs `pg_basebackup` on demand for the cases that
need it. There is no separate "backup service" in this repo.

**Why.** The substrate already does the hard part. GlideFS snapshots
are:

- Atomic at the block layer (taken under the write-cache rotation
  lock — `glidefs/src/block/write_cache/flush.rs:441-568`). Crash-
  consistent by construction; Postgres' standard recovery handles
  the rest. No `pg_start_backup`/`pg_stop_backup` coordination.
- CoW-cheap. Each snapshot ships only the diff against the previous
  to S3. `pg_basebackup` always re-ships everything.
- Restore = fork. The same primitive we use for branching. Sub-
  second to first query (with pre-fork CHECKPOINT — see
  Prerequisites in DESIGN.md).

What 90 % of managed-PG providers ship as "backup":

- Daily base backup → `pg_basebackup` to S3 _or_ a volume snapshot.
- WAL archive between backups → `archive_command` to S3.
- Restore → replay base + WAL.

Snapshot-based version is strictly cheaper, faster, and more
correct. Only thing missing is sub-snapshot-interval PITR, which
`archive_command` already covers. We do exactly the same thing as
RDS or Supabase, with the substrate doing more of the work.

**Alternatives considered.**

- _Build a separate "backup service" that runs `pg_basebackup` to
  S3 on a schedule._ Rejected: reinvents what GlideFS snapshots
  already do, with worse performance and worse atomicity guarantees.
- _Ship `pg_basebackup` as the only backup path, no snapshot
  integration._ Rejected: throws away the substrate's free
  primitive.

**What lives outside the image:**

- Snapshot scheduling cadence (hourly / daily / both). Tens of lines
  of control-plane glue, not a service.
- Snapshot retention policy. Same place.
- User-facing restore UX (`glide pg restore --to-time T`). CLI
  surface, post-MVP.

**What stays in the image:**

- `archive_command` wired to `beyond-pg archive`, which ships WAL
  segments to `BEYOND_PG_ARCHIVE_TARGET` (S3 path from MMDS).
- `backup` vsock RPC on the supervisor — runs `pg_basebackup` for
  the rare cases that need a logical-base-backup-shaped artifact
  (cross-major-version migration setup, logical replication
  initialization). Stub in MVP.

This is the substrate-thesis playing out for backup the same way it
does for storage CoW (we don't build a pageserver) and durability
(Tier 2 uses stock PG sync replication). The substrate already has
the primitive; the image inherits it.

### B-009 — Ephemerality is a substrate property; image inherits via MMDS flag

**Decision.** Preview, branch, and throwaway environments are marked
ephemeral at the GlideFS export level (`ephemeral = true`), owned by
the Beyond platform. The Postgres image reads
`BEYOND_VOLUME_EPHEMERAL` from MMDS and, when true, drops
`02-durability.conf` with `synchronous_commit = off`.

**Why.**

1. **Ephemerality is a property of the environment, not the database.**
   A preview deploy is throwaway end-to-end — its queue volume, auth
   volume, and Postgres volume are all disposable. Beyond marking the
   environment ephemeral propagates uniformly to every primitive on it.
   Postgres-specific opt-in would only solve a third of the problem.
2. **GlideFS already implements it.** Per-export `ephemeral = true`
   keeps writes local-SSD-only and never flushes to S3. Cost: zero S3
   storage, zero S3 PUT/GET. The substrate does the heavy lifting; the
   image just needs to behave well on top of it.
3. **Postgres responds with one config knob.** `synchronous_commit = off`
   makes commits async (5–10× faster), bounded by an ~10 ms data-loss
   window on Postgres crash. That window is irrelevant on an ephemeral
   volume — host loss already loses everything.

**Alternatives considered.**

- _Per-database Postgres toggle, no platform integration._ Rejected:
  Postgres can't tell GlideFS to skip S3, so the cost stays. And it
  doesn't compose — queue and auth on the same preview environment
  would still ship to S3.
- _`wal_level = minimal` on ephemeral._ Rejected: conflicts with B-005
  (logical decoding readiness from day one). Logical decoding has real
  value even on preview environments and the WAL volume difference is
  trivial when nothing flushes to S3 anyway.
- _Disable WAL entirely._ Not possible. WAL is load-bearing for
  crash recovery; without it, an unclean shutdown leaves the database
  unreadable, not stale.
- _`fsync = off`._ Rejected: risks database **corruption** (not just
  loss) on crash. A crash during a developer's debugging session
  shouldn't brick the DB and force a restore — should lose 10 ms at
  most.

**Cost / consequences.**

- Ephemeral volumes have no durability beyond the host. Documented
  user-facing as the explicit contract for preview/branch
  environments.
- `synchronous_commit = off` loses up to ~10 ms of acked transactions
  on a Postgres crash but never corrupts. Recovery is automatic.
- `full_page_writes` stays `on` even when ephemeral — local SSDs can
  still tear writes on power loss, and turning it off risks page
  corruption with no meaningful benefit on disposable storage.
- This composes with `BEYOND_PG_TIER` (L-003). The expected
  combinations: `single + ephemeral=true` (preview), `single +
  ephemeral=false` (production single-instance), `primary/replica +
  ephemeral=false` (production HA). `primary + ephemeral=true` is
  technically permitted but operationally meaningless — there's nothing
  to be HA about if the volume is throwaway.

---

## C — Availability

### C-001 — No HA in MVP

**Decision.** No automatic failover, no warm standby, no health-based
restart escalation in MVP.

**Why.** Supabase doesn't ship it either. RDS single-AZ doesn't.
Building HA correctly requires multi-VM coordination (placement,
replication topology management, split-brain prevention, promotion
arbitration) — the operational complexity is significant and benefits
only the production tier. MVP focuses on "is the substrate-native
single-instance model correct and ergonomic?"

### C-002 — Tier 1 availability falls out of substrate volume portability

**Decision.** Tier 1 host-failure recovery is "box-manager rehomes
the VM on a healthy host; volume reattaches; Postgres recovers."

**Why.** GlideFS volumes are host-independent
(`glidefs/ARCHITECTURE.md`). The substrate already solves
"compute can find data after a host dies." We don't need replicas to
solve that — we need replicas to solve "minimize _downtime_ during
that recovery." Replicas are a Tier 2 optimization on the downtime
budget, not a durability requirement.

**Cost.** Tier 1 RTO is minutes (host detection + VM rehome + Postgres
crash recovery). RPO is the GlideFS write-behind window (~5 s).

### C-003 — Tier 2 = warm-standby failover, not stateless-compute failover

**Decision.** Tier 2 promotes a warm standby on primary failure. The
standby has the data hot in shared_buffers and the WAL up to the last
quorum-acked LSN.

**Why.** Standard Postgres pattern. Patroni / repmgr orchestrate it.
Failover is seconds, not milliseconds, but doesn't require any custom
Postgres work. The gain from going further (Aurora-style stateless
compute) is small enough to not justify forking Postgres — see B-006.

---

## D — Scalability

### D-001 — Vertical scaling is the default lever

**Decision.** "Need more capacity" → resize the VM. Volume follows.

**Why.** Postgres scales vertically very well — up to ~256 GB / 64
vCPU covers ~95 % of users. Beyond's box-resize primitive handles
this in-place; volume portability means data follows automatically.
Vertical scaling has no consistency or query-shape implications;
horizontal scaling does. Default to the lever that doesn't affect
correctness.

### D-002 — Read replicas via async streaming replication, same image

**Decision.** Read replicas are the same image with
`BEYOND_PG_TIER=replica` and `primary_conninfo` from MMDS.

**Why.** Standard Postgres streaming replication. No new mechanism.
The bootstrap branches on the tier flag and drops a `03-replication.conf`
that points at the primary. Replicas are a separate pricing/scaling
unit but a single image build.

### D-003 — No horizontal write scaling (no sharding)

**Decision.** Sharding (Citus, Vitess-for-Postgres) is explicitly out
of scope for this image.

**Why.** Sharding is **a different product**. It requires a
coordinator, query rewriting, distributed transactions, and a reshape
of user schema design. The image's contract is "standard Postgres at
`localhost:5432`" — that contract breaks under sharding. If Beyond
ships sharding someday, it ships as a separate primitive that runs
Postgres VMs underneath (this image), not as a config flag on this
image.

### D-004 — No multi-master

**Decision.** Active-active replication topologies are not supported.

**Why.** Postgres doesn't natively do multi-master. Bolt-ons (BDR,
Bucardo) come with conflict-resolution semantics that confuse users
more than help them. The audience this image targets wants standard
Postgres.

---

## E — Connection layer

### E-001 — PgBouncer co-located on the Postgres VM

**Decision.** PgBouncer runs as a sibling process on the same VM as
Postgres. Listens on `0.0.0.0:5432`. Postgres listens on
`127.0.0.1:5433` and a Unix socket.

**Why.** This is the established norm for managed Postgres.
PlanetScale ships "Local PgBouncer" by default
(`planetscale.com/docs/postgres/connecting/pgbouncer`). Supabase
co-locates PgBouncer on every project. Connection pooling is so
universally needed that bundling it removes a foot-gun without
constraining anyone — direct port (5433) is one config away.

**Alternatives considered.**

- _PgBouncer as a separate VM (sidecar fleet)._ Rejected: extra
  network hop on every connection, extra failure mode, no benefit at
  Beyond's scale.
- _No PgBouncer, just Postgres._ Rejected: every user above ~50
  concurrent connections needs pooling; not bundling it pushes the
  problem onto every user.

### E-002 — Transaction pooling as default mode

**Decision.** PgBouncer mode = `transaction`. `default_pool_size = 20`,
`max_client_conn = 100`, `server_idle_timeout = 600`,
`server_lifetime = 3600`, `max_prepared_statements = 200`.

**Why.** PlanetScale's defaults verbatim. Transaction pooling is the
universal default for managed Postgres. Session pooling provides
weaker isolation (a connection holds state across transactions);
statement pooling breaks too many real workloads.

### E-003 — Direct port (5433) for transaction-pooling-incompatible operations

**Decision.** Postgres listens on `127.0.0.1:5433` separately from
PgBouncer's 5432. ETL, `pg_dump`, DDL, advisory locks, `LISTEN/NOTIFY`,
long transactions, session-pinned features → 5433.

**Why.** Transaction pooling can't preserve session-scoped state.
Forcing every operation through the pool would break migration
runners, dump tools, and replication tooling. The 5432/5433 split is
the standard convention; documented in user-facing docs.

---

## F — Configuration

### F-001 — Layered config: `00-beyond.conf`, `01-tuning.conf`, `02-durability.conf`, `03-replication.conf`, `99-user.conf`

**Decision.** `postgresql.conf` includes `conf.d/` (numerically
ordered). Beyond owns `00-` (image opinions), `01-` (RAM-derived
tuning), and `02-` (ephemeral-mode overrides — see B-009). Tier 2
owns `03-` (replication). User owns `99-`.

**Why.** Standard Postgres `include_dir` ordering: higher numbers
win. Clear ownership boundaries. Image swaps replace `00-` and
`01-`; user changes in `99-` survive every image upgrade and tier
transition. This is the same convention RDS, Crunchy, and Debian PG
all use.

### F-002 — `postgresql.conf` in `/etc` (rootfs), `conf.d/` in PGDATA (data volume)

**Decision.** The base `postgresql.conf` lives at
`/etc/postgresql/18/main/postgresql.conf` (PGDG idiom, on the rootfs).
The `include_dir` points at `/var/lib/postgresql/18/main/conf.d/` (on
the data volume).

**Why.** Two distinct lifecycles. Base config + extension SQL +
binaries belong to the image (rootfs, swappable). Tunables and user
overrides belong to the database (data volume, persistent). Splitting
this way means image upgrades pick up new defaults from the base
file, while user customizations and RAM-derived tuning survive the
upgrade because they're on the persistent volume.

### F-003 — Auto-tune from VM resources every boot, cgroup-aware

**Decision.** `beyond-pg supervisor` reads VM RAM and vCPU count at
every boot and writes `01-tuning.conf` with derived values. RAM is
read from `/sys/fs/cgroup/memory.max` first, falling back to
`/proc/meminfo`. vCPUs from `/sys/fs/cgroup/cpu.max` first, falling
back to `/proc/cpuinfo`.

Tuned values include: `shared_buffers`, `effective_cache_size`,
`maintenance_work_mem`, `work_mem`, `max_connections`, `wal_buffers`,
plus the vCPU-derived parallelism knobs (`max_worker_processes`,
`max_parallel_workers`, `max_parallel_workers_per_gather`,
`max_parallel_maintenance_workers`).

**Why.** Static defaults are wrong everywhere except one size. The
image runs on 4 GB / 2 vCPU dev boxes and 256 GB / 64 vCPU production
boxes from the same artifact. Resizing a box (`glide pg scale`) should
leave the database correctly tuned without a manual step. Regenerating
every boot makes that automatic.

**Why cgroup-aware.** Inside Docker for `beyond dev`, `/proc/meminfo`
reports host RAM, not the container's cgroup limit. If we set
`shared_buffers = 25 % of host RAM` against an 8 GB cgroup limit,
Postgres OOMs at startup. Reading the cgroup limit fixes this. In
Firecracker the VM's view is the actual VM RAM, so both paths give
the same answer.

**`work_mem` formula** (current):

```
work_mem = max(32MB, ram_mb / 2 / (pgbouncer.default_pool_size * 5))
```

Earlier formula (`ram * 0.01 / max_connections`) was too conservative
for the bundled extensions. On a 64 GB box at pool size 20 it produced
~7 MB — every pgvector/postgis/pg_search query would log temp-files
warnings on the workloads the image is bundled to support. The current
formula bounds peak query-workspace memory to ~50 % of RAM assuming
pool-size × 5 ops worth of concurrent hash/sort work. Floor 32 MB so
small boxes still get usable space.

**`max_connections` formula** (current):

```
max_connections = clamp(vcpus * 25, 100, ram_mb / 50)
```

Earlier formula (`pool_size * 2 + 50`) was fixed at 90 across all box
sizes. On a 64-vCPU box that left port 5433 (the direct-PG path docs
explicitly point ETL/migrations/parallel queries at) capped at ~40
slots. New formula scales with vCPUs (25 per core), floors at 100,
ceilings at `ram_mb / 50` (each connection costs ~10 MB process
overhead).

### F-004 — User overrides preserved across image swaps via `99-user.conf`

**Decision.** The image never touches `99-user.conf`. Bootstrap creates
it as an empty file on first init and never writes to it again.

**Why.** User trust. Anything we'd overwrite would be a footgun.
Anything they put in `99-user.conf` overrides our defaults because
of include order — and survives every image rebuild because it's on
the data volume.

### F-005 — Storage cost model tuned for SSD-class storage

**Decision.** `00-beyond.conf` sets:

```
random_page_cost = 1.1
effective_io_concurrency = 200
```

**Why.** PG defaults (`random_page_cost = 4`, `effective_io_concurrency = 1`)
assume rotational storage. They make the planner prefer sequential
scans over indexes on SSD-class storage and disable prefetch. GlideFS'
hot path is local-SSD-fast (random I/O ~10–50 µs). `1.1` is the
SSD-tuned community default. `200` is the standard prefetch setting
for any SSD-class storage; PG handles ~1000 concurrent I/Os easily.

Cold S3 backfills are bounded by S3 latency, but those are rare on
the steady-state hot path and the planner has no way to differentiate
"hot block on local SSD" from "cold block on S3" in its cost model
anyway. Optimize for the common case.

### F-006 — Defensive timeouts and connection liveness

**Decision.** `00-beyond.conf` sets:

```
idle_in_transaction_session_timeout = 5min
lock_timeout = 30s
tcp_keepalives_idle = 60
tcp_keepalives_interval = 10
tcp_keepalives_count = 6
```

**Why.** Idle-in-transaction is a real-world footgun: a misbehaving
client (network blip, debugger paused, SIGSTOP'd process) holds row
locks indefinitely and prevents vacuum. Auto-rolling back after 5 min
is a defense, not a guarantee. Long-running admin transactions can
override in `99-user.conf`.

`lock_timeout = 30s` makes DDL fail fast instead of blocking forever
behind a long transaction. The operator sees the failure and decides
what to do; current default (no timeout) makes DDL hang silently.

TCP keepalives detect dead connections within ~120 s. PG default `0`
means "use OS default," which is 7200 s (2 hours) on Linux. Without
keepalives, PgBouncer-to-Postgres connections through Beyond's private
network can stay open across VM reboots, network reconvergence, or
partial failures — pool fills with zombies that consume `max_connections`
slots until next use.

**`statement_timeout` left unset.** Different workloads have wildly
different reasonable upper bounds. PgBouncer's `query_wait_timeout`
provides per-pool backpressure instead.

### F-007 — Autovacuum tightened for write-heavy bundled workloads

**Decision.**

```
autovacuum_vacuum_scale_factor = 0.1
autovacuum_naptime = 30s
```

**Why.** PG defaults (0.2 / 1 min) are tuned for general use.
pgvector/postgis indexes get bloated quickly under heavy
insert/update workloads — the workloads the image is bundled to
support. Tighter scale factor triggers vacuum on smaller table
changes; shorter naptime means autovacuum re-evaluates more often.
Cost is a slight increase in background CPU; in exchange the
planner's stats stay fresh and the indexes stay tight.

### F-008 — Lock budget for `pg_partman`

**Decision.** `max_locks_per_transaction = 256` (default 64).

**Why.** Default 64 collides with `pg_partman` partition setups.
Monthly partitions over 5 years × multiple indexes per partition × a
cross-partition query and you're out. 256 is conservative headroom
at trivial per-connection memory cost (~~200 bytes per slot).

### F-009 — Kernel sysctls and disabled THP

**Decision.** Image installs `/etc/sysctl.d/99-postgres.conf`:

```
vm.swappiness = 10
vm.overcommit_memory = 2
```

And sets `transparent_hugepage=never` on the kernel cmdline at image
build time.

**Why `vm.swappiness = 10`.** Linux defaults to 60 — swaps out
application pages (including `shared_buffers`) under modest memory
pressure to grow the page cache. Backwards for a database:
`shared_buffers` _is_ the cache we want to keep. 10 keeps it in RAM
until things are genuinely tight.

**Why `vm.overcommit_memory = 2`.** PG community recommendation. With
overcommit on (default), the OOM killer chooses victims when memory
runs out — can mean killing the postmaster and dropping every
connection. Overcommit = 2 makes allocations fail at malloc time
beyond `swap + (RAM × overcommit_ratio / 100)`, which Postgres handles
gracefully (returns ERROR, keeps running).

**Why `transparent_hugepage=never`.** THP's `khugepaged` daemon
periodically defragments memory by promoting 4 KB pages into 2 MB
pages. Causes unpredictable latency spikes on busy databases —
well-known PG footgun. Disable it. Use explicit huge pages
(`vm.nr_hugepages` + `huge_pages = on`) for `shared_buffers` if we
want them; PG default `huge_pages = try` is fine without explicit
setup.

---

## G — Bootstrap

### G-001 — Idempotent every-boot setup via `beyond-pg boot`

**Decision.** `beyond-pg boot` runs once at every boot as a guest-agent
startup hook before `postgres` starts. Idempotent: the same volume
booted ten times produces the same result as booted once. Detects
`PGDATA/PG_VERSION` to decide whether to run `initdb`; either way, it
refreshes `00-beyond.conf`, `01-tuning.conf`, the `pg_wal` symlink
target, and the ephemerality config.

**Why.** Beyond's substrate principle: all state-modifying operations
must be idempotent and atomic (`postgres/CLAUDE.md`). The same volume
might be rebooted, image-swapped, forked, or restored from backup —
all of which produce a "PGDATA already exists" boot. Idempotence is
correctness here, not just hygiene. Running every boot also means a
resized VM picks up new tuning automatically.

### G-002 — MMDS for superuser password and tier configuration

**Decision.** `beyond-pg` reads `POSTGRES_PASSWORD`,
`POSTGRES_DATABASE`, `BEYOND_PG_TIER`, `BEYOND_VOLUME_EPHEMERAL`,
`BEYOND_PG_ARCHIVE_TARGET` directly from MMDS at
`169.254.169.254/latest/meta-data/` at startup, with env var fallback
for local dev (Docker, direct invocation).

**Why.** MMDS is how Firecracker delivers secrets and config to guests
without baking them into the image. Direct read — rather than having
an outer init write them to `/etc/environment` first — keeps the code
path simple: one binary, one MMDS read, done. Env var fallback means
the same binary works locally without MMDS infrastructure.

**Alternatives considered.**

- _Generate a password on first boot, store on volume._ Rejected:
  Beyond can't surface the password without round-tripping through
  the volume; MMDS is the existing channel.
- _Env vars only (no MMDS)._ Rejected: doesn't compose with Firecracker
  boot where secrets aren't baked into the image or environment block.
- _Have paraglide-init write MMDS data to `/etc/environment`; read from
  there._ Rejected: ties the image to a platform binary. Env var fallback
  achieves the same local-dev composability without the dependency.

### G-003 — `CREATE EXTENSION` runs every boot (idempotent)

**Decision.** `beyond-pg supervisor`, after Postgres is healthy, runs
`CREATE EXTENSION IF NOT EXISTS` for every preloaded extension. Each
call is a no-op if the extension is already installed.

**Why.** New extensions added to the image (e.g. an upgrade adds
`pgvectorscale`) get installed automatically on the next boot of an
existing volume. Without this, extension upgrades would require a
manual user step.

### G-004 — `beyond-pg` as PID 1

**Decision.** `beyond-pg` is PID 1. `/sbin/init` symlinks to
`/usr/local/bin/beyond-pg`. The binary handles zombie reaping, signal
handling, network configuration, MMDS reading, and Postgres supervision
in a single self-contained process. No intermediate init binary.

**Why.**

1. **Composability.** `beyond-pg` reads MMDS directly with env var
   fallback. The same binary works in Firecracker (`psql localhost`
   with MMDS config) and in Docker (`docker run beyond-postgres` with
   env vars). A two-tier model where an outer init writes MMDS data to
   `/etc/environment` would require a platform binary in every local dev
   environment. That's the wrong abstraction boundary.

2. **Open-source legibility.** This is a public repo. The boot path —
   `kernel → /sbin/init → beyond-pg → postgres` — is self-contained.
   Anyone can understand how the image boots without knowing Beyond's
   internal toolchain. A binary named `paraglide-init` in `/sbin/init`
   requires context that no external reader has.

3. **Minimum effective abstraction.** A two-tier model (outer init +
   inner binary) would save ~200 lines of init code at the cost of:
   - A binary dependency on a Beyond-internal `paraglide-init`
   - An extra process slot per VM
   - Inter-process coordination (MMDS data from outer tier → inner tier)
   - A Postgres image that only works on Beyond's platform
     Not the right trade for a database image that must also run locally.

4. **`paraglide-agent` is the wrong inner tier.** It carries ~20 kLoC
   of features for the user-app loop (file watching, rsync, MCP,
   lifecycle phases, PTY). None of it applies to a database. We'd carry
   dead weight to use 200 lines of supervision logic.

**`beyond-pg` binary layout.**

Three callable subcommands:

- `supervisor` — the long-running process (runs as PID 1; also callable
  for debugging). Init responsibilities + Postgres supervision.
- `boot` — boot-time setup exposed as a standalone subcommand for ops
  re-execution. Called inline by `supervisor`.
- `archive %p %f` — per-WAL hook invoked by `archive_command`.

**Process count at runtime.**

`beyond-pg` (PID 1), `postgres`, `pgbouncer`. Three processes. Four
if a backup is running.

**Alternatives considered.**

- _Two-tier: `paraglide-init` outer (PID 1) + `beyond-pg supervisor`
  inner._ Rejected: see reasons 1–3 above. The two-tier model existed
  to reuse Beyond's generic init work; the cost (platform binary in
  image, no local-dev composability, abstraction leak) outweighs the
  benefit (~200 saved lines of Rust).
- _`paraglide-agent` as the inner tier, unmodified._ Rejected: ~20 kLoC
  of user-app features, none of which applies to a database. File
  watching during `initdb` is a footgun.
- _systemd as PID 1._ Rejected: no other Beyond image runs systemd.
  Diverges from the operational model operators already understand.
- _Five separate binaries (one per concern) instead of subcommands._
  Rejected: each would be its own deploy unit, version surface, and
  place for shared logic to drift.

**Consequences.**

- `beyond-pg` is ~400 lines longer than it would be without init
  responsibilities (zombie reap, signal handling, mount setup, network
  config). Worth it for composability and legibility.
- Box-manager injects the standard `paraglide-init` and `paraglide-agent`
  into the rootfs (as it does for all images), but neither is started.
  `/sbin/init` points to `beyond-pg`. Content-addressed blocks are
  shared; storage cost is zero.
- No `.service` files, no custom init in the image beyond the symlink.
- One Rust crate (`src/`). Subcommand dispatch in `main.rs`.
  `supervisor.rs` is the long-running entry; everything else is library
  code it calls into.

---

## H — Logging

### H-001 — vsock log forwarding with local-dev pass-through fallback

**Decision.** `beyond-pg` spawns postgres and pgbouncer with piped
stderr, reads lines via async Tokio tasks, and forwards them over vsock
as `UserProcessStreamData` frames — the same wire format and rate-limit
parameters as `paraglide-agent`'s `log_forwarder.rs` (500 lines/sec
sustained, 1000 burst, truncation at `MAX_USER_PROCESS_LINE_BYTES`).

Log shipping mode is **auto-detected at startup**: if vsock connects
successfully, pipe-and-forward is enabled; if the connection fails (no
`/dev/vsock`, Docker, direct invocation), child stderr is inherited
directly and the terminal / `docker logs` captures it.
`BEYOND_LOG_VSOCK=false` forces pass-through even in Firecracker.

**Why vsock (not serial console).** Box-manager does not read the
Firecracker serial console for application logs — it reads exclusively
from vsock (`box-manager/src/vsock/connection/message_handler.rs`).
Serial console output goes to a Firecracker-internal log file and is
not forwarded to the Beyond log pipeline. Vsock is the only path logs
can take to reach the host.

**Why auto-detect (not a required flag).** The binary must work in
Docker for local dev without any Beyond infrastructure. A hard
vsock dependency would break `docker run beyond-postgres`. Auto-detect
with pass-through fallback gives composability without giving up proper
Beyond integration.

**Why match `paraglide-agent` wire format exactly.** Box-manager's
`UserProcessStreamData` handler emits structured log events (`box.log`
tracing target) with full context fields. Matching the format means the
host receiver needs no changes — the Postgres image plugs into the
existing log pipeline.

**Alternatives considered.**

- _Serial console only._ Rejected: box-manager does not capture it for
  application logs. Logs would be silently lost in production.
- _Log files in PGDATA._ Rejected: fork with every branch; bloat every
  snapshot.
- _journald._ Rejected: adds a daemon and socket; not present on the
  rootfs; unnecessary given vsock already covers the use case.

### H-002 — No `logging_collector`, no log files

**Decision.** `logging_collector = off`. No log files written
anywhere.

**Why.** Log files in PGDATA would fork with every branch and bloat
snapshots. Log files outside PGDATA wouldn't be captured by
guest-agent's pipe-based log capture. Stderr is the only path that
satisfies both constraints.

---

## I — Security

### I-001 — TLS on by default with auto-provisioned self-signed cert

**Decision.** `ssl = on` in `00-beyond.conf`. `beyond-pg supervisor`
generates a self-signed cert at first boot under
`PGDATA/beyond/server.{crt,key}` and regenerates it within 30 days
of expiry. Cert lives on the data volume so it forks with the
database identity. PgBouncer terminates client TLS on port 5432
using the same cert; PgBouncer→PG runs over the unix socket
(no TLS needed).

**Why.** Earlier sketch was `ssl = off`, reasoning that "Beyond's
network is the perimeter (mTLS tunnel, private VXLAN, eBPF)." That's
correct for VM-to-VM but the user-facing port 5432 is the connection
an app makes — defense in depth at the wire is nearly free if we
auto-provision the cert. Once we ship TLS-off, turning it on later
is a breaking change for some clients (driver `sslmode` defaults
vary). Pay the cost in MVP, never break an upgrade for it.

**Why self-signed and not real CA.** A real CA chain (Let's Encrypt
via ACME, or a Beyond-internal CA) is its own infrastructure.
MVP's floor is "the wire is encrypted, even if the cert isn't
chain-validated." Customers with strict CA-chain requirements
override `ssl_cert_file` / `ssl_key_file` in `99-user.conf` and
supply their own. The auto-provisioned cert is the floor, not the
ceiling.

**Alternatives considered.**

- _Keep `ssl = off`, document `99-user.conf` override._ Rejected:
  shifts cert provisioning to every customer. Most won't bother and
  ship plaintext. Also turning on later breaks some clients.
- _Real CA via ACME / internal CA at first boot._ Rejected for MVP:
  requires a public-DNS-resolvable hostname for ACME, or a
  Beyond-internal CA infrastructure that doesn't exist yet. Both
  worth doing later; not blockers for shipping.
- _PgBouncer-only TLS, plaintext PG._ Rejected: clients hitting the
  direct port 5433 (ETL, dumps) get plaintext. Want consistent
  posture across both ports.

**Cost / consequences.**

- ~30–40 lines in `boot.rs` for cert generation (using `rcgen` crate
  or shelling out to `openssl`).
- Cert renewal is idempotent in `boot.rs`: if cert exists and is
  outside the 30-day-to-expiry window, leave alone; otherwise
  regenerate and `pg_ctl reload`.
- `client_encoding`-style cert mode (`sslmode=prefer` clients accept
  the cert; `sslmode=verify-full` clients reject the self-signed cert
  and need the override path). Document this in user-facing
  connection examples.
- Self-signed certs do not satisfy compliance regimes that require
  CA-chain validation (HIPAA tech-implementation, PCI). Documented
  user-facing as a known limit; real CA is the compliance path.

### I-002 — `scram-sha-256` for password auth

**Decision.** `pg_hba.conf` defaults to `scram-sha-256` for all
network access. Unix socket uses `peer`.

**Why.** `scram-sha-256` is the modern Postgres default (PG 14+
default). `md5` is legacy; `trust` would be wrong. `peer` on the
Unix socket is the standard local-admin convention.

### I-003 — Vsock for control plane RPC, not network

**Decision.** `beyond-pg supervisor` listens on a vsock port for
control RPC, not a network port.

**Why.** Vsock is host-local: only the host's box-manager can reach
it, never another VM, never the network. No auth needed, no firewall
hole, no surface for credential theft. This is how every other Beyond
guest agent talks to its host.

---

## J — Extensions

### J-001 — Extension set: pgvector, pgvectorscale, pg_trgm, postgis, pg_cron, pg_partman, pg_jsonschema, hypopg, pg_repack, pg_search, pg_stat_statements, auto_explain, beyond_auth, beyond_queue

**Decision.** Above set ships in the image and is auto-installed via
`CREATE EXTENSION IF NOT EXISTS` on every boot.

**Why each.**

- `pgvector` + `pgvectorscale` — vector workloads are a 2026 baseline
  expectation. Scale is the StreamingDiskANN accelerator; meaningful
  perf at >1M vectors. Bundling both means users don't have to pick.
- `postgis` — see J-003.
- `pg_cron` — in-DB scheduled jobs without an external scheduler.
- `pg_partman` — partition management; required for any large table.
- `pg_jsonschema` — schema validation on JSONB columns; common ask.
- `hypopg` — hypothetical indexes for `EXPLAIN`-driven design work.
- `pg_repack` — online table reorg without long locks.
- `pg_trgm` — fuzzy search; cheap to ship.
- `pg_search` — BM25 full-text via ParadeDB; covers the search
  workload `pg_trgm` doesn't.
- `pg_stat_statements` — query stats; non-negotiable for any prod DB.
- `auto_explain` — slow-query plans; "how did this run?" debugging
  primitive. Added by us, not on the user's original list — this is
  indispensable in production and costs nothing.
- `beyond_auth` — Beyond's authz BFS extension, required for the
  Auth primitive (`auth/beyond-auth-extension`).
- `beyond_queue` — Beyond's queue/workflow extension
  (`queue/beyond-queue-extension`).

### J-002 — `shared_preload_libraries` order

**Decision.** `pg_stat_statements, auto_explain, pg_cron, beyond_auth,
beyond_queue`.

**Why.** Functional order doesn't matter much for these (none of them
hook each other); stable order prevents config churn across image
rebuilds. Listed in roughly "observability first, then features"
order for readability.

### J-003 — PostGIS shipped despite ~250 MB cost

**Decision.** PostGIS + libgeos + libproj + libgdal are in every
image, regardless of whether the user uses them.

**Why.** GlideFS' rootfs is content-addressed. PostGIS' blocks are
stored once globally in S3 and shared across every Postgres VM in
the fleet. Cold blocks (which most PostGIS regions are for non-GIS
users) are never demand-fetched. The marginal cost is paid once at
image build, never per-VM. Effectively free.

**Alternatives considered.**

- _Ship a no-PostGIS image and a PostGIS image._ Rejected: doubles
  the build matrix, doubles the user confusion ("which one do I
  pick?"), provides no actual savings due to content-addressing.

### J-004 — Sibling extensions consumed as `.deb` from S3

**Decision.** `beyond_auth` and `beyond_queue` are pulled into the
image at build time as versioned `.deb` packages from
`s3://beyond-extensions/{auth,queue}/{version}/{arch}/*.deb`,
published by the sibling repos' release pipelines.

**Why.** Decouples release cadence. The sibling repos can ship new
versions without coordinating with the Postgres image; the Postgres
image can rebuild without coordinating with the sibling repos. The
image specifies pinned versions in `extensions.toml`; build fails
fast if pinned version isn't in S3.

**Alternatives considered.**

- _Build sibling extensions in-tree during Packer build._ Rejected:
  drags the pgrx toolchain into the image build (slow), couples
  build infrastructure (every sibling repo change can break the
  Postgres image build), violates separation of concerns.
- _Path dependencies in Cargo._ Same problems as above plus binds
  the build to a monorepo layout we don't have.

### J-004a — Sibling extensions downloaded from GitHub Releases (supersedes J-004)

**Decision.** `beyond_auth` and `beyond_queue` are fetched at Packer build
time as pre-built `.deb` packages from their GitHub Release assets, pinned
by tag. `extensions.toml` stores the GitHub repo URL and tag for each.
Build fails if the release asset is absent.

**Why.** S3-based distribution (J-004) leaks internal infrastructure and
requires AWS credentials at image build time. GitHub Releases are public,
credential-free, and auditable — the artifact URL is derivable from the
repo URL and tag alone.

**Why not build from source (git clone + cargo pgrx).** Building from
source drags the pgrx toolchain and a full `cargo build` into the Packer
container (minutes of compile time, cargo registry fetches, cargo-pgrx
version-matching against the extension's Cargo.toml). Downloading a
pre-built `.deb` is seconds and needs only `curl`. The sibling repos'
release pipelines own the build; the Postgres image only consumes the
artifact. J-004's original "decouple release cadence" rationale stands —
we just use a public artifact host instead of internal S3.

---

## K — Build pipeline

### K-001 — Mirror `beyond/packer` exactly

**Decision.** Same Packer + Docker → ext4 → tier sizing → bless
flow as the rootfs build. Same `mise.toml` shape. Same script naming
convention (`01-...`, `02-...`, `post-process.sh`). Same publish-to-S3

- NATS-fleet-sync flow.

**Why.** The Beyond ops surface already exists for the rootfs
pipeline. Operators know how to debug it. Reusing the shape means
zero new operational knowledge to ship a new image. Every divergence
would be a new failure mode to learn.

### K-002 — Reuse existing tiered blank ext4 volumes for data

**Decision.** Data volumes use the existing `image:build-volume-blanks`
mise task; no new builder. `initdb` runs into the empty ext4 on first
boot.

**Why.** A blank ext4 volume is a blank ext4 volume. Building a
"Postgres-specific" blank (e.g. with PGDATA pre-seeded) buys nothing
the bootstrap doesn't already do, and adds another SKU to manage.

### K-003 — Image versioned with git SHA; pin every extension version

**Decision.** Image filename: `postgres-noble-{git_sha}.img`. An
`extensions.toml` at the repo root pins exact versions of **every**
extension shipped in the image — bundled (pg_stat_statements, pg_trgm),
PGDG (pgvector, postgis, pg_cron, etc.), ParadeDB (pg_search), and
sibling Beyond .debs (beyond_auth, beyond_queue). The Packer build
runs `apt install postgresql-18-<name>=<version>` for PGDG packages
and fails fast if any pinned version is unavailable.

**Why pin everything.** Earlier sketch only pinned the sibling .debs
and let PGDG packages float. That's a footgun: pgvector 0.7 → 0.8
introduced an index format break (real, not hypothetical). A
no-code-change image rebuild that pulls a different pgvector version
silently changes index behavior. Pin everything or pin nothing —
the split is the worst of both.

**Cost.** PGDG removes old versions over time, so a rebuild from a
historical SHA may fail if the pinned version has aged out. Two
mitigations available, both out of MVP:
(a) Mirror the apt repo to an internal S3 bucket at first publish,
so we control retention.
(b) Document "rebuild from old SHA may fail; bump pins or use the
archived .img" as the operator procedure.

(b) is acceptable for MVP. (a) is the durable answer for a serious
managed service and tracked separately.

**Bumping a pin** = a new image build = an image-swap rollout against
existing volumes. Standard ops procedure.

---

## L — Extensibility seams

These exist in MVP. They're cheap to add now and operationally
hostile to retrofit. Each unlocks one or more trajectory commitments
(D = durability, A = availability, S = scalability).

### L-001 — Vsock RPC surface in `beyond-pg supervisor` — A

**Decision.** `beyond-pg supervisor` listens on a vsock port for
control-plane RPCs. MVP commands: `checkpoint`, `health`, `reload`,
`backup`.

**Why.** The control plane needs an in-VM RPC surface to drive Tier 2
operations: pre-fork CHECKPOINT, replica promotion, sync-standby
configuration changes, graceful drain. Building this surface in MVP
(even with a tiny command set) means future commands grow the set
additively, without an image rebuild for the wire format. Folding it
into `supervisor` (rather than a separate daemon) means the RPC
handler can act directly on the supervised children — restart
postgres, drain pgbouncer, etc. — without inter-process coordination.

### L-002 — Hook directories — A, D

**Decision.** `pre-start.d/`, `post-start.d/`, `pre-stop.d/`,
`pre-fork.d/`. `beyond-pg supervisor` runs scripts in each directory
at the corresponding lifecycle point. Empty in MVP except for the
post-start `CREATE EXTENSION` pass (which lives in the binary, not
in a hook script).

**Why.** Future tier-specific behavior (replica `standby.signal`
setup, pre-fork CHECKPOINT, post-promotion PgBouncer rewiring,
graceful drain on failover) lands as drop-in scripts without
modifying the binary. Same pattern as `/etc/cron.daily/`,
`/etc/network/if-pre-up.d/`, etc. Linux convention.

### L-003 — `BEYOND_PG_TIER` MMDS flag — D, A, S

**Decision.** A single MMDS field, `BEYOND_PG_TIER`, dispatches every
tier-specific code path. Valid values today: `single`. Future:
`primary`, `replica`.

**Why.** One flag, one branch point. `beyond-pg supervisor` inspects
it once and dispatches. No tier-specific files in the image; no
parallel code paths until they're needed; no "what tier am I?"
detection logic.

### L-004 — `archive_command` always wired (B-008)

Already covered in B-008. Tagged D for indexing.

### L-005 — Backup over vsock RPC — D

**Decision.** Backup is a `backup` vsock RPC handled inside
`beyond-pg supervisor`. Stub implementation in MVP.

**Why.** When Beyond's backup service is built, it calls the `backup`
RPC on the supervisor; the supervisor runs `pg_basebackup` against
the local Postgres and ships the result via the
`BEYOND_PG_ARCHIVE_TARGET` configured in MMDS. The image is already
wired; the handler gets a real implementation; no image rebuild
required. No separate `beyond-pg backup` subcommand because there's
no external invoker that needs a CLI surface.

---

## Cross-cutting principles

A few decisions don't fit cleanly into one area but recur as the
reasoning behind several:

### P-001 — Pay irreversible costs in MVP

`wal_level = logical` (B-005), generous replication knobs (B-007),
`archive_mode = on` (B-008), and the `pg_wal` symlink (B-004) all
share a pattern: they're **operationally hostile to change later**
(require a primary restart) and **trivially cheap to set in MVP**
(config tweaks). Whenever a setting falls into both categories, MVP
sets it.

### P-002 — Mirror the substrate's existing patterns where they fit

Same MMDS pattern as the rootfs (G-002). Same Packer pipeline as
`beyond/packer` (K-001). Same scripts directory layout (K-001). Same
`mise.toml` task shape (K-001). Operators already know how to operate
Beyond; the Postgres image doesn't ask them to learn anything new.

**Where we deliberately diverge:**

- _PID 1 (G-004)._ User-app images use `paraglide-init` as PID 1 with
  guest-agent in the inner slot. We use `beyond-pg` as PID 1 directly.
  Composability and open-source legibility outweigh the saved init code.
- _Log shipping mode (H-001)._ We auto-detect vsock availability rather
  than requiring it. Same wire format as guest-agent when vsock is
  present; pass-through to stderr when it isn't (local dev, Docker).

### P-003 — The wire protocol is the SDK

No Beyond-specific Postgres library, no proprietary RPC, no ORM
adapter. `psql localhost:5432` works. `pg.connect("postgres://...")`
works. Every tool in the Postgres ecosystem works. Adding a Beyond
SDK would be a _worse_ surface than the wire protocol — see DESIGN.md
"Trade-offs we're choosing."

### P-004 — Substrate primitives compose; we don't reinvent them

GlideFS does CoW + S3 + caching; we don't build a pageserver.
Postgres does sync replication; we don't build a Safekeeper.
Box-manager does volume rehoming; we don't build a failover
orchestrator. Guest-agent does log shipping; we don't build journald
integration. Every "why didn't we build X?" question boils down to
"because Beyond already has Y, and Y composes." This is the core
substrate-thesis playing out at the Postgres-image layer.
