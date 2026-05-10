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

### B-006 — Tier 2 durability via Postgres sync replication (not storage-layer quorum)

**Decision.** When Tier 2 ships, durability comes from
`synchronous_commit = remote_write` + `synchronous_standby_names =
'ANY 1 (r1, r2)'` — a Postgres-native primitive — not from a custom
storage protocol underneath Postgres.

**Why.** Three reasons.

1. **Beyond's substrate hands us multi-VM as a primitive.** Spinning
   up replica VMs is a 200 ms control-plane op against an existing
   primitive. Sync replication just plugs into that.
2. **Standard Postgres replication is battle-tested.** Two decades of
   production use, every monitoring tool understands it, every DBA
   knows how to debug it. Custom storage protocols don't have this.
3. **GlideFS does the storage-layer-CoW work already.** The reason
   Aurora and Neon built custom storage was to get CoW + S3 + local
   caching. We have those. The remaining gap is quorum-durable WAL,
   which is exactly what sync replication provides.

**Alternatives considered.**

- _Aurora-style: WAL is the only thing the primary writes; data
  reconstructed from WAL by storage nodes._ Rejected: requires
  forking Postgres (custom `smgr`); buys stateless-compute failover
  (sub-second) over warm-standby failover (seconds); not worth a
  person-year of fork maintenance for the operational delta.
- _Neon-style: Safekeepers (Paxos WAL log) + Pageserver._ Rejected:
  same reasoning — Pageserver's job is GlideFS' job already, and
  Safekeepers' job is sync replication's job already.
- _GlideFS adds a "durable WAL" mode._ Out of scope. Possible
  long-term GlideFS work but not on the critical path.

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

### F-003 — Auto-tune from MMDS RAM at every boot

**Decision.** A one-shot service reads VM RAM size from MMDS, writes
`shared_buffers`, `effective_cache_size`, `work_mem`,
`maintenance_work_mem`, `wal_buffers`, `max_connections` to
`01-tuning.conf`. Runs on every boot.

**Why.** Every tier ships the same image but at different sizes
(16 GB box, 64 GB box, 256 GB box). Static defaults are wrong
everywhere except one size. RAM-derived tuning gives sane settings
at every tier with zero user input.

**Why every boot, not just first boot.** Because users resize
boxes — `glide pg scale --memory 64gb` should leave the database
correctly tuned for the new RAM without a manual step.

### F-004 — User overrides preserved across image swaps via `99-user.conf`

**Decision.** The image never touches `99-user.conf`. Bootstrap creates
it as an empty file on first init and never writes to it again.

**Why.** User trust. Anything we'd overwrite would be a footgun.
Anything they put in `99-user.conf` overrides our defaults because
of include order — and survives every image rebuild because it's on
the data volume.

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

**Decision.** `beyond-pg boot` reads `POSTGRES_PASSWORD`,
`POSTGRES_DATABASE`, `BEYOND_PG_TIER`, `BEYOND_VOLUME_EPHEMERAL`,
`BEYOND_PG_ARCHIVE_TARGET` from MMDS.

**Why.** Parity with the rootfs MMDS pattern
(`beyond/packer/scripts/05-mmds.sh`). Beyond already has this
primitive; reusing it costs nothing and keeps the operational model
consistent.

**Alternatives considered.**

- _Generate a password on first boot, store on volume._ Rejected:
  Beyond can't surface the password without round-tripping through
  the volume; MMDS is the existing channel.
- _Environment variables._ Rejected: doesn't compose with guest-agent
  restarts cleanly; Beyond's existing pattern is MMDS.

### G-003 — `CREATE EXTENSION` runs every boot (idempotent)

**Decision.** `beyond-pg supervisor`, after Postgres is healthy, runs
`CREATE EXTENSION IF NOT EXISTS` for every preloaded extension. Each
call is a no-op if the extension is already installed.

**Why.** New extensions added to the image (e.g. an upgrade adds
`pgvectorscale`) get installed automatically on the next boot of an
existing volume. Without this, extension upgrades would require a
manual user step.

### G-004 — Two-tier supervision: `paraglide-init` outer, our binary inner

**Decision.** The Postgres image reuses Beyond's existing two-tier
supervision shape but slots its own binary into the inner tier:

- **Outer tier — `paraglide-init`** (Beyond's generic init,
  unchanged). PID 1. Mounts virtual filesystems, configures network,
  fetches MMDS, sets up zram, reaps zombies, powers off cleanly.
  Supervises one child at `/usr/local/bin/paraglide-agent` with
  restart-on-crash and a 10-second SIGTERM drain. Zero
  Postgres-specific code.
- **Inner tier — `beyond-pg supervisor`** (our binary, ships in this
  image at `/usr/local/bin/paraglide-agent`). Runs boot-time setup
  (initdb if needed, drop config, regenerate tuning, etc.), spawns
  and supervises postgres + pgbouncer with restart-on-crash,
  forwards their stdio over vsock to the host log pipeline, runs
  CREATE EXTENSION post-start, listens on vsock for control-plane
  RPC.
- **`beyond-pg` binary** has three callable subcommands: `supervisor`
  (the long-running everything daemon), `boot` (boot-time setup
  exposed for ops re-execution; called inline by `supervisor`),
  `archive` (per-WAL hook invoked by Postgres' `archive_command`).
  Backup is a vsock RPC handled inside `supervisor`.

**Why.**

1. **`paraglide-init` was already designed for this.** Its contract
   is "supervise one child, restart on crash, drain on shutdown."
   That is exactly the contract the inner tier needs. We slot in.
   Zero changes to `paraglide-init`.
2. **`paraglide-agent` is the wrong inner tier for this image.**
   It carries ~20 kLoC of features built for the user-app loop
   (file change detection, mirror bridge for rsync-from-host, MCP
   server, lifecycle phases for setup/build/start, workload
   watcher, PTY support, checkpoint scanner, exec-from-host). None
   of it applies to a database. We'd carry the weight to use 200
   lines of supervision logic.
3. **No Postgres knowledge in Beyond's primitives.** `paraglide-
   init` stays generic. `paraglide-agent` is unmodified for user
   apps. Box-manager doesn't grow Postgres awareness. The vsock
   protocol doesn't grow Postgres tasks. Database-specific behavior
   is entirely confined to `/usr/local/bin/paraglide-agent` on this
   image — and that's our binary, not Beyond's.
4. **One binary, one thing to ship.** Three subcommands instead of
   five separate binaries. Standard pattern for this shape
   (`kubelet`, `git`, `cargo`).

**Alternatives considered.**

- _Use `paraglide-agent` unmodified plus a control plane that pushes
  task definitions over vsock at boot._ Rejected: requires either
  (a) a Postgres-specific service in the Beyond control plane that
  knows what tasks to send to a Postgres VM, or (b) extending
  `paraglide-agent` with a "read tasks from local manifest" feature.
  Both push Postgres knowledge into shared infrastructure. The image
  itself is the right place for image-specific knowledge.
- _Extend `paraglide-agent` with the inner-tier behavior we need._
  Rejected: we'd be carrying 20× the code we use. Forking it would
  burden us with maintenance against upstream. Most of its features
  conflict with database semantics (file watching during `initdb`
  is a footgun).
- _systemd as PID 1._ Rejected: no other Beyond image runs systemd.
  Diverges from the supervision model operators already understand.
- _Run `beyond-pg` as PID 1 directly, replace `paraglide-init`
  too._ Rejected: reinvents zombie reaping, signal handling, MMDS
  fetching, network setup, zram. Beyond already has a battle-tested
  PID 1; we use it.
- _Five separate binaries (one per concern) instead of subcommands._
  Rejected: each was its own deploy unit, version surface, and place
  for shared logic to drift.

**Box-manager prerequisite.**

`paraglide-agent` is normally injected into VMs by box-manager via a
GlideFS derived snapshot. For this image that injection would shadow
our binary at `/usr/local/bin/paraglide-agent` and break us.
Box-manager needs a generic capability: **skip agent injection when
the image manifest declares `self_supervised: true`**. Not
Postgres-specific — every official image (queue, auth, kv) wants
the same behavior. Out of this image's scope but a prerequisite to
running it.

**Consequences.**

- No `.service` files, no systemd, no custom init in the image.
  `/sbin/init` symlinks to `paraglide-init` (same as every Beyond
  rootfs).
- One Rust crate at `packer/files/beyond-pg/`. Subcommand dispatch
  in `main`. `supervisor.rs` is the long-running entry; everything
  else is library code it calls into.
- Total processes at runtime: `paraglide-init` (PID 1), `beyond-pg
  supervisor` (one inner-tier child), `postgres`, `pgbouncer`. Four
  processes. Five if a backup is running.
- Beyond's primitives (`paraglide-init`, `paraglide-agent`,
  box-manager, vsock protocol) require **one** generic feature
  (`self_supervised: true` flag on the image manifest) to support
  this image. Nothing more.

---

## H — Logging

### H-001 — stderr → `beyond-pg supervisor` → vsock

**Decision.** Postgres and PgBouncer write to stderr. `beyond-pg
supervisor` spawns each with piped stdio, reads lines from both pipes,
and forwards them over vsock to the host log pipeline using the same
wire format `paraglide-agent` uses for user-app logs.

**Why.** Same log pipeline the rest of Beyond uses (the host receiver
doesn't distinguish official-image traffic from user-app traffic).
Mirroring the wire format keeps the host side generic. Watching
journald instead would require a different pipeline; writing to log
files would bloat snapshots on every fork.

**Cost.** Rate limit is ~500 lines/sec sustained per stream, 1000
burst (host pipeline default). Configuration must respect this —
`log_statement = 'all'` would trip the limit. Default `log_statement
= 'ddl'` + `log_min_duration_statement = 1000` keeps us comfortably
under.

### H-002 — No `logging_collector`, no log files

**Decision.** `logging_collector = off`. No log files written
anywhere.

**Why.** Log files in PGDATA would fork with every branch and bloat
snapshots. Log files outside PGDATA wouldn't be captured by
guest-agent's pipe-based log capture. Stderr is the only path that
satisfies both constraints.

---

## I — Security

### I-001 — TLS off in MVP

**Decision.** `ssl = off`. No certificates managed by the image.

**Why.** Beyond's network is the perimeter:

- The Beyond tunnel does mTLS.
- Internal traffic runs over private VXLAN with eBPF policy.
- Postgres' connection is never on a public network at MVP.

Adding PG-level TLS in MVP costs CPU + cert management for zero
marginal security gain. Users who need defense in depth or have apps
connecting outside Beyond's tunnel can flip `ssl = on` in
`99-user.conf` and provide certs.

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

### K-003 — Image versioned with git SHA + extensions manifest pin

**Decision.** Image filename: `postgres-noble-{git_sha}.img`. An
`extensions.toml` at the repo root pins exact versions of every
non-PGDG extension (specifically `beyond_auth`, `beyond_queue`).

**Why.** Reproducible builds. Anyone can rebuild a historical image
exactly. Extension version mismatches fail at build time, not at
first boot.

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

### P-002 — Mirror the substrate's existing patterns

Same MMDS pattern as the rootfs (G-002). Same logging pattern as
guest-agent (H-001). Same Packer pipeline as `beyond/packer`
(K-001). Same scripts directory layout (K-001). Same `mise.toml`
task shape (K-001). Operators already know how to operate Beyond;
the Postgres image doesn't ask them to learn anything new.

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
