# Postgres on Beyond

Standard Postgres. On a substrate that forks. The database your code already writes for, sitting on GlideFS.

That's the whole product.

---

## The bet

Every managed Postgres that ships branching rebuilt storage to get there.

| Who         | What they built                                                   |
| ----------- | ----------------------------------------------------------------- |
| Neon        | A custom storage tier. Pageservers. Safekeepers. A Postgres fork. |
| Aurora      | A distributed log-structured storage layer. Six-way quorum.       |
| Supabase    | Standard Postgres. Branching bolted on top.                       |
| RDS, Vercel | No branching.                                                     |

Each one took years. Each one is a forked codebase, a parallel storage tier, a feature one team owns forever.

We didn't build any of it. GlideFS already does CoW. Standard Postgres runs on it. Branching is free.

The fork is a primitive of the substrate, not a feature of the database.

Everything below this line is what falls out of that bet.

---

## What forks

Every byte under `/var/lib/postgresql`. Tables. Indexes. WAL. Extensions. Roles. Statistics. The full state of the database at the snapshot timestamp.

Postgres doesn't know it forked. It boots, sees a crash-consistent snapshot, replays WAL. The same code path Postgres has shipped for two decades.

The substrate does the work. Auth uses the Postgres we gave you, under a different schema, on the same volume. The queue does too. Both branch with the database. Not by design: because they share the substrate.

---

## Durability

GlideFS is write-behind. When Postgres fsyncs, the bytes hit local SSD immediately. Local SSD survives Postgres crashes, VM reboots, and GlideFS process restarts. It does not survive losing the host.

A host failure costs you up to 64 MB of acked WAL. Five seconds. The window before GlideFS' next S3 flush.

This is the same durability bar Supabase ships in their default tier. RDS single-AZ ships it. EBS makes it stronger because EBS does synchronous quorum within an AZ at the block layer: an EBS fsync = the bytes are on multiple servers. Local SSD doesn't do that, and we don't have EBS.

For dev, preview, and branch databases, the five-second window is fine. The data is reproducible. The environment is throwaway. The cost is right.

For a production database holding committed customer transactions, it is not fine.

We had four options.

| Path                                      | What it requires                                                      | What it gives                                          | Verdict         |
| ----------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------ | --------------- |
| Storage-layer quorum in GlideFS           | Custom cross-host replication protocol underneath every export.       | EBS-equivalent fsync durability.                       | Out of scope.   |
| Fork Postgres, ship WAL to a Safekeeper.  | A Postgres fork. A Paxos WAL service. Maintained forever.             | Aurora/Neon-equivalent durability. Stateless compute.  | Not worth it.   |
| WAL sink (`pg_receivewal --synchronous`). | A small VM. A directory served over HTTP. A step in `beyond-pg boot`. | Quorum-durable WAL. No data copy. No full replica.     | Yes — Tier 1.5. |
| Postgres sync replication (full replica). | Full standbys. A SIGHUP.                                              | Quorum durability + promotable standby + read scaling. | Yes — Tier 2.   |

The WAL sink is the right first answer. A small VM runs `pg_receivewal --synchronous`, which streams WAL from the primary and acknowledges each record. The primary includes it in `synchronous_standby_names` with `synchronous_commit = remote_write`. A transaction commits when the WAL is on the sink's local SSD — a second failure domain. No data copy. Storage proportional to WAL retention, not database size.

Recovery is handled by `beyond-pg boot`: it reads `BEYOND_PG_WAL_SINK` from MMDS, checks `pg_controldata` for the last valid WAL location, fetches any gap from the sink's HTTP endpoint, places the segments in `pg_wal/`, and boots Postgres normally. Box-manager does what it already does — rehome the VM, reattach the volume. The WAL injection is one more step in the idempotent boot sequence. Nothing new at the orchestration layer.

Full replicas (Tier 2) add promotability and fast failover on top of that. The right answer when the availability SLA demands seconds of downtime, not minutes. But zero data loss doesn't require them.

That's the durability story. Not the fastest answer. The one that's correct using primitives nobody has to audit a second time.

What it forces, in the image, today:

- `wal_level = logical` from day one. Logical decoding readiness. Can't be raised without a primary restart, so we set it now and never bounce production for it later.
- `max_wal_senders = 10`, `max_replication_slots = 10`, `hot_standby = on`. Replicas can attach to a Tier 1 primary without a restart.
- `pg_wal` as a symlink. Tier 2 relocates WAL to a local-NVMe scratch device by changing the symlink target. No Postgres reconfiguration.
- `archive_mode = on` with a no-op script. PITR target gets set later via MMDS. No restart.

Tier 2 ships when sync replication is wired through the control plane. The image is ready for it. Adding HA isn't a rewrite. It's a config flag and replicas you didn't have yesterday.

---

## Availability

Durability is "the data survives." Availability is "compute can find the data."

GlideFS volumes are not bound to a host. A volume can be detached from a dead host and reattached on a healthy one. When a host dies, box-manager rehomes the VM. The volume comes with it. Postgres boots, sees a crash-consistent volume, recovers from WAL.

This is what the substrate gives us for free. The data survives host loss for every tier, by default, with no replicas.

That changes what replicas are for.

| Without volume portability                                                               | With volume portability                                                         |
| ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Replicas exist to survive host loss. Without them, a host dies and the database is gone. | Replicas exist to minimize downtime during recovery. The data already survived. |

Tier 1 RTO is the time it takes to detect the failure, schedule a new VM, and recover Postgres. Minutes. For most workloads that's fine.

Tier 2 keeps a warm standby running on a different host. Different GlideFS volume, kept up-to-date by streaming replication. When the primary fails: promote. The standby has the data hot in shared_buffers, the WAL up to the last acked LSN, the buffer pool warm. PgBouncer reroutes. Seconds.

The path we deliberately did not take: Aurora's. Aurora's compute is stateless. The storage tier holds the WAL and materializes pages. Failover is pointing a new compute node at the same storage. Milliseconds.

It requires forking Postgres. The custom storage manager, the WAL-shipping path, the buffer pool eviction protocol. Maintained against upstream forever. Every operational tool needs to know about the fork.

The gain over warm-standby is milliseconds, not seconds. The cost is a forked codebase. Not worth it.

The substrate gives us volume portability. Tier 2 trades a warm replica for a faster failover. We don't fork Postgres for the marginal millisecond gain.

---

## Scalability

Beyond's atomic unit is the box. Box-manager resizes boxes in place: volumes are portable, so the data follows.

That makes vertical scaling the default lever.

```
glide pg scale --memory 64gb
```

A control-plane operation against the existing primitive. No coordination. No replication topology change. No re-shard. No downtime worth measuring. The auto-tuning service reads the new RAM size from MMDS and rewrites the tuning conf on next boot.

Postgres scales vertically very well. The industry built sharding fashion at a time when "big box" meant 64 GB and a CPU from 2014. That world is gone. A 256 GB box with 64 vCPUs is unremarkable hardware in 2026 and covers most production Postgres workloads.

Vertical scaling has no consistency or query-shape implications. Sharding has both. Default to the lever that doesn't compromise correctness.

After vertical: read replicas. The same image with `BEYOND_PG_TIER=replica` and `primary_conninfo` set in MMDS. The bootstrap drops a `standby.signal` and replication conf. Streaming replication. Reads scale linearly per replica added.

Sharding is not on this image's roadmap. It is a different product. The image's contract is "standard Postgres at `localhost:5432`." Sharding breaks that contract. Different contract, different product. If Beyond ships sharding someday, it ships as a separate primitive that runs Postgres VMs underneath.

---

## Ephemeral environments are free

The substrate has another property we lean on hard.

GlideFS exports can be marked ephemeral. Writes stay on local SSD. Nothing flushes to S3. Zero storage cost. Zero PUT cost. Zero GET cost when the fork goes cold.

Beyond marks preview, branch, and throwaway-fork environments ephemeral at the substrate level. The Postgres image reads `BEYOND_VOLUME_EPHEMERAL` from MMDS and drops `synchronous_commit = off` into the config. Commits return faster. The data-loss window doesn't matter on a volume designed to be thrown away.

This is not a Postgres feature. It is a substrate feature. The queue volume, the auth volume, every primitive on a preview environment inherits ephemerality the same way.

You don't ask for it. The environment is ephemeral or it isn't.

A consequence: a preview database has zero S3 footprint. A fork of production for a debugging session has zero S3 footprint. Branch teardown is instant because there is nothing to clean up in S3. Tens of thousands of preview environments cost what one cheap VM costs.

That's the substrate showing up in the bill.

---

## Tiers

Three of them. The same image plays all roles. The fourth axis (ephemeral or not) is set by the substrate, not the image.

| Tier              | Environment | Durability                                                  | Availability                           | Use                               |
| ----------------- | ----------- | ----------------------------------------------------------- | -------------------------------------- | --------------------------------- |
| Single            | Durable     | GlideFS write-behind. ~5 s window on host loss.             | Volume rehomes on a new host. Minutes. | Dev. Low-stakes production.       |
| Single            | Ephemeral   | Local SSD only. Volume gone on host loss.                   | Volume rehomes if it can. Often, not.  | Preview. Branch. Throwaway forks. |
| Single + WAL sink | Durable     | WAL sink quorum. Zero data loss on host failure.            | Volume rehomes on a new host. Minutes. | Production without HA budget.     |
| HA                | Durable     | Sync replication. Quorum across failure domains. Zero loss. | Warm standby promotes. Seconds.        | Production needing fast failover. |

`HA + ephemeral` is permitted but meaningless. There is nothing to be highly available about if the volume is throwaway.

---

## What we won't build

The substrate-thesis playing out as a consequence list.

**A Postgres fork.** The reason to fork Postgres is to give it a storage layer that does CoW + S3 + local caching. GlideFS already does that. Forking Postgres to add what we already have is paying twice.

**A pageserver.** GlideFS is the pageserver.

**A custom durability protocol.** Postgres has `synchronous_standby_names`. It works.

**A Beyond SDK for Postgres.** Every Postgres library, every ORM, every migration tool, every backup tool: they all work today, unchanged. Adding ours would be a worse surface than the wire protocol.

**Sharding.** A different product. If sharding lands on Beyond someday, it lands as a separate primitive on top of this Postgres. Not a flag on this image.

**A query proxy that hides our limitations.** PgBouncer is the only proxy. If a tool needs more, the answer is a different tool.

**A backup service.** GlideFS snapshots are backups. Atomic at the block layer, CoW-cheap, restored by forking. Most managed-PG providers ship `pg_basebackup` to S3 on a schedule and call it a backup. We get the same thing — strictly cheaper, faster, more correct — from the substrate's existing snapshot primitive. Sub-snapshot-interval PITR is `archive_command` shipping WAL to S3 between snapshots, which we already wire.

Every "why aren't you building X?" reduces to the same answer. Beyond already has the primitive. The primitive composes.

---

## Principles

**The wire protocol is the SDK.** `psql localhost`. Every Postgres tool works. We don't invent surfaces that compete with the standard.

**The substrate does the work.** If GlideFS already does it, we don't do it. If Postgres already does it, we don't do it. We're not in the business of reinventing primitives.

**Pay irreversible costs upfront.** Settings that need a primary restart later get set right today. We don't bounce production for `wal_level = logical`.

**Forks are the unit of work.** Every operation that benefits from isolation gets a fork. The fork is cheap. Promote it or destroy it.

**Production is the only truth.** The fork has production data. The replica has production data. We don't pretend a simulation is real.

---

## What this is not

This is not Aurora-compatible. Not a Postgres fork. Not a sharding layer. Not a query optimizer rewrite. Not a competitor in the "managed Postgres" category.

It's the database that falls out of running standard Postgres on a CoW substrate.

That's all it has to be.
