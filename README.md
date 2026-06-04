# beyond/postgres

Standard Postgres 18 on a CoW substrate. Forks with the volume: tables, indexes, WAL, extensions, roles, statistics. The same `psql localhost:5432` in dev and prod.

No Postgres fork. No custom storage tier. The substrate (GlideFS) does CoW; Postgres runs unmodified on top.

## Quick Start

Inside a Beyond box, Postgres is already running. Connect through PgBouncer on `localhost:5432`:

```sh
psql "postgres://postgres@localhost:5432/postgres"
```

Forks boot in seconds against a CoW snapshot of `/var/lib/postgresql`. Postgres replays WAL and comes up crash-consistent.

## What it does

- **Forks with the substrate.** Every byte under `/var/lib/postgresql` snapshots atomically; the new box boots on a CoW copy. No `pg_dump`, no replication topology, no wait.
- **PgBouncer on the front, sized to load.** Transaction pooling and TLS termination on `:5432`; Postgres listens on the loopback only. The pooler runs one worker when idle and adds `so_reuseport` workers across cores as connection-handshake load rises, then reaps them when it falls. The worker count survives restarts.
- **Logical decoding from day one.** `wal_level = logical`, `max_wal_senders = 10`, `max_replication_slots = 10`. No primary restart to enable CDC later.
- **Standard extensions, pinned.** pgvector, pgvectorscale, PostGIS, pg_cron, pg_partman, pg_jsonschema, hypopg, pg_repack, pg_search, pg_stat_statements, pg_trgm, auto_explain.
- **Beyond extensions on the same volume.** `beyond-auth` and `beyond-queue` ship in the image and live under their own schemas in your database. Forking your DB forks their state automatically.
- **Vertical scale in place.** `byd pg scale --size 1t`; the volume is portable and resizable. The auto-tuner rewrites `postgresql.conf`.
- **Ephemeral previews are free.** Preview and branch volumes set `synchronous_commit = off`, never flush to S3. Zero storage cost, instant teardown.
- **WAL sink for quorum durability.** Tier 1.5 streams WAL to a second failure domain. Zero data loss on host failure without a full standby.
- **HA via streaming replication.** Tier 2 keeps a warm standby on a different host; promote on primary loss. No Postgres fork, no custom storage protocol.
- **Zero-downtime supervisor restart.** `beyond-pg-init` adopts a restarted supervisor via pidfd; Postgres and PgBouncer stay running across `beyond-pg` upgrades.

## Tiers

The same image plays every role. The tier is set by MMDS at boot.

| Tier               | Durability                              | Availability                | Use                              |
| ------------------ | --------------------------------------- | --------------------------- | -------------------------------- |
| Single (durable)   | GlideFS write-behind, ~5 s on host loss | Volume rehomes, minutes RTO | Dev, low-stakes production       |
| Single (ephemeral) | Local SSD only, gone on host loss       | Best-effort rehome          | Preview, branch, fork            |
| Single + WAL sink  | Quorum WAL, zero data loss              | Volume rehomes, minutes RTO | Production without HA budget     |
| HA                 | Sync replication across hosts           | Warm standby, seconds RTO   | Production needing fast failover |

`HA + ephemeral` is rejected. Nothing to be highly available about on a throwaway volume.

## Components

This repo builds four binaries plus the rootfs image:

| Binary           | Role                                                                                            |
| ---------------- | ----------------------------------------------------------------------------------------------- |
| `beyond-pg-init` | PID 1. One-shot boot setup (mounts, network, MMDS, zram), then supervises `beyond-pg` via pidfd |
| `beyond-pg`      | Boot orchestrator and supervisor. Runs Postgres and PgBouncer; serves the vsock RPC surface     |
| `beyond-pg-sink` | WAL sink. Runs `pg_receivewal --synchronous` and serves segments over HTTPS                     |
| `beyond-pg-cdc`  | Logical decoding sidecar. Streams changes over QUIC to downstream consumers                     |

The image is built with Packer; `mise run build:image` assembles the rootfs.

## Configuration

Settings come from MMDS at boot. The image reads them and rewrites Postgres config without a restart.

| MMDS key                     | Effect                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------- |
| `BEYOND_PG_TIER`             | `primary` (default), `replica`, or `sink`. Drops `standby.signal` for replicas      |
| `BEYOND_PG_WAL_SINK`         | URL of the WAL sink. Adds the sink to `synchronous_standby_names`                   |
| `BEYOND_VOLUME_EPHEMERAL`    | `1` sets `synchronous_commit = off`                                                 |
| `BEYOND_PG_MEMORY_MB`        | Read by the auto-tuner; rewrites `shared_buffers`, `effective_cache_size`, work_mem |
| `BEYOND_PG_PITR_TARGET`      | Restore target timestamp or LSN                                                     |
| `BEYOND_PG_PRIMARY_CONNINFO` | Replica only. Sets `primary_conninfo`                                               |

The boot sequence is idempotent. Every step checks state before acting; every transient failure is retryable.

## What this is not

Not a Postgres fork. Not Aurora-compatible. Not a sharding layer. Not a managed-Postgres reimplementation.

It's standard Postgres on a substrate that forks. Every Postgres client, ORM, migration tool, and backup tool works unchanged. The wire protocol is the SDK.
