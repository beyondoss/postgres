# beyond-pg-sink

Receive PostgreSQL WAL segments and serve them back on boot. Zero data loss on a single host failure, without a full standby replica.

PostgreSQL commits wait for WAL acknowledgment from this sink before returning to clients (`synchronous_commit = remote_write`). On boot, the `beyond-pg` pipeline fetches any missing segments and places them in `pg_wal/` before Postgres starts. This closes the GlideFS write-behind window without the cost of a full streaming replica.

## Run

**QUIC mode** (default) — sink waits for the `wal-forwarder` bridge to connect:

```sh
beyond-pg-sink \
  --dir /var/lib/postgresql/wal-sink \
  --port 9000 \
  --slot wal_sink
```

On the primary, run the forwarder pointing at the sink:

```sh
wal-forwarder \
  --pg-port 5433 \
  --sink-addr 10.0.0.2:9000 \
  --slot wal_sink
```

**TCP mode** — sink connects directly to Postgres (no forwarder needed):

```sh
beyond-pg-sink \
  --mode tcp \
  --connstr "postgresql://replicator:secret@10.0.0.1/postgres" \
  --dir /var/lib/postgresql/wal-sink \
  --port 9000 \
  --slot wal_sink
```

## Performance

Measured: N=2000 single-row transactions, `synchronous_commit=remote_write`, Postgres 18 in Docker,
sink in Alpine Docker container, forwarder on host (Docker Desktop, Apple Silicon).

```
Transport                       p50       p95       p99      max
──────────────────────────────────────────────────────────────────
TCP  (pg_receivewal, Phase 0)  4410 µs   6120 µs   7327 µs  35000 µs
TCP  (native Rust, Phase 1)    4201 µs   6007 µs   9006 µs  30070 µs
QUIC (quinn-proto, Phase 2)     703 µs    877 µs   1120 µs   1225 µs
──────────────────────────────────────────────────────────────────
QUIC improvement vs TCP          ~6×       ~7×       ~8×      ~25×
```

QUIC is the default. Run the benchmark yourself:

```sh
cargo test -p beyond-pg-sink --test e2e -- --ignored --nocapture latency_baseline_real
```

## Configuration

| Flag                   | Default                        | Description                                                         |
| ---------------------- | ------------------------------ | ------------------------------------------------------------------- |
| `--mode`               | `quic`                         | Transport: `quic` (QUIC server) or `tcp` (native Postgres receiver) |
| `--connstr`            | —                              | PostgreSQL connection string. Required in `--mode tcp`.             |
| `--dir`                | `/var/lib/postgresql/wal-sink` | Directory to store WAL segments.                                    |
| `--port`               | `9000`                         | TCP port for the HTTP server (QUIC listens on the same UDP port).   |
| `--slot`               | `wal_sink`                     | Replication slot name. Created if it doesn't exist.                 |
| `--retention-segments` | `64`                           | Number of segments to retain per timeline. Minimum: 8.              |

## HTTP API

**`GET /list`** — Returns a newline-separated list of complete WAL segment filenames.

```
00000001000000000000000A
00000001000000000000000B
```

**`GET /<segment>`** — Returns the raw binary contents of a WAL segment. 404 if not found.

Segment names must be exactly 24 hexadecimal characters — no directory traversal possible.

## Integration

Set `BEYOND_PG_WAL_SINK=<host>:<port>` in the machine's MMDS metadata. The `beyond-pg` boot
process reads this, fetches `/list`, downloads any missing segments into `pg_wal/`, and writes
the synchronous replication config before starting Postgres.

## Durability Model

| Tier    | Mechanism                  | Loss window                     |
| ------- | -------------------------- | ------------------------------- |
| 1       | GlideFS write-behind cache | Up to 5s or 64MB                |
| **1.5** | **WAL sink (this)**        | **Zero on single host failure** |
| 2       | Full standby replica       | Zero                            |

Tier 1.5 is stronger than async replication and lighter than a full replica.
