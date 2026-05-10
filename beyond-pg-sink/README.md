# beyond-pg-sink

Receive PostgreSQL WAL segments and serve them back on boot. Zero data loss on a single host failure, without a full standby replica.

PostgreSQL commits wait for WAL acknowledgment from this sink before returning to clients. On boot, the `beyond-pg` pipeline fetches any missing segments and places them in `pg_wal/` before Postgres starts. This closes the GlideFS write-behind window without the cost of a full streaming replica.

## Run

```sh
beyond-pg-sink \
  --connstr "postgresql://replicator:secret@10.0.0.1/postgres" \
  --dir /var/lib/postgresql/wal-sink \
  --port 9000 \
  --slot wal_sink
```

Passwords are stripped from `--connstr` and passed to `pg_receivewal` via `PGPASSWORD`.

## Configuration

| Flag                   | Default                        | Description                                                |
| ---------------------- | ------------------------------ | ---------------------------------------------------------- |
| `--connstr`            | —                              | **Required.** PostgreSQL connection string (libpq format). |
| `--dir`                | `/var/lib/postgresql/wal-sink` | Directory to store WAL segments.                           |
| `--port`               | `9000`                         | TCP port for the HTTP server.                              |
| `--slot`               | `wal_sink`                     | Replication slot name. Created if it doesn't exist.        |
| `--retention-segments` | `64`                           | Number of segments to retain per timeline. Minimum: 8.     |

## HTTP API

**`GET /list`** — Returns a newline-separated list of complete WAL segment filenames in `--dir`.

```
00000001000000000000000A
00000001000000000000000B
```

**`GET /<segment>`** — Returns the raw binary contents of a WAL segment. Returns 404 if not found.

Segment names must be exactly 24 hexadecimal characters. Requests that fail this check are rejected — no directory traversal possible.

## How It Works

Two threads run concurrently:

- **Receiver** — Spawns `pg_receivewal --synchronous`, monitors it, and restarts with a 2-second backoff on failure.
- **HTTP server** — Serves `/list` and `/<segment>` to clients fetching segments during boot or recovery.

PostgreSQL is configured with `synchronous_commit = remote_write` and `synchronous_standby_names = 'wal_sink'` so commits block until the sink acknowledges receipt. On Linux, segment serving uses `sendfile(2)` for zero-copy transfer.

Retention is pruned per timeline — so a timeline 2 backup never evicts timeline 1 segments needed for recovery.

## Integration

Set `BEYOND_PG_WAL_SINK=<host>:<port>` in the machine's MMDS metadata. The `beyond-pg` boot process reads this, fetches `/list`, downloads any missing segments into `pg_wal/`, and writes the synchronous replication config before starting Postgres.

## Durability Model

| Tier    | Mechanism                  | Loss window                     |
| ------- | -------------------------- | ------------------------------- |
| 1       | GlideFS write-behind cache | Up to 5s or 64MB                |
| **1.5** | **WAL sink (this)**        | **Zero on single host failure** |
| 2       | Full standby replica       | Zero                            |

Tier 1.5 is stronger than async replication and lighter than a full replica.
