# beyond-pg-cdc Architecture

Consumes a PostgreSQL logical replication stream (pgoutput format) over a Unix socket, decodes each WAL change into a JSON event, and fans the event out to all connected RESP3 TCP subscribers.

## Data Flow

```
PostgreSQL (Unix socket)
        │
        │  XLogData / PrimaryKeepalive frames (CopyBoth)
        ▼
  proto::recv_wal
        │
        │  WalMsg::XLogData { lsn, data }
        ▼
  decode::Decoder::decode
        │  ├── RELATION  → update relation cache, return None
        │  ├── BEGIN/COMMIT → track commit LSN, return None
        │  ├── INSERT/UPDATE/DELETE → emit JSON, return Some(bytes)
        │  └── TRUNCATE/TYPE/ORIGIN → return None
        │
        │  Arc<[u8]> (JSON, zero-copy shared across all subscribers)
        ▼
  broadcast (Mutex<Vec<SyncSender>>)
        │  try_send to each subscriber channel (cap 64)
        │  prunes sender on failure (full channel or dead receiver)
        │
        ├──► subscriber 1 channel ──► resp connection thread ──► TCP client A
        ├──► subscriber 2 channel ──► resp connection thread ──► TCP client B
        └──► ...

WalMsg::Keepalive { reply_needed: true }  ──►  proto::send_status
I/O timeout (10s read timeout on replication conn)  ──►  proto::send_status
Proactive (every 10s elapsed)  ──►  proto::send_status
```

## Concepts & Terminology

| Term            | What It Controls                                                                                          | NOT                                                                                                                    |
| --------------- | --------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Slot            | Server-side replication slot name; determines which WAL is buffered and the resume LSN on reconnect       | Client-side state; the slot is always `confirmed_flush_lsn`-based server-side                                          |
| Publication     | The PostgreSQL publication name sent in `START_REPLICATION`; filters which tables emit change events      | A client-side filter; filtering happens in Postgres, not the decoder                                                   |
| Subscriber      | A RESP3 TCP connection that has sent `WATCH`; holds a bounded `SyncSender<Arc<[u8]>>` in the shared list  | A durable consumer; subscribers are best-effort and get pruned on backpressure                                         |
| LSN             | 64-bit log sequence number, displayed as `hi/lo` hex (e.g., `1/23456780`); appears in every emitted event | A byte offset; the `hi` word is the WAL segment, `lo` is offset within it                                              |
| Commit LSN      | The `final_lsn` field from the BEGIN message                                                              | The individual XLogData `lsn`, which is the record's own position; the decoder does not currently store the commit LSN |
| TOAST Unchanged | A column value marked `'u'` in an UPDATE tuple, meaning Postgres did not replicate its value              | NULL; `'u'` means "unchanged since last time" and is omitted from the JSON output entirely                             |

## Core Mechanism

### Thread Model

Three threads run for the lifetime of the process:

```
main thread          RESP listener thread      per-connection thread (N)
───────────          ─────────────────────     ────────────────────────
parse_args           resp::serve (never        handle_conn
install_signals      returns; exits on         read_cmd loop:
reconnect loop       bind failure)               HELLO → write_hello
  run_replication                                PING  → +PONG
  broadcast                                      WATCH → stream_events
                                                 UNWATCH → +OK
```

The main thread owns the replication loop; the RESP listener thread is detached (handle stored to surface panics on join). Each TCP connection gets its own OS thread — there is no thread pool.

### Replication Loop (`main.rs:run_replication`)

1. Connect to Postgres via Unix socket at `{socket_dir}/.s.PGSQL.{pg_port}`.
2. Send `START_REPLICATION SLOT {slot} LOGICAL 0/0 (proto_version '1', publication_names '{publication}')`. The start LSN `0/0` tells Postgres to start from `confirmed_flush_lsn` — the slot's durable resume point.
3. Set a 10-second read timeout. `recv_wal` returns `WouldBlock`/`TimedOut` after 10s of silence; the loop uses this as a trigger to send a proactive status update.
4. For each `XLogData`, call `Decoder::decode`. If it returns `Some(json)`, wrap it in an `Arc<[u8]>` and broadcast to all subscribers.
5. Send `StandbyStatusUpdate` (write=flush=apply=last_lsn) every 10 seconds or whenever Postgres requests a reply. This advances `confirmed_flush_lsn` on the slot — WAL before this LSN can be reclaimed.

### Decoder (`decode.rs`)

Stateful parser for the [pgoutput logical replication protocol](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html).

**State**: a `HashMap<u32, RelationInfo>` keyed by relation OID. A `RELATION` message must arrive before any `INSERT`/`UPDATE`/`DELETE` for that OID. If the OID is missing from the cache when a DML arrives, the message is silently dropped (returns `None`).

**Type coercion**: pgoutput delivers all column values as text strings. The decoder converts them to native JSON types by OID:

| OID           | Type     | JSON representation                                                                                                                     |
| ------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| 16            | bool     | `true`/`false` (`"t"` → `true`, `"f"` → `false`, anything else → JSON string)                                                           |
| 21, 23, 20    | int2/4/8 | JSON number (parsed as `i64`)                                                                                                           |
| 700, 701      | float4/8 | JSON number (parsed as `f64`); falls back to string on `NaN`/`Infinity` (not valid JSON numbers)                                        |
| 1700          | numeric  | JSON string — exact Postgres text representation preserved; **not** coerced to `f64` to avoid precision loss on high-precision decimals |
| anything else | —        | JSON string                                                                                                                             |

**UPDATE handling**: an UPDATE message may carry an old tuple (replica identity `FULL` → marker `'O'`, or key-only → marker `'K'`). If present, it becomes the `"old"` field. Unchanged TOAST columns (`'u'` marker) are omitted from the JSON entirely — neither `"old"` nor `"new"` will contain them.

### Emitted JSON Shape

Every DML event produces one JSON object:

```json
{
  "lsn":    "1/23456780",
  "op":     "insert" | "update" | "delete",
  "schema": "public",
  "table":  "users",
  "old":    { ... },   // present on UPDATE (if replica identity supplies it) and DELETE
  "new":    { ... }    // present on INSERT and UPDATE
}
```

### Broadcaster (`main.rs:broadcast`)

Holds the subscriber list under a `Mutex`. Uses `try_send` (non-blocking) and calls `retain` in the same pass — a subscriber is pruned immediately if its channel is full or the receiver has been dropped. This means **a slow subscriber that falls 64 events behind is disconnected on the next event**.

The message body is `Arc<[u8]>`: all subscribers share a single allocation with no copies.

### RESP3 Server (`resp.rs`)

Implements a strict subset of RESP3. Only RESP3 array commands are accepted (no inline commands). Verbs are case-insensitive.

**Command handling**:

| Command             | Response                                               | Effect                                                                |
| ------------------- | ------------------------------------------------------ | --------------------------------------------------------------------- |
| `HELLO 3`           | `%3\r\n` map (server/proto/version)                    | Handshake only; protocol version not enforced                         |
| `PING`              | `+PONG\r\n`                                            | Keep-alive                                                            |
| `WATCH`             | `>2\r\n$6\r\nchange\r\n$5\r\nready\r\n` then streaming | Registers subscriber; stream starts from slot's `confirmed_flush_lsn` |
| `WATCH SINCE <lsn>` | `-ERR SINCE is not supported; ...`                     | Error; use bare `WATCH` — slot-based replay is server-side            |
| `UNWATCH`           | `+OK\r\n` then close                                   | Removes subscriber (channel dropped)                                  |
| unknown             | `-ERR unknown command '...'`                           | Connection stays open                                                 |

**Streaming** (`stream_events`): creates a `SyncSender`/`Receiver` pair, pushes the sender into `Subscribers`, sends the `ready` push, then loops on `recv_timeout(30s)`. On timeout it sends `["change", "heartbeat"]` to keep intermediate proxies alive. A write failure exits the loop; the receiver drop triggers self-pruning on the broadcaster's next `retain`.

**Limits**: RESP3 array elements capped at 16; bulk strings capped at 16 MiB. Both reject at the framing layer before the command is dispatched.

## Reconnect State Machine

```
┌──────────────────────────────────────────────┐
│                   running                    │
│  (recv_wal loop, broadcasting events)        │
└──────────────────┬───────────────────────────┘
                   │ io::Error (not WouldBlock/TimedOut)
                   ▼
            ┌─────────────┐
            │  backoff    │  delay: 100ms → ×2 → 30s cap
            │  sleep      │  interruptible: checks SHUTDOWN every 100ms
            └──────┬──────┘
                   │ delay elapsed AND !SHUTDOWN
                   ▼
            ┌─────────────┐
            │ connecting  │  proto::connect → proto::start_replication
            └──────┬──────┘
           ┌───────┴──────────┐
        success            error (→ backoff, delay doubles)
           │
           ▼
      running (backoff resets to 100ms if connection was stable for ≥60s)
```

Note: the reconnect delay doubles on each error up to a 30s cap. If the connection ran successfully for at least 60 seconds before failing, the delay resets to 100ms so a transient drop after a long healthy run does not inherit a stale 30s delay.

## Trust Boundaries

**What the system verifies:**

- Postgres auth type must be `AuthenticationOk` (kind=0, trust auth). Any other auth method (MD5, SCRAM, etc.) causes an immediate error — there is no credential handling.
- RESP3 clients: no authentication at all. Any TCP client that can reach port 9001 can subscribe.

**What passes through unchecked:**

- RESP3 client identity — no auth, no TLS.
- RESP3 `SINCE <lsn>` argument — parsed and discarded; actual resume position is always the slot's `confirmed_flush_lsn`.
- Column values beyond type coercion — no schema validation, no value sanitization.

**Why these boundaries:**

- The process runs inside a VM alongside PostgreSQL. The Unix socket is local-only; the RESP port is VM-internal. Network-layer isolation is the security perimeter, not application-layer auth.
- Trust auth is a deliberate prerequisite for the deployment environment (not a fallback).

## Configuration

All parameters are CLI flags. There is no config file or environment variable support.

| Flag            | Default               | What It Controls                                                             |
| --------------- | --------------------- | ---------------------------------------------------------------------------- |
| `--socket-dir`  | `/var/run/postgresql` | Directory containing the Postgres Unix socket (`{dir}/.s.PGSQL.{pg-port}`)   |
| `--pg-port`     | `5433`                | Postgres port number (used in the Unix socket filename, not a TCP port)      |
| `--user`        | `replicator`          | Postgres user sent in the startup message; must have `REPLICATION` privilege |
| `--dbname`      | `postgres`            | Database to connect to; must match the publication's database                |
| `--port`        | `9001`                | TCP port for the RESP3 subscriber server (`0.0.0.0:{port}`)                  |
| `--slot`        | `cdc`                 | Logical replication slot name; must already exist on the server              |
| `--publication` | `cdc`                 | Publication name passed to `START_REPLICATION`                               |

The replication slot and publication must be created externally before starting the binary.

## Failure Modes

| Failure                                 | What Actually Happens                                                                             | Recovery                                                                                      |
| --------------------------------------- | ------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Postgres connection drops               | `recv_wal` returns `io::Error`; replication loop logs and enters exponential backoff (100ms→30s)  | Automatic reconnect; subscribers stay registered but receive no events during downtime        |
| Postgres auth method not trust          | `connect()` returns `CdcError::Config`; process exits with code 1                                 | Fatal; fix `pg_hba.conf` and restart the process                                              |
| RESP3 bind failure                      | `resp::serve` calls `process::exit(1)`                                                            | Fatal; process must be restarted                                                              |
| Subscriber slow consumer (channel full) | `try_send` returns `Err(Full)`; sender pruned from subscriber list on that broadcast              | Client connection closed; client must reconnect and WATCH again                               |
| Subscriber TCP write failure            | `write_push` returns `io::Error`; `stream_events` exits; receiver dropped                         | Sender pruned on broadcaster's next event                                                     |
| SIGTERM / SIGINT                        | `SHUTDOWN` AtomicBool set to `true`; `sleep_interruptible` and the replication loop both check it | Clean exit after current event is broadcast; in-flight WAL is not acknowledged                |
| Unknown relation OID in DML             | `Decoder::decode` returns `None`; event silently dropped                                          | Normal startup behavior — RELATION messages precede their DML in the stream; no action needed |

## File Map

| File            | What It Does                                                                                                    |
| --------------- | --------------------------------------------------------------------------------------------------------------- |
| `src/main.rs`   | Entry point: arg parsing, signal handlers, replication loop with backoff, `broadcast`                           |
| `src/proto.rs`  | Postgres wire protocol: connect (Unix/TCP), startup, trust auth, `START_REPLICATION`, `recv_wal`, `send_status` |
| `src/decode.rs` | pgoutput decoder: RELATION cache, INSERT/UPDATE/DELETE → JSON, type-aware coercion                              |
| `src/resp.rs`   | RESP3 TCP server: listener, per-connection handler, subscriber registration, push streaming, heartbeat          |
| `src/lsn.rs`    | `Lsn` newtype (u64): `Display` (`hi/lo` hex), `FromStr`, `to_be_bytes`/`from_be_bytes`                          |
