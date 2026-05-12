# beyond-pg-sink Architecture

Takes a PostgreSQL connection string (TCP mode) or listens for a QUIC connection (QUIC mode), receives a live physical WAL stream, writes WAL segments to disk with atomic rename semantics, prunes old segments by retention count, and serves stored segments over HTTP so the Beyond boot pipeline can fetch any missing WAL before starting Postgres.

## Context

`beyond-pg-sink` is the "Tier 1.5" durability layer described in `POV.md`. Standard PostgreSQL fsyncs to local SSD, which GlideFS flushes to S3 every ~5s. That 5-second window is acceptable for dev/preview but not for production. Rather than forking Postgres or building a Paxos WAL service, `beyond-pg-sink` receives a synchronous WAL stream. PostgreSQL blocks commits until the sink acknowledges receipt — giving quorum durability with no data copy and no full replica.

The implementation is a native Rust receiver (no `pg_receivewal` subprocess). It speaks the PostgreSQL physical replication protocol directly over TCP, or accepts framed WAL chunks over QUIC from a `wal-forwarder` bridge process.

## Data Flow

### TCP Mode (native receiver)

```
PostgreSQL primary
        │  (physical replication protocol, TCP)
        ▼
  WalReceiver (wal_recv.rs)
    ├── recv_wal() → XLogData frames
    └── WalWriter::write()
          ├── writes {name}.partial  (in progress, fdatasync per chunk)
          └── renames to {name}      (complete, 16 MiB, 24 hex chars)
                    │
                    ├──► send_status() (write_lsn, flush_lsn back to primary)
                    │
                    ├──► inotify IN_MOVED_TO (Linux) or 30s poll (non-Linux)
                    │         │
                    │         ▼
                    │    prune_old_segments()
                    │    (keeps N newest per timeline)
                    │
                    ▼
            WAL dir on disk
                    │
                    ▼
           HTTP server (port 9000)
            ├── GET /list       → sorted newline-separated filenames
            └── GET /{24hex}    → raw segment bytes (sendfile on Linux)
                    │
                    ▼
           beyond-pg boot pipeline
```

### QUIC Mode (forwarder bridge)

```
PostgreSQL primary
        │  (physical replication protocol, TCP)
        ▼
  wal-forwarder (src/bin/wal_forwarder.rs)
    ├── PG reader thread: recv_wal() → frame as [u32 BE len][0x77 + LSNs + data]
    └── QUIC pump thread: quinn-proto client → UDP → beyond-pg-sink

  beyond-pg-sink (quic_recv.rs, --mode quic)
    ├── quinn-proto server (mio UDP)
    ├── recv framed XLogData → WalWriter::write()
    └── send 8-byte ACK (flush_lsn) → forwarder → send_status() to Postgres
                    │
                    ▼
            WAL dir + HTTP server (same as TCP mode)
```

### Startup sequence

```
parse args → extract/strip password → mkdir WAL dir (0o750) →
prune_old_segments() →
install SIGTERM handler → bind HTTP listener →
  ┌─ thread: start_retention_watcher()     (inotify or poll loop)
  ├─ thread: run_native_receiver() or      (TCP: connect → stream → retry)
  │          run_quic_receiver()           (QUIC: UDP bind → accept → stream)
  └─ main:   run_http_server()             (polls SHUTDOWN every 200ms)
```

### Shutdown sequence (SIGTERM)

```
signal handler sets SHUTDOWN=true →
  HTTP accept loop exits (within 200ms) →
  receiver thread exits on next iteration →
  process exits
```

## Concepts & Terminology

| Term                    | What It Controls                                                                            | NOT                                              |
| ----------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| WAL segment             | A 24-hex-char filename holding 16 MiB of raw WAL bytes; unit of replication                 | Parsed, indexed, or checkpointed by this process |
| `.partial` file         | A segment currently being written; excluded from `/list` and pruning                        | A complete segment; never served via HTTP        |
| Replication slot        | PostgreSQL mechanism keeping WAL on the primary until this sink confirms receipt            | A full streaming replica; no standby promotion   |
| Synchronous replication | Postgres blocks commits until `flush_lsn` covers the commit record                          | Full replica; no promotion                       |
| Timeline prefix         | First 8 hex chars of a segment name; used for independent per-timeline retention counting   | A WAL sequence number                            |
| `write_lsn`             | Highest LSN written to OS buffer (acknowledged as "written" to Postgres)                    | Guaranteed durable; that is `flush_lsn`          |
| `flush_lsn`             | Highest LSN synced to disk via `fdatasync`; what Postgres uses to advance the slot          | A commit ACK; Postgres still manages its own WAL |
| Shutdown flag           | `SHUTDOWN` atomic bool; set by SIGTERM handler, polled by all threads                       | Checked inside signal handler (unsafe)           |
| XLogData frame          | Postgres replication protocol message type `w` (0x77); carries raw WAL bytes + LSN metadata | A logical replication message                    |

## Core Mechanism

### WalWriter (`src/wal_recv.rs:37`)

`WalWriter` is the shared write path for both transport modes. It maintains a single open `.partial` file, an `offset` within the current segment, and tracks `write_lsn` / `flush_lsn`.

```
write(start_lsn, data):
  while data is not empty:
    bytes_to_end_of_segment = WAL_SEGMENT_SIZE - offset
    chunk = data[..bytes_to_end_of_segment]
    file.write_all(chunk)
    fdatasync(file)               ← flush to disk on every chunk
    write_lsn = start_lsn + written
    flush_lsn = write_lsn

    if segment is full:
      fsync(file)                 ← full sync before rename
      rename("{name}.partial" → "{name}")   ← atomic on POSIX
      current = None; offset = 0
```

Segment name format: `{timeline:08X}{segment_hi:08X}{segment_lo:08X}` (24 hex chars). Segment number = `lsn / WAL_SEGMENT_SIZE`.

The `.partial` → final rename is the sole visibility gate: HTTP handlers and the retention pruner only see complete segments.

### Native WAL Receiver (`src/wal_recv.rs:141`)

Speaks the Postgres physical replication protocol directly (no `pg_receivewal` subprocess):

1. Connect via libpq TCP
2. `CREATE_REPLICATION_SLOT ... IF NOT EXISTS` (physical, no-export-snapshot)
3. Determine start LSN: slot's `confirmed_flush_lsn`, or the highest complete segment already on disk
4. `START_REPLICATION SLOT ... PHYSICAL` — begins streaming
5. Loop:
   - `XLogData` → `writer.write()` → `send_status(write_lsn, flush_lsn)`
   - `Keepalive(reply_needed=true)` or timeout → `send_status()`
6. On any error: log, sleep 2s, reconnect from step 1

`send_status()` sends a `StandbyStatusUpdate` message. When `flush_lsn` covers a commit, Postgres treats the commit as durable and unblocks the client.

### QUIC Server (`src/quic_recv.rs:75`)

Single OS thread, `mio`-backed event loop, `quinn-proto` state machine:

- Binds UDP to `0.0.0.0:{port}` (the same port as `--port` + offset, or configured separately)
- Accepts one QUIC connection at a time; earlier connections are replaced
- Per connection: one bidirectional stream for WAL
  - Recv: `[u32 BE len][0x77 + 8-byte start_lsn + 8-byte end_lsn + 8-byte ts + data]`
  - Send: `[u64 BE flush_lsn]` — ACK dispatched back to forwarder after each frame
- Calls `WalWriter::write()` per frame; same atomic rename / fdatasync path as TCP

No Tokio runtime: the event loop is a `mio::Poll` over the UDP socket, with `quinn-proto::Connection::poll()` draining connection events after each UDP batch.

### WAL Forwarder (`src/bin/wal_forwarder.rs:134`)

Bridge process for QUIC mode. Runs two threads coordinated by a sync channel:

```
PG reader thread:
  recv_wal() → frame payload
  wal_tx.send((start_lsn, payload))
  waker.wake()            ← interrupt mio.poll()
  ack_rx.recv()           ← BLOCKS until QUIC ACK arrives (back-pressure)
  send_status() to Postgres

QUIC pump thread (main):
  wal_rx.try_recv() → pending_writes queue
  try_drain_writes() → stream writable → send framed data
  poll_transmit() → UDP socket send
  mio.poll(timeout) → drain UDP recv
  read_acks() → parse 8-byte ACKs → ack_tx.send(lsn)
```

The per-frame request/reply discipline (PG reader blocks on `ack_rx`) provides back-pressure: if the QUIC stream stalls, Postgres stalls. Prevents unbounded buffering.

On any error, `run_forever()` retries with exponential backoff (500ms initial, 2× per attempt, capped at 30s).

### Retention Pruner (`src/main.rs:326`)

`prune_old_segments()` scans the WAL dir, filters to complete 24-hex-char names, groups by timeline prefix (first 8 chars), sorts lexicographically within each timeline, and `unlink()`s the oldest beyond `--retention-segments`.

Timelines are pruned independently so a large timeline-2 backlog after failover does not evict timeline-1 segments still needed for recovery.

`unlink()` removes the directory entry but leaves the inode live until all file descriptors are closed. An HTTP handler mid-`sendfile` on a just-pruned segment completes without error (POSIX unlink semantics).

**Trigger:**

- Linux: `inotify IN_MOVED_TO` on the WAL dir — fires exactly when `WalWriter` renames `.partial` → final
- Non-Linux: 30-second poll

### HTTP Server (`src/main.rs:469`)

Accepts TCP connections with a 200ms wakeup to check `SHUTDOWN`. Hard cap of 256 concurrent connections (`CONN_COUNT` atomic); excess connections are dropped immediately.

Routes:

- `GET /list` — reads WAL dir, filters to 24-hex-char names (no `.partial`), returns sorted newline-separated list
- `GET /{24-hex-chars}` — validates name is exactly 24 hex chars (prevents path traversal), opens file, streams bytes
- Otherwise → 404 or 405

Segment delivery uses `sendfile(2)` on Linux (zero-copy); userspace 64 KiB copy loop elsewhere. `EPIPE`/`ECONNRESET` silently discarded.

On non-Linux, accepted sockets inherit `O_NONBLOCK` from the listener; `set_nonblocking(false)` resets them so `write_all` does not silently drop data when the TCP send buffer is temporarily full.

## State Machine

```
[stopped]
    │
    ▼
[starting] ─── bind fails ──────────────────────────────► [exit 1]
    │
    ▼
[running]
    ├─ retention watcher:  inotify/poll → prune → wait (cycle)
    │                      SHUTDOWN → exit
    │
    ├─ receiver loop (TCP): connect → stream → error → sleep 2s → connect
    │  receiver loop (QUIC): UDP bind → accept → stream → lost → accept
    │                        SHUTDOWN → exit
    │
    └─ HTTP server:  accept → handle (per-thread)
                     SHUTDOWN → exit (within 200ms)
    │
    ▼
[stopping]
    │ receiver thread exits; HTTP thread exits
    ▼
[exited]
```

**WAL segment lifecycle:**

```
[nonexistent]
    ↓ WalWriter opens file
[{name}.partial]     ← not in /list; excluded from pruning; never served
    ↓ segment full; fsync; rename()
[{name}]             ← in /list; eligible for pruning; served via GET /{name}
    ↓ retention exceeded; prune_old_segments() calls unlink()
[dir entry removed]  ← inode live until open fds close; sendfile completes
```

| Event                                     | What Actually Happens                                                                     |
| ----------------------------------------- | ----------------------------------------------------------------------------------------- |
| Postgres sends XLogData                   | `write()` + `fdatasync()` per chunk; `send_status()` after each message                   |
| WAL receiver disconnects (any reason)     | Logged; 2s sleep; reconnects from scratch                                                 |
| Segment rename fires (TCP or QUIC)        | inotify wakes retention watcher; `prune_old_segments()` runs synchronously                |
| HTTP handler reads a just-pruned segment  | POSIX unlink leaves inode live; `sendfile` or buffered copy completes normally            |
| QUIC connection lost                      | Marked dead; cleaned up next sweep; server waits for next connection                      |
| SIGTERM                                   | `SHUTDOWN` set; HTTP loop exits within 200ms; receiver exits on next check; process exits |
| 257th HTTP connection arrives             | Connection dropped at TCP level; client sees reset; no response sent                      |
| Request line > 4096 bytes                 | Buffer fills; parsed as-is; likely 400 or 404                                             |
| Segment deleted between `/list` and `GET` | File open fails; 404 returned; client must retry or declare segment missing               |
| Disk full                                 | `write_all` or `fdatasync` fails; receiver loop logs and reconnects after 2s              |

## Configuration

| Flag                   | Default                        | What It Controls                                                                    |
| ---------------------- | ------------------------------ | ----------------------------------------------------------------------------------- |
| `--connstr`            | _(required in TCP mode)_       | PostgreSQL connection string; password stripped and passed via `PGPASSWORD` env var |
| `--dir`                | `/var/lib/postgresql/wal-sink` | Directory where WAL segments are written, retained, and served from                 |
| `--port`               | `9000`                         | TCP port the HTTP server binds                                                      |
| `--slot`               | `wal_sink`                     | Replication slot name; created via `CREATE_REPLICATION_SLOT ... IF NOT EXISTS`      |
| `--retention-segments` | `64`                           | Segments to keep per timeline; oldest pruned when exceeded; minimum 8 enforced      |
| `--mode`               | `quic`                         | Transport: `quic` (QUIC server waiting for forwarder) or `tcp` (native receiver)    |

**Hardcoded constants:**

| Constant           | Value           | Effect                                                                       |
| ------------------ | --------------- | ---------------------------------------------------------------------------- |
| `WAL_SEGMENT_SIZE` | 16 MiB          | Segment size; must match Postgres server `wal_segment_size`                  |
| `MAX_CONNS`        | 256             | Hard HTTP connection cap; excess dropped                                     |
| Accept timeout     | 200ms           | How often the HTTP accept loop checks `SHUTDOWN`                             |
| Receiver restart   | 2s              | Sleep between WAL receiver error and reconnect                               |
| Dir permissions    | `0o750`         | Owner rwx, group rx, world none; set atomically at mkdir                     |
| Retention poll     | 30s (non-Linux) | Fallback poll interval when inotify is unavailable                           |
| HTTP read buffer   | 4096 bytes      | Max request header size                                                      |
| Send buffer        | 64 KiB          | Per-chunk copy buffer for non-Linux segment delivery                         |
| Forwarder backoff  | 500ms → 30s     | Exponential backoff on forwarder failure; doubles per attempt, capped at 30s |

## Trust Boundaries

**What this process verifies:**

- WAL filenames are exactly 24 hex characters (serving and listing) — no directory traversal possible
- HTTP method is GET
- Request fits in 4096 bytes
- `--retention-segments` ≥ 8

**What passes through unchecked:**

- No authentication on the HTTP server — any client on the network can list and download WAL segments
- WAL segment content not validated; bytes served as-is from disk
- No TLS; plaintext HTTP only
- QUIC mode: no client certificate verification (server cert only)

**Why these boundaries are here:**
The sink runs inside the Beyond infrastructure network, not exposed to the internet. If the threat model changes, a reverse proxy with mTLS is the correct layer.

## Failure Modes

| Failure                                 | What Actually Happens                                                     | Recovery                                                   |
| --------------------------------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------- |
| WAL receiver disconnects (any reason)   | Logged; 2s sleep; reconnects; resumes from highest durable LSN            | Automatic; no data loss (slot preserves WAL on primary)    |
| Slot already exists at startup          | `IF NOT EXISTS` makes slot creation a no-op                               | None needed                                                |
| Port already bound at startup           | `bind()` fails; process exits with error                                  | Operator must free port or change `--port`                 |
| inotify unavailable (non-Linux)         | Logged; retention watcher falls back to 30s polling                       | Automatic                                                  |
| SIGTERM during active HTTP connections  | Accept loop exits; in-flight connections may be interrupted mid-transfer  | Clients retry                                              |
| Disk full                               | `write_all`/`fdatasync` fails in receiver loop; 2s retry                  | Operator must free space; automatic recovery on next retry |
| Client disconnects mid-transfer         | `EPIPE`/`ECONNRESET` silently discarded                                   | None needed                                                |
| Malformed HTTP request                  | 400 or 404; connection closed                                             | None needed                                                |
| Segment deleted between `/list` and GET | 404 returned                                                              | Client retries; if permanently missing, recovery from S3   |
| QUIC connection lost                    | Dead connection cleaned up; server accepts next connection from forwarder | Forwarder reconnects with exponential backoff              |
| Forwarder crashes                       | QUIC server waits; forwarder restarts with backoff                        | Automatic via forwarder's `run_forever()` loop             |

## Benchmarks

Measured with `cargo test -p beyond-pg-sink --test e2e -- --ignored --nocapture latency_baseline_real`.
Setup: N=2000 single-row transactions, `synchronous_commit=remote_write`, Postgres 18 in Docker,
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

QUIC is the default transport. The gains come from two sources:

1. **Lower RTT**: UDP with quinn-proto's single-RTT 0-RTT resumption vs TCP's three-way handshake
   and kernel scheduler latency on each write.
2. **No head-of-line blocking**: QUIC streams don't stall on retransmit; the WAL stream is a
   single bidi stream so ordering is preserved without blocking on unrelated retransmits.

The p99/max improvement (8–25×) is larger than the median improvement (6×) because QUIC eliminates
TCP's occasional kernel-scheduler pauses on the ACK path.

## Files

| File                       | What It Does                                                                                            |
| -------------------------- | ------------------------------------------------------------------------------------------------------- |
| `src/main.rs`              | Entry point, arg parsing, thread spawning, signal handling, HTTP server, retention watcher              |
| `src/wal_recv.rs`          | Native Postgres replication protocol client; `WalWriter` (segment writes, atomic rename, fdatasync)     |
| `src/quic_recv.rs`         | QUIC server (quinn-proto + mio); framed XLogData reception; ACK dispatch                                |
| `src/bin/wal_forwarder.rs` | Bridge process: dual-thread Postgres reader + QUIC pump; back-pressure via per-frame ACK blocking       |
| `Cargo.toml`               | Single workspace member; `libc` (Linux-only for inotify/sendfile); `quinn-proto`, `mio` for QUIC        |
| `tests/e2e.rs`             | End-to-end tests: Postgres 18 container; verifies TCP and QUIC modes, `/list`, segment content, latency |
