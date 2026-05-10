# beyond-pg-sink Architecture

Takes a PostgreSQL connection string, spawns `pg_receivewal` to stream WAL segments to a local directory, prunes old segments by retention count, and serves stored segments over HTTP so the Beyond boot pipeline can fetch any missing WAL before starting Postgres.

## Context

`beyond-pg-sink` is the "Tier 1.5" durability layer described in `POV.md`. Standard PostgreSQL fsyncs to local SSD, which GlideFS flushes to S3 every ~5s. That 5-second window is acceptable for dev/preview but not for production. Rather than forking Postgres or building a Paxos WAL service, `beyond-pg-sink` receives a synchronous WAL stream via `pg_receivewal --synchronous`. PostgreSQL waits for the sink to confirm each segment before acking a commit — giving quorum durability with no data copy and no full replica.

## Data Flow

```
PostgreSQL WAL stream
        │
        ▼
  pg_receivewal
    ├── writes {name}.partial (in progress)
    └── renames to {name}     (complete, 24 hex chars)
                  │
                  ├──► inotify IN_MOVED_TO (Linux)
                  │    or 30s poll (non-Linux)
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

**Startup sequence:**

```
parse args → extract/strip password → mkdir WAL dir (0o750) →
prune_old_segments() (clear excess before streaming starts) →
install SIGTERM handler → bind TCP listener →
  ┌─ thread: start_retention_watcher()   (inotify or poll loop)
  ├─ thread: run_pg_receivewal()          (two-phase: slot creation, then streaming)
  └─ main:   run_http_server()            (polls SHUTDOWN every 200ms)
```

**Shutdown sequence (SIGTERM):**

```
signal handler sets SHUTDOWN=true →
  HTTP accept loop exits →
  pg_receivewal subprocess killed (SIGTERM via CHILD_PID) →
  receiver thread joined → process exits
```

## Concepts & Terminology

| Term                    | What It Controls                                                                                            | NOT                                               |
| ----------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| WAL segment             | A 24-hex-char filename holding raw WAL bytes; unit of replication                                           | Parsed, indexed, or checkpointed by this process  |
| `.partial` file         | A segment currently being written by `pg_receivewal`; excluded from `/list` and pruning                     | A complete segment; never served via HTTP         |
| Replication slot        | PostgreSQL mechanism keeping WAL from being pruned on the primary until this sink has received it           | Created externally; slot name passed via `--slot` |
| Synchronous replication | `pg_receivewal --synchronous` makes PostgreSQL block commits until this sink acknowledges receipt           | Full streaming replication; no standby promotion  |
| Timeline prefix         | First 8 hex chars of a segment name; used to group segments for independent per-timeline retention counting | A WAL sequence number                             |
| Shutdown flag           | `SHUTDOWN` atomic; set by SIGTERM handler, polled by all three threads                                      | Checked inside signal handler (unsafe)            |

## Core Mechanism

The process runs three concurrent threads sharing three atomics with no mutexes:

```
SHUTDOWN:   AtomicBool  — set by SIGTERM handler, polled by all three loops
CHILD_PID:  AtomicI32   — pg_receivewal OS pid; main thread kills it on shutdown
CONN_COUNT: AtomicUsize — live HTTP connections; hard cap at 256
```

**Retention watcher thread** (`start_retention_watcher`, `src/main.rs:316`):
On Linux, registers an inotify watch on the WAL dir for `IN_MOVED_TO` events. `pg_receivewal` completes a segment by renaming `{name}.partial` → `{name}`, which fires `IN_MOVED_TO` exactly when pruning is needed. On non-Linux, falls back to a 30-second poll. Either way, calls `prune_old_segments()` each time.

**Segment pruning** (`prune_old_segments`, `src/main.rs:269`):
Scans the WAL dir, filters to complete segments (exactly 24 hex chars, no `.partial` suffix), groups by timeline prefix (first 8 chars), sorts each group lexicographically, and deletes the oldest beyond `--retention-segments`. Timeline groups are pruned independently so a large timeline-2 backlog after failover doesn't evict timeline-1 segments still needed for recovery. Uses POSIX `unlink`, which removes the directory entry but leaves the inode live until all open file descriptors close — an HTTP handler mid-`sendfile` on a pruned segment completes without error.

**WAL receiver thread** (`run_pg_receivewal`, `src/main.rs:392`):
Two-phase startup before the streaming loop:

1. **Slot creation phase**: Spawns `pg_receivewal --create-slot --if-not-exists --slot <slot>`. This call exits immediately after slot creation; it does not stream. Retries every 2 seconds until success.
2. **Streaming phase**: Spawns `pg_receivewal --synchronous --slot <slot> --directory <dir>`. Stores its PID in `CHILD_PID`. On any exit (crash, error, or EOF), logs the status, sleeps 2 seconds, and restarts. The loop breaks only when `SHUTDOWN` is set.

**HTTP server** (`run_http_server`, `src/main.rs:469`):
Accepts TCP connections with a 200ms wakeup (Linux: `SO_RCVTIMEO` on the listener socket; non-Linux: `set_nonblocking(true)` + 100ms sleep on `WouldBlock`). On non-Linux, accepted sockets inherit `O_NONBLOCK` from the listener; `set_nonblocking(false)` resets them to blocking so `write_all` doesn't silently drop data when the TCP send buffer is temporarily full. If `CONN_COUNT` is already at 256, the connection is dropped immediately. Otherwise, increments `CONN_COUNT` and spawns a per-connection thread.

**Request handling** (`handle_conn`, `src/main.rs:532`):
Reads up to 4096 bytes, parsing until `\r\n\r\n` or buffer full. Parses the HTTP/1.x request line, then routes:

- `GET /list` → `serve_list()`: reads the WAL dir, filters to exactly 24 hex chars, returns them sorted and newline-separated.
- `GET /{24-hex-chars}` → `serve_segment()`: validates name is exactly 24 hex chars (no path traversal possible), opens the file, streams it.
- Anything else → 404 or 405.

**Segment delivery** (`serve_segment`, `src/main.rs:622`):
On Linux, uses `sendfile(2)` for zero-copy transfer in a loop until all bytes are sent. On other platforms, copies through a 65536-byte userspace buffer. Both paths silently discard `EPIPE`/`ECONNRESET` (normal client disconnects).

**Password handling** (`extract_password` / `strip_kv_password`, `src/main.rs:106`):
Strips the password from the connection string (both URI and `key=value` formats) before passing it to `pg_receivewal`, then sets `PGPASSWORD` in the subprocess environment. Prevents the password from appearing in `/proc/*/cmdline` or process listings.

## State Machine

```
[stopped]
    │
    ▼
[starting] ─── bind fails ──────────────────────────────► [exit 1]
    │
    ▼
[running]
    ├─ retention watcher:  waiting for inotify/poll → prune → waiting (cycle)
    │                      SHUTDOWN=true → watcher thread exits
    │
    ├─ pg_receivewal loop: slot creation (retry) → streaming → crashed → sleep 2s → streaming
    │                      streaming → SHUTDOWN=true → [stopping]
    │
    └─ HTTP server loop:   listening → accept → handle (per-thread)
                           listening → SHUTDOWN=true → [stopping]
    │
    ▼
[stopping]
    │ kill CHILD_PID → join receiver thread
    ▼
[exited]
```

**WAL segment lifecycle:**

```
[nonexistent]
    ↓ pg_receivewal begins write
[{name}.partial]          ← not served; excluded from /list and pruning
    ↓ write complete; pg_receivewal renames
[{name}]                  ← served via GET /{name}; eligible for pruning
    ↓ retention exceeded; prune_old_segments() calls unlink()
[directory entry removed] ← inode live until all open fds closed (POSIX semantics)
```

| Event                                         | What Actually Happens                                                               |
| --------------------------------------------- | ----------------------------------------------------------------------------------- |
| `pg_receivewal` crashes                       | Restarted after 2s; existing WAL files still served; `.partial` files left in place |
| `pg_receivewal` down, client requests `/list` | Serves existing complete segments; no error                                         |
| Segment completed (rename fires)              | inotify wakes retention watcher; `prune_old_segments()` runs synchronously          |
| HTTP handler reads a just-pruned segment      | POSIX unlink leaves inode live; `sendfile` or buffered copy completes normally      |
| SIGTERM                                       | `SHUTDOWN` set; HTTP loop exits within 200ms; subprocess killed; thread joined      |
| 257th connection arrives                      | Connection dropped at TCP level (stream dropped); client sees a reset; no response  |
| Request > 4096 bytes                          | Buffer fills; request parsed as-is; likely 400 or 404                               |

## Configuration

| Flag                   | Default                        | What It Controls                                                                         |
| ---------------------- | ------------------------------ | ---------------------------------------------------------------------------------------- |
| `--connstr`            | _(required)_                   | PostgreSQL connection string (URI or `key=value`); password stripped before subprocess   |
| `--dir`                | `/var/lib/postgresql/wal-sink` | Directory where WAL segments are written and served from                                 |
| `--port`               | `9000`                         | TCP port the HTTP server binds                                                           |
| `--slot`               | `wal_sink`                     | Replication slot name; created by `pg_receivewal --create-slot --if-not-exists` at start |
| `--retention-segments` | `64`                           | Segments to keep per timeline; oldest pruned when exceeded; minimum 8                    |

**Hardcoded constants:**

| Constant         | Value           | Effect                                                                                                              |
| ---------------- | --------------- | ------------------------------------------------------------------------------------------------------------------- |
| `MAX_CONNS`      | 256             | Hard connection cap; excess dropped                                                                                 |
| Accept timeout   | 200ms           | How often the HTTP accept loop checks `SHUTDOWN` (Linux: `SO_RCVTIMEO`; non-Linux: `set_nonblocking` + 100ms sleep) |
| Restart delay    | 2s              | Sleep between `pg_receivewal` crash and restart; also between slot-creation retries                                 |
| Dir permissions  | `0o750`         | Owner rwx, group rx, world none; re-applied on pre-existing directories (Linux only)                                |
| Retention poll   | 30s (non-Linux) | Fallback poll interval for retention watcher when inotify is unavailable                                            |
| HTTP read buffer | 4096 bytes      | Max request header size; oversized requests parsed as-is, likely resulting in 404                                   |
| Send buffer      | 65536 bytes     | Per-chunk copy buffer for non-Linux segment delivery                                                                |

## Trust Boundaries

**What this process verifies:**

- WAL filenames are exactly 24 hex characters (both listing and serving) — no directory traversal
- HTTP method is GET
- Request fits in 4096 bytes

**What passes through unchecked:**

- No authentication on the HTTP server — any client on the network can list and download WAL segments
- No validation of WAL segment content; bytes are served as-is from disk
- No TLS; plaintext HTTP only

**Why these boundaries are here:**
The sink runs inside the Beyond infrastructure network, not exposed to the internet. WAL bytes are not sensitive in the same way as user data, but access should be network-scoped. If the threat model changes, a reverse proxy with mTLS in front is the right layer.

## Failure Modes

| Failure                                 | What Actually Happens                                                  | Recovery                                                  |
| --------------------------------------- | ---------------------------------------------------------------------- | --------------------------------------------------------- |
| `pg_receivewal` exits (any reason)      | Logged; 2s sleep; restarted                                            | Automatic; no operator action                             |
| `pg_receivewal` fails to launch         | Launch error logged; 2s sleep; retried indefinitely                    | Automatic until binary is found or slot creation succeeds |
| Slot already exists at startup          | `--if-not-exists` makes `pg_receivewal --create-slot` succeed silently | None needed                                               |
| Port already bound at startup           | `bind()` fails; process exits with error                               | Operator must free port or change `--port`                |
| inotify unavailable (non-Linux)         | Logged; retention watcher falls back to 30s polling                    | Automatic; no operator action                             |
| SIGTERM during active connections       | HTTP loop exits; in-flight connections may be interrupted mid-transfer | Connections interrupted; clients retry                    |
| Disk full                               | `pg_receivewal` writes fail; it exits; restart loop kicks in           | Operator must free space; restart loop handles recovery   |
| Client disconnects mid-transfer         | `EPIPE`/`ECONNRESET` silently discarded                                | None needed                                               |
| Malformed HTTP request                  | 400 or 404 response; connection closed                                 | None needed                                               |
| Segment deleted between `/list` and GET | File open fails; 404 returned                                          | Client retries fetch; if segment is gone, recovery needed |

## Files

| File           | What It Does                                                                                                    |
| -------------- | --------------------------------------------------------------------------------------------------------------- |
| `src/main.rs`  | Entire implementation (~860 lines); no modules; `libc` used for signal handling, inotify, and sendfile          |
| `Cargo.toml`   | Single binary crate; `libc` dependency on Linux only; dev deps: `postgres`, `testcontainers-modules`            |
| `tests/e2e.rs` | End-to-end test: spins up a Postgres 18 container, verifies replication slot, `/list`, and segment byte content |
