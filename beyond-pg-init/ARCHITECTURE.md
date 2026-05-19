# beyond-pg-init Architecture

Takes a freshly-booted Firecracker VM from kernel handoff to a running `beyond-pg supervisor`, then serves as a lifecycle control plane for the lifetime of the VM — handling supervisor crashes, zero-downtime binary upgrades, and ordered shutdown.

## Data Flow

### Boot phase (one-shot, synchronous)

```
Kernel boot
     │
     ▼
[PID guard: exit(1) if pid != 1]
     │
     ▼
bootsetup::run()
     │
     ├─ set PATH
     │
     ├─ mount /proc  ──── fails? ──► exit(1) FATAL
     │
     ├─ mount /sys, /sys/fs/cgroup, /dev, /dev/pts, /dev/shm, /run
     │                               fails? ──► eprintln WARNING (boot continues)
     │
     ├─ setup_network()
     │     ├─ ip route add 169.254.169.254 dev eth0   (MMDS link-local)
     │     ├─ IPv6 addr + default route (from /proc/cmdline ipv6=<addr>/<prefix>@<gw>)
     │     ├─ IPv6 GUA /128 (from /proc/cmdline ipv6_ext=<addr>/...)
     │     └─ write /etc/resolv.conf  (IPv6 gw → 8.8.8.8 fallback)
     │
     ├─ fetch_mmds()
     │     ├─ PUT /latest/api/token  →  IMDSv2 token (300s TTL)
     │     ├─ GET /  (30 retries × 10ms)
     │     │     └─ HTTP 200 + JSON body → write atomically to /run/mmds/metadata.json
     │     └─ on failure → POSTGRES_PASSWORD env var → synthetic JSON
     │                            absent? ──► exit(1) FATAL
     │
     └─ setup_zram()
           ├─ skip if zram0 already in /proc/swaps
           ├─ modprobe zram
           ├─ set comp_algorithm = lz4 (if supported)
           ├─ set disksize = min(RAM/10, 256 MiB)
           └─ mkswap + swapon -p 100 /dev/zram0
```

### Supervise phase (long-running poll loop)

```
supervise::run()
     │
     ├─ block SIGTERM/SIGINT → signalfd  (converts signals to poll-readable fd)
     ├─ bind vsock LIFECYCLE_PORT (5429)  →  lifecycle_vsock
     ├─ bind vsock RPC_PORT (5430)        →  rpc_fd        ← passed to supervisor
     ├─ bind unix /run/beyond-pg/lifecycle.sock  →  lifecycle_unix
     ├─ handoff::Supervisor::new() + with_listener("rpc", rpc_fd) + resume_from_journal()
     └─ spawn_supervisor_with_pidfd(rpc_fd)  →  (supervisor_pid, supervisor_pidfd)
           │
           ▼
     poll([signalfd, supervisor_pidfd, lifecycle_vsock, lifecycle_unix], timeout=-1)
           │
           ├─ signalfd readable ──────────────────────────────────────┐
           │                                                           │
           ├─ supervisor_pidfd readable (supervisor exited)           │
           │     ├─ waitpid WNOHANG (reap zombie)                     │
           │     ├─ reap_orphans()                                     │
           │     └─ loop: sleep 1s → spawn_supervisor_with_pidfd      │
           │                                                           │
           ├─ lifecycle_vsock readable                                 │
           │     └─ accept → handle_lifecycle(stream, &sup) ──┐       │
           │                                                   │       │
           └─ lifecycle_unix readable                          │       │
                 └─ accept → handle_lifecycle(stream, &sup) ──┘       │
                                                                       │
handle_lifecycle:                                                      │
     read 4-byte big-endian length                                     │
     read body (≤64 KiB)                                               │
     deserialize LifecycleCmd (JSON, deny_unknown_fields)              │
          │                                                            │
          ├─ Upgrade { binary } ──► validate_upgrade_binary()          │
          │       ├─ reject: len > PATH_MAX (4096)                     │
          │       ├─ reject: not absolute                              │
          │       ├─ reject: contains ".." components                  │
          │       ├─ reject: not under ALLOWED_UPGRADE_PREFIXES        │
          │       └─ ok → sup.perform_handoff(SpawnSpec::new(path))    │
          │                                                            │
          ├─ Shutdown ──► kill(getpid(), SIGTERM)  ←── queued to signalfd
          │                                                            │
          └─ Status ──► { ok: true }                                   │
                                                                       │
     write 4-byte length + JSON response                               │
                                                                       │
shutdown path (from signalfd): ◄──────────────────────────────────────┘
     drain_supervisor(supervisor_pid, &supervisor_pidfd)
          ├─ pidfd_send_signal(SIGTERM)
          ├─ poll pidfd (10s timeout)
          └─ timeout → pidfd_send_signal(SIGKILL) + poll (1s) + waitpid WNOHANG
     reap_orphans()  (waitpid -1 WNOHANG loop)
     sync()
     reboot(LINUX_REBOOT_CMD_POWER_OFF)
```

## Concepts & Terminology

| Term                  | What It Controls                                                                                                                          | NOT                                                                                               |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| MMDS                  | Firecracker's link-local metadata service at `169.254.169.254`; delivers VM config (password, tier, archive target, etc.) as JSON         | A durable store; data is only present while the VM is running and the hypervisor has populated it |
| Lifecycle socket      | The vsock port 5429 (and unix fallback) that accepts `Upgrade`/`Shutdown`/`Status` from the host; owned permanently by init               | The RPC socket (5430), which is passed to and owned by the supervisor                             |
| RPC socket            | vsock port 5430; bound once by init, duplicated into every supervisor generation via `LISTEN_FDS`; never closed by init                   | A socket init listens on; init hands it off and never reads from it                               |
| `handoff::Supervisor` | The `beyond-handoff` crate coordinator for zero-downtime binary swaps; tracks the handoff journal and orchestrates drain → spawn → commit | The process being supervised; the supervisor is the `beyond-pg supervisor` binary                 |
| pidfd                 | Linux file descriptor for a specific process instance; readable (via `poll`) when the process exits; immune to PID recycling races        | A plain numeric PID; those can be reassigned after reap                                           |
| Handoff journal       | WAL of in-progress handoff state at `STATE_DIR/handoff.journal`; replayed on cold boot to recover from a crash mid-handoff                | A persistent config file; it holds transient handoff coordination state only                      |
| Drain                 | Signal the incumbent supervisor to stop accepting new RPCs and wait for in-flight handlers to finish before a handoff                     | A kill; drain is cooperative and bounded by a deadline                                            |

## Core Mechanism

### PID 1 responsibilities

`beyond-pg-init` runs as PID 1 because Firecracker VMs boot without a system supervisor. The Linux kernel delivers all signals that have no other target to PID 1, and zombies whose parent is dead are reparented to PID 1. Init handles both:

- **Zombie reaping**: `reap_orphans()` calls `waitpid(-1, WNOHANG)` in a loop after every supervisor exit and before every poweroff. Without this, terminated grandchildren (postgres, pgbouncer) would accumulate as zombies and eventually exhaust the kernel's process table.
- **SIGTERM forwarding**: the host sends SIGTERM to PID 1 to initiate shutdown. Init converts this to a structured drain + poweroff rather than an uncontrolled kill.

### Signal handling via signalfd

SIGTERM and SIGINT are blocked with `sigprocmask` at startup, then redirected to a `signalfd`. This makes signals poll-able alongside the other four fds in the main loop — no signal handlers, no async-signal-safety constraints, no re-entrancy problems. The signalfd has `SFD_CLOEXEC` so it doesn't leak into the supervisor child.

The `Shutdown` lifecycle command exploits this: it sends `kill(getpid(), SIGTERM)` to queue the signal into the signalfd. Because SIGTERM is blocked, it can't interrupt the dispatch path mid-response; the response is written first, then the next `poll` iteration sees the signalfd readable and begins the drain sequence.

### Supervisor respawn

When `supervisor_pidfd` becomes readable, the supervisor has exited. Init:

1. Reaps the zombie with `waitpid(WNOHANG)`.
2. Reaps any orphaned grandchildren.
3. Loops: `sleep(1s)` → `cold_start_supervisor(rpc_fd)` → `pidfd_open(pid)`.

The `spawn_supervisor_with_pidfd` function treats `pidfd_open` failure as fatal for that attempt: if `pidfd_open` returns `ESRCH` (process already gone), it skips the kill and retries. For other errors (EPERM, ENOSYS), it kills the orphaned child via numeric PID (safe because the pid slot is still held by the unreaped child), reaps it, and retries. This guarantees that the main loop always holds a valid `supervisor_pidfd` — a stale readable pidfd would spin the CPU on every `poll` iteration.

### RPC socket ownership

Init binds `RPC_PORT` (vsock 5430) once at boot and holds the `OwnedFd` for the VM's lifetime. On every supervisor spawn (cold start and handoff respawn), the fd is duplicated into the child's fd slot 3 via `handoff::pass_listener_fds_on_spawn`. The kernel's `accept(2)` queue is attached to the bound socket, not to any process — connections queued while the supervisor is being replaced are not dropped.

### Zero-downtime upgrade (handoff)

An `Upgrade { binary }` lifecycle command triggers `handoff::Supervisor::perform_handoff(SpawnSpec::new(path))`. The `beyond-handoff` crate:

1. Signals the incumbent supervisor's `Drainable::drain()` — stops accepting new RPCs, waits for in-flight handlers to complete (up to a deadline).
2. Calls `Drainable::seal()` — no-op for `beyond-pg` because postgres is the durable source of truth; there is no in-process state to flush.
3. Spawns the new binary with the same `rpc_fd` passed via `LISTEN_FDS`.
4. Runs the handoff protocol handshake between incumbent and successor.
5. Commits: terminates the incumbent, updates `supervisor_pid`/`supervisor_pidfd` in the main loop.
6. On abort (new binary fails to start, handoff times out): calls `resume_after_abort()` on the drainable, which re-enables RPC accept.

The lifecycle socket (port 5429) is **not** part of the handoff. It is bound by init and served inline on the init thread for the VM's lifetime. An upgrade cannot disrupt the channel that initiated it.

### MMDS fetch

`fetch_mmds()` runs before the supervisor is started and before tokio is running. It uses raw `TcpStream` with 200ms timeouts, retrying up to 30 times at 10ms intervals (300ms total window). Firecracker's MMDS may not be ready at the instant the guest kernel boots, so the retries absorb hypervisor-side initialization lag.

The result is written atomically: `serde_json::to_string_pretty` → write to `{MMDS_PATH}.tmp` → `rename(tmp, MMDS_PATH)`. The rename is an atomic operation on Linux for same-filesystem paths, so the supervisor never reads a partially-written file.

The IMDSv2-style token (`PUT /latest/api/token`, TTL 300s) is fetched first and attached to the `GET /` request. If the PUT fails or returns non-200, the GET proceeds without a token — some Firecracker configurations don't require IMDSv2.

### zram swap

Sized to `min(MemTotal / 10, 256 MiB)` with lz4 compression. Priority 100 (`swapon -p 100`) makes the kernel prefer zram over any other swap device. Setup is idempotent: if `zram0` already appears in `/proc/swaps` (e.g., re-exec after a crash), the entire setup block is skipped.

## State Machine

`beyond-pg-init` does not have an explicit state machine, but the supervisor child has an implicit two-state lifecycle visible to init:

```
cold_start
    │
    ▼
running ◄──── respawn (1s backoff)
    │                    ▲
pidfd readable           │
    │                    │
    ▼                    │
exited ──────────────────┘
    │
(if shutdown in progress)
    │
    ▼
poweroff
```

| From    | Event                    | To       | What Actually Happens                                                         |
| ------- | ------------------------ | -------- | ----------------------------------------------------------------------------- |
| —       | boot complete            | running  | `spawn_supervisor_with_pidfd` called; rpc_fd duplicated into child fd 3       |
| running | pidfd readable           | exited   | zombie reaped, orphans reaped                                                 |
| exited  | respawn succeeds         | running  | new `supervisor_pid` + `supervisor_pidfd` set; loop resumes                   |
| exited  | respawn fails            | exited   | error logged; retry after 1s                                                  |
| running | SIGTERM/SIGINT           | poweroff | drain_supervisor → reap_orphans → sync() → reboot(POWER_OFF)                  |
| running | `Shutdown` lifecycle cmd | poweroff | SIGTERM queued to signalfd; response sent; main loop drains on next iteration |
| running | `Upgrade` lifecycle cmd  | running  | handoff::perform_handoff; supervisor_pid/pidfd updated on commit              |

## Trust Boundaries

**What the system verifies (rejects if invalid):**

- Lifecycle request body size: rejected if > 64 KiB (protects against OOM in `vec![0u8; len]`).
- Lifecycle command shape: unknown `cmd` discriminator values return a deserialization error → `{ ok: false }` without panicking. `deny_unknown_fields` rejects extra fields on the `Upgrade` struct variant (e.g. `{"cmd":"upgrade","binary":"/x","extra":1}`). Note: serde's `deny_unknown_fields` does **not** apply to unit variants of internally-tagged enums, so extras alongside `{"cmd":"status"}` or `{"cmd":"shutdown"}` are silently ignored. This is harmless for dispatch — the variant routes correctly — but it means the `Status`/`Shutdown` envelopes are not strictly schema-checked.
- Upgrade binary path:
  - Length ≤ `PATH_MAX` (4096 bytes)
  - Must be absolute
  - Must not contain `..` components (kernel canonicalization could escape prefix checks)
  - Must start with `/usr/local/bin/` or `/usr/local/sbin/`
- MMDS token: CRLF characters stripped before use as an HTTP header value.

**What passes through unchecked:**

- The identity of who sent the lifecycle command — any process that can connect to vsock port 5429 or the unix socket is trusted. The vsock endpoint is only reachable from the host hypervisor; the unix socket is only reachable within the VM.
- MMDS metadata values beyond structural validation (parsing `MmdsConfig` is done in the supervisor, not init).
- The new supervisor binary's behavior after exec — init verifies the path, not the binary.

**Why these boundaries are where they are:**

- The lifecycle vsock is a host-to-guest channel; physical access to the vsock device is equivalent to root on the host, so additional authN adds no security value.
- Binary path validation guards against a malformed or confused host request accidentally exec-ing an arbitrary file. It does not guard against a compromised host.

## Package Structure

| File               | What It Does                                                                                                                                                                                                                                                                                           |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `src/main.rs`      | PID guard (`exit(1)` if not PID 1), sequences `bootsetup::run()` then `supervise::run()`. Linux-only via `#[cfg]`; prints an error and exits on non-Linux.                                                                                                                                             |
| `src/bootsetup.rs` | One-shot setup: mounts, network, MMDS fetch + atomic write, zram. Contains `unsafe` blocks for `libc::mount`, `std::env::set_var`. Sync only.                                                                                                                                                          |
| `src/supervise.rs` | Long-running `poll(2)` loop: signalfd, supervisor pidfd, lifecycle vsock, lifecycle unix. Lifecycle protocol framing and dispatch. Shutdown sequence. Contains `unsafe` blocks for `pidfd_open`, `pidfd_send_signal`, vsock `socket`/`bind`/`accept4`, `sigprocmask`, `signalfd`, `reboot`. Sync only. |

## Configuration

All configuration arrives via MMDS at boot. `beyond-pg-init` itself has no configuration flags; the kernel command line is read only for network parameters:

| `/proc/cmdline` Parameter | Format                                                    | What It Controls                                                                                   |
| ------------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `ipv6=`                   | `<addr>/<prefix>@<gateway>`                               | Primary IPv6 address and default route assigned to eth0                                            |
| `ipv6_ext=`               | `<addr>/<prefix>`                                         | Additional GUA /128 alias added to eth0 (only the address is used; prefix is parsed but discarded) |
| `ip=`                     | `ip=<addr>:<server>:<gw>:...` (classic Linux boot format) | IPv4 gateway used as fallback DNS nameserver if no IPv6 is configured                              |

| Compile-time constant      | Value                                 | What It Controls                                                                           |
| -------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------ |
| `MMDS_MAX_ATTEMPTS`        | 30                                    | Total retries before MMDS is declared unavailable                                          |
| `MMDS_RETRY`               | 10ms                                  | Sleep between MMDS fetch attempts                                                          |
| `HTTP_TIMEOUT`             | 200ms                                 | Per-request connect + read + write timeout for MMDS HTTP                                   |
| `MAX_RESPONSE_BYTES`       | 64 KiB                                | Maximum bytes read from a single MMDS response                                             |
| `DRAIN_TIMEOUT_MS`         | 10s                                   | How long init waits for the supervisor to exit after SIGTERM before escalating to SIGKILL  |
| `SIGKILL_REAP_TIMEOUT_MS`  | 1s                                    | How long init waits for the supervisor to exit after SIGKILL before proceeding to poweroff |
| `RESTART_BACKOFF`          | 1s                                    | Sleep between supervisor crash and respawn attempt                                         |
| `ALLOWED_UPGRADE_PREFIXES` | `/usr/local/bin/`, `/usr/local/sbin/` | Path prefixes that `Upgrade` commands may target                                           |

## Failure Modes

| Failure                                   | What Actually Happens                                                         | Recovery                                                                            |
| ----------------------------------------- | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `/proc` mount fails                       | `exit(1)` immediately — no further initialization                             | None; VM is unusable without `/proc`                                                |
| Other filesystem mount fails              | `eprintln!` WARNING; boot continues                                           | Best-effort; postgres may work without e.g. `/dev/shm` on older kernels             |
| MMDS unreachable after 30 retries (300ms) | Falls back to `POSTGRES_PASSWORD` env var                                     | If env var also absent, `exit(1)` FATAL                                             |
| MMDS write fails                          | `exit(1)` FATAL                                                               | None                                                                                |
| `zram` unavailable                        | WARNING; no swap configured                                                   | Postgres runs without swap; OOM killer may terminate it under memory pressure       |
| Supervisor crashes                        | `pidfd` becomes readable; supervisor reaped; respawn after 1s with retry loop | Automatic; in-flight RPCs at crash time are lost                                    |
| `pidfd_open` fails after spawn (`ESRCH`)  | Kill skipped; retry after 1s                                                  | Process was already gone; clean retry                                               |
| `pidfd_open` fails after spawn (other)    | Orphaned child killed via numeric PID; reaped; retry after 1s                 | SIGKILL + waitpid before retry                                                      |
| Handoff journal replay fails              | WARNING logged; supervisor starts cold                                        | In-flight handoffs at time of crash are lost; new supervisor boots fresh            |
| Handoff `perform_handoff` fails or aborts | `resume_after_abort()` called; RPC accept loop re-enabled                     | Incumbent supervisor continues running as if the upgrade never happened             |
| Lifecycle request body > 64 KiB           | Error response sent; connection closed                                        | No state change; caller can retry                                                   |
| Upgrade binary path outside allow-list    | Error response `{ ok: false, error: "..." }`                                  | No exec attempted; caller can retry with a valid path                               |
| Shutdown during handoff                   | SIGTERM queued; main loop sees it after `perform_handoff` returns             | If handoff was in progress it completes (or aborts) first, then drain+poweroff runs |
| `poweroff` syscall fails                  | `exit(1)` — kernel panics when PID 1 exits                                    | Kernel panic causes Firecracker to halt the VM                                      |
