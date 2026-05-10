# Phase 1 — `beyond-pg` binary

Build `beyond-pg`. One binary that is PID 1 in the Firecracker VM —
handles Linux init responsibilities, boots Postgres, supervises it and
PgBouncer, ships logs over vsock, and serves control RPC.

This is the most code-heavy phase. Can run in parallel with phase 2
once phase 0's skeleton exists.

## Goal

A working `beyond-pg` binary that, in Firecracker, is PID 1 and brings
up a healthy Postgres + PgBouncer with logs flowing over vsock and the
vsock RPC reachable. In Docker for local dev, runs with env var config
(no MMDS, no vsock) and logs to stderr. Tested standalone before being
baked into the image.

## Dependencies

- Phase 0 (repo skeleton).

## Architecture

```
beyond-pg
├── main.rs                # subcommand dispatch; PID 1 detection
├── init.rs                # PID 1 responsibilities: mount, network, zram
├── supervisor.rs          # `beyond-pg supervisor` — long-running entry
├── boot.rs                # boot-time setup; called inline by supervisor; also `beyond-pg boot`
├── archive.rs             # `beyond-pg archive %p %f` per-WAL hook
├── rpc.rs                 # vsock RPC server (used by supervisor)
├── log_forwarder.rs       # pipe stdio → vsock log frames
├── mmds.rs                # MMDS HTTP client; reads boot-time config
├── config.rs              # write conf.d/00-beyond.conf, 01-tuning.conf, 02-durability.conf
└── pg.rs                  # thin wrapper around psql, pg_isready, pg_ctl
```

## Tasks

0. **PID 1 init (`init.rs`).** `beyond-pg` detects `getpid() == 1` in
   `main.rs` and calls `init::run()` before anything else. `init::run()`
   performs the Linux init responsibilities that `beyond-init` used to
   handle, then falls through to `supervisor::run()`.

   Steps, in order:

   a. **Mount essential filesystems.** Same set as `beyond-init`:

   ```rust
   mount("proc",  "/proc",     "proc",   MS_NOSUID|MS_NOEXEC|MS_NODEV, "");
   mount("sys",   "/sys",      "sysfs",  MS_NOSUID|MS_NOEXEC|MS_NODEV, "");
   mount("dev",   "/dev",      "devtmpfs", MS_NOSUID|MS_STRICTATIME,   "");
   mount("devpts","/dev/pts",  "devpts", MS_NOSUID|MS_NOEXEC,          "");
   mount("run",   "/run",      "tmpfs",  MS_NOSUID|MS_NODEV,           "");
   ```

   Use `nix::mount::mount()`. Fail hard if any mount fails — if we can't
   mount `/proc`, we have nothing.

   b. **MMDS route.** IPv4 is already configured by the kernel via the
   `ip=<addr>::<gw>:<mask>:hostname:eth0:off` cmdline parameter
   (`CONFIG_IP_PNP`). We just need the link-local route:

   ```
   ip route add 169.254.169.254 dev eth0
   ```

   Shell out to `ip` (available via `iproute2` in the base packages).

   c. **IPv6.** Parse `/proc/cmdline` for `ipv6=<addr>/<prefix>@<gw>`
   and `ipv6_ext=<gua>/128`. For each present:

   ```
   ip -6 addr add <addr>/<prefix> dev eth0
   ip -6 route add default via <gw> dev eth0
   ip -6 addr add <gua>/128 dev eth0       # if ipv6_ext= present
   ```

   d. **DNS.** Write `/etc/resolv.conf`. Prefer the IPv6 gateway as
   primary nameserver (unique per VM, ensures replies route to the right
   TAP on the host). Fall back to the IPv4 gateway. Always append
   `nameserver 8.8.8.8`.

   e. **zram swap.** Mirror `beyond-init`: create a zram device,
   format as swap, `swapon`. Size: 10% of RAM or 256 MB, whichever is
   smaller. Best-effort — log and continue if it fails.

   f. **sysctl.** Apply the same tuning from `beyond-init`:
   `vm.swappiness`, `net.core.somaxconn`, etc. Shell out to `sysctl -w`
   or write `/proc/sys/` directly.

   When not PID 1 (Docker, direct invocation), `init::run()` is skipped
   entirely — the caller's environment is already set up.

1. **Subcommand dispatch (`main.rs`).** `clap` with three subcommands:
   `supervisor`, `boot`, `archive`. Default behavior on no-arg is to
   print usage. Wire each to a `pub fn run(...)` in its respective
   module.

2. **MMDS client (`mmds.rs`).** Mirror the rootfs `mmds-client` shell
   script in Rust. Read fields:
   - `BEYOND_PG_TIER` (`single` | `primary` | `replica`)
   - `BEYOND_VOLUME_EPHEMERAL` (`true` | `false`)
   - `BEYOND_VM_RAM_BYTES` (or whatever Beyond's MMDS field name is —
     check `beyond/packer/scripts/05-mmds.sh` for the convention)
   - `POSTGRES_PASSWORD` (required, fail closed)
   - `POSTGRES_DATABASE` (default `postgres`)
   - `BEYOND_PG_ARCHIVE_TARGET` (optional)

   Retry on transient HTTP failure with backoff. Token lifecycle per
   the rootfs script.

3. **`boot.rs` — idempotent every-boot setup.** Steps from DESIGN.md
   "What `beyond-pg supervisor` does at boot." Implementation order:
   1. Parse MMDS fields. Fail closed on missing required fields.
   2. Detect PGDATA empty (`PG_VERSION` absent).
   3. If empty: run `initdb` with the right flags + password file.
      **Use `--waldir=/var/lib/postgresql/18/wal`** so the symlink is
      created by initdb itself; don't relocate `pg_wal` afterward
      (initdb writes WAL files into it during init, so a post-init
      relocation has to delete-and-replace, which is racy).
      Password tempfile under `/run/` with `mode 0o600`; remove after
      initdb returns.
   4. Confirm `pg_wal` symlink target matches tier. MVP: always vdb.
      Tier 2: returns `Err("not implemented")`.
   5. Generate `00-beyond.conf` from the template embedded via
      `include_str!("../packer/files/postgresql/00-beyond.conf")` and
      drop into `PGDATA/conf.d/`. Overwrite.
   6. Generate `01-tuning.conf` from MMDS RAM **and vCPU**. Overwrite.
      Formula matches DESIGN.md "What `beyond-pg supervisor` does at
      boot" exactly:
      - `work_mem = max(32MB, ram_mb / 2 / (pool_size * 5))` (NOT the
        old `ram * 0.01 / max_connections`)
      - `max_connections = clamp(vcpus * 25, 100, ram_mb / 50)` (NOT
        the old `pool_size * 2 + 50`)
      - vCPU-derived parallelism (max_worker_processes,
        max_parallel_workers, max_parallel_workers_per_gather,
        max_parallel_maintenance_workers)
        Cgroup-aware RAM/vCPU detection via
        `/sys/fs/cgroup/{memory.max,cpu.max}` first, `/proc/{meminfo,cpuinfo}`
        fallback.
   7. Generate `02-durability.conf` if `BEYOND_VOLUME_EPHEMERAL=true`,
      else delete it if present.
   8. Install/refresh `pg_hba.conf` from
      `packer/files/postgresql/pg_hba.conf`.
   9. Drop `/etc/sysctl.d/99-postgres.conf` (vm.swappiness=10,
      vm.overcommit_memory=2) and write to
      `/sys/kernel/mm/transparent_hugepage/enabled` if not already
      `never`. (THP via cmdline is the better path; this is the
      runtime safety net.)
   10. Generate PgBouncer's `pgbouncer` Postgres role + auth_query
       SECURITY DEFINER function — see step 9b.
   11. Run `pre-start.d/` scripts (no-op if empty).

   Exposed both as `beyond-pg boot` (CLI) and called inline from
   `supervisor::run()`.

4. **`config.rs`.** Templates for the three Beyond-managed conf files.
   Use a tiny templating approach (string substitution; no full
   templating engine). Write atomically (write to `.tmp` in the same
   directory as the target, fsync, rename — same-FS rename guaranteed
   atomic by POSIX).

5. **Process spawning (`supervisor.rs::spawn_*`).** For each child
   (`postgres`, `pgbouncer`):
   - Spawn with `Stdio::piped()` for stdout and stderr.
   - Take ownership of stdout/stderr file descriptors.
   - Hand them to `log_forwarder` for line-by-line vsock shipping.
   - Track child PID, restart policy.

6. **Restart logic (`supervisor.rs::supervise`).** On child exit:
   - If exit was clean and supervisor is shutting down: skip restart.
   - Otherwise: exponential backoff (start 100 ms, max 30 s, reset
     after 60 s of stable runtime). Mirror `beyond-init`'s shape.
   - Log every restart with reason.

7. **Signal handling (`supervisor.rs`).** `signalfd` for SIGTERM and
   SIGINT. On either: stop accepting new work, send SIGTERM to
   children, wait up to 10 s, then SIGKILL stragglers, exit 0.
   As PID 1, exiting calls `reboot(LINUX_REBOOT_CMD_POWER_OFF)` so
   Firecracker drops the VM cleanly.

   **Note**: `tokio::process::Child::kill()` sends SIGKILL, not
   SIGTERM. Use `nix::sys::signal::kill(Pid::from_raw(child.id()
   .unwrap() as i32), Signal::SIGTERM)` for the polite path, and
   only fall back to `Child::kill()` after the 10 s deadline.

   Backoff math: `min(100 * 2.pow(restart_count.min(8)), 30_000)` ms
   to avoid `u32::pow` overflow at high restart counts. Reset on
   `restart_count` after 60 s of stable runtime, measured from the
   _last successful start_, not from process spawn.

8. **`log_forwarder.rs`.** Read lines from a child's stdout/stderr
   pipe, frame them, ship over vsock. Wire format mirrors
   `beyond-agent`'s `log_forwarder.rs` so the host receiver can't
   tell the difference. Cite
   `beyond/boxes/guest-agent/src/supervisor/log_forwarder.rs` and
   match the `UserProcessStreamData` payload format from
   `vsock-protocol/src/lib.rs`.

   Apply the same rate limiting (500 lines/sec sustained, 1000 burst,
   per stream) so behavior matches user-app images.

9. **Post-start (`supervisor.rs::post_start`).** After Postgres
   reports ready (poll `pg_isready` on the local socket):
   1. `ALTER ROLE postgres WITH PASSWORD :pw` (read from MMDS).
   2. For each preloaded extension (read from a const list):
      `CREATE EXTENSION IF NOT EXISTS :ext`.
   3. Apply per-extension config:
      `ALTER SYSTEM SET cron.database_name = 'postgres'` (etc.) then
      `SELECT pg_reload_conf()`.
   4. Run `post-start.d/` scripts.

9.b. **PgBouncer auth_query setup.** PgBouncer needs to verify SCRAM
passwords against PG, but our MVP design says the supervisor
generates the auth surface from PG roles, not a static
`userlist.txt`. The right pattern is `auth_query`: PgBouncer
queries PG via a restricted role for the SCRAM hash on each new
client login.

Set up in `post_start`, idempotent:

```sql
-- create the lookup function in postgres database
CREATE SCHEMA IF NOT EXISTS pgbouncer;
CREATE OR REPLACE FUNCTION pgbouncer.get_auth(p_usename TEXT)
RETURNS TABLE (username TEXT, password TEXT)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RAISE WARNING 'PgBouncer auth request: %', p_usename;
  RETURN QUERY
    SELECT usename::TEXT, passwd::TEXT
    FROM pg_catalog.pg_shadow
    WHERE usename = p_usename;
END;
$$;
REVOKE ALL ON FUNCTION pgbouncer.get_auth(TEXT) FROM PUBLIC, pgbouncer;
GRANT EXECUTE ON FUNCTION pgbouncer.get_auth(TEXT) TO pgbouncer;

-- create the pgbouncer role for the lookup
CREATE ROLE pgbouncer LOGIN PASSWORD :auth_password;
GRANT USAGE ON SCHEMA pgbouncer TO pgbouncer;
```

The `pgbouncer` role's password is a separate secret (also in MMDS
or generated and persisted). `pgbouncer.ini` carries:

```
auth_user = pgbouncer
auth_query = SELECT username, password FROM pgbouncer.get_auth($1)
```

No `userlist.txt`. PgBouncer authenticates dynamically against PG
roles — and a new role created mid-flight is reachable through the
pooler immediately, no PgBouncer restart.

10. **`rpc.rs` — vsock server.** Listen on a vsock port. Pick
    something not 53 (DNS clash, confusing) — `5430` (PG-themed,
    unused) is fine pending a Beyond port-allocation convention.
    Wire format: MessagePack request/response, mirror the shape of
    `vsock-protocol::Task` for consistency. Commands:
    - `checkpoint` → `psql -c CHECKPOINT;`
    - `health` → `pg_isready` + `SELECT 1`
    - `reload` → `pg_ctl reload`
    - `backup` → run `pg_basebackup`, ship to MMDS-configured target
      (stub returns "not implemented" in MVP).

    Use `tokio-vsock` (maintained, works on Linux). For local dev
    (Docker, no vsock), bind a Unix domain socket at
    `/run/beyond-pg-rpc.sock` instead, gated by detecting absence of
    `/dev/vsock`.

11. **`archive.rs` — per-WAL hook.** Two subcommands:

    - `beyond-pg archive push <path> <filename>` — the
      `archive_command` hook. Reads `BEYOND_PG_ARCHIVE_TARGET` from
      MMDS. If unset: log + exit 0 (the no-op contract). If set:
      copy the file to the target, return non-zero on failure
      (Postgres will retry).

    - `beyond-pg archive pull <filename> <path>` — the
      `restore_command` hook. Reads the same
      `BEYOND_PG_ARCHIVE_TARGET`. Fetches the named WAL segment from
      the archive into `<path>`. Exit 1 if the segment is not found
      (signals Postgres to stop recovery). Used during PITR: fork
      from a GlideFS snapshot, set `restore_command`, boot — Postgres
      replays archived WAL to the target time.

12. **Standalone test harness.** A `cargo test`-runnable harness
    that:
    - Spins up Postgres in a temp directory.
    - Runs `beyond-pg boot` against a fake MMDS server.
    - Runs `beyond-pg supervisor` for ~5 s.
    - Connects via psql, runs a SELECT.
    - Sends SIGTERM, verifies clean shutdown.

    Use `testcontainers` or just shell out to a system Postgres for
    speed.

## Acceptance criteria

- [ ] `cargo build --release` produces a single binary at
      `target/release/beyond-pg`.
- [ ] `beyond-pg --help` prints usage with three subcommands.
- [ ] On a dev box with Postgres 18 installed:
      - [ ] `beyond-pg boot` against an empty PGDATA runs initdb,
      drops the three Beyond conf files, exits 0.
      - [ ] `beyond-pg boot` against a populated PGDATA skips initdb,
      refreshes the conf files, exits 0.
      - [ ] `beyond-pg supervisor` starts postgres + pgbouncer,
      CREATE EXTENSION succeeds for every extension installed,
      psql can connect on `localhost:5432` (PgBouncer) and
      `localhost:5433` (direct).
      - [ ] Postgres logs appear on supervisor's stdout (will become
      vsock frames in the VM).
      - [ ] SIGTERM to supervisor results in clean shutdown of all
      children within 10 s.
      - [ ] `beyond-pg archive push` with no MMDS target exits 0.
      - [ ] `beyond-pg archive pull` with no MMDS target exits 0.
- [ ] Vsock RPC: `checkpoint` and `health` work end-to-end (test
      with a vsock client on the host or via a local Unix-socket
      shim during dev).
- [ ] Restart-on-crash: kill `postgres` manually, supervisor restarts
      it within 1 s.

## Out of scope

- Tier 2 logic (replica role, sync replication setup). The MMDS
  fields are read; `BEYOND_PG_TIER != single` triggers a
  `not implemented` error in `boot`. That's the seam.
- Real `backup` implementation. Stub returns an error.
- Pre-fork CHECKPOINT hook. The RPC command exists; Beyond's
  fork API doesn't call it yet.
- Cross-platform: Linux only. Mac/Windows builds are not a goal.

## References

- DESIGN.md "Lifecycle" section for the boot sequence and subcommand
  table.
- DECISIONS.md G-001 (idempotent every-boot setup), G-004 (beyond-pg
  as PID 1), L-001 (vsock RPC).
- `beyond/boxes/beyond-init/src/main.rs` — reference for mount
  sequence, network setup, zram, and signal handling shape to mirror.
- `beyond/boxes/guest-agent/src/supervisor/log_forwarder.rs` — log
  shipping wire format to mirror.
- `beyond/boxes/vsock-protocol/src/lib.rs` — frame format.
