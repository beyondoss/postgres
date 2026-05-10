# Phase 3 — Cross-repo prerequisites and end-to-end boot

The image is built. Now make it actually boot in Beyond. Land the
one cross-repo change (pre-fork CHECKPOINT in box-manager), provision
a real VM with the image, and prove the full lifecycle works.

This is the integration phase. Everything before this was preparation;
this is where Postgres-on-Beyond exists for the first time.

## Goal

`glide pg create myapp` (or whatever the temporary control-plane
incantation is) provisions a Firecracker VM from the postgres image
against a fresh GlideFS data volume, and a developer can
`psql -h <vm-ip>` and run SQL within 30 seconds.

## Dependencies

- Phase 1 (`beyond-pg` binary).
- Phase 2 (Packer image).
- **One cross-repo coordination point:**
  **Box-manager**: pre-fork CHECKPOINT before snapshot — box-manager
  calls our `checkpoint` vsock RPC on the source VM before
  `POST /snapshot`. Promoted to MVP because a 5–30 s WAL-replay
  window on hot-source forks would undermine the substrate-thesis
  fork pitch (POV.md). Implementation cost is low: the RPC already
  exists in `beyond-pg supervisor`; box-manager just needs to call
  it.

## Tasks

1. **Box-manager prerequisite: pre-fork CHECKPOINT hook.** Before
   calling `POST /api/exports/{vol}/snapshot` on a Postgres VM's data
   volume, box-manager (or whichever component drives `glide fork`)
   sends `{ "cmd": "checkpoint" }` to our supervisor's vsock RPC port
   on the source VM. The RPC blocks until the CHECKPOINT completes
   (~tens of ms on a small DB, longer on a hot one); then the snapshot
   captures a quiesced state.

Acceptance from box-manager's side:

- Snapshot path supports an optional pre-snapshot vsock RPC, addressed
  to the supervisor's port.
- Configurable per image type (other images may want different
  pre-snapshot semantics or none at all).
- Failure of the RPC is surfaced (don't snapshot a VM that we
  thought we'd quiesced and didn't).

Out of this repo, but tracked alongside #1. Validated in phase 4.

2. **Kernel cmdline.** Verify `packer/scripts/post-process.sh` boot_args
   are `root=/dev/vda console=ttyS0 reboot=k panic=1 pci=off` with no
   `paraglide.agent_path=` — `beyond-pg` is PID 1, no agent path needed.

3. **Provisioning script.** A throwaway shell or Rust script
   (`scripts/provision-test-vm.sh`) that:
   - Calls box-manager's API (or whatever the equivalent today is)
     to create a VM from `postgres-noble.img` with:
     - A fresh GlideFS data volume mounted at `/var/lib/postgresql/18/main`.
     - MMDS containing `POSTGRES_PASSWORD`, optional
       `BEYOND_PG_TIER=single`, optional `BEYOND_VOLUME_EPHEMERAL=false`.
   - Returns the VM's IP and waits for `psql -h <ip> -p 5432 -U
     postgres -c "SELECT 1"` to succeed.

   This is a phase-3-only test driver. The real CLI surface
   (`glide pg create`) is post-MVP.

4. **End-to-end boot test.** Run the provisioning script. Watch
   logs via box-manager's log API. Expect:
   - `beyond-pg` (PID 1) completes init, reads MMDS, runs `boot`
     (initdb on fresh volume), spawns postgres + pgbouncer, runs
     post-start `CREATE EXTENSION` pass.
   - Logs flow back to the host (visible via box-manager's log API).
   - Total cold-boot time: target under 15 s. Measure.

5. **Failure-mode tests.** For each row in DESIGN.md's "Failure modes"
   table, exercise it and confirm the documented behavior:
   - Kill `postgres` mid-flight; supervisor restarts.
   - Kill `pgbouncer`; supervisor restarts.
   - SIGKILL `beyond-pg` (PID 1); kernel panics and Firecracker
     terminates the VM — confirm box-manager surfaces this as a VM
     crash, not a silent hang.
   - Boot a VM with PGDATA already populated (image-swap path);
     `boot` skips initdb, refreshes confs.
   - Boot with `POSTGRES_PASSWORD` missing from MMDS; supervisor
     fails closed with a clear error log line; VM shuts down.

6. **Vsock RPC end-to-end.** From the host, send a `checkpoint`
   command to the supervisor's vsock port. Verify Postgres logs
   show the checkpoint completion. This validates the
   pre-snapshot hook path that fork relies on (phase 4).

7. **Logging end-to-end.** Confirm Postgres log lines reach the
   host log pipeline with the same structure as user-app logs.
   Compare a sample line against a user-app VM's line. Wire format
   should be indistinguishable.

8. **Boot-time measurement.** Instrument cold boot:
   - kernel handoff → `beyond-pg` (PID 1) init complete
   - init complete → `beyond-pg boot` start
   - `beyond-pg boot` start → postgres ready (first SELECT 1 succeeds)
   - postgres ready → all extensions installed
     Capture in a measurement table. If anything is over 5 s, dig.

## Acceptance criteria

- [ ] Box-manager has merged the pre-fork-CHECKPOINT hook (validated in phase 4).
- [ ] One running Postgres VM provisioned from the image.
- [ ] `psql` connects on port 5432 (PgBouncer) and 5433 (direct).
- [ ] `SELECT extname FROM pg_extension` lists every extension we
      shipped.
- [ ] Cold boot end-to-end under 15 s. Document the breakdown.
- [ ] All failure-mode rows in DESIGN.md exercised; behavior matches.
- [ ] Vsock `checkpoint` RPC works from the host.
- [ ] Postgres log lines reach the host log pipeline.

## Out of scope

- A user-facing CLI (`glide pg create`). The phase-3 driver is a
  test script.
- Multi-VM scenarios (replicas, HA). Single VM only.
- Backups, archiving with a real target. Stub paths only.
- Performance tuning. Functional correctness first.

## References

- DESIGN.md "Lifecycle" — the full boot tree.
- DESIGN.md "Failure modes" table — what to exercise.
- DECISIONS.md G-004 — the supervision model.
- `beyond/boxes/box-manager/` — coordinate the manifest flag here.
