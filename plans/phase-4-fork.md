# Phase 4 — Fork validation

A Postgres VM that boots cleanly is half the product. The other half
is forking. This phase proves `glide fork` against a Postgres volume
produces a working forked database, end-to-end, in 200 ms (snapshot)
plus boot time.

This is the wedge demo. Get it right.

## Goal

A live Postgres VM with real data. Run `glide fork`. The forked VM
boots, has the same data, has the same extensions installed, accepts
queries. Production keeps moving. Branches diverge.

## Dependencies

- Phase 3 (a working Postgres VM that boots from the image).

## Tasks

1. **Source-VM preparation.** Provision a Postgres VM from the image
   (phase-3 path). Load real data: a few schemas, a few tables, a
   modest insert load (~1 GB). Run a brief stream of writes for ~1
   minute to establish hot WAL. Capture pg_stat_statements + pg_class
   ROW_NUMBER counts before fork.

2. **Snapshot the volume.** `POST /api/exports/{vol}/snapshot` against
   the GlideFS API on the host. Time it. Expect 500 ms – 2 s for
   active Postgres (S3 manifest PUT). Log the manifest URL.

3. **Provision a fork VM.** Create a new VM from the same postgres
   image, but with a forked GlideFS volume from step 2. Same
   provisioning flow as phase 3.

4. **Boot the fork.** Watch the boot sequence. Expect:
   - `beyond-pg` (PID 1) completes init, reads MMDS.
   - `beyond-pg supervisor` runs `boot`. PGDATA is populated, so
     initdb is skipped. Confs get refreshed (idempotent).
   - Postgres starts. Performs WAL recovery from last checkpoint
     to the snapshot timestamp.
   - PgBouncer starts.
   - Post-start `CREATE EXTENSION IF NOT EXISTS` is a no-op (every
     extension already exists from the source).
   - Vsock RPC available.

5. **Validate fork data.**
   - `psql` to the fork VM. Run `SELECT count(*)` on each table.
     Compare to source counts at snapshot time.
   - Check `pg_stat_statements` was preserved.
   - Check that extension SQL state was preserved (e.g. `pg_cron`
     scheduled jobs are present).
   - Run a write on the fork. Run a write on the source. Confirm
     they don't see each other.

6. **Measure cold-fork boot latency.** From `glide fork` invocation
   to first successful `psql` query against the fork:
   - Snapshot time (S3 manifest PUT).
   - VM provisioning time (Firecracker startup).
   - Boot time (kernel → beyond-pg init → supervisor → boot →
     postgres ready).
   - WAL replay time.

   Document each number. WAL replay is bounded by
   `checkpoint_timeout` (15 min). For a busy source, replay can be
   measurable. Confirm.

7. **Pre-fork CHECKPOINT validation.** Send a `checkpoint` vsock RPC
   to the source VM, _then_ snapshot. Boot the fork. Expect WAL
   replay to be ~empty (only post-checkpoint writes to replay).
   Document the latency win. This is the seam future Beyond fork
   integration will use to make forks fast.

8. **Hot-set bless test.** GlideFS supports prefetching specific
   inode patterns to local SSD before fork I/O starts. Apply a
   bless hint for `pg_class`, `pg_attribute`, `pg_proc`,
   `pg_index`, `pg_namespace`, recent WAL, and `pg_control`. Boot
   a fork without bless, then with bless. Compare cold-read latency
   on the first queries. Document the difference.

9. **Ephemeral-volume fork test.** Provision a source VM with
   `BEYOND_VOLUME_EPHEMERAL=true`. Confirm `synchronous_commit =
   off` was applied. Run writes. Snapshot the volume — but the
   GlideFS export is ephemeral, so what does snapshot mean here?
   Document the answer (probably: snapshot still works for the
   blocks on local SSD; no S3 manifest because nothing flushes;
   fork is local-host-only).

   This is an important corner case. Confirm with the GlideFS team
   whether `ephemeral=true` exports are snapshotable at all.

## Acceptance criteria

- [ ] A Postgres VM forks via `glide fork` (or the equivalent API
      call) and the fork accepts queries within 30 s of the snapshot
      command returning.
- [ ] Forked database has identical row counts to source at snapshot
      time. No data loss, no ghost rows.
- [ ] Source and fork are isolated: writes to one don't appear in
      the other.
- [ ] `pg_stat_statements`, `pg_cron` scheduled jobs, and other
      extension state are preserved across fork.
- [ ] Cold-fork boot latency measured and documented. Each phase of
      the boot has a number.
- [ ] Pre-fork CHECKPOINT measurably reduces WAL replay time on
      fork.
- [ ] Hot-set bless measurably reduces first-query latency on fork.
- [ ] Behavior of ephemeral-volume forks documented (works /
      doesn't work / works with caveats).

## Out of scope

- Automating the pre-fork CHECKPOINT hook. The seam exists
  (vsock RPC); wiring it into Beyond's fork API is post-MVP.
- Sub-second forks. Beyond's snapshot is sync and the S3 manifest
  PUT is ~500 ms minimum. The 200 ms claim is a metadata-fork
  number, not first-query.
- Replication-aware forks (Tier 2). Single instance only.

## References

- DESIGN.md "The fork story" — the contract we're validating.
- DECISIONS.md B-003 (single volume holds data + WAL → forks include
  WAL → standard crash recovery), B-009 (ephemeral semantics).
- `glidefs/ARCHITECTURE.md` — snapshot atomicity, hot-set bless,
  the 5 s / 64 MB write-behind window.
- POV.md "What forks" — the user-facing claim we're proving.
