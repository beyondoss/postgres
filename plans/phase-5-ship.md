# Phase 5 — Ship

The image works, it boots, it forks. Now publish it, wire it into the
fleet, write the operator-facing docs, and define the release process.

After this phase, "the postgres image" is a thing other Beyond
components can target by name and version, with a documented
upgrade path.

## Goal

A versioned postgres image lives in S3. Hosts in the fleet can pull
it. Operators have a runbook. Releases are reproducible. The image is
ready for the first wave of internal Beyond projects to use it.

## Dependencies

- Phase 4 (fork validation passing).

## Tasks

1. **GlideFS bless step.** Wire up `mise run image:postgres:bless`
   to call `packer/scripts/bless.sh` (mirror beyond/packer's). Each
   tier gets a content-addressed bless under
   `bases/postgres-noble-{tier}`.

2. **Publish to S3.**
   - `mise run image:postgres:publish` ships the tiered `.img` files
     and their JSON manifests to
     `s3://beyond-images-{env}/postgres/`.
   - Keep N latest versions; prune older.
   - Manifest includes the boot args, the `extensions.toml` snapshot,
     the git SHA, the build timestamp.

3. **Fleet sync.** Post a NATS message on
   `storage.postgres.updated` so subscribed hosts pull the new
   image. Mirror `beyond/packer/scripts/sync-fleet.sh`.

4. **Versioning convention.**
   - Image filename: `postgres-noble-{git_sha}.img`.
   - Symlink: `postgres-noble.img` → latest.
   - Major version (PG version) bump = new image lineage entirely
     (`postgres-noble-pg19-...`). Existing `postgres-noble-...`
     still points at PG 18.
   - Extension version pins live in `extensions.toml`. Bumping a
     pin creates a new image build. Document the bump-and-build
     workflow.

5. **README.md (top of repo).** Replace the phase-0 stub. One page,
   what-this-is + how-to-use + where-to-look. No marketing voice;
   this is operator-facing. Link to POV.md, DESIGN.md, DECISIONS.md,
   plans/.

6. **`docs/operations.md`.** Operator runbook:
   - How to provision a Postgres VM (Beyond control-plane
     incantation).
   - How to read MMDS for a running VM (debugging password issues,
     tier mismatches).
   - How to read Postgres logs (vsock pipeline → host log API).
   - How to call `beyond-pg control` RPCs from the host
     (`checkpoint`, `health`, `reload`, `backup`).
   - How to escape PgBouncer (port 5433 for direct PG, when to use it).
   - What to do when a VM won't boot (the DESIGN.md failure-mode
     table, expanded with concrete debug steps).

7. **`docs/upgrades.md`.** Upgrade runbook:
   - Image-swap upgrade (same PG major). Drop in new image, reboot
     VM, `beyond-pg boot` refreshes confs, no PGDATA changes.
   - Major-version upgrade (PG 18 → 19). Out of image scope:
     describe the maintenance-VM `pg_upgrade` flow as the operator
     procedure.
   - Extension version bump (`extensions.toml` change). Build new
     image, image-swap, post-start `CREATE EXTENSION ... UPDATE`
     runs.

8. **`docs/durability.md`.** User-facing durability statement.
   Be honest:
   - Tier 1 + durable: GlideFS write-behind, ~5 s / 64 MB window
     on host loss.
   - Tier 1 + ephemeral: local SSD only, gone on host loss, free.
   - Tier 2 + durable: sync replication quorum, zero data loss on
     single-host loss. (When Tier 2 ships.)
   - PITR: when `BEYOND_PG_ARCHIVE_TARGET` is set, WAL segments are
     archived between GlideFS snapshots. Restore by forking a
     snapshot and replaying archived WAL to the target time. Recovery
     granularity is per-WAL-segment (~16 MB / seconds of writes),
     not per-transaction.

   This is the contract. No marketing words. State the bound.

9. **CI for image build.** GitHub Actions (or whatever Beyond uses):
   - On main: build, run smoke tests, publish to staging S3.
   - On tag: promote staging build to production S3.
   - On PR: build only (no publish), run lints + Rust tests.

10. **Smoke test in CI.** A subset of phase-3's end-to-end test that
    runs in CI: build image → boot a VM in a test sandbox →
    `psql SELECT 1` → tear down. Fail the CI on failure. Catches
    image regressions before they ship.

11. **Bug-fix retro.** Anything found in phases 3–4 that needs a
    fix lands here. Cleanup sweep before declaring MVP done.

## Acceptance criteria

- [ ] `mise run image:postgres:publish` ships an image to S3.
- [ ] Manifest includes the correct `beyond.agent_path=` boot arg and the extensions
      pin snapshot.
- [ ] Fleet hosts auto-pull on NATS notification.
- [ ] CI builds and smoke-tests every PR + main + tag.
- [ ] `README.md`, `docs/operations.md`, `docs/upgrades.md`,
      `docs/durability.md` exist and are accurate.
- [ ] First internal Beyond project successfully provisions a
      Postgres VM from the published image and runs against it.

## Out of scope

- A user-facing CLI surface (`byd pg create/scale/promote-to-ha`).
  Tracked separately.
- Tier 2 (HA, sync replication). Documented as future work in
  `docs/upgrades.md` but not implemented.
- Backup service. Image is wired (`beyond-pg backup` RPC exists),
  but the backup orchestration lives elsewhere.
- Marketing the image externally. Internal-first. External GA is
  a separate launch.

## References

- DESIGN.md "Image build pipeline" for the publish task shape.
- `beyond/packer/scripts/publish.sh`, `pull.sh`, `sync-fleet.sh`
  to mirror.
- DECISIONS.md K-001 through K-003 (build pipeline mirrors
  beyond/packer; reuse blank volumes; image versioning).
- POV.md for the durability statement we expose user-facing.
