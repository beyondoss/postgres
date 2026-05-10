# Phase 2 — Packer image build

Build the Postgres rootfs image. Packer drives Docker, installs
Postgres 18 + every extension, installs PgBouncer, drops config
files, embeds the `beyond-pg` binary, sets `/sbin/init` →
`beyond-pg`, runs `post-process.sh` to produce a tiered ext4
`.img`. Same shape as `beyond/packer`.

## Goal

`mise run build:image noble` produces a bootable
`postgres-noble-{git_sha}.img` (and tiered variants) that, when
booted in Firecracker against a blank data volume and a working
MMDS, brings Postgres + PgBouncer up healthy.

## Dependencies

- Phase 0 (skeleton). The Packer scripts in skeleton form get
  filled in here.
- Phase 1 (`beyond-pg` binary). Phase 2 embeds the built binary
  into the image.

Phase 2's Packer template and most provisioner scripts can be
drafted in parallel with phase 1; the integration step (script
06: `beyond-pg-install.sh`) needs phase 1's binary to actually
work end-to-end.

## Tasks

1. **Packer template (`templates/postgres-rootfs.pkr.hcl`).** Mirror
   `beyond/packer/templates/ubuntu-rootfs.pkr.hcl`:
   - Same `docker.ubuntu` source, `noble` default.
   - Variables: `image_version`, `target_arch`, `output_dir`,
     `build_tiers`, `auth_ext_version`, `queue_ext_version`
     (extracted from `extensions.toml` by the mise task).
   - Provisioner runs scripts `01` through `09` in order.
   - Post-processor: `docker-tag` then `shell-local` → `post-process.sh`.

2. **`01-base-packages.sh`.** Same OS packages as the rootfs:
   `systemd-sysv` (NOT used at runtime; some PG tools touch it during
   install — verify and drop if unnecessary), `iproute2`, `iptables`,
   `curl`, `vim`, `ca-certificates`, `locales`, `jq`, `e2fsprogs`,
   `rsync`, `git`, `zstd`, `netcat-openbsd`. Generate `en_US.UTF-8`
   locale (PG's default initdb locale).

   **Critical:** symlink `/sbin/init → /usr/local/bin/beyond-pg`.
   The binary is built and installed by script 06; the symlink must
   exist so the kernel finds PID 1 on boot.

3. **`02-postgres-install.sh`.** Add the PGDG apt repo. Install
   `postgresql-18`, `postgresql-contrib-18`, `postgresql-server-dev-18`
   (the last only if needed for any extension build; otherwise skip).
   Disable the systemd unit (`systemctl disable postgresql`) — we don't
   use it.

4. **`03-pgdg-extensions.sh`.** From PGDG apt:
   `postgresql-18-pgvector`, `postgresql-18-pgvectorscale`,
   `postgresql-18-postgis-3`, `postgresql-18-cron`,
   `postgresql-18-partman`, `postgresql-18-pg-jsonschema`,
   `postgresql-18-hypopg`, `postgresql-18-repack`. From the ParadeDB
   apt repo: `postgresql-18-pg-search`. `pg_trgm`, `pg_stat_statements`,
   `auto_explain` are in `postgresql-contrib-18` already.

5. **`04-beyond-extensions.sh`.** Pull
   `s3://beyond-extensions/auth/{auth_ext_version}/{arch}/beyond-auth.deb`
   and the queue equivalent. `dpkg -i` each. Versions come from
   `extensions.toml` via Packer variables. Build fails if S3 returns
   404 — version pinning is enforced at build time, not runtime.

6. **`05-pgbouncer-install.sh`.** `apt install pgbouncer`. Disable
   the systemd unit. (PgBouncer is supervised by `beyond-pg`.)

7. **`06-beyond-pg-install.sh`.** Build `beyond-pg` for the target
   arch (cross-compile via `cross` or use a builder container that
   matches), copy the binary to `/usr/local/bin/beyond-pg`. Make it
   executable. No symlinks needed — `/sbin/init` already points here
   (script 01) and `archive_command` references the real path.

   Also stage hook directories:
   `/etc/postgresql/18/hooks/{pre-start,post-start,pre-stop,pre-fork}.d/`,
   each empty.

8. **`07-config.sh`.** Drop:
   - `packer/files/postgresql/00-beyond.conf` →
     `/etc/postgresql/18/main/00-beyond.conf` (template; `boot`
     subcommand re-stamps into `PGDATA/conf.d/` on boot).
   - `packer/files/postgresql/pg_hba.conf` →
     `/etc/postgresql/18/main/pg_hba.conf` (template).
   - `packer/files/pgbouncer/pgbouncer.ini` →
     `/etc/pgbouncer/pgbouncer.ini`.
   - Edit `/etc/postgresql/18/main/postgresql.conf` to add
     `include_dir = '/var/lib/postgresql/18/main/conf.d'`.

9. **`08-mmds.sh`.** Same as the rootfs's
   `beyond/packer/scripts/05-mmds.sh`. Installs the MMDS client tooling
   used by the rootfs; `beyond-pg` reads MMDS directly at runtime.

10. **`09-cleanup.sh`.** Same as rootfs `07-cleanup.sh`. Trim apt
    caches, log files, locale files we don't need.

11. **`post-process.sh`.** Copy verbatim from `beyond/packer`'s
    post-process.sh. Tiered ext4 build, BLAKE3 hashes, JSON manifests.
    Default tiers: `16g 64g 128g 256g 512g 1t`. Output dir per the
    Packer variable.

12. **Mise task bodies.** Replace the phase-0 `echo "TODO"` with the
    real shell logic, mirroring `beyond/.mise.toml`'s `image:build`,
    `image:bless`, `image:publish` patterns. Pull the auth/queue
    extension versions from `extensions.toml` at task-run time:
    ```bash
    AUTH_VER=$(yq -p toml '.extensions.beyond_auth.version' extensions.toml)
    QUEUE_VER=$(yq -p toml '.extensions.beyond_queue.version' extensions.toml)
    ```

13. **Config file content.**
    - `00-beyond.conf` — every value from DESIGN.md "Configuration".
    - `pg_hba.conf` — the four lines from DESIGN.md "Authentication".
    - `pgbouncer.ini` — the values from DESIGN.md "Connection topology".

## Acceptance criteria

- [ ] `mise run image:postgres:build noble` succeeds on a Linux
      builder host, producing tiered `.img` files in
      `/pglide/images/postgres/`.
- [ ] Each `.img` boots in Firecracker against a blank data volume
      and a working MMDS (with `POSTGRES_PASSWORD` set), and:
      - [ ] Postgres listens on `127.0.0.1:5433`.
      - [ ] PgBouncer listens on `0.0.0.0:5432`.
      - [ ] `psql -h <vm-ip> -p 5432 -U postgres -c "SELECT 1"`
      returns `1`.
      - [ ] Every extension from the list is installed and shows up in
      `pg_available_extensions`.
- [ ] Image rootfs size is under 4 GB before tier-pad.
- [ ] BLAKE3 manifests are emitted alongside each `.img`.

## Out of scope

- GlideFS bless step. That happens in phase 5 (release).
- Direct-boot vs overlay decision. We assume direct-boot (per DESIGN.md).
- Multi-arch builds. amd64 only for MVP. arm64 added later.

## References

- DESIGN.md "Image build pipeline" for layout and mise tasks.
- DESIGN.md "Configuration" for `00-beyond.conf` content.
- DESIGN.md "Connection topology" for `pgbouncer.ini`.
- DECISIONS.md J-001 through J-005 for extension list and sourcing.
- `beyond/packer/scripts/post-process.sh` to copy verbatim.
- `beyond/.mise.toml` `image:build` task for shape.
