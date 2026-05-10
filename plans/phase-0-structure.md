# Phase 0 — Structure

Lay down the repo skeleton. Directories, mise tasks, manifest files,
empty stub files at every path the design references. No real code.
After this phase, every subsequent phase has a place to put its work.

## Goal

A repo that mirrors `beyond/packer`'s shape, with empty-but-present
stubs for every file the design names. `mise tasks | grep image:postgres`
returns the build/bless/publish task names. `tree` matches DESIGN.md's
file tree.

## Dependencies

None.

## Tasks

1. **Top-level files.**
   - `README.md` — one paragraph describing the repo, links to
     `POV.md`, `DESIGN.md`, `DECISIONS.md`, and `plans/`.
   - `extensions.toml` — version manifest. Stub:
     ```toml
     [postgres]
     version = "18.0"

     [extensions.beyond_auth]
     version = "0.0.0" # placeholder; bumped when sibling repo cuts release

     [extensions.beyond_queue]
     version = "0.0.0"
     ```

2. **Mise tasks** (`mise.toml`). Mirror the shape of
   `beyond/.mise.toml`'s `image:build`, `image:bless`, and
   `image:publish` tasks but for the Postgres image:
   - `image:postgres:build [version] [--bless] [--tier 128g]`
   - `image:postgres:bless <image_path> <base_name>`
   - `image:postgres:publish [image_name]`

   Each task should currently `echo "TODO: phase 2"` and exit 0.
   The shape needs to be right; the bodies fill in phase 2.

3. **Packer skeleton.**
   ```
   packer/
   ├── templates/
   │   └── postgres-rootfs.pkr.hcl       # empty packer file with sources block stub
   ├── scripts/
   │   ├── 01-base-packages.sh           # shebang + echo, exits 0
   │   ├── 02-postgres-install.sh
   │   ├── 03-pgdg-extensions.sh
   │   ├── 04-beyond-extensions.sh
   │   ├── 05-pgbouncer-install.sh
   │   ├── 06-beyond-pg-install.sh
   │   ├── 07-config.sh
   │   ├── 08-mmds.sh
   │   ├── 09-cleanup.sh
   │   └── post-process.sh
   └── files/
       ├── postgresql/
       │   ├── 00-beyond.conf            # empty
       │   └── pg_hba.conf               # empty
       ├── pgbouncer/
       │   └── pgbouncer.ini             # empty
       └── beyond-pg/                    # Rust crate stub
           ├── Cargo.toml
           ├── rust-toolchain.toml
           └── src/
               ├── main.rs               # subcommand dispatch stub (just prints help)
               ├── supervisor.rs         # `mod`, no impl
               ├── boot.rs
               ├── archive.rs
               ├── rpc.rs
               └── log_forwarder.rs
   ```

4. **Hook directory placeholders.** The runtime hook directories live
   on the data volume (PGDATA), not the rootfs. But the image needs
   to install them on first boot. For now, document this in
   `packer/files/postgresql/hooks-readme.md` so phase 1's `boot`
   subcommand knows where to put them.

5. **`Cargo.toml` for `beyond-pg`.** Bare minimum:
   ```toml
   [package]
   name = "beyond-pg"
   version = "0.1.0"
   edition = "2024"

   [[bin]]
   name = "beyond-pg"
   path = "src/main.rs"

   [dependencies]
   # Empty for phase 0; phase 1 adds these.
   ```

6. **`.gitignore`** for `target/`, build artifacts, `.DS_Store`, etc.

7. **`AGENTS.md`** (or update `CLAUDE.md` if preferred) — short note
   pointing at `POV.md` / `DESIGN.md` / `DECISIONS.md` / `plans/` so
   future agents land in the right doc first.

## Acceptance criteria

- [ ] `tree -L 4` matches DESIGN.md's "Image build pipeline" file tree.
- [ ] `mise tasks | grep image:postgres` lists the three tasks.
- [ ] `cargo check` (or `cargo build --workspace`) succeeds in
      `packer/files/beyond-pg/`.
- [ ] Every file referenced in DESIGN.md exists, even if empty.
- [ ] `git ls-files` shows no `target/` or build artifacts.

## Out of scope

- Any real code in `beyond-pg/src/*.rs`. Stubs only.
- Working Packer build. Templates can be syntactically empty
  (`source "docker" "ubuntu" { ... }` without provisioners).
- Any actual mise task body beyond `echo`.
- Configuration values in `00-beyond.conf` or `pgbouncer.ini`. Empty
  files; phase 2 fills them.

## References

- DESIGN.md — "Image build pipeline" section for file tree.
- `beyond/packer/` — the existing rootfs build to mirror.
- `beyond/.mise.toml` — `image:build`, `image:bless`, `image:publish`
  tasks for shape reference.
