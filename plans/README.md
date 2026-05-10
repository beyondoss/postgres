# Implementation plans

Phased work plan for the Postgres image. Each phase is a chunk of
work one person can pick up and execute, with explicit dependencies,
tasks, and acceptance criteria.

Read `DESIGN.md` and `DECISIONS.md` first. These plans assume
familiarity with the design.

## Phases

| Phase | File                                           | Summary                                                                       |
| ----- | ---------------------------------------------- | ----------------------------------------------------------------------------- |
| 0     | [phase-0-structure.md](phase-0-structure.md)   | Repo skeleton: directories, mise tasks, manifest files. No code yet.          |
| 1     | [phase-1-supervisor.md](phase-1-supervisor.md) | The `beyond-pg` Rust binary: supervisor + boot + archive + vsock RPC.         |
| 2     | [phase-2-image.md](phase-2-image.md)           | Packer image build: Postgres 18, extensions, PgBouncer, configs, `beyond-pg`. |
| 3     | [phase-3-boot.md](phase-3-boot.md)             | Box-manager integration. End-to-end boot of a fresh VM against MMDS.          |
| 4     | [phase-4-fork.md](phase-4-fork.md)             | Fork validation. Prove `byd fork` produces a working Postgres.                |
| 5     | [phase-5-ship.md](phase-5-ship.md)             | Publish to S3. Release process. Ops documentation. Versioning.                |

Phases 0 and 1 can run in parallel after phase 0's repo skeleton
exists. Phase 2 depends on phase 0 (skeleton) but its Packer scripts
can be drafted before phase 1 ships. Phase 3 needs both 1 and 2.

## Out of plan scope

Tier 2 (HA, sync replication), backup service, control-plane CLI
(`byd pg ...`), and async read replicas are post-MVP. The seams
land in MVP per `DECISIONS.md` L-series, but the implementations
are separate work tracked elsewhere.
