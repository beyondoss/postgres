# beyond/postgres

The official Postgres image for Beyond. Boots Postgres 18 on a GlideFS-backed data volume, ships the
extensions the modern stack expects, runs PgBouncer on the front, and forks with the substrate.

`psql localhost:5432` — same command in dev, same in prod.

---

- [POV.md](POV.md) — the bet and what falls out of it
- [DESIGN.md](DESIGN.md) — architecture, volume topology, lifecycle, configuration
- [DECISIONS.md](DECISIONS.md) — every significant decision with full reasoning
- [plans/](plans/) — phased implementation plans
