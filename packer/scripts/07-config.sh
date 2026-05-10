#!/usr/bin/env bash
set -euo pipefail

echo "==> Installing config files..."

# 00-beyond.conf → rootfs copy for operator inspection only.
# Postgres never reads /etc/postgresql/18/main/00-beyond.conf at runtime.
# beyond-pg boot writes the embedded copy to PGDATA/conf.d/ on every boot.
install -m 0644 /tmp/00-beyond.conf \
  "/etc/postgresql/${POSTGRES_VERSION}/main/00-beyond.conf"

# pg_hba.conf template — beyond-pg boot overwrites PGDATA/pg_hba.conf on every boot.
install -m 0640 /tmp/pg_hba.conf \
  "/etc/postgresql/${POSTGRES_VERSION}/main/pg_hba.conf"
chown postgres:postgres "/etc/postgresql/${POSTGRES_VERSION}/main/pg_hba.conf"

# pgbouncer.ini — beyond-pg boot also rewrites /etc/pgbouncer/pgbouncer.ini
# from the embedded copy. The rootfs copy is the initial state after install.
install -m 0640 /tmp/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini
chown postgres:postgres /etc/pgbouncer/pgbouncer.ini

# postgresql.conf — add include_dir so conf.d/ on the data volume (vdb) is
# picked up. The PGDG default postgresql.conf has no include_dir; we append
# ours. beyond-pg boot writes the RAM-tuned conf.d/ files on every boot.
if ! grep -q "include_dir" "/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf"; then
  echo "" >> "/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf"
  echo "# Beyond: load per-boot tuning from the data volume" >> \
    "/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf"
  echo "include_dir = '/var/lib/postgresql/${POSTGRES_VERSION}/main/conf.d'" >> \
    "/etc/postgresql/${POSTGRES_VERSION}/main/postgresql.conf"
fi

echo "==> 07-config done"
