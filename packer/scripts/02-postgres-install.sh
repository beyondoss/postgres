#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

echo "==> Adding PGDG apt repository..."

curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg

echo "deb http://apt.postgresql.org/pub/repos/apt ${UBUNTU_VERSION}-pgdg main" \
  > /etc/apt/sources.list.d/pgdg.list

apt-get update -qq

echo "==> Installing PostgreSQL ${POSTGRES_VERSION}..."

apt-get install -y --no-install-recommends \
  "postgresql-${POSTGRES_VERSION}" \
  "postgresql-contrib-${POSTGRES_VERSION}" \
  "postgresql-client-${POSTGRES_VERSION}"

# Disable systemd unit — beyond-pg supervisor manages the process lifecycle.
# The unit would try to start postgres at boot if left enabled; we don't use systemd.
systemctl disable postgresql || true

echo "==> 02-postgres-install done"
