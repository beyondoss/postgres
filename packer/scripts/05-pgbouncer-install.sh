#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

echo "==> Installing PgBouncer..."

apt-get install -y --no-install-recommends pgbouncer

# Disable systemd unit — beyond-pg supervisor manages PgBouncer directly.
systemctl disable pgbouncer || true

echo "==> 05-pgbouncer-install done"
