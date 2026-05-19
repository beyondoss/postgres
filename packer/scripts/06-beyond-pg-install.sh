#!/usr/bin/env bash
set -euo pipefail

echo "==> Installing beyond-pg-init (pre-built musl static binary)..."
install -m 0755 /tmp/beyond-pg-bin/beyond-pg-init /usr/local/bin/beyond-pg-init

echo "==> Installing beyond-pg (pre-built musl static binary)..."
install -m 0755 /tmp/beyond-pg-bin/beyond-pg /usr/local/bin/beyond-pg

echo "==> Installing beyond-pg-sink (pre-built musl static binary)..."
install -m 0755 /tmp/beyond-pg-bin/beyond-pg-sink /usr/local/bin/beyond-pg-sink

echo "==> Creating hook directories..."
# Empty in MVP; tier-specific scripts drop in later via image updates.
for dir in pre-start post-start pre-stop pre-fork; do
  mkdir -p "/etc/postgresql/${POSTGRES_VERSION}/hooks/${dir}.d"
done

echo "==> 06-beyond-pg-install done"
