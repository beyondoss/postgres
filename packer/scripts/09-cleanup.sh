#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

echo "==> Removing Rust toolchain (build-only; not needed at runtime)..."
# shellcheck source=/dev/null
source "${HOME}/.cargo/env" 2>/dev/null || true
rustup self uninstall -y 2>/dev/null || true
rm -rf "${HOME}/.cargo" "${HOME}/.rustup"

echo "==> Removing build artifacts..."
rm -rf /tmp/beyond-pg-src
rm -f /tmp/beyond-pg \
      /tmp/00-beyond.conf \
      /tmp/pg_hba.conf \
      /tmp/pgbouncer.ini

echo "==> Removing dev headers..."
apt-get purge -y "postgresql-server-dev-${POSTGRES_VERSION}" 2>/dev/null || true

echo "==> Cleaning apt caches..."
apt-get autoremove -y
apt-get clean
rm -rf /var/lib/apt/lists/*

echo "==> Trimming locale data (keeping en_US only)..."
find /usr/share/locale -mindepth 1 -maxdepth 1 \
  ! -name 'en_US' ! -name 'en' -exec rm -rf {} +

echo "==> Trimming docs and man pages..."
# Remove generic docs/man but keep /usr/share/postgresql — psql \h help text lives there.
rm -rf /usr/share/doc /usr/share/man /usr/share/info
find /usr/share/postgresql -name "*.html" -delete 2>/dev/null || true

echo "==> 09-cleanup done"
