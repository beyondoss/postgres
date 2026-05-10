#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# Install a PGDG apt package at a specific upstream version.
# apt-cache madison resolves epochs and Debian revision suffixes generically —
# we match against the upstream version string without hardcoding the full
# "2:0.8.0-1.pgdg24.04+1" form.
pin_install() {
  local pkg="$1" upstream_ver="$2"
  if [[ -z "${upstream_ver}" ]]; then
    echo "ERROR: version not set for ${pkg}" >&2
    exit 1
  fi
  local full_ver
  full_ver=$(apt-cache madison "${pkg}" \
    | awk -F'|' '{print $2}' \
    | tr -d ' ' \
    | grep "${upstream_ver}" \
    | head -1)
  if [[ -z "${full_ver}" ]]; then
    echo "ERROR: ${pkg} version matching '${upstream_ver}' not found in apt cache" >&2
    echo "  Available versions:" >&2
    apt-cache madison "${pkg}" | awk -F'|' '{print "   " $2}' >&2
    exit 1
  fi
  echo "==> Installing ${pkg}=${full_ver}..."
  apt-get install -y --no-install-recommends "${pkg}=${full_ver}"
}

echo "==> Installing PGDG extensions..."

pin_install "postgresql-${POSTGRES_VERSION}-pgvector"      "${PGVECTOR_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-pgvectorscale" "${PGVECTORSCALE_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-postgis-3"     "${POSTGIS_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-cron"          "${PG_CRON_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-partman"       "${PG_PARTMAN_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-pg-jsonschema" "${PG_JSONSCHEMA_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-hypopg"        "${HYPOPG_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-repack"        "${PG_REPACK_VERSION}"

echo "==> Adding ParadeDB apt repository (pg_search)..."
# ParadeDB publishes pg_search to packagecloud.
# TODO: verify this URL against ParadeDB's current repo path before shipping.
# Fallback option: download .deb directly from their GitHub releases.
curl -fsSL https://packagecloud.io/paradedb/paradedb/gpgkey \
  | gpg --dearmor -o /etc/apt/trusted.gpg.d/paradedb.gpg
echo "deb [signed-by=/etc/apt/trusted.gpg.d/paradedb.gpg] \
https://packagecloud.io/paradedb/paradedb/ubuntu/ ${UBUNTU_VERSION} main" \
  > /etc/apt/sources.list.d/paradedb.list
apt-get update -qq

pin_install "postgresql-${POSTGRES_VERSION}-pg-search" "${PG_SEARCH_VERSION}"

echo "==> 03-pgdg-extensions done"
