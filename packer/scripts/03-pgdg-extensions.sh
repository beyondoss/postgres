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
pin_install "postgresql-${POSTGRES_VERSION}-postgis-3"     "${POSTGIS_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-cron"          "${PG_CRON_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-partman"       "${PG_PARTMAN_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-hypopg"        "${HYPOPG_VERSION}"
pin_install "postgresql-${POSTGRES_VERSION}-repack"        "${PG_REPACK_VERSION}"

# Dropped (no PGDG build for noble+pg18 as of 2026-06; see extensions.toml):
#   pgvectorscale, pg_jsonschema
# ParadeDB pg_search: packagecloud repo is now paywalled (HTTP 402); ParadeDB
# ships pg18 via GitHub Releases. Re-add via a GitHub-release install (like
# 04-beyond-extensions.sh) once needed.

echo "==> 03-pgdg-extensions done"
