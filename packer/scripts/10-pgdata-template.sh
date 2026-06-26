#!/usr/bin/env bash
set -euo pipefail

# Pre-bake an initialized PGDATA template into the image.
#
# `beyond-pg build-template` runs `initdb` (the canonical runtime flag set) and
# then the full `CREATE EXTENSION` suite against a throwaway build-time postgres,
# leaving a cluster at /usr/local/share/beyond-pg/pgdata-template. At first boot,
# `beyond-pg-init` copies this onto the fresh data volume instead of running
# initdb + CREATE EXTENSION in the guest — taking both off the cold-boot path.
#
# Runs LAST: it needs the postgres server + every extension `.so` installed
# (02/03/04) and the beyond-pg binary in place (06). Building on the cleaned
# image (post-09) mirrors the runtime environment exactly (same trimmed locales,
# so the en_US.UTF-8 initdb locale resolves identically).
#
# Per-instance state is NOT baked in (superuser/replicator passwords, roles) —
# the supervisor's post_start applies those on every boot.

TEMPLATE_DIR="/usr/local/share/beyond-pg/pgdata-template"

echo "==> Building pre-initialized PGDATA template at ${TEMPLATE_DIR}..."
/usr/local/bin/beyond-pg build-template "${TEMPLATE_DIR}"

# Sanity: the template must carry a complete cluster (PG_VERSION present) or the
# first-boot materialize would silently fall back to runtime initdb.
test -f "${TEMPLATE_DIR}/main/PG_VERSION" \
  || { echo "FATAL: template missing PG_VERSION" >&2; exit 1; }

echo "==> 10-pgdata-template done ($(du -sh "${TEMPLATE_DIR}" | cut -f1))"
