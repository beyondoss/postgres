#!/usr/bin/env bash
# Runs once on first DB init (against POSTGRES_DB). Sets up the schemas the
# primitives expect in the shared app database:
#   - auth   : auth server auto-migrates + CREATE EXTENSION beyond_auth (installed in the image)
#   - queue  : schema + PL/pgSQL hot paths
#   - public : the app's own migrations
set -euo pipefail

# beyond_auth owns (creates) the `auth` schema, so let the extension make it
# rather than pre-creating it (pre-creating triggers "schema auth is not a member
# of extension"). The auth server's CREATE EXTENSION IF NOT EXISTS is then a no-op.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE EXTENSION IF NOT EXISTS beyond_auth;
CREATE SCHEMA IF NOT EXISTS queue;
SQL

# Queue: base schema then the PL/pgSQL hot-path overrides (search_path=queue).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -c 'SET search_path = queue, public;' -f /opt/queue/schema.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -c 'SET search_path = queue, public;' -f /opt/queue/hot_paths.sql

echo "[beyond-init] auth/queue schemas ready in ${POSTGRES_DB}"
