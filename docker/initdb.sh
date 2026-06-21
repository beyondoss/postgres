#!/usr/bin/env bash
# Runs once on first DB init (against POSTGRES_DB). Sets up the schemas the
# primitives expect in the shared app database:
#   - auth   : auth server auto-migrates + CREATE EXTENSION beyond_auth (installed in the image)
#   - queue  : schema + PL/pgSQL hot paths
#   - public : the app's own migrations
set -euo pipefail

# Create the schemas the primitives expect. The auth server manages the
# beyond_auth extension itself in its own migrations (the extension is installed
# in the image), so initdb must NOT create it here — doing so makes the auth
# server's migration fail with "function authz_check already exists".
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE SCHEMA IF NOT EXISTS auth;
CREATE SCHEMA IF NOT EXISTS queue;
SQL

# Queue: base schema then the PL/pgSQL hot-path overrides (search_path=queue).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -c 'SET search_path = queue, public;' -f /opt/queue/schema.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -c 'SET search_path = queue, public;' -f /opt/queue/hot_paths.sql

echo "[beyond-init] auth/queue schemas ready in ${POSTGRES_DB}"
