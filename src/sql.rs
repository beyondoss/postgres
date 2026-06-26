//! SQL constants executed by the supervisor during post-start setup.
//!
//! Centralised here so integration tests run the exact SQL that production runs.
//! If any of these strings change, the tests automatically pick up the new version.

/// Creates the `replicator` role (idempotent). Used when WAL sink or CDC is enabled.
pub const REPLICATOR_ROLE_SQL: &str = "DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
    CREATE ROLE replicator LOGIN REPLICATION PASSWORD NULL;
  END IF;
END
$$";

/// Creates the `cdc` logical replication slot (idempotent).
pub const CDC_SLOT_SQL: &str = "DO $$
BEGIN
  IF NOT EXISTS (
    SELECT FROM pg_replication_slots WHERE slot_name = 'cdc'
  ) THEN
    PERFORM pg_create_logical_replication_slot('cdc', 'pgoutput');
  END IF;
END
$$";

/// Creates the `cdc` publication (idempotent, empty — tables added by the CDC consumer).
pub const CDC_PUBLICATION_SQL: &str = "DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'cdc') THEN
    EXECUTE 'CREATE PUBLICATION cdc';
  END IF;
END
$$";

/// pgbouncer auth: the `pgbouncer` login role + the SECURITY DEFINER lookup
/// function PgBouncer's `auth_query` calls, plus the schema USAGE the role needs
/// to call it. Idempotent (CREATE IF NOT EXISTS / OR REPLACE), and every
/// statement is `;`-terminated so it composes into the post-start batch script.
///
/// USAGE on the schema is REQUIRED — without it `auth_query` fails with
/// "permission denied for schema pgbouncer" for every client and no one can
/// connect through the pooler (EXECUTE on the function alone is not enough).
pub const PGBOUNCER_AUTH_SQL: &str = "DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgbouncer') THEN
    CREATE ROLE pgbouncer LOGIN PASSWORD NULL;
  END IF;
END
$$;
CREATE SCHEMA IF NOT EXISTS pgbouncer;
CREATE OR REPLACE FUNCTION pgbouncer.get_auth(p_user text)
  RETURNS TABLE(username text, password text)
  SECURITY DEFINER LANGUAGE sql AS $$
    SELECT usename::text, passwd::text FROM pg_shadow WHERE usename = p_user
  $$;
GRANT USAGE ON SCHEMA pgbouncer TO pgbouncer;
GRANT EXECUTE ON FUNCTION pgbouncer.get_auth(text) TO pgbouncer;";
