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
