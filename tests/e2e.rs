use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt, runners::SyncRunner};

/// Runs the CDC setup SQL from `post_start` against a real Postgres instance and
/// verifies the slot and publication are created correctly and idempotently.
#[test]
#[ignore = "requires Docker"]
fn cdc_slot_and_publication_created() {
    let container = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let port = container.get_host_port_ipv4(5432).unwrap();
    let url = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");

    let mut client =
        postgres::Client::connect(&url, postgres::NoTls).expect("failed to connect to Postgres");

    // replicator role — same SQL as supervisor post_start
    client
        .batch_execute(
            "DO $$
             BEGIN
               IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
                 CREATE ROLE replicator LOGIN REPLICATION PASSWORD NULL;
               END IF;
             END
             $$",
        )
        .expect("failed to create replicator role");

    // CDC slot + publication — same SQL as supervisor post_start
    let setup_cdc = "
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT FROM pg_replication_slots WHERE slot_name = 'cdc'
          ) THEN
            PERFORM pg_create_logical_replication_slot('cdc', 'pgoutput');
          END IF;
        END
        $$;
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'cdc') THEN
            EXECUTE 'CREATE PUBLICATION cdc';
          END IF;
        END
        $$;
    ";

    client
        .batch_execute(setup_cdc)
        .expect("failed to set up CDC");

    // Verify slot
    let row = client
        .query_one(
            "SELECT plugin, slot_type::text FROM pg_replication_slots WHERE slot_name = 'cdc'",
            &[],
        )
        .expect("cdc slot not found");
    assert_eq!(row.get::<_, String>(0), "pgoutput", "wrong decoder plugin");
    assert_eq!(row.get::<_, String>(1), "logical", "wrong slot type");

    // Verify publication — empty (not FOR ALL TABLES)
    let row = client
        .query_one(
            "SELECT puballtables FROM pg_publication WHERE pubname = 'cdc'",
            &[],
        )
        .expect("cdc publication not found");
    assert!(
        !row.get::<_, bool>(0),
        "publication should not cover all tables"
    );

    // Idempotence: running the same SQL again must not error
    client
        .batch_execute(setup_cdc)
        .expect("CDC setup not idempotent");

    // Exactly one slot, one publication after two runs
    let slot_count: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'cdc'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(slot_count, 1, "duplicate slots created");

    let pub_count: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = 'cdc'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(pub_count, 1, "duplicate publications created");

    // Suppress unused-variable warning from the container not being explicitly dropped
    drop(container);
}
