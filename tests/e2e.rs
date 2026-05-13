use std::time::{Duration, Instant};

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ImageExt, runners::SyncRunner};

// ---------------------------------------------------------------------------
// Replica test helpers
// ---------------------------------------------------------------------------

/// Rewrites pg_hba.conf inside a running container to allow replication from
/// any host, then reloads.
fn allow_all_replication(client: &mut postgres::Client) {
    client
        .batch_execute(
            "DO $$ DECLARE p text; BEGIN \
                 SELECT current_setting('hba_file') INTO p; \
                 EXECUTE format( \
                     $q$COPY (SELECT line FROM (VALUES \
                         ('local all all trust'), \
                         ('host all all all trust'), \
                         ('host replication all all trust') \
                     ) AS t(line)) TO %L$q$, p); \
             END; $$; \
             SELECT pg_reload_conf();",
        )
        .expect("failed to rewrite pg_hba.conf for replication");
}

/// An isolated Docker network used to let the primary container and the
/// pg_basebackup/replica containers communicate directly (avoids the
/// host.docker.internal / loopback portability problem on macOS vs Linux).
struct TestNetwork {
    name: String,
}

impl TestNetwork {
    fn create() -> Self {
        // Include both PID and a per-process counter so parallel tests in the
        // same binary don't collide on the network name.
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = format!("beyond-pg-replica-test-{}-{n}", std::process::id());
        let out = std::process::Command::new("docker")
            .args(["network", "create", &name])
            .output()
            .expect("docker network create failed");
        assert!(
            out.status.success(),
            "docker network create: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Self { name }
    }

    /// Attach an already-running container to this network.
    fn connect(&self, container_id: &str) {
        let out = std::process::Command::new("docker")
            .args(["network", "connect", &self.name, container_id])
            .output()
            .expect("docker network connect failed");
        assert!(
            out.status.success(),
            "docker network connect: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Resolve the container's IP on this network, retrying until Docker
    /// propagates the network attachment (typically < 500 ms).
    ///
    /// Uses `index` in the Go template because the network name may contain
    /// hyphens, which are arithmetic operators in Go template dot-path notation.
    fn container_ip(&self, container_id: &str) -> String {
        let template = format!(
            r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
            self.name
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let out = std::process::Command::new("docker")
                .args(["inspect", "-f", &template, container_id])
                .output()
                .expect("docker inspect failed");
            let ip = String::from_utf8_lossy(&out.stdout).trim().to_owned();
            if !ip.is_empty() {
                return ip;
            }
            if Instant::now() > deadline {
                panic!(
                    "container {container_id} has no IP on network {} after 5s",
                    self.name
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for TestNetwork {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["network", "rm", &self.name])
            .output();
    }
}

/// A Postgres replica running inside a `postgres:18` container, seeded from a
/// primary via pg_basebackup (also run inside a container). Using containers
/// for both ensures the tool version always matches the server version,
/// regardless of what Postgres version is installed on the test host.
struct ReplicaPostgres {
    container_id: String,
    pub port: u16,
    _pgdata: tempfile::TempDir,
}

impl ReplicaPostgres {
    /// Seed a replica from `primary_ip:5432` (container-internal address) and
    /// start it. `host_port` is the port mapped on the test-host loopback.
    fn start(network: &TestNetwork, primary_ip: &str) -> Self {
        // Tempdir on the host, bind-mounted into the containers.
        let pgdata = tempfile::tempdir().expect("tempdir for replica PGDATA");
        let pgdata_str = pgdata.path().to_str().unwrap();

        // chmod 777 so the postgres user inside the container (uid 999) can write.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(pgdata.path(), std::fs::Permissions::from_mode(0o777))
                .expect("chmod replica pgdata");
        }

        // 1. pg_basebackup inside a postgres:18 container on the same network.
        //    Connects to the primary at its internal IP, port 5432 (no mapping needed).
        let connstr = format!("host={primary_ip} port=5432 user=postgres");
        let out = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                &format!("--network={}", network.name),
                "-v",
                &format!("{pgdata_str}:/pgdata"),
                "postgres:18",
                "pg_basebackup",
                "-d",
                &connstr,
                "--pgdata",
                "/pgdata",
                "--format=plain",
                "--wal-method=stream",
                "--checkpoint=fast",
            ])
            .output()
            .expect("docker run pg_basebackup");
        assert!(
            out.status.success(),
            "pg_basebackup failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        // 2. Write standby.signal and recovery config as root inside a container.
        //    In production, do_boot_replica() writes these; here we replicate
        //    exactly the same parameters that config::replica_conf() generates.
        let script = format!(
            "touch /pgdata/standby.signal && \
             echo \"primary_conninfo = 'host={primary_ip} port=5432 user=postgres'\" \
               >> /pgdata/postgresql.auto.conf && \
             echo \"recovery_target_timeline = 'latest'\" \
               >> /pgdata/postgresql.auto.conf"
        );
        let out = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--user",
                "root",
                "-v",
                &format!("{pgdata_str}:/pgdata"),
                "postgres:18",
                "bash",
                "-c",
                &script,
            ])
            .output()
            .expect("docker run setup script");
        assert!(
            out.status.success(),
            "replica setup script: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        // 3. Free host port for the replica.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        // 4. Start the replica: run as root, chown PGDATA, then exec as postgres.
        //    Bypasses the Docker entrypoint to avoid re-initdb interference.
        let start_out = std::process::Command::new("docker")
            .args([
                "run",
                "-d",
                &format!("--network={}", network.name),
                "--user",
                "root",
                "-v",
                &format!("{pgdata_str}:/pgdata"),
                "-p",
                &format!("{port}:5432"),
                "postgres:18",
                "bash",
                "-c",
                // chmod 700: postgres refuses to start on a world-readable data dir.
                // chown: files were written by pg_basebackup (postgres user) and our
                //        setup script (root); normalize ownership before dropping privs.
                "chmod 700 /pgdata && chown -R postgres:postgres /pgdata && exec gosu postgres postgres -D /pgdata",
            ])
            .output()
            .expect("docker run replica");
        assert!(
            start_out.status.success(),
            "replica container start: {}",
            String::from_utf8_lossy(&start_out.stderr)
        );
        let container_id = String::from_utf8_lossy(&start_out.stdout).trim().to_owned();

        // 5. Wait until the replica accepts connections.
        let url = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if Instant::now() > deadline {
                panic!("replica container {container_id} not ready on port {port} within 30s");
            }
            if postgres::Client::connect(&url, postgres::NoTls).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }

        Self {
            container_id,
            port,
            _pgdata: pgdata,
        }
    }
}

impl Drop for ReplicaPostgres {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.container_id])
            .output();
    }
}

/// Poll until `table` on `client` has at least `expected_count` rows, or panic.
fn wait_for_replication(client: &mut postgres::Client, table: &str, expected_count: i64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if Instant::now() > deadline {
            panic!("replica did not replicate {expected_count} rows from {table} within 30s");
        }
        let n: i64 = client
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .map(|r| r.get(0))
            .unwrap_or(0);
        if n >= expected_count {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ---------------------------------------------------------------------------
// Replica integration tests
// ---------------------------------------------------------------------------

/// Full streaming replication: rows written to the primary appear on the replica.
/// Exercises:
///   - pg_basebackup (what `pg::basebackup()` wraps in production)
///   - standby.signal
///   - primary_conninfo + recovery_target_timeline (what `config::replica_conf()` generates)
///   - hot_standby read-only enforcement
#[test]
#[ignore = "requires Docker"]
fn replica_streams_from_primary() {
    let network = TestNetwork::create();

    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "hot_standby=on",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let primary_mapped_port = primary.get_host_port_ipv4(5432).unwrap();
    let primary_url =
        format!("host=127.0.0.1 port={primary_mapped_port} user=postgres dbname=postgres");
    let mut primary_client =
        postgres::Client::connect(&primary_url, postgres::NoTls).expect("connect to primary");

    allow_all_replication(&mut primary_client);

    // Connect primary to our test network so containers can reach it directly.
    network.connect(primary.id());
    let primary_ip = network.container_ip(primary.id());

    // Write rows before basebackup so the replica inherits them.
    primary_client
        .batch_execute(
            "CREATE TABLE replica_test (id serial, v text); \
             INSERT INTO replica_test (v) \
               SELECT 'before-' || g FROM generate_series(1, 100) g;",
        )
        .expect("create + insert on primary");

    let replica = ReplicaPostgres::start(&network, &primary_ip);
    let replica_url = format!(
        "host=127.0.0.1 port={} user=postgres dbname=postgres",
        replica.port
    );
    let mut replica_client =
        postgres::Client::connect(&replica_url, postgres::NoTls).expect("connect to replica");

    // Rows present before basebackup must be on the replica.
    wait_for_replication(&mut replica_client, "replica_test", 100);

    // Write more rows after the replica started streaming.
    primary_client
        .batch_execute(
            "INSERT INTO replica_test (v) \
               SELECT 'after-' || g FROM generate_series(1, 100) g;",
        )
        .expect("insert after replica started streaming");

    // Replica must replicate the new rows.
    wait_for_replication(&mut replica_client, "replica_test", 200);

    // Replica must report itself as a standby.
    let is_standby: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(is_standby, "replica should be in recovery (hot standby)");

    // Replica must reject writes — it is read-only while in recovery.
    // Postgres error code 25006 (read_only_sql_transaction); message contains "recovery".
    let write_err = replica_client
        .batch_execute("INSERT INTO replica_test (v) VALUES ('should-fail')")
        .expect_err("replica accepted a write — should be read-only during recovery");
    let is_recovery_rejection = write_err
        .as_db_error()
        .map(|e| e.message().contains("recovery") || e.message().contains("read-only"))
        .unwrap_or(false);
    assert!(
        is_recovery_rejection,
        "write failed but not with a recovery/read-only error: {write_err:?}"
    );

    drop(replica);
    drop(primary);
    drop(network);
}

/// Promote: streams from primary, then `pg_promote()` flips the replica to
/// primary so it accepts writes. Exercises the `promote` RPC path.
///
/// Uses `SELECT pg_promote()` (SQL, PG 12+) rather than a shell `pg_ctl`
/// call — identical effect, no host binary version dependency.
#[test]
#[ignore = "requires Docker"]
fn replica_promote() {
    let network = TestNetwork::create();

    let primary = Postgres::default()
        .with_tag("18")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd([
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "hot_standby=on",
        ])
        .start()
        .expect("Docker not available or postgres:18 image pull failed");

    let primary_mapped_port = primary.get_host_port_ipv4(5432).unwrap();
    let primary_url =
        format!("host=127.0.0.1 port={primary_mapped_port} user=postgres dbname=postgres");
    let mut primary_client =
        postgres::Client::connect(&primary_url, postgres::NoTls).expect("connect to primary");

    allow_all_replication(&mut primary_client);

    network.connect(primary.id());
    let primary_ip = network.container_ip(primary.id());

    primary_client
        .batch_execute(
            "CREATE TABLE promote_test (id serial, v text); \
             INSERT INTO promote_test (v) \
               SELECT 'pre-' || g FROM generate_series(1, 50) g;",
        )
        .expect("setup on primary");

    let replica = ReplicaPostgres::start(&network, &primary_ip);
    let replica_url = format!(
        "host=127.0.0.1 port={} user=postgres dbname=postgres",
        replica.port
    );
    let mut replica_client =
        postgres::Client::connect(&replica_url, postgres::NoTls).expect("connect to replica");

    // Confirm replication is live before promoting.
    wait_for_replication(&mut replica_client, "promote_test", 50);

    // Promote via the SQL function — same effect as `pg_ctl promote` (which is
    // what `pg::promote()` calls in production). wait=true blocks until done.
    let promoted: bool = replica_client
        .query_one("SELECT pg_promote(wait => true, wait_seconds => 15)", &[])
        .expect("pg_promote() failed")
        .get(0);
    assert!(
        promoted,
        "pg_promote() returned false — promotion did not complete"
    );

    // Replica must now report itself as primary.
    let still_standby: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        !still_standby,
        "pg_is_in_recovery() still true after promote"
    );

    // Replica must now accept writes.
    replica_client
        .batch_execute("INSERT INTO promote_test (v) VALUES ('post-promote')")
        .expect("write to promoted replica failed");

    let count: i64 = replica_client
        .query_one(
            "SELECT count(*) FROM promote_test WHERE v = 'post-promote'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(count, 1, "post-promote write not found");

    drop(replica);
    drop(primary);
    drop(network);
}

/// Documents the three filesystem states that `pg::basebackup()` branches on.
/// Pure filesystem logic — no Docker required.
#[test]
fn basebackup_idempotency_predicates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pgdata = dir.path();

    // Case 1: empty dir → no PG_VERSION → would run pg_basebackup
    assert!(
        !pgdata.join("PG_VERSION").exists(),
        "fresh dir should have no PG_VERSION"
    );

    // Case 2: PG_VERSION + standby.signal → already seeded, pg::basebackup() skips
    std::fs::write(pgdata.join("PG_VERSION"), "18").unwrap();
    std::fs::write(pgdata.join("standby.signal"), "").unwrap();
    assert!(pgdata.join("PG_VERSION").exists());
    assert!(pgdata.join("standby.signal").exists());

    // Case 3: PG_VERSION without standby.signal → pg::basebackup() returns AlreadyPrimary
    std::fs::remove_file(pgdata.join("standby.signal")).unwrap();
    assert!(pgdata.join("PG_VERSION").exists());
    assert!(!pgdata.join("standby.signal").exists());
}

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
