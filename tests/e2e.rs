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
    ///
    /// Uses `config::replica_conf()` — the production function — to write the
    /// replica config. If `replica_conf()` output changes, this test catches it.
    fn start(network: &TestNetwork, primary_ip: &str) -> Self {
        let pgdata = tempfile::tempdir().expect("tempdir for replica PGDATA");
        let pgdata_str = pgdata.path().to_str().unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(pgdata.path(), std::fs::Permissions::from_mode(0o777))
                .expect("chmod replica pgdata");
        }

        // 1. pg_basebackup inside a postgres:18 container on the same network.
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

        // 2. Write the replica config using the production function.
        //    config::replica_conf() generates exactly what `do_boot_replica` writes
        //    in production. If the function's output changes, this test uses the new
        //    config — not a stale hand-written copy.
        let conninfo = format!("host={primary_ip} port=5432 user=postgres");
        let replica_conf = beyond_pg::config::replica_conf(&conninfo, None);

        // Write conf.d/04-replica.conf from the host (pgdata dir is 777 so host can create).
        std::fs::create_dir_all(pgdata.path().join("conf.d")).expect("create conf.d");
        std::fs::write(pgdata.path().join("conf.d/04-replica.conf"), &replica_conf)
            .expect("write 04-replica.conf");

        // postgresql.auto.conf is owned by the postgres user inside the container,
        // so write to it via a root container. Also touch standby.signal.
        let script = "touch /pgdata/standby.signal && \
                      echo \"include_dir = 'conf.d'\" >> /pgdata/postgresql.auto.conf";
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
                script,
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

        // 4. Start the replica.
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
///   - `config::replica_conf()` — the production function — generates config that
///     Postgres 18 actually accepts for streaming replication
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

    network.connect(primary.id());
    let primary_ip = network.container_ip(primary.id());

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

    wait_for_replication(&mut replica_client, "replica_test", 100);

    primary_client
        .batch_execute(
            "INSERT INTO replica_test (v) \
               SELECT 'after-' || g FROM generate_series(1, 100) g;",
        )
        .expect("insert after replica started streaming");

    wait_for_replication(&mut replica_client, "replica_test", 200);

    let is_standby: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(is_standby, "replica should be in recovery (hot standby)");

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

    wait_for_replication(&mut replica_client, "promote_test", 50);

    let promoted: bool = replica_client
        .query_one("SELECT pg_promote(wait => true, wait_seconds => 15)", &[])
        .expect("pg_promote() failed")
        .get(0);
    assert!(
        promoted,
        "pg_promote() returned false — promotion did not complete"
    );

    let still_standby: bool = replica_client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        !still_standby,
        "pg_is_in_recovery() still true after promote"
    );

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

// ---------------------------------------------------------------------------
// PITR helpers
// ---------------------------------------------------------------------------

/// A Postgres primary with archive_mode=on, writing completed WAL segments to
/// a host-accessible directory via `cp`. PGDATA and the archive directory are
/// both bind-mounted from host tempdirs so the test can copy them directly.
struct PitrPrimary {
    container_id: String,
    pub port: u16,
    pub pgdata: tempfile::TempDir,
    pub archive: tempfile::TempDir,
}

impl PitrPrimary {
    fn start(network: &TestNetwork) -> Self {
        let pgdata = tempfile::tempdir().expect("pgdata tempdir");
        let archive = tempfile::tempdir().expect("archive tempdir");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for dir in [pgdata.path(), archive.path()] {
                std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o777))
                    .expect("chmod pitr dirs");
            }
        }

        let pgdata_str = pgdata.path().to_str().unwrap();
        let archive_str = archive.path().to_str().unwrap();

        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let script = "\
            chmod 700 /pgdata && chown postgres:postgres /pgdata /archive && \
            gosu postgres initdb -D /pgdata \
              --auth-host=trust --auth-local=trust --no-instructions && \
            exec gosu postgres postgres -D /pgdata \
              -c archive_mode=on \
              -c 'archive_command=cp %p /archive/%f' \
              -c wal_level=replica \
              -c max_wal_senders=5 \
              -c listen_addresses='*'";

        let out = std::process::Command::new("docker")
            .args([
                "run",
                "-d",
                &format!("--network={}", network.name),
                "--user",
                "root",
                "-v",
                &format!("{pgdata_str}:/pgdata"),
                "-v",
                &format!("{archive_str}:/archive"),
                "-p",
                &format!("{port}:5432"),
                "postgres:18",
                "bash",
                "-c",
                script,
            ])
            .output()
            .expect("docker run pitr primary");

        assert!(
            out.status.success(),
            "pitr primary start: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        let container_id = String::from_utf8_lossy(&out.stdout).trim().to_owned();

        let url = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if Instant::now() > deadline {
                panic!("pitr primary not ready on port {port} within 30s");
            }
            if postgres::Client::connect(&url, postgres::NoTls).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }

        Self {
            container_id,
            port,
            pgdata,
            archive,
        }
    }

    /// Copy PGDATA to a new tempdir via a one-shot container running as root.
    fn fork_pgdata_into(&self, fork_dir: &tempfile::TempDir) {
        let fork_str = fork_dir.path().to_str().unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(fork_dir.path(), std::fs::Permissions::from_mode(0o777))
                .expect("chmod fork dir");
        }

        let out = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--user",
                "root",
                "-v",
                &format!("{}:/fork", fork_str),
                "-v",
                &format!("{}:/pgdata:ro", self.pgdata.path().to_str().unwrap()),
                "postgres:18",
                "bash",
                "-c",
                "cp -a /pgdata/. /fork/",
            ])
            .output()
            .expect("docker cp pgdata to fork");

        assert!(
            out.status.success(),
            "fork copy: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

impl Drop for PitrPrimary {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.container_id])
            .output();
    }
}

// ---------------------------------------------------------------------------
// PITR integration test
// ---------------------------------------------------------------------------

/// PITR: write rows in three batches; fork PGDATA after batch 1 (simulating a
/// GlideFS volume snapshot); archive WAL for batches 2 and 3; recover the fork
/// to a point between batches 2 and 3.
///
/// Validates:
///   - `config::pitr_conf()` — the production function — generates a
///     `restore_command` / `recovery_target_time` config that Postgres 18
///     accepts and correctly executes for point-in-time recovery.
///   - `recovery.signal` triggers point-in-time recovery and promotes.
///   - Batches 1+2 present; batch 3 absent after recovery.
///
/// The archive uses `cp` as the archive_command (no S3 needed). The
/// restore_command comes verbatim from `config::pitr_conf()`:
///   `aws s3 cp /archive/%f %p --no-progress`
/// A fake `aws` binary installed in the recovery container translates this to
/// `cp /archive/%f %p` — so we test the exact command string production uses.
#[test]
#[ignore = "requires Docker"]
fn pitr_recovery() {
    let network = TestNetwork::create();
    let primary = PitrPrimary::start(&network);

    let primary_url = format!(
        "host=127.0.0.1 port={} user=postgres dbname=postgres",
        primary.port
    );
    let mut client =
        postgres::Client::connect(&primary_url, postgres::NoTls).expect("connect to pitr primary");

    // Batch 1: 50 rows — will be in the base snapshot.
    client
        .batch_execute(
            "CREATE TABLE events (id serial, batch int, ts timestamptz DEFAULT now()); \
             INSERT INTO events (batch) SELECT 1 FROM generate_series(1, 50);",
        )
        .expect("batch 1");

    client.batch_execute("CHECKPOINT").expect("checkpoint");
    client
        .batch_execute("SELECT pg_switch_wal()")
        .expect("switch wal after batch 1");
    std::thread::sleep(Duration::from_millis(300));

    // Fork PGDATA — simulates a GlideFS volume snapshot.
    let fork_dir = tempfile::tempdir().expect("fork pgdata tempdir");
    primary.fork_pgdata_into(&fork_dir);

    // Batch 2: will be replayed during PITR recovery.
    client
        .batch_execute("INSERT INTO events (batch) SELECT 2 FROM generate_series(1, 50);")
        .expect("batch 2");
    client
        .batch_execute("SELECT pg_switch_wal()")
        .expect("switch wal after batch 2");
    std::thread::sleep(Duration::from_millis(500));

    // Recovery target: strictly after batch 2, before batch 3.
    let target_time: String = client
        .query_one(
            "SELECT to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')",
            &[],
        )
        .expect("get target time")
        .get(0);
    std::thread::sleep(Duration::from_millis(500));

    // Batch 3: must NOT appear after recovery.
    client
        .batch_execute("INSERT INTO events (batch) SELECT 3 FROM generate_series(1, 50);")
        .expect("batch 3");
    client
        .batch_execute("SELECT pg_switch_wal()")
        .expect("switch wal after batch 3");
    std::thread::sleep(Duration::from_millis(300));

    let total: i64 = client
        .query_one("SELECT count(*) FROM events", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        total, 150,
        "primary should have 150 rows before recovery test"
    );

    // -----------------------------------------------------------------------
    // Prepare the fork for PITR recovery using the production config function.
    //
    // config::pitr_conf("/archive", Some(&target_time)) generates:
    //   restore_command = 'aws s3 cp /archive/%f %p --no-progress'
    //   recovery_target_time = '<target_time>'
    //   recovery_target_action = promote
    //   recovery_target_inclusive = true
    //
    // The recovery container installs a fake `aws` that translates
    // `aws s3 cp SRC DST ...` → `cp SRC DST`, so we exercise the exact
    // restore_command string that production uses.
    // -----------------------------------------------------------------------

    // Add include_dir to postgresql.auto.conf (always-included; container
    // owns the file so we need a root container to append to it).
    let setup_script = "echo \"include_dir = 'conf.d'\" >> /pgdata/postgresql.auto.conf";
    let fork_str = fork_dir.path().to_str().unwrap();
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--user",
            "root",
            "-v",
            &format!("{fork_str}:/pgdata"),
            "postgres:18",
            "bash",
            "-c",
            setup_script,
        ])
        .output()
        .expect("docker run fork setup");
    assert!(
        out.status.success(),
        "fork setup: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Write conf.d/05-pitr.conf using the production function.
    let conf_d = fork_dir.path().join("conf.d");
    std::fs::create_dir_all(&conf_d).expect("create conf.d");
    let pitr_conf = beyond_pg::config::pitr_conf("/archive", Some(&target_time));
    std::fs::write(conf_d.join("05-pitr.conf"), &pitr_conf).expect("write 05-pitr.conf");

    // recovery.signal triggers PITR mode instead of normal startup.
    std::fs::write(fork_dir.path().join("recovery.signal"), "").expect("write recovery.signal");

    // -----------------------------------------------------------------------
    // Start recovery container on the fork.
    // Install fake `aws` before starting postgres: translates
    // `aws s3 cp SRC DST --no-progress` → `cp SRC DST`.
    // -----------------------------------------------------------------------
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let recovery_port = probe.local_addr().unwrap().port();
    drop(probe);

    let archive_str = primary.archive.path().to_str().unwrap();

    let start = std::process::Command::new("docker")
        .args([
            "run",
            "-d",
            &format!("--network={}", network.name),
            "--user",
            "root",
            "-v",
            &format!("{fork_str}:/pgdata"),
            "-v",
            &format!("{archive_str}:/archive"),
            "-p",
            &format!("{recovery_port}:5432"),
            "postgres:18",
            "bash",
            "-c",
            // Fake `aws` maps `aws s3 cp SRC DST --no-progress` → `cp SRC DST`.
            // This lets us use the exact restore_command that config::pitr_conf()
            // generates without needing real AWS credentials.
            "printf '%s\\n%s\\n' '#!/bin/sh' 'exec cp \"$3\" \"$4\"' > /usr/local/bin/aws && \
             chmod +x /usr/local/bin/aws && \
             chmod 700 /pgdata && chown -R postgres:postgres /pgdata && \
             exec gosu postgres postgres -D /pgdata",
        ])
        .output()
        .expect("docker run recovery");

    assert!(
        start.status.success(),
        "recovery container start: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let recovery_id = String::from_utf8_lossy(&start.stdout).trim().to_owned();

    // Wait for Postgres to promote and accept connections.
    let recovery_url = format!("host=127.0.0.1 port={recovery_port} user=postgres dbname=postgres");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut recovered = loop {
        if Instant::now() > deadline {
            panic!("recovery instance not ready on port {recovery_port} within 60s");
        }
        if let Ok(c) = postgres::Client::connect(&recovery_url, postgres::NoTls) {
            break c;
        }
        std::thread::sleep(Duration::from_millis(500));
    };

    // -----------------------------------------------------------------------
    // Verify recovery stopped at the right point.
    // -----------------------------------------------------------------------

    let count: i64 = recovered
        .query_one("SELECT count(*) FROM events", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count, 100,
        "expected 100 rows after PITR (batches 1+2), got {count}"
    );

    let batch3: i64 = recovered
        .query_one("SELECT count(*) FROM events WHERE batch = 3", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        batch3, 0,
        "batch 3 rows should not be present after PITR recovery"
    );

    let in_recovery: bool = recovered
        .query_one("SELECT pg_is_in_recovery()", &[])
        .unwrap()
        .get(0);
    assert!(
        !in_recovery,
        "instance should be promoted (not in recovery) after PITR"
    );

    let _ = std::process::Command::new("docker")
        .args(["rm", "-f", &recovery_id])
        .output();
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

/// Runs the CDC setup SQL from `supervisor::post_start` against a real Postgres
/// instance and verifies the slot and publication are created correctly and
/// idempotently.
///
/// Uses `beyond_pg::sql::CDC_SLOT_SQL` and `beyond_pg::sql::CDC_PUBLICATION_SQL`
/// — the exact constants that `supervisor.rs` executes. If the production SQL
/// changes, this test automatically uses the new SQL.
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

    // Use the exact SQL constants from sql.rs — the same ones supervisor.rs runs.
    client
        .batch_execute(beyond_pg::sql::REPLICATOR_ROLE_SQL)
        .expect("failed to create replicator role");

    client
        .batch_execute(beyond_pg::sql::CDC_SLOT_SQL)
        .expect("failed to create CDC slot");

    client
        .batch_execute(beyond_pg::sql::CDC_PUBLICATION_SQL)
        .expect("failed to create CDC publication");

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

    // Idempotence: running the same SQL again must not error.
    client
        .batch_execute(beyond_pg::sql::CDC_SLOT_SQL)
        .expect("CDC slot SQL not idempotent");
    client
        .batch_execute(beyond_pg::sql::CDC_PUBLICATION_SQL)
        .expect("CDC publication SQL not idempotent");

    // Exactly one slot, one publication after two runs.
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

    drop(container);
}
