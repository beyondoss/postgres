//! End-to-end tests for `beyond-pg boot`.
//!
//! Builds a static musl binary, bind-mounts it into a `postgres:18` Docker
//! container alongside host tempdirs at every hardcoded system path, runs
//! `beyond-pg boot`, then asserts output files from the host.
//!
//! Key design decisions:
//! - Mount `/var/lib/postgresql/18` (the PARENT), not PGDATA itself.
//!   `maybe_initdb()` calls `remove_dir_all(PGDATA)` on partial-init state;
//!   that fails with EBUSY if PGDATA is the bind-mount point.  Mounting the
//!   parent makes PGDATA an ordinary subdirectory that can be removed.
//!
//! - The binary runs as `--user root` (writes to /etc/, /run/, /sys/).
//!   `initdb` refuses to run as root, so `run_boot` installs a thin wrapper
//!   at `/usr/local/bin/initdb` that chmod 0644s the pwfile then execs
//!   `gosu postgres initdb`.
//!
//! Run: cargo test --test boot -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::json;

// Serialize Docker container launches: parallel launches of containers that
// bind-mount the same freshly-built binary trigger VirtioFS cache-race on
// macOS Docker Desktop.  A single mutex is cheap — boot tests are I/O-bound.
static DOCKER: Mutex<()> = Mutex::new(());

// ---------------------------------------------------------------------------
// Build helper
// ---------------------------------------------------------------------------

/// Build `beyond-pg` as a static musl binary for the Docker host's architecture.
/// Returns `(binary_path, docker_platform)`, or `None` to skip the test.
fn build_linux_beyond_pg() -> Option<(PathBuf, &'static str)> {
    let arch_out = std::process::Command::new("docker")
        .args(["info", "--format", "{{.Architecture}}"])
        .output()
        .ok()?;
    let arch = std::str::from_utf8(&arch_out.stdout)
        .ok()?
        .trim()
        .to_owned();
    let (target, platform): (&'static str, &'static str) = match arch.as_str() {
        a if a == "aarch64" || a == "arm64" => ("aarch64-unknown-linux-musl", "linux/arm64"),
        _ => ("x86_64-unknown-linux-musl", "linux/amd64"),
    };

    let installed = std::process::Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .ok()?;
    let list = std::str::from_utf8(&installed.stdout).ok()?.to_owned();
    if !list.lines().any(|l| l.trim() == target) {
        eprintln!("skipping: {target} not installed\nrun: rustup target add {target}");
        return None;
    }

    let status = std::process::Command::new("cargo")
        .args(["build", "--release", "-p", "beyond-pg", "--target", target])
        .status()
        .ok()?;
    if !status.success() {
        eprintln!("skipping: musl build failed for beyond-pg");
        return None;
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let binary = manifest
        .join("target")
        .join(target)
        .join("release")
        .join("beyond-pg");
    binary.exists().then_some((binary, platform))
}

// ---------------------------------------------------------------------------
// BootEnv — bind-mount manager
// ---------------------------------------------------------------------------

struct BootEnv {
    /// → /var/lib/postgresql/18  (contains main/ = PGDATA, wal/ = WAL)
    pg_lib: tempfile::TempDir,
    /// → /etc/postgresql/18/main  (pg_hba.conf lives here)
    etc_pg: tempfile::TempDir,
    /// → /etc/pgbouncer
    etc_pgb: tempfile::TempDir,
    /// → /etc/sysctl.d
    etc_sysctl: tempfile::TempDir,
}

impl BootEnv {
    fn new() -> Self {
        let env = Self {
            pg_lib: tempfile::tempdir().expect("pg_lib tempdir"),
            etc_pg: tempfile::tempdir().expect("etc_pg tempdir"),
            etc_pgb: tempfile::tempdir().expect("etc_pgb tempdir"),
            etc_sysctl: tempfile::tempdir().expect("etc_sysctl tempdir"),
        };
        // chmod 777 so the postgres user inside the container can write.
        // Root can write anyway; world-write lets postgres write to conf dirs.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for dir in [&env.pg_lib, &env.etc_pg, &env.etc_pgb, &env.etc_sysctl] {
                std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777))
                    .expect("chmod 777 tempdir");
            }
        }
        env
    }

    /// PGDATA path on the host (inside pg_lib).
    fn pgdata(&self) -> PathBuf {
        self.pg_lib.path().join("main")
    }

    /// Run `beyond-pg boot` inside a fresh `postgres:18` container.
    ///
    /// An initdb wrapper is installed at `/usr/local/bin/initdb` to bridge the
    /// root→postgres transition: `initdb` refuses uid 0, so the wrapper
    /// 1. chmod 0644s the pwfile (created 0600 root-owned by run_initdb), then
    /// 2. execs `gosu postgres /usr/lib/postgresql/18/bin/initdb`.
    fn run_boot(&self, binary: &Path, platform: &str, mmds_json: &str) -> std::process::Output {
        self.run_boot_ex(binary, platform, mmds_json, &[], "postgres:18")
    }

    /// Like `run_boot` but accepts extra docker args (inserted before the image name)
    /// and a custom image.  Used to add `--network`, `--add-host`, etc.
    fn run_boot_ex(
        &self,
        binary: &Path,
        platform: &str,
        mmds_json: &str,
        extra_docker_args: &[&str],
        image: &str,
    ) -> std::process::Output {
        let mmds_file = tempfile::NamedTempFile::new().expect("mmds tempfile");
        std::fs::write(mmds_file.path(), mmds_json).expect("write mmds");

        let pg_lib = self.pg_lib.path().display().to_string();
        let etc_pg = self.etc_pg.path().display().to_string();
        let etc_pgb = self.etc_pgb.path().display().to_string();
        let etc_sysctl = self.etc_sysctl.path().display().to_string();
        let mmds = mmds_file.path().display().to_string();
        let bin = binary.display().to_string();

        // Pre-format mount strings so they live long enough for the args slice.
        let v_bin = format!("{bin}:/usr/local/bin/beyond-pg");
        let v_mmds = format!("{mmds}:/run/mmds/metadata.json");
        let v_pglib = format!("{pg_lib}:/var/lib/postgresql/18");
        let v_etcpg = format!("{etc_pg}:/etc/postgresql/18/main");
        let v_etcpgb = format!("{etc_pgb}:/etc/pgbouncer");
        let v_sysctl = format!("{etc_sysctl}:/etc/sysctl.d");

        const BOOT_CMD: &str = "printf '%s\\n' '#!/bin/sh' \
                 'for a in \"$@\"; do case \"$a\" in --pwfile=*) chmod 0644 \"${a#--pwfile=}\" 2>/dev/null;; esac; done' \
                 'chown postgres:postgres /var/lib/postgresql/18/main 2>/dev/null || true' \
                 'exec gosu postgres /usr/lib/postgresql/18/bin/initdb \"$@\"' \
                 > /usr/local/bin/initdb && \
             chmod +x /usr/local/bin/initdb && \
             mkdir -p /etc/postgresql/18/hooks/pre-start.d && \
             /usr/local/bin/beyond-pg boot";

        let mut args: Vec<&str> = vec![
            "run",
            "--rm",
            "--platform",
            platform,
            "--user",
            "root",
            "-v",
            &v_bin,
            "-v",
            &v_mmds,
            "-v",
            &v_pglib,
            "-v",
            &v_etcpg,
            "-v",
            &v_etcpgb,
            "-v",
            &v_sysctl,
        ];
        args.extend_from_slice(extra_docker_args);
        args.extend_from_slice(&[image, "bash", "-c", BOOT_CMD]);

        let out = std::process::Command::new("docker")
            .args(&args)
            .output()
            .expect("docker run");
        // mmds_file dropped here — tempfile removes it
        out
    }
}

// ---------------------------------------------------------------------------
// MMDS JSON helper
// ---------------------------------------------------------------------------

fn mmds(fields: &[(&str, &str)]) -> String {
    let mut meta = serde_json::Map::new();
    meta.insert("POSTGRES_PASSWORD".into(), json!("testpassword123"));
    for (k, v) in fields {
        meta.insert(k.to_string(), json!(v));
    }
    json!({ "latest": { "meta-data": meta } }).to_string()
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

/// Asserts standard output files are correct after any successful boot.
/// Does NOT check PITR state (recovery.signal / 05-pitr.conf) — call
/// `assert_no_pitr_state` separately for non-PITR boots.
fn assert_boot_files(env: &BootEnv) {
    let pgdata = env.pgdata();

    assert!(pgdata.join("PG_VERSION").exists(), "PG_VERSION missing");

    // pg_wal must be a symlink to the production container path, not the host tempdir.
    let link = pgdata.join("pg_wal");
    let meta = link
        .symlink_metadata()
        .expect("pg_wal entry missing from PGDATA");
    assert!(meta.file_type().is_symlink(), "pg_wal is not a symlink");
    assert_eq!(
        std::fs::read_link(&link).unwrap(),
        Path::new("/var/lib/postgresql/18/wal"),
        "pg_wal symlink points to wrong target"
    );

    assert_eq!(
        std::fs::read_to_string(pgdata.join("postgresql.conf")).unwrap(),
        beyond_pg::config::BEYOND_CONF,
        "postgresql.conf content mismatch"
    );

    assert_eq!(
        std::fs::read_to_string(env.etc_pg.path().join("pg_hba.conf")).unwrap(),
        beyond_pg::config::PG_HBA_CONF,
        "pg_hba.conf content mismatch"
    );

    assert_eq!(
        std::fs::read_to_string(env.etc_pgb.path().join("pgbouncer.ini")).unwrap(),
        beyond_pg::config::PGBOUNCER_INI,
        "pgbouncer.ini content mismatch"
    );

    for name in ["01-tuning.conf", "02-memory.conf"] {
        assert!(
            pgdata.join("conf.d").join(name).exists(),
            "{name} missing from conf.d"
        );
    }

    assert!(
        pgdata.join("beyond/server.crt").exists(),
        "TLS cert missing"
    );
    assert!(pgdata.join("beyond/server.key").exists(), "TLS key missing");
}

fn assert_no_pitr_state(env: &BootEnv) {
    let pgdata = env.pgdata();
    assert!(
        !pgdata.join("recovery.signal").exists(),
        "unexpected recovery.signal — would put Postgres into recovery mode"
    );
    assert!(
        !pgdata.join("conf.d/05-pitr.conf").exists(),
        "unexpected 05-pitr.conf for non-PITR boot"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Core end-to-end proof: `beyond-pg boot` on a blank data volume produces a
/// working Postgres cluster that accepts connections and is not in recovery mode.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_initializes_fresh_pgdata() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();
    let out = env.run_boot(&binary, platform, &mmds(&[]));
    eprintln!("stdout: {}", String::from_utf8_lossy(&out.stdout));
    eprintln!("stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "boot exited {}", out.status);

    assert_boot_files(&env);
    assert_no_pitr_state(&env);

    // Verify 01-tuning.conf has the correct production value for huge_pages.
    let tuning = std::fs::read_to_string(env.pgdata().join("conf.d/01-tuning.conf")).unwrap();
    assert!(
        tuning.contains("huge_pages = on"),
        "01-tuning.conf missing huge_pages=on: {tuning}"
    );

    // Start Postgres in a second container on the same bind-mounted PGDATA.
    // Test-only overrides appended to postgresql.auto.conf (takes precedence):
    //   huge_pages = try   — unprivileged Docker can't reserve hugepages; verified above
    //   shared_preload_libraries — strip custom extensions absent from postgres:18
    let pg_lib = env.pg_lib.path().display().to_string();
    let etc_pg = env.etc_pg.path().display().to_string();
    let bin = binary.display().to_string();

    let pg_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--platform",
            platform,
            "--user",
            "root",
            "-v",
            &format!("{bin}:/usr/local/bin/beyond-pg"),
            "-v",
            &format!("{pg_lib}:/var/lib/postgresql/18"),
            "-v",
            &format!("{etc_pg}:/etc/postgresql/18/main"),
            "postgres:18",
            "bash",
            "-c",
            "printf '%s\\n' \
                 'huge_pages = try' \
                 \"shared_preload_libraries = 'pg_stat_statements,auto_explain'\" \
                 >> /var/lib/postgresql/18/main/postgresql.auto.conf && \
             chown -R postgres:postgres /var/lib/postgresql/18/ && \
             mkdir -p /var/run/postgresql && chown postgres:postgres /var/run/postgresql && \
             gosu postgres postgres -D /var/lib/postgresql/18/main -p 5433 & \
             PG_PID=$! && \
             for i in $(seq 1 30); do \
               pg_isready -h /var/run/postgresql -p 5433 -q && break; \
               sleep 1; \
             done && \
             gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -c 'SELECT 1' && \
             gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres \
               -c 'SELECT NOT pg_is_in_recovery()' | grep -q t && \
             kill $PG_PID && wait $PG_PID 2>/dev/null; true",
        ])
        .output()
        .expect("docker run postgres");

    eprintln!("pg stdout: {}", String::from_utf8_lossy(&pg_out.stdout));
    eprintln!("pg stderr: {}", String::from_utf8_lossy(&pg_out.stderr));
    assert!(
        pg_out.status.success(),
        "Postgres failed to start or accept connections: {}",
        pg_out.status
    );
}

/// Running boot twice on the same PGDATA must not error and must not corrupt files.
/// Exercises the `maybe_initdb()` "PGDATA already initialized, skipping" path.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_is_idempotent() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();
    let mmds_json = mmds(&[]);

    // First boot
    let out1 = env.run_boot(&binary, platform, &mmds_json);
    eprintln!("boot1 stderr: {}", String::from_utf8_lossy(&out1.stderr));
    assert!(out1.status.success(), "first boot failed: {}", out1.status);

    // Snapshot file contents
    let conf = std::fs::read_to_string(env.pgdata().join("postgresql.conf")).unwrap();
    let tuning = std::fs::read_to_string(env.pgdata().join("conf.d/01-tuning.conf")).unwrap();
    let hba = std::fs::read_to_string(env.etc_pg.path().join("pg_hba.conf")).unwrap();
    let pgb = std::fs::read_to_string(env.etc_pgb.path().join("pgbouncer.ini")).unwrap();

    // Second boot — must succeed without corrupting any files
    let out2 = env.run_boot(&binary, platform, &mmds_json);
    eprintln!("boot2 stderr: {}", String::from_utf8_lossy(&out2.stderr));
    assert!(out2.status.success(), "second boot failed: {}", out2.status);

    assert_eq!(
        std::fs::read_to_string(env.pgdata().join("postgresql.conf")).unwrap(),
        conf,
        "postgresql.conf changed on second boot"
    );
    assert_eq!(
        std::fs::read_to_string(env.pgdata().join("conf.d/01-tuning.conf")).unwrap(),
        tuning,
        "01-tuning.conf changed on second boot"
    );
    assert_eq!(
        std::fs::read_to_string(env.etc_pg.path().join("pg_hba.conf")).unwrap(),
        hba,
        "pg_hba.conf changed on second boot"
    );
    assert_eq!(
        std::fs::read_to_string(env.etc_pgb.path().join("pgbouncer.ini")).unwrap(),
        pgb,
        "pgbouncer.ini changed on second boot"
    );
}

/// Boot with BEYOND_PG_ARCHIVE_TARGET + BEYOND_PG_RECOVERY_TARGET_TIME writes
/// exactly `config::pitr_conf(...)` and creates recovery.signal.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_writes_pitr_config() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();
    let out = env.run_boot(
        &binary,
        platform,
        &mmds(&[
            ("BEYOND_PG_ARCHIVE_TARGET", "s3://test-bucket/wal"),
            ("BEYOND_PG_RECOVERY_TARGET_TIME", "2026-05-14 03:00:00"),
        ]),
    );
    eprintln!("stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "boot failed: {}", out.status);

    let pitr_path = env.pgdata().join("conf.d/05-pitr.conf");
    assert!(pitr_path.exists(), "05-pitr.conf not written");
    assert_eq!(
        std::fs::read_to_string(&pitr_path).unwrap(),
        beyond_pg::config::pitr_conf("s3://test-bucket/wal", Some("2026-05-14 03:00:00")),
        "05-pitr.conf content mismatch — binary diverged from pitr_conf()"
    );

    assert!(
        env.pgdata().join("recovery.signal").exists(),
        "recovery.signal not created in PITR mode"
    );

    // Standard files must still be correct
    assert_boot_files(&env);
}

/// When PITR config is removed from MMDS between boots, the second boot must
/// remove `05-pitr.conf` and `recovery.signal`. A stale `recovery.signal` would
/// put Postgres into unexpected recovery mode on the next start — data loss risk.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_clears_pitr_state_on_second_boot() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();

    // First boot: PITR mode enabled
    let out1 = env.run_boot(
        &binary,
        platform,
        &mmds(&[
            ("BEYOND_PG_ARCHIVE_TARGET", "s3://test-bucket/wal"),
            ("BEYOND_PG_RECOVERY_TARGET_TIME", "2026-05-14 03:00:00"),
        ]),
    );
    eprintln!("boot1 stderr: {}", String::from_utf8_lossy(&out1.stderr));
    assert!(out1.status.success(), "first boot failed: {}", out1.status);
    assert!(env.pgdata().join("conf.d/05-pitr.conf").exists());
    assert!(env.pgdata().join("recovery.signal").exists());

    // Second boot: PITR config removed (recovery completed, operator cleared MMDS)
    let out2 = env.run_boot(&binary, platform, &mmds(&[]));
    eprintln!("boot2 stderr: {}", String::from_utf8_lossy(&out2.stderr));
    assert!(out2.status.success(), "second boot failed: {}", out2.status);

    assert!(
        !env.pgdata().join("conf.d/05-pitr.conf").exists(),
        "05-pitr.conf not removed after PITR config cleared"
    );
    assert!(
        !env.pgdata().join("recovery.signal").exists(),
        "recovery.signal not removed — stale signal would cause unexpected recovery"
    );
}

/// Boot with BEYOND_PG_WAL_SINK writes 03-wal-sink.conf; boot without it removes it.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_configures_wal_sink() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();

    // First boot: WAL sink enabled
    let out1 = env.run_boot(
        &binary,
        platform,
        &mmds(&[("BEYOND_PG_WAL_SINK", "http://10.0.0.5:9000")]),
    );
    eprintln!("boot1 stderr: {}", String::from_utf8_lossy(&out1.stderr));
    assert!(
        out1.status.success(),
        "boot with WAL sink failed: {}",
        out1.status
    );

    let sink_path = env.pgdata().join("conf.d/03-wal-sink.conf");
    assert!(sink_path.exists(), "03-wal-sink.conf not written");
    assert_eq!(
        std::fs::read_to_string(&sink_path).unwrap(),
        beyond_pg::config::wal_sink_conf(),
        "03-wal-sink.conf content mismatch"
    );

    // Second boot: WAL sink removed
    let out2 = env.run_boot(&binary, platform, &mmds(&[]));
    eprintln!("boot2 stderr: {}", String::from_utf8_lossy(&out2.stderr));
    assert!(out2.status.success(), "second boot failed: {}", out2.status);
    assert!(
        !sink_path.exists(),
        "03-wal-sink.conf not removed when BEYOND_PG_WAL_SINK cleared"
    );
}

/// Boot with BEYOND_VOLUME_EPHEMERAL=true writes 03-durability.conf;
/// boot without it removes it.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_configures_ephemeral_volume() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();

    // First boot: ephemeral mode
    let out1 = env.run_boot(
        &binary,
        platform,
        &mmds(&[("BEYOND_VOLUME_EPHEMERAL", "true")]),
    );
    eprintln!("boot1 stderr: {}", String::from_utf8_lossy(&out1.stderr));
    assert!(
        out1.status.success(),
        "ephemeral boot failed: {}",
        out1.status
    );

    let durability_path = env.pgdata().join("conf.d/03-durability.conf");
    assert!(durability_path.exists(), "03-durability.conf not written");
    assert_eq!(
        std::fs::read_to_string(&durability_path).unwrap(),
        beyond_pg::config::DURABILITY_CONF_EPHEMERAL,
        "03-durability.conf content mismatch"
    );

    // Second boot: ephemeral mode off
    let out2 = env.run_boot(&binary, platform, &mmds(&[]));
    eprintln!("boot2 stderr: {}", String::from_utf8_lossy(&out2.stderr));
    assert!(out2.status.success(), "second boot failed: {}", out2.status);
    assert!(
        !durability_path.exists(),
        "03-durability.conf not removed when BEYOND_VOLUME_EPHEMERAL cleared"
    );
}

/// Replica boot writes correct config files.  PGDATA is pre-seeded so
/// `pg::basebackup()` short-circuits on the idempotency path
/// (PG_VERSION + standby.signal present) without needing a live primary.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_replica_with_preseeded_pgdata() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();

    // Pre-seed: run initdb as postgres and touch standby.signal.
    // This makes basebackup() return Ok(()) immediately (idempotency skip path).
    let pg_lib = env.pg_lib.path().display().to_string();
    let seed_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--platform",
            platform,
            "--user",
            "root",
            "-v",
            &format!("{pg_lib}:/var/lib/postgresql/18"),
            "postgres:18",
            "bash",
            "-c",
            "mkdir -p /var/lib/postgresql/18 && \
             chown -R postgres:postgres /var/lib/postgresql/18/ && \
             gosu postgres initdb -D /var/lib/postgresql/18/main \
               --waldir=/var/lib/postgresql/18/wal \
               --auth-host=trust --auth-local=trust && \
             touch /var/lib/postgresql/18/main/standby.signal",
        ])
        .output()
        .expect("docker run pre-seed");

    eprintln!("seed stderr: {}", String::from_utf8_lossy(&seed_out.stderr));
    assert!(
        seed_out.status.success(),
        "pre-seed initdb failed: {}",
        seed_out.status
    );

    // Run replica boot — basebackup() sees PG_VERSION + standby.signal and skips.
    let out = env.run_boot(
        &binary,
        platform,
        &mmds(&[
            ("BEYOND_PG_TIER", "replica"),
            (
                "BEYOND_PG_PRIMARY_CONNINFO",
                "host=10.0.0.1 port=5432 user=replicator",
            ),
        ]),
    );
    eprintln!("boot stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "replica boot failed: {}", out.status);

    let pgdata = env.pgdata();

    assert!(
        pgdata.join("standby.signal").exists(),
        "standby.signal missing after replica boot"
    );

    let replica_conf_path = pgdata.join("conf.d/04-replica.conf");
    assert!(replica_conf_path.exists(), "04-replica.conf not written");
    assert_eq!(
        std::fs::read_to_string(&replica_conf_path).unwrap(),
        beyond_pg::config::replica_conf("host=10.0.0.1 port=5432 user=replicator", None),
        "04-replica.conf content mismatch — binary diverged from replica_conf()"
    );

    assert!(
        pgdata.join("beyond/server.crt").exists(),
        "TLS cert missing"
    );
    assert!(pgdata.join("beyond/server.key").exists(), "TLS key missing");

    assert_no_pitr_state(&env);
}

/// `fetch_wal_gap` re-downloads a missing WAL segment from the sink.
///
/// Steps:
///   1. Boot to initialize PGDATA.
///   2. Read the redo WAL segment from pg_controldata.
///   3. Remove that segment from the WAL dir (simulate a gap).
///   4. Start a fake WAL sink HTTP server on the host.
///   5. Boot again with BEYOND_PG_WAL_SINK pointing at the fake server.
///   6. Assert the segment was re-fetched.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_fetch_wal_gap() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();

    // Phase 1: boot to initialize PGDATA.
    let boot_out = env.run_boot(&binary, platform, &mmds(&[]));
    eprintln!("boot stderr: {}", String::from_utf8_lossy(&boot_out.stderr));
    assert!(
        boot_out.status.success(),
        "initial boot failed: {}",
        boot_out.status
    );

    // Phase 2: get the redo WAL segment from pg_controldata.
    let pg_lib = env.pg_lib.path().display().to_string();
    let ctrl_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--platform",
            platform,
            "-v",
            &format!("{pg_lib}:/var/lib/postgresql/18"),
            "postgres:18",
            "pg_controldata",
            "-D",
            "/var/lib/postgresql/18/main",
        ])
        .output()
        .expect("pg_controldata docker run");
    assert!(
        ctrl_out.status.success(),
        "pg_controldata failed: {}",
        String::from_utf8_lossy(&ctrl_out.stderr)
    );
    let ctrl_text = String::from_utf8_lossy(&ctrl_out.stdout);
    let redo_segment = ctrl_text
        .lines()
        .find(|l| l.contains("Latest checkpoint's REDO WAL file"))
        .and_then(|l| l.splitn(2, ':').nth(1))
        .map(|s| s.trim().to_string())
        .expect("could not parse redo WAL segment from pg_controldata output");

    assert_eq!(
        redo_segment.len(),
        24,
        "redo segment name should be 24 hex chars: {redo_segment}"
    );

    // Phase 3: read the segment bytes from the WAL dir then delete it.
    let wal_dir = env.pg_lib.path().join("wal");
    let segment_path = wal_dir.join(&redo_segment);
    let segment_bytes = std::fs::read(&segment_path).unwrap_or_else(|_| {
        panic!(
            "redo WAL segment {redo_segment} not found in WAL dir: {}",
            wal_dir.display()
        )
    });
    std::fs::remove_file(&segment_path)
        .unwrap_or_else(|e| panic!("failed to remove {redo_segment}: {e}"));

    // Phase 4: start a minimal HTTP server on the host to act as the WAL sink.
    // Serves GET /list → segment name, GET /{segment} → bytes.
    // `--add-host=host.docker.internal:host-gateway` makes this hostname
    // resolvable inside the container on both macOS Docker Desktop and Linux Docker.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fake WAL sink listener");
    let port = listener.local_addr().unwrap().port();
    let seg_name = redo_segment.clone();
    let seg_bytes = segment_bytes.clone();
    std::thread::spawn(move || serve_wal_sink(listener, seg_name, seg_bytes));

    // Phase 5: boot with WAL sink — fetch_wal_gap should re-download the segment.
    let sink_url = format!("http://host.docker.internal:{port}");
    let out = env.run_boot_ex(
        &binary,
        platform,
        &mmds(&[("BEYOND_PG_WAL_SINK", &sink_url)]),
        &["--add-host=host.docker.internal:host-gateway"],
        "postgres:18",
    );
    eprintln!(
        "wal-gap boot stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "boot with WAL sink failed: {}",
        out.status
    );

    // Phase 6: verify the segment was re-fetched.
    assert!(
        segment_path.exists(),
        "redo WAL segment {redo_segment} was not re-fetched by fetch_wal_gap"
    );
    assert_eq!(
        std::fs::read(&segment_path).unwrap(),
        segment_bytes,
        "re-fetched segment bytes differ from original"
    );
}

/// Minimal HTTP server that acts as a WAL sink for `boot_fetch_wal_gap`.
///
/// Serves two endpoints:
///   GET /list          → one-line response: the single segment name
///   GET /{segment}     → the raw WAL segment bytes
fn serve_wal_sink(listener: std::net::TcpListener, segment: String, bytes: Vec<u8>) {
    use std::io::{Read, Write};
    // Serve a small number of requests (boot makes at most 2: /list and /segment).
    for stream in listener.incoming().take(4) {
        let Ok(mut stream) = stream else {
            break;
        };
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);
        let first_line = req.lines().next().unwrap_or("");

        if first_line.contains(" /list ") || first_line.ends_with(" /list") {
            let body = format!("{segment}\n");
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
        } else if first_line.contains(&format!(" /{segment} "))
            || first_line.ends_with(&format!(" /{segment}"))
        {
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&bytes);
        } else {
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Docker RAII helpers for multi-container tests
// ---------------------------------------------------------------------------

struct DockerContainer(String);
impl DockerContainer {
    fn start_detached(args: &[&str]) -> Self {
        let out = std::process::Command::new("docker")
            .args(args)
            .output()
            .expect("docker run -d");
        assert!(out.status.success(), "docker run -d failed");
        let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
        Self(id)
    }

    fn id(&self) -> &str {
        &self.0
    }

    fn exec_status(&self, args: &[&str]) -> bool {
        let mut cmd_args = vec!["exec", self.id()];
        cmd_args.extend_from_slice(args);
        std::process::Command::new("docker")
            .args(&cmd_args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn exec_output(&self, args: &[&str]) -> std::process::Output {
        let mut cmd_args = vec!["exec", self.id()];
        cmd_args.extend_from_slice(args);
        std::process::Command::new("docker")
            .args(&cmd_args)
            .output()
            .expect("docker exec")
    }
}
impl Drop for DockerContainer {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .status();
    }
}

struct DockerNetwork(String);
impl DockerNetwork {
    fn create(name: &str) -> Self {
        let s = std::process::Command::new("docker")
            .args(["network", "create", name])
            .status()
            .expect("docker network create");
        assert!(s.success(), "docker network create failed");
        Self(name.to_string())
    }
}
impl Drop for DockerNetwork {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["network", "rm", &self.0])
            .status();
    }
}

/// `pg::basebackup()` actually runs pg_basebackup against a live primary.
///
/// Steps:
///   1. Create a Docker network for primary↔replica communication.
///   2. Start a postgres:18 primary with trust auth on that network.
///   3. Create the replicator role on the primary.
///   4. Run `beyond-pg boot` with tier=replica — pg_basebackup seeds the PGDATA.
///   5. Assert PG_VERSION + standby.signal present, and 04-replica.conf correct.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_replica_real_basebackup() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };

    // Create an isolated Docker network so containers can reach each other by IP.
    let network_name = format!(
        "beyond-boot-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    );
    let _network = DockerNetwork::create(&network_name);

    // Start the primary — postgres:18 with trust auth so pg_basebackup
    // can connect as `replicator` without a password.
    let primary = DockerContainer::start_detached(&[
        "run",
        "-d",
        "--platform",
        platform,
        "--network",
        &network_name,
        "-e",
        "POSTGRES_HOST_AUTH_METHOD=trust",
        "-e",
        "POSTGRES_PASSWORD=ignored",
        "postgres:18",
    ]);

    // Wait for primary to be ready.
    let ready = (0..30).any(|_| {
        if primary.exec_status(&["pg_isready", "-h", "/var/run/postgresql", "-q"]) {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
        false
    });
    assert!(ready, "primary container never became ready");

    // Create the replicator role and allow replication connections from all hosts.
    // POSTGRES_HOST_AUTH_METHOD=trust covers regular connections but not replication;
    // we append the replication trust rule and reload.
    let role_out = primary.exec_output(&[
        "psql",
        "-U",
        "postgres",
        "-c",
        "CREATE ROLE replicator LOGIN REPLICATION",
    ]);
    assert!(
        role_out.status.success(),
        "failed to create replicator role: {}",
        String::from_utf8_lossy(&role_out.stderr)
    );
    let hba_out = primary.exec_output(&[
        "bash",
        "-c",
        "echo 'host replication all all trust' >> \"${PGDATA}/pg_hba.conf\" && \
         psql -U postgres -c 'SELECT pg_reload_conf()'",
    ]);
    assert!(
        hba_out.status.success(),
        "failed to add replication hba rule: {}",
        String::from_utf8_lossy(&hba_out.stderr)
    );

    // Get the primary's IP on our test network.
    let inspect_out = std::process::Command::new("docker")
        .args([
            "inspect",
            "-f",
            &format!(
                r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
                network_name
            ),
            primary.id(),
        ])
        .output()
        .expect("docker inspect primary");
    let primary_ip = String::from_utf8_lossy(&inspect_out.stdout)
        .trim()
        .to_string();
    assert!(!primary_ip.is_empty(), "could not get primary container IP");

    let conninfo = format!("host={primary_ip} port=5432 user=replicator");

    // Run `beyond-pg boot` with replica tier — pg_basebackup will actually run.
    let env = BootEnv::new();
    let out = env.run_boot_ex(
        &binary,
        platform,
        &mmds(&[
            ("BEYOND_PG_TIER", "replica"),
            ("BEYOND_PG_PRIMARY_CONNINFO", &conninfo),
        ]),
        &["--network", &network_name],
        "postgres:18",
    );
    eprintln!(
        "replica boot stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    eprintln!(
        "replica boot stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "replica boot (real pg_basebackup) failed: {}",
        out.status
    );

    let pgdata = env.pgdata();

    assert!(
        pgdata.join("PG_VERSION").exists(),
        "PG_VERSION missing — pg_basebackup did not seed PGDATA"
    );
    assert!(
        pgdata.join("standby.signal").exists(),
        "standby.signal missing — do_boot_replica did not write it"
    );

    let replica_conf_path = pgdata.join("conf.d/04-replica.conf");
    assert!(replica_conf_path.exists(), "04-replica.conf not written");
    assert_eq!(
        std::fs::read_to_string(&replica_conf_path).unwrap(),
        beyond_pg::config::replica_conf(&conninfo, None),
        "04-replica.conf content mismatch"
    );
}
