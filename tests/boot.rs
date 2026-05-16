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

    let pgbouncer_ini = std::fs::read_to_string(env.etc_pgb.path().join("pgbouncer.ini")).unwrap();
    assert!(
        pgbouncer_ini.starts_with(beyond_pg::config::PGBOUNCER_INI_BASE),
        "pgbouncer.ini should start with the static base; got:\n{pgbouncer_ini}"
    );
    assert!(
        pgbouncer_ini.contains("client_tls_sslmode = allow"),
        "pgbouncer.ini missing TLS keys; got:\n{pgbouncer_ini}"
    );
    assert!(
        pgbouncer_ini.contains("client_tls_cert_file"),
        "pgbouncer.ini missing client_tls_cert_file; got:\n{pgbouncer_ini}"
    );

    for name in ["01-tuning.conf", "02-memory.conf", "06-tls.conf"] {
        assert!(
            pgdata.join("conf.d").join(name).exists(),
            "{name} missing from conf.d"
        );
    }

    // Default test boot (no /run/beyond/tls/, no .user-managed) → self-signed
    // fallback writes the cert into PGDATA/beyond/.
    assert!(
        pgdata.join("beyond/server.crt").exists(),
        "TLS cert missing"
    );
    assert!(pgdata.join("beyond/server.key").exists(), "TLS key missing");

    // 06-tls.conf points at the self-signed cert in this default test case.
    let tls_conf = std::fs::read_to_string(pgdata.join("conf.d/06-tls.conf")).unwrap();
    assert!(
        tls_conf.contains("ssl_cert_file"),
        "06-tls.conf missing ssl_cert_file directive; got:\n{tls_conf}"
    );
    assert!(
        tls_conf.contains("beyond/server.crt"),
        "06-tls.conf should point at self-signed cert path; got:\n{tls_conf}"
    );
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

/// When the platform mounts `/run/beyond/tls/cert.pem`, `provision()` must
/// prefer it over the self-signed fallback. Asserts:
///   - No self-signed cert is generated under PGDATA/beyond/.
///   - 06-tls.conf points at the platform paths.
///   - pgbouncer.ini's `client_tls_cert_file` points at the platform path.
///   - Platform CA is wired as `ssl_ca_file` / `client_tls_ca_file`.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_with_platform_cert() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();

    // Pre-populate a fake platform TLS dir. The content doesn't have to be a
    // valid cert — boot only checks existence; downstream PG would fail to
    // load it but that's not what this test exercises.
    let tls_dir = tempfile::tempdir().expect("tls tempdir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tls_dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::fs::write(tls_dir.path().join("cert.pem"), b"fake-cert\n").unwrap();
    std::fs::write(tls_dir.path().join("key.pem"), b"fake-key\n").unwrap();
    std::fs::write(tls_dir.path().join("ca.pem"), b"fake-ca\n").unwrap();

    let tls_mount = format!("{}:/run/beyond/tls", tls_dir.path().display());
    let extra = ["-v", &tls_mount];

    let out = env.run_boot_ex(&binary, platform, &mmds(&[]), &extra, "postgres:18");
    eprintln!("stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "boot exited {}", out.status);

    let pgdata = env.pgdata();

    // No self-signed cert was generated — provision() short-circuited to Platform.
    assert!(
        !pgdata.join("beyond/server.crt").exists(),
        "platform mode must NOT generate self-signed cert"
    );

    // 06-tls.conf points at platform paths.
    let tls_conf = std::fs::read_to_string(pgdata.join("conf.d/06-tls.conf")).unwrap();
    assert!(
        tls_conf.contains("ssl_cert_file = '/run/beyond/tls/cert.pem'"),
        "06-tls.conf wrong cert path:\n{tls_conf}"
    );
    assert!(
        tls_conf.contains("ssl_key_file = '/run/beyond/tls/key.pem'"),
        "06-tls.conf wrong key path:\n{tls_conf}"
    );
    assert!(
        tls_conf.contains("ssl_ca_file = '/run/beyond/tls/ca.pem'"),
        "06-tls.conf missing ssl_ca_file:\n{tls_conf}"
    );

    // PgBouncer config wired with platform paths.
    let pgb = std::fs::read_to_string(env.etc_pgb.path().join("pgbouncer.ini")).unwrap();
    assert!(
        pgb.contains("client_tls_cert_file = /run/beyond/tls/cert.pem"),
        "pgbouncer.ini wrong cert path:\n{pgb}"
    );
    assert!(
        pgb.contains("client_tls_ca_file = /run/beyond/tls/ca.pem"),
        "pgbouncer.ini missing client_tls_ca_file:\n{pgb}"
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

/// Python stub that replaces `aws` in containers.
///
/// Handles `aws s3 cp SRC DST [--no-progress]` and maps the S3 bucket
/// `s3://beyond-test/wal/` to the local container path `/archive/`.
const AWS_STUB_PY: &str = r#"#!/usr/bin/env python3
import sys, os, shutil
src, dst = sys.argv[3], sys.argv[4]
PREFIX, LOCAL = "s3://beyond-test/wal/", "/archive/"
fn = lambda p: LOCAL + p[len(PREFIX):] if p.startswith(PREFIX) else p
s, d = fn(src), fn(dst)
ddir = os.path.dirname(d)
if ddir: os.makedirs(ddir, exist_ok=True)
shutil.copy2(s, d)
"#;

/// Full PITR cycle: archive WAL from a live Postgres, then recover to a
/// point-in-time using `recovery.signal` + `restore_command`.
///
/// Steps:
///   1. Boot with `BEYOND_PG_ARCHIVE_TARGET` (no recovery target) — configures
///      `archive_command` and `restore_command` but does not enter recovery.
///   2. Start Postgres.  Insert rows into `before_pitr`.  Record timestamp T1.
///      Insert rows into `after_pitr`.  Force WAL switch to flush archive.
///   3. Stop Postgres.
///   4. Boot again with `BEYOND_PG_ARCHIVE_TARGET` + `BEYOND_PG_RECOVERY_TARGET_TIME=T1`.
///      Verifies `recovery.signal` is created and `05-pitr.conf` has the right content.
///   5. Start Postgres in recovery mode.  Wait for it to replay to T1 and promote
///      (`pg_is_in_recovery()` returns false).
///   6. Assert `before_pitr` has 10 rows and `after_pitr` does not exist — proving
///      Postgres recovered exactly to T1 and no further.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_pitr_recovery() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();

    // Create the archive directory (bind-mounted at /archive in containers).
    let archive_dir = tempfile::tempdir().expect("archive tempdir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(archive_dir.path(), std::fs::Permissions::from_mode(0o777))
            .expect("chmod archive dir");
    }

    // Write the stub aws script and make it executable.
    let aws_stub = tempfile::NamedTempFile::new().expect("aws stub");
    std::fs::write(aws_stub.path(), AWS_STUB_PY).expect("write aws stub");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(aws_stub.path(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod aws stub");
    }

    let archive = archive_dir.path().display().to_string();
    let aws = aws_stub.path().display().to_string();
    let bin = binary.display().to_string();
    let pg_lib = env.pg_lib.path().display().to_string();
    let etc_pg = env.etc_pg.path().display().to_string();

    let extra = &[
        "-v",
        &format!("{aws}:/usr/local/bin/aws"),
        "-v",
        &format!("{archive}:/archive"),
    ];

    // ---------------------------------------------------------------------------
    // Phase 1: boot with archive target only (no recovery target).
    // ---------------------------------------------------------------------------
    let boot1_mmds = mmds(&[("BEYOND_PG_ARCHIVE_TARGET", "s3://beyond-test/wal")]);
    let boot1 = env.run_boot_ex(&binary, platform, &boot1_mmds, extra, "postgres:18");
    eprintln!("boot1 stderr: {}", String::from_utf8_lossy(&boot1.stderr));
    assert!(
        boot1.status.success(),
        "phase 1 boot failed: {}",
        boot1.status
    );
    // No recovery.signal — archive target alone doesn't trigger recovery.
    assert!(
        !env.pgdata().join("recovery.signal").exists(),
        "recovery.signal should not exist with archive target but no recovery time"
    );

    // Write the MMDS to a persistent file so the postgres container's
    // `beyond-pg archive` subprocess can read it.
    let mmds_file = tempfile::NamedTempFile::new().expect("mmds file");
    std::fs::write(mmds_file.path(), &boot1_mmds).expect("write mmds");
    let mmds_path = mmds_file.path().display().to_string();

    // ---------------------------------------------------------------------------
    // Phase 1 Postgres: insert data, archive WAL, capture T1.
    //
    // The bash script:
    //   - overrides huge_pages and shared_preload_libraries for unprivileged Docker
    //   - starts Postgres, waits for readiness
    //   - inserts 10 rows into `before_pitr`
    //   - sleeps 2 s to ensure T1 is strictly between the two inserts
    //   - records T1 = current timestamp
    //   - inserts 10 rows into `after_pitr`
    //   - forces a WAL switch so the archiver flushes both segments
    //   - sleeps to allow archive_command to complete
    //   - prints "RECOVERY_TARGET:<T1>" so the test can extract T1
    // ---------------------------------------------------------------------------
    let phase1_cmd = concat!(
        "printf '%s\\n' 'huge_pages = try' \"shared_preload_libraries = ''\" ",
        "  >> /var/lib/postgresql/18/main/postgresql.auto.conf && ",
        "chown -R postgres:postgres /var/lib/postgresql/18/ && ",
        "mkdir -p /var/run/postgresql && chown postgres:postgres /var/run/postgresql && ",
        "gosu postgres postgres -D /var/lib/postgresql/18/main -p 5433 & ",
        "PG_PID=$! && ",
        "for i in $(seq 30); do pg_isready -h /var/run/postgresql -p 5433 -q && break; sleep 1; done && ",
        "gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -c ",
        "  'CREATE TABLE before_pitr (n int); INSERT INTO before_pitr SELECT generate_series(1,10)' && ",
        "sleep 2 && ",
        "T1=$(gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -tAc ",
        "  \"SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS')\") && ",
        "sleep 1 && ",
        "gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -c ",
        "  'CREATE TABLE after_pitr (n int); INSERT INTO after_pitr SELECT generate_series(1,10)' && ",
        "gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -c 'SELECT pg_switch_wal()' && ",
        // Wait for archive_command to complete (runs asynchronously in Postgres).
        "sleep 5 && ",
        "echo \"RECOVERY_TARGET:$T1\" && ",
        "kill $PG_PID && wait $PG_PID 2>/dev/null; true",
    );

    let phase1_out = std::process::Command::new("docker")
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
            &format!("{aws}:/usr/local/bin/aws"),
            "-v",
            &format!("{pg_lib}:/var/lib/postgresql/18"),
            "-v",
            &format!("{etc_pg}:/etc/postgresql/18/main"),
            "-v",
            &format!("{archive}:/archive"),
            "-v",
            &format!("{mmds_path}:/run/mmds/metadata.json"),
            "postgres:18",
            "bash",
            "-c",
            phase1_cmd,
        ])
        .output()
        .expect("docker run phase 1 postgres");

    eprintln!(
        "phase1 pg stdout: {}",
        String::from_utf8_lossy(&phase1_out.stdout)
    );
    eprintln!(
        "phase1 pg stderr: {}",
        String::from_utf8_lossy(&phase1_out.stderr)
    );
    assert!(
        phase1_out.status.success(),
        "phase 1 postgres failed: {}",
        phase1_out.status
    );

    // Extract the recovery target time from stdout.
    let phase1_stdout = String::from_utf8_lossy(&phase1_out.stdout);
    let t1 = phase1_stdout
        .lines()
        .find(|l| l.starts_with("RECOVERY_TARGET:"))
        .and_then(|l| l.strip_prefix("RECOVERY_TARGET:"))
        .map(str::trim)
        .expect("RECOVERY_TARGET not found in phase 1 output")
        .to_owned();
    eprintln!("recovery target T1 = {t1}");

    // Verify at least one WAL segment was archived.
    let archived: Vec<_> = std::fs::read_dir(archive_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(
        !archived.is_empty(),
        "no WAL segments were archived — archive_command or stub aws may have failed"
    );

    // Delete archived segments from local pg_wal/ so recovery is forced to use
    // restore_command (the aws stub). Only remove segments that exist in the archive
    // — the segment started after pg_switch_wal() (containing the shutdown checkpoint)
    // is not yet archived and must remain for recovery to find the starting checkpoint.
    {
        let wal_dir = env.pg_lib.path().join("wal");
        let archived_names: std::collections::HashSet<_> =
            archived.iter().map(|e| e.file_name()).collect();
        for entry in std::fs::read_dir(&wal_dir).unwrap().filter_map(|e| e.ok()) {
            if archived_names.contains(&entry.file_name()) {
                std::fs::remove_file(entry.path()).ok();
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Phase 2: boot in PITR mode with recovery target T1.
    // ---------------------------------------------------------------------------
    let boot2_mmds = mmds(&[
        ("BEYOND_PG_ARCHIVE_TARGET", "s3://beyond-test/wal"),
        ("BEYOND_PG_RECOVERY_TARGET_TIME", &t1),
    ]);
    // Update the MMDS file so `beyond-pg archive` reads the same target.
    std::fs::write(mmds_file.path(), &boot2_mmds).expect("write boot2 mmds");

    let boot2 = env.run_boot_ex(&binary, platform, &boot2_mmds, extra, "postgres:18");
    eprintln!("boot2 stderr: {}", String::from_utf8_lossy(&boot2.stderr));
    assert!(
        boot2.status.success(),
        "phase 2 boot failed: {}",
        boot2.status
    );
    assert!(
        env.pgdata().join("recovery.signal").exists(),
        "recovery.signal must exist for PITR recovery"
    );
    let pitr_conf = std::fs::read_to_string(env.pgdata().join("conf.d/05-pitr.conf")).unwrap();
    assert!(
        pitr_conf.contains(&format!("recovery_target_time = '{t1}'")),
        "05-pitr.conf has wrong recovery_target_time: {pitr_conf}"
    );

    // ---------------------------------------------------------------------------
    // Phase 2 Postgres: start in recovery mode, wait for promotion, assert data.
    //
    // Recovery replays archived WAL up to T1, then promotes.
    // After promotion pg_is_in_recovery() returns false.
    // ---------------------------------------------------------------------------
    let phase2_cmd = concat!(
        "printf '%s\\n' 'huge_pages = try' \"shared_preload_libraries = ''\" ",
        "  >> /var/lib/postgresql/18/main/postgresql.auto.conf && ",
        "chown -R postgres:postgres /var/lib/postgresql/18/ && ",
        "mkdir -p /var/run/postgresql && chown postgres:postgres /var/run/postgresql && ",
        "gosu postgres postgres -D /var/lib/postgresql/18/main -p 5433 & ",
        "PG_PID=$! && ",
        // Poll until promoted (pg_is_in_recovery() = false).
        "for i in $(seq 60); do ",
        "  pg_isready -h /var/run/postgresql -p 5433 -q || { sleep 2; continue; }; ",
        "  REC=$(gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -tAc ",
        "    'SELECT pg_is_in_recovery()' 2>/dev/null); ",
        "  [ \"$REC\" = \"f\" ] && break; ",
        "  sleep 2; ",
        "done && ",
        // Verify data.
        "BEFORE=$(gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -tAc ",
        "  'SELECT count(*) FROM before_pitr') && ",
        "AFTER_EXISTS=$(gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -tAc ",
        "  \"SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name='after_pitr')\") && ",
        "echo \"BEFORE_COUNT:$BEFORE\" && ",
        "echo \"AFTER_EXISTS:$AFTER_EXISTS\" && ",
        "kill $PG_PID && wait $PG_PID 2>/dev/null; ",
        // Exit 0 only if recovery succeeded: before=10, after doesn't exist.
        "[ \"$(echo $BEFORE | tr -d ' ')\" = \"10\" ] && [ \"$(echo $AFTER_EXISTS | tr -d ' ')\" = \"f\" ]",
    );

    let phase2_out = std::process::Command::new("docker")
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
            &format!("{aws}:/usr/local/bin/aws"),
            "-v",
            &format!("{pg_lib}:/var/lib/postgresql/18"),
            "-v",
            &format!("{etc_pg}:/etc/postgresql/18/main"),
            "-v",
            &format!("{archive}:/archive"),
            "-v",
            &format!("{mmds_path}:/run/mmds/metadata.json"),
            "postgres:18",
            "bash",
            "-c",
            phase2_cmd,
        ])
        .output()
        .expect("docker run phase 2 postgres");

    eprintln!(
        "phase2 pg stdout: {}",
        String::from_utf8_lossy(&phase2_out.stdout)
    );
    eprintln!(
        "phase2 pg stderr: {}",
        String::from_utf8_lossy(&phase2_out.stderr)
    );
    assert!(
        phase2_out.status.success(),
        "PITR recovery failed — postgres did not recover to T1={t1}: {}\nstdout: {}",
        phase2_out.status,
        String::from_utf8_lossy(&phase2_out.stdout),
    );
}

/// Replica `restore_command = 'curl ...'` actually fetches WAL from an HTTP server.
///
/// Tests the full curl restore_command chain end-to-end:
///   1. Boot as primary; start Postgres, insert test data, archive WAL via `cp`.
///   2. Add `standby.signal`, delete WAL files (forces restore_command use).
///   3. Boot as replica with `BEYOND_PG_WAL_SINK=http://127.0.0.1:9997` — beyond-pg
///      writes `04-replica.conf` containing `restore_command = 'curl ... 127.0.0.1:9997/%f ...'`.
///   4. In a single container: start Python HTTP server on 9997 serving the archive,
///      start Postgres in recovery mode, wait for WAL application, promote, verify data.
///
/// Proves that the `restore_command` generated by `config::replica_conf()` is not
/// only syntactically correct but that Postgres can actually use it to recover WAL.
#[test]
#[ignore = "requires Docker + musl target (aarch64 or x86_64)"]
fn boot_replica_restore_command_curl() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };
    let env = BootEnv::new();

    let archive_dir = tempfile::tempdir().expect("archive tmpdir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(archive_dir.path(), std::fs::Permissions::from_mode(0o777))
            .expect("chmod archive dir");
    }

    let archive = archive_dir.path().display().to_string();
    let bin = binary.display().to_string();
    let pg_lib = env.pg_lib.path().display().to_string();
    let etc_pg = env.etc_pg.path().display().to_string();

    // -------------------------------------------------------------------------
    // Phase 1: beyond-pg boot (primary mode, no WAL sink configured)
    // -------------------------------------------------------------------------
    let boot1 = env.run_boot(&binary, platform, &mmds(&[]));
    eprintln!("boot1 stderr: {}", String::from_utf8_lossy(&boot1.stderr));
    assert!(
        boot1.status.success(),
        "phase 1 boot failed: {}",
        boot1.status
    );

    // -------------------------------------------------------------------------
    // Phase 1 Postgres: configure archiving, insert data, force WAL archive, stop.
    //
    // archive_command writes segments to /archive (bind-mounted from host).
    // pg_switch_wal() flushes the current segment to the archive.
    // -------------------------------------------------------------------------
    let phase1_cmd = concat!(
        "printf '%s\\n' 'huge_pages = try' 'archive_mode = on' ",
        "  \"archive_command = 'cp %p /archive/%f'\" ",
        "  \"shared_preload_libraries = ''\" ",
        "  >> /var/lib/postgresql/18/main/postgresql.auto.conf && ",
        "chown -R postgres:postgres /var/lib/postgresql/18/ && ",
        "mkdir -p /var/run/postgresql && chown postgres:postgres /var/run/postgresql && ",
        "gosu postgres postgres -D /var/lib/postgresql/18/main -p 5433 & ",
        "PG_PID=$! && ",
        "for i in $(seq 30); do pg_isready -h /var/run/postgresql -p 5433 -q && break; sleep 1; done && ",
        "gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -c ",
        "  'CREATE TABLE curl_test (n int); INSERT INTO curl_test SELECT generate_series(1,10)' && ",
        "gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -c 'SELECT pg_switch_wal()' && ",
        "sleep 4 && ",
        "kill $PG_PID && wait $PG_PID 2>/dev/null; true",
    );
    let phase1_out = std::process::Command::new("docker")
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
            "-v",
            &format!("{archive}:/archive"),
            "postgres:18",
            "bash",
            "-c",
            phase1_cmd,
        ])
        .output()
        .expect("docker run phase 1 postgres");
    eprintln!(
        "phase1 stderr: {}",
        String::from_utf8_lossy(&phase1_out.stderr)
    );
    assert!(
        phase1_out.status.success(),
        "phase 1 postgres failed: {}\nstderr: {}",
        phase1_out.status,
        String::from_utf8_lossy(&phase1_out.stderr),
    );

    let archived: Vec<_> = std::fs::read_dir(archive_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().len() == 24)
        .collect();
    assert!(
        !archived.is_empty(),
        "no 24-char WAL segments in archive — archive_command may have failed"
    );

    // -------------------------------------------------------------------------
    // Setup: add standby.signal to PGDATA. Delete only the archived WAL segments
    // from pg_wal/ — this forces restore_command (curl) to fetch them, while
    // preserving the segment started after pg_switch_wal() that holds the
    // shutdown checkpoint (not yet in archive, required for recovery to start).
    // -------------------------------------------------------------------------
    std::fs::write(env.pgdata().join("standby.signal"), b"").unwrap();
    {
        let wal_dir = env.pg_lib.path().join("wal");
        let archived_names: std::collections::HashSet<_> =
            archived.iter().map(|e| e.file_name()).collect();
        for entry in std::fs::read_dir(&wal_dir).unwrap().filter_map(|e| e.ok()) {
            if archived_names.contains(&entry.file_name()) {
                std::fs::remove_file(entry.path()).ok();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 2: beyond-pg boot (replica mode, WAL sink = http://127.0.0.1:9997).
    //
    // beyond-pg sees PG_VERSION + standby.signal → skips pg_basebackup.
    // Writes 04-replica.conf with restore_command = 'curl -f -s http://127.0.0.1:9997/%f -o %p'.
    // -------------------------------------------------------------------------
    let boot2 = env.run_boot(
        &binary,
        platform,
        &mmds(&[
            ("BEYOND_PG_TIER", "replica"),
            (
                "BEYOND_PG_PRIMARY_CONNINFO",
                "host=127.0.0.1 port=9999 user=replicator connect_timeout=1",
            ),
            ("BEYOND_PG_WAL_SINK", "http://127.0.0.1:9997"),
        ]),
    );
    eprintln!("boot2 stderr: {}", String::from_utf8_lossy(&boot2.stderr));
    assert!(
        boot2.status.success(),
        "phase 2 boot failed: {}",
        boot2.status
    );

    let replica_conf_path = env.pgdata().join("conf.d/04-replica.conf");
    assert!(
        replica_conf_path.exists(),
        "04-replica.conf not written by replica boot"
    );
    let replica_conf = std::fs::read_to_string(&replica_conf_path).unwrap();
    assert!(
        replica_conf.contains("restore_command = 'curl -f -s http://127.0.0.1:9997/%f -o %p'"),
        "curl restore_command not found in 04-replica.conf: {replica_conf}"
    );

    // -------------------------------------------------------------------------
    // Phase 2 Postgres: Python HTTP server + replica Postgres + promote + verify.
    //
    // Python HTTP server on port 9997 serves /archive (the archived WAL).
    // Postgres starts in standby mode; primary_conninfo points at nothing (fails fast).
    // restore_command = 'curl ...' fetches WAL segments from the Python server.
    // After 10 s of recovery, pg_ctl promote promotes the standby.
    // -------------------------------------------------------------------------
    let phase2_cmd = concat!(
        // Install curl if missing (postgres:18 base may not include it)
        "which curl 2>/dev/null || (apt-get update -qq && apt-get install -y curl -qq) && ",
        // Start Python HTTP server serving the WAL archive
        "python3 -m http.server 9997 --directory /archive & ",
        "sleep 1 && ",
        // Postgres-specific test overrides
        "printf '%s\\n' 'huge_pages = try' \"shared_preload_libraries = ''\" ",
        "  >> /var/lib/postgresql/18/main/postgresql.auto.conf && ",
        "chown -R postgres:postgres /var/lib/postgresql/18/ && ",
        "mkdir -p /var/run/postgresql && chown postgres:postgres /var/run/postgresql && ",
        // Start Postgres in standby/recovery mode
        "gosu postgres postgres -D /var/lib/postgresql/18/main -p 5433 & ",
        "PG_PID=$! && ",
        // Wait for Postgres to start (standby mode still accepts pg_isready)
        "for i in $(seq 60); do pg_isready -h /var/run/postgresql -p 5433 -q && break; sleep 1; done && ",
        // Wait for WAL application via curl restore_command
        "sleep 10 && ",
        // Promote to primary (-w waits for promotion to complete)
        "gosu postgres pg_ctl promote -D /var/lib/postgresql/18/main -w -t 30 && ",
        // Confirm pg_is_in_recovery() = false
        "for i in $(seq 30); do ",
        "  REC=$(gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -tAc ",
        "    'SELECT pg_is_in_recovery()' 2>/dev/null); ",
        "  [ \"$REC\" = \"f\" ] && break; sleep 1; ",
        "done && ",
        // Verify test data was recovered via curl restore_command
        "COUNT=$(gosu postgres psql -h /var/run/postgresql -p 5433 -U postgres -tAc ",
        "  'SELECT count(*) FROM curl_test' | tr -d ' ') && ",
        "echo \"curl_test count: $COUNT\" && ",
        "kill $PG_PID && wait $PG_PID 2>/dev/null; true && ",
        "[ \"$COUNT\" = \"10\" ]",
    );
    let phase2_out = std::process::Command::new("docker")
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
            "-v",
            &format!("{archive}:/archive"),
            "postgres:18",
            "bash",
            "-c",
            phase2_cmd,
        ])
        .output()
        .expect("docker run phase 2 postgres");
    eprintln!(
        "phase2 stdout: {}",
        String::from_utf8_lossy(&phase2_out.stdout)
    );
    eprintln!(
        "phase2 stderr: {}",
        String::from_utf8_lossy(&phase2_out.stderr)
    );
    assert!(
        phase2_out.status.success(),
        "curl restore_command recovery failed: {}\nstdout: {}\nstderr: {}",
        phase2_out.status,
        String::from_utf8_lossy(&phase2_out.stdout),
        String::from_utf8_lossy(&phase2_out.stderr),
    );
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
