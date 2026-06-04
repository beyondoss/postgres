//! Supervisor lifecycle end-to-end tests.
//!
//! Tests the full `beyond-pg supervisor` binary inside a Docker container:
//!
//!   - [`supervisor_lifecycle_primary`]        — boot → ready → post-start → pgbouncer → clean shutdown
//!   - [`supervisor_postgres_restarts_on_crash`] — supervisor restarts postgres after SIGKILL
//!   - [`supervisor_pgbouncer_restarts_on_crash`] — supervisor restarts pgbouncer after SIGKILL
//!   - [`supervisor_pre_start_hook_runs`]      — pre-start hook executes before postgres
//!   - [`supervisor_post_start_hook_runs`]     — post-start hook executes after post-start setup
//!
//! # Prerequisites
//!   1. musl target: `rustup target add {arch}-unknown-linux-musl`
//!   2. Test image:  `mise run build:test-image`
//!      (`beyond-pg-test:latest` = postgres:18 + pgbouncer + pg_cron + stub extensions)
//!
//! Run: `cargo test --test supervisor -- --ignored --nocapture`

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use serde_json::json;

// Serialize Docker launches to avoid VirtioFS cache-race on macOS Docker Desktop.
static DOCKER: Mutex<()> = Mutex::new(());

const IMAGE: &str = "beyond-pg-test:latest";

// ---------------------------------------------------------------------------
// Build helper
// ---------------------------------------------------------------------------

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
        eprintln!("skipping: musl build failed");
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

fn docker_image_exists(name: &str) -> bool {
    std::process::Command::new("docker")
        .args(["image", "inspect", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
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
// BootEnv — bind-mount directories
// ---------------------------------------------------------------------------

/// Host tempdirs bind-mounted into the supervisor container at the paths the
/// binary hardcodes.  All dirs are chmod 777 so the container's postgres user
/// (uid 999) can write to them without the container running as root.
struct BootEnv {
    /// → /var/lib/postgresql/18   (main/ = PGDATA, wal/ = WAL)
    pg_lib: tempfile::TempDir,
    /// → /etc/postgresql/18/main  (pg_hba.conf)
    etc_pg: tempfile::TempDir,
    /// → /etc/pgbouncer
    etc_pgb: tempfile::TempDir,
    /// → /etc/sysctl.d
    etc_sysctl: tempfile::TempDir,
    /// → /etc/postgresql/18/hooks  (pre-start.d, post-start.d)
    hooks: tempfile::TempDir,
}

impl BootEnv {
    fn new() -> Self {
        let env = Self {
            pg_lib: tempfile::tempdir().expect("pg_lib tempdir"),
            etc_pg: tempfile::tempdir().expect("etc_pg tempdir"),
            etc_pgb: tempfile::tempdir().expect("etc_pgb tempdir"),
            etc_sysctl: tempfile::tempdir().expect("etc_sysctl tempdir"),
            hooks: tempfile::tempdir().expect("hooks tempdir"),
        };

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for dir in [
                &env.pg_lib,
                &env.etc_pg,
                &env.etc_pgb,
                &env.etc_sysctl,
                &env.hooks,
            ] {
                std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777))
                    .expect("chmod 777 tempdir");
            }
        }

        // Create hook subdirectories the supervisor expects.
        let pre_start = env.hooks.path().join("pre-start.d");
        let post_start = env.hooks.path().join("post-start.d");
        std::fs::create_dir_all(&pre_start).expect("create pre-start.d");
        std::fs::create_dir_all(&post_start).expect("create post-start.d");

        // Production config has huge_pages=on; unprivileged Docker cannot reserve
        // hugepages.  This hook overrides to 'try' so Postgres starts successfully.
        // The production value is verified separately in the boot tests.
        env.write_hook(
            &pre_start,
            "00-test-hugepages.sh",
            "#!/bin/sh\necho huge_pages = try >> /var/lib/postgresql/18/main/postgresql.auto.conf\n",
        );

        env
    }

    fn pre_start_dir(&self) -> PathBuf {
        self.hooks.path().join("pre-start.d")
    }

    fn post_start_dir(&self) -> PathBuf {
        self.hooks.path().join("post-start.d")
    }

    /// Write an executable hook script to `dir`.
    fn write_hook(&self, dir: &Path, name: &str, script: &str) {
        #[cfg(unix)]
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, script).expect("write hook script");
        #[cfg(unix)]
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod hook script");
    }
}

// ---------------------------------------------------------------------------
// RunningContainer — RAII Docker container handle
// ---------------------------------------------------------------------------

/// Handle to a running supervisor container.  Removes the container on drop.
struct RunningContainer {
    id: String,
    /// Keeps the MMDS tempfile alive while the container is running.
    /// The supervisor reads /run/mmds/metadata.json once at startup; this file
    /// is bind-mounted from the host and must not be deleted before that read.
    _mmds: tempfile::NamedTempFile,
}

impl RunningContainer {
    fn exec(&self, args: &[&str]) -> std::process::Output {
        let mut cmd = vec!["exec", &self.id];
        cmd.extend_from_slice(args);
        std::process::Command::new("docker")
            .args(&cmd)
            .output()
            .expect("docker exec")
    }

    fn exec_ok(&self, args: &[&str]) -> bool {
        self.exec(args).status.success()
    }

    /// Poll `cmd` every second until it exits 0 or `timeout` elapses.
    fn wait_for(&self, cmd: &[&str], timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.exec_ok(cmd) {
                return true;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        false
    }

    fn wait_postgres_ready(&self, timeout: Duration) -> bool {
        self.wait_for(
            &[
                "pg_isready",
                "-h",
                "/var/run/postgresql",
                "-p",
                "5433",
                "-q",
            ],
            timeout,
        )
    }

    fn wait_pgbouncer_up(&self, timeout: Duration) -> bool {
        self.wait_for(&["pgrep", "-x", "pgbouncer"], timeout)
    }

    /// Number of live pgbouncer processes (worker 0 + scaled/warm-started extras).
    fn pgbouncer_count(&self) -> u32 {
        let out = self.exec(&["pgrep", "-c", "-x", "pgbouncer"]);
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .unwrap_or(0)
    }

    /// Poll until exactly `target` pgbouncer processes are live (or timeout).
    fn wait_pgbouncer_count(&self, target: u32, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.pgbouncer_count() == target {
                return true;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        false
    }

    /// `docker restart` — SIGTERM the supervisor, then re-run the entrypoint
    /// (a fresh cold start in the SAME container, so the writable layer —
    /// children.json, PGDATA — persists, exactly like a real reboot).
    fn restart(&self, timeout: Duration) {
        let secs = timeout.as_secs().to_string();
        let out = std::process::Command::new("docker")
            .args(["restart", "-t", &secs, &self.id])
            .output()
            .expect("docker restart");
        assert!(
            out.status.success(),
            "docker restart failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Poll until the pgbouncer role exists — proxy for post_start completion.
    fn wait_post_start_done(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            let out = self.exec(&[
                "gosu",
                "postgres",
                "psql",
                "-h",
                "/var/run/postgresql",
                "-p",
                "5433",
                "-tAc",
                "SELECT 1 FROM pg_roles WHERE rolname='pgbouncer'",
            ]);
            if out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "1" {
                return true;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        false
    }

    fn logs(&self) -> String {
        let out = std::process::Command::new("docker")
            .args(["logs", &self.id])
            .output()
            .expect("docker logs");
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        )
    }

    /// Poll until the container has stopped.  Returns true if it stopped within
    /// `timeout`; false if still running when the timeout elapses.
    fn wait_exit(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            let out = std::process::Command::new("docker")
                .args(["inspect", "-f", "{{.State.Running}}", &self.id])
                .output();
            if let Ok(o) = out {
                if o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "false" {
                    return true;
                }
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        false
    }

    /// Send SIGTERM via `docker stop` and return the container's exit code.
    fn stop(&self, timeout: Duration) -> i32 {
        let secs = timeout.as_secs().to_string();
        let _ = std::process::Command::new("docker")
            .args(["stop", "-t", &secs, &self.id])
            .output();
        let wait = std::process::Command::new("docker")
            .args(["wait", &self.id])
            .output()
            .expect("docker wait");
        std::str::from_utf8(&wait.stdout)
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok())
            .unwrap_or(-1)
    }
}

impl Drop for RunningContainer {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.id])
            .status();
    }
}

// ---------------------------------------------------------------------------
// SupervisorHarness
// ---------------------------------------------------------------------------

/// Owns the musl binary, platform string, and bind-mount environment.
/// Call [`SupervisorHarness::start`] to launch a container and get a
/// [`RunningContainer`] handle.
struct SupervisorHarness {
    binary: PathBuf,
    platform: &'static str,
    env: BootEnv,
}

impl SupervisorHarness {
    /// Returns `None` (test skips) if Docker is unavailable, the musl target
    /// is not installed, or the test image has not been built.
    fn new() -> Option<Self> {
        let (binary, platform) = build_linux_beyond_pg()?;
        if !docker_image_exists(IMAGE) {
            eprintln!("skipping: {IMAGE} not found — run `mise run build:test-image` first");
            return None;
        }
        Some(Self {
            binary,
            platform,
            env: BootEnv::new(),
        })
    }

    /// Start the supervisor in a detached container and return a RAII handle.
    ///
    /// `extra_volumes` is a list of `(host_path, container_path)` pairs for
    /// additional bind mounts (e.g. stub binaries for CDC tests).
    fn start(&self, mmds_fields: &[(&str, &str)]) -> RunningContainer {
        self.start_extra(mmds_fields, &[])
    }

    /// Like [`start`] but with extra volumes AND extra `docker run` flags
    /// (e.g. `["--add-host", "host.docker.internal:host-gateway"]`).
    fn start_extra_args(
        &self,
        mmds_fields: &[(&str, &str)],
        extra_volumes: &[(&str, &str)],
        extra_docker_args: &[&str],
    ) -> RunningContainer {
        // Inject extra_docker_args just before the image name.
        // We abuse start_extra's arg-building by not having it — instead
        // delegate through a closure that patches the args vec.
        //
        // Simplest: re-implement the relevant portion inline here.
        let mmds_file = tempfile::NamedTempFile::new().expect("mmds tempfile");
        std::fs::write(mmds_file.path(), mmds(mmds_fields)).expect("write mmds");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(mmds_file.path(), std::fs::Permissions::from_mode(0o644))
                .expect("chmod mmds file");
        }
        let pg_lib = self.env.pg_lib.path().display().to_string();
        let etc_pg = self.env.etc_pg.path().display().to_string();
        let etc_pgb = self.env.etc_pgb.path().display().to_string();
        let etc_sysctl = self.env.etc_sysctl.path().display().to_string();
        let hooks = self.env.hooks.path().display().to_string();
        let mmds_path = mmds_file.path().display().to_string();
        let bin = self.binary.display().to_string();

        const BASH_CMD2: &str = concat!(
            "printf '%s\\n' '#!/bin/sh' ",
            "'for a in \"$@\"; do case \"$a\" in --pwfile=*) chmod 0644 \"${a#--pwfile=}\" 2>/dev/null;; esac; done' ",
            "'chown postgres:postgres /var/lib/postgresql/18/main 2>/dev/null || true' ",
            "'exec gosu postgres /usr/lib/postgresql/18/bin/initdb \"$@\"' ",
            "> /usr/local/bin/initdb && ",
            "chmod +x /usr/local/bin/initdb; ",
            "trap 'kill \"$SVPID\" 2>/dev/null; wait \"$SVPID\"; exit $?' TERM INT; ",
            "/usr/local/bin/beyond-pg supervisor & ",
            "SVPID=$!; ",
            "wait \"$SVPID\"",
        );

        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--platform".into(),
            self.platform.into(),
            "-v".into(),
            format!("{bin}:/usr/local/bin/beyond-pg"),
            "-v".into(),
            format!("{mmds_path}:/run/mmds/metadata.json"),
            "-v".into(),
            format!("{pg_lib}:/var/lib/postgresql/18"),
            "-v".into(),
            format!("{etc_pg}:/etc/postgresql/18/main"),
            "-v".into(),
            format!("{etc_pgb}:/etc/pgbouncer"),
            "-v".into(),
            format!("{etc_sysctl}:/etc/sysctl.d"),
            "-v".into(),
            format!("{hooks}:/etc/postgresql/18/hooks"),
        ];
        for (host, container) in extra_volumes {
            args.push("-v".into());
            args.push(format!("{host}:{container}"));
        }
        for arg in extra_docker_args {
            args.push(arg.to_string());
        }
        args.extend([IMAGE.into(), "bash".into(), "-c".into(), BASH_CMD2.into()]);

        let out = std::process::Command::new("docker")
            .args(&args)
            .output()
            .expect("docker run -d supervisor");
        assert!(
            out.status.success(),
            "failed to start supervisor container: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        RunningContainer {
            id: String::from_utf8_lossy(&out.stdout).trim().to_string(),
            _mmds: mmds_file,
        }
    }

    /// Like [`start`] but with additional bind-mount volumes.
    ///
    /// # Container design
    ///
    /// bash runs as PID 1 (not the supervisor).  bash:
    ///   1. Installs an `initdb` wrapper that drops from root to the postgres user
    ///      (the postgres binary refuses to execute as root).
    ///   2. Sets a SIGTERM trap that forwards the signal to the supervisor and
    ///      waits for it to exit — this is what makes `docker stop` produce a
    ///      clean exit rather than a SIGKILL after the 30s timeout.
    ///   3. Launches the supervisor in the background and waits for it.
    ///
    /// MMDS is provided via a bind-mounted file rather than embedded JSON so
    /// that passwords or URLs with shell-special characters don't require escaping.
    ///
    /// The hooks directory is bind-mounted so tests can inject scripts before
    /// the container starts.
    fn start_extra(
        &self,
        mmds_fields: &[(&str, &str)],
        extra_volumes: &[(&str, &str)],
    ) -> RunningContainer {
        // Write MMDS to a tempfile and chmod 0644 so the container's postgres
        // user (uid 999) can read it.
        let mmds_file = tempfile::NamedTempFile::new().expect("mmds tempfile");
        std::fs::write(mmds_file.path(), mmds(mmds_fields)).expect("write mmds");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(mmds_file.path(), std::fs::Permissions::from_mode(0o644))
                .expect("chmod mmds file");
        }

        let pg_lib = self.env.pg_lib.path().display().to_string();
        let etc_pg = self.env.etc_pg.path().display().to_string();
        let etc_pgb = self.env.etc_pgb.path().display().to_string();
        let etc_sysctl = self.env.etc_sysctl.path().display().to_string();
        let hooks = self.env.hooks.path().display().to_string();
        let mmds = mmds_file.path().display().to_string();
        let bin = self.binary.display().to_string();

        // The initdb wrapper:
        //   - chmod 0644s the password file (run_initdb creates it 0600 as root;
        //     gosu postgres can't read a root-owned 0600 file)
        //   - chown PGDATA to postgres (initdb refuses to run on a root-owned dir)
        //   - exec gosu postgres initdb (drops to postgres user then execs)
        //
        // The trap must be set in bash (PID 1), NOT in a sub-shell, because:
        //   - bash as PID 1 ignores SIGTERM by default
        //   - a trap in PID 1 overrides that default and makes `docker stop` work
        //   - if the trap were inside an `&&`-chained background sub-shell (`cmd &`),
        //     PID 1 would have no trap and SIGTERM would be silently discarded,
        //     causing `docker stop` to wait the full timeout then send SIGKILL
        // The `;` before `trap` breaks the `&&`-chain so the trap runs in PID 1.
        const BASH_CMD: &str = concat!(
            "printf '%s\\n' '#!/bin/sh' ",
            "'for a in \"$@\"; do case \"$a\" in --pwfile=*) chmod 0644 \"${a#--pwfile=}\" 2>/dev/null;; esac; done' ",
            "'chown postgres:postgres /var/lib/postgresql/18/main 2>/dev/null || true' ",
            "'exec gosu postgres /usr/lib/postgresql/18/bin/initdb \"$@\"' ",
            "> /usr/local/bin/initdb && ",
            "chmod +x /usr/local/bin/initdb; ",
            "trap 'kill \"$SVPID\" 2>/dev/null; wait \"$SVPID\"; exit $?' TERM INT; ",
            "/usr/local/bin/beyond-pg supervisor & ",
            "SVPID=$!; ",
            "wait \"$SVPID\"",
        );

        let mut args = vec![
            "run".to_owned(),
            "-d".to_owned(),
            "--platform".to_owned(),
            self.platform.to_owned(),
            "-v".to_owned(),
            format!("{bin}:/usr/local/bin/beyond-pg"),
            "-v".to_owned(),
            format!("{mmds}:/run/mmds/metadata.json"),
            "-v".to_owned(),
            format!("{pg_lib}:/var/lib/postgresql/18"),
            "-v".to_owned(),
            format!("{etc_pg}:/etc/postgresql/18/main"),
            "-v".to_owned(),
            format!("{etc_pgb}:/etc/pgbouncer"),
            "-v".to_owned(),
            format!("{etc_sysctl}:/etc/sysctl.d"),
            "-v".to_owned(),
            format!("{hooks}:/etc/postgresql/18/hooks"),
        ];
        for (host, container) in extra_volumes {
            args.push("-v".to_owned());
            args.push(format!("{host}:{container}"));
        }
        args.extend([
            IMAGE.to_owned(),
            "bash".to_owned(),
            "-c".to_owned(),
            BASH_CMD.to_owned(),
        ]);

        let out = std::process::Command::new("docker")
            .args(&args)
            .output()
            .expect("docker run -d supervisor");

        assert!(
            out.status.success(),
            "failed to start supervisor container: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        RunningContainer {
            id: String::from_utf8_lossy(&out.stdout).trim().to_string(),
            _mmds: mmds_file,
        }
    }
}

// ---------------------------------------------------------------------------
// Seed helper — used by replica tests
// ---------------------------------------------------------------------------

/// Pre-seed PGDATA with `initdb + standby.signal` so `pg::basebackup()` takes
/// its idempotent short-circuit path (PG_VERSION + standby.signal present → skip).
fn seed_replica_pgdata(harness: &SupervisorHarness) {
    let pg_lib = harness.env.pg_lib.path().display().to_string();
    let status = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "--platform",
            harness.platform,
            "-v",
            &format!("{pg_lib}:/var/lib/postgresql/18"),
            IMAGE,
            "bash",
            "-c",
            concat!(
                "chown -R postgres:postgres /var/lib/postgresql/18 && ",
                "gosu postgres initdb -D /var/lib/postgresql/18/main ",
                "--waldir=/var/lib/postgresql/18/wal ",
                "--auth-host=trust --auth-local=trust && ",
                "touch /var/lib/postgresql/18/main/standby.signal",
            ),
        ])
        .status()
        .expect("docker run seed replica PGDATA");
    assert!(
        status.success(),
        "failed to pre-seed PGDATA for replica test"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full primary-tier supervisor lifecycle.
///
/// Verifies the complete startup sequence and clean shutdown:
///   boot → initdb → postgres ready → post-start (roles, extensions) →
///   pgbouncer spawned → SIGTERM → clean exit 0.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_lifecycle_primary() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };
    let container = harness.start(&[]);

    let ready = container.wait_postgres_ready(Duration::from_secs(90));
    if !ready {
        eprintln!("logs:\n{}", container.logs());
        panic!("Postgres never became ready");
    }

    let post_start_done = container.wait_post_start_done(Duration::from_secs(30));
    if !post_start_done {
        eprintln!("logs:\n{}", container.logs());
        panic!("post-start did not complete: pgbouncer role not found");
    }

    let pgb_running = container.wait_pgbouncer_up(Duration::from_secs(30));
    if !pgb_running {
        eprintln!("logs:\n{}", container.logs());
        panic!("PgBouncer process not found after post-start");
    }

    let code = container.stop(Duration::from_secs(30));
    eprintln!("supervisor exit code: {code}");
    assert!(
        code == 0 || code == 143,
        "supervisor did not exit cleanly on SIGTERM: exit code {code}\nlogs:\n{}",
        container.logs()
    );
}

/// Warm-start: a busy box that cold-restarts must come back at its pre-restart
/// pooler size, not a single worker — the post-restart reconnect storm is peak
/// handshake churn, the worst moment to be under-provisioned. children.json is
/// durable across reboots (GlideFS-backed rootfs) and records the extra workers;
/// cold start respawns that many.
///
/// CI-only (like the sibling supervisor lifecycle tests): uses the standard
/// unprivileged harness. Assumes a CI runner with >= 8 cores so `read_vcpus` →
/// `pgbouncer_max_workers >= 2` (so a warm-started extra is within the cap).
/// NOTE: do NOT run this with `--privileged` on the shared homelab host — the
/// supervisor's boot reserves host hugepages and writes host sysctls, mutating
/// kernel state shared with production. That's exactly why the harness is
/// unprivileged; the trade-off is this test can only boot in CI's permissive
/// Docker, not on a host with a restrictive seccomp profile.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image (>= 8 cores)"]
fn supervisor_warm_starts_pooler_after_restart() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };
    let container = harness.start(&[]);

    assert!(
        container.wait_postgres_ready(Duration::from_secs(90)),
        "postgres not ready\n{}",
        container.logs()
    );
    assert!(
        container.wait_post_start_done(Duration::from_secs(30)),
        "post_start did not complete\n{}",
        container.logs()
    );
    assert!(
        container.wait_pgbouncer_up(Duration::from_secs(30)),
        "pgbouncer not up\n{}",
        container.logs()
    );
    assert_eq!(
        container.pgbouncer_count(),
        1,
        "expected a single pooler at idle before the restart"
    );

    // Simulate a box that had scaled up to 2 workers before the restart: record an
    // extra worker in the durable children.json the scaler would have persisted.
    let inject = container.exec(&[
        "sh",
        "-c",
        "echo '{\"version\":1,\"pgbouncer-1\":{\"pid\":424242,\"starttime\":1}}' \
         > /var/lib/beyond-pg/state/children.json",
    ]);
    assert!(
        inject.status.success(),
        "failed to inject children.json: {}",
        String::from_utf8_lossy(&inject.stderr)
    );

    // Cold-restart the supervisor; warm-start must restore the extra worker.
    container.restart(Duration::from_secs(30));
    assert!(
        container.wait_postgres_ready(Duration::from_secs(90)),
        "postgres not ready after restart\n{}",
        container.logs()
    );
    assert!(
        container.wait_pgbouncer_count(2, Duration::from_secs(45)),
        "warm-start should restore 2 pooler workers after restart, saw {}\n{}",
        container.pgbouncer_count(),
        container.logs()
    );
}

/// Supervisor restarts postgres after a crash (SIGKILL to the postmaster).
///
/// The backoff on first crash is 100 ms, so the restart is nearly immediate.
/// This exercises the `maybe_restart()` path in the supervision loop.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_postgres_restarts_on_crash() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };
    let container = harness.start(&[]);

    assert!(
        container.wait_postgres_ready(Duration::from_secs(90)),
        "postgres did not become ready initially\nlogs:\n{}",
        container.logs()
    );
    // Wait for post-start so the cluster is fully initialized before we crash it.
    assert!(
        container.wait_post_start_done(Duration::from_secs(30)),
        "post-start did not complete\nlogs:\n{}",
        container.logs()
    );

    // SIGKILL the postmaster.  All backends die with it; the supervisor's
    // ch.wait() future resolves and maybe_restart() is called (100 ms backoff).
    assert!(
        container.exec_ok(&["pkill", "-KILL", "-x", "postgres"]),
        "pkill postgres failed — is postgres running?"
    );

    // Brief pause to let the supervisor detect the exit before we start polling.
    std::thread::sleep(Duration::from_millis(500));

    let restarted = container.wait_postgres_ready(Duration::from_secs(30));
    if !restarted {
        eprintln!("logs:\n{}", container.logs());
        panic!("postgres did not restart after crash");
    }

    let code = container.stop(Duration::from_secs(30));
    assert!(code == 0 || code == 143, "unexpected exit code: {code}");
}

/// Supervisor restarts pgbouncer after a crash (SIGKILL to the pgbouncer process).
///
/// Exercises the `maybe_restart_pgbouncer()` path.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_pgbouncer_restarts_on_crash() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };
    let container = harness.start(&[]);

    assert!(
        container.wait_pgbouncer_up(Duration::from_secs(90)),
        "pgbouncer did not start initially\nlogs:\n{}",
        container.logs()
    );

    assert!(
        container.exec_ok(&["pkill", "-KILL", "-x", "pgbouncer"]),
        "pkill pgbouncer failed — is pgbouncer running?"
    );

    std::thread::sleep(Duration::from_millis(500));

    let restarted = container.wait_pgbouncer_up(Duration::from_secs(30));
    if !restarted {
        eprintln!("logs:\n{}", container.logs());
        panic!("pgbouncer did not restart after crash");
    }

    let code = container.stop(Duration::from_secs(30));
    assert!(code == 0 || code == 143, "unexpected exit code: {code}");
}

/// Pre-start hooks execute before postgres starts.
///
/// A hook is injected into the bind-mounted hooks dir before the container
/// starts.  It writes a sentinel file to the bind-mounted /etc/postgresql/18/main
/// directory so the result is readable from the host without docker exec.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_pre_start_hook_runs() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };

    // Write sentinel to /etc/postgresql/18/main (bind-mounted → etc_pg tempdir).
    harness.env.write_hook(
        &harness.env.pre_start_dir(),
        "99-test-sentinel.sh",
        "#!/bin/sh\ntouch /etc/postgresql/18/main/pre-start-hook-ran\n",
    );

    let container = harness.start(&[]);

    // Pre-start hooks run synchronously in do_boot before postgres is spawned.
    // Postgres being ready is sufficient proof the hooks have run.
    assert!(
        container.wait_postgres_ready(Duration::from_secs(90)),
        "postgres did not become ready\nlogs:\n{}",
        container.logs()
    );

    // Check the sentinel on the host via the bind-mounted directory.
    assert!(
        harness
            .env
            .etc_pg
            .path()
            .join("pre-start-hook-ran")
            .exists(),
        "pre-start hook sentinel missing — hook did not run\nlogs:\n{}",
        container.logs()
    );

    container.stop(Duration::from_secs(30));
}

/// Post-start hooks execute after post-start setup completes.
///
/// A hook is injected into the post-start.d directory.  It writes a sentinel
/// to the bind-mounted /etc/postgresql/18/main directory so the host can
/// verify it ran without docker exec.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_post_start_hook_runs() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };

    harness.env.write_hook(
        &harness.env.post_start_dir(),
        "99-test-sentinel.sh",
        "#!/bin/sh\ntouch /etc/postgresql/18/main/post-start-hook-ran\n",
    );

    let container = harness.start(&[]);

    // Post-start hooks run at the end of post_start(), after extensions and roles
    // are created.  Wait for pgbouncer (spawned after post_start) as the proxy.
    assert!(
        container.wait_pgbouncer_up(Duration::from_secs(90)),
        "pgbouncer did not start\nlogs:\n{}",
        container.logs()
    );

    // Give the hook a moment to complete (it runs just before pgbouncer spawns).
    std::thread::sleep(Duration::from_millis(500));

    assert!(
        harness
            .env
            .etc_pg
            .path()
            .join("post-start-hook-ran")
            .exists(),
        "post-start hook sentinel missing — hook did not run\nlogs:\n{}",
        container.logs()
    );

    container.stop(Duration::from_secs(30));
}

/// Supervisor lifecycle for a replica-tier instance.
///
/// PGDATA is pre-seeded with `initdb + standby.signal` so `basebackup` short-
/// circuits (idempotent skip path).  The primary_conninfo points at a non-existent
/// host so postgres starts in standby mode and retries streaming indefinitely —
/// `pg_isready` still returns true because postgres accepts read-only connections.
///
/// This exercises the replica branch of `run_inner()`:
///   - `post_start` (roles, extensions, replicator slot) is skipped entirely
///   - WAL sink and CDC forwarders are not spawned
///   - pgbouncer still starts for read-only connection pooling
///   - `pg_is_in_recovery()` returns true
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_lifecycle_replica() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };

    seed_replica_pgdata(&harness);

    let container = harness.start(&[
        ("BEYOND_PG_TIER", "replica"),
        // Non-existent primary: postgres starts in standby and keeps retrying.
        // connect_timeout=1 makes each retry fail quickly without blocking startup.
        (
            "BEYOND_PG_PRIMARY_CONNINFO",
            "host=127.0.0.1 port=9999 user=replicator connect_timeout=1",
        ),
    ]);

    assert!(
        container.wait_postgres_ready(Duration::from_secs(90)),
        "standby postgres did not become ready\nlogs:\n{}",
        container.logs()
    );

    // Verify that postgres is running as a standby.
    let out = container.exec(&[
        "gosu",
        "postgres",
        "psql",
        "-h",
        "/var/run/postgresql",
        "-p",
        "5433",
        "-tAc",
        "SELECT pg_is_in_recovery()",
    ]);
    assert!(out.status.success(), "psql failed on standby");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "t",
        "expected pg_is_in_recovery()=true on replica tier"
    );

    // pgbouncer still starts on replicas (read-only connection pooling).
    assert!(
        container.wait_pgbouncer_up(Duration::from_secs(30)),
        "pgbouncer did not start on replica\nlogs:\n{}",
        container.logs()
    );

    let code = container.stop(Duration::from_secs(30));
    assert!(
        code == 0 || code == 143,
        "replica supervisor did not exit cleanly: exit code {code}\nlogs:\n{}",
        container.logs()
    );
}

/// Supervisor restarts the CDC process after a crash (SIGKILL to beyond-pg-cdc).
///
/// A stub script is injected at `/usr/local/bin/beyond-pg-cdc` via a bind-mount.
/// The stub ignores all args and sleeps indefinitely — it just needs to stay up
/// long enough to be killed and then be respawned.  This exercises the
/// `maybe_restart_cdc()` path in the supervision loop.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_cdc_restarts_on_crash() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };

    // Write a stub beyond-pg-cdc that accepts any args and stays alive without
    // exec-ing, so the sh process keeps the script path in /proc/*/cmdline and
    // `pgrep -f beyond-pg-cdc` can find it after the supervisor spawns it.
    let stub = tempfile::NamedTempFile::new().expect("cdc stub tempfile");
    std::fs::write(
        stub.path(),
        "#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile true; do sleep 10 & wait $!; done\n",
    )
    .expect("write cdc stub");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(stub.path(), std::fs::Permissions::from_mode(0o755))
            .expect("chmod cdc stub");
    }

    let stub_path = stub.path().display().to_string();
    let container = harness.start_extra(
        &[("BEYOND_PG_CDC_ENABLED", "true")],
        &[(&stub_path, "/usr/local/bin/beyond-pg-cdc")],
    );

    // Wait for postgres and post-start (CDC starts after postgres is ready).
    assert!(
        container.wait_postgres_ready(Duration::from_secs(90)),
        "postgres did not become ready\nlogs:\n{}",
        container.logs()
    );
    assert!(
        container.wait_post_start_done(Duration::from_secs(30)),
        "post-start did not complete\nlogs:\n{}",
        container.logs()
    );

    // Wait for CDC stub to appear (spawned just after post_start returns).
    assert!(
        container.wait_for(&["pgrep", "-f", "beyond-pg-cdc"], Duration::from_secs(15)),
        "beyond-pg-cdc stub never appeared\nlogs:\n{}",
        container.logs()
    );

    // Kill the stub process. The supervisor detects the exit via child.wait() and
    // calls maybe_restart_cdc() with a 100 ms backoff on the first crash.
    assert!(
        container.exec_ok(&["pkill", "-KILL", "-f", "beyond-pg-cdc"]),
        "pkill beyond-pg-cdc failed"
    );
    std::thread::sleep(Duration::from_millis(500));

    let restarted = container.wait_for(&["pgrep", "-f", "beyond-pg-cdc"], Duration::from_secs(15));
    if !restarted {
        eprintln!("logs:\n{}", container.logs());
        panic!("beyond-pg-cdc did not restart after crash");
    }

    let code = container.stop(Duration::from_secs(30));
    assert!(code == 0 || code == 143, "unexpected exit code: {code}");
}

/// A failing pre-start hook must cause the supervisor to exit nonzero without
/// starting Postgres.
///
/// `do_boot` calls `run_hook_scripts` last; on `HookFailed` it returns `Err`
/// which propagates via `?` in `run_inner` → `std::process::exit(1)`.
/// This verifies that error path end-to-end through the real binary.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_pre_start_hook_failure_exits_nonzero() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };

    // Install a hook that always fails (after the hugepages override at 00-).
    harness.env.write_hook(
        &harness.env.pre_start_dir(),
        "99-fail.sh",
        "#!/bin/sh\nexit 1\n",
    );

    let container = harness.start(&[]);

    // The supervisor runs initdb (~15 s) then pre-start hooks, hits the failure,
    // and exits before spawning Postgres.  Wait up to 90 s for the container to stop.
    assert!(
        container.wait_exit(Duration::from_secs(90)),
        "container did not exit in 90 s — supervisor may have started Postgres despite hook failure\nlogs:\n{}",
        container.logs()
    );

    let code = container.stop(Duration::from_secs(5));
    assert_ne!(
        code,
        0,
        "supervisor should exit nonzero when pre-start hook fails\nlogs:\n{}",
        container.logs()
    );

    // The logs must mention the hook failure.
    let logs = container.logs();
    assert!(
        logs.contains("hook script") || logs.contains("HookFailed") || logs.contains("boot failed"),
        "expected hook failure message in logs\nlogs:\n{logs}"
    );
}

/// Sending SIGINT directly to the supervisor process triggers the same clean
/// drain path as SIGTERM.
///
/// The bash PID-1 wrapper traps SIGTERM/SIGINT and forwards SIGTERM to the
/// supervisor.  This test additionally sends SIGINT directly to the supervisor
/// process via `docker exec kill -INT` so that tokio's `sigint.recv()` path
/// is exercised (rather than the bash-forwarded SIGTERM path exercised by
/// `supervisor_lifecycle_primary`).
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_sigint_clean_shutdown() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };
    let container = harness.start(&[]);

    assert!(
        container.wait_postgres_ready(Duration::from_secs(90)),
        "postgres did not become ready\nlogs:\n{}",
        container.logs()
    );
    assert!(
        container.wait_post_start_done(Duration::from_secs(30)),
        "post-start did not complete\nlogs:\n{}",
        container.logs()
    );

    // Send SIGINT directly to the supervisor process (beyond-pg), bypassing
    // bash.  This exercises the supervisor's own sigint.recv() branch.
    assert!(
        container.exec_ok(&["bash", "-c", "kill -INT $(pgrep -x beyond-pg)"]),
        "failed to send SIGINT to supervisor process"
    );

    // Wait for the container to exit cleanly after the drain.
    assert!(
        container.wait_exit(Duration::from_secs(30)),
        "supervisor did not exit within 30 s after SIGINT\nlogs:\n{}",
        container.logs()
    );

    let code = container.stop(Duration::from_secs(5));
    assert!(
        code == 0 || code == 143,
        "supervisor did not exit cleanly after SIGINT: exit code {code}\nlogs:\n{}",
        container.logs()
    );

    let logs = container.logs();
    assert!(
        logs.contains("received SIGINT"),
        "expected SIGINT log message\nlogs:\n{logs}"
    );
}

// ---------------------------------------------------------------------------
// Docker RAII helpers for multi-container tests
// ---------------------------------------------------------------------------

/// RAII wrapper for a named Docker network.  Calls `docker network rm` on drop.
struct DockerNetwork(String);

impl DockerNetwork {
    fn create(name: &str) -> Self {
        let s = std::process::Command::new("docker")
            .args(["network", "create", name])
            .status()
            .expect("docker network create");
        assert!(s.success(), "docker network create {name} failed");
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

/// RAII wrapper for a detached Docker container.  Calls `docker rm -f` on drop.
struct DetachedContainer(String);

impl DetachedContainer {
    fn start(args: &[&str]) -> Self {
        let out = std::process::Command::new("docker")
            .args(args)
            .output()
            .expect("docker run -d");
        assert!(
            out.status.success(),
            "docker run -d failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Self(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn id(&self) -> &str {
        &self.0
    }

    fn exec_output(&self, args: &[&str]) -> std::process::Output {
        let mut cmd_args = vec!["exec", self.id()];
        cmd_args.extend_from_slice(args);
        std::process::Command::new("docker")
            .args(&cmd_args)
            .output()
            .expect("docker exec")
    }

    fn wait_postgres_ready(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self
                .exec_output(&["pg_isready", "-h", "/var/run/postgresql", "-q"])
                .status
                .success()
            {
                return true;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        false
    }

    /// Returns the container's IP address on `network`.
    fn ip_on(&self, network: &str) -> String {
        let fmt = format!(
            r#"{{{{(index .NetworkSettings.Networks "{}").IPAddress}}}}"#,
            network
        );
        let out = std::process::Command::new("docker")
            .args(["inspect", "-f", &fmt, self.id()])
            .output()
            .expect("docker inspect");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }
}

impl Drop for DetachedContainer {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .status();
    }
}

/// Replica supervisor connects to a live primary and actually streams WAL.
///
/// Unlike `supervisor_lifecycle_replica` (which points at a dead host and only
/// verifies standby mode), this test runs a real vanilla `postgres:18` primary
/// and verifies end-to-end streaming replication:
///   - `pg_basebackup` seeds the replica's PGDATA from a live primary
///   - The replica enters streaming recovery (`pg_is_in_recovery()=true`)
///   - The primary's `pg_stat_replication` shows the replica in `streaming` state
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_replica_streams_from_primary() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };

    // Isolated bridge network — both containers address each other by IP.
    let network_name = format!(
        "beyond-replica-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    );
    let _network = DockerNetwork::create(&network_name);

    // Start a vanilla postgres:18 primary with trust auth.
    // POSTGRES_HOST_AUTH_METHOD=trust sets trust for normal connections; we add
    // the replication rule explicitly below so it's not relying on env-var quirks.
    let primary = DetachedContainer::start(&[
        "run",
        "-d",
        "--platform",
        harness.platform,
        "--network",
        &network_name,
        "-e",
        "POSTGRES_PASSWORD=testpassword",
        "-e",
        "POSTGRES_HOST_AUTH_METHOD=trust",
        "postgres:18",
    ]);

    assert!(
        primary.wait_postgres_ready(Duration::from_secs(60)),
        "primary postgres never became ready"
    );

    // Create the replicator role and add a blanket trust replication rule.
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
        "failed to configure replication on primary: {}",
        String::from_utf8_lossy(&hba_out.stderr)
    );

    // Resolve primary IP on the test network.
    let primary_ip = primary.ip_on(&network_name);
    assert!(!primary_ip.is_empty(), "could not determine primary IP");

    let conninfo = format!("host={primary_ip} port=5432 user=replicator");

    // Start the replica supervisor on the same network.
    // --network gives it access to the primary; pg_basebackup will run against it.
    // The huge_pages pre-start hook is already installed by BootEnv::new().
    let replica = harness.start_extra_args(
        &[
            ("BEYOND_PG_TIER", "replica"),
            ("BEYOND_PG_PRIMARY_CONNINFO", &conninfo),
        ],
        &[],
        &["--network", &network_name],
    );

    // pg_basebackup + postgres startup can take 30-60 s; give generous headroom.
    assert!(
        replica.wait_postgres_ready(Duration::from_secs(120)),
        "replica postgres did not become ready\nlogs:\n{}",
        replica.logs()
    );

    // Replica must be in recovery mode.
    let recovery_out = replica.exec(&[
        "gosu",
        "postgres",
        "psql",
        "-h",
        "/var/run/postgresql",
        "-p",
        "5433",
        "-tAc",
        "SELECT pg_is_in_recovery()",
    ]);
    assert!(recovery_out.status.success(), "psql on replica failed");
    assert_eq!(
        String::from_utf8_lossy(&recovery_out.stdout).trim(),
        "t",
        "expected pg_is_in_recovery()=true on replica tier"
    );

    // Primary must show the replica in streaming state.
    // Poll briefly — the connection may take a moment after postgres starts.
    let streaming = (0..15).any(|_| {
        let out = primary.exec_output(&[
            "psql",
            "-U",
            "postgres",
            "-tAc",
            "SELECT count(*) FROM pg_stat_replication WHERE state='streaming'",
        ]);
        if out.status.success() {
            let count: u32 = String::from_utf8_lossy(&out.stdout)
                .trim()
                .parse()
                .unwrap_or(0);
            if count > 0 {
                return true;
            }
        }
        std::thread::sleep(Duration::from_secs(1));
        false
    });
    assert!(
        streaming,
        "primary pg_stat_replication shows no streaming replica\nlogs:\n{}",
        replica.logs()
    );

    let code = replica.stop(Duration::from_secs(30));
    assert!(
        code == 0 || code == 143,
        "replica supervisor did not exit cleanly: exit code {code}\nlogs:\n{}",
        replica.logs()
    );
}

/// Supervisor starts and forwards WAL when a WAL sink is configured.
///
/// `synchronous_commit = remote_write` + `synchronous_standby_names = 'wal_sink'`
/// means every postgres commit waits for the WAL sink to acknowledge receipt.
/// A non-existent sink makes commits block indefinitely, so this test runs a
/// minimal QUIC stub on the host that accepts connections, reads each
/// length-prefixed WAL frame, and immediately sends back a flush-LSN ACK.
///
/// The container connects to the host via `host.docker.internal`.  On Linux
/// Docker (CI), `--add-host=host.docker.internal:host-gateway` is injected.
/// On macOS Docker Desktop the alias is set automatically.
///
/// This exercises:
///   - `boot::write_wal_sink_conf()` (config written) ← also in boot tests
///   - `wal_forwarder::run()` task is spawned and connects
///   - `post_start` SQL commits successfully (sync-commit ack path)
///   - Clean supervisor shutdown with the forwarder task running
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_starts_with_wal_sink_configured() {
    use std::sync::Arc;

    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());
    let Some(harness) = SupervisorHarness::new() else {
        return;
    };

    // ---------------------------------------------------------------------------
    // Spawn a minimal QUIC sink stub on the host.
    //
    // Wire format (wal_forwarder → sink):  [4-byte BE u32 length][payload]
    // Wire format (sink → wal_forwarder):  [8-byte BE u64 flush_lsn]
    //
    // We ACK every frame immediately with u64::MAX (all WAL confirmed) so that
    // postgres's synchronous_commit requirement is satisfied.
    // ---------------------------------------------------------------------------
    const STUB_PORT: u16 = 19999;

    // Install ring as the rustls crypto provider (same as wal_forwarder does).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert = rcgen::generate_simple_self_signed(vec!["beyond-pg-sink".to_owned()])
        .expect("rcgen self-signed cert");
    let cert_der: rustls::pki_types::CertificateDer = cert.cert.into();
    let key_der = rustls::pki_types::PrivateKeyDer::from(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
    );

    let tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("rustls server config");
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).expect("quic server config"),
    ));

    // Drive the stub in a background thread with its own tokio runtime.
    // quinn::Endpoint::server requires an active runtime, so it is created
    // inside block_on.  A channel signals when the endpoint is bound and ready.
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
    let _stub = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let endpoint = quinn::Endpoint::server(
                    server_config,
                    format!("0.0.0.0:{STUB_PORT}").parse().expect("bind addr"),
                )
                .expect("quinn endpoint");
                let _ = ready_tx.send(());
                while let Some(incoming) = endpoint.accept().await {
                    tokio::spawn(async move {
                        let Ok(conn) = incoming.await else { return };
                        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                            tokio::spawn(async move {
                                loop {
                                    let mut len_buf = [0u8; 4];
                                    if recv.read(&mut len_buf).await.is_err() {
                                        break;
                                    }
                                    let len = u32::from_be_bytes(len_buf) as usize;
                                    let mut body = vec![0u8; len];
                                    if recv.read(&mut body).await.is_err() {
                                        break;
                                    }
                                    // ACK all WAL immediately.
                                    if send.write(&u64::MAX.to_be_bytes()).await.is_err() {
                                        break;
                                    }
                                }
                            });
                        }
                    });
                }
            });
    });
    ready_rx.recv().expect("QUIC stub failed to start");

    // `host.docker.internal` resolves to the host from inside a container.
    // On Linux Docker (CI), supply the mapping via --add-host.
    // On macOS Docker Desktop the alias is already set automatically; the flag
    // is a harmless no-op there.
    let sink_url = format!("http://host.docker.internal:{STUB_PORT}");
    let container = harness.start_extra_args(
        &[("BEYOND_PG_WAL_SINK", &sink_url)],
        &[],
        &["--add-host", "host.docker.internal:host-gateway"],
    );

    assert!(
        container.wait_postgres_ready(Duration::from_secs(90)),
        "postgres did not become ready with WAL sink configured\nlogs:\n{}",
        container.logs()
    );
    assert!(
        container.wait_post_start_done(Duration::from_secs(30)),
        "post-start did not complete (sync-commit may be blocked)\nlogs:\n{}",
        container.logs()
    );
    assert!(
        container.wait_pgbouncer_up(Duration::from_secs(30)),
        "pgbouncer did not start\nlogs:\n{}",
        container.logs()
    );

    // Verify the forwarder is actually connecting (look for its log line).
    let logs = container.logs();
    assert!(
        logs.contains("wal forwarder: QUIC connected"),
        "wal forwarder did not connect to stub sink\nlogs:\n{logs}"
    );

    let code = container.stop(Duration::from_secs(30));
    assert!(
        code == 0 || code == 143,
        "supervisor did not exit cleanly with WAL sink: exit code {code}\nlogs:\n{logs}"
    );
}
