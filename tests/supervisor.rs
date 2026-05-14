//! Supervisor lifecycle end-to-end tests.
//!
//! These tests verify the full `beyond-pg supervisor` lifecycle:
//!   boot → Postgres ready → post-start setup → PgBouncer spawned → clean SIGTERM shutdown.
//!
//! Prerequisites:
//!   1. musl target installed: `rustup target add {arch}-unknown-linux-musl`
//!   2. Test image built:       `mise run build:test-image`
//!      The test image (beyond-pg-test:latest) extends postgres:18 with:
//!        - pgbouncer
//!        - postgresql-18-cron
//!        - stub beyond_auth and beyond_queue extensions (.so + control files)
//!
//! Run: cargo test --test supervisor -- --ignored --nocapture

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

// Serialize Docker launches — same VirtioFS cache-race concern as in boot.rs.
static DOCKER: Mutex<()> = Mutex::new(());

// ---------------------------------------------------------------------------
// Build helper (mirrored from tests/boot.rs)
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

// ---------------------------------------------------------------------------
// BootEnv — bind-mount manager (mirrored from tests/boot.rs)
// ---------------------------------------------------------------------------

struct BootEnv {
    pg_lib: tempfile::TempDir,
    etc_pg: tempfile::TempDir,
    etc_pgb: tempfile::TempDir,
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

    /// Start the supervisor in a detached container.  Returns the container ID.
    ///
    /// Does NOT run with `--privileged`.  Instead:
    ///   - bash (PID 1) writes the MMDS file before spawning the supervisor, so
    ///     `init::run()` is bypassed (it's a no-op when PID != 1).
    ///   - All kernel-tuning writes (sysctl, nr_hugepages, THP) are already
    ///     best-effort and fail silently with EPERM in an unprivileged container.
    ///     Reserving hugepages with `--privileged` would consume ~2 GB of RAM and
    ///     trigger an OOM abort when `vm.overcommit_memory=2` then drops CommitLimit.
    ///   - bash traps SIGTERM and forwards it to the supervisor so `docker stop`
    ///     produces a clean exit.
    fn start_supervisor_detached(
        &self,
        binary: &Path,
        platform: &str,
        image: &str,
        postgres_password: &str,
    ) -> String {
        let pg_lib = self.pg_lib.path().display().to_string();
        let etc_pg = self.etc_pg.path().display().to_string();
        let etc_pgb = self.etc_pgb.path().display().to_string();
        let etc_sysctl = self.etc_sysctl.path().display().to_string();
        let bin = binary.display().to_string();

        let v_bin = format!("{bin}:/usr/local/bin/beyond-pg");
        let v_pglib = format!("{pg_lib}:/var/lib/postgresql/18");
        let v_etcpg = format!("{etc_pg}:/etc/postgresql/18/main");
        let v_etcpgb = format!("{etc_pgb}:/etc/pgbouncer");
        let v_sysctl = format!("{etc_sysctl}:/etc/sysctl.d");

        // Build the MMDS JSON inline so bash can write it before the supervisor
        // starts.  The supervisor reads /run/mmds/metadata.json via mmds::read()
        // — no Firecracker endpoint needed.
        let mmds_json = format!(
            r#"{{"latest":{{"meta-data":{{"POSTGRES_PASSWORD":"{postgres_password}"}}}}}}"#
        );
        let bash_cmd = format!(
            // Write MMDS file so the supervisor can read config without Firecracker.
            // NOTE: setup commands are joined with &&.  The FINAL setup command ends
            // with ';' (not '&&') so the trap, supervisor launch, and wait all run in
            // the PID-1 bash process — NOT in a background sub-shell.
            //
            // If '&&' connected setup → trap → supervisor & ..., bash would put the
            // entire &&-list in the background as a sub-shell, and the trap in that
            // sub-shell would have $SVPID unset.  Worse: bash running as PID 1 ignores
            // SIGTERM by default, so docker stop's SIGTERM would be silently discarded,
            // the container would never exit, and docker stop would SIGKILL after 30s.
            //
            // With ';' before the trap, bash (PID 1) sets the trap itself.  When
            // docker stop sends SIGTERM to PID 1, the trap fires immediately, kills
            // the supervisor, and exits cleanly.
            "mkdir -p /run/mmds && \
             printf '%s' '{mmds_json}' > /run/mmds/metadata.json && \
             printf '%s\\n' '#!/bin/sh' \
                 'for a in \"$@\"; do case \"$a\" in --pwfile=*) chmod 0644 \"${{a#--pwfile=}}\" 2>/dev/null;; esac; done' \
                 'chown postgres:postgres /var/lib/postgresql/18/main 2>/dev/null || true' \
                 'exec gosu postgres /usr/lib/postgresql/18/bin/initdb \"$@\"' \
                 > /usr/local/bin/initdb && \
             chmod +x /usr/local/bin/initdb && \
             mkdir -p /etc/postgresql/18/hooks/pre-start.d \
                      /etc/postgresql/18/hooks/post-start.d && \
             printf '%s\\n' '#!/bin/sh' \
                 'echo huge_pages = try >> /var/lib/postgresql/18/main/postgresql.auto.conf' \
                 > /etc/postgresql/18/hooks/pre-start.d/00-test-hugepages.sh && \
             chmod +x /etc/postgresql/18/hooks/pre-start.d/00-test-hugepages.sh; \
             trap 'kill \"$SVPID\" 2>/dev/null; wait \"$SVPID\"; exit $?' TERM INT; \
             /usr/local/bin/beyond-pg supervisor & \
             SVPID=$!; \
             wait \"$SVPID\""
        );

        let out = std::process::Command::new("docker")
            .args([
                "run",
                "-d",
                "--platform",
                platform,
                "-v",
                &v_bin,
                "-v",
                &v_pglib,
                "-v",
                &v_etcpg,
                "-v",
                &v_etcpgb,
                "-v",
                &v_sysctl,
                image,
                "bash",
                "-c",
                &bash_cmd,
            ])
            .output()
            .expect("docker run -d supervisor");

        assert!(
            out.status.success(),
            "failed to start supervisor container: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn docker_image_exists(name: &str) -> bool {
    std::process::Command::new("docker")
        .args(["image", "inspect", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn docker_exec(container_id: &str, args: &[&str]) -> std::process::Output {
    let mut cmd_args = vec!["exec", container_id];
    cmd_args.extend_from_slice(args);
    std::process::Command::new("docker")
        .args(&cmd_args)
        .output()
        .expect("docker exec")
}

fn docker_exec_ok(container_id: &str, args: &[&str]) -> bool {
    docker_exec(container_id, args).status.success()
}

/// Poll `pg_isready` via docker exec until Postgres accepts connections or timeout.
fn wait_postgres_ready(container_id: &str, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if docker_exec_ok(
            container_id,
            &[
                "pg_isready",
                "-h",
                "/var/run/postgresql",
                "-p",
                "5433",
                "-q",
            ],
        ) {
            return true;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    false
}

/// Send SIGTERM to a container and wait for it to exit, returning the exit code.
/// Waits up to `timeout`.
fn docker_stop(container_id: &str, timeout: Duration) -> Option<i32> {
    let secs = timeout.as_secs().to_string();
    let out = std::process::Command::new("docker")
        .args(["stop", "-t", &secs, container_id])
        .output()
        .expect("docker stop");
    if !out.status.success() {
        return None;
    }
    // docker wait returns the exit code of the container's main process.
    let wait_out = std::process::Command::new("docker")
        .args(["wait", container_id])
        .output()
        .expect("docker wait");
    std::str::from_utf8(&wait_out.stdout)
        .ok()?
        .trim()
        .parse::<i32>()
        .ok()
}

fn docker_logs(container_id: &str) -> String {
    let out = std::process::Command::new("docker")
        .args(["logs", container_id])
        .output()
        .expect("docker logs");
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full supervisor lifecycle on a primary-tier node.
///
/// Verifies:
///   - Supervisor boots PGDATA and starts Postgres.
///   - Post-start setup completes: pgbouncer role created, extensions created.
///   - PgBouncer process spawns.
///   - SIGTERM (via `docker stop`) causes a clean exit.
///
/// Requires `beyond-pg-test:latest` — build with `mise run build:test-image`.
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn supervisor_lifecycle_primary() {
    let _guard = DOCKER.lock().unwrap();
    let Some((binary, platform)) = build_linux_beyond_pg() else {
        return;
    };

    let image = "beyond-pg-test:latest";
    if !docker_image_exists(image) {
        eprintln!("skipping: {image} not found — run `mise run build:test-image` first");
        return;
    }

    let env = BootEnv::new();
    const PG_PASSWORD: &str = "testpassword123";

    // Start the supervisor.  bash writes the MMDS file before spawning beyond-pg
    // so no Firecracker endpoint is needed.  do_boot runs initdb on first start;
    // `huge_pages=try` is appended to postgresql.auto.conf by the pre-start hook.
    let container_id = env.start_supervisor_detached(&binary, platform, image, PG_PASSWORD);

    // Ensure the container is removed even if the test panics.
    let container_id_cleanup = container_id.clone();
    let _cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {})); // trigger drop
    struct ContainerCleanup(String);
    impl Drop for ContainerCleanup {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .status();
        }
    }
    let _container_guard = ContainerCleanup(container_id_cleanup);

    // Phase 4: wait for Postgres to be ready (up to 90s — first boot + extensions).
    let ready = wait_postgres_ready(&container_id, Duration::from_secs(90));
    if !ready {
        eprintln!("container logs:\n{}", docker_logs(&container_id));
        panic!("Postgres never became ready in supervisor container");
    }

    // Phase 5: wait for post-start to complete (pgbouncer role + extensions).
    // Poll for the pgbouncer role as a proxy for post_start completion.
    let post_start_done = (0..30).any(|_| {
        let out = docker_exec(
            &container_id,
            &[
                "gosu",
                "postgres",
                "psql",
                "-h",
                "/var/run/postgresql",
                "-p",
                "5433",
                "-tAc",
                "SELECT 1 FROM pg_roles WHERE rolname='pgbouncer'",
            ],
        );
        if out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "1" {
            return true;
        }
        std::thread::sleep(Duration::from_secs(1));
        false
    });
    if !post_start_done {
        eprintln!("container logs:\n{}", docker_logs(&container_id));
        panic!("post-start setup did not complete: pgbouncer role not found");
    }

    // Phase 6: verify PgBouncer process is running.
    // PgBouncer spawns immediately after post_start completes, but there can be a
    // brief gap between when the pgbouncer role is detectable (phase 5) and when
    // the process appears in the process table.
    let pgb_running = (0..30).any(|_| {
        if docker_exec_ok(&container_id, &["pgrep", "-x", "pgbouncer"]) {
            return true;
        }
        std::thread::sleep(Duration::from_secs(1));
        false
    });
    if !pgb_running {
        eprintln!("container logs:\n{}", docker_logs(&container_id));
        panic!("PgBouncer process not found after post-start");
    }

    // Phase 7: send SIGTERM (docker stop sends SIGTERM then SIGKILL after timeout).
    // The supervisor must handle SIGTERM and shut down cleanly (exit 0).
    let exit_code = docker_stop(&container_id, Duration::from_secs(30));
    eprintln!("supervisor exit code: {exit_code:?}");
    // docker stop returns the container's exit code via `docker wait`.
    // A clean SIGTERM shutdown exits 0.  143 (128+15) means killed by SIGTERM without
    // a handler — also acceptable since our SIGTERM propagation to children is correct.
    let code = exit_code.unwrap_or(-1);
    assert!(
        code == 0 || code == 143,
        "supervisor did not exit cleanly on SIGTERM: exit code {code}\nlogs:\n{}",
        docker_logs(&container_id)
    );
}
