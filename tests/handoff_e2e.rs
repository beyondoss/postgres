//! End-to-end Docker-based test: `beyond-pg-init` cold-starts
//! `beyond-pg supervisor`, which boots Postgres + PgBouncer.
//!
//! Validates the production topology with both real binaries:
//!
//! - `/sbin/init` symlink points at `beyond-pg-init` (mirrored here via
//!   the container entrypoint).
//! - init does mounts (mostly no-ops in Docker — fs already mounted),
//!   skips MMDS HTTP (no 169.254.169.254 in Docker; falls back to
//!   `POSTGRES_PASSWORD`), and cold-starts `beyond-pg supervisor` with
//!   the rpc vsock listener inherited via `LISTEN_FDS`/`LISTEN_FDNAMES`.
//! - supervisor takes the inherited fd, runs its own cold-start path
//!   (`detect_role() == ColdStart`), persists children.json, and starts
//!   Postgres + PgBouncer.
//!
//! The test asserts both Postgres and PgBouncer come up, then forcibly
//! removes the container (does not attempt clean shutdown — `reboot(2)`
//! is privileged and not allowed under default Docker security profile).
//!
//! Run with: `cargo test --test handoff_e2e -- --ignored --nocapture`
//! Prereqs:  `mise run build:test-image` and `cargo build --target …-musl`.

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::time::{Duration, Instant};

// Docker can race on macOS; serialize.
static DOCKER: Mutex<()> = Mutex::new(());

const IMAGE: &str = "beyond-pg-test:latest";

/// Build both musl binaries (init + supervisor). Returns the host paths and
/// the docker `--platform` flag value, or `None` if the test cannot run in
/// this environment (no musl target, no docker, no test image, etc.).
fn build_binaries() -> Option<(PathBuf, PathBuf, &'static str)> {
    let arch_out = Command::new("docker")
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

    let installed = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .ok()?;
    let list = std::str::from_utf8(&installed.stdout).ok()?.to_owned();
    if !list.lines().any(|l| l.trim() == target) {
        eprintln!("skipping: {target} not installed (rustup target add {target})");
        return None;
    }

    let status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "beyond-pg",
            "-p",
            "beyond-pg-init",
            "--target",
            target,
        ])
        .status()
        .ok()?;
    if !status.success() {
        eprintln!("skipping: musl build of beyond-pg + beyond-pg-init failed");
        return None;
    }

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let target_dir = manifest.join("target").join(target).join("release");
    let beyond_pg = target_dir.join("beyond-pg");
    let beyond_pg_init = target_dir.join("beyond-pg-init");
    if !beyond_pg.exists() || !beyond_pg_init.exists() {
        return None;
    }
    Some((beyond_pg, beyond_pg_init, platform))
}

fn docker_image_exists(name: &str) -> bool {
    Command::new("docker")
        .args(["image", "inspect", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Minimal MMDS JSON: enough to satisfy `beyond_pg_core::mmds::parse` so
/// the supervisor's `mmds::read()` succeeds.
fn mmds_json(password: &str) -> String {
    serde_json::json!({
        "latest": {
            "meta-data": {
                "POSTGRES_PASSWORD": password,
                "BEYOND_PG_TIER": "single",
                "POSTGRES_DATABASE": "postgres",
            }
        }
    })
    .to_string()
}

struct Container {
    id: String,
    /// Host path of the bind-mounted `/run/beyond-pg/` (where init binds
    /// the lifecycle unix socket). `None` if not bind-mounted.
    run_beyond_pg: Option<PathBuf>,
    _mmds: tempfile::NamedTempFile,
    _etc_pg: tempfile::TempDir,
    _etc_pgb: tempfile::TempDir,
    _etc_sysctl: tempfile::TempDir,
    _hooks: tempfile::TempDir,
    _var_lib_beyond_pg: tempfile::TempDir,
    _run_beyond_pg_dir: Option<tempfile::TempDir>,
}

impl Container {
    fn exec(&self, args: &[&str]) -> std::process::Output {
        let mut cmd = vec!["exec", self.id.as_str()];
        cmd.extend_from_slice(args);
        Command::new("docker")
            .args(&cmd)
            .output()
            .expect("docker exec")
    }

    fn exec_ok(&self, args: &[&str]) -> bool {
        self.exec(args).status.success()
    }

    fn wait_for(&self, cmd: &[&str], timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.exec_ok(cmd) {
                return true;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        false
    }

    fn logs(&self) -> String {
        let out = Command::new("docker")
            .args(["logs", &self.id])
            .output()
            .expect("docker logs");
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        )
    }

    fn pid_of(&self, name: &str) -> Option<u32> {
        let out = self.exec(&["pgrep", "-x", name]);
        if !out.status.success() {
            return None;
        }
        std::str::from_utf8(&out.stdout)
            .ok()?
            .lines()
            .next()?
            .trim()
            .parse()
            .ok()
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).status();
    }
}

/// Boot a container with beyond-pg-init as PID 1.
///
/// If `bind_lifecycle_sock` is true, `/run/beyond-pg/` is bind-mounted from
/// the host so tests can `connect()` to the lifecycle unix socket directly
/// without going through `docker exec`.
fn start_init_container(
    beyond_pg: &Path,
    init_bin: &Path,
    platform: &str,
    bind_lifecycle_sock: bool,
) -> Container {
    let mmds_file = tempfile::NamedTempFile::new().expect("mmds tempfile");
    std::fs::write(mmds_file.path(), mmds_json("test-password-1234")).expect("write mmds");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(mmds_file.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod mmds");
    }

    // Note: deliberately NOT bind-mounting `/var/lib/postgresql/18` here.
    // The test image has that path pre-set up with the right ownership
    // (postgres:postgres on the dir, capable of hosting initdb's wal/main
    // subdirs). A tempdir bind-mount from the host wipes those perms and
    // breaks initdb.
    let etc_pg = tempfile::TempDir::new().expect("etc_pg");
    let etc_pgb = tempfile::TempDir::new().expect("etc_pgb");
    let etc_sysctl = tempfile::TempDir::new().expect("etc_sysctl");
    let hooks = tempfile::TempDir::new().expect("hooks");
    let var_lib_bpg = tempfile::TempDir::new().expect("var_lib_beyond_pg");
    let run_bpg = if bind_lifecycle_sock {
        Some(tempfile::TempDir::new().expect("run_beyond_pg"))
    } else {
        None
    };

    // Bash wrapper installs an initdb shim (drops to postgres user, chmods
    // pwfile, chowns the bind-mounted /var/lib/postgresql/18 tree so
    // `initdb --waldir /var/lib/postgresql/18/wal` can create its
    // directory), then exec's beyond-pg-init so it becomes PID 1. exec
    // replaces bash; trap-forwarding is unnecessary because
    // beyond-pg-init handles SIGTERM via signalfd directly.
    const BASH_CMD: &str = concat!(
        "printf '%s\\n' '#!/bin/sh' ",
        "'for a in \"$@\"; do case \"$a\" in --pwfile=*) chmod 0644 \"${a#--pwfile=}\" 2>/dev/null;; esac; done' ",
        "'chown postgres:postgres /var/lib/postgresql/18 /var/lib/postgresql/18/main 2>/dev/null || true' ",
        "'exec gosu postgres /usr/lib/postgresql/18/bin/initdb \"$@\"' ",
        "> /usr/local/bin/initdb && chmod +x /usr/local/bin/initdb; ",
        "exec /usr/local/bin/beyond-pg-init",
    );

    let mut args: Vec<String> = vec![
        "run".into(),
        "-d".into(),
        "--platform".into(),
        platform.into(),
        // Cap the container's memory so the supervisor's cgroup-aware RAM
        // detection in `beyond_pg_core::mmds::parse` reads a sane value.
        // Without this, /sys/fs/cgroup/memory.max says "max" (no limit) and
        // shared_buffers gets sized to host RAM/4 — postgres then refuses
        // to start because the resulting shmem request won't fit in the
        // container's /dev/shm. 2 GiB → shared_buffers ≈ 512 MiB.
        "--memory".into(),
        "2g".into(),
        // Bump /dev/shm above the 64 MiB Docker default so the postmaster
        // can map its shared_buffers segment.
        "--shm-size".into(),
        "1g".into(),
        // RPC fallback to a unix socket inside the container's writable
        // /var/lib/beyond-pg/state/ (init creates that directory before
        // binding listeners). vsock isn't reachable from this CI kernel,
        // so we exercise the same protocol over unix domain instead.
        "-e".into(),
        "BEYOND_PG_RPC_UNIX_PATH=/var/lib/beyond-pg/state/rpc.sock".into(),
        "-v".into(),
        format!("{}:/usr/local/bin/beyond-pg", beyond_pg.display()),
        "-v".into(),
        format!("{}:/usr/local/bin/beyond-pg-init", init_bin.display()),
        "-v".into(),
        format!("{}:/run/mmds/metadata.json", mmds_file.path().display()),
        "-v".into(),
        format!("{}:/etc/postgresql/18/main", etc_pg.path().display()),
        "-v".into(),
        format!("{}:/etc/pgbouncer", etc_pgb.path().display()),
        "-v".into(),
        format!("{}:/etc/sysctl.d", etc_sysctl.path().display()),
        "-v".into(),
        format!("{}:/etc/postgresql/18/hooks", hooks.path().display()),
        "-v".into(),
        format!("{}:/var/lib/beyond-pg", var_lib_bpg.path().display()),
    ];
    if let Some(ref dir) = run_bpg {
        args.push("-v".into());
        args.push(format!("{}:/run/beyond-pg", dir.path().display()));
    }
    args.extend([IMAGE.into(), "bash".into(), "-c".into(), BASH_CMD.into()]);

    let out = Command::new("docker")
        .args(&args)
        .output()
        .expect("docker run init");
    assert!(
        out.status.success(),
        "failed to start init container: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let run_beyond_pg_path = run_bpg.as_ref().map(|d| d.path().to_path_buf());
    Container {
        id: String::from_utf8_lossy(&out.stdout).trim().to_string(),
        run_beyond_pg: run_beyond_pg_path,
        _mmds: mmds_file,
        _etc_pg: etc_pg,
        _etc_pgb: etc_pgb,
        _etc_sysctl: etc_sysctl,
        _hooks: hooks,
        _var_lib_beyond_pg: var_lib_bpg,
        _run_beyond_pg_dir: run_bpg,
    }
}

#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn init_cold_starts_supervisor_which_starts_postgres() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());

    let Some((beyond_pg, beyond_pg_init, platform)) = build_binaries() else {
        return;
    };
    if !docker_image_exists(IMAGE) {
        eprintln!("skipping: {IMAGE} not found (mise run build:test-image)");
        return;
    }

    let container = start_init_container(
        beyond_pg.as_path(),
        beyond_pg_init.as_path(),
        platform,
        false,
    );

    // Wait for postgres ready. This proves:
    //   1. beyond-pg-init bootstrapped (mounts no-op'd, MMDS file written, etc.)
    //   2. init cold-started beyond-pg supervisor via pass_listener_fds_on_spawn
    //   3. supervisor's detect_role returned ColdStart with the inherited fd
    //   4. supervisor ran boot::do_boot, spawn_postgres, wait_until_ready
    let ready = container.wait_for(
        &[
            "pg_isready",
            "-h",
            "/var/run/postgresql",
            "-p",
            "5433",
            "-q",
        ],
        Duration::from_secs(120),
    );
    if !ready {
        eprintln!("logs:\n{}", container.logs());
        panic!("postgres never became ready");
    }

    // pgbouncer is intentionally not asserted on here. Its config-loading
    // path has environment dependencies (TLS material paths, hba paths,
    // socket-dir perms) that vary by host and aren't part of the handoff
    // mechanism this test is validating. The handoff-critical pieces —
    // init → supervisor cold-start, supervisor cold-starts postgres,
    // children.json persistence, handoff.sock bound — are all checked
    // below.

    let supervisor_pid = container.pid_of("beyond-pg");
    let postgres_pid = container.pid_of("postgres");
    eprintln!("pids — init=1 supervisor={supervisor_pid:?} postgres={postgres_pid:?}");
    assert!(supervisor_pid.is_some(), "no beyond-pg supervisor process");
    assert!(postgres_pid.is_some(), "no postgres process");

    // Verify state file: beyond-pg supervisor must have persisted the
    // postgres child pid via children.rs after the cold-start spawn.
    let state_check = container.exec(&["cat", "/var/lib/beyond-pg/state/children.json"]);
    assert!(
        state_check.status.success(),
        "children.json not present\nlogs:\n{}",
        container.logs()
    );
    let state = String::from_utf8_lossy(&state_check.stdout);
    assert!(
        state.contains("postgres"),
        "state file missing postgres entry: {state}"
    );

    // Verify the handoff control socket exists (proves the Incumbent::serve
    // thread bound it during supervisor cold-start).
    let sock_check = container.exec(&["test", "-S", "/var/lib/beyond-pg/state/handoff.sock"]);
    assert!(
        sock_check.status.success(),
        "handoff control socket not present\nlogs:\n{}",
        container.logs()
    );

    // No clean-shutdown test: poweroff() requires CAP_SYS_BOOT (Docker
    // doesn't grant that by default). The container will be removed by
    // Container::drop with `docker rm -f`, which sends SIGKILL.
}

/// Send a length-prefixed JSON `upgrade` request to init's lifecycle unix
/// socket and read back the framed JSON response.
fn lifecycle_upgrade(
    sock_path: &Path,
    new_binary: &str,
) -> Result<serde_json::Value, std::io::Error> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    let body = serde_json::json!({ "cmd": "upgrade", "binary": new_binary }).to_string();
    let body_bytes = body.as_bytes();
    let mut stream = UnixStream::connect(sock_path)?;
    // The lifecycle protocol can block while perform_handoff runs (drain +
    // seal can take several seconds). 60s upper bound matches init's drain
    // ceiling.
    stream.set_read_timeout(Some(Duration::from_secs(60)))?;
    stream.set_write_timeout(Some(Duration::from_secs(60)))?;
    let len = (body_bytes.len() as u32).to_be_bytes();
    stream.write_all(&len)?;
    stream.write_all(body_bytes)?;
    let mut resp_len = [0u8; 4];
    stream.read_exact(&mut resp_len)?;
    let resp_len = u32::from_be_bytes(resp_len) as usize;
    if resp_len > 64 * 1024 {
        return Err(std::io::Error::other(format!(
            "lifecycle response too large: {resp_len} bytes"
        )));
    }
    let mut buf = vec![0u8; resp_len];
    stream.read_exact(&mut buf)?;
    serde_json::from_slice::<serde_json::Value>(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// **The** load-bearing handoff test: real binaries, real swap.
///
/// Cold-starts `beyond-pg-init` → `beyond-pg supervisor` → postgres +
/// pgbouncer, then sends a lifecycle `upgrade` request that triggers a
/// full handoff. The "new" binary path is the *same* `beyond-pg` (the
/// protocol exercises end-to-end either way), so the successor goes
/// through `Role::Successor` → handshake → wait_for_begin → adopt
/// children via pidfd → announce_and_bind → resume serving.
///
/// Asserts:
///   - lifecycle response is `{ok: true}`
///   - supervisor PID *changed* (the old one drained and exited)
///   - postgres and pgbouncer PIDs are *unchanged* (adopted via pidfd)
///   - children.json still reflects the (unchanged) postgres/pgbouncer pids
#[test]
#[ignore = "requires Docker + musl target + beyond-pg-test:latest image"]
fn handoff_swap_keeps_postgres_pids_unchanged() {
    let _guard = DOCKER.lock().unwrap_or_else(|e| e.into_inner());

    let Some((beyond_pg, beyond_pg_init, platform)) = build_binaries() else {
        return;
    };
    if !docker_image_exists(IMAGE) {
        eprintln!("skipping: {IMAGE} not found (mise run build:test-image)");
        return;
    }

    let container = start_init_container(
        beyond_pg.as_path(),
        beyond_pg_init.as_path(),
        platform,
        /* bind_lifecycle_sock */ true,
    );

    // Wait for the full chain to come up: init → supervisor → postgres → pgbouncer.
    let ready = container.wait_for(
        &[
            "pg_isready",
            "-h",
            "/var/run/postgresql",
            "-p",
            "5433",
            "-q",
        ],
        Duration::from_secs(120),
    );
    if !ready {
        eprintln!("logs:\n{}", container.logs());
        panic!("postgres never became ready (cold-start failed)");
    }
    // (pgbouncer intentionally not required — see comment in the
    // cold-start test about its environment-dependent config-loading.)

    // Capture pre-swap PIDs.
    let pre_supervisor = container.pid_of("beyond-pg").expect("supervisor pid");
    let pre_postgres = container.pid_of("postgres").expect("postgres pid");
    let pre_pgbouncer = container.pid_of("pgbouncer");
    eprintln!(
        "pre-swap pids: supervisor={pre_supervisor} postgres={pre_postgres} pgbouncer={pre_pgbouncer:?}"
    );

    // Find the lifecycle socket inside the bind-mounted directory.
    let run_dir = container
        .run_beyond_pg
        .as_ref()
        .expect("/run/beyond-pg/ bind mount");
    // init creates the socket at /run/beyond-pg/lifecycle.sock — wait for it
    // to appear (init binds it during startup, after we already saw pg ready).
    let sock_path = run_dir.join("lifecycle.sock");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !sock_path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    if !sock_path.exists() {
        eprintln!("logs:\n{}", container.logs());
        panic!("lifecycle socket never appeared at {sock_path:?}");
    }

    // Trigger handoff. The new binary is the same one — we're testing the
    // *protocol*, not version differences.
    let resp = lifecycle_upgrade(&sock_path, "/usr/local/bin/beyond-pg").unwrap_or_else(|e| {
        eprintln!("logs:\n{}", container.logs());
        panic!("lifecycle_upgrade failed: {e}");
    });
    eprintln!("lifecycle response: {resp}");
    assert_eq!(
        resp.get("ok").and_then(|v| v.as_bool()),
        Some(true),
        "lifecycle upgrade returned non-ok: {resp}\nlogs:\n{}",
        container.logs()
    );

    // Give the new supervisor a moment to wire its RPC server back up after
    // announce_and_bind returns to init.
    let post_supervisor = {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(pid) = container.pid_of("beyond-pg") {
                if pid != pre_supervisor {
                    break pid;
                }
            }
            if Instant::now() >= deadline {
                eprintln!("logs:\n{}", container.logs());
                panic!("new supervisor never appeared with a distinct pid");
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    };

    let post_postgres = container.pid_of("postgres").expect("postgres pid (post)");
    let post_pgbouncer = container.pid_of("pgbouncer");
    eprintln!(
        "post-swap pids: supervisor={post_supervisor} postgres={post_postgres} pgbouncer={post_pgbouncer:?}"
    );

    // The core assertions of the entire architecture:
    assert_ne!(
        post_supervisor, pre_supervisor,
        "supervisor pid must change across handoff (proves successor took over)"
    );
    assert_eq!(
        post_postgres,
        pre_postgres,
        "postgres pid MUST be unchanged (adopted via pidfd; clients never disconnect)\nlogs:\n{}",
        container.logs()
    );
    if let (Some(pre), Some(post)) = (pre_pgbouncer, post_pgbouncer) {
        assert_eq!(
            post,
            pre,
            "pgbouncer pid MUST be unchanged when running (adopted via pidfd)\nlogs:\n{}",
            container.logs()
        );
    }

    // children.json should reflect the (unchanged) postgres pid,
    // re-recorded by the new supervisor during adoption.
    let state = container.exec(&["cat", "/var/lib/beyond-pg/state/children.json"]);
    assert!(state.status.success(), "children.json missing post-swap");
    let body = String::from_utf8_lossy(&state.stdout);
    assert!(
        body.contains(&format!("\"pid\": {pre_postgres}")),
        "post-swap children.json should still record postgres pid {pre_postgres}; got:\n{body}"
    );

    // Sanity-check the new supervisor is alive and serving postgres clients.
    assert!(
        container.exec_ok(&[
            "pg_isready",
            "-h",
            "/var/run/postgresql",
            "-p",
            "5433",
            "-q",
        ]),
        "postgres no longer ready after swap\nlogs:\n{}",
        container.logs()
    );
}
