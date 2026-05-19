//! PID 1 supervise loop for `beyond-pg-init`.
//!
//! Sync only. Modeled on `paraglide-init/src/main.rs`:
//!
//! - One `poll(2)` over `[signalfd, supervisor_pidfd, lifecycle_vsock,
//!   lifecycle_unix]`.
//! - On signalfd: SIGTERM/SIGINT → drain supervisor → `reboot(POWER_OFF)`.
//! - On supervisor pidfd: child died → respawn with 1s backoff.
//! - On lifecycle accept: read framed JSON, dispatch to
//!   `handoff::Supervisor::perform_handoff`.
//!
//! `handoff::Supervisor` and its `perform_handoff` are sync and may block for
//! seconds while a handoff drains/seals. We just run them inline on this
//! thread; the only thing that needs immediate response is signalfd, and
//! during a handoff the only legitimate signal is "VM shutdown" which we'll
//! see after perform_handoff returns.
// Direct libc syscalls (pidfd_open, pidfd_send_signal, sigprocmask, signalfd,
// reboot, vsock socket/bind/listen/accept). Each `unsafe` block carries its
// own `// SAFETY:` justification at the call site.
#![allow(unsafe_code)]

use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::Duration;

use beyond_pg_core::vsock::{LIFECYCLE_PORT, RPC_PORT};

pub const SUPERVISOR_BINARY: &str = "/usr/local/bin/beyond-pg";
pub const STATE_DIR: &str = "/var/lib/beyond-pg/state";
pub const HANDOFF_SOCK: &str = "/var/lib/beyond-pg/state/handoff.sock";
pub const LIFECYCLE_UNIX_PATH: &str = "/run/beyond-pg/lifecycle.sock";

/// Directory prefixes a lifecycle `Upgrade` may target. The vsock/unix socket
/// is host-controlled, but the path itself comes from the wire — restrict it
/// to the locations where we expect supervisor binaries to live so a stray or
/// malformed request can't ask us to exec something arbitrary.
const ALLOWED_UPGRADE_PREFIXES: &[&str] = &["/usr/local/bin/", "/usr/local/sbin/"];

const DRAIN_TIMEOUT_MS: i32 = 10_000;
/// Max wait between SIGKILL and child reap before drain_supervisor gives up.
/// The kernel almost always reaps within milliseconds once SIGKILL fires;
/// this exists so the shutdown path can never block forever if the kill
/// itself failed (e.g. EPERM, ESRCH) and there's nothing to reap.
const SIGKILL_REAP_TIMEOUT_MS: i32 = 1_000;
const RESTART_BACKOFF: Duration = Duration::from_secs(1);
/// After a handoff commits, the incumbent (O) is on its way out — it
/// received `Commit` and the handoff library expects it to exit. We poll
/// its pidfd for this many milliseconds before escalating to SIGKILL so a
/// misbehaving O can never indefinitely shadow the new incumbent (N).
const POST_HANDOFF_EXIT_TIMEOUT_MS: i32 = 5_000;

// read_request casts a u32 length to usize and then bounds-checks it. This
// is lossless iff usize is at least 32 bits; Linux + vsock implies 64-bit
// in practice, but make the assumption a compile-time check rather than a
// silent invariant.
const _: () = assert!(usize::BITS >= 32, "init requires usize >= 32 bits");

/// Entry point. Never returns; either reboots the VM or panics.
pub fn run() -> ! {
    block_term_signals();
    let sigfd = signalfd_for_term().expect("signalfd");

    std::fs::create_dir_all(STATE_DIR).expect("create state dir");
    std::fs::create_dir_all(
        Path::new(LIFECYCLE_UNIX_PATH)
            .parent()
            .unwrap_or(Path::new("/")),
    )
    .ok();

    // Bind the listeners we'll hand to the supervisor (rpc) and keep here (lifecycle).
    //
    // RPC binding chooses transport based on the BEYOND_PG_RPC_UNIX_PATH env
    // var: when set, we bind a unix socket at that path (used by tests and
    // by local dev environments that have no vsock kernel module); otherwise
    // we bind vsock on RPC_PORT (production / Firecracker). Same env var is
    // also honored by `beyond-pg supervisor`'s cold-start path, which uses
    // the matching listener type when no inherited fd is passed.
    let rpc_fd: OwnedFd = match std::env::var("BEYOND_PG_RPC_UNIX_PATH") {
        Ok(path) => {
            eprintln!("[init] RPC listener: unix socket at {path}");
            bind_unix_listener(&path).expect("bind rpc unix")
        }
        Err(_) => bind_vsock_listener(RPC_PORT).expect("bind rpc vsock"),
    };
    // Lifecycle listener: vsock binding is best-effort because the host
    // kernel might not have AF_VSOCK exposed (Docker test environments).
    // The unix-socket fallback at LIFECYCLE_UNIX_PATH is always bound and
    // tests use it directly; production uses vsock.
    let lifecycle_vsock = bind_vsock_listener(LIFECYCLE_PORT).ok();
    if lifecycle_vsock.is_none() {
        eprintln!(
            "[init] WARNING: lifecycle vsock bind failed (no vsock kernel \
             support?); proceeding with unix-socket fallback only"
        );
    }
    let lifecycle_unix = bind_unix_listener(LIFECYCLE_UNIX_PATH).expect("bind lifecycle unix");

    // Build the handoff supervisor. The rpc fd is registered so it'll be
    // duplicated into the spawned supervisor's slot 3 on every spawn.
    let sup = handoff::Supervisor::new(Path::new(HANDOFF_SOCK))
        .expect("handoff::Supervisor::new")
        .with_listener("rpc", rpc_fd.as_raw_fd())
        .with_journal(PathBuf::from(format!("{STATE_DIR}/handoff.journal")));
    if let Err(e) = sup.resume_from_journal() {
        // Cold start without prior handoff state. We log loudly so an operator
        // can correlate dropped in-flight handoffs with the boot they happened
        // on; the supervisor will start fresh either way.
        eprintln!("[init] WARNING: handoff journal replay failed: {e}; starting cold");
    }

    // Cold-start spawn of the supervisor.
    let (mut supervisor_pid, mut supervisor_pidfd) =
        spawn_supervisor_with_pidfd(rpc_fd.as_raw_fd()).expect("cold start supervisor");
    eprintln!("[init] beyond-pg supervisor cold-started pid={supervisor_pid}");

    loop {
        // Build the poll set. The lifecycle vsock slot may be absent on
        // hosts without vsock — we put `-1` there which `poll(2)` skips.
        let lifecycle_vsock_fd = lifecycle_vsock
            .as_ref()
            .map(|f| f.as_raw_fd())
            .unwrap_or(-1);
        let mut fds = [
            pollfd(sigfd.as_raw_fd()),
            pollfd(supervisor_pidfd.as_raw_fd()),
            pollfd(lifecycle_vsock_fd),
            pollfd(lifecycle_unix.as_raw_fd()),
        ];
        // SAFETY: `fds` is a stack array of 4 valid pollfds; `len()` matches
        // the slice length we hand to the kernel. Timeout -1 = wait forever.
        let r = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
        if r < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            eprintln!("[init] poll error: {e}");
            continue;
        }

        // Order matters: handle shutdown before everything else.
        if (fds[0].revents & libc::POLLIN) != 0 {
            eprintln!("[init] shutdown signal received");
            drain_supervisor(supervisor_pid, &supervisor_pidfd);
            reap_orphans();
            poweroff();
        }

        if (fds[1].revents & libc::POLLIN) != 0 {
            eprintln!("[init] supervisor pid={supervisor_pid} exited");
            // Reap the zombie so the kernel releases its slot.
            let mut status: libc::c_int = 0;
            // SAFETY: WNOHANG is non-blocking; `status` is a stack out-pointer.
            // The pid was a direct child of ours.
            unsafe { libc::waitpid(supervisor_pid as i32, &mut status, libc::WNOHANG) };
            reap_orphans();
            // Respawn with backoff. We retry until both `spawn` and `pidfd_open`
            // succeed atomically — otherwise the stale `supervisor_pidfd` would
            // remain readable on every `poll` iteration and spin the CPU.
            loop {
                std::thread::sleep(RESTART_BACKOFF);
                match spawn_supervisor_with_pidfd(rpc_fd.as_raw_fd()) {
                    Ok((pid, fd)) => {
                        supervisor_pid = pid;
                        supervisor_pidfd = fd;
                        eprintln!("[init] supervisor respawned pid={supervisor_pid}");
                        break;
                    }
                    Err(e) => {
                        eprintln!("[init] supervisor respawn failed: {e}; retrying");
                    }
                }
            }
        }

        if (fds[2].revents & libc::POLLIN) != 0
            && let Some(ref l) = lifecycle_vsock
        {
            match accept_vsock(l.as_raw_fd()) {
                Ok(stream) => {
                    handle_and_adopt(stream, &sup, &mut supervisor_pid, &mut supervisor_pidfd)
                }
                Err(e) => eprintln!("[init] lifecycle vsock accept: {e}"),
            }
        }

        if (fds[3].revents & libc::POLLIN) != 0 {
            match accept_unix(lifecycle_unix.as_raw_fd()) {
                Ok(stream) => {
                    handle_and_adopt(stream, &sup, &mut supervisor_pid, &mut supervisor_pidfd)
                }
                Err(e) => eprintln!("[init] lifecycle unix accept: {e}"),
            }
        }
    }
}

/// Run the lifecycle request, write the response, and if the request was a
/// committed `Upgrade`, swap `supervisor_pid`/`supervisor_pidfd` to track the
/// new incumbent (N) instead of the outgoing one (O). Without this step the
/// main loop would keep watching O's pidfd, see it become readable when O
/// exits (expected after `Commit`), and cold-start a fresh supervisor that
/// fights N for the data-dir lock — see `adopt_handoff_child` for the
/// reap-O / track-N choreography.
fn handle_and_adopt(
    stream: std::fs::File,
    sup: &handoff::Supervisor,
    supervisor_pid: &mut u32,
    supervisor_pidfd: &mut OwnedFd,
) {
    let Some(new_child) = handle_lifecycle(stream, sup) else {
        return;
    };
    match adopt_handoff_child(*supervisor_pid, supervisor_pidfd, new_child) {
        Ok((new_pid, new_pidfd)) => {
            eprintln!(
                "[init] adopted post-handoff supervisor pid={new_pid} (was {})",
                *supervisor_pid
            );
            *supervisor_pid = new_pid;
            *supervisor_pidfd = new_pidfd;
        }
        Err(e) => {
            // Couldn't open a pidfd on N (it raced to exit between Commit
            // and adoption, or pidfd_open hit ENOSYS/EPERM). Leave the
            // tracking on O — the existing crash-recovery path will spawn
            // a fresh supervisor when O's pidfd fires.
            eprintln!(
                "[init] WARNING: failed to adopt post-handoff supervisor: {e}; \
                 cold-restart path will recover"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------

fn block_term_signals() {
    // SAFETY: `set` is initialized by `sigemptyset` before any read by
    // `sigprocmask`. The third arg (old set) is null because we don't care
    // about the previous mask. Called once at startup while single-threaded.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigprocmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

fn signalfd_for_term() -> std::io::Result<OwnedFd> {
    // SAFETY: `set` is initialized by `sigemptyset` before signalfd reads it.
    // `signalfd(-1, ...)` creates a new fd; on success it is a valid,
    // exclusively-owned descriptor that we immediately wrap in OwnedFd.
    // SFD_CLOEXEC prevents the fd from leaking into the supervisor child.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::sigaddset(&mut set, libc::SIGINT);
        let fd = libc::signalfd(-1, &set, libc::SFD_CLOEXEC);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(OwnedFd::from_raw_fd(fd))
    }
}

// ---------------------------------------------------------------------------
// poll(2) helper
// ---------------------------------------------------------------------------

fn pollfd(fd: RawFd) -> libc::pollfd {
    libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    }
}

// ---------------------------------------------------------------------------
// pidfd
// ---------------------------------------------------------------------------

fn pidfd_open(pid: u32) -> std::io::Result<OwnedFd> {
    // SAFETY: pidfd_open is a kernel syscall whose only inputs are the pid
    // and flags (0). On success it returns a freshly-allocated, exclusively
    // owned fd. The pidfd inherits FD_CLOEXEC by default (since Linux 5.3).
    let r = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if r < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        // SAFETY: `r` is a valid fd we just opened and have not handed out
        // to anyone else.
        Ok(unsafe { OwnedFd::from_raw_fd(r as RawFd) })
    }
}

fn pidfd_send_signal(fd: &OwnedFd, sig: libc::c_int) -> std::io::Result<()> {
    // SAFETY: `fd` is borrowed for the duration of the call (lifetime
    // outlives the syscall). pidfd_send_signal's kernel state is just the
    // fd, signal, and the null `info`/0 flags arguments we pass.
    let r = unsafe { libc::syscall(libc::SYS_pidfd_send_signal, fd.as_raw_fd(), sig, 0, 0) };
    if r < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// vsock helpers (sync, raw)
// ---------------------------------------------------------------------------

const AF_VSOCK: libc::c_int = 40;
const SOCK_STREAM: libc::c_int = 1;
const VMADDR_CID_ANY: u32 = 0xffff_ffff;

#[repr(C)]
struct sockaddr_vm {
    svm_family: libc::sa_family_t,
    svm_reserved1: u16,
    svm_port: u32,
    svm_cid: u32,
    svm_zero: [u8; 4],
}

fn bind_vsock_listener(port: u32) -> std::io::Result<OwnedFd> {
    // SAFETY: socket() with valid family/type returns a new fd on success.
    // SOCK_CLOEXEC is set so the fd doesn't accidentally leak into children
    // we haven't explicitly handed it to via handoff.
    let fd = unsafe { libc::socket(AF_VSOCK, SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `fd` is a valid, exclusively-owned descriptor we just created.
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    let addr = sockaddr_vm {
        svm_family: AF_VSOCK as libc::sa_family_t,
        svm_reserved1: 0,
        svm_port: port,
        svm_cid: VMADDR_CID_ANY,
        svm_zero: [0; 4],
    };
    // SAFETY: `addr` is a `#[repr(C)]` struct laid out as the kernel's
    // sockaddr_vm. The cast to `*const sockaddr` is the standard pattern
    // for bind(2); we pass the correct size.
    let r = unsafe {
        libc::bind(
            owned.as_raw_fd(),
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of::<sockaddr_vm>() as libc::socklen_t,
        )
    };
    if r < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `owned` is a valid bound socket; backlog 16 is positive.
    let r = unsafe { libc::listen(owned.as_raw_fd(), 16) };
    if r < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(owned)
}

fn accept_vsock(listener_fd: RawFd) -> std::io::Result<std::fs::File> {
    // SAFETY: `sockaddr_vm` is `#[repr(C)]` with no padding-sensitive
    // invariants; an all-zero pattern is a valid (unfilled) value that the
    // kernel will overwrite via the out-pointer below.
    let mut addr: sockaddr_vm = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<sockaddr_vm>() as libc::socklen_t;
    // SAFETY: `addr` and `len` are stack values borrowed for the duration
    // of the call. accept4 returns a new fd on success that we wrap in File.
    // SOCK_CLOEXEC prevents leaking accepted fds into future children.
    let fd = unsafe {
        libc::accept4(
            listener_fd,
            &mut addr as *mut _ as *mut libc::sockaddr,
            &mut len,
            libc::SOCK_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `fd` is a valid, exclusively-owned descriptor returned by accept4.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

// ---------------------------------------------------------------------------
// unix-domain socket fallback (used by tests; vsock isn't reachable from Docker)
// ---------------------------------------------------------------------------

fn bind_unix_listener(path: &str) -> std::io::Result<OwnedFd> {
    // Best-effort unlink: ignore NotFound.
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(e);
    }
    let listener = std::os::unix::net::UnixListener::bind(path)?;
    // Make the socket world-rw so callers reaching it through the
    // filesystem (the host control plane process — production: an agent
    // bind-mounted to the same dir; tests: the test harness on the host)
    // can connect. The dir-mount gates exposure; the socket mode is
    // intentionally permissive within that gate.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
    Ok(OwnedFd::from(listener))
}

fn accept_unix(listener_fd: RawFd) -> std::io::Result<std::fs::File> {
    // SAFETY: `sockaddr_un` is `#[repr(C)]`; all-zero is a valid placeholder
    // that the kernel overwrites via the out-pointer below.
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
    // SAFETY: `addr` and `len` are stack values borrowed for the call.
    // accept4 returns a new exclusively-owned fd on success. SOCK_CLOEXEC
    // prevents the accepted fd from leaking into future children.
    let fd = unsafe {
        libc::accept4(
            listener_fd,
            &mut addr as *mut _ as *mut libc::sockaddr,
            &mut len,
            libc::SOCK_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `fd` is a valid, exclusively-owned descriptor from accept4.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

// ---------------------------------------------------------------------------
// Lifecycle protocol (length-prefixed JSON over a stream)
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
#[cfg_attr(test, derive(Debug))]
#[serde(tag = "cmd", rename_all = "snake_case", deny_unknown_fields)]
enum LifecycleCmd {
    Upgrade { binary: String },
    Shutdown,
    Status,
}

#[derive(serde::Serialize)]
struct LifecycleResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Read and dispatch a lifecycle request, write the response, and return the
/// new supervisor `Child` if the request was a committed `Upgrade` — the
/// caller is responsible for adopting it via `adopt_handoff_child`.
fn handle_lifecycle(mut stream: std::fs::File, sup: &handoff::Supervisor) -> Option<Child> {
    let (resp, new_child) = match read_request(&mut stream) {
        Ok(cmd) => dispatch(cmd, sup),
        Err(e) => (
            LifecycleResponse {
                ok: false,
                error: Some(format!("read request: {e}")),
            },
            None,
        ),
    };
    // `LifecycleResponse` is bool + Option<String>; serde_json::to_vec is
    // infallible for these types. Failing here would indicate a bug we want
    // to see loudly rather than mask with a wrong-but-valid `{}` body.
    let body = serde_json::to_vec(&resp).expect("LifecycleResponse serialization is infallible");
    let len = (body.len() as u32).to_be_bytes();
    // A `BrokenPipe` is expected when the caller has timed out and closed
    // its end; other errors (e.g. partial write, ECONNRESET on a healthy
    // connection) point at a protocol bug worth surfacing.
    if let Err(e) = stream
        .write_all(&len)
        .and_then(|()| stream.write_all(&body))
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        eprintln!("[init] WARNING: lifecycle response write failed: {e}");
    }
    new_child
}

fn read_request(stream: &mut std::fs::File) -> std::io::Result<LifecycleCmd> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 64 * 1024 {
        return Err(std::io::Error::other(format!("body too large: {len}")));
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;
    serde_json::from_slice(&body)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Reject `Upgrade` requests whose `binary` is not an absolute path under a
/// known supervisor directory. The wire is host-controlled but the path comes
/// over the wire — refuse anything outside the allow-list so a malformed or
/// malicious request can't ask us to handoff-exec an arbitrary file.
fn validate_upgrade_binary(binary: &str) -> Result<PathBuf, String> {
    // PATH_MAX on Linux is 4096; refuse anything past that before allocating.
    if binary.len() > libc::PATH_MAX as usize {
        return Err(format!("binary path too long: {} bytes", binary.len()));
    }
    let path = PathBuf::from(binary);
    if !path.is_absolute() {
        return Err(format!("binary path must be absolute: {binary}"));
    }
    // `..` segments could escape an allow-listed prefix after the kernel
    // resolves them; refuse the request rather than canonicalize.
    if path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(format!("binary path may not contain `..`: {binary}"));
    }
    if !ALLOWED_UPGRADE_PREFIXES
        .iter()
        .any(|p| binary.starts_with(p))
    {
        return Err(format!(
            "binary path must be under one of {ALLOWED_UPGRADE_PREFIXES:?}: {binary}"
        ));
    }
    Ok(path)
}

/// Dispatch a lifecycle command. Returns the response and, on a committed
/// `Upgrade`, the `Child` for the new supervisor — the caller adopts it into
/// `supervisor_pid`/`supervisor_pidfd`.
fn dispatch(cmd: LifecycleCmd, sup: &handoff::Supervisor) -> (LifecycleResponse, Option<Child>) {
    match cmd {
        LifecycleCmd::Upgrade { binary } => {
            let path = match validate_upgrade_binary(&binary) {
                Ok(p) => p,
                Err(msg) => {
                    return (
                        LifecycleResponse {
                            ok: false,
                            error: Some(msg),
                        },
                        None,
                    );
                }
            };
            // `beyond-pg`'s clap parser requires a `Supervisor` subcommand
            // (see `src/main.rs`). The cold-start path passes it explicitly;
            // the handoff path needs the same arg or N would exit at clap
            // parsing before sending Hello, and `perform_handoff` would
            // time out waiting for the handshake.
            let mut spec = handoff::SpawnSpec::new(path);
            spec.args.push("supervisor".to_string());
            match sup.perform_handoff(spec) {
                Ok(outcome) if outcome.committed => (
                    LifecycleResponse {
                        ok: true,
                        error: None,
                    },
                    outcome.child,
                ),
                Ok(outcome) => (
                    LifecycleResponse {
                        ok: false,
                        error: Some(format!("aborted: {:?}", outcome.abort_reason)),
                    },
                    None,
                ),
                Err(e) => (
                    LifecycleResponse {
                        ok: false,
                        error: Some(format!("{e}")),
                    },
                    None,
                ),
            }
        }
        LifecycleCmd::Shutdown => {
            // Trigger our own shutdown via the signalfd path so the main loop
            // runs the same drain+poweroff sequence as a host-initiated SIGTERM.
            // The response is written before the next poll iteration picks
            // up the signal.
            //
            // SAFETY: `kill` is async-signal-safe. `libc::getpid()` returns
            // our own pid and `SIGTERM` is blocked via the sigprocmask we
            // installed in `block_term_signals`, so the signal is queued for
            // our signalfd rather than delivered as an interrupt — it cannot
            // re-enter this dispatch path or perturb shared state.
            unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
            (
                LifecycleResponse {
                    ok: true,
                    error: None,
                },
                None,
            )
        }
        LifecycleCmd::Status => (
            LifecycleResponse {
                ok: true,
                error: None,
            },
            None,
        ),
    }
}

// ---------------------------------------------------------------------------
// Post-handoff adoption: the new incumbent (N) was just spawned and committed
// by `handoff::Supervisor::perform_handoff`. Open a pidfd on N so the main
// loop can poll it, then reap the outgoing incumbent (O) so its zombie
// doesn't linger after we drop the pidfd that was watching it.
// ---------------------------------------------------------------------------

fn adopt_handoff_child(
    old_pid: u32,
    old_pidfd: &OwnedFd,
    new_child: Child,
) -> std::io::Result<(u32, OwnedFd)> {
    let new_pid = new_child.id();
    let new_pidfd = pidfd_open(new_pid)?;
    // From this point N is tracked via pidfd; the `Child` handle is no
    // longer needed. `Child::drop` does NOT kill or wait, so N keeps
    // running normally and its eventual zombie is reaped by the main
    // loop's `fds[1]` handler when `new_pidfd` fires.
    drop(new_child);

    // O received `Commit` during `perform_handoff` and is on its way out.
    // Wait up to POST_HANDOFF_EXIT_TIMEOUT_MS for its pidfd to fire; if it
    // doesn't, force-kill it so we can replace the tracking cleanly.
    let mut pfd = [pollfd(old_pidfd.as_raw_fd())];
    // SAFETY: `pfd` is a stack array of 1 valid pollfd; timeout is positive.
    let r = unsafe { libc::poll(pfd.as_mut_ptr(), 1, POST_HANDOFF_EXIT_TIMEOUT_MS) };
    if r <= 0 {
        eprintln!(
            "[init] WARNING: outgoing supervisor pid={old_pid} did not exit \
             {POST_HANDOFF_EXIT_TIMEOUT_MS}ms after Commit; sending SIGKILL"
        );
        let _ = pidfd_send_signal(old_pidfd, libc::SIGKILL);
        // SAFETY: stack pollfd array; bounded timeout.
        let _ = unsafe { libc::poll(pfd.as_mut_ptr(), 1, SIGKILL_REAP_TIMEOUT_MS) };
    }
    // Reap O specifically (don't wait — it should be a zombie now), then
    // sweep any orphans that O reparented to us on exit.
    let mut status: libc::c_int = 0;
    // SAFETY: WNOHANG is non-blocking; `status` is a stack out-pointer; pid
    // slot is still pinned by O's unreaped zombie if it's gone, or by O
    // itself if it's still alive after SIGKILL (in which case waitpid
    // returns 0 and reap_orphans below catches it on a later sweep).
    unsafe { libc::waitpid(old_pid as i32, &mut status, libc::WNOHANG) };
    reap_orphans();

    Ok((new_pid, new_pidfd))
}

// ---------------------------------------------------------------------------
// Cold-start spawn of the supervisor child
// ---------------------------------------------------------------------------

fn cold_start_supervisor(rpc_fd: RawFd) -> std::io::Result<u32> {
    let mut cmd = std::process::Command::new(SUPERVISOR_BINARY);
    cmd.arg("supervisor")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());
    // Pass rpc listener as inherited FD via systemd-style LISTEN_FDS/FDNAMES.
    handoff::pass_listener_fds_on_spawn(&mut cmd, &[("rpc".to_string(), rpc_fd)], None);
    let child = cmd.spawn()?;
    Ok(child.id())
}

/// Spawn the supervisor and open its pidfd in one step. Failing to open the
/// pidfd after spawn would leave us with a child we can't `poll(2)` on; in
/// that case kill the child and return the error so the caller retries
/// cleanly. Returns `(pid, OwnedFd)`.
fn spawn_supervisor_with_pidfd(rpc_fd: RawFd) -> std::io::Result<(u32, OwnedFd)> {
    let pid = cold_start_supervisor(rpc_fd)?;
    match pidfd_open(pid) {
        Ok(fd) => Ok((pid, fd)),
        Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {
            // The kernel says this pid doesn't exist. We never called
            // waitpid, so a live or zombie child of ours should still hold
            // its pid slot — ESRCH here means the process is genuinely gone
            // (reaped by some other path or never registered). Signaling
            // the numeric pid in this state risks hitting an unrelated
            // process that has since been allocated the same id, so we
            // skip the kill+wait entirely and just propagate the error.
            Err(e)
        }
        Err(e) => {
            // Non-ESRCH failure (EPERM, ENOSYS on ancient kernels, etc.):
            // we haven't called waitpid, so the pid slot is still pinned by
            // our unreaped child and cannot have been recycled. SIGKILL via
            // numeric pid is safe.
            // SAFETY: `kill` with SIGKILL is async-signal-safe; the pid
            // slot is held by our unreaped child for the duration of the
            // call.
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
            // Reap so we don't leave a zombie.
            let mut status: libc::c_int = 0;
            // SAFETY: pid is our direct child; `status` is a stack out-pointer.
            unsafe { libc::waitpid(pid as i32, &mut status, 0) };
            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Shutdown sequence
// ---------------------------------------------------------------------------

fn drain_supervisor(pid: u32, pidfd: &OwnedFd) {
    let _ = pidfd_send_signal(pidfd, libc::SIGTERM);
    let mut pfd = [pollfd(pidfd.as_raw_fd())];
    // SAFETY: `pfd` is a stack array of 1 valid pollfd; timeout is positive.
    let r = unsafe { libc::poll(pfd.as_mut_ptr(), 1, DRAIN_TIMEOUT_MS) };
    if r > 0 && (pfd[0].revents & libc::POLLIN) != 0 {
        let mut status: libc::c_int = 0;
        // SAFETY: WNOHANG is non-blocking; pid is our direct child.
        unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) };
        return;
    }
    // Timed out — escalate to SIGKILL. We bound the post-kill wait so that
    // a failed kill (EPERM/ESRCH, or a pid already reaped by reap_orphans)
    // can never block shutdown forever.
    let _ = pidfd_send_signal(pidfd, libc::SIGKILL);
    let mut pfd = [pollfd(pidfd.as_raw_fd())];
    // SAFETY: stack pollfd array; bounded timeout.
    let _ = unsafe { libc::poll(pfd.as_mut_ptr(), 1, SIGKILL_REAP_TIMEOUT_MS) };
    let mut status: libc::c_int = 0;
    // SAFETY: WNOHANG keeps this non-blocking. If the child is already
    // gone (e.g. reap_orphans got it), waitpid returns -1/ECHILD and we
    // proceed to poweroff regardless.
    unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) };
}

fn reap_orphans() {
    loop {
        let mut status: libc::c_int = 0;
        // SAFETY: waitpid(-1, ..., WNOHANG) reaps any reapable child without
        // blocking; `status` is a stack out-pointer.
        let r = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if r <= 0 {
            break;
        }
    }
}

fn poweroff() -> ! {
    // SAFETY: sync() takes no arguments and cannot fail in a way that
    // requires recovery — it returns void.
    unsafe { libc::sync() };
    // SAFETY: reboot(LINUX_REBOOT_CMD_POWER_OFF) is the canonical kernel
    // shutdown entry. We've called sync() so dirty buffers are flushed.
    // On success this does not return; on failure we exit so the kernel
    // panics (PID 1 exit triggers a kernel panic).
    unsafe { libc::reboot(libc::LINUX_REBOOT_CMD_POWER_OFF) };
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_binary_accepts_allowed_prefixes() {
        assert!(validate_upgrade_binary("/usr/local/bin/beyond-pg").is_ok());
        assert!(validate_upgrade_binary("/usr/local/sbin/beyond-pg").is_ok());
        // Nested paths under the allow-list are fine.
        assert!(validate_upgrade_binary("/usr/local/bin/sub/beyond-pg-next").is_ok());
    }

    #[test]
    fn upgrade_binary_rejects_relative_paths() {
        let err = validate_upgrade_binary("usr/local/bin/beyond-pg").unwrap_err();
        assert!(err.contains("absolute"), "got: {err}");
        let err = validate_upgrade_binary("./beyond-pg").unwrap_err();
        assert!(err.contains("absolute"), "got: {err}");
    }

    #[test]
    fn upgrade_binary_rejects_parent_dir_components() {
        let err = validate_upgrade_binary("/usr/local/bin/../../etc/passwd").unwrap_err();
        assert!(err.contains(".."), "got: {err}");
        // The `..` component check is structural — substrings with `..` inside
        // a single filename like `foo..bar` are still allowed (they are not a
        // ParentDir component).
        assert!(validate_upgrade_binary("/usr/local/bin/foo..bar").is_ok());
    }

    #[test]
    fn upgrade_binary_rejects_paths_outside_allow_list() {
        let err = validate_upgrade_binary("/tmp/evil").unwrap_err();
        assert!(err.contains("under"), "got: {err}");
        let err = validate_upgrade_binary("/etc/passwd").unwrap_err();
        assert!(err.contains("under"), "got: {err}");
        // Prefix lookalike: `/usr/local/bin-evil/...` does not start with
        // `/usr/local/bin/` (trailing slash distinguishes them).
        let err = validate_upgrade_binary("/usr/local/bin-evil/beyond-pg").unwrap_err();
        assert!(err.contains("under"), "got: {err}");
        let err = validate_upgrade_binary("/usr/local/binfoo").unwrap_err();
        assert!(err.contains("under"), "got: {err}");
    }

    #[test]
    fn upgrade_binary_rejects_oversized_paths() {
        let huge = format!("/usr/local/bin/{}", "a".repeat(libc::PATH_MAX as usize));
        let err = validate_upgrade_binary(&huge).unwrap_err();
        assert!(err.contains("too long"), "got: {err}");
    }

    #[test]
    fn lifecycle_cmd_rejects_unknown_fields_in_struct_variants() {
        // serde's `deny_unknown_fields` enforces field-set on struct variants
        // of internally-tagged enums (`Upgrade { binary }` here). It does not
        // apply to unit variants — `Status`/`Shutdown` silently ignore extras.
        let json = br#"{"cmd":"upgrade","binary":"/usr/local/bin/x","extra":1}"#;
        let err = serde_json::from_slice::<LifecycleCmd>(json).unwrap_err();
        assert!(
            err.to_string().contains("unknown field"),
            "deny_unknown_fields should reject extras on Upgrade; got: {err}"
        );
    }

    #[test]
    fn lifecycle_cmd_parses_variants() {
        let status: LifecycleCmd = serde_json::from_slice(br#"{"cmd":"status"}"#).unwrap();
        assert!(matches!(status, LifecycleCmd::Status));
        let shutdown: LifecycleCmd = serde_json::from_slice(br#"{"cmd":"shutdown"}"#).unwrap();
        assert!(matches!(shutdown, LifecycleCmd::Shutdown));
        let up: LifecycleCmd =
            serde_json::from_slice(br#"{"cmd":"upgrade","binary":"/usr/local/bin/x"}"#).unwrap();
        match up {
            LifecycleCmd::Upgrade { binary } => assert_eq!(binary, "/usr/local/bin/x"),
            _ => panic!("expected Upgrade"),
        }
    }
}
