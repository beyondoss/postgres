#![deny(unused_must_use)]

mod quic_recv;
mod wal_recv;

// Re-export send_one from the crate lib so quic_recv (a submodule) can reach
// it as `crate::send_one` without naming beyond_pg_sink directly.
pub(crate) use beyond_pg_sink::send_one;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);
static CONN_COUNT: AtomicUsize = AtomicUsize::new(0);
const MAX_CONNS: usize = 256;

#[cfg(unix)]
extern "C" fn handle_sigterm(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Release);
}

#[derive(PartialEq)]
enum Mode {
    Tcp,
    Quic,
}

struct Args {
    /// Required for `--mode tcp`; ignored for `--mode quic`.
    connstr: Option<String>,
    dir: String,
    port: u16,
    slot: String,
    retention_segments: usize,
    mode: Mode,
}

fn parse_args() -> Args {
    let mut connstr: Option<String> = None;
    let mut dir = String::from("/var/lib/postgresql/wal-sink");
    let mut port: u16 = 9000;
    let mut slot = String::from("wal_sink");
    let mut retention_segments: usize = 64;
    let mut mode = Mode::Quic;

    let mut args = std::env::args().skip(1);
    loop {
        match args.next().as_deref() {
            None => break,
            Some("--connstr") => connstr = args.next(),
            Some("--dir") => {
                if let Some(v) = args.next() {
                    dir = v;
                }
            }
            Some("--port") => match args.next().as_deref() {
                Some(v) => match v.parse() {
                    Ok(n) => port = n,
                    Err(_) => {
                        eprintln!("error: --port must be a number");
                        std::process::exit(1);
                    }
                },
                None => {
                    eprintln!("error: --port requires a value");
                    std::process::exit(1);
                }
            },
            Some("--slot") => {
                if let Some(v) = args.next() {
                    slot = v;
                }
            }
            Some("--retention-segments") => match args.next().as_deref() {
                Some(v) => match v.parse::<usize>() {
                    Ok(n) => retention_segments = n,
                    Err(_) => {
                        eprintln!("error: --retention-segments must be a number");
                        std::process::exit(1);
                    }
                },
                None => {
                    eprintln!("error: --retention-segments requires a value");
                    std::process::exit(1);
                }
            },
            Some("--mode") => match args.next().as_deref() {
                Some("tcp") => mode = Mode::Tcp,
                Some("quic") => mode = Mode::Quic,
                Some(other) => {
                    eprintln!("error: --mode must be 'tcp' or 'quic' (got '{other}')");
                    std::process::exit(1);
                }
                None => {
                    eprintln!("error: --mode requires a value");
                    std::process::exit(1);
                }
            },
            Some(other) => {
                eprintln!("error: unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }

    if mode == Mode::Tcp && connstr.is_none() {
        eprintln!("error: --connstr is required for --mode tcp");
        std::process::exit(1);
    }

    if retention_segments < 8 {
        eprintln!(
            "error: --retention-segments must be at least 8 (minimum safe margin for recovery)"
        );
        std::process::exit(1);
    }

    Args {
        connstr,
        dir,
        port,
        slot,
        retention_segments,
        mode,
    }
}

fn parse_kv_value(s: &str) -> (String, usize) {
    if let Some(inner) = s.strip_prefix('\'') {
        let mut val = String::new();
        let mut consumed = s.len();
        let mut chars = inner.char_indices();
        while let Some((i, c)) = chars.next() {
            match c {
                '\\' => {
                    if let Some((_, nc)) = chars.next() {
                        val.push(nc);
                    }
                }
                '\'' => {
                    consumed = i + 2; // +1 for opening-quote offset, +1 past closing quote
                    break;
                }
                _ => val.push(c),
            }
        }
        (val, consumed)
    } else {
        let end = s.find(char::is_whitespace).unwrap_or(s.len());
        (s[..end].to_owned(), end)
    }
}

fn main() {
    let args = parse_args();

    // Create with mode 0o750 atomically so there is no window between mkdir and
    // chmod where the directory is world-accessible.
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        if let Err(e) = std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o750)
            .create(&args.dir)
        {
            eprintln!("error: failed to create --dir {}: {e}", args.dir);
            std::process::exit(1);
        }
        // Correct permissions if the directory pre-existed with a looser mode.
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(e) =
                std::fs::set_permissions(&args.dir, std::fs::Permissions::from_mode(0o750))
            {
                eprintln!("warning: could not set permissions on {}: {e}", args.dir);
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(e) = std::fs::create_dir_all(&args.dir) {
        eprintln!("error: failed to create --dir {}: {e}", args.dir);
        std::process::exit(1);
    }

    // Remove any .partial files left by a previous crash.  The streaming
    // receiver always restarts from highest_local_lsn (the end of the last
    // *complete* segment), so partial files are never needed for recovery and
    // would accumulate as dead disk weight across restarts.
    cleanup_partial_segments(&args.dir);

    // Clear any pre-existing excess before pg_receivewal starts writing.
    prune_old_segments(&args.dir, args.retention_segments);

    // Install SIGTERM handler before binding or spawning threads.
    #[cfg(unix)]
    {
        // SAFETY: handle_sigterm only writes to a static AtomicBool, which is
        // async-signal-safe per POSIX. sa is fully initialized via zeroed() and
        // explicit field assignments. SA_RESTART is intentionally not set so
        // that accept(2) can be interrupted; the accept loop also polls a
        // timeout to ensure SHUTDOWN is checked promptly regardless.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = handle_sigterm as libc::sighandler_t;
            libc::sigemptyset(&mut sa.sa_mask);
            sa.sa_flags = 0;
            if libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut()) < 0 {
                eprintln!(
                    "warn: sigaction failed: {} — SIGTERM will not trigger graceful shutdown",
                    std::io::Error::last_os_error()
                );
            }
        }
    }

    // Bind before spawning the receiver thread so that a bind failure exits
    // cleanly without leaving a background thread running.
    let listener = match TcpListener::bind(("0.0.0.0", args.port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: failed to bind 0.0.0.0:{}: {e}", args.port);
            std::process::exit(1);
        }
    };

    let dir_recv = args.dir.clone();
    let watcher_thread = start_retention_watcher(args.dir.clone(), args.retention_segments);

    let recv_thread = match args.mode {
        Mode::Tcp => {
            let connstr = args.connstr.as_deref().expect("validated above");
            let recv_cfg = parse_connstr_to_receiver_config(connstr, &args.dir, &args.slot);
            std::thread::spawn(move || {
                run_native_receiver(recv_cfg, &dir_recv);
            })
        }
        Mode::Quic => {
            let dir_quic = std::path::PathBuf::from(dir_recv);
            let port = args.port;
            std::thread::spawn(move || {
                if let Err(e) = quic_recv::run_quic_server(port, dir_quic) {
                    eprintln!("quic: fatal: {e}");
                    std::process::exit(1);
                }
            })
        }
    };

    run_http_server(listener, &args.dir, args.port);

    if recv_thread.join().is_err() {
        eprintln!("warn: receiver thread panicked");
    }
    if watcher_thread.join().is_err() {
        eprintln!("warn: retention watcher thread panicked");
    }
}

fn cleanup_partial_segments(dir: &str) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s.ends_with(".partial") {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => eprintln!("startup: removed orphaned partial segment {s}"),
                Err(e) => eprintln!("warn: could not remove partial segment {s}: {e}"),
            }
        }
    }
}

fn prune_old_segments(dir: &str, retain: usize) {
    let entries: Vec<String> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let raw = e.file_name();
                match raw.to_str() {
                    Some(s) => Some(s.to_owned()),
                    None => {
                        eprintln!("warn: skipping non-UTF-8 filename in WAL dir: {raw:?}");
                        None
                    }
                }
            })
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .collect(),
        Err(e) => {
            eprintln!("warn: prune_old_segments: read_dir failed: {e}");
            return;
        }
    };

    // Group by timeline prefix (first 8 chars) and prune each independently.
    // Timeline 1 and timeline 2 segments coexist after a failover; treating
    // them as separate pools prevents a large timeline-2 backlog from evicting
    // all timeline-1 segments needed for recovery.
    let mut by_timeline: std::collections::HashMap<&str, Vec<&str>> =
        std::collections::HashMap::new();
    for name in &entries {
        by_timeline
            .entry(&name[..8])
            .or_default()
            .push(name.as_str());
    }

    for segs in by_timeline.values_mut() {
        segs.sort_unstable();
        if segs.len() <= retain {
            continue;
        }
        let to_delete = segs.len() - retain;
        for name in &segs[..to_delete] {
            let path = format!("{dir}/{name}");
            // POSIX unlink removes the directory entry but the inode and file
            // data persist until all open file descriptors are closed. An HTTP
            // handler calling sendfile on this segment sees the complete file.
            // No lock between the retention watcher and HTTP handlers is needed.
            match std::fs::remove_file(&path) {
                Ok(()) => eprintln!("debug: pruned WAL segment {name}"),
                Err(e) => eprintln!("warn: failed to prune WAL segment {name}: {e}"),
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn start_retention_watcher(dir: String, retain: usize) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // SAFETY: inotify_init1 is always safe to call; IN_CLOEXEC ensures the
        // fd is not inherited across exec.
        let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
        if fd < 0 {
            eprintln!(
                "warn: inotify_init1 failed: {} — retention watcher disabled",
                std::io::Error::last_os_error()
            );
            return;
        }

        let path = match std::ffi::CString::new(dir.as_bytes()) {
            Ok(p) => p,
            Err(_) => {
                eprintln!("warn: WAL dir path contains null byte — retention watcher disabled");
                // SAFETY: fd is a valid inotify descriptor returned by inotify_init1 above.
                unsafe { libc::close(fd) };
                return;
            }
        };

        // Watch for IN_MOVED_TO: pg_receivewal completes a segment by renaming
        // <name>.partial → <name>. That rename fires exactly when pruning is needed.
        // SAFETY: fd and path are valid for the duration of this call.
        let wd = unsafe { libc::inotify_add_watch(fd, path.as_ptr(), libc::IN_MOVED_TO) };
        if wd < 0 {
            eprintln!(
                "warn: inotify_add_watch failed: {} — retention watcher disabled",
                std::io::Error::last_os_error()
            );
            // SAFETY: fd is a valid inotify descriptor returned by inotify_init1 above.
            unsafe { libc::close(fd) };
            return;
        }

        let mut buf = [0u8; 4096];
        loop {
            // Poll with a 500 ms timeout so SIGTERM is noticed promptly even
            // if it is delivered to a different thread (which would not
            // interrupt this thread's syscall with EINTR). A bare read(2)
            // would block indefinitely in that case, hanging process exit.
            //
            // SAFETY: pfd is fully initialised with all three fields; the
            // pointer is valid for the duration of the poll(2) call.
            let mut pfd = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe { libc::poll(&mut pfd, 1, 500) };

            if SHUTDOWN.load(Ordering::Acquire) {
                break;
            }

            if rc < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                eprintln!("warn: inotify poll error: {err} — falling back to 30s poll");
                unsafe { libc::close(fd) };
                loop {
                    std::thread::sleep(Duration::from_secs(30));
                    if SHUTDOWN.load(Ordering::Acquire) {
                        return;
                    }
                    prune_old_segments(&dir, retain);
                }
            }

            if rc == 0 {
                // Timeout — loop to re-check SHUTDOWN.
                continue;
            }

            // POLLIN: drain the inotify descriptor (events are not inspected;
            // any IN_MOVED_TO on the WAL dir is sufficient to trigger a prune).
            //
            // SAFETY: fd is a valid inotify file descriptor; buf is a mutable
            // byte slice of sufficient size for at least one inotify_event.
            let nr = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if nr < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                eprintln!("warn: inotify read error: {err} — falling back to 30s poll");
                unsafe { libc::close(fd) };
                loop {
                    std::thread::sleep(Duration::from_secs(30));
                    if SHUTDOWN.load(Ordering::Acquire) {
                        return;
                    }
                    prune_old_segments(&dir, retain);
                }
            }
            prune_old_segments(&dir, retain);
        }

        unsafe { libc::close(fd) };
    })
}

#[cfg(not(target_os = "linux"))]
fn start_retention_watcher(dir: String, retain: usize) -> std::thread::JoinHandle<()> {
    poll_retention_watcher(dir, retain, Duration::from_secs(30))
}

#[cfg(not(target_os = "linux"))]
fn poll_retention_watcher(
    dir: String,
    retain: usize,
    interval: Duration,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(interval);
            if SHUTDOWN.load(Ordering::Acquire) {
                break;
            }
            prune_old_segments(&dir, retain);
        }
    })
}

/// Parse a libpq connection string into a `ReceiverConfig`.
/// Extracts `host`, `port`, `user` keyword-value pairs; falls back to
/// Postgres defaults for anything not specified.
fn parse_connstr_to_receiver_config(
    connstr: &str,
    dir: &str,
    slot: &str,
) -> wal_recv::ReceiverConfig {
    let mut host = "127.0.0.1".to_owned();
    let mut port: u16 = 5432;
    let mut user = "postgres".to_owned();
    let mut password: Option<String> = None;

    // Handle URI form: postgres://user[:password]@host:port/db
    for prefix in ["postgresql://", "postgres://"] {
        if let Some(rest) = connstr.strip_prefix(prefix) {
            let rest = rest.split('/').next().unwrap_or(rest); // strip /db
            let (userinfo, hostport) = if let Some(at) = rest.rfind('@') {
                (&rest[..at], &rest[at + 1..])
            } else {
                ("", rest)
            };
            if !userinfo.is_empty() {
                if let Some(colon) = userinfo.find(':') {
                    user = userinfo[..colon].to_owned();
                    password = Some(userinfo[colon + 1..].to_owned());
                } else {
                    user = userinfo.to_owned();
                }
            }
            if let Some(bracket_end) = hostport.find(']') {
                // IPv6 literal [::1]:port
                host = hostport[1..bracket_end].to_owned();
                if let Some(p) = hostport[bracket_end + 1..].strip_prefix(':') {
                    port = p.parse().unwrap_or(5432);
                }
            } else if let Some(colon) = hostport.rfind(':') {
                host = hostport[..colon].to_owned();
                port = hostport[colon + 1..].parse().unwrap_or(5432);
            } else if !hostport.is_empty() {
                host = hostport.to_owned();
            }
            break;
        }
    }

    // Handle keyword=value form.
    if !connstr.starts_with("postgresql://") && !connstr.starts_with("postgres://") {
        let mut s = connstr;
        while !s.is_empty() {
            s = s.trim_start();
            let Some(eq) = s.find('=') else { break };
            let key = s[..eq].trim_end();
            s = &s[eq + 1..];
            let (value, consumed) = parse_kv_value(s);
            s = &s[consumed..];
            match key {
                "host" => host = value,
                "port" => port = value.parse().unwrap_or(5432),
                "user" => user = value,
                "password" => password = Some(value),
                _ => {}
            }
        }
    }

    wal_recv::ReceiverConfig {
        host,
        port,
        user,
        password,
        slot: slot.to_owned(),
        dir: std::path::PathBuf::from(dir),
        status_interval: Duration::from_secs(10),
    }
}

/// Native Rust WAL receiver loop. Replaces the pg_receivewal subprocess.
/// Retries with 2-second backoff on any error until SHUTDOWN is set.
fn run_native_receiver(cfg: wal_recv::ReceiverConfig, _dir: &str) {
    loop {
        if SHUTDOWN.load(Ordering::Acquire) {
            return;
        }
        match wal_recv::run_receiver(&cfg) {
            Ok(()) => eprintln!("wal receiver: connection closed cleanly"),
            Err(e) => eprintln!("wal receiver: {e}"),
        }
        if SHUTDOWN.load(Ordering::Acquire) {
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn run_http_server(listener: TcpListener, dir: &str, port: u16) {
    // Set a periodic wakeup on accept() so SHUTDOWN is checked promptly after
    // SIGTERM without waiting for the next connection to arrive.
    // On Linux: SO_RCVTIMEO makes accept(2) return EAGAIN after 200 ms.
    // Elsewhere: set_nonblocking(true) + sleep in the WouldBlock arm.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let tv = libc::timeval {
            tv_sec: 0,
            tv_usec: 200_000,
        };
        // SAFETY: listener fd is valid and outlives this call; tv is fully
        // initialized with tv_sec=0, tv_usec=200_000.
        let rc = unsafe {
            libc::setsockopt(
                listener.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVTIMEO,
                &tv as *const libc::timeval as *const libc::c_void,
                std::mem::size_of::<libc::timeval>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            eprintln!(
                "warn: SO_RCVTIMEO failed: {} — shutdown may be slow",
                std::io::Error::last_os_error()
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    listener.set_nonblocking(true).ok();

    eprintln!("listening on 0.0.0.0:{port}");

    loop {
        if SHUTDOWN.load(Ordering::Acquire) {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                // Atomically claim a slot before spawning. fetch_add returns
                // the previous value; if it was already at the limit, undo and
                // drop the connection (client sees a reset).
                if CONN_COUNT.fetch_add(1, Ordering::Relaxed) >= MAX_CONNS {
                    CONN_COUNT.fetch_sub(1, Ordering::Relaxed);
                    continue;
                }
                // On macOS, accepted sockets inherit O_NONBLOCK from the
                // listener. Reset to blocking so write_all doesn't silently
                // drop data when the TCP send buffer is temporarily full.
                #[cfg(not(target_os = "linux"))]
                if let Err(e) = stream.set_nonblocking(false) {
                    CONN_COUNT.fetch_sub(1, Ordering::Relaxed);
                    eprintln!("warn: set_nonblocking(false) failed: {e} — dropping connection");
                    continue;
                }
                // Bound how long a slow or stalled client can hold a handler
                // thread. Without a timeout, 256 slow clients exhaust the
                // pool and new connections are silently dropped.
                stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
                let dir = dir.to_owned();
                std::thread::spawn(move || {
                    handle_conn(stream, &dir);
                    CONN_COUNT.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                #[cfg(not(target_os = "linux"))]
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => eprintln!("accept: {e}"),
        }
    }
}

fn handle_conn(mut stream: TcpStream, dir: &str) {
    let mut buf = [0u8; 4096];
    let mut filled = 0usize;

    // Read until we have the full request headers (\r\n\r\n) or the buffer fills.
    // scan_from tracks how far we've already searched; each iteration only scans
    // new bytes plus a 3-byte overlap so the delimiter can span two reads.
    let mut scan_from = 0usize;
    loop {
        if filled >= buf.len() {
            break;
        }
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return,
            Ok(n) => filled += n,
            Err(_) => return,
        }
        let search_start = scan_from.saturating_sub(3);
        if buf[search_start..filled]
            .windows(4)
            .any(|w| w == b"\r\n\r\n")
        {
            break;
        }
        scan_from = filled;
    }

    let request_line_end = match buf[..filled].windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos,
        None => {
            let _ = stream.write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
            return;
        }
    };

    let request_line = match std::str::from_utf8(&buf[..request_line_end]) {
        Ok(s) => s,
        Err(_) => {
            let _ = stream.write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
            return;
        }
    };

    let mut parts = request_line.split_ascii_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    if method != "GET" {
        let _ = stream.write_all(
            b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        return;
    }

    match path {
        "/list" => serve_list(&mut stream, dir),
        p if p.len() == 25
            && p.starts_with('/')
            && p[1..].bytes().all(|b| b.is_ascii_hexdigit()) =>
        {
            serve_segment(&mut stream, dir, &p[1..]);
        }
        // Postgres timeline history files: /{8 hex digits}.history
        // e.g. /00000002.history — needed for restore_command to traverse
        // timeline boundaries after a failover/promotion.
        p if p.len() == 17
            && p.starts_with('/')
            && p.ends_with(".history")
            && p[1..9].bytes().all(|b| b.is_ascii_hexdigit()) =>
        {
            serve_segment(&mut stream, dir, &p[1..]);
        }
        _ => {
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    }
}

fn serve_list(stream: &mut TcpStream, dir: &str) {
    // Collect segment names as fixed [u8; 24] arrays — avoids one heap
    // allocation per filename that Vec<String> would incur.
    let mut names: Vec<[u8; 24]> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let raw = e.file_name();
            match raw.to_str() {
                Some(s) if s.len() == 24 && s.bytes().all(|b| b.is_ascii_hexdigit()) => {
                    let mut arr = [0u8; 24];
                    arr.copy_from_slice(s.as_bytes());
                    Some(arr)
                }
                Some(_) => None,
                None => {
                    eprintln!("warn: skipping non-UTF-8 filename in WAL dir: {raw:?}");
                    None
                }
            }
        })
        .collect();
    names.sort_unstable();

    // Build body in one allocation: N * 24 bytes + (N-1) newlines.
    let n = names.len();
    let body_len = if n == 0 { 0 } else { n * 25 - 1 };
    let mut body = Vec::with_capacity(body_len);
    for (i, name) in names.iter().enumerate() {
        body.extend_from_slice(name);
        if i + 1 < n {
            body.push(b'\n');
        }
    }

    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(&body);
}

fn write_404(stream: &mut TcpStream) {
    let _ = stream
        .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
}

fn serve_segment(stream: &mut TcpStream, dir: &str, name: &str) {
    // name is already validated as exactly 24 hex chars — no path traversal possible
    let path = format!("{dir}/{name}");

    let file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(_) => {
            write_404(stream);
            return;
        }
    };

    let len = match file.metadata() {
        Ok(m) => m.len(),
        Err(_) => {
            write_404(stream);
            return;
        }
    };

    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n"
    );
    let _ = stream.write_all(header.as_bytes());

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let mut offset: libc::off_t = 0;
        let mut remaining = len as libc::size_t;
        while remaining > 0 {
            // SAFETY: stream and file are valid open file descriptors owned by
            // this thread and remain valid for the duration of the call. offset
            // and remaining are stack-local variables passed by pointer.
            // sent > 0 is verified before casting ssize_t → size_t, preventing
            // sign confusion. WAL segments are ≤ 1 GiB so len fits in size_t
            // even on 32-bit targets.
            let sent = unsafe {
                libc::sendfile(stream.as_raw_fd(), file.as_raw_fd(), &mut offset, remaining)
            };
            if sent <= 0 {
                if sent < 0 {
                    let err = std::io::Error::last_os_error();
                    let raw = err.raw_os_error().unwrap_or(0);
                    if raw != libc::EPIPE && raw != libc::ECONNRESET {
                        eprintln!("sendfile: {err}");
                    }
                }
                break;
            }
            remaining -= sent as libc::size_t;
        }
        if remaining > 0 {
            eprintln!(
                "warn: sendfile incomplete for {name}: {remaining} bytes unsent (client will see truncated response)"
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut buf = [0u8; 65536];
        let mut f = file;
        loop {
            match f.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let _ = stream.write_all(&buf[..n]);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    // ---------------------------------------------------------------------------
    // HTTP handler helpers
    // ---------------------------------------------------------------------------

    /// Send `raw_request` to a fresh `handle_conn` call and return (status, body).
    fn test_http_conn(dir: &std::path::Path, raw_request: &[u8]) -> (u16, Vec<u8>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let dir_str = dir.to_str().unwrap().to_owned();
        let t = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_conn(stream, &dir_str);
        });
        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client.write_all(raw_request).unwrap();
        let mut response = Vec::new();
        let _ = client.read_to_end(&mut response);
        drop(client);
        t.join().unwrap();

        let header_end = response
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("no header terminator in response");
        let headers = std::str::from_utf8(&response[..header_end]).unwrap();
        let status: u16 = headers
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        (status, response[header_end + 4..].to_vec())
    }

    // ---------------------------------------------------------------------------
    // HTTP handler tests
    // ---------------------------------------------------------------------------

    #[test]
    fn http_list_empty_dir_returns_200_empty_body() {
        let dir = make_dir("http-list-empty");
        let (status, body) = test_http_conn(
            &dir,
            b"GET /list HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 200);
        assert!(body.is_empty(), "empty dir should produce empty /list body");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn http_list_returns_sorted_segment_names() {
        let dir = make_dir("http-list-sorted");
        write_segment(&dir, 1, 5);
        write_segment(&dir, 1, 3);
        write_segment(&dir, 1, 7);
        let (status, body) = test_http_conn(
            &dir,
            b"GET /list HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 200);
        let body_str = std::str::from_utf8(&body).unwrap();
        let names: Vec<&str> = body_str.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(names.len(), 3);
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(
            names, sorted,
            "/list must return segment names in sorted order"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn http_list_excludes_partial_files() {
        let dir = make_dir("http-list-partial");
        write_segment(&dir, 1, 0);
        let partial = format!("{:08X}{:08X}{:08X}.partial", 1u32, 0u32, 1u32);
        std::fs::write(dir.join(&partial), b"in-progress").unwrap();
        let (status, body) = test_http_conn(
            &dir,
            b"GET /list HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 200);
        let body_str = std::str::from_utf8(&body).unwrap();
        let names: Vec<&str> = body_str.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(names.len(), 1, "/list must not include .partial files");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn http_get_segment_returns_bytes() {
        let dir = make_dir("http-get-bytes");
        let name = write_segment(&dir, 1, 0);
        let expected = std::fs::read(dir.join(&name)).unwrap();
        let (status, body) = test_http_conn(
            &dir,
            format!("GET /{name} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body, expected,
            "segment response must match on-disk bytes exactly"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn http_get_missing_segment_returns_404() {
        let dir = make_dir("http-get-missing");
        let name = format!("{:08X}{:08X}{:08X}", 1u32, 0u32, 0u32);
        let (status, _) = test_http_conn(
            &dir,
            format!("GET /{name} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        );
        assert_eq!(status, 404);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn http_get_pruned_segment_returns_404() {
        let dir = make_dir("http-get-pruned");
        let name = write_segment(&dir, 1, 0);
        // Simulate the retention pruner deleting the segment before the GET.
        std::fs::remove_file(dir.join(&name)).unwrap();
        let (status, _) = test_http_conn(
            &dir,
            format!("GET /{name} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        );
        assert_eq!(status, 404);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn http_post_returns_405() {
        let dir = make_dir("http-post-405");
        let (status, _) = test_http_conn(
            &dir,
            b"POST /list HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 405);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn http_unknown_path_returns_404() {
        let dir = make_dir("http-unknown-404");
        let (status, _) = test_http_conn(
            &dir,
            b"GET /notapath HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 404);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---------------------------------------------------------------------------
    // Connection string parser tests
    // ---------------------------------------------------------------------------

    #[test]
    fn parse_connstr_kv_basic() {
        let cfg = parse_connstr_to_receiver_config(
            "host=10.0.0.1 port=5433 user=repuser",
            "/wal",
            "myslot",
        );
        assert_eq!(cfg.host, "10.0.0.1");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.user, "repuser");
        assert_eq!(cfg.slot, "myslot");
        assert_eq!(cfg.dir, std::path::PathBuf::from("/wal"));
    }

    #[test]
    fn parse_connstr_uri_basic() {
        let cfg =
            parse_connstr_to_receiver_config("postgres://repuser@10.0.0.1:5433/mydb", "/wal", "s");
        assert_eq!(cfg.host, "10.0.0.1");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.user, "repuser");
    }

    #[test]
    fn parse_connstr_defaults_on_empty() {
        let cfg = parse_connstr_to_receiver_config("", "/wal", "slot");
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.user, "postgres");
    }

    #[test]
    fn parse_connstr_kv_quoted_value() {
        let cfg =
            parse_connstr_to_receiver_config("host='my.host' port=5432 user=postgres", "/wal", "s");
        assert_eq!(cfg.host, "my.host");
    }

    fn make_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("wal-sink-retention-{tag}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_segment(dir: &std::path::Path, timeline: u32, seq: u32) -> String {
        let name = format!("{:08X}{:08X}{:08X}", timeline, 0u32, seq);
        std::fs::write(dir.join(&name), b"fake").unwrap();
        name
    }

    fn count_complete_segments(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
            .count()
    }

    #[test]
    fn retention_prunes_to_retain_count() {
        let dir = make_dir("prune-basic");
        for i in 0..40u32 {
            write_segment(&dir, 1, i);
        }
        prune_old_segments(dir.to_str().unwrap(), 32);
        assert_eq!(count_complete_segments(&dir), 32);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn retention_spares_partial_files() {
        let dir = make_dir("prune-partial");
        for i in 0..10u32 {
            write_segment(&dir, 1, i);
        }
        let partial = format!("{:08X}{:08X}{:08X}.partial", 1u32, 0u32, 10u32);
        std::fs::write(dir.join(&partial), b"in-progress").unwrap();

        prune_old_segments(dir.to_str().unwrap(), 5);

        assert!(
            dir.join(&partial).exists(),
            ".partial file was deleted by prune"
        );
        assert_eq!(count_complete_segments(&dir), 5);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn retention_prunes_timelines_independently() {
        let dir = make_dir("prune-timelines");
        for i in 0..20u32 {
            write_segment(&dir, 1, i);
            write_segment(&dir, 2, i);
        }
        prune_old_segments(dir.to_str().unwrap(), 10);

        let tl1 = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| {
                n.len() == 24
                    && n.bytes().all(|b| b.is_ascii_hexdigit())
                    && n.starts_with("00000001")
            })
            .count();
        let tl2 = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| {
                n.len() == 24
                    && n.bytes().all(|b| b.is_ascii_hexdigit())
                    && n.starts_with("00000002")
            })
            .count();

        assert_eq!(tl1, 10, "timeline 1: expected 10, got {tl1}");
        assert_eq!(tl2, 10, "timeline 2: expected 10, got {tl2}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn retention_prunes_newest_kept() {
        let dir = make_dir("prune-newest");
        for i in 0..20u32 {
            write_segment(&dir, 1, i);
        }
        prune_old_segments(dir.to_str().unwrap(), 10);

        // The 10 newest (seq 10..19) must survive; the 10 oldest (seq 0..9) must be gone.
        for i in 0..10u32 {
            let name = format!("{:08X}{:08X}{:08X}", 1u32, 0u32, i);
            assert!(
                !dir.join(&name).exists(),
                "old segment {name} should have been pruned"
            );
        }
        for i in 10..20u32 {
            let name = format!("{:08X}{:08X}{:08X}", 1u32, 0u32, i);
            assert!(
                dir.join(&name).exists(),
                "new segment {name} should be retained"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn retention_watcher_triggers_on_segment_completion() {
        let dir = make_dir("watcher-trigger");
        for i in 0..10u32 {
            write_segment(&dir, 1, i);
        }

        // Start the watcher. On Linux, give the inotify watch a moment to be
        // established before the rename fires. On non-Linux, use a short poll
        // interval so the test doesn't wait 30 seconds.
        #[cfg(target_os = "linux")]
        {
            let _ = start_retention_watcher(dir.to_str().unwrap().to_owned(), 5);
            std::thread::sleep(Duration::from_millis(50));
        }
        #[cfg(not(target_os = "linux"))]
        let _ = poll_retention_watcher(
            dir.to_str().unwrap().to_owned(),
            5,
            Duration::from_millis(50),
        );

        // Simulate pg_receivewal completing a segment: rename <name>.partial → <name>.
        let partial = format!("{:08X}{:08X}{:08X}.partial", 1u32, 0u32, 10u32);
        let complete = format!("{:08X}{:08X}{:08X}", 1u32, 0u32, 10u32);
        std::fs::write(dir.join(&partial), b"fake").unwrap();
        std::fs::rename(dir.join(&partial), dir.join(&complete)).unwrap();

        // Wait for the watcher thread to fire and prune (generous headroom).
        std::thread::sleep(Duration::from_millis(500));

        // 10 original + 1 new = 11 complete segments; watcher must prune to 5.
        assert_eq!(
            count_complete_segments(&dir),
            5,
            "watcher did not prune to retain=5"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn retention_noop_when_under_limit() {
        let dir = make_dir("prune-noop");
        for i in 0..5u32 {
            write_segment(&dir, 1, i);
        }
        prune_old_segments(dir.to_str().unwrap(), 32);
        assert_eq!(count_complete_segments(&dir), 5);
        std::fs::remove_dir_all(&dir).ok();
    }
}
