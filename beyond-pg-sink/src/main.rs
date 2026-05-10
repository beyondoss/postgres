use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::time::Duration;

static SHUTDOWN: AtomicBool = AtomicBool::new(false);
static CHILD_PID: AtomicI32 = AtomicI32::new(0);
static CONN_COUNT: AtomicUsize = AtomicUsize::new(0);
const MAX_CONNS: usize = 256;

#[cfg(unix)]
extern "C" fn handle_sigterm(_: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Release);
}

struct Args {
    connstr: String,
    dir: String,
    port: u16,
    slot: String,
    retention_segments: usize,
}

fn parse_args() -> Args {
    let mut connstr: Option<String> = None;
    let mut dir = String::from("/var/lib/postgresql/wal-sink");
    let mut port: u16 = 9000;
    let mut slot = String::from("wal_sink");
    let mut retention_segments: usize = 64;

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
            Some(other) => {
                eprintln!("error: unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }

    let connstr = match connstr {
        Some(s) => s,
        None => {
            eprintln!("error: --connstr is required");
            std::process::exit(1);
        }
    };

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
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut decoded: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (from_hex(bytes[i + 1]), from_hex(bytes[i + 2])) {
                decoded.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        decoded.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

/// Strips `password=…` from a libpq connection string and returns it separately
/// so it can be passed via `PGPASSWORD` instead of appearing in process argv.
/// Supports both URI (`postgresql://user:pass@host/db`) and keyword=value formats.
fn extract_password(connstr: &str) -> (String, Option<String>) {
    for prefix in ["postgresql://", "postgres://"] {
        if let Some(rest) = connstr.strip_prefix(prefix) {
            if let Some(at) = rest.find('@') {
                let userinfo = &rest[..at];
                if let Some(colon) = userinfo.find(':') {
                    let password = percent_decode(&userinfo[colon + 1..]);
                    let cleaned = format!("{}{}{}", prefix, &userinfo[..colon], &rest[at..]);
                    return (cleaned, Some(password));
                }
            }
            return (connstr.to_owned(), None);
        }
    }
    strip_kv_password(connstr)
}

fn strip_kv_password(connstr: &str) -> (String, Option<String>) {
    let mut out = String::with_capacity(connstr.len());
    let mut password: Option<String> = None;
    let mut s = connstr;

    while !s.is_empty() {
        s = s.trim_start();
        let Some(eq) = s.find('=') else { break };
        let key = s[..eq].trim_end();
        s = &s[eq + 1..];
        let (value, consumed) = parse_kv_value(s);
        s = &s[consumed..];

        if key == "password" {
            password = Some(value);
        } else {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(key);
            out.push('=');
            if value.contains(char::is_whitespace) || value.is_empty() {
                out.push('\'');
                out.push_str(&value.replace('\\', "\\\\").replace('\'', "\\'"));
                out.push('\'');
            } else {
                out.push_str(&value);
            }
        }
    }

    (out, password)
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

    // Bind before spawning the pg_receivewal thread so that a bind failure
    // exits cleanly without orphaning a background subprocess.
    let listener = match TcpListener::bind(("0.0.0.0", args.port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: failed to bind 0.0.0.0:{}: {e}", args.port);
            std::process::exit(1);
        }
    };

    let (connstr_clean, password) = extract_password(&args.connstr);
    let dir_recv = args.dir.clone();
    let slot = args.slot.clone();

    let watcher_thread = start_retention_watcher(args.dir.clone(), args.retention_segments);

    let recv_thread = std::thread::spawn(move || {
        run_pg_receivewal(connstr_clean, dir_recv, slot, password);
    });

    run_http_server(listener, &args.dir, args.port);

    // Signal the child so it exits cleanly, then wait for the receiver thread.
    let pid = CHILD_PID.load(Ordering::Acquire);
    if pid != 0 {
        #[cfg(unix)]
        // SAFETY: pid was stored by run_pg_receivewal after a successful spawn and is
        // protected by the `!= 0` guard above. kill(2) with an already-exited PID
        // returns ESRCH harmlessly; no race can produce an invalid or recycled PID
        // because the receiver thread is still alive and will be joined below.
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
    }
    let _ = recv_thread.join();
    let _ = watcher_thread.join();
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
            // SAFETY: fd is a valid inotify file descriptor; buf is a mutable
            // byte slice of sufficient size for at least one inotify_event.
            let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    if SHUTDOWN.load(Ordering::Acquire) {
                        break;
                    }
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
    });
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

fn run_pg_receivewal(connstr: String, dir: String, slot: String, password: Option<String>) {
    // pg_receivewal --create-slot exits immediately after creating the slot and
    // does not stream. Run slot creation in its own retry loop first, then enter
    // the streaming loop below.
    loop {
        if SHUTDOWN.load(Ordering::Acquire) {
            return;
        }
        let mut cmd = std::process::Command::new("pg_receivewal");
        cmd.args([
            "--create-slot",
            "--if-not-exists",
            "--slot",
            &slot,
            "--dbname",
            &connstr,
        ]);
        if let Some(ref pw) = password {
            cmd.env("PGPASSWORD", pw);
        }
        match cmd.status() {
            Ok(s) if s.success() => break,
            Ok(s) => eprintln!("pg_receivewal --create-slot exited: {s}"),
            Err(e) => eprintln!("pg_receivewal --create-slot failed to launch: {e}"),
        }
        if SHUTDOWN.load(Ordering::Acquire) {
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    loop {
        if SHUTDOWN.load(Ordering::Acquire) {
            break;
        }

        let mut cmd = std::process::Command::new("pg_receivewal");
        cmd.args([
            "--synchronous",
            "--slot",
            &slot,
            "--directory",
            &dir,
            "--dbname",
            &connstr,
        ]);
        if let Some(ref pw) = password {
            cmd.env("PGPASSWORD", pw);
        }

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("pg_receivewal failed to launch: {e}");
                if !SHUTDOWN.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_secs(2));
                }
                continue;
            }
        };

        CHILD_PID.store(child.id() as i32, Ordering::Release);
        let status = child.wait();
        CHILD_PID.store(0, Ordering::Release);

        match status {
            Ok(s) => eprintln!("pg_receivewal exited: {s}"),
            Err(e) => eprintln!("pg_receivewal wait error: {e}"),
        }

        if SHUTDOWN.load(Ordering::Acquire) {
            break;
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
                if CONN_COUNT.load(Ordering::Relaxed) >= MAX_CONNS {
                    // Drop stream to close; client sees a reset.
                    continue;
                }
                // On macOS, accepted sockets inherit O_NONBLOCK from the
                // listener. Reset to blocking so write_all doesn't silently
                // drop data when the TCP send buffer is temporarily full.
                #[cfg(not(target_os = "linux"))]
                if let Err(e) = stream.set_nonblocking(false) {
                    eprintln!("warn: set_nonblocking(false) failed: {e} — dropping connection");
                    continue;
                }
                CONN_COUNT.fetch_add(1, Ordering::Relaxed);
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
    loop {
        if filled >= buf.len() {
            break;
        }
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return,
            Ok(n) => filled += n,
            Err(_) => return,
        }
        if buf[..filled].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
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
        _ => {
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    }
}

fn serve_list(stream: &mut TcpStream, dir: &str) {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
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
        .collect();
    names.sort_unstable();

    let body = names.join("\n");
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body.as_bytes());
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
    use std::time::Duration;

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
