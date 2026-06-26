//! One-shot PID 1 boot setup: mount filesystems, configure network, fetch
//! MMDS, set up zram. Ported from `beyond-pg`'s `src/init.rs`; sync only.
//!
//! Linux-only (mount(2), zram, MMDS via the Firecracker virtio-net path).
//! Module-level gating done at the `mod bootsetup;` declaration site.
// This module is a thin wrapper over libc mount(2) and TCP/IO; every `unsafe`
// block calls a syscall that must be invoked from unsafe. Each block carries
// its own `// SAFETY:` justification.
#![allow(unsafe_code)]

use std::ffi::CString;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const MMDS_ADDR: &str = "169.254.169.254:80";
const MMDS_MAX_ATTEMPTS: u32 = 30;
const MMDS_RETRY: Duration = Duration::from_millis(200);
// Firecracker's MMDS can take up to ~1–2s to answer the first request after
// boot on this substrate; a 200ms read timeout raced that and every attempt
// died with EAGAIN (the connect succeeds — it's the response that's slow), so
// the whole loop fell back to "POSTGRES_PASSWORD not set" and panicked. guest-
// init happens to win the race with its smaller VM; we must not depend on luck.
const HTTP_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_RESPONSE_BYTES: u64 = 64 * 1024;

/// Run the full PID 1 boot sequence. Bails (exit 1) on fatal failures.
pub fn run() {
    // SAFETY: `std::env::set_var` is `unsafe` as of Rust 1.81 because the
    // libc setenv it wraps is not thread-safe with respect to concurrent
    // getenv readers. This is the very first thing PID 1 does — no other
    // threads exist, and the supervisor child hasn't been spawned yet, so
    // no other code can observe the environment mid-mutation.
    //
    // postgres is at `/usr/lib/postgresql/18/bin/` on Debian-derived images
    // (the production rootfs and the docker test image both follow this).
    // `beyond-pg supervisor` does `Command::new("postgres")` which goes
    // through `execvp` PATH lookup — drop that dir from PATH and the
    // supervisor's first spawn fails with ENOENT. Keep it explicitly.
    unsafe {
        std::env::set_var(
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/postgresql/18/bin",
        );
    }
    mount_essential_filesystems();
    setup_network();
    fetch_mmds();
    // instd records attached data volumes in MMDS; mount them (as root, before
    // the supervisor child spawns) so postgres finds its data dir at
    // /var/lib/postgresql. Tolerant: no volumes → no-op; a missing device is
    // logged FATAL but never aborts the VM.
    crate::volumes::mount_from_mmds();
    setup_zram();
}

fn do_mount(src: &str, target: &str, fstype: &str, flags: libc::c_ulong) -> bool {
    let _ = std::fs::create_dir_all(target);
    // Infallible: callers pass hardcoded ASCII literals from the mount table
    // in `mount_essential_filesystems`; none contain interior NUL bytes.
    let src_c = CString::new(src).unwrap();
    let tgt_c = CString::new(target).unwrap();
    let fs_c = CString::new(fstype).unwrap();
    // SAFETY: src_c/tgt_c/fs_c are owned CStrings that live until end of scope,
    // so their pointers are valid for the duration of the call. `data` is null
    // which is permitted by mount(2) for all of the filesystems mounted here.
    let ret = unsafe {
        libc::mount(
            src_c.as_ptr(),
            tgt_c.as_ptr(),
            fs_c.as_ptr(),
            flags,
            std::ptr::null(),
        )
    };
    if ret == 0 {
        return true;
    }
    // EBUSY = already mounted (resume / re-exec case).
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EBUSY)
}

/// True if a filesystem is already mounted at this path (the outer init or
/// container runtime got there first). Used to make our mount attempts
/// idempotent in environments where we run as PID 1 but don't hold
/// CAP_SYS_ADMIN — Docker tests, dev containers, etc.
fn already_mounted(target: &str) -> bool {
    // /proc/mounts is the authoritative list. We may not have /proc/mounts
    // available (chicken-and-egg with /proc), so fall back to existence
    // of a path that only exists under a real mount of that fs.
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            // Each line: "<src> <target> <fstype> <opts> ..."
            let mut fields = line.split_whitespace();
            fields.next();
            if fields.next() == Some(target) {
                return true;
            }
        }
        return false;
    }
    // Fallbacks for the bootstrap case: /proc/self exists iff /proc is up.
    match target {
        "/proc" => std::path::Path::new("/proc/self").exists(),
        "/sys" => std::path::Path::new("/sys/kernel").exists(),
        "/dev" => std::path::Path::new("/dev/null").exists(),
        _ => false,
    }
}

fn mount_essential_filesystems() {
    let security = libc::MS_NOEXEC | libc::MS_NOSUID | libc::MS_NODEV;
    if !already_mounted("/proc") && !do_mount("proc", "/proc", "proc", security) {
        eprintln!("[init] FATAL: mount /proc failed");
        std::process::exit(1);
    }
    let mounts: &[(&str, &str, &str, libc::c_ulong)] = &[
        ("sysfs", "/sys", "sysfs", security),
        ("cgroup2", "/sys/fs/cgroup", "cgroup2", security),
        ("devtmpfs", "/dev", "devtmpfs", libc::MS_NOSUID),
        (
            "devpts",
            "/dev/pts",
            "devpts",
            libc::MS_NOEXEC | libc::MS_NOSUID,
        ),
        // postgres POSIX shared memory needs exec mappings.
        (
            "tmpfs",
            "/dev/shm",
            "tmpfs",
            libc::MS_NOSUID | libc::MS_NODEV,
        ),
        ("tmpfs", "/run", "tmpfs", security),
    ];
    for &(src, target, fstype, flags) in mounts {
        if already_mounted(target) {
            continue;
        }
        if !do_mount(src, target, fstype, flags) {
            eprintln!("[init] WARNING: mount {target} ({fstype}) failed");
        }
    }
}

/// Best-effort `ip` command invocation: logs a warning on failure but never
/// aborts (network commands are mostly idempotent and the daemon can still
/// boot without IPv6 / DNS being perfectly configured).
fn run_ip(args: &[&str], what: &str) {
    let status = std::process::Command::new("ip").args(args).status();
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => eprintln!("[init] WARNING: {what} failed (exit {:?})", s.code()),
        Err(e) => eprintln!("[init] WARNING: {what} failed: {e}"),
    }
}

fn setup_network() {
    if std::process::Command::new("ip")
        .args(["route", "add", "169.254.169.254", "dev", "eth0"])
        .status()
        .map(|s| !s.success())
        .unwrap_or(true)
    {
        eprintln!("[init] WARNING: failed to add MMDS route");
    }

    // `/proc/cmdline` is immutable for the VM's lifetime; read it once and
    // parse all the fields we care about from the same string.
    let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
    let ipv6 = parse_cmdline_ipv6(&cmdline);
    if let Some((ref addr, prefix_len, ref gw)) = ipv6 {
        // `nodad`: the address is unique per VM by construction, so Duplicate
        // Address Detection can never find a conflict — it only adds ~2.5s of
        // "tentative" state during which the kernel won't source packets from
        // the address. Since the IPv6 gateway is also the primary resolver
        // (resolv.conf below), DAD makes internal DNS unreachable for that
        // whole window, stalling early dependents by ~5s. Skip it.
        run_ip(
            &[
                "-6",
                "addr",
                "add",
                &format!("{addr}/{prefix_len}"),
                "dev",
                "eth0",
                "nodad",
            ],
            &format!("add IPv6 address {addr}/{prefix_len}"),
        );
        run_ip(
            &["-6", "route", "add", "default", "via", gw, "dev", "eth0"],
            &format!("add IPv6 default route via {gw}"),
        );
    }
    if let Some(ref gua) = parse_cmdline_ipv6_ext(&cmdline) {
        run_ip(
            &["-6", "addr", "add", &format!("{gua}/128"), "dev", "eth0", "nodad"],
            &format!("add IPv6 GUA {gua}/128"),
        );
    }

    let mut nameservers = String::new();
    if let Some((_, _, ref gw6)) = ipv6 {
        nameservers.push_str(&format!("nameserver {gw6}\n"));
    } else if let Some(gw4) = parse_cmdline_gateway(&cmdline) {
        nameservers.push_str(&format!("nameserver {gw4}\n"));
    }
    nameservers.push_str("nameserver 8.8.8.8\n");
    if let Err(e) = std::fs::write("/etc/resolv.conf", &nameservers) {
        eprintln!("[init] WARNING: failed to write resolv.conf: {e}");
    }
}

fn parse_cmdline_ipv6(cmdline: &str) -> Option<(String, u8, String)> {
    let param = cmdline
        .split_whitespace()
        .find(|s| s.starts_with("ipv6="))?
        .strip_prefix("ipv6=")?;
    let (addr_part, gateway) = param.split_once('@')?;
    let (addr, prefix_str) = addr_part.split_once('/')?;
    if gateway.is_empty() {
        return None;
    }
    Some((
        addr.to_string(),
        prefix_str.parse().ok()?,
        gateway.to_string(),
    ))
}

fn parse_cmdline_ipv6_ext(cmdline: &str) -> Option<String> {
    let param = cmdline
        .split_whitespace()
        .find(|s| s.starts_with("ipv6_ext="))?
        .strip_prefix("ipv6_ext=")?;
    let (addr, _) = param.split_once('/')?;
    if addr.is_empty() {
        None
    } else {
        Some(addr.to_string())
    }
}

fn parse_cmdline_gateway(cmdline: &str) -> Option<String> {
    let ip_param = cmdline
        .split_whitespace()
        .find(|s| s.starts_with("ip="))?
        .strip_prefix("ip=")?;
    let parts: Vec<&str> = ip_param.splitn(7, ':').collect();
    let gw = parts.get(2)?;
    if gw.is_empty() {
        None
    } else {
        Some(gw.to_string())
    }
}

fn fetch_mmds() {
    if let Err(e) = std::fs::create_dir_all("/run/mmds") {
        eprintln!("[init] FATAL: failed to create /run/mmds: {e}");
        std::process::exit(1);
    }
    // If `/run/mmds/metadata.json` is already present *and* parses as
    // valid JSON, treat it as authoritative and skip the HTTP fetch.
    // This makes init idempotent across crash-restarts and lets tests
    // pre-populate the file via a bind mount (Docker has no MMDS endpoint).
    let path = beyond_pg_core::mmds::MMDS_PATH;
    if let Ok(raw) = std::fs::read_to_string(path)
        && !raw.trim().is_empty()
        && serde_json::from_str::<serde_json::Value>(&raw).is_ok()
    {
        eprintln!("[init] MMDS file already present at {path}; skipping HTTP fetch");
        return;
    }
    match poll_mmds() {
        Ok(json) => write_mmds_file(&json),
        Err(()) => match std::env::var("POSTGRES_PASSWORD") {
            Ok(pw) => {
                let synthetic = serde_json::json!({
                    "latest": { "meta-data": { "POSTGRES_PASSWORD": pw } }
                });
                write_mmds_file(&synthetic);
            }
            Err(_) => {
                eprintln!("[init] FATAL: MMDS unavailable and POSTGRES_PASSWORD not set");
                std::process::exit(1);
            }
        },
    }
}

fn write_mmds_file(json: &serde_json::Value) {
    let path = beyond_pg_core::mmds::MMDS_PATH;
    let tmp = format!("{path}.tmp");
    // serializing an in-memory serde_json::Value is infallible; degrading to
    // an empty string would leave a valid-but-empty MMDS file that the
    // supervisor would silently fail to parse credentials from.
    let content =
        serde_json::to_string_pretty(json).expect("serde_json::Value -> string is infallible");
    if std::fs::write(&tmp, &content)
        .and_then(|_| std::fs::rename(&tmp, path))
        .is_err()
    {
        eprintln!("[init] FATAL: failed to write MMDS metadata file");
        std::process::exit(1);
    }
}

fn poll_mmds() -> Result<serde_json::Value, ()> {
    let token = get_mmds_token();
    for attempt in 1..=MMDS_MAX_ATTEMPTS {
        let t0 = std::time::Instant::now();
        match fetch_mmds_metadata(token.as_deref()) {
            Ok(Some(data)) => {
                eprintln!("[init] MMDS data available (attempt {attempt})");
                return Ok(data);
            }
            Ok(None) => {}
            Err(e) => eprintln!(
                "[init] MMDS fetch attempt {attempt} failed after {}ms: {e}",
                t0.elapsed().as_millis()
            ),
        }
        std::thread::sleep(MMDS_RETRY);
    }
    let total_ms = MMDS_MAX_ATTEMPTS as u128 * MMDS_RETRY.as_millis();
    eprintln!(
        "[init] WARNING: MMDS unavailable after {MMDS_MAX_ATTEMPTS} attempts ({total_ms}ms total); falling back"
    );
    Err(())
}

fn get_mmds_token() -> Option<String> {
    let request = concat!(
        "PUT /latest/api/token HTTP/1.1\r\n",
        "Host: 169.254.169.254\r\n",
        "X-metadata-token-ttl-seconds: 300\r\n",
        "Content-Length: 0\r\n",
        "Connection: close\r\n",
        "\r\n"
    );
    let response = http_roundtrip(request.as_bytes()).ok()?;
    if http_status(&response)? != 200 {
        return None;
    }
    let body = http_body(&response)?.trim();
    // The token is interpolated into `X-metadata-token: {body}\r\n`. Any
    // control byte (CR/LF most obviously, but also NUL/tab/etc.) or
    // non-ASCII char would either split the header or be silently
    // re-encoded — refuse the token entirely rather than try to sanitize
    // it. Real MMDS tokens are short base64-ish ASCII; anything else is a
    // bug in the MMDS or an attempt to inject.
    if body.is_empty() || !body.chars().all(|c| c.is_ascii_graphic()) {
        return None;
    }
    Some(body.to_string())
}

fn fetch_mmds_metadata(
    token: Option<&str>,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let mut request =
        String::from("GET / HTTP/1.1\r\nHost: 169.254.169.254\r\nAccept: application/json\r\n");
    if let Some(t) = token {
        request.push_str("X-metadata-token: ");
        request.push_str(t);
        request.push_str("\r\n");
    }
    request.push_str("Connection: close\r\n\r\n");
    let response = http_roundtrip(request.as_bytes())?;
    match http_status(&response) {
        Some(200) => {}
        Some(status) => return Err(format!("MMDS returned HTTP {status}").into()),
        None => return Err("MMDS returned malformed HTTP response".into()),
    }
    let body = http_body(&response).unwrap_or_default();
    if body.is_empty() || body.trim() == "latest/" {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(body)?))
}

fn http_roundtrip(request: &[u8]) -> std::io::Result<Vec<u8>> {
    // connect_timeout (non-blocking connect + poll) handles the post-boot window
    // where the eth0 link-local neighbor isn't resolved yet.
    let addr: std::net::SocketAddr = MMDS_ADDR.parse().expect("MMDS_ADDR is a literal");
    let mut stream = TcpStream::connect_timeout(&addr, HTTP_TIMEOUT)?;
    stream.set_write_timeout(Some(HTTP_TIMEOUT))?;
    stream.set_read_timeout(Some(HTTP_TIMEOUT))?;
    stream.write_all(request)?;

    // Read headers, then exactly `Content-Length` body bytes — do NOT read to
    // EOF. Firecracker's MMDS keeps the TCP connection OPEN after the response
    // (it ignores `Connection: close`), so there is no EOF; `read_to_end` blocks
    // for the full read timeout on every request and surfaces as EAGAIN. This is
    // the same Content-Length-bounded read guest-init's MMDS client uses.
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if let Some(end) = find_headers_end(&buf) {
            if let Some(len) = content_length(&buf[..end]) {
                if buf.len() >= end + len {
                    buf.truncate(end + len);
                    break;
                }
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) => break, // EOF (Connection: close honored)
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => return Err(e), // timeout / error — surface to the retry loop
        }
        if buf.len() as u64 > MAX_RESPONSE_BYTES {
            break;
        }
    }
    Ok(buf)
}

/// Byte offset just past the `\r\n\r\n` header terminator, if fully buffered.
fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

/// Parse the `Content-Length` header (case-insensitive) from a header block.
fn content_length(headers: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(headers).ok()?;
    text.split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse().ok())
}

fn http_status(response: &[u8]) -> Option<u16> {
    let line = response.split(|&b| b == b'\n').next()?;
    let s = std::str::from_utf8(line).ok()?;
    s.split_whitespace().nth(1)?.parse().ok()
}

fn http_body(response: &[u8]) -> Option<&str> {
    let sep = response.windows(4).position(|w| w == b"\r\n\r\n")?;
    std::str::from_utf8(&response[sep + 4..]).ok()
}

fn setup_zram() {
    if std::fs::read_to_string("/proc/swaps")
        .unwrap_or_default()
        .contains("zram0")
    {
        return;
    }
    if !std::process::Command::new("modprobe")
        .arg("zram")
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        eprintln!("[init] WARNING: modprobe zram failed");
        return;
    }
    if let Ok(algos) = std::fs::read_to_string("/sys/block/zram0/comp_algorithm")
        && algos.contains("lz4")
    {
        let _ = std::fs::write("/sys/block/zram0/comp_algorithm", "lz4");
    }
    let mem_kib =
        beyond_pg_core::mmds::read_proc_meminfo_kib("MemTotal").unwrap_or(4 * 1024 * 1024);
    let size = (mem_kib * 1024 / 10).min(256 * 1024 * 1024);
    if std::fs::write("/sys/block/zram0/disksize", size.to_string()).is_err() {
        eprintln!("[init] WARNING: failed to set zram disksize");
        return;
    }
    if !std::process::Command::new("mkswap")
        .arg("/dev/zram0")
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        eprintln!("[init] WARNING: mkswap /dev/zram0 failed");
        return;
    }
    if !std::process::Command::new("swapon")
        .args(["-p", "100", "/dev/zram0"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        eprintln!("[init] WARNING: swapon /dev/zram0 failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv6_cmdline_parses_addr_prefix_and_gateway() {
        let cmd = "console=ttyS0 ipv6=fd00::2/64@fd00::1 root=/dev/vda ro";
        let (addr, prefix, gw) = parse_cmdline_ipv6(cmd).expect("ipv6 parsed");
        assert_eq!(addr, "fd00::2");
        assert_eq!(prefix, 64);
        assert_eq!(gw, "fd00::1");
    }

    #[test]
    fn ipv6_cmdline_rejects_missing_pieces() {
        // missing gateway
        assert!(parse_cmdline_ipv6("ipv6=fd00::2/64").is_none());
        // missing prefix
        assert!(parse_cmdline_ipv6("ipv6=fd00::2@fd00::1").is_none());
        // empty gateway after '@'
        assert!(parse_cmdline_ipv6("ipv6=fd00::2/64@").is_none());
        // non-numeric prefix
        assert!(parse_cmdline_ipv6("ipv6=fd00::2/abc@fd00::1").is_none());
        // parameter absent entirely
        assert!(parse_cmdline_ipv6("console=ttyS0").is_none());
    }

    #[test]
    fn ipv6_ext_cmdline_returns_only_addr() {
        assert_eq!(
            parse_cmdline_ipv6_ext("ipv6_ext=2001:db8::1/128"),
            Some("2001:db8::1".to_string())
        );
        // The prefix is discarded — only the address matters for an alias.
        assert_eq!(
            parse_cmdline_ipv6_ext("foo ipv6_ext=2001:db8::5/64 bar"),
            Some("2001:db8::5".to_string())
        );
    }

    #[test]
    fn ipv6_ext_cmdline_rejects_malformed() {
        assert!(parse_cmdline_ipv6_ext("ipv6_ext=").is_none());
        assert!(parse_cmdline_ipv6_ext("ipv6_ext=/64").is_none());
        assert!(parse_cmdline_ipv6_ext("ipv6_ext=2001:db8::1").is_none());
        assert!(parse_cmdline_ipv6_ext("nothing here").is_none());
    }

    #[test]
    fn gateway_cmdline_parses_third_field() {
        // ip=<client>:<server>:<gw>:<netmask>:<host>:<dev>:<proto>
        let cmd = "ip=10.0.0.2:10.0.0.1:10.0.0.254:255.255.255.0::eth0:none";
        assert_eq!(parse_cmdline_gateway(cmd), Some("10.0.0.254".to_string()));
    }

    #[test]
    fn gateway_cmdline_rejects_empty_or_missing() {
        assert!(parse_cmdline_gateway("ip=10.0.0.2:10.0.0.1::255.255.255.0").is_none());
        assert!(parse_cmdline_gateway("ip=10.0.0.2").is_none());
        assert!(parse_cmdline_gateway("console=ttyS0").is_none());
    }

    #[test]
    fn http_status_parses_well_formed_response() {
        let resp = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}";
        assert_eq!(http_status(resp), Some(200));
        let resp = b"HTTP/1.1 404 Not Found\r\n\r\n";
        assert_eq!(http_status(resp), Some(404));
    }

    #[test]
    fn http_status_returns_none_for_malformed() {
        assert_eq!(http_status(b""), None);
        assert_eq!(http_status(b"garbage\r\n\r\n"), None);
        assert_eq!(http_status(b"HTTP/1.1\r\n\r\n"), None);
    }

    #[test]
    fn http_body_returns_text_after_crlfcrlf() {
        let resp = b"HTTP/1.1 200 OK\r\nX: y\r\n\r\nhello body";
        assert_eq!(http_body(resp), Some("hello body"));
    }

    #[test]
    fn http_body_returns_none_without_separator() {
        let resp = b"HTTP/1.1 200 OK\r\nX: y\r\n";
        assert_eq!(http_body(resp), None);
    }

    #[test]
    fn http_body_empty_when_separator_at_end() {
        let resp = b"HTTP/1.1 200 OK\r\n\r\n";
        assert_eq!(http_body(resp), Some(""));
    }
}
