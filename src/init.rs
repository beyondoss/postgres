pub fn run() {
    #[cfg(target_os = "linux")]
    linux::run();
}

#[cfg(target_os = "linux")]
mod linux {
    use std::ffi::CString;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    const MMDS_ADDR: &str = "169.254.169.254:80";
    const MMDS_MAX_ATTEMPTS: u32 = 30;
    const MMDS_RETRY: Duration = Duration::from_millis(10);
    const HTTP_TIMEOUT: Duration = Duration::from_millis(200);
    const MAX_RESPONSE_BYTES: u64 = 64 * 1024;

    pub fn run() {
        if std::process::id() != 1 {
            return;
        }
        // SAFETY: single-threaded, no other threads reading env yet.
        unsafe {
            std::env::set_var(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            );
        }
        mount_essential_filesystems();
        setup_network();
        fetch_mmds();
        setup_zram();
    }

    fn do_mount(src: &str, target: &str, fstype: &str, flags: libc::c_ulong) -> bool {
        let _ = std::fs::create_dir_all(target);
        let src_c = CString::new(src).unwrap();
        let tgt_c = CString::new(target).unwrap();
        let fs_c = CString::new(fstype).unwrap();
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
        // EBUSY = already mounted — valid on resume
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EBUSY)
    }

    fn mount_essential_filesystems() {
        let security = libc::MS_NOEXEC | libc::MS_NOSUID | libc::MS_NODEV;
        if !do_mount("proc", "/proc", "proc", security) {
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
            // Postgres POSIX shared memory — must allow executable mappings (no MS_NOEXEC)
            (
                "tmpfs",
                "/dev/shm",
                "tmpfs",
                libc::MS_NOSUID | libc::MS_NODEV,
            ),
            ("tmpfs", "/run", "tmpfs", security),
        ];
        for &(src, target, fstype, flags) in mounts {
            if !do_mount(src, target, fstype, flags) {
                eprintln!("[init] WARNING: mount {target} ({fstype}) failed");
            }
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

        let ipv6 = read_cmdline_ipv6();
        if let Some((ref addr, prefix_len, ref gw)) = ipv6 {
            let _ = std::process::Command::new("ip")
                .args([
                    "-6",
                    "addr",
                    "add",
                    &format!("{addr}/{prefix_len}"),
                    "dev",
                    "eth0",
                ])
                .status();
            let _ = std::process::Command::new("ip")
                .args(["-6", "route", "add", "default", "via", gw, "dev", "eth0"])
                .status();
        }
        if let Some(ref gua) = read_cmdline_ipv6_ext() {
            let _ = std::process::Command::new("ip")
                .args(["-6", "addr", "add", &format!("{gua}/128"), "dev", "eth0"])
                .status();
        }

        let mut nameservers = String::new();
        if let Some((_, _, ref gw6)) = ipv6 {
            nameservers.push_str(&format!("nameserver {gw6}\n"));
        } else if let Some(gw4) = read_cmdline_gateway() {
            nameservers.push_str(&format!("nameserver {gw4}\n"));
        }
        nameservers.push_str("nameserver 8.8.8.8\n");
        if let Err(e) = std::fs::write("/etc/resolv.conf", &nameservers) {
            eprintln!("[init] WARNING: failed to write resolv.conf: {e}");
        }
    }

    fn read_cmdline_ipv6() -> Option<(String, u8, String)> {
        let cmdline = std::fs::read_to_string("/proc/cmdline").ok()?;
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

    fn read_cmdline_ipv6_ext() -> Option<String> {
        let cmdline = std::fs::read_to_string("/proc/cmdline").ok()?;
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

    fn read_cmdline_gateway() -> Option<String> {
        let cmdline = std::fs::read_to_string("/proc/cmdline").ok()?;
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
        let tmp = format!("{}.tmp", crate::mmds::MMDS_PATH);
        let content = serde_json::to_string_pretty(json).unwrap_or_default();
        if std::fs::write(&tmp, &content)
            .and_then(|_| std::fs::rename(&tmp, crate::mmds::MMDS_PATH))
            .is_err()
        {
            eprintln!("[init] FATAL: failed to write MMDS metadata file");
            std::process::exit(1);
        }
    }

    fn poll_mmds() -> Result<serde_json::Value, ()> {
        let token = get_mmds_token();
        for attempt in 1..=MMDS_MAX_ATTEMPTS {
            match fetch_mmds_metadata(token.as_deref()) {
                Ok(Some(data)) => {
                    eprintln!("[init] MMDS data available (attempt {attempt})");
                    return Ok(data);
                }
                Ok(None) => {}
                Err(e) => eprintln!("[init] MMDS fetch attempt {attempt} failed: {e}"),
            }
            std::thread::sleep(MMDS_RETRY);
        }
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
        let body = http_body(&response)?.trim().to_string();
        // Strip CRLF to prevent header injection when used as a request header value.
        let body: String = body.chars().filter(|&c| c != '\r' && c != '\n').collect();
        if body.is_empty() { None } else { Some(body) }
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
        let mut stream = TcpStream::connect(MMDS_ADDR)?;
        stream.set_write_timeout(Some(HTTP_TIMEOUT))?;
        stream.set_read_timeout(Some(HTTP_TIMEOUT))?;
        stream.write_all(request)?;
        let mut buf = Vec::new();
        let _ = stream.take(MAX_RESPONSE_BYTES).read_to_end(&mut buf);
        Ok(buf)
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

        if std::process::Command::new("modprobe")
            .arg("zram")
            .status()
            .is_err()
        {
            eprintln!("[init] WARNING: modprobe zram failed");
            return;
        }

        if let Ok(algos) = std::fs::read_to_string("/sys/block/zram0/comp_algorithm") {
            if algos.contains("lz4") {
                let _ = std::fs::write("/sys/block/zram0/comp_algorithm", "lz4");
            }
        }

        let mem_kib = read_proc_meminfo_kib("MemTotal").unwrap_or(4 * 1024 * 1024);
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

    fn read_proc_meminfo_kib(field: &str) -> Option<u64> {
        let content = std::fs::read_to_string("/proc/meminfo").ok()?;
        for line in content.lines() {
            if line.starts_with(field) {
                return line.split_whitespace().nth(1)?.parse::<u64>().ok();
            }
        }
        None
    }
}
