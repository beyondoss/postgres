//! Child-process PID persistence and pidfd-based adoption.
//!
//! On every successful spawn, the supervisor writes a state file mapping
//! `child_name → (pid, starttime)`. A new supervisor (post-handoff or
//! post-crash) reads that file, calls `pidfd_open(2)`, and verifies the
//! `starttime` matches before claiming the pid.
//!
//! Starttime (`/proc/<pid>/stat` field 22, jiffies-since-boot) is the
//! standard guard against pid recycling: a different process at the same pid
//! has a different start time. systemd-style.
//!
//! The state file lives outside `PGDATA` because it tracks beyond-pg's view
//! of its children, not postgres' durable state.
//!
//! Linux-only (pidfd_open). Module-level gating done at the `mod children;`
//! declaration sites in `main.rs` and `lib.rs`.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const STATE_DIR: &str = "/var/lib/beyond-pg/state";
const STATE_FILE: &str = "children.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildRecord {
    pub pid: u32,
    pub starttime: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PersistedChildren {
    pub version: u32,
    #[serde(flatten)]
    pub children: BTreeMap<String, ChildRecord>,
}

impl PersistedChildren {
    pub fn empty() -> Self {
        Self {
            version: 1,
            children: BTreeMap::new(),
        }
    }

    #[allow(dead_code)] // wired in phase 5b (successor reads on startup)
    pub fn load(state_dir: &Path) -> std::io::Result<Self> {
        let path = state_dir.join(STATE_FILE);
        let raw = std::fs::read_to_string(&path)?;
        let parsed: Self = serde_json::from_str(&raw)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(parsed)
    }

    /// Write atomically: temp file in the same dir, fsync, rename.
    /// Same-dir requirement is non-negotiable — rename(2) is only atomic
    /// when source and destination are on the same filesystem.
    pub fn save(&self, state_dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(state_dir)?;
        let final_path = state_dir.join(STATE_FILE);
        let tmp_path = state_dir.join(format!("{STATE_FILE}.tmp"));
        let json = serde_json::to_vec_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        {
            let mut f = File::create(&tmp_path)?;
            f.write_all(&json)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    pub fn record(&mut self, name: &str, pid: u32) -> std::io::Result<()> {
        let starttime = read_starttime(pid)?;
        self.record_with_starttime(name, pid, starttime)
    }

    /// Insert a pid + caller-provided starttime. Useful for tests that
    /// need to lock in the starttime before a potential race with reap.
    pub fn record_with_starttime(
        &mut self,
        name: &str,
        pid: u32,
        starttime: u64,
    ) -> std::io::Result<()> {
        self.children
            .insert(name.to_string(), ChildRecord { pid, starttime });
        Ok(())
    }

    #[allow(dead_code)] // wired in phase 5b
    pub fn remove(&mut self, name: &str) {
        self.children.remove(name);
    }

    #[allow(dead_code)] // wired in phase 5b
    pub fn get(&self, name: &str) -> Option<&ChildRecord> {
        self.children.get(name)
    }
}

/// Outcome of attempting to adopt a persisted pid.
#[allow(dead_code)] // wired up in phase 5b (successor adoption)
#[derive(Debug)]
pub enum AdoptResult {
    /// Successfully opened pidfd for a process whose starttime matches.
    Adopted(OwnedFd),
    /// The pid no longer corresponds to any live process.
    Dead,
    /// The pid is in use, but starttime differs — pid was recycled.
    Recycled { saved: u64, live: u64 },
}

/// Try to adopt a previously-spawned child via `pidfd_open(2)` + starttime check.
#[allow(dead_code)] // wired up in phase 5b (successor adoption)
pub fn adopt(record: &ChildRecord) -> std::io::Result<AdoptResult> {
    let live_starttime = match read_starttime(record.pid) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(AdoptResult::Dead),
        Err(e) => return Err(e),
    };
    if live_starttime != record.starttime {
        return Ok(AdoptResult::Recycled {
            saved: record.starttime,
            live: live_starttime,
        });
    }
    let fd = pidfd_open(record.pid as i32)?;
    Ok(AdoptResult::Adopted(fd))
}

/// Read `/proc/<pid>/stat` field 22 (start time in clock ticks since boot).
///
/// Robust against process names containing spaces or parens: parses from the
/// last `)` (closes the comm field), then splits the remainder by whitespace.
pub fn read_starttime(pid: u32) -> std::io::Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "no `)` in stat"))?;
    let after = stat
        .get(close + 2..)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "stat truncated"))?;
    let fields: Vec<&str> = after.split_whitespace().collect();
    // After ") ", field 3 (state) is idx 0; field 22 (starttime) is idx 19.
    fields
        .get(19)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "stat too short"))?
        .parse::<u64>()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Read `/proc/<pid>/stat` CPU time = field 14 (utime) + field 15 (stime), in
/// clock ticks. Used by the PgBouncer scaler to sample per-worker CPU: sample
/// twice, delta / SC_CLK_TCK / elapsed_secs = cores consumed.
///
/// Same robust parse as [`read_starttime`]: split after the last `)` so a comm
/// with spaces/parens can't shift the field indices. After `") "`, field 3
/// (state) is idx 0, so utime (field 14) is idx 11 and stime (field 15) is idx 12.
pub fn read_proc_cpu_ticks(pid: u32) -> std::io::Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "no `)` in stat"))?;
    let after = stat
        .get(close + 2..)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "stat truncated"))?;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let parse = |idx: usize| -> std::io::Result<u64> {
        fields
            .get(idx)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "stat too short"))?
            .parse::<u64>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    };
    Ok(parse(11)? + parse(12)?)
}

#[allow(dead_code)] // exercised via tests + phase 5b adopt path
fn pidfd_open(pid: i32) -> std::io::Result<OwnedFd> {
    // SAFETY: SYS_pidfd_open with flags=0 returns either a new owned fd or
    // -errno via raw syscall. We immediately wrap a valid fd in OwnedFd so
    // the kernel resource is closed via Drop.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if raw < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(raw as RawFd) })
}

/// Send a signal to a process via its pidfd. Immune to PID-recycling races
/// because pidfds reference a specific process instance, not a pid number.
#[allow(dead_code)] // wired up in phase 5b (successor adoption / SIGHUP)
pub fn pidfd_send_signal(fd: &OwnedFd, sig: libc::c_int) -> std::io::Result<()> {
    let r = unsafe { libc::syscall(libc::SYS_pidfd_send_signal, fd.as_raw_fd(), sig, 0, 0) };
    if r < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Convenience: the default state directory as a `PathBuf`.
pub fn state_dir() -> PathBuf {
    PathBuf::from(STATE_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_save_load() {
        let dir = tempfile::tempdir().unwrap();
        let mut pc = PersistedChildren::empty();
        pc.children.insert(
            "postgres".into(),
            ChildRecord {
                pid: 1234,
                starttime: 56789,
            },
        );
        pc.save(dir.path()).unwrap();
        let loaded = PersistedChildren::load(dir.path()).unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.children.len(), 1);
        let r = loaded.get("postgres").unwrap();
        assert_eq!(r.pid, 1234);
        assert_eq!(r.starttime, 56789);
    }

    #[test]
    fn save_is_atomic_via_tmp_file() {
        // After save, no .tmp file should remain.
        let dir = tempfile::tempdir().unwrap();
        let pc = PersistedChildren::empty();
        pc.save(dir.path()).unwrap();
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(
            entries
                .iter()
                .all(|name| !name.to_string_lossy().ends_with(".tmp")),
            "{:?}",
            entries
        );
    }

    #[test]
    fn read_starttime_for_self() {
        let st = read_starttime(std::process::id()).unwrap();
        assert!(st > 0);
    }

    #[test]
    fn adopt_dead_pid_returns_dead() {
        // Spawn and immediately wait so we know the pid is gone.
        let child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        let _ = child.wait_with_output();
        // Give the kernel a moment to fully reap.
        std::thread::sleep(std::time::Duration::from_millis(20));
        let record = ChildRecord {
            pid,
            starttime: 1, // wouldn't match anyway
        };
        match adopt(&record).unwrap() {
            AdoptResult::Dead => {}
            other => panic!("expected Dead, got {other:?}"),
        }
    }

    #[test]
    fn adopt_self_succeeds() {
        let pid = std::process::id();
        let starttime = read_starttime(pid).unwrap();
        let record = ChildRecord { pid, starttime };
        match adopt(&record).unwrap() {
            AdoptResult::Adopted(_fd) => {}
            other => panic!("expected Adopted, got {other:?}"),
        }
    }

    #[test]
    fn adopt_with_wrong_starttime_reports_recycled() {
        let pid = std::process::id();
        let record = ChildRecord {
            pid,
            starttime: u64::MAX, // definitely not the real starttime
        };
        match adopt(&record).unwrap() {
            AdoptResult::Recycled { saved, live } => {
                assert_eq!(saved, u64::MAX);
                assert!(live < u64::MAX);
            }
            other => panic!("expected Recycled, got {other:?}"),
        }
    }
}
