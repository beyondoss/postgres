//! Data-volume mounting for `beyond-pg-init`.
//!
//! instd attaches data volumes as block devices and records them in MMDS;
//! `beyond-pg-init` must mount them before the supervisor child is spawned so
//! postgres finds its data dir. The postgres data volume is recorded with
//! `path = /var/lib/postgresql`.
//!
//! Ported from guest-init's `mmds::read_attachments` + `mounts` (the canonical
//! mount-option behavior per storage class), adapted to beyond-pg-init's
//! `[init]` logging and made tolerant of a not-yet-present block device (a
//! brief retry) — a missing volume is logged FATAL but does not panic the VM,
//! so postgres can still surface the underlying error.
//!
//! Runs as root (PID 1) during [`crate::bootsetup::run`], after MMDS has been
//! fetched and before [`crate::supervise::run`] spawns the supervisor.

use std::path::Path;
use std::time::{Duration, Instant};

use nix::mount::{MsFlags, mount};
use serde_json::Value;

/// Max time to wait for a declared block device to appear before giving up.
const DEVICE_WAIT: Duration = Duration::from_millis(500);
const DEVICE_POLL: Duration = Duration::from_millis(50);

/// One data-volume attachment as written by instd into MMDS.
pub struct AttachmentMeta {
    pub volume_id: String,
    pub device: String,
    pub path: String,
    pub fstype: String,
    pub readonly: bool,
    pub storage_class: String,
}

/// Read volume attachments from the already-written MMDS metadata file and
/// mount each one. Best-effort: returns immediately if there are no volumes.
pub fn mount_from_mmds() {
    let attachments = read_attachments();
    if attachments.is_empty() {
        return;
    }
    eprintln!("[init] mounting {} data volume(s) from MMDS", attachments.len());
    mount_data_volumes(&attachments);
}

/// Read volume attachments from the MMDS metadata file written by
/// [`crate::bootsetup::fetch_mmds`].
///
/// Returns an empty vec if the file is absent, unparseable, or the
/// `volumes` array is missing — instances without data volumes see none.
fn read_attachments() -> Vec<AttachmentMeta> {
    let path = beyond_pg_core::mmds::MMDS_PATH;
    let raw = match std::fs::read(path) {
        Ok(b) => b,
        Err(_) => return vec![],
    };
    let val: Value = match serde_json::from_slice(&raw) {
        Ok(v) => v,
        Err(_) => return vec![],
    };
    parse_attachments(&val)
}

/// Parse the `latest.meta-data.volumes` array out of an MMDS value.
fn parse_attachments(val: &Value) -> Vec<AttachmentMeta> {
    let arr = match val["latest"]["meta-data"]["volumes"].as_array() {
        Some(a) => a,
        None => return vec![],
    };
    arr.iter()
        .filter_map(|v| {
            Some(AttachmentMeta {
                volume_id: v["volume_id"].as_str()?.to_string(),
                device: v["device"].as_str()?.to_string(),
                path: v["path"].as_str()?.to_string(),
                fstype: v["fstype"].as_str().unwrap_or("ext4").to_string(),
                readonly: v["readonly"].as_bool().unwrap_or(false),
                storage_class: v["storage_class"].as_str().unwrap_or("standard").to_string(),
            })
        })
        .collect()
}

/// Mount each attachment. Idempotent (skips already-mounted paths) and tolerant
/// of a transiently-absent block device (brief retry). A volume that can't be
/// mounted is logged FATAL but does NOT abort the VM, so postgres can still
/// start and surface the underlying error.
fn mount_data_volumes(attachments: &[AttachmentMeta]) {
    for a in attachments {
        if let Err(e) = mount_one(a) {
            eprintln!(
                "[init] FATAL: failed to mount data volume {} ({} → {}): {e}; continuing",
                a.volume_id, a.device, a.path
            );
        }
    }
}

fn mount_one(a: &AttachmentMeta) -> Result<(), String> {
    if !wait_for_device(&a.device) {
        return Err(format!("device {} not present after {DEVICE_WAIT:?}", a.device));
    }

    ensure_dir(&a.path)?;

    if is_mounted(&a.path, &a.device)? {
        eprintln!("[init] already mounted {}, skipping", a.path);
        return Ok(());
    }
    if let Some(other) = mounted_device(&a.path)? {
        return Err(format!(
            "path {} is already mounted with {} but expected {}",
            a.path, other, a.device
        ));
    }

    let (flags, opts) = mount_params(&a.storage_class, a.readonly);
    mount(
        Some(a.device.as_str()),
        a.path.as_str(),
        Some(a.fstype.as_str()),
        flags,
        Some(opts.as_str()),
    )
    .map_err(|e| format!("mount {} → {}: {}", a.device, a.path, e))?;

    eprintln!(
        "[init] mounted {} at {} ({}{})",
        a.device,
        a.path,
        opts,
        if a.readonly { ",ro" } else { "" }
    );
    Ok(())
}

/// Wait briefly for the block device to appear — instd attaches it around the
/// same time the guest boots, so it may not be visible the instant we look.
fn wait_for_device(device: &str) -> bool {
    let dev = Path::new(device);
    let deadline = Instant::now() + DEVICE_WAIT;
    loop {
        if dev.exists() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(DEVICE_POLL);
    }
}

/// Returns (mount flags, mount options string) for a given storage class.
///
/// The opts string is passed as the ext4-specific data argument to mount(2).
/// VFS-level flags (noatime, ro) MUST go in the flags bitfield — passing them
/// in the data string makes ext4 reject the mount with EINVAL. Mirrors
/// guest-init's `mounts::mount_params`.
fn mount_params(storage_class: &str, readonly: bool) -> (MsFlags, String) {
    let mut flags = MsFlags::MS_NOATIME;
    if readonly {
        flags |= MsFlags::MS_RDONLY;
    }
    let opts = match storage_class {
        "standard" => "commit=60",
        s if s.starts_with("database:") => "data=ordered,commit=30,barrier=1",
        "ephemeral" => "commit=120,nobarrier",
        "scratch" => "commit=120,nobarrier,data=writeback",
        _ => "commit=60",
    };
    (flags, opts.to_string())
}

/// Read `/proc/self/mountinfo` to check whether `path` is mounted with `device`.
fn is_mounted(path: &str, device: &str) -> Result<bool, String> {
    let info = read_mountinfo()?;
    Ok(info.iter().any(|(mp, dev)| mp == path && dev == device))
}

/// Return the device currently mounted at `path`, or `None` if nothing is.
fn mounted_device(path: &str) -> Result<Option<String>, String> {
    let info = read_mountinfo()?;
    Ok(info.into_iter().find(|(mp, _)| mp == path).map(|(_, dev)| dev))
}

fn read_mountinfo() -> Result<Vec<(String, String)>, String> {
    let content = std::fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| format!("read mountinfo: {e}"))?;
    Ok(parse_mountinfo(&content))
}

/// Parse `/proc/self/mountinfo` into `(mount_point, source)` pairs.
fn parse_mountinfo(content: &str) -> Vec<(String, String)> {
    let mut entries = Vec::new();
    for line in content.lines() {
        // Format: id parent major:minor root mount-point mount-options ... - fstype source ...
        // mount-point is field 4 (0-indexed); source is the 2nd token after "-".
        let fields: Vec<&str> = line.splitn(10, ' ').collect();
        if fields.len() < 5 {
            continue;
        }
        let mount_point = fields[4];
        let Some(dash_pos) = line.find(" - ") else {
            continue;
        };
        let after_dash: Vec<&str> = line[dash_pos + 3..].splitn(3, ' ').collect();
        if after_dash.len() < 2 {
            continue;
        }
        entries.push((mount_point.to_string(), after_dash[1].to_string()));
    }
    entries
}

fn ensure_dir(path: &str) -> Result<(), String> {
    std::fs::create_dir_all(path).map_err(|e| format!("create dir {path}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_attachments_postgres_volume() {
        let val = serde_json::json!({
            "latest": { "meta-data": { "volumes": [
                {
                    "volume_id": "vol-abc",
                    "device": "/dev/vdb",
                    "path": "/var/lib/postgresql",
                    "fstype": "ext4",
                    "readonly": false,
                    "storage_class": "database:postgres"
                }
            ] } }
        });
        let atts = parse_attachments(&val);
        assert_eq!(atts.len(), 1);
        let a = &atts[0];
        assert_eq!(a.volume_id, "vol-abc");
        assert_eq!(a.device, "/dev/vdb");
        assert_eq!(a.path, "/var/lib/postgresql");
        assert_eq!(a.fstype, "ext4");
        assert!(!a.readonly);
        assert_eq!(a.storage_class, "database:postgres");
    }

    #[test]
    fn parse_attachments_applies_defaults() {
        // fstype/readonly/storage_class omitted → defaults.
        let val = serde_json::json!({
            "latest": { "meta-data": { "volumes": [
                { "volume_id": "v", "device": "/dev/vdc", "path": "/data" }
            ] } }
        });
        let atts = parse_attachments(&val);
        assert_eq!(atts.len(), 1);
        assert_eq!(atts[0].fstype, "ext4");
        assert!(!atts[0].readonly);
        assert_eq!(atts[0].storage_class, "standard");
    }

    #[test]
    fn parse_attachments_skips_entries_missing_required_fields() {
        // device missing → entry dropped (filter_map on the `?`s).
        let val = serde_json::json!({
            "latest": { "meta-data": { "volumes": [
                { "volume_id": "v", "path": "/data" }
            ] } }
        });
        assert!(parse_attachments(&val).is_empty());
    }

    #[test]
    fn parse_attachments_empty_when_no_volumes_key() {
        let val = serde_json::json!({ "latest": { "meta-data": { "hostname": "pg" } } });
        assert!(parse_attachments(&val).is_empty());
    }

    #[test]
    fn parse_attachments_empty_on_garbage() {
        let val = serde_json::json!({ "anything": 1 });
        assert!(parse_attachments(&val).is_empty());
    }

    #[test]
    fn mount_params_database_uses_ordered_journaling() {
        let (flags, opts) = mount_params("database:postgres", false);
        assert!(flags.contains(MsFlags::MS_NOATIME));
        assert!(!flags.contains(MsFlags::MS_RDONLY));
        assert!(opts.contains("data=ordered"));
        assert!(opts.contains("commit=30"));
        assert!(opts.contains("barrier=1"));
    }

    #[test]
    fn mount_params_standard_and_readonly() {
        let (flags, opts) = mount_params("standard", true);
        assert!(flags.contains(MsFlags::MS_NOATIME));
        assert!(flags.contains(MsFlags::MS_RDONLY));
        assert!(opts.contains("commit=60"));
        assert!(!opts.contains("data=ordered"));
    }

    #[test]
    fn mount_params_unknown_falls_back_to_standard() {
        let (_flags, opts) = mount_params("weird_class", false);
        assert!(opts.contains("commit=60") && !opts.contains("data=ordered"));
    }

    #[test]
    fn parse_mountinfo_extracts_mountpoint_and_source() {
        let line = "36 35 8:1 / /var/lib/postgresql rw,noatime shared:1 - ext4 /dev/vdb rw,data=ordered";
        let entries = parse_mountinfo(line);
        assert_eq!(entries, vec![("/var/lib/postgresql".to_string(), "/dev/vdb".to_string())]);
    }

    #[test]
    fn parse_mountinfo_skips_malformed_lines() {
        // No " - " separator → skipped.
        assert!(parse_mountinfo("garbage without dash").is_empty());
    }
}
