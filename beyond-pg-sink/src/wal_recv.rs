//! Postgres physical WAL streaming receiver.
//!
//! Wire protocol primitives live in the `wal-proto` workspace crate so the
//! forwarder (in `beyond-pg`) can share them. This module only contains the
//! sink-specific pieces: the WAL segment writer and the receive loop.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub use wal_proto::{
    Lsn, RecvError, WalMsg, connect, create_slot_if_not_exists, highest_local_lsn, identify_system,
    recv_wal, send_status, start_replication,
};

/// Physical WAL segment size: 16 MiB (Postgres default, compile-time constant).
const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

// ---------------------------------------------------------------------------
// WAL segment writer
// ---------------------------------------------------------------------------

/// Segment name from LSN.
///
/// Format: `{timeline:08X}{segment_hi:08X}{segment_lo:08X}`
/// where segment = lsn / WAL_SEGMENT_SIZE.
fn segment_name(lsn: Lsn, timeline: u32) -> String {
    let segno = lsn.0 / WAL_SEGMENT_SIZE;
    let seg_hi = (segno >> 32) as u32;
    let seg_lo = (segno & 0xFFFF_FFFF) as u32;
    format!("{timeline:08X}{seg_hi:08X}{seg_lo:08X}")
}

/// Manages writing a stream of raw WAL bytes to segment files on disk.
pub struct WalWriter {
    dir: PathBuf,
    timeline: u32,
    /// Open `.partial` file for the current segment, plus its path.
    current: Option<(File, PathBuf)>,
    /// Byte offset within the current segment file.
    offset: u64,
    /// Highest LSN written to OS buffer (for `write_lsn` in status updates).
    pub write_lsn: Lsn,
    /// Highest LSN fsynced to disk (for `flush_lsn` in status updates).
    pub flush_lsn: Lsn,
}

impl WalWriter {
    pub fn new(dir: &Path, timeline: u32) -> Self {
        WalWriter {
            dir: dir.to_owned(),
            timeline,
            current: None,
            offset: 0,
            write_lsn: Lsn::ZERO,
            flush_lsn: Lsn::ZERO,
        }
    }

    /// Update the replication timeline. Must be called before the first
    /// `write()` in QUIC mode, where the timeline is received via the
    /// preamble hello frame rather than an `IDENTIFY_SYSTEM` call.
    pub fn set_timeline(&mut self, timeline: u32) {
        self.timeline = timeline;
    }

    /// Write `data` starting at `start_lsn`. Splits across segment boundaries
    /// automatically. Calls `fdatasync` after each segment's worth of writes
    /// and renames the segment file to its final name on completion.
    pub fn write(&mut self, start_lsn: Lsn, data: &[u8]) -> io::Result<()> {
        let mut lsn = start_lsn.0;
        let mut remaining = data;

        while !remaining.is_empty() {
            let seg_offset = lsn % WAL_SEGMENT_SIZE;
            let space_in_seg = WAL_SEGMENT_SIZE - seg_offset;
            let to_write = remaining.len().min(space_in_seg as usize);
            let chunk = &remaining[..to_write];

            if self.current.is_none() || seg_offset == 0 {
                self.close_current()?;
                let name = segment_name(Lsn(lsn), self.timeline);
                let partial_path = self.dir.join(format!("{name}.partial"));
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&partial_path)?;
                // Pre-allocate the full 16 MiB segment upfront. This avoids
                // incremental extent allocation on each write, reduces
                // fragmentation on HDD/shared storage, and surfaces ENOSPC
                // before we start writing rather than mid-segment.
                // EOPNOTSUPP is silently ignored for filesystems (e.g. tmpfs)
                // that don't support fallocate.
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::io::AsRawFd;
                    unsafe {
                        libc::fallocate(file.as_raw_fd(), 0, 0, WAL_SEGMENT_SIZE as libc::off_t);
                    }
                }
                self.current = Some((file, partial_path));
                self.offset = seg_offset;
            }

            let (file, _) = self.current.as_mut().unwrap();
            file.write_all(chunk)?;
            self.offset += chunk.len() as u64;
            lsn += chunk.len() as u64;
            remaining = &remaining[to_write..];

            let (file, _) = self.current.as_mut().unwrap();
            file.sync_data()?;
            self.write_lsn = Lsn(lsn);
            self.flush_lsn = Lsn(lsn);

            if self.offset == WAL_SEGMENT_SIZE {
                self.close_current()?;
            }
        }

        Ok(())
    }

    /// Fsync and rename the current partial segment to its final name.
    fn close_current(&mut self) -> io::Result<()> {
        if let Some((file, partial_path)) = self.current.take() {
            file.sync_all()?;
            drop(file);
            let final_name = partial_path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".partial"))
                .map(|n| partial_path.with_file_name(n));
            if let Some(final_path) = final_name {
                fs::rename(&partial_path, &final_path)?;
            }
            self.offset = 0;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Main receive loop
// ---------------------------------------------------------------------------

/// Configuration for the native WAL receiver loop.
pub struct ReceiverConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub slot: String,
    pub dir: PathBuf,
    /// How often to send keepalive status updates to the primary.
    pub status_interval: Duration,
}

/// Run the WAL receive loop: connect, stream WAL, write to disk, send acks.
/// Returns only on error. The caller should retry with backoff.
pub fn run_receiver(cfg: &ReceiverConfig) -> Result<(), RecvError> {
    let mut conn = connect(
        &cfg.host,
        cfg.port,
        &cfg.user,
        &cfg.slot,
        cfg.password.as_deref(),
    )?;

    conn.set_read_timeout(Some(cfg.status_interval))
        .map_err(RecvError::Io)?;

    // Identify the server first to get the current timeline and WAL position.
    // The timeline is required to name segments correctly after a failover.
    let (timeline, sys_lsn) = identify_system(&mut conn)?;

    let start_lsn = match create_slot_if_not_exists(&mut conn, &cfg.slot)? {
        Some(lsn) if lsn != Lsn::ZERO => lsn,
        _ => highest_local_lsn(&cfg.dir).unwrap_or(sys_lsn),
    };
    start_replication(&mut conn, &cfg.slot, start_lsn)?;

    let mut writer = WalWriter::new(&cfg.dir, timeline);
    let mut last_status = std::time::Instant::now();

    loop {
        match recv_wal(&mut conn) {
            Ok(WalMsg::XLogData {
                start_lsn,
                wal_data,
                ..
            }) => {
                writer.write(start_lsn, &wal_data)?;
                send_status(&mut conn, writer.write_lsn, writer.flush_lsn)?;
                last_status = std::time::Instant::now();
            }
            Ok(WalMsg::Keepalive { reply_needed, .. }) => {
                if reply_needed || last_status.elapsed() >= cfg.status_interval {
                    send_status(&mut conn, writer.write_lsn, writer.flush_lsn)?;
                    last_status = std::time::Instant::now();
                }
            }
            Err(RecvError::Io(e))
                if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock =>
            {
                send_status(&mut conn, writer.write_lsn, writer.flush_lsn)?;
                last_status = std::time::Instant::now();
            }
            Err(e) => return Err(e),
        }
    }
}
