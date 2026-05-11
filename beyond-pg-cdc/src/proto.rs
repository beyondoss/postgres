//! Postgres frontend/backend wire protocol — the subset needed for trust-auth
//! simple queries and logical replication CopyBoth streaming. All integers are
//! big-endian; backend message length includes itself but excludes the tag.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::lsn::Lsn;

/// Postgres epoch (2000-01-01 00:00:00 UTC) as Unix seconds.
const PG_EPOCH_UNIX_SECS: u64 = 946_684_800;

/// Connection wrapper supporting either Unix or TCP sockets.
pub enum Conn {
    Unix(UnixStream),
    Tcp(TcpStream),
}

impl Conn {
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            Conn::Unix(s) => s.set_read_timeout(dur),
            Conn::Tcp(s) => s.set_read_timeout(dur),
        }
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Conn::Unix(s) => s.read(buf),
            Conn::Tcp(s) => s.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Conn::Unix(s) => s.write(buf),
            Conn::Tcp(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Unix(s) => s.flush(),
            Conn::Tcp(s) => s.flush(),
        }
    }
}

// --- low-level framing -----------------------------------------------------

/// Read one tagged backend message: `byte(tag) + int32(len incl. self) + payload`.
pub fn read_msg<R: Read>(r: &mut R) -> io::Result<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 5];
    r.read_exact(&mut hdr)?;
    let tag = hdr[0];
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]);
    if len < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid message length {len} for tag {tag}"),
        ));
    }
    let payload_len = (len - 4) as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        r.read_exact(&mut payload)?;
    }
    Ok((tag, payload))
}

/// Write a tagged frontend message: `byte(tag) + int32(4 + payload_len) + payload`.
pub fn write_msg<W: Write>(w: &mut W, tag: u8, payload: &[u8]) -> io::Result<()> {
    let len = (payload.len() as u32 + 4).to_be_bytes();
    w.write_all(&[tag])?;
    w.write_all(&len)?;
    w.write_all(payload)?;
    w.flush()
}

/// Send the untagged StartupMessage.
pub fn send_startup<W: Write>(w: &mut W, params: &[(&str, &str)]) -> io::Result<()> {
    let mut body = Vec::with_capacity(64);
    body.extend_from_slice(&196_608u32.to_be_bytes()); // protocol version 3.0
    for (k, v) in params {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0); // terminator

    let len = (body.len() as u32 + 4).to_be_bytes();
    w.write_all(&len)?;
    w.write_all(&body)?;
    w.flush()
}

// --- error helpers ---------------------------------------------------------

/// Decode an ErrorResponse / NoticeResponse payload into a human-readable line.
fn decode_error(payload: &[u8]) -> String {
    let mut msg = String::new();
    let mut i = 0;
    while i < payload.len() {
        let field = payload[i];
        if field == 0 {
            break;
        }
        i += 1;
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let value = String::from_utf8_lossy(&payload[start..i]);
        if field == b'M' {
            msg = value.into_owned();
            break;
        }
        if i < payload.len() {
            i += 1; // skip null terminator
        }
    }
    if msg.is_empty() {
        "unknown postgres error".to_owned()
    } else {
        msg
    }
}

// --- connect + auth --------------------------------------------------------

pub fn connect(
    host: &str,
    port: u16,
    user: &str,
    dbname: &str,
    replication: bool,
) -> Result<Conn, String> {
    let mut conn = if let Some(dir) = host.strip_prefix('/').map(|_| host) {
        let path = format!("{dir}/.s.PGSQL.{port}");
        let s = UnixStream::connect(&path).map_err(|e| format!("unix connect {path}: {e}"))?;
        Conn::Unix(s)
    } else {
        let s = TcpStream::connect((host, port))
            .map_err(|e| format!("tcp connect {host}:{port}: {e}"))?;
        Conn::Tcp(s)
    };

    let mut params: Vec<(&str, &str)> = vec![
        ("user", user),
        ("database", dbname),
        ("client_encoding", "UTF8"),
    ];
    if replication {
        params.push(("replication", "database"));
    }
    send_startup(&mut conn, &params).map_err(|e| format!("send startup: {e}"))?;

    loop {
        let (tag, payload) = read_msg(&mut conn).map_err(|e| format!("read auth msg: {e}"))?;
        match tag {
            b'R' => {
                if payload.len() < 4 {
                    return Err("auth: short Authentication message".into());
                }
                let kind = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                if kind != 0 {
                    return Err(format!(
                        "auth: only trust (AuthenticationOk) is supported, got method {kind}"
                    ));
                }
            }
            b'S' | b'K' | b'N' => {
                // ParameterStatus / BackendKeyData / NoticeResponse — informational
            }
            b'Z' => return Ok(conn),
            b'E' => return Err(format!("postgres error: {}", decode_error(&payload))),
            other => {
                return Err(format!("unexpected message tag {other:?} during connect"));
            }
        }
    }
}

// --- simple query ----------------------------------------------------------

#[allow(dead_code)]
pub fn query(conn: &mut Conn, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    write_msg(conn, b'Q', &payload).map_err(|e| format!("write query: {e}"))?;

    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut err: Option<String> = None;
    loop {
        let (tag, body) = read_msg(conn).map_err(|e| format!("read query response: {e}"))?;
        match tag {
            b'T' | b'C' | b'I' | b'S' | b'N' | b'n' | b'1' | b'2' | b'3' => {
                // RowDescription / CommandComplete / EmptyQuery / ParameterStatus /
                // Notice / NoData / Parse|Bind|CloseComplete — ignored
            }
            b'D' => {
                if body.len() < 2 {
                    return Err("DataRow too short".into());
                }
                let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
                let mut i = 2;
                let mut row = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    if i + 4 > body.len() {
                        return Err("DataRow truncated".into());
                    }
                    let len_bytes = [body[i], body[i + 1], body[i + 2], body[i + 3]];
                    i += 4;
                    let len = i32::from_be_bytes(len_bytes);
                    if len < 0 {
                        row.push(None);
                    } else {
                        let len = len as usize;
                        if i + len > body.len() {
                            return Err("DataRow value truncated".into());
                        }
                        let s = String::from_utf8_lossy(&body[i..i + len]).into_owned();
                        i += len;
                        row.push(Some(s));
                    }
                }
                rows.push(row);
            }
            b'E' => err = Some(decode_error(&body)),
            b'Z' => break,
            _ => {}
        }
    }

    if let Some(e) = err {
        return Err(format!("postgres error: {e}"));
    }
    Ok(rows)
}

// --- logical replication ---------------------------------------------------

pub enum WalMsg {
    XLogData { lsn: Lsn, data: Vec<u8> },
    Keepalive { reply_needed: bool },
}

pub fn start_replication(
    conn: &mut Conn,
    slot: &str,
    publication: &str,
    lsn: Lsn,
) -> Result<(), String> {
    let sql = format!(
        "START_REPLICATION SLOT {slot} LOGICAL {lsn} (proto_version '1', publication_names '{publication}')"
    );
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    write_msg(conn, b'Q', &payload).map_err(|e| format!("write START_REPLICATION: {e}"))?;

    loop {
        let (tag, body) =
            read_msg(conn).map_err(|e| format!("read START_REPLICATION response: {e}"))?;
        match tag {
            b'W' => return Ok(()), // CopyBothResponse — streaming begins
            b'E' => return Err(format!("postgres error: {}", decode_error(&body))),
            // T (RowDescription), D (DataRow), C (CommandComplete), N (Notice),
            // S (ParameterStatus) may precede 'W' depending on server version.
            _ => {}
        }
    }
}

pub fn recv_wal(conn: &mut Conn) -> io::Result<WalMsg> {
    loop {
        let (tag, body) = read_msg(conn)?;
        match tag {
            b'd' => {
                // CopyData — first byte is the streaming sub-message type.
                if body.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "empty CopyData"));
                }
                match body[0] {
                    b'w' => {
                        // XLogData: int64 start_lsn + int64 server_lsn + int64 timestamp + data
                        if body.len() < 1 + 24 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "short XLogData header",
                            ));
                        }
                        let mut lsn_bytes = [0u8; 8];
                        lsn_bytes.copy_from_slice(&body[1..9]);
                        let lsn = Lsn::from_be_bytes(lsn_bytes);
                        let data = body[25..].to_vec();
                        return Ok(WalMsg::XLogData { lsn, data });
                    }
                    b'k' => {
                        // PrimaryKeepalive: int64 lsn + int64 ts + int8 reply_requested
                        if body.len() < 1 + 17 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "short PrimaryKeepalive",
                            ));
                        }
                        let reply_needed = body[17] != 0;
                        return Ok(WalMsg::Keepalive { reply_needed });
                    }
                    other => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unknown CopyData sub-type {other:?}"),
                        ));
                    }
                }
            }
            b'c' | b'C' | b'S' | b'N' => {
                // CopyDone / CommandComplete / ParameterStatus / Notice — keep reading
            }
            b'E' => {
                return Err(io::Error::other(format!(
                    "postgres error during replication: {}",
                    decode_error(&body)
                )));
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected tag {other:?} during replication"),
                ));
            }
        }
    }
}

pub fn send_status(conn: &mut Conn, lsn: Lsn) -> io::Result<()> {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| {
            let pg_secs = d.as_secs().saturating_sub(PG_EPOCH_UNIX_SECS);
            pg_secs.saturating_mul(1_000_000) + d.subsec_micros() as u64
        })
        .unwrap_or(0);

    // Sub-payload: 'r' + 4 * int64 + int8(reply_now=0) = 1 + 32 + 1 = 34 bytes
    let mut sub = [0u8; 34];
    sub[0] = b'r';
    sub[1..9].copy_from_slice(&lsn.to_be_bytes()); // write
    sub[9..17].copy_from_slice(&lsn.to_be_bytes()); // flush
    sub[17..25].copy_from_slice(&lsn.to_be_bytes()); // apply
    sub[25..33].copy_from_slice(&(now_us as i64).to_be_bytes());
    sub[33] = 0;

    write_msg(conn, b'd', &sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_read_msg_roundtrip() {
        let mut buf: Vec<u8> = Vec::new();
        write_msg(&mut buf, b'Q', b"SELECT 1\0").unwrap();
        let mut cursor = std::io::Cursor::new(&buf);
        let (tag, payload) = read_msg(&mut cursor).unwrap();
        assert_eq!(tag, b'Q');
        assert_eq!(payload, b"SELECT 1\0");
    }

    #[test]
    fn startup_encodes_params() {
        let mut buf: Vec<u8> = Vec::new();
        send_startup(&mut buf, &[("user", "alice"), ("database", "db")]).unwrap();
        // 4 len + 4 proto + "user\0alice\0database\0db\0" + 1 terminator
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(len, buf.len());
        assert_eq!(&buf[4..8], &196_608u32.to_be_bytes());
        assert!(buf.windows(5).any(|w| w == b"user\0"));
        assert!(buf.windows(9).any(|w| w == b"database\0"));
    }

    #[test]
    fn decode_error_extracts_message_field() {
        // S=ERROR, C=42P01, M=table not found
        let payload = b"SERROR\0C42P01\0Mtable not found\0\0";
        assert_eq!(decode_error(payload), "table not found");
    }
}
