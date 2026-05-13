//! Postgres physical-replication wire protocol primitives.
//!
//! Extracted from beyond-pg-sink so that other binaries (notably the
//! WAL forwarder running inside `beyond-pg`) can share the wire-level
//! framing, slot creation, and start_replication / status_update code.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

/// Postgres epoch (2000-01-01 00:00:00 UTC) as Unix seconds.
const PG_EPOCH_UNIX_SECS: u64 = 946_684_800;

/// Physical WAL segment size: 16 MiB (Postgres default, compile-time constant).
const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

// ---------------------------------------------------------------------------
// LSN
// ---------------------------------------------------------------------------

/// A Postgres Log Sequence Number. Wire format: big-endian u64.
/// Text format: `"{hi:X}/{lo:08X}"` (e.g. `"1/23456780"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Lsn(u64::from_be_bytes(bytes))
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hi = (self.0 >> 32) as u32;
        let lo = (self.0 & 0xFFFF_FFFF) as u32;
        write!(f, "{hi:X}/{lo:08X}")
    }
}

impl std::str::FromStr for Lsn {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s
            .split_once('/')
            .ok_or_else(|| format!("invalid LSN '{s}': missing '/'"))?;
        let hi = u32::from_str_radix(hi, 16).map_err(|e| format!("invalid LSN hi '{hi}': {e}"))?;
        let lo = u32::from_str_radix(lo, 16).map_err(|e| format!("invalid LSN lo '{lo}': {e}"))?;
        Ok(Lsn(((hi as u64) << 32) | (lo as u64)))
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Categorized error for the replication loop.
///
/// `Config` variants are fatal (auth method mismatch, etc.) and must not be
/// retried. `Io` and `Protocol` variants are potentially transient.
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("config: {0}")]
    Config(String),
}

// ---------------------------------------------------------------------------
// Wire protocol framing
// ---------------------------------------------------------------------------

fn read_msg(r: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
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

fn write_msg(w: &mut TcpStream, tag: u8, payload: &[u8]) -> io::Result<()> {
    let len = (payload.len() as u32 + 4).to_be_bytes();
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(tag);
    buf.extend_from_slice(&len);
    buf.extend_from_slice(payload);
    w.write_all(&buf)
}

fn send_startup(w: &mut TcpStream, params: &[(&str, &str)]) -> io::Result<()> {
    let mut body = Vec::with_capacity(128);
    body.extend_from_slice(&196_608u32.to_be_bytes()); // protocol 3.0
    for (k, v) in params {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let len = (body.len() as u32 + 4).to_be_bytes();
    let mut msg = Vec::with_capacity(4 + body.len());
    msg.extend_from_slice(&len);
    msg.extend_from_slice(&body);
    w.write_all(&msg)?;
    w.flush()
}

pub(crate) fn decode_error(payload: &[u8]) -> String {
    let mut severity = String::new();
    let mut code = String::new();
    let mut msg = String::new();
    let mut detail = String::new();
    let mut hint = String::new();
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
        match field {
            b'S' => severity = value.into_owned(),
            b'C' => code = value.into_owned(),
            b'M' => msg = value.into_owned(),
            b'D' => detail = value.into_owned(),
            b'H' => hint = value.into_owned(),
            _ => {}
        }
        if i < payload.len() {
            i += 1;
        }
    }
    if msg.is_empty() {
        return "unknown postgres error".to_owned();
    }
    let mut out = String::new();
    if !severity.is_empty() {
        out.push_str(&severity);
        out.push(' ');
    }
    if !code.is_empty() {
        out.push_str(&code);
        out.push_str(": ");
    }
    out.push_str(&msg);
    if !detail.is_empty() {
        out.push_str(" (DETAIL: ");
        out.push_str(&detail);
        out.push(')');
    }
    if !hint.is_empty() {
        out.push_str(" (HINT: ");
        out.push_str(&hint);
        out.push(')');
    }
    out
}

// ---------------------------------------------------------------------------
// Connect + auth (trust and SCRAM-SHA-256)
// ---------------------------------------------------------------------------

/// Open a physical replication connection to Postgres.
///
/// `application_name` is included in startup params and must match
/// `synchronous_standby_names` on the primary (e.g. `"wal_sink"`).
///
/// `password` is required when the server is configured for SCRAM-SHA-256
/// authentication. Pass `None` for trust auth.
pub fn connect(
    host: &str,
    port: u16,
    user: &str,
    application_name: &str,
    password: Option<&str>,
) -> Result<TcpStream, RecvError> {
    let mut conn = TcpStream::connect((host, port))
        .map_err(|e| RecvError::Protocol(format!("tcp connect {host}:{port}: {e}")))?;
    conn.set_nodelay(true)
        .map_err(|e| RecvError::Protocol(format!("set_nodelay: {e}")))?;

    send_startup(
        &mut conn,
        &[
            ("user", user),
            ("database", "replication"),
            ("replication", "true"),
            ("application_name", application_name),
            ("client_encoding", "UTF8"),
        ],
    )
    .map_err(|e| RecvError::Protocol(format!("send startup: {e}")))?;

    loop {
        let (tag, payload) =
            read_msg(&mut conn).map_err(|e| RecvError::Protocol(format!("read auth: {e}")))?;
        match tag {
            b'R' => {
                if payload.len() < 4 {
                    return Err(RecvError::Protocol("short AuthenticationMessage".into()));
                }
                let kind = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                match kind {
                    0 => {} // AuthenticationOk — trust auth, continue to ReadyForQuery
                    5 => {
                        return Err(RecvError::Config(
                            "MD5 auth is not supported; configure the server for SCRAM-SHA-256 or trust".into(),
                        ));
                    }
                    10 => {
                        // AuthenticationSASL — SCRAM-SHA-256
                        let pw = password.ok_or_else(|| {
                            RecvError::Config(
                                "server requires SCRAM-SHA-256 authentication but no password was provided".into(),
                            )
                        })?;
                        scram_auth(&mut conn, user, pw, &payload[4..])
                            .map_err(|e| RecvError::Protocol(format!("SCRAM auth: {e}")))?;
                    }
                    other => {
                        return Err(RecvError::Config(format!(
                            "unsupported auth method {other}; only trust and SCRAM-SHA-256 are supported"
                        )));
                    }
                }
            }
            b'S' | b'K' | b'N' => {}
            b'Z' => return Ok(conn),
            b'E' => {
                return Err(RecvError::Protocol(format!(
                    "postgres error: {}",
                    decode_error(&payload)
                )));
            }
            other => {
                return Err(RecvError::Protocol(format!(
                    "unexpected tag {other:?} during connect"
                )));
            }
        }
    }
}

/// Execute the SCRAM-SHA-256 authentication handshake.
fn scram_auth(
    conn: &mut TcpStream,
    user: &str,
    password: &str,
    mechanisms: &[u8],
) -> io::Result<()> {
    // Verify the server offers SCRAM-SHA-256.
    let has_scram = mechanisms
        .split(|&b| b == 0)
        .any(|m| m == b"SCRAM-SHA-256" || m == b"SCRAM-SHA-256-PLUS");
    if !has_scram {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "server does not offer SCRAM-SHA-256; available: {:?}",
                String::from_utf8_lossy(mechanisms)
            ),
        ));
    }

    // Generate a random 18-byte client nonce, base64-encoded to 24 chars.
    let mut nonce_raw = [0u8; 18];
    getrandom::getrandom(&mut nonce_raw)
        .map_err(|e| io::Error::other(format!("getrandom: {e}")))?;
    let cnonce = base64_encode(&nonce_raw);

    // SASLprep: for ASCII-only usernames (typical for replication) this is a no-op.
    let user_enc = user.replace('=', "=3D").replace(',', "=2C");
    let client_first_bare = format!("n={user_enc},r={cnonce}");
    // "n,," = GS2 header: no channel binding, no authzid.
    let client_first = format!("n,,{client_first_bare}");

    // --- Send SASLInitialResponse (tag 'p') ---
    let mech = b"SCRAM-SHA-256";
    let cf_bytes = client_first.as_bytes();
    let mut payload = Vec::with_capacity(mech.len() + 1 + 4 + cf_bytes.len());
    payload.extend_from_slice(mech);
    payload.push(0);
    payload.extend_from_slice(&(cf_bytes.len() as i32).to_be_bytes());
    payload.extend_from_slice(cf_bytes);
    write_msg(conn, b'p', &payload)?;

    // --- Read AuthenticationSASLContinue (R, kind=11) ---
    let (tag, body) = read_msg(conn)?;
    if tag != b'R' || body.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected AuthenticationSASLContinue",
        ));
    }
    let kind = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    if kind != 11 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected SASL continue (11), got {kind}"),
        ));
    }
    let server_first = std::str::from_utf8(&body[4..]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SASL server-first not UTF-8: {e}"),
        )
    })?;

    // Parse server-first-message: r=<nonce>,s=<salt_b64>,i=<iterations>
    let mut full_nonce = "";
    let mut salt_b64 = "";
    let mut iterations: u32 = 0;
    for part in server_first.split(',') {
        if let Some(v) = part.strip_prefix("r=") {
            full_nonce = v;
        } else if let Some(v) = part.strip_prefix("s=") {
            salt_b64 = v;
        } else if let Some(v) = part.strip_prefix("i=") {
            iterations = v.parse().unwrap_or(0);
        }
    }
    if !full_nonce.starts_with(cnonce.as_str()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "server nonce does not extend client nonce",
        ));
    }
    if iterations == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid SCRAM iteration count",
        ));
    }
    let salt = base64_decode(salt_b64)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("SCRAM salt: {e}")))?;

    // Derive keys.
    let salted_pw = pbkdf2_hmac_sha256(password.as_bytes(), &salt, iterations);
    let client_key = hmac_sha256(&salted_pw, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted_pw, b"Server Key");

    // client_final_without_proof: "c=biws,r=<full_nonce>"
    // "biws" = base64("n,,") — the GS2 header we sent.
    let cfwp = format!("c=biws,r={full_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{cfwp}");

    let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut client_proof = client_key;
    for (a, b) in client_proof.iter_mut().zip(client_sig.iter()) {
        *a ^= b;
    }
    let server_sig = hmac_sha256(&server_key, auth_message.as_bytes());

    // --- Send SASLResponse (tag 'p') ---
    let client_final = format!("{cfwp},p={}", base64_encode(&client_proof));
    write_msg(conn, b'p', client_final.as_bytes())?;

    // --- Read AuthenticationSASLFinal (R, kind=12) ---
    let (tag, body) = read_msg(conn)?;
    if tag != b'R' || body.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected AuthenticationSASLFinal",
        ));
    }
    let kind = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    if kind != 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected SASL final (12), got {kind}"),
        ));
    }
    let server_final = std::str::from_utf8(&body[4..]).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SASL server-final not UTF-8: {e}"),
        )
    })?;

    // Verify server signature or report server-side error.
    for part in server_final.split(',') {
        if let Some(v) = part.strip_prefix("v=") {
            if v != base64_encode(&server_sig) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "SCRAM server signature mismatch",
                ));
            }
            return Ok(());
        } else if let Some(e) = part.strip_prefix("e=") {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("SCRAM server error: {e}"),
            ));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "SASL final message missing 'v=' field",
    ))
}

// ---------------------------------------------------------------------------
// SCRAM crypto helpers
// ---------------------------------------------------------------------------

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(data).into()
}

/// PBKDF2-HMAC-SHA256 with a single 32-byte output block.
fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut data = Vec::with_capacity(salt.len() + 4);
    data.extend_from_slice(salt);
    data.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac_sha256(password, &data);
    let mut result = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (r, u_byte) in result.iter_mut().zip(u.iter()) {
            *r ^= u_byte;
        }
    }
    result
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn base64_decode(s: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s)
}

// ---------------------------------------------------------------------------
// Physical replication
// ---------------------------------------------------------------------------

pub enum WalMsg {
    /// XLogData CopyData frame from the primary.
    ///
    /// `wal_data` contains only the raw WAL bytes starting at `start_lsn`.
    /// The 25-byte CopyData framing prefix (`b'w'`, LSNs, timestamp) has been
    /// stripped. `end_lsn` is the server's current WAL end at time of send.
    XLogData {
        start_lsn: Lsn,
        end_lsn: Lsn,
        wal_data: Vec<u8>,
    },
    /// Server keepalive. `reply_needed` means send a status update immediately.
    Keepalive { server_lsn: Lsn, reply_needed: bool },
}

/// Create the physical replication slot if it doesn't already exist.
///
/// Returns the `consistent_point` LSN from the CREATE response. Returns
/// `None` if the slot already existed (error 42710).
pub fn create_slot_if_not_exists(
    conn: &mut TcpStream,
    slot: &str,
) -> Result<Option<Lsn>, RecvError> {
    let sql = format!("CREATE_REPLICATION_SLOT {slot} PHYSICAL");
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    write_msg(conn, b'Q', &payload)
        .map_err(|e| RecvError::Protocol(format!("write CREATE_REPLICATION_SLOT: {e}")))?;

    let mut consistent_point: Option<Lsn> = None;
    loop {
        let (tag, body) = read_msg(conn).map_err(|e| {
            RecvError::Protocol(format!("read CREATE_REPLICATION_SLOT response: {e}"))
        })?;
        match tag {
            b'T' | b'C' | b'N' | b'S' => {}
            b'D' => {
                if body.len() >= 2 {
                    let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut pos = 2usize;
                    for col in 0..ncols {
                        if pos + 4 > body.len() {
                            break;
                        }
                        let col_len = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
                        pos += 4;
                        if col_len < 0 {
                            continue;
                        }
                        let col_len = col_len as usize;
                        if pos + col_len > body.len() {
                            break;
                        }
                        if col == 1
                            && let Ok(s) = std::str::from_utf8(&body[pos..pos + col_len])
                        {
                            consistent_point = s.parse().ok();
                        }
                        pos += col_len;
                    }
                }
            }
            b'Z' => return Ok(consistent_point),
            b'E' => {
                let msg = decode_error(&body);
                if msg.contains("42710") {
                    loop {
                        let (t, _) = read_msg(conn)
                            .map_err(|e| RecvError::Protocol(format!("drain after 42710: {e}")))?;
                        if t == b'Z' {
                            break;
                        }
                    }
                    return Ok(None);
                }
                return Err(RecvError::Protocol(format!("postgres error: {msg}")));
            }
            _ => {}
        }
    }
}

/// Send `IDENTIFY_SYSTEM` and return `(timeline, xlogpos)`.
pub fn identify_system(conn: &mut TcpStream) -> Result<(u32, Lsn), RecvError> {
    let payload = b"IDENTIFY_SYSTEM\0";
    write_msg(conn, b'Q', payload)
        .map_err(|e| RecvError::Protocol(format!("write IDENTIFY_SYSTEM: {e}")))?;
    let mut timeline: u32 = 1;
    let mut xlogpos = Lsn::ZERO;
    loop {
        let (tag, body) = read_msg(conn)
            .map_err(|e| RecvError::Protocol(format!("read IDENTIFY_SYSTEM response: {e}")))?;
        match tag {
            b'T' | b'C' | b'N' | b'S' => {}
            b'D' => {
                if body.len() >= 2 {
                    let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
                    let mut pos = 2usize;
                    for col in 0..ncols {
                        if pos + 4 > body.len() {
                            break;
                        }
                        let col_len = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
                        pos += 4;
                        if col_len < 0 {
                            continue;
                        }
                        let col_len = col_len as usize;
                        if pos + col_len > body.len() {
                            break;
                        }
                        if col == 1 {
                            if let Ok(s) = std::str::from_utf8(&body[pos..pos + col_len]) {
                                timeline = s.parse().unwrap_or(1);
                            }
                        } else if col == 2
                            && let Ok(s) = std::str::from_utf8(&body[pos..pos + col_len])
                        {
                            xlogpos = s.parse().unwrap_or(Lsn::ZERO);
                        }
                        pos += col_len;
                    }
                }
            }
            b'Z' => return Ok((timeline, xlogpos)),
            b'E' => {
                return Err(RecvError::Protocol(format!(
                    "postgres error: {}",
                    decode_error(&body)
                )));
            }
            _ => {}
        }
    }
}

/// Scan `dir` for the highest complete WAL segment and return the LSN just
/// past its end (i.e. the LSN to pass to `START_REPLICATION` to resume).
///
/// Segment filenames are `{timeline:08X}{lsn_hi:08X}{seg_lo:08X}` (24 hex chars).
/// `lsn_hi` is the high 32 bits of the LSN; `seg_lo` is the segment number
/// within that XLogId, i.e. `(lsn & 0xFFFF_FFFF) / WAL_SEGMENT_SIZE`.
pub fn highest_local_lsn(dir: &std::path::Path) -> Option<Lsn> {
    let highest = std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.len() == 24 && n.bytes().all(|b| b.is_ascii_hexdigit()))
        .max()?;

    // Chars [0..8]  = timeline (ignored for LSN reconstruction)
    // Chars [8..16] = high 32 bits of the segment-start LSN
    // Chars [16..24] = segment number within the XLogId
    let seg_hi = u32::from_str_radix(&highest[8..16], 16).ok()?;
    let seg_lo = u32::from_str_radix(&highest[16..24], 16).ok()?;
    let seg_start = ((seg_hi as u64) << 32) | ((seg_lo as u64) * WAL_SEGMENT_SIZE);
    Some(Lsn(seg_start + WAL_SEGMENT_SIZE))
}

/// Send `START_REPLICATION SLOT {slot} PHYSICAL {lsn}`.
pub fn start_replication(conn: &mut TcpStream, slot: &str, lsn: Lsn) -> Result<(), RecvError> {
    let sql = format!("START_REPLICATION SLOT {slot} PHYSICAL {lsn}");
    let mut payload = Vec::with_capacity(sql.len() + 1);
    payload.extend_from_slice(sql.as_bytes());
    payload.push(0);
    write_msg(conn, b'Q', &payload)
        .map_err(|e| RecvError::Protocol(format!("write START_REPLICATION: {e}")))?;

    loop {
        let (tag, body) = read_msg(conn)
            .map_err(|e| RecvError::Protocol(format!("read START_REPLICATION response: {e}")))?;
        match tag {
            b'W' => return Ok(()),
            b'E' => {
                return Err(RecvError::Protocol(format!(
                    "postgres error: {}",
                    decode_error(&body)
                )));
            }
            _ => {}
        }
    }
}

/// Read one WAL message from the replication stream.
pub fn recv_wal(conn: &mut TcpStream) -> Result<WalMsg, RecvError> {
    loop {
        let (tag, body) = read_msg(conn)?;
        match tag {
            b'd' => {
                if body.is_empty() {
                    return Err(RecvError::Protocol("empty CopyData".into()));
                }
                match body[0] {
                    b'w' => {
                        if body.len() < 25 {
                            return Err(RecvError::Protocol("short XLogData header".into()));
                        }
                        let start_lsn = Lsn::from_be_bytes(body[1..9].try_into().unwrap());
                        let end_lsn = Lsn::from_be_bytes(body[9..17].try_into().unwrap());
                        return Ok(WalMsg::XLogData {
                            start_lsn,
                            end_lsn,
                            wal_data: body[25..].to_vec(),
                        });
                    }
                    b'k' => {
                        if body.len() < 18 {
                            return Err(RecvError::Protocol("short PrimaryKeepalive".into()));
                        }
                        let server_lsn = Lsn::from_be_bytes(body[1..9].try_into().unwrap());
                        let reply_needed = body[17] != 0;
                        return Ok(WalMsg::Keepalive {
                            server_lsn,
                            reply_needed,
                        });
                    }
                    other => {
                        return Err(RecvError::Protocol(format!(
                            "unknown CopyData sub-type {other:#x}"
                        )));
                    }
                }
            }
            b'c' | b'C' | b'S' | b'N' => {}
            b'E' => {
                return Err(RecvError::Protocol(format!(
                    "postgres error: {}",
                    decode_error(&body)
                )));
            }
            other => {
                return Err(RecvError::Protocol(format!(
                    "unexpected tag {other:#x} during replication"
                )));
            }
        }
    }
}

/// Send a StandbyStatusUpdate to the primary.
pub fn send_status(conn: &mut TcpStream, write_lsn: Lsn, flush_lsn: Lsn) -> io::Result<()> {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| {
            let pg_secs = d.as_secs().saturating_sub(PG_EPOCH_UNIX_SECS);
            pg_secs.saturating_mul(1_000_000) + d.subsec_micros() as u64
        })
        .unwrap_or(0);

    let mut sub = [0u8; 34];
    sub[0] = b'r';
    sub[1..9].copy_from_slice(&write_lsn.to_be_bytes());
    sub[9..17].copy_from_slice(&flush_lsn.to_be_bytes());
    sub[17..25].copy_from_slice(&flush_lsn.to_be_bytes());
    sub[25..33].copy_from_slice(&(now_us.min(i64::MAX as u64) as i64).to_be_bytes());
    sub[33] = 0;

    write_msg(conn, b'd', &sub)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Lsn ---

    #[test]
    fn lsn_roundtrip() {
        let lsn = Lsn(0x0000_0001_2345_6780);
        assert_eq!(lsn.to_string(), "1/23456780");
        assert_eq!("1/23456780".parse::<Lsn>().unwrap(), lsn);
    }

    #[test]
    fn lsn_zero() {
        assert_eq!(Lsn::ZERO.to_string(), "0/00000000");
        assert_eq!("0/00000000".parse::<Lsn>().unwrap(), Lsn::ZERO);
    }

    #[test]
    fn lsn_be_bytes() {
        let lsn = Lsn(0x0102_0304_0506_0708);
        assert_eq!(lsn.to_be_bytes(), [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(Lsn::from_be_bytes([1, 2, 3, 4, 5, 6, 7, 8]), lsn);
    }

    #[test]
    fn lsn_parse_error() {
        assert!("noslash".parse::<Lsn>().is_err());
        assert!("X/GGGGGGGG".parse::<Lsn>().is_err());
    }

    #[test]
    fn lsn_ordering() {
        assert!(Lsn(1) < Lsn(2));
        assert!(Lsn(0x1_0000_0000) > Lsn(0xFFFF_FFFF));
    }

    // --- highest_local_lsn ---

    #[test]
    fn highest_local_lsn_seg_hi_zero() {
        let dir = tempdir();
        // Timeline 1, seg_hi=0, seg_lo=1 → segment starts at 1 * 16MiB
        touch(&dir, "0000000100000000000000001"); // 25 chars — ignored (not 24)
        touch(&dir, "000000010000000000000001"); // 24 chars ✓
        let lsn = highest_local_lsn(&dir).unwrap();
        // seg_start = (0 << 32) | (1 * WAL_SEGMENT_SIZE) = 16 MiB
        // next = 32 MiB
        assert_eq!(lsn, Lsn(2 * WAL_SEGMENT_SIZE));
    }

    #[test]
    fn highest_local_lsn_seg_hi_nonzero() {
        let dir = tempdir();
        // Timeline 1, seg_hi=1, seg_lo=0 → segment starts at 1<<32
        touch(&dir, "000000010000000100000000");
        let lsn = highest_local_lsn(&dir).unwrap();
        // seg_start = (1u64 << 32) | 0 = 4 GiB
        // next = 4 GiB + 16 MiB
        assert_eq!(lsn, Lsn((1u64 << 32) + WAL_SEGMENT_SIZE));
    }

    #[test]
    fn highest_local_lsn_multiple_timelines() {
        let dir = tempdir();
        // Timeline 1 segments
        touch(&dir, "000000010000000000000001");
        touch(&dir, "000000010000000000000002");
        // Timeline 2 segment — lexicographically highest
        touch(&dir, "000000020000000000000000");
        let lsn = highest_local_lsn(&dir).unwrap();
        // Timeline-2 file sorts last; seg_hi=0, seg_lo=0
        // seg_start = 0, next = 16 MiB
        assert_eq!(lsn, Lsn(WAL_SEGMENT_SIZE));
    }

    #[test]
    fn highest_local_lsn_empty_dir() {
        let dir = tempdir();
        assert!(highest_local_lsn(&dir).is_none());
    }

    #[test]
    fn highest_local_lsn_ignores_partials() {
        let dir = tempdir();
        touch(&dir, "000000010000000000000001.partial"); // ignored: not 24 hex chars
        touch(&dir, "000000010000000000000001");
        let lsn = highest_local_lsn(&dir).unwrap();
        assert_eq!(lsn, Lsn(2 * WAL_SEGMENT_SIZE));
    }

    // --- decode_error ---

    #[test]
    fn decode_error_extracts_fields() {
        let payload = b"SERROR\0C42P01\0Mtable not found\0\0";
        assert_eq!(decode_error(payload), "ERROR 42P01: table not found");
    }

    #[test]
    fn decode_error_includes_detail_and_hint() {
        let payload = b"SERROR\0C42P01\0Mmsg\0Dsome detail\0Htry this\0\0";
        let out = decode_error(payload);
        assert!(out.contains("DETAIL: some detail"), "{out}");
        assert!(out.contains("HINT: try this"), "{out}");
    }

    #[test]
    fn decode_error_unknown_when_no_message() {
        assert_eq!(decode_error(b"\0"), "unknown postgres error");
    }

    // --- SCRAM crypto ---

    #[test]
    fn hmac_sha256_known_vector() {
        // RFC 2104 test vector (using SHA256 underneath)
        let key = b"key";
        let data = b"The quick brown fox jumps over the lazy dog";
        let result = hmac_sha256(key, data);
        // Precomputed: f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8
        let expected = [
            0xf7, 0xbc, 0x83, 0xf4, 0x30, 0x53, 0x84, 0x24, 0xb1, 0x32, 0x98, 0xe6, 0xaa, 0x6f,
            0xb1, 0x43, 0xef, 0x4d, 0x59, 0xa1, 0x49, 0x46, 0x17, 0x59, 0x97, 0x47, 0x9d, 0xbc,
            0x2d, 0x1a, 0x3c, 0xd8,
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn pbkdf2_one_iteration_equals_single_hmac() {
        // With iterations=1, PBKDF2 output = HMAC(password, salt || 0x00000001).
        let result = pbkdf2_hmac_sha256(b"password", b"salt", 1);
        let expected = hmac_sha256(b"password", b"salt\x00\x00\x00\x01");
        assert_eq!(result, expected);
    }

    #[test]
    fn pbkdf2_two_iterations_xor() {
        // With iterations=2, output = U1 XOR U2.
        let u1 = hmac_sha256(b"password", b"salt\x00\x00\x00\x01");
        let u2 = hmac_sha256(b"password", &u1);
        let mut expected = u1;
        for (a, b) in expected.iter_mut().zip(u2.iter()) {
            *a ^= b;
        }
        assert_eq!(pbkdf2_hmac_sha256(b"password", b"salt", 2), expected);
    }

    #[test]
    fn recv_error_implements_error_trait() {
        // Verify RecvError implements std::error::Error so it composes with Box<dyn Error>
        let e: Box<dyn std::error::Error> = Box::new(RecvError::Protocol("test".into()));
        assert!(e.to_string().contains("test"));
    }

    // --- helpers ---

    fn tempdir() -> TempDir {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("wal-proto-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    fn touch(dir: &std::path::Path, name: &str) {
        std::fs::write(dir.join(name), b"").unwrap();
    }

    struct TempDir(std::path::PathBuf);
    impl std::ops::Deref for TempDir {
        type Target = std::path::PathBuf;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
