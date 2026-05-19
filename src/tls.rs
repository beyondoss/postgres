//! TLS certificate provisioning for the Postgres server.
//!
//! Three sources, checked in order:
//!
//! 1. **User-managed** (`PGDATA/beyond/.user-managed` sentinel): operator
//!    supplies their own `server.{crt,key}` at the standard path. We do
//!    nothing.
//! 2. **Platform** (`/run/beyond/tls/cert.pem` exists): the Beyond box
//!    contract — guest agent provisions a per-app CA-chained leaf cert at
//!    boot, rotated every 22h via atomic rename. We point config at it and
//!    a watcher elsewhere triggers reloads on rotation. See
//!    `../beyond/boxes/docs/09-internal-tls.md`.
//! 3. **Self-signed** (fallback): for dev/test and any environment without
//!    the platform contract. Generates an Ed25519 cert under `PGDATA/beyond/`
//!    and renews it within 30 days of expiry.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rcgen::string::Ia5String;
use rcgen::{CertificateParams, DnType, SanType};
use tracing::{info, warn};

const RENEWAL_THRESHOLD_SECS: u64 = 30 * 24 * 3600;
const CERT_VALIDITY_DAYS: i64 = 365;

/// Default location where the Beyond platform mounts the per-VM cert.
/// Tests override via `provision_with_paths`.
pub const PLATFORM_TLS_DIR: &str = "/run/beyond/tls";

/// Outcome of [`ensure_cert`]. The caller uses this to decide whether to
/// trigger a `pg_ctl reload`.
#[derive(Debug, PartialEq, Eq)]
pub enum TlsCertOutcome {
    /// No cert existed; a fresh one was written.
    Generated,
    /// Existing cert was within 30 days of expiry and has been replaced.
    Renewed,
    /// `PGDATA/beyond/.user-managed` sentinel present; we did nothing.
    UserManaged,
    /// Existing cert is valid; no action taken.
    StillValid,
}

/// Resolved TLS material — paths the supervisor templates into
/// `postgresql.conf` and `pgbouncer.ini`. Returned by [`provision`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    pub source: TlsSource,
    pub cert: PathBuf,
    pub key: PathBuf,
    /// CA bundle path. `None` for self-signed (no chain to validate against).
    pub ca: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsSource {
    /// Operator-managed cert at `PGDATA/beyond/server.{crt,key}`. Whatever's
    /// there, untouched.
    UserManaged,
    /// Beyond platform-provisioned cert at `/run/beyond/tls/`. Auto-rotated
    /// every 22h — caller is expected to watch the cert file for changes.
    Platform,
    /// Self-signed cert generated and renewed by this process.
    SelfSigned,
}

/// Provision TLS material for Postgres.
///
/// Detection order is precedence order: a user-managed sentinel beats the
/// platform cert beats the self-signed fallback. The platform path is
/// detected by file existence (the env var is informational; the file
/// contract is what matters and survives shell-quoting surprises).
pub fn provision(pgdata: &Path) -> Result<TlsConfig, TlsError> {
    provision_with_paths(pgdata, Path::new(PLATFORM_TLS_DIR))
}

/// Test entry point: lets tests redirect the platform path to a temp dir.
pub fn provision_with_paths(pgdata: &Path, platform_dir: &Path) -> Result<TlsConfig, TlsError> {
    let beyond_dir = pgdata.join("beyond");
    let user_sentinel = beyond_dir.join(".user-managed");

    if user_sentinel.exists() {
        info!("tls: .user-managed sentinel present, using operator-supplied cert");
        return Ok(TlsConfig {
            source: TlsSource::UserManaged,
            cert: beyond_dir.join("server.crt"),
            key: beyond_dir.join("server.key"),
            // A user-managed cert may or may not be CA-chained — if the
            // operator wants `verify-full` they can drop a ca.crt and wire
            // it via 99-user.conf themselves. We don't assume.
            ca: None,
        });
    }

    let platform_cert = platform_dir.join("cert.pem");
    if platform_cert.exists() {
        info!(
            "tls: using platform cert at {} (rotated by guest agent)",
            platform_dir.display()
        );
        return Ok(TlsConfig {
            source: TlsSource::Platform,
            cert: platform_cert,
            key: platform_dir.join("key.pem"),
            ca: Some(platform_dir.join("ca.pem")),
        });
    }

    // Fallback: generate/renew under PGDATA/beyond/.
    let _ = ensure_cert(pgdata)?;
    Ok(TlsConfig {
        source: TlsSource::SelfSigned,
        cert: beyond_dir.join("server.crt"),
        key: beyond_dir.join("server.key"),
        ca: None,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("cert I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cert generation error: {0}")]
    Cert(#[from] rcgen::Error),
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    #[error("postgres system user not found — image is misbuilt")]
    PostgresUserNotFound,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    #[error("chown failed on cert files: {0}")]
    Chown(std::io::Error),
}

/// Ensure `PGDATA/beyond/server.{crt,key}` exist and are not near expiry.
///
/// Idempotent: safe to call on every boot. Returns the outcome so the caller
/// can trigger a `pg_ctl reload` when the cert was written.
pub fn ensure_cert(pgdata: &Path) -> Result<TlsCertOutcome, TlsError> {
    let beyond_dir = pgdata.join("beyond");
    let sentinel = beyond_dir.join(".user-managed");
    let cert_path = beyond_dir.join("server.crt");
    let key_path = beyond_dir.join("server.key");

    if sentinel.exists() {
        info!("tls: .user-managed sentinel present, skipping cert generation");
        return Ok(TlsCertOutcome::UserManaged);
    }

    std::fs::create_dir_all(&beyond_dir)?;

    let outcome = if cert_path.exists() {
        if cert_needs_renewal(&cert_path) {
            TlsCertOutcome::Renewed
        } else {
            info!("tls: cert is valid, no action needed");
            return Ok(TlsCertOutcome::StillValid);
        }
    } else {
        TlsCertOutcome::Generated
    };

    match outcome {
        TlsCertOutcome::Generated => info!("tls: no cert found, generating self-signed cert"),
        TlsCertOutcome::Renewed => info!("tls: cert within 30-day expiry window, renewing"),
        _ => unreachable!(),
    }

    generate_cert(&cert_path, &key_path)?;
    set_permissions_and_ownership(&cert_path, &key_path)?;

    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Renewal check
// ---------------------------------------------------------------------------

/// Returns `true` if the cert is missing, unparseable, or within 30 days of
/// expiry. Parse errors are treated as "regenerate" per the error-handling spec.
fn cert_needs_renewal(path: &Path) -> bool {
    let pem = match std::fs::read(path) {
        Ok(p) => p,
        Err(_) => return true,
    };

    let threshold = now_secs().saturating_add(RENEWAL_THRESHOLD_SECS) as i64;

    match x509_parser::pem::parse_x509_pem(&pem) {
        Ok((_, pem_obj)) => match pem_obj.parse_x509() {
            Ok(cert) => {
                let not_after = cert.validity().not_after.timestamp();
                if not_after < threshold {
                    info!("tls: cert expires at unix:{not_after}, threshold unix:{threshold}");
                    true
                } else {
                    false
                }
            }
            Err(e) => {
                warn!("tls: failed to parse X.509 cert ({e}), treating as expired");
                true
            }
        },
        Err(e) => {
            warn!("tls: failed to parse PEM ({e}), treating as expired");
            true
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Certificate generation
// ---------------------------------------------------------------------------

fn generate_cert(cert_path: &Path, key_path: &Path) -> Result<(), TlsError> {
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519)?;

    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, hostname());

    params.subject_alt_names = vec![
        SanType::DnsName(Ia5String::try_from("localhost")?),
        SanType::DnsName(Ia5String::try_from("*.beyond.dev")?),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
        SanType::IpAddress(IpAddr::V6(Ipv6Addr::LOCALHOST)),
    ];

    let now = time::OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now + time::Duration::days(CERT_VALIDITY_DAYS);

    let cert = params.self_signed(&key_pair)?;

    crate::config::write_atomic(cert_path, &cert.pem())?;
    // Key is written with mode 0o600 set *before* content. This closes the
    // window in which a chmod-after-write approach would leave the private
    // key briefly readable under a wider umask.
    crate::config::write_atomic_bytes_with_mode(
        key_path,
        key_pair.serialize_pem().as_bytes(),
        0o600,
    )?;

    Ok(())
}

/// Hostname for the cert CN. Reads `/etc/hostname` (written by `init::run()`
/// from MMDS), falls back to `gethostname(2)`, then to `"localhost"`.
fn hostname() -> String {
    if let Ok(s) = std::fs::read_to_string("/etc/hostname") {
        let s = s.trim().to_owned();
        if !s.is_empty() {
            return s;
        }
    }

    #[cfg(target_os = "linux")]
    if let Ok(name) = nix::unistd::gethostname()
        && let Ok(s) = name.into_string()
        && !s.is_empty()
    {
        return s;
    }

    "localhost".to_owned()
}

// ---------------------------------------------------------------------------
// File modes and ownership
// ---------------------------------------------------------------------------

fn set_permissions_and_ownership(cert_path: &Path, key_path: &Path) -> Result<(), TlsError> {
    // The key was already created at 0o600 by `write_atomic_bytes_with_mode`;
    // this is defense-in-depth in case the file was generated by a prior
    // build that pre-dated that helper.
    std::fs::set_permissions(key_path, std::fs::Permissions::from_mode(0o600))?;
    std::fs::set_permissions(cert_path, std::fs::Permissions::from_mode(0o644))?;

    #[cfg(target_os = "linux")]
    chown_postgres(cert_path, key_path)?;

    Ok(())
}

/// `chown postgres:postgres` on the cert files. The `postgres` OS user must
/// exist; its absence means a misbuilt image.
///
/// Silently skips when not running as root — chown requires CAP_CHOWN and
/// beyond-pg always runs as root in production containers, but tests and
/// local dev do not.
#[cfg(target_os = "linux")]
fn chown_postgres(cert_path: &Path, key_path: &Path) -> Result<(), TlsError> {
    use std::ffi::CString;

    // SAFETY: geteuid() is always safe to call.
    if unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }

    let username = CString::new("postgres").expect("static string is valid");
    // SAFETY: getpwnam is thread-safe when called before any threads are
    // spawned that also call it, which is the case here (single-threaded boot).
    let pwd = unsafe { libc::getpwnam(username.as_ptr()) };
    if pwd.is_null() {
        return Err(TlsError::PostgresUserNotFound);
    }
    let (uid, gid) = unsafe { ((*pwd).pw_uid, (*pwd).pw_gid) };

    for path in [cert_path, key_path] {
        let path_c = CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|e| TlsError::Io(std::io::Error::other(e.to_string())))?;
        // SAFETY: path_c is a valid NUL-terminated path string.
        if unsafe { libc::chown(path_c.as_ptr(), uid, gid) } != 0 {
            return Err(TlsError::Chown(std::io::Error::last_os_error()));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;
    use std::os::unix::fs::PermissionsExt as _;

    use tempfile::TempDir;
    use x509_parser::prelude::*;

    use super::*;

    fn pgdata() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        (dir, path)
    }

    /// Generate a cert, parse it back with x509-parser, and assert all required
    /// properties: CN non-empty, correct SANs, valid NotBefore/NotAfter, key mode 0o600.
    #[test]
    fn test_generate_and_parse() {
        let (_dir, pgdata) = pgdata();

        let outcome = ensure_cert(&pgdata).unwrap();
        assert_eq!(outcome, TlsCertOutcome::Generated);

        let cert_path = pgdata.join("beyond/server.crt");
        let key_path = pgdata.join("beyond/server.key");

        assert!(cert_path.exists(), "server.crt must exist");
        assert!(key_path.exists(), "server.key must exist");

        // File modes
        let key_mode = key_path.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(
            key_mode, 0o600,
            "server.key must be 0o600, got {key_mode:o}"
        );
        let cert_mode = cert_path.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(
            cert_mode, 0o644,
            "server.crt must be 0o644, got {cert_mode:o}"
        );

        // Parse
        let pem_bytes = std::fs::read(&cert_path).unwrap();
        let (_, pem_obj) = parse_x509_pem(&pem_bytes).expect("must parse PEM");
        let cert = pem_obj.parse_x509().expect("must parse X.509");

        // CN must be non-empty
        let cn = cert
            .subject()
            .iter_common_name()
            .next()
            .expect("must have CN");
        assert!(
            !cn.as_str().unwrap_or("").is_empty(),
            "CN must be non-empty"
        );

        // NotBefore <= now < NotAfter
        let now_i64 = now_secs() as i64;
        assert!(
            cert.validity().not_before.timestamp() <= now_i64,
            "NotBefore must be in the past"
        );
        assert!(
            cert.validity().not_after.timestamp() > now_i64,
            "NotAfter must be in the future"
        );

        // Validity ~1 year (±1 day tolerance)
        let validity_secs =
            cert.validity().not_after.timestamp() - cert.validity().not_before.timestamp();
        assert!(
            validity_secs >= 364 * 86400 && validity_secs <= 366 * 86400,
            "validity should be ~1 year, got {validity_secs}s"
        );

        // SANs
        let (mut has_localhost, mut has_beyond_dev, mut has_ip4, mut has_ip6) =
            (false, false, false, false);

        for ext in cert.extensions() {
            if let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() {
                for name in &san.general_names {
                    match name {
                        GeneralName::DNSName(s) => {
                            if *s == "localhost" {
                                has_localhost = true;
                            }
                            if *s == "*.beyond.dev" {
                                has_beyond_dev = true;
                            }
                        }
                        GeneralName::IPAddress(bytes) => {
                            if *bytes == [127u8, 0, 0, 1] {
                                has_ip4 = true;
                            }
                            if *bytes == Ipv6Addr::LOCALHOST.octets() {
                                has_ip6 = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        assert!(has_localhost, "cert must have DNS:localhost SAN");
        assert!(has_beyond_dev, "cert must have DNS:*.beyond.dev SAN");
        assert!(has_ip4, "cert must have IP:127.0.0.1 SAN");
        assert!(has_ip6, "cert must have IP:::1 SAN");
    }

    /// Second call to `ensure_cert` on an already-valid cert must return
    /// `StillValid` without rewriting the cert file.
    #[test]
    fn test_still_valid_on_second_call() {
        let (_dir, pgdata) = pgdata();

        let out1 = ensure_cert(&pgdata).unwrap();
        assert_eq!(out1, TlsCertOutcome::Generated);

        let cert_path = pgdata.join("beyond/server.crt");
        let mtime1 = cert_path.metadata().unwrap().modified().unwrap();

        // Brief pause so a rewrite would produce a different mtime.
        std::thread::sleep(std::time::Duration::from_millis(50));

        let out2 = ensure_cert(&pgdata).unwrap();
        assert_eq!(out2, TlsCertOutcome::StillValid);

        let mtime2 = cert_path.metadata().unwrap().modified().unwrap();
        assert_eq!(
            mtime1, mtime2,
            "cert must not be rewritten when still valid"
        );
    }

    /// A cert expiring within 30 days must be renewed.
    #[test]
    fn test_renew_near_expiry() {
        let (_dir, pgdata) = pgdata();
        let beyond_dir = pgdata.join("beyond");
        std::fs::create_dir_all(&beyond_dir).unwrap();
        write_expiring_cert(
            &beyond_dir.join("server.crt"),
            &beyond_dir.join("server.key"),
            20,
        );

        let outcome = ensure_cert(&pgdata).unwrap();
        assert_eq!(outcome, TlsCertOutcome::Renewed);
    }

    /// If `.user-managed` sentinel exists, `ensure_cert` must return `UserManaged`
    /// without writing any files.
    #[test]
    fn test_user_managed_sentinel() {
        let (_dir, pgdata) = pgdata();
        let beyond_dir = pgdata.join("beyond");
        std::fs::create_dir_all(&beyond_dir).unwrap();
        std::fs::write(beyond_dir.join(".user-managed"), "").unwrap();

        let outcome = ensure_cert(&pgdata).unwrap();
        assert_eq!(outcome, TlsCertOutcome::UserManaged);
        assert!(
            !beyond_dir.join("server.crt").exists(),
            "must not write cert when user-managed"
        );
        assert!(
            !beyond_dir.join("server.key").exists(),
            "must not write key when user-managed"
        );
    }

    /// `provision()` prefers the user-managed sentinel over the platform dir
    /// and the self-signed fallback. No cert files are generated.
    #[test]
    fn test_provision_user_managed_wins() {
        let (_dir, pgdata) = pgdata();
        let beyond_dir = pgdata.join("beyond");
        std::fs::create_dir_all(&beyond_dir).unwrap();
        std::fs::write(beyond_dir.join(".user-managed"), "").unwrap();

        // Also populate a fake platform dir to prove user-managed beats it.
        let platform = TempDir::new().unwrap();
        std::fs::write(platform.path().join("cert.pem"), "").unwrap();

        let tls = provision_with_paths(&pgdata, platform.path()).unwrap();
        assert_eq!(tls.source, TlsSource::UserManaged);
        assert_eq!(tls.cert, beyond_dir.join("server.crt"));
        assert_eq!(tls.key, beyond_dir.join("server.key"));
        assert!(
            tls.ca.is_none(),
            "user-managed CA is opt-in via 99-user.conf"
        );
        assert!(!beyond_dir.join("server.crt").exists(), "must not generate");
    }

    /// Platform cert wins over self-signed fallback when present.
    #[test]
    fn test_provision_platform_preferred() {
        let (_dir, pgdata) = pgdata();
        let platform = TempDir::new().unwrap();
        std::fs::write(platform.path().join("cert.pem"), "").unwrap();
        std::fs::write(platform.path().join("key.pem"), "").unwrap();
        std::fs::write(platform.path().join("ca.pem"), "").unwrap();

        let tls = provision_with_paths(&pgdata, platform.path()).unwrap();
        assert_eq!(tls.source, TlsSource::Platform);
        assert_eq!(tls.cert, platform.path().join("cert.pem"));
        assert_eq!(tls.key, platform.path().join("key.pem"));
        assert_eq!(
            tls.ca.as_deref(),
            Some(platform.path().join("ca.pem").as_path())
        );
        assert!(
            !pgdata.join("beyond/server.crt").exists(),
            "must not generate self-signed when platform cert present"
        );
    }

    /// No sentinel and no platform cert → self-signed generated under PGDATA.
    #[test]
    fn test_provision_self_signed_fallback() {
        let (_dir, pgdata) = pgdata();
        let platform = TempDir::new().unwrap(); // empty

        let tls = provision_with_paths(&pgdata, platform.path()).unwrap();
        assert_eq!(tls.source, TlsSource::SelfSigned);
        assert_eq!(tls.cert, pgdata.join("beyond/server.crt"));
        assert!(tls.ca.is_none());
        assert!(pgdata.join("beyond/server.crt").exists());
        assert!(pgdata.join("beyond/server.key").exists());
    }

    /// Write a self-signed cert that expires in `days_until_expiry` days,
    /// used to simulate a near-expiry cert for renewal tests.
    fn write_expiring_cert(cert_path: &Path, key_path: &Path, days_until_expiry: i64) {
        let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let mut params = CertificateParams::default();
        // Use ::time:: prefix to avoid shadowing by x509_parser::prelude::*.
        let now = ::time::OffsetDateTime::now_utc();
        params.not_before = now - ::time::Duration::days(CERT_VALIDITY_DAYS - days_until_expiry);
        params.not_after = now + ::time::Duration::days(days_until_expiry);
        let cert = params.self_signed(&key_pair).unwrap();
        std::fs::write(cert_path, cert.pem()).unwrap();
        std::fs::write(key_path, key_pair.serialize_pem()).unwrap();
    }
}
