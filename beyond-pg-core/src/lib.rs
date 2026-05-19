//! Shared sync-only code for `beyond-pg-init` (PID 1) and `beyond-pg` (supervisor).
//!
//! Anything tokio-flavored stays in `beyond-pg`. This crate is intentionally
//! cheap to link into PID 1.

pub mod mmds;
pub mod vsock;
