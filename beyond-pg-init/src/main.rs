//! `beyond-pg-init` — PID 1 for the beyond-pg VM image.
//!
//! See [`bootsetup`] for the one-shot boot work (mounts, network, MMDS, zram)
//! and [`supervise`] for the long-running poll(2) loop that supervises the
//! `beyond-pg supervisor` child and serves the vsock lifecycle listener.

#![deny(unsafe_code)]
#![deny(unused_must_use)]

#[cfg(target_os = "linux")]
mod bootsetup;
#[cfg(target_os = "linux")]
mod substrate;
#[cfg(target_os = "linux")]
mod supervise;
#[cfg(target_os = "linux")]
mod volumes;

#[cfg(target_os = "linux")]
fn main() -> ! {
    if std::process::id() != 1 {
        eprintln!(
            "[init] not PID 1; refusing to run (current pid={})",
            std::process::id()
        );
        std::process::exit(1);
    }
    bootsetup::run();
    // Report "guest ready" to the host substrate over vsock and keep the
    // connection alive for the VM's lifetime. instd waits for this handshake
    // before considering the create successful. Spawned after boot setup (so
    // the network/MMDS are up) and before the supervise loop takes over.
    substrate::spawn_handshake();
    supervise::run();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("beyond-pg-init: only runs on Linux (vsock + signalfd + pidfd required)");
    std::process::exit(1);
}
