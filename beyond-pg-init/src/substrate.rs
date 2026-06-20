//! Substrate vsock "guest ready" handshake for `beyond-pg-init`.
//!
//! instd waits ~30s after spawning firecracker for the guest to complete the
//! [`guest_runtime::VsockClient::connect`] handshake (which sends a `Ready`
//! message); without it instd reports "guest not ready: timeout" and the
//! create fails. `beyond-pg-init` is a sync PID 1 (see [`crate::supervise`]),
//! so we host the async handshake + keep-alive loop on a dedicated OS thread
//! running a current-thread tokio runtime for the VM's lifetime.
//!
//! The handshake is soft-fail: if the connection can't be established (e.g. no
//! AF_VSOCK in a Docker test environment) we log a warning and let the thread
//! exit, exactly as `service-init`'s `EnvelopeSink::try_connect` does. The
//! supervise loop owns shutdown via signalfd (instd also sends SIGTERM), so on
//! a `Shutdown` event we only log — we never poweroff from this thread.

/// Spawn the dedicated `substrate-vsock` thread that performs the guest-runtime
/// `Ready` handshake and then keeps the connection alive for the VM's lifetime.
///
/// Returns immediately; the handshake happens on the spawned thread. Soft-fail
/// throughout: a failed spawn or a failed connect is logged, never fatal.
pub fn spawn_handshake() {
    let builder = std::thread::Builder::new().name("substrate-vsock".to_string());
    if let Err(e) = builder.spawn(run) {
        eprintln!("[init] WARNING: failed to spawn substrate-vsock thread: {e}");
    }
}

fn run() {
    // Current-thread runtime: this thread does nothing but host the single
    // vsock connection and its keep-alive loop, so a multi-thread scheduler
    // would only waste worker threads.
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[init] WARNING: substrate-vsock tokio runtime build failed: {e}");
            return;
        }
    };
    runtime.block_on(handshake_loop());
}

async fn handshake_loop() {
    let cfg = guest_runtime::VsockClientConfig::for_primitive(format!(
        "beyond-pg-init/{}",
        env!("CARGO_PKG_VERSION")
    ));
    let mut client = match guest_runtime::VsockClient::connect(&cfg).await {
        Ok(client) => {
            eprintln!("[init] substrate vsock handshake complete; guest reported ready");
            client
        }
        Err(e) => {
            // Soft-fail like service-init: no vsock (e.g. Docker tests) just
            // means no host substrate to report to. Let the thread exit.
            eprintln!("[init] WARNING: substrate vsock connect failed; guest-ready unreported: {e}");
            return;
        }
    };

    // Serve the administrative exec/ping/shell channel on the same connection
    // and keep it alive so instd sees the guest as live. The loop only exits
    // on a Shutdown event or a connection error.
    client.enable_admin(guest_runtime::SessionConfig::default());
    loop {
        match client.next_event().await {
            Ok(guest_runtime::SubstrateEvent::Shutdown) => {
                // The supervise loop owns shutdown via signalfd (instd also
                // sends SIGTERM); we only log and let this thread exit.
                eprintln!("[init] substrate requested shutdown; vsock loop exiting");
                break;
            }
            Ok(guest_runtime::SubstrateEvent::AppMessage(_)) => {
                // beyond-pg-init has no workload-env channel; ignore app frames.
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("[init] substrate vsock loop ended (connection closed): {e}");
                break;
            }
        }
    }
}
