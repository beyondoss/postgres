mod archive;
mod boot;
mod cert_watcher;
#[cfg(target_os = "linux")]
mod children;
mod config;
#[cfg(target_os = "linux")]
mod handoff_bridge;
mod init;
mod log_forwarder;
mod mmds;
mod pg;
mod rpc;
mod sql;
mod supervisor;
mod tls;
mod vsock;
mod wal_forwarder;

#[derive(clap::Parser)]
#[command(name = "beyond-pg", about = "Beyond Postgres agent")]
enum Cmd {
    /// Run the supervisor (boot + supervise postgres + pgbouncer)
    Supervisor,
    /// Run idempotent boot setup and exit
    Boot,
    /// Archive a WAL segment (called by archive_command)
    Archive {
        /// WAL segment path (%p)
        path: String,
        /// WAL segment filename (%f)
        filename: String,
    },
}

fn main() {
    use clap::Parser;
    let cmd = Cmd::parse();

    match cmd {
        Cmd::Archive { path, filename } => {
            archive::run(&path, &filename);
        }
        Cmd::Supervisor => {
            init::run();
            // handoff::detect_role mutates env vars (LISTEN_FDS, HANDOFF_ROLE,
            // HANDOFF_SOCK_FD). Per its module contract, it must run on a
            // single-threaded context before any tokio worker starts. We're
            // still on the main thread here, before runtime construction.
            #[cfg(target_os = "linux")]
            let role = handoff::detect_role().expect("handoff::detect_role");
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime")
                .block_on(async move {
                    #[cfg(target_os = "linux")]
                    supervisor::run(role).await;
                    #[cfg(not(target_os = "linux"))]
                    supervisor::run().await;
                });
        }
        Cmd::Boot => {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime")
                .block_on(boot::run());
        }
    }
}
