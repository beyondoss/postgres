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
mod template;
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
    /// Build a pre-initialized PGDATA template (initdb + extensions) at <dir>.
    /// Run at image-build time; baked into the rootfs and copied onto the data
    /// volume on first boot instead of running initdb in the guest.
    BuildTemplate {
        /// Output directory for the template (default: the rootfs template dir).
        #[arg(default_value = template::TEMPLATE_DIR)]
        dir: String,
    },
}

fn main() {
    use clap::Parser;
    let cmd = Cmd::parse();

    match cmd {
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
        Cmd::BuildTemplate { dir } => {
            // The Debian postgresql server binaries (initdb, postgres) live in
            // /usr/lib/postgresql/18/bin and are NOT on the default PATH — at
            // runtime PID 1 puts them there, but the image-build chroot does not.
            // Set it before the runtime starts (still single-threaded).
            // SAFETY: main thread, before any tokio worker exists.
            unsafe {
                std::env::set_var(
                    "PATH",
                    format!(
                        "/usr/lib/postgresql/18/bin:{}",
                        std::env::var("PATH").unwrap_or_default()
                    ),
                );
            }
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime")
                .block_on(template::run(&dir));
        }
    }
}
