mod archive;
mod boot;
mod config;
mod init;
mod log_forwarder;
mod mmds;
mod pg;
mod rpc;
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
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime")
                .block_on(supervisor::run());
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
