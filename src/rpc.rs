//! Control RPC server — `beyond-pg supervisor` serves this on the inherited
//! listener (vsock in production; unix-socket fallback for tests / local dev).
//!
//! Wire format: length-prefixed MessagePack.
//!   Request:  [4-byte BE u32 length][msgpack RpcRequest]
//!   Response: [4-byte BE u32 length][msgpack RpcResponse]
//!
//! Commands:
//!   checkpoint → CHECKPOINT via psql
//!   health     → pg_isready
//!   reload     → pg_ctl reload
//!   promote    → pg_ctl promote -w (blocks until standby exits recovery; ok:true = primary)
//!   pooler     → PgBouncer scaler telemetry (live/max workers, CPU, at_ceiling)
//!   backup     → stub (not implemented)
//!
//! The RPC server is Linux-only. The pooler telemetry types
//! ([`crate::handoff_bridge::PoolerStats`]) live in the library (next to
//! `SharedState`, which carries the handle) so the supervisor's scaler can
//! populate them on any host.

#[cfg(target_os = "linux")]
mod inner {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener};
    use tracing::{error, info, warn};

    use crate::handoff_bridge::SharedState;
    use crate::vsock::RPC_PORT;

    const MAX_RPC_BODY: usize = 1024 * 1024;
    const RPC_TIMEOUT: Duration = Duration::from_secs(60);

    /// Carries one of two concrete listener types behind a single accept-loop.
    ///
    /// Production: vsock (Firecracker has it as a virtio device, no extra caps).
    /// Tests / local dev: unix domain socket (vsock is not available on most
    /// host kernels without `vhost_vsock` module + capabilities).
    pub enum RpcListener {
        Vsock(VsockListener),
        Unix(tokio::net::UnixListener),
    }

    impl RpcListener {
        async fn accept(&self) -> std::io::Result<RpcStream> {
            match self {
                RpcListener::Vsock(l) => {
                    let (s, _addr) = l.accept().await?;
                    Ok(RpcStream::Vsock(s))
                }
                RpcListener::Unix(l) => {
                    let (s, _addr) = l.accept().await?;
                    Ok(RpcStream::Unix(s))
                }
            }
        }
    }

    enum RpcStream {
        Vsock(tokio_vsock::VsockStream),
        Unix(tokio::net::UnixStream),
    }

    #[derive(serde::Deserialize)]
    struct RpcRequest {
        cmd: String,
    }

    #[derive(serde::Serialize)]
    struct RpcResponse {
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pooler: Option<crate::handoff_bridge::PoolerStats>,
    }

    impl RpcResponse {
        fn ok() -> Self {
            Self {
                ok: true,
                error: None,
                pooler: None,
            }
        }
        fn err(msg: impl Into<String>) -> Self {
            Self {
                ok: false,
                error: Some(msg.into()),
                pooler: None,
            }
        }
        fn pooler(stats: crate::handoff_bridge::PoolerStats) -> Self {
            Self {
                ok: true,
                error: None,
                pooler: Some(stats),
            }
        }
    }

    /// Serve the control RPC.
    ///
    /// `listener` is the bound listener (vsock in production, unix-socket
    /// fallback for tests / local dev). `state` shares `accept_paused` +
    /// `in_flight` with the handoff `Drainable` so drain can pause accepts
    /// and wait for in-flight handlers to complete.
    pub async fn serve(listener: RpcListener, state: SharedState) {
        let kind = match &listener {
            RpcListener::Vsock(_) => format!("vsock port {RPC_PORT}"),
            RpcListener::Unix(_) => "unix socket".to_string(),
        };
        info!("rpc: listening on {kind}");

        loop {
            // Honor the drain pause without spinning the accept loop tight.
            while state.accept_paused.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }

            match listener.accept().await {
                Ok(stream) => {
                    let in_flight = Arc::clone(&state.in_flight);
                    let pooler = Arc::clone(&state.pooler);
                    in_flight.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(async move {
                        let result =
                            tokio::time::timeout(RPC_TIMEOUT, handle_stream(stream, pooler)).await;
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => warn!("rpc: connection error: {e}"),
                            Err(_) => warn!("rpc: connection timed out"),
                        }
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                Err(e) => {
                    error!("rpc: accept error: {e}");
                }
            }
        }
    }

    /// Bind a fresh listener for the cold-start path. Picks the unix-socket
    /// fallback when `BEYOND_PG_RPC_UNIX_PATH` is set; otherwise binds vsock
    /// on [`RPC_PORT`]. Tests set the env var; production leaves it unset.
    pub fn bind_cold_start() -> std::io::Result<RpcListener> {
        if let Ok(path) = std::env::var("BEYOND_PG_RPC_UNIX_PATH") {
            let _ = std::fs::remove_file(&path);
            let std_listener = std::os::unix::net::UnixListener::bind(&path)?;
            std_listener.set_nonblocking(true)?;
            let listener = tokio::net::UnixListener::from_std(std_listener)?;
            return Ok(RpcListener::Unix(listener));
        }
        let v = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, RPC_PORT))?;
        Ok(RpcListener::Vsock(v))
    }

    async fn handle_stream(
        stream: RpcStream,
        pooler: crate::handoff_bridge::PoolerStatsHandle,
    ) -> std::io::Result<()> {
        match stream {
            RpcStream::Vsock(s) => handle(s, pooler).await,
            RpcStream::Unix(s) => handle(s, pooler).await,
        }
    }

    async fn handle<S>(
        mut stream: S,
        pooler: crate::handoff_bridge::PoolerStatsHandle,
    ) -> std::io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_RPC_BODY {
            return Err(std::io::Error::other(format!(
                "rpc body too large: {len} bytes"
            )));
        }

        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await?;

        let response = match rmp_serde::from_slice::<RpcRequest>(&body) {
            Err(e) => RpcResponse::err(format!("invalid request: {e}")),
            Ok(req) => dispatch(&req.cmd, &pooler).await,
        };

        let encoded =
            rmp_serde::to_vec_named(&response).expect("RpcResponse serialization is infallible");
        let resp_len = encoded.len() as u32;
        stream.write_all(&resp_len.to_be_bytes()).await?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;

        Ok(())
    }

    async fn dispatch(cmd: &str, pooler: &crate::handoff_bridge::PoolerStatsHandle) -> RpcResponse {
        match cmd {
            "checkpoint" => match crate::pg::psql("CHECKPOINT").await {
                Ok(()) => RpcResponse::ok(),
                Err(e) => RpcResponse::err(e.to_string()),
            },
            "pooler" => match pooler.lock() {
                Ok(stats) => RpcResponse::pooler(stats.clone()),
                Err(_) => RpcResponse::err("pooler stats lock poisoned"),
            },
            "health" => {
                if crate::pg::is_ready().await {
                    RpcResponse::ok()
                } else {
                    RpcResponse::err("postgres not ready")
                }
            }
            "reload" => match crate::pg::reload().await {
                Ok(()) => RpcResponse::ok(),
                Err(e) => RpcResponse::err(e.to_string()),
            },
            "promote" => match crate::pg::promote().await {
                Ok(()) => RpcResponse::ok(),
                Err(e) => RpcResponse::err(e.to_string()),
            },
            "backup" => RpcResponse::err("not implemented"),
            other => {
                warn!("rpc: unknown command {other:?}");
                RpcResponse::err(format!("unknown command: {other}"))
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub use inner::{RpcListener, bind_cold_start, serve};

#[cfg(not(target_os = "linux"))]
pub async fn serve(_listener: (), _state: crate::handoff_bridge::SharedState) {
    tracing::warn!("rpc: vsock not available on this platform");
}
