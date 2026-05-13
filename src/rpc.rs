//! Vsock control RPC server — `beyond-pg supervisor` binds this on `RPC_PORT`.
//!
//! Wire format: length-prefixed MessagePack.
//!   Request:  [4-byte BE u32 length][msgpack RpcRequest]
//!   Response: [4-byte BE u32 length][msgpack RpcResponse]
//!
//! Commands:
//!   checkpoint → CHECKPOINT via psql
//!   health     → pg_isready
//!   reload     → pg_ctl reload
//!   promote    → pg_ctl promote (replica → primary; host decides when)
//!   backup     → stub (not implemented)
//!
//! Only available on Linux (vsock is a Linux kernel feature).

#[cfg(target_os = "linux")]
mod inner {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_vsock::{VMADDR_CID_ANY, VsockAddr, VsockListener, VsockStream};
    use tracing::{error, info, warn};

    use crate::vsock::RPC_PORT;

    const MAX_RPC_BODY: usize = 1024 * 1024; // 1 MiB — guards against host sending a huge length
    const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    #[derive(serde::Deserialize)]
    struct RpcRequest {
        cmd: String,
    }

    #[derive(serde::Serialize)]
    struct RpcResponse {
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    }

    impl RpcResponse {
        fn ok() -> Self {
            Self {
                ok: true,
                error: None,
            }
        }
        fn err(msg: impl Into<String>) -> Self {
            Self {
                ok: false,
                error: Some(msg.into()),
            }
        }
    }

    pub async fn serve() {
        let listener = match VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, RPC_PORT)) {
            Ok(l) => l,
            Err(e) => {
                error!("rpc: failed to bind vsock port {RPC_PORT}: {e}");
                return;
            }
        };
        info!("rpc: listening on vsock port {RPC_PORT}");

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("rpc: connection from {addr:?}");
                    tokio::spawn(async move {
                        let result = tokio::time::timeout(RPC_TIMEOUT, handle(stream)).await;
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => warn!("rpc: connection error: {e}"),
                            Err(_) => warn!("rpc: connection timed out"),
                        }
                    });
                }
                Err(e) => {
                    error!("rpc: accept error: {e}");
                }
            }
        }
    }

    async fn handle(mut stream: VsockStream) -> std::io::Result<()> {
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
            Ok(req) => dispatch(&req.cmd).await,
        };

        let encoded =
            rmp_serde::to_vec_named(&response).expect("RpcResponse serialization is infallible");
        let resp_len = encoded.len() as u32;
        stream.write_all(&resp_len.to_be_bytes()).await?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;

        Ok(())
    }

    async fn dispatch(cmd: &str) -> RpcResponse {
        match cmd {
            "checkpoint" => match crate::pg::psql("CHECKPOINT").await {
                Ok(()) => RpcResponse::ok(),
                Err(e) => RpcResponse::err(e.to_string()),
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
pub use inner::serve;

#[cfg(not(target_os = "linux"))]
pub async fn serve() {
    tracing::warn!("rpc: vsock not available on this platform");
}
