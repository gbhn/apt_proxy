use crate::metrics;
use anyhow::{Context, Result};
use axum::Router;
use listenfd::ListenFd;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

pub async fn serve(app: Router, port: u16) -> Result<()> {
    let listener = match ListenFd::from_env().take_tcp_listener(0)? {
        Some(std) => {
            std.set_nonblocking(true)?;
            info!("Using systemd socket");
            TcpListener::from_std(std)?
        }
        None => {
            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            info!(%addr, "Binding");
            TcpListener::bind(addr).await?
        }
    };

    info!(addr = %listener.local_addr()?, "Listening");
    metrics::set_health_status(true);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
        .context("Server error")
}

async fn shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    
    #[cfg(unix)]
    let term = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Ctrl+C"),
        _ = term => info!("SIGTERM"),
    }
}