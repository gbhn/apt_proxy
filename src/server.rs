use crate::metrics;
use anyhow::{Context, Result};
use axum::Router;
use listenfd::ListenFd;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

/// Starts the HTTP server with graceful shutdown support
pub async fn serve(app: Router, port: u16) -> Result<()> {
    let listener = create_listener(port).await?;

    let local_addr = listener
        .local_addr()
        .context("Failed to get local address")?;

    info!(addr = %local_addr, "Server listening");

    // Set health status to healthy once we're listening
    metrics::set_health_status(true);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Server error")?;

    // Set health status to unhealthy during shutdown
    metrics::set_health_status(false);
    
    info!("Server stopped gracefully");
    Ok(())
}

/// Creates a TCP listener, preferring systemd socket activation
async fn create_listener(port: u16) -> Result<TcpListener> {
    // Try systemd socket activation first
    if let Some(std_listener) = ListenFd::from_env()
        .take_tcp_listener(0)
        .context("Failed to take systemd socket")?
    {
        std_listener
            .set_nonblocking(true)
            .context("Failed to set socket non-blocking")?;
        info!("Using systemd socket activation");
        return TcpListener::from_std(std_listener).context("Failed to convert socket");
    }

    // Fall back to binding directly
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!(%addr, "Binding to address");
    TcpListener::bind(addr)
        .await
        .context("Failed to bind to address")
}

/// Waits for shutdown signals (Ctrl+C or SIGTERM)
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C, shutting down..."),
        _ = terminate => info!("Received SIGTERM, shutting down..."),
    }
}