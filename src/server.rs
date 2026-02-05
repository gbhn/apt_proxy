use axum::Router;
use listenfd::ListenFd;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

pub async fn serve(app: Router, port: u16) -> anyhow::Result<()> {
    let listener = match ListenFd::from_env().take_tcp_listener(0)? {
        Some(std_listener) => {
            std_listener.set_nonblocking(true)?;
            info!("Using systemd socket activation");
            TcpListener::from_std(std_listener)?
        }
        None => {
            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            info!(%addr, "Binding to address");
            TcpListener::bind(addr).await?
        }
    };

    info!(addr = %listener.local_addr()?, "Server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server stopped gracefully");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

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
        _ = ctrl_c => info!("Received Ctrl+C"),
        _ = terminate => info!("Received SIGTERM"),
    }

    info!("Initiating graceful shutdown...");
}