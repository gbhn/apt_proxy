use axum::{body::Body, extract::Request, Router};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{net::SocketAddr, path::Path};
use tokio::{
    fs,
    net::{TcpListener, UnixListener},
};
use tower::Service;
use tracing::{error, info, warn};

pub async fn serve_tcp(app: Router, port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!(address = %addr, "Starting TCP server");

    let listener = TcpListener::bind(addr).await?;

    info!(address = %addr, "Server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server stopped");
    Ok(())
}

pub async fn serve_unix(app: Router, socket_path: &Path) -> anyhow::Result<()> {
    info!(socket = %socket_path.display(), "Starting Unix socket server");

    if socket_path.exists() {
        warn!(
            socket = %socket_path.display(),
            "Removing existing socket file"
        );
        fs::remove_file(socket_path).await?;
    }

    let listener = UnixListener::bind(socket_path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o660)).await?;
    }

    info!(socket = %socket_path.display(), "Server listening");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, _) = result?;
                let io = TokioIo::new(socket);
                let app = app.clone();

                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                        let mut app = app.clone();
                        async move {
                            let (parts, incoming) = req.into_parts();
                            app.call(Request::from_parts(parts, Body::new(incoming))).await
                        }
                    });

                    if let Err(err) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await
                    {
                        error!(error = %err, "Unix socket connection error");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
                break;
            }
        }
    }

    if socket_path.exists() {
        fs::remove_file(socket_path).await.ok();
    }

    info!("Server stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler");
        term.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C"),
        _ = terminate => info!("Received SIGTERM"),
    }

    info!("Initiating graceful shutdown...");
}