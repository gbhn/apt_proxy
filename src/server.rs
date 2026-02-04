use axum::{body::Body, extract::Request, Router};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{net::SocketAddr, path::Path};
use tokio::{fs, net::{TcpListener, UnixListener}};
use tower::Service;
use tracing::{error, info};

pub async fn serve_tcp(app: Router, port: u16) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Listening on TCP {}", addr);
    
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

pub async fn serve_unix(app: Router, socket_path: &Path) -> anyhow::Result<()> {
    info!("Listening on Unix socket {:?}", socket_path);
    
    if socket_path.exists() {
        fs::remove_file(socket_path).await?;
    }

    let listener = UnixListener::bind(socket_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o660)).await?;
    }

    loop {
        let (socket, _) = listener.accept().await?;
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
                error!("Unix socket connection error: {}", err);
            }
        });
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! { 
        _ = ctrl_c => info!("Received Ctrl+C, shutting down"),
        _ = terminate => info!("Received SIGTERM, shutting down")
    }
}