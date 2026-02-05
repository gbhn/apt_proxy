//! Server module с поддержкой systemd socket activation через listenfd

use axum::{body::Body, extract::Request, Router};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use listenfd::ListenFd;
use std::{net::SocketAddr, path::Path};
use tokio::{
    fs,
    net::{TcpListener, UnixListener},
};
use tower::Service;
use tracing::{error, info, warn};

/// Запускает TCP сервер с поддержкой systemd socket activation
pub async fn serve_tcp(app: Router, port: u16) -> anyhow::Result<()> {
    // Пробуем получить сокет от systemd
    let listener = match get_systemd_tcp_listener()? {
        Some(listener) => {
            info!("Using systemd socket activation");
            listener
        }
        None => {
            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            info!(address = %addr, "Starting TCP server");
            TcpListener::bind(addr).await?
        }
    };

    let local_addr = listener.local_addr()?;
    info!(address = %local_addr, "Server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server stopped");
    Ok(())
}

/// Пробует получить TCP listener от systemd через listenfd
fn get_systemd_tcp_listener() -> anyhow::Result<Option<TcpListener>> {
    let mut listenfd = ListenFd::from_env();
    
    if let Some(listener) = listenfd.take_tcp_listener(0)? {
        listener.set_nonblocking(true)?;
        let tokio_listener = TcpListener::from_std(listener)?;
        return Ok(Some(tokio_listener));
    }
    
    Ok(None)
}

/// Запускает Unix socket сервер
pub async fn serve_unix(app: Router, socket_path: &Path) -> anyhow::Result<()> {
    // Пробуем systemd socket activation для Unix sockets
    let listener = match get_systemd_unix_listener()? {
        Some(listener) => {
            info!("Using systemd Unix socket activation");
            listener
        }
        None => {
            info!(socket = %socket_path.display(), "Starting Unix socket server");

            if socket_path.exists() {
                warn!(socket = %socket_path.display(), "Removing existing socket file");
                fs::remove_file(socket_path).await?;
            }

            let listener = UnixListener::bind(socket_path)?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o660)).await?;
            }

            listener
        }
    };

    info!(socket = %socket_path.display(), "Server listening");

    // Используем tokio::select для graceful shutdown
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

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
            _ = &mut shutdown => {
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

/// Пробует получить Unix listener от systemd
#[cfg(unix)]
fn get_systemd_unix_listener() -> anyhow::Result<Option<UnixListener>> {
    use std::os::unix::io::{FromRawFd, RawFd};
    
    let mut listenfd = ListenFd::from_env();
    
    // listenfd не имеет прямого метода для Unix sockets,
    // но можно использовать take_raw_fd
    if let Some(fd) = listenfd.take_raw_fd(0)? {
        let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(fd as RawFd) };
        std_listener.set_nonblocking(true)?;
        let tokio_listener = UnixListener::from_std(std_listener)?;
        return Ok(Some(tokio_listener));
    }
    
    Ok(None)
}

#[cfg(not(unix))]
fn get_systemd_unix_listener() -> anyhow::Result<Option<UnixListener>> {
    Ok(None)
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