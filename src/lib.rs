pub mod cache;
pub mod config;
pub mod downloader;
pub mod error;
pub mod metrics;
pub mod server;
pub mod storage;

use axum::{
    extract::{Path, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use cache::CacheManager;
use config::Settings;
use downloader::Downloader;
use error::{ProxyError, Result};
use std::sync::Arc;
use std::time::Instant;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::catch_panic::CatchPanicLayer;
use tracing::{info, warn};

pub struct App {
    pub settings: Settings,
    downloader: Downloader,
}

impl App {
    pub async fn new(settings: Settings) -> anyhow::Result<Self> {
        let storage = Arc::new(storage::Storage::new(settings.cache_dir.clone()).await?);
        let cache_config = Arc::new(settings.cache.clone());
        let cache = CacheManager::new(storage, cache_config, settings.max_cache_size).await?;
        let downloader = Downloader::new(cache);

        Ok(Self { settings, downloader })
    }

    fn resolve_url(&self, path: &str) -> Option<String> {
        let (prefix, rest) = path.split_once('/')?;
        let base = self.settings.repositories.get(prefix)?;
        Some(format!("{}/{}", base.trim_end_matches('/'), rest))
    }
}

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/health", get(|| async { "OK" }))
        .route("/metrics", get(metrics_handler))
        .route("/{*path}", get(proxy_handler))
        .layer(
            ServiceBuilder::new()
                .layer(CatchPanicLayer::new())
                .layer(ConcurrencyLimitLayer::new(1000)),
        )
        .layer(middleware::from_fn(logging_middleware))
        .with_state(app)
}

async fn logging_middleware(req: axum::extract::Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let start = Instant::now();

    let resp = next.run(req).await;

    let status = resp.status().as_u16();
    let ms = start.elapsed().as_millis();

    if status < 400 {
        info!(status, path, ms, "");
    } else {
        warn!(status, path, ms, "");
    }

    resp
}

async fn metrics_handler() -> impl IntoResponse {
    match metrics::render() {
        Some(m) => ([(axum::http::header::CONTENT_TYPE, "text/plain")], m),
        None => (
            [(axum::http::header::CONTENT_TYPE, "text/plain")],
            "# Prometheus disabled\n".to_string(),
        ),
    }
}

async fn proxy_handler(
    Path(path): Path<String>,
    State(app): State<Arc<App>>,
) -> Result<Response> {
    validate_path(&path)?;

    let url = app.resolve_url(&path).ok_or(ProxyError::RepositoryNotFound)?;
    app.downloader.fetch(&path, &url).await
}

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > 2048 {
        return Err(ProxyError::InvalidPath("Invalid length".into()));
    }

    if memchr::memchr(0, path.as_bytes()).is_some() {
        return Err(ProxyError::InvalidPath("Null byte".into()));
    }

    let clean = path_clean::PathClean::clean(std::path::Path::new(path));
    if clean.to_string_lossy().starts_with("..") {
        return Err(ProxyError::InvalidPath("Path traversal".into()));
    }

    Ok(())
}