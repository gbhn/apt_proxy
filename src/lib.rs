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
use percent_encoding::percent_decode_str;
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
    // Length check on raw path
    if path.is_empty() || path.len() > 2048 {
        return Err(ProxyError::InvalidPath("Invalid length".into()));
    }

    // Decode percent-encoding for security checks
    let decoded = percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| ProxyError::InvalidPath("Invalid UTF-8 encoding".into()))?;

    // Check for null bytes
    if decoded.contains('\0') {
        return Err(ProxyError::InvalidPath("Null byte not allowed".into()));
    }

    // Check for backslashes (Windows path separator)
    if decoded.contains('\\') {
        return Err(ProxyError::InvalidPath("Backslashes not allowed".into()));
    }

    // Check for absolute paths
    if decoded.starts_with('/') {
        return Err(ProxyError::InvalidPath("Absolute path not allowed".into()));
    }

    // Check for double slashes
    if decoded.contains("//") {
        return Err(ProxyError::InvalidPath("Double slashes not allowed".into()));
    }

    // Check each path component for traversal attempts
    for component in decoded.split('/') {
        // Empty components are already caught by // check above, but be safe
        if component.is_empty() {
            continue;
        }

        // Block . and .. explicitly
        if component == "." || component == ".." {
            return Err(ProxyError::InvalidPath("Path traversal not allowed".into()));
        }

        // Block hidden files except .well-known
        if component.starts_with('.') && component != ".well-known" {
            return Err(ProxyError::InvalidPath("Hidden files not allowed".into()));
        }
    }

    // Final check: normalize path and verify no traversal
    let normalized = path_clean::PathClean::clean(std::path::Path::new(decoded.as_ref()));
    let normalized_str = normalized.to_string_lossy();

    // Check if normalized path escapes
    if normalized_str.starts_with("..") {
        return Err(ProxyError::InvalidPath("Path traversal detected".into()));
    }

    // Also check the normalized path doesn't start with /
    if normalized_str.starts_with('/') || normalized_str.starts_with('\\') {
        return Err(ProxyError::InvalidPath("Path traversal detected".into()));
    }

    Ok(())
}