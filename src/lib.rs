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
use std::time::{Duration, Instant};
use tower::limit::ConcurrencyLimitLayer;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tracing::{info, warn};

const MAX_PATH_LENGTH: usize = 2048;
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const MAX_CONCURRENT_REQUESTS: usize = 1000;

pub struct App {
    pub settings: Settings,
    downloader: Downloader,
}

impl App {
    pub async fn new(settings: Settings) -> anyhow::Result<Self> {
        let storage = Arc::new(storage::Storage::new(settings.cache_dir.clone()).await?);
        let cache_config = Arc::new(settings.cache.clone());
        let cache = CacheManager::new(storage, cache_config, settings.max_cache_size).await?;

        // Background maintenance task
        let maintenance_cache = cache.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(MAINTENANCE_INTERVAL);
            loop {
                interval.tick().await;
                maintenance_cache.run_maintenance().await;
            }
        });

        Ok(Self {
            settings,
            downloader: Downloader::new(cache),
        })
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
                .layer(ConcurrencyLimitLayer::new(MAX_CONCURRENT_REQUESTS)),
        )
        .layer(middleware::from_fn(request_logging))
        .with_state(app)
}

async fn request_logging(req: axum::extract::Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let start = Instant::now();

    let resp = next.run(req).await;

    let status = resp.status().as_u16();
    let elapsed_ms = start.elapsed().as_millis();

    if status < 400 {
        info!(status, path, ms = elapsed_ms, "");
    } else {
        warn!(status, path, ms = elapsed_ms, "");
    }

    resp
}

async fn metrics_handler() -> impl IntoResponse {
    let content_type = [(axum::http::header::CONTENT_TYPE, "text/plain")];
    match metrics::render() {
        Some(m) => (content_type, m),
        None => (content_type, "# Prometheus disabled\n".to_string()),
    }
}

async fn proxy_handler(Path(path): Path<String>, State(app): State<Arc<App>>) -> Result<Response> {
    validate_path(&path)?;
    let url = app.resolve_url(&path).ok_or(ProxyError::RepositoryNotFound)?;
    app.downloader.fetch(&path, &url).await
}

/// Validates and sanitizes request path to prevent path traversal and other attacks
fn validate_path(path: &str) -> Result<()> {
    // Length check
    if path.is_empty() || path.len() > MAX_PATH_LENGTH {
        return Err(ProxyError::InvalidPath("Invalid path length".into()));
    }

    // Must have repository prefix
    if !path.contains('/') {
        return Err(ProxyError::InvalidPath("Missing repository prefix".into()));
    }

    // Decode and validate UTF-8
    let decoded = percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| ProxyError::InvalidPath("Invalid UTF-8 encoding".into()))?;

    // Forbidden characters/patterns
    let forbidden_checks = [
        (decoded.contains('\0'), "Null byte not allowed"),
        (decoded.contains('\\'), "Backslashes not allowed"),
        (decoded.starts_with('/'), "Absolute path not allowed"),
        (decoded.contains("//"), "Double slashes not allowed"),
    ];

    for (condition, message) in forbidden_checks {
        if condition {
            return Err(ProxyError::InvalidPath(message.into()));
        }
    }

    // Component validation
    for component in decoded.split('/').filter(|c| !c.is_empty()) {
        if component == "." || component == ".." {
            return Err(ProxyError::InvalidPath("Path traversal not allowed".into()));
        }
        if component.starts_with('.') && component != ".well-known" {
            return Err(ProxyError::InvalidPath("Hidden files not allowed".into()));
        }
    }

    // Final normalization check
    let normalized = path_clean::PathClean::clean(std::path::Path::new(decoded.as_ref()));
    let normalized_str = normalized.to_string_lossy();

    if normalized_str.starts_with("..") || normalized_str.starts_with(['/', '\\']) {
        return Err(ProxyError::InvalidPath("Path traversal detected".into()));
    }

    Ok(())
}