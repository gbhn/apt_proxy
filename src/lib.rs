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
use tower::limit::ConcurrencyLimitLayer;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tracing::{info, warn};

/// Application configuration constants
mod app_config {
    use std::time::Duration;

    pub const MAX_PATH_LENGTH: usize = 2048;
    pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
    pub const MAX_CONCURRENT_REQUESTS: usize = 1000;
}

/// Main application state
pub struct App {
    pub settings: Settings,
    downloader: Downloader,
}

impl App {
    /// Creates a new application instance
    pub async fn new(settings: Settings) -> anyhow::Result<Self> {
        let storage = Arc::new(storage::Storage::new(settings.cache_dir.clone()).await?);
        let cache_config = Arc::new(settings.cache.clone());
        let cache = CacheManager::new(storage, cache_config, settings.max_cache_size).await?;

        // Start background maintenance task
        let maintenance_cache = cache.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(app_config::MAINTENANCE_INTERVAL);
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

    /// Resolves a path to an upstream URL
    fn resolve_url(&self, path: &str) -> Option<(String, String)> {
        let (prefix, rest) = path.split_once('/')?;
        let base = self.settings.repositories.get(prefix)?;
        let url = format!("{}/{}", base.trim_end_matches('/'), rest);
        Some((prefix.to_string(), url))
    }
}

/// Creates the application router
pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/{*path}", get(proxy_handler))
        .layer(
            ServiceBuilder::new()
                .layer(CatchPanicLayer::new())
                .layer(ConcurrencyLimitLayer::new(app_config::MAX_CONCURRENT_REQUESTS)),
        )
        .layer(middleware::from_fn(request_logging))
        .with_state(app)
}

/// Health check endpoint
async fn health_handler() -> impl IntoResponse {
    "OK"
}

/// Request logging middleware
async fn request_logging(req: axum::extract::Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let start = Instant::now();

    let resp = next.run(req).await;

    let status = resp.status().as_u16();
    let elapsed = start.elapsed();

    if status < 400 {
        info!(
            method = %method,
            path,
            status,
            ms = elapsed.as_millis() as u64,
            ""
        );
    } else {
        warn!(
            method = %method,
            path,
            status,
            ms = elapsed.as_millis() as u64,
            ""
        );
    }

    resp
}

/// Prometheus metrics endpoint
async fn metrics_handler() -> impl IntoResponse {
    let content_type = [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")];
    match metrics::render() {
        Some(m) => (content_type, m),
        None => (content_type, "# Prometheus metrics disabled\n".to_string()),
    }
}

/// Main proxy handler
async fn proxy_handler(
    Path(path): Path<String>,
    State(app): State<Arc<App>>,
) -> Result<Response> {
    // Validate path before processing
    validate_path(&path)?;

    // Resolve URL from configured repositories
    let (_prefix, url) = app
        .resolve_url(&path)
        .ok_or_else(|| ProxyError::RepositoryNotFound(path.split('/').next().unwrap_or("").into()))?;

    // Fetch from cache or upstream
    app.downloader.fetch(&path, &url).await
}

/// Path validation module
mod path_validation {
    use super::*;

    /// Forbidden path patterns
    const FORBIDDEN_PATTERNS: &[(&str, &str)] = &[
        ("\0", "Null bytes not allowed"),
        ("\\", "Backslashes not allowed"),
        ("//", "Double slashes not allowed"),
    ];

    /// Validates and sanitizes request path
    pub fn validate(path: &str) -> Result<()> {
        // Length check
        if path.is_empty() || path.len() > app_config::MAX_PATH_LENGTH {
            return Err(ProxyError::InvalidPath(format!(
                "Path length must be 1-{} characters",
                app_config::MAX_PATH_LENGTH
            )));
        }

        // Must have repository prefix
        if !path.contains('/') {
            return Err(ProxyError::InvalidPath(
                "Missing repository prefix".into(),
            ));
        }

        // Decode percent-encoding and validate UTF-8
        let decoded = percent_decode_str(path)
            .decode_utf8()
            .map_err(|_| ProxyError::InvalidPath("Invalid UTF-8 encoding".into()))?;

        // Check for absolute paths
        if decoded.starts_with('/') {
            return Err(ProxyError::InvalidPath("Absolute paths not allowed".into()));
        }

        // Check forbidden patterns
        for (pattern, message) in FORBIDDEN_PATTERNS {
            if decoded.contains(pattern) {
                return Err(ProxyError::InvalidPath((*message).into()));
            }
        }

        // Validate each path component
        for component in decoded.split('/').filter(|c| !c.is_empty()) {
            validate_component(component)?;
        }

        // Final normalization check
        let normalized = path_clean::PathClean::clean(std::path::Path::new(decoded.as_ref()));
        let normalized_str = normalized.to_string_lossy();

        if normalized_str.starts_with("..") || normalized_str.starts_with(['/', '\\']) {
            return Err(ProxyError::InvalidPath(
                "Path traversal detected".into(),
            ));
        }

        Ok(())
    }

    fn validate_component(component: &str) -> Result<()> {
        match component {
            "." | ".." => {
                Err(ProxyError::InvalidPath("Path traversal not allowed".into()))
            }
            c if c.starts_with('.') && c != ".well-known" => {
                Err(ProxyError::InvalidPath("Hidden files not allowed".into()))
            }
            _ => Ok(()),
        }
    }
}

/// Public validation function
fn validate_path(path: &str) -> Result<()> {
    path_validation::validate(path)
}