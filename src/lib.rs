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
    cache: CacheManager,
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
            downloader: Downloader::new(cache.clone()),
            cache,
        })
    }

    /// Resolves a path to an upstream URL
    fn resolve_url(&self, path: &str) -> Option<(String, String)> {
        let (prefix, rest) = path.split_once('/')?;
        let base = self.settings.repositories.get(prefix)?;
        let url = format!("{}/{}", base.trim_end_matches('/'), rest);
        Some((prefix.to_string(), url))
    }

    /// Returns the repository name from a path
    fn extract_repo(path: &str) -> &str {
        path.split('/').next().unwrap_or("unknown")
    }
}

/// Creates the application router
pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/stats", get(stats_handler))
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
async fn health_handler(State(app): State<Arc<App>>) -> impl IntoResponse {
    let loading = app.cache.is_loading();
    let active = metrics::active_downloads();
    
    if loading {
        metrics::set_health_status(true); // Still healthy, just loading
    }
    
    let status = if loading { "loading" } else { "ready" };
    format!("OK\nstatus: {}\nactive_downloads: {}\n", status, active)
}

/// Stats endpoint with detailed information
async fn stats_handler(State(app): State<Arc<App>>) -> impl IntoResponse {
    let entries = app.cache.entry_count();
    let size_kb = app.cache.weighted_size();
    let loading = app.cache.is_loading();
    let active = metrics::active_downloads();
    let connections = metrics::active_connections();
    let in_flight = metrics::requests_in_flight();
    
    format!(
        "cache_entries: {}\n\
         cache_size_kb: {}\n\
         cache_size_mb: {}\n\
         index_loading: {}\n\
         active_downloads: {}\n\
         active_connections: {}\n\
         requests_in_flight: {}\n",
        entries,
        size_kb,
        size_kb / 1024,
        loading,
        active,
        connections,
        in_flight
    )
}

/// Request logging middleware with metrics
async fn request_logging(req: axum::extract::Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let start = Instant::now();

    // Extract repo from path (skip leading slash if present)
    let path_trimmed = path.trim_start_matches('/');
    let repo = App::extract_repo(path_trimmed);

    metrics::inc_requests_in_flight();
    
    let resp = next.run(req).await;

    metrics::dec_requests_in_flight();

    let status = resp.status().as_u16();
    let elapsed = start.elapsed();

    // Record detailed request metrics (skip internal endpoints)
    if !path.starts_with("/health") && !path.starts_with("/metrics") && !path.starts_with("/stats") {
        metrics::record_request(
            method.as_str(),
            status,
            repo,
            elapsed,
            0, // request size - would need body wrapper
            0, // response size - would need body wrapper
        );
    }

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
    let repo = App::extract_repo(&path);
    
    // Validate path before processing
    validate_path(&path)?;

    // Record repository request
    metrics::record_repo_request(repo);

    // Resolve URL from configured repositories
    let (_prefix, url) = app
        .resolve_url(&path)
        .ok_or_else(|| {
            metrics::record_repo_error(repo, "not_configured");
            ProxyError::RepositoryNotFound(path.split('/').next().unwrap_or("").into())
        })?;

    // Fetch from cache or upstream
    app.downloader.fetch(&path, &url).await
}

/// Path validation with metrics
fn validate_path(path: &str) -> Result<()> {
    match path_validation::validate(path) {
        Ok(()) => {
            metrics::record_path_validation(true, None);
            Ok(())
        }
        Err(e) => {
            let reason = match &e {
                ProxyError::InvalidPath(msg) => {
                    if msg.contains("length") {
                        "invalid_length"
                    } else if msg.contains("traversal") {
                        "path_traversal"
                    } else if msg.contains("UTF-8") {
                        "invalid_utf8"
                    } else if msg.contains("Hidden") {
                        "hidden_file"
                    } else if msg.contains("Null") {
                        "null_byte"
                    } else if msg.contains("Backslash") {
                        "backslash"
                    } else if msg.contains("Double") {
                        "double_slash"
                    } else {
                        "other"
                    }
                }
                _ => "unknown",
            };
            metrics::record_path_validation(false, Some(reason));
            Err(e)
        }
    }
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