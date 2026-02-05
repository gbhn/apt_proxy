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
const MAX_CONCURRENT: usize = 1000;

pub struct App {
    pub settings: Settings,
    downloader: Downloader,
    cache: CacheManager,
}

impl App {
    pub async fn new(settings: Settings) -> anyhow::Result<Self> {
        let storage = Arc::new(storage::Storage::new(settings.cache_dir.clone()).await?);
        let cache_config = Arc::new(settings.cache.clone());
        let cache = CacheManager::new(storage, cache_config, settings.max_cache_size).await?;

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
            downloader: Downloader::new(cache.clone()),
            cache,
        })
    }

    fn resolve_url(&self, path: &str) -> Option<(String, String)> {
        let (prefix, rest) = path.split_once('/')?;
        let base = self.settings.repositories.get(prefix)?;
        Some((prefix.to_string(), format!("{}/{}", base.trim_end_matches('/'), rest)))
    }
}

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .route("/stats", get(stats))
        .route("/{*path}", get(proxy))
        .layer(ServiceBuilder::new()
            .layer(CatchPanicLayer::new())
            .layer(ConcurrencyLimitLayer::new(MAX_CONCURRENT)))
        .layer(middleware::from_fn(logging))
        .with_state(app)
}

async fn metrics_handler() -> impl IntoResponse {
    ([(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
     metrics::render().unwrap_or_else(|| "# Metrics disabled\n".into()))
}

async fn health(State(app): State<Arc<App>>) -> impl IntoResponse {
    let status = if app.cache.is_loading() { "loading" } else { "ready" };
    format!("OK\nstatus: {}\nactive: {}\n", status, metrics::active_downloads())
}

async fn stats(State(app): State<Arc<App>>) -> impl IntoResponse {
    format!(
        "entries: {}\nsize_kb: {}\nloading: {}\nactive: {}\n",
        app.cache.entry_count(),
        app.cache.weighted_size(),
        app.cache.is_loading(),
        metrics::active_downloads()
    )
}

async fn logging(req: axum::extract::Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let start = Instant::now();

    metrics::inc_requests_in_flight();
    let resp = next.run(req).await;
    metrics::dec_requests_in_flight();

    let status = resp.status().as_u16();
    let elapsed = start.elapsed();

    if !path.starts_with("/health") && !path.starts_with("/stats") {
        let repo = path.trim_start_matches('/').split('/').next().unwrap_or("unknown");
        metrics::record_request(method.as_str(), status, repo, elapsed, 0, 0);
    }

    if status < 400 {
        info!(method = %method, path, status, ms = elapsed.as_millis() as u64, "");
    } else {
        warn!(method = %method, path, status, ms = elapsed.as_millis() as u64, "");
    }

    resp
}

async fn proxy(Path(path): Path<String>, State(app): State<Arc<App>>) -> Result<Response> {
    let repo = path.split('/').next().unwrap_or("unknown");
    validate_path(&path)?;
    metrics::record_repo_request(repo);

    let (_prefix, url) = app.resolve_url(&path).ok_or_else(|| {
        metrics::record_repo_error(repo, "not_configured");
        ProxyError::RepositoryNotFound(repo.into())
    })?;

    app.downloader.fetch(&path, &url).await
}

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > MAX_PATH_LENGTH || !path.contains('/') {
        return Err(ProxyError::InvalidPath("Invalid path format".into()));
    }

    let decoded = percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| ProxyError::InvalidPath("Invalid UTF-8".into()))?;

    if decoded.starts_with('/') || decoded.contains('\0') || decoded.contains('\\') || decoded.contains("//") {
        return Err(ProxyError::InvalidPath("Forbidden characters".into()));
    }

    for component in decoded.split('/').filter(|c| !c.is_empty()) {
        if component == "." || component == ".." {
            return Err(ProxyError::InvalidPath("Path traversal".into()));
        }
        if component.starts_with('.') && component != ".well-known" {
            return Err(ProxyError::InvalidPath("Hidden files".into()));
        }
    }

    let normalized = std::panic::catch_unwind(|| {
        path_clean::PathClean::clean(std::path::Path::new(decoded.as_ref()))
    }).map_err(|_| ProxyError::InvalidPath("Malformed path".into()))?;
    
    if normalized.to_string_lossy().starts_with("..") {
        return Err(ProxyError::InvalidPath("Path traversal".into()));
    }

    Ok(())
}