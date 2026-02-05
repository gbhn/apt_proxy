pub mod cache_manager;
pub mod cache_policy;
pub mod config;
pub mod download_manager;
pub mod error;
pub mod logging;
pub mod server;
pub mod storage;
pub mod utils;

use axum::{
    extract::{Path, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tracing::{debug, info, info_span, warn, Instrument};

use cache_manager::CacheManager;
use config::Settings;
use download_manager::DownloadManager;
use error::{ProxyError, Result};
use storage::Storage;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct AppState {
    pub settings: Settings,
    cache: CacheManager,
    downloader: DownloadManager,
    storage: Arc<Storage>,
}

impl AppState {
    pub async fn new(settings: Settings) -> anyhow::Result<Self> {
        let storage = Arc::new(Storage::new(settings.cache_dir.clone()).await?);

        let http_client = reqwest::Client::builder()
            .user_agent(format!("apt-cacher-rs/{}", env!("CARGO_PKG_VERSION")))
            .timeout(std::time::Duration::from_secs(300))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(16)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .tcp_nodelay(false)
            .build()?;

        let cache_settings = Arc::new(settings.cache.clone());
        
        let downloader = DownloadManager::new(
            http_client, 
            storage.clone(),
            cache_settings.clone(),
        );
        
        let cache = CacheManager::new(
            storage.clone(),
            cache_settings,
            settings.max_cache_size,
            settings.max_lru_entries,
        )
        .await?;

        cache.spawn_expiry_checker(3600);

        Ok(Self {
            settings,
            cache,
            downloader,
            storage,
        })
    }

    #[inline]
    pub fn resolve_upstream<'s, 'p>(&'s self, path: &'p str) -> Option<(&'s str, &'p str)> {
        let (prefix, remainder) = path.split_once('/')?;
        let repo_url = self.settings.repositories.get(prefix)?;
        Some((repo_url.as_str(), remainder))
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(health_check))
        .route("/health", get(health_check))
        .route("/stats", get(stats_handler))
        .route("/{*path}", get(proxy_handler))
        .layer(middleware::from_fn(request_logging_middleware))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .with_state(state)
}

async fn request_logging_middleware(request: Request, next: Next) -> Response {
    let request_id = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path().to_string();

    let start = Instant::now();

    let span = info_span!(
        "request",
        id = request_id,
        method = %method,
    );

    let response = next.run(request).instrument(span).await;

    let elapsed = start.elapsed();
    let status = response.status();

    if status.is_success() {
        info!(
            target: "http",
            status = %status.as_u16(),
            path = %logging::fields::path(&path),
            time = %logging::fields::duration(elapsed),
            "Request completed"
        );
    } else if status.is_client_error() {
        warn!(
            target: "http",
            status = %status.as_u16(),
            path = %logging::fields::path(&path),
            time = %logging::fields::duration(elapsed),
            "Client error"
        );
    } else {
        tracing::error!(
            target: "http",
            status = %status.as_u16(),
            path = %logging::fields::path(&path),
            time = %logging::fields::duration(elapsed),
            "Server error"
        );
    }

    response
}

async fn health_check() -> &'static str {
    "OK"
}

async fn stats_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stats = state.cache.stats().await;
    let active = state.downloader.active_count();
    format!("{}\nActive Downloads: {}", stats, active)
}

async fn proxy_handler(
    Path(path): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Response> {
    debug!(path = %logging::fields::path(&path), "Processing request");
    utils::validate_path(&path)?;

    let (upstream_url, upstream_path) = state
        .resolve_upstream(&path)
        .ok_or(ProxyError::RepositoryNotFound)?;

    if state.cache.contains(&path).await {
        info!(
            path = %logging::fields::path(&path),
            "Cache HIT"
        );

        let stored = state
            .storage
            .open(&path)
            .await
            .map_err(|e| ProxyError::Cache(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .ok_or(ProxyError::RepositoryNotFound)?;

        let meta_size = state.storage.metadata_size(&path).await.unwrap_or(0);
        
        state.cache.mark_used(&path, stored.size, meta_size, stored.metadata.clone()).await;

        let stream = tokio_util::io::ReaderStream::with_capacity(stored.file, 256 * 1024);
        let mut response = Response::new(axum::body::Body::from_stream(stream));
        response
            .headers_mut()
            .extend(stored.metadata.headers.clone());

        return Ok(response);
    }

    let existing_metadata = state.storage.open(&path).await.ok().flatten().map(|s| s.metadata);

    info!(
        path = %logging::fields::path(&path),
        upstream = %upstream_url,
        revalidating = existing_metadata.is_some(),
        "Cache MISS -> fetching"
    );

    let full_url = format!(
        "{}/{}",
        upstream_url.trim_end_matches('/'),
        upstream_path.trim_start_matches('/')
    );

    let (response, file_size, meta_size, metadata) = state
        .downloader
        .get_or_download(&path, full_url, existing_metadata)
        .await?;

    state.cache.mark_used(&path, file_size, meta_size, metadata).await;

    if state.cache.needs_cleanup() {
        state.cache.spawn_cleanup();
    }

    Ok(response)
}