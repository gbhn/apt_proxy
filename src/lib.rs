pub mod cache_manager;
pub mod config;
pub mod download_manager;
pub mod error;
pub mod server;
pub mod storage;
pub mod utils;

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use std::sync::Arc;
use tower_http::{trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer}, LatencyUnit};
use tracing::{debug, info, Level};

use cache_manager::CacheManager;
use config::Settings;
use download_manager::DownloadManager;
use error::{ProxyError, Result};
use storage::Storage;

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
            .user_agent("apt-cacher-rs/3.0")
            .timeout(std::time::Duration::from_secs(300))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(16)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .tcp_nodelay(false)
            .build()?;

        let downloader = DownloadManager::new(http_client, storage.clone());
        let cache = CacheManager::new(
            storage.clone(),
            settings.max_cache_size,
            settings.max_lru_entries,
        ).await?;

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
        .route("/*path", get(proxy_handler))
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::DEBUG))
                .on_response(
                    DefaultOnResponse::new()
                        .level(Level::DEBUG)
                        .latency_unit(LatencyUnit::Millis),
                ),
        )
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
    debug!("Request: {}", path);
    utils::validate_path(&path)?;

    let (upstream_url, upstream_path) = state
        .resolve_upstream(&path)
        .ok_or(ProxyError::RepositoryNotFound)?;

    if state.cache.contains(&path).await {
        info!("Cache HIT: {}", path);

        let stored = state.storage.open(&path).await
            .map_err(|e| ProxyError::Cache(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .ok_or(ProxyError::RepositoryNotFound)?;

        state.cache.mark_used(&path, stored.size, 0).await;

        let stream = tokio_util::io::ReaderStream::with_capacity(stored.file, 256 * 1024);
        let mut response = Response::new(axum::body::Body::from_stream(stream));
        response.headers_mut().extend(stored.metadata.headers.clone());

        return Ok(response);
    }

    info!("Cache MISS: {}", path);

    let full_url = format!(
        "{}/{}",
        upstream_url.trim_end_matches('/'),
        upstream_path.trim_start_matches('/')
    );

    let response = state.downloader.get_or_download(&path, full_url).await?;

    if state.cache.needs_cleanup() {
        state.cache.spawn_cleanup();
    }

    Ok(response)
}