use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use dashmap::DashMap;
use std::sync::Arc;

pub mod cache;
pub mod config;
pub mod download;
pub mod error;
pub mod server;
pub mod utils;

use cache::CacheManager;
use config::Settings;
use download::ActiveDownloads;
use error::{ProxyError, Result};

pub struct AppState {
    pub settings: Settings,
    pub cache: CacheManager,
    pub http_client: reqwest::Client,
    pub active_downloads: ActiveDownloads,
}

impl AppState {
    pub fn new(settings: Settings, cache: CacheManager) -> Self {
        let http_client = reqwest::Client::builder()
            .user_agent("apt-cacher-rs/2.1")
            .timeout(std::time::Duration::from_secs(300))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            settings,
            cache,
            http_client,
            active_downloads: Arc::new(DashMap::with_capacity(128)),
        }
    }

    /// Возвращает (upstream_url, path_remainder)
    /// - upstream_url живёт столько же, сколько self
    /// - path_remainder живёт столько же, сколько входной path
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
        .layer(tower_http::trace::TraceLayer::new_for_http())
}

async fn health_check() -> &'static str {
    "OK"
}

async fn stats_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.cache.get_stats().await
}

async fn proxy_handler(
    Path(path): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Response> {
    utils::validate_path(&path)?;

    let (upstream_url, upstream_path) = state
        .resolve_upstream(&path)
        .ok_or(ProxyError::RepositoryNotFound)?;

    if upstream_path.is_empty() {
        return Err(ProxyError::InvalidPath("Path missing file name".into()));
    }

    if let Some(response) = state.cache.serve_cached(&path).await? {
        return Ok(response);
    }

    let downloader = download::Downloader::new(
        state.http_client.clone(),
        upstream_url.to_string(),  // Конвертируем в String здесь, когда нужно владение
        upstream_path.to_string(),
        state.active_downloads.clone(),
    );

    downloader.fetch_and_stream(&path, &state.cache).await
}