use crate::cache::CacheManager;
use crate::error::{ProxyError, Result};
use crate::metrics;
use crate::storage::Metadata;
use async_stream::stream;
use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use axum::response::Response;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio_util::io::ReaderStream;
use tracing::{debug, info, warn};

/// HTTP client configuration
mod http_config {
    use std::time::Duration;

    pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
    pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    pub const POOL_MAX_IDLE: usize = 32;
}

/// Download limits and buffer sizes
mod limits {
    pub const MAX_DOWNLOAD_SIZE: u64 = 2 * 1024 * 1024 * 1024; // 2 GB
    pub const STREAM_BUFFER_SIZE: usize = 64 * 1024; // 64 KB
}

/// Headers to forward from upstream responses
const FORWARDED_HEADERS: &[axum::http::HeaderName] = &[
    CONTENT_TYPE,
    CONTENT_LENGTH,
    ETAG,
    LAST_MODIFIED,
    CACHE_CONTROL,
];

/// State of an active download, shared between producer and followers
#[derive(Clone, Debug)]
enum DownloadState {
    /// Download is initializing
    Starting,
    /// Download is in progress
    Streaming {
        written: u64,
        content_length: Option<u64>,
        headers: Arc<axum::http::HeaderMap>,
    },
    /// Download completed successfully
    Complete {
        /// True if response was 304 Not Modified
        from_cache: bool,
    },
    /// Download failed with error
    Failed(String),
}

/// Active download tracker with shared state
struct ActiveDownload {
    state_tx: watch::Sender<DownloadState>,
    state_rx: watch::Receiver<DownloadState>,
    temp_path: PathBuf,
}

impl ActiveDownload {
    fn new(temp_path: PathBuf) -> Self {
        let (state_tx, state_rx) = watch::channel(DownloadState::Starting);
        Self {
            state_tx,
            state_rx,
            temp_path,
        }
    }

    fn notify(&self, state: DownloadState) {
        let _ = self.state_tx.send(state);
    }

    fn subscribe(&self) -> watch::Receiver<DownloadState> {
        self.state_rx.clone()
    }
}

/// Downloader managing concurrent downloads with request coalescing
#[derive(Clone)]
pub struct Downloader {
    client: reqwest::Client,
    cache: CacheManager,
    active: Arc<DashMap<String, Arc<ActiveDownload>>>,
}

impl Downloader {
    /// Creates a new downloader with the given cache manager
    pub fn new(cache: CacheManager) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(concat!("apt-cacher-rs/", env!("CARGO_PKG_VERSION")))
            .timeout(http_config::REQUEST_TIMEOUT)
            .connect_timeout(http_config::CONNECT_TIMEOUT)
            .pool_max_idle_per_host(http_config::POOL_MAX_IDLE)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            cache,
            active: Arc::new(DashMap::new()),
        }
    }

    /// Extracts repository name from cache key
    fn extract_repo(key: &str) -> &str {
        key.split('/').next().unwrap_or("unknown")
    }

    /// Fetches a file, returning from cache or downloading from upstream
    pub async fn fetch(&self, key: &str, url: &str) -> Result<Response> {
        let repo = Self::extract_repo(key);
        let lookup_start = Instant::now();

        // Fast path: cache hit
        if let Some((file, meta)) = self.cache.open(key).await {
            metrics::record_cache_lookup(true, lookup_start.elapsed());
            metrics::record_repo_hit(repo, meta.size);
            debug!(key, size = meta.size, "Cache hit");
            return Ok(file_response(file, meta));
        }

        metrics::record_cache_lookup(false, lookup_start.elapsed());
        metrics::record_repo_miss(repo);

        // Check for existing download to coalesce
        if let Some(download) = self.active.get(key) {
            metrics::record_coalesced();
            debug!(key, "Joining existing download");
            let _follower_guard = metrics::FollowerGuard::new();
            return self.follow_download(key, download.clone()).await;
        }

        // Start new download
        self.start_download(key, url, repo).await
    }

    async fn start_download(&self, key: &str, url: &str, repo: &str) -> Result<Response> {
        // Create temp file for download
        let (temp_path, temp_file) = self.cache.create_temp_data().await.map_err(|e| {
            metrics::record_storage_error("create_temp");
            ProxyError::download(format!("Failed to create temp file: {e}"))
        })?;

        let _temp_guard = metrics::TempFileGuard::new();
        let download = Arc::new(ActiveDownload::new(temp_path.clone()));

        // Try to register as primary downloader (atomic operation)
        use dashmap::mapref::entry::Entry;
        match self.active.entry(key.to_string()) {
            Entry::Occupied(existing) => {
                // Race lost - cleanup and follow the winner
                metrics::record_coalesced();
                debug!(key, "Race condition: joining existing download");
                let existing_download = existing.get().clone();
                drop(existing);
                let _ = tokio::fs::remove_file(&temp_path).await;
                let _follower_guard = metrics::FollowerGuard::new();
                return self.follow_download(key, existing_download).await;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(download.clone());
            }
        };

        metrics::inc_active();
        metrics::record_download_started();

        // Spawn background download task
        let this = self.clone();
        let key_owned = key.to_string();
        let url_owned = url.to_string();
        let repo_owned = repo.to_string();
        let download_clone = download.clone();

        tokio::spawn(async move {
            this.perform_download(&key_owned, &url_owned, &repo_owned, temp_file, download_clone)
                .await;
        });

        let _follower_guard = metrics::FollowerGuard::new();
        self.follow_download(key, download).await
    }

    async fn follow_download(
        &self,
        key: &str,
        download: Arc<ActiveDownload>,
    ) -> Result<Response> {
        let mut rx = download.subscribe();

        // Wait for headers or terminal state
        let (headers, content_length) = loop {
            let state = rx.borrow_and_update().clone();

            match state {
                DownloadState::Starting => {
                    if rx.changed().await.is_err() {
                        return Err(ProxyError::download("Download cancelled"));
                    }
                }
                DownloadState::Streaming {
                    headers,
                    content_length,
                    ..
                } => {
                    break (headers, content_length);
                }
                DownloadState::Complete { from_cache: true } => {
                    // 304 response - serve from cache
                    return self
                        .cache
                        .open(key)
                        .await
                        .map(|(f, m)| file_response(f, m))
                        .ok_or_else(|| ProxyError::download("Cache miss after 304"));
                }
                DownloadState::Complete { from_cache: false } => {
                    // Download complete - serve from cache
                    return self
                        .cache
                        .open(key)
                        .await
                        .map(|(f, m)| file_response(f, m))
                        .ok_or_else(|| ProxyError::download("File not found after download"));
                }
                DownloadState::Failed(err) => {
                    return Err(ProxyError::Download(err));
                }
            }
        };

        // Create streaming response following the download
        let stream = create_follow_stream(download.temp_path.clone(), rx);
        let mut resp = Response::new(Body::from_stream(stream));

        resp.headers_mut().extend((*headers).clone());
        if let Some(len) = content_length {
            if let Ok(v) = axum::http::HeaderValue::from_str(&len.to_string()) {
                resp.headers_mut().insert(CONTENT_LENGTH, v);
            }
        }

        Ok(resp)
    }

    async fn perform_download(
        &self,
        key: &str,
        url: &str,
        repo: &str,
        mut temp_file: File,
        download: Arc<ActiveDownload>,
    ) {
        let start = Instant::now();
        let temp_path = download.temp_path.clone();

        let result = self
            .do_download(key, url, repo, &mut temp_file, &download)
            .await;

        // Drop the file handle before any file operations
        drop(temp_file);

        // Always cleanup active download registration
        self.active.remove(key);
        metrics::dec_active();
        metrics::clear_download_progress(key);

        match result {
            Ok(Some((meta, bytes_written))) => {
                // Commit to cache
                match self.cache.commit(key, temp_path.clone(), &meta).await {
                    Ok(_) => {
                        info!(key, size = meta.size, "Download complete");
                        download.notify(DownloadState::Complete { from_cache: false });
                        metrics::record_download_complete(bytes_written, start.elapsed(), repo);
                        metrics::record_repo_bytes_served(repo, bytes_written);
                    }
                    Err(e) => {
                        warn!(key, error = %e, "Failed to commit to cache");
                        // Cleanup temp file on commit failure
                        if let Err(rm_err) = tokio::fs::remove_file(&temp_path).await {
                            debug!(
                                key,
                                error = %rm_err,
                                "Failed to remove temp file after commit error"
                            );
                        }
                        // Notify followers about the failure
                        download.notify(DownloadState::Failed(format!("Commit failed: {e}")));
                        metrics::record_download_failed("commit_error", repo);
                        metrics::record_storage_error("commit");
                    }
                }
            }
            Ok(None) => {
                // 304 Not Modified - cleanup temp file
                let _ = tokio::fs::remove_file(&temp_path).await;
                // Note: 304 metrics already recorded in do_download
            }
            Err(e) => {
                warn!(key, error = %e, "Download failed");
                download.notify(DownloadState::Failed(e.to_string()));
                // Cleanup temp file on download failure
                if let Err(rm_err) = tokio::fs::remove_file(&temp_path).await {
                    debug!(
                        key,
                        error = %rm_err,
                        "Failed to remove temp file after download error"
                    );
                }
                metrics::record_download_failed(&categorize_error(&e), repo);
            }
        }
    }

    async fn do_download(
        &self,
        key: &str,
        url: &str,
        repo: &str,
        temp_file: &mut File,
        download: &ActiveDownload,
    ) -> Result<Option<(Metadata, u64)>> {
        debug!(key, url, "Starting download");

        // Build request with conditional headers for revalidation
        let mut req = self.client.get(url);
        if let Some(meta) = self.cache.get_metadata(key).await {
            if let Some(ref etag) = meta.etag {
                req = req.header("If-None-Match", etag);
            }
            if let Some(ref lm) = meta.last_modified {
                req = req.header("If-Modified-Since", lm);
            }
        }

        let request_start = Instant::now();
        let resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                metrics::record_upstream_timeout(repo);
            } else if e.is_connect() {
                metrics::record_upstream_error("connect", repo);
            } else {
                metrics::record_upstream_error("request", repo);
            }
            ProxyError::Upstream(e)
        })?;

        let connect_time = request_start.elapsed();
        metrics::record_upstream_connect_time(connect_time);

        let status = resp.status();

        // Handle 304 Not Modified
        if status == reqwest::StatusCode::NOT_MODIFIED {
            info!(key, "304 Not Modified");
            metrics::record_304();
            metrics::record_upstream_request(304, connect_time, None, repo);

            if let Err(e) = self.cache.touch(key).await {
                warn!(key, error = %e, "Failed to touch cache entry");
            }

            if self.cache.open(key).await.is_some() {
                download.notify(DownloadState::Complete { from_cache: true });
            }
            return Ok(None);
        }

        if !status.is_success() {
            warn!(key, %status, "Upstream error");
            metrics::record_upstream_error(&format!("status_{}", status.as_u16()), repo);
            metrics::record_upstream_request(status.as_u16(), connect_time, None, repo);
            return Err(ProxyError::UpstreamStatus(
                axum::http::StatusCode::from_u16(status.as_u16())
                    .unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            ));
        }

        let content_len = resp.content_length();
        if content_len.unwrap_or(0) > limits::MAX_DOWNLOAD_SIZE {
            metrics::record_upstream_error("too_large", repo);
            return Err(ProxyError::download(format!(
                "File too large: {} bytes exceeds {} byte limit",
                content_len.unwrap_or(0),
                limits::MAX_DOWNLOAD_SIZE
            )));
        }

        // Extract headers before consuming response
        let resp_headers = resp.headers().clone();
        let headers = filter_headers(&resp_headers);
        let headers_arc = Arc::new(headers.clone());

        // Record TTFB
        let ttfb = request_start.elapsed();
        metrics::record_upstream_ttfb(ttfb);

        // Notify followers that streaming has started
        download.notify(DownloadState::Streaming {
            written: 0,
            content_length: content_len,
            headers: headers_arc.clone(),
        });

        info!(key, size = content_len, "Downloading");

        // Stream response body to temp file
        let mut stream = resp.bytes_stream();
        let mut written = 0u64;

        while let Some(chunk) = stream.next().await {
            let bytes = chunk.map_err(|e| {
                metrics::record_upstream_error("stream", repo);
                ProxyError::Upstream(e)
            })?;

            written = written.saturating_add(bytes.len() as u64);
            if written > limits::MAX_DOWNLOAD_SIZE {
                metrics::record_upstream_error("size_exceeded", repo);
                return Err(ProxyError::download(
                    "File exceeded size limit during download",
                ));
            }

            temp_file.write_all(&bytes).await.map_err(|e| {
                metrics::record_storage_error("write");
                ProxyError::Cache(e)
            })?;
            temp_file.flush().await.map_err(ProxyError::Cache)?;

            // Update progress for followers and metrics
            download.notify(DownloadState::Streaming {
                written,
                content_length: content_len,
                headers: headers_arc.clone(),
            });
            metrics::set_download_progress(key, written);
        }

        temp_file.sync_all().await.map_err(ProxyError::Cache)?;

        // Verify download size
        if let Some(expected) = content_len {
            if written != expected {
                warn!(key, expected, actual = written, "Size mismatch");
                metrics::record_upstream_error("size_mismatch", repo);
                return Err(ProxyError::download(format!(
                    "Incomplete download: expected {} bytes, got {}",
                    expected, written
                )));
            }
        }

        // Record successful upstream request
        metrics::record_upstream_request(
            status.as_u16(),
            request_start.elapsed(),
            Some(written),
            repo,
        );
        metrics::record_storage_write(written);

        // Build metadata
        let meta = Metadata::new(key, url, written, headers).with_conditionals(
            resp_headers
                .get(http::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            resp_headers
                .get(http::header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
        );

        Ok(Some((meta, written)))
    }
}

/// Categorizes an error for metrics
fn categorize_error(e: &ProxyError) -> String {
    match e {
        ProxyError::Upstream(req_err) => {
            if req_err.is_timeout() {
                "timeout".to_string()
            } else if req_err.is_connect() {
                "connect".to_string()
            } else {
                "request".to_string()
            }
        }
        ProxyError::UpstreamStatus(status) => format!("status_{}", status.as_u16()),
        ProxyError::Cache(_) => "cache_io".to_string(),
        ProxyError::Download(msg) => {
            if msg.contains("size") {
                "size".to_string()
            } else {
                "download".to_string()
            }
        }
        ProxyError::Timeout => "timeout".to_string(),
        _ => "other".to_string(),
    }
}

/// Filters response headers to only include those we want to forward
fn filter_headers(headers: &reqwest::header::HeaderMap) -> axum::http::HeaderMap {
    let mut filtered = axum::http::HeaderMap::new();
    for name in FORWARDED_HEADERS {
        if let Some(value) = headers.get(name.as_str()) {
            if let Ok(v) = axum::http::HeaderValue::from_bytes(value.as_bytes()) {
                filtered.insert(name.clone(), v);
            }
        }
    }
    filtered
}

/// Creates a response from a cached file
fn file_response(file: File, mut meta: Metadata) -> Response {
    // Ensure Content-Length is set
    if !meta.headers.contains_key(CONTENT_LENGTH) {
        if let Ok(v) = axum::http::HeaderValue::from_str(&meta.size.to_string()) {
            meta.headers.insert(CONTENT_LENGTH, v);
        }
    }

    let mut resp = Response::new(Body::from_stream(ReaderStream::new(file)));
    resp.headers_mut().extend(meta.headers);
    resp
}

/// Creates a stream that follows an active download's progress
fn create_follow_stream(
    temp_path: PathBuf,
    mut state_rx: watch::Receiver<DownloadState>,
) -> impl Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send {
    stream! {
        let mut file = match File::open(&temp_path).await {
            Ok(f) => f,
            Err(e) => {
                yield Err(e);
                return;
            }
        };

        let mut position = 0u64;
        let mut buffer = vec![0u8; limits::STREAM_BUFFER_SIZE];

        loop {
            // Get current state (clone to release borrow)
            let (written, is_complete) = {
                let state = state_rx.borrow_and_update().clone();
                match state {
                    DownloadState::Starting => (0, false),
                    DownloadState::Streaming { written, .. } => (written, false),
                    DownloadState::Complete { .. } => (u64::MAX, true),
                    DownloadState::Failed(err) => {
                        yield Err(std::io::Error::other(err));
                        return;
                    }
                }
            };

            // Read all available data
            while position < written {
                let to_read = ((written - position) as usize).min(buffer.len());
                match file.read(&mut buffer[..to_read]).await {
                    Ok(0) => {
                        // EOF but more data expected - seek and retry
                        if let Err(e) = file.seek(SeekFrom::Start(position)).await {
                            yield Err(e);
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        break;
                    }
                    Ok(n) => {
                        position += n as u64;
                        yield Ok(Bytes::copy_from_slice(&buffer[..n]));
                    }
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                }
            }

            if is_complete {
                // Drain any remaining data
                loop {
                    match file.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(n) => yield Ok(Bytes::copy_from_slice(&buffer[..n])),
                        Err(e) => {
                            yield Err(e);
                            return;
                        }
                    }
                }
                return;
            }

            // Wait for more data
            if state_rx.changed().await.is_err() {
                yield Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Download interrupted",
                ));
                return;
            }
        }
    }
}