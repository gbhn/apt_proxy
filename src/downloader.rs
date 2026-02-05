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
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio_util::io::ReaderStream;
use tracing::{debug, info, warn};

const TIMEOUT: Duration = Duration::from_secs(300);
const MAX_DOWNLOAD_SIZE: u64 = 2 * 1024 * 1024 * 1024;
const STREAM_BUFFER_SIZE: usize = 64 * 1024;

const ALLOWED_HEADERS: &[axum::http::HeaderName] = &[
    CONTENT_TYPE,
    CONTENT_LENGTH,
    ETAG,
    LAST_MODIFIED,
    CACHE_CONTROL,
];

#[derive(Clone)]
struct DownloadProgress {
    written: u64,
    content_length: Option<u64>,
    headers: Option<Arc<axum::http::HeaderMap>>,
    status: ProgressStatus,
}

#[derive(Clone)]
enum ProgressStatus {
    Starting,
    Downloading,
    Complete { from_cache: bool },
    Failed(String),
}

struct ActiveDownload {
    progress_tx: watch::Sender<DownloadProgress>,
    progress_rx: watch::Receiver<DownloadProgress>,
    temp_path: PathBuf,
}

impl ActiveDownload {
    fn new(temp_path: PathBuf) -> Self {
        let (progress_tx, progress_rx) = watch::channel(DownloadProgress {
            written: 0,
            content_length: None,
            headers: None,
            status: ProgressStatus::Starting,
        });
        Self {
            progress_tx,
            progress_rx,
            temp_path,
        }
    }
}

#[derive(Clone)]
pub struct Downloader {
    client: reqwest::Client,
    cache: CacheManager,
    active: Arc<DashMap<String, Arc<ActiveDownload>>>,
}

impl Downloader {
    pub fn new(cache: CacheManager) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(concat!("apt-cacher-rs/", env!("CARGO_PKG_VERSION")))
            .timeout(TIMEOUT)
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(32)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            cache,
            active: Arc::new(DashMap::new()),
        }
    }

    pub async fn fetch(&self, key: &str, url: &str) -> Result<Response> {
        // Check cache first
        if let Some((file, meta)) = self.cache.open(key).await {
            metrics::record_hit(meta.size);
            debug!(key, size = meta.size, "Cache hit");
            return Ok(Self::response_from_file(file, meta));
        }

        metrics::record_miss();

        // Check for existing download
        if let Some(download) = self.active.get(key) {
            metrics::record_coalesced();
            debug!(key, "Joining existing download");
            return self.follow_download(key, download.clone()).await;
        }

        // Start new download
        self.start_download(key, url).await
    }

    async fn start_download(&self, key: &str, url: &str) -> Result<Response> {
        // Create temp file
        let (temp_path, temp_file) = self.cache.create_temp_data().await.map_err(|e| {
            metrics::record_error();
            ProxyError::Download(format!("Failed to create temp file: {e}"))
        })?;

        let download = Arc::new(ActiveDownload::new(temp_path.clone()));

        // Insert into active downloads
        {
            use dashmap::mapref::entry::Entry;
            match self.active.entry(key.to_string()) {
                Entry::Occupied(e) => {
                    // Someone else started, join them
                    metrics::record_coalesced();
                    debug!(key, "Race: joining existing download");
                    let existing = e.get().clone();
                    drop(e);
                    // Clean up our temp file
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    return self.follow_download(key, existing).await;
                }
                Entry::Vacant(e) => {
                    e.insert(download.clone());
                }
            }
        }

        metrics::inc_active();

        // Spawn download task
        let downloader = self.clone();
        let key_owned = key.to_string();
        let url_owned = url.to_string();
        let download_clone = download.clone();

        tokio::spawn(async move {
            downloader
                .perform_download(&key_owned, &url_owned, temp_file, download_clone)
                .await;
        });

        // Return streaming response
        self.follow_download(key, download).await
    }

    async fn follow_download(
        &self,
        key: &str,
        download: Arc<ActiveDownload>,
    ) -> Result<Response> {
        let mut rx = download.progress_rx.clone();

        // Wait for headers or completion
        loop {
            let progress = rx.borrow().clone();
            match progress.status {
                ProgressStatus::Starting => {
                    if rx.changed().await.is_err() {
                        return Err(ProxyError::Download("Download cancelled".into()));
                    }
                }
                ProgressStatus::Downloading => break,
                ProgressStatus::Complete { from_cache } => {
                    if from_cache {
                        // 304 case - serve from cache
                        if let Some((file, meta)) = self.cache.open(key).await {
                            return Ok(Self::response_from_file(file, meta));
                        }
                        return Err(ProxyError::Download("Cache miss after 304".into()));
                    }
                    break;
                }
                ProgressStatus::Failed(ref err) => {
                    return Err(ProxyError::Download(err.clone()));
                }
            }
        }

        let progress = rx.borrow().clone();
        let headers = progress.headers.clone().unwrap_or_default();
        let temp_path = download.temp_path.clone();

        let stream = Self::create_follow_stream(temp_path, rx);

        let mut resp = Response::new(Body::from_stream(stream));
        resp.headers_mut().extend((*headers).clone());

        if let Some(len) = progress.content_length {
            if let Ok(v) = axum::http::HeaderValue::from_str(&len.to_string()) {
                resp.headers_mut().insert(CONTENT_LENGTH, v);
            }
        }

        Ok(resp)
    }

    fn create_follow_stream(
        temp_path: PathBuf,
        progress_rx: watch::Receiver<DownloadProgress>,
    ) -> impl Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send {
        stream! {
            let mut rx = progress_rx;
            let mut file = match File::open(&temp_path).await {
                Ok(f) => f,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            let mut position = 0u64;
            let mut buffer = vec![0u8; STREAM_BUFFER_SIZE];

            loop {
                let progress = rx.borrow().clone();

                // Read available data
                while position < progress.written {
                    let available = progress.written - position;
                    let to_read = (available as usize).min(buffer.len());

                    match file.read(&mut buffer[..to_read]).await {
                        Ok(0) => {
                            // EOF but data should be there, seek and retry
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

                match progress.status {
                    ProgressStatus::Starting | ProgressStatus::Downloading => {
                        // Wait for more data
                        if rx.changed().await.is_err() {
                            yield Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "Download interrupted",
                            ));
                            return;
                        }
                    }
                    ProgressStatus::Complete { .. } => {
                        // Read any remaining data
                        loop {
                            match file.read(&mut buffer).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    yield Ok(Bytes::copy_from_slice(&buffer[..n]));
                                }
                                Err(e) => {
                                    yield Err(e);
                                    return;
                                }
                            }
                        }
                        return;
                    }
                    ProgressStatus::Failed(err) => {
                        yield Err(std::io::Error::new(std::io::ErrorKind::Other, err));
                        return;
                    }
                }
            }
        }
    }

    async fn perform_download(
        &self,
        key: &str,
        url: &str,
        mut temp_file: File,
        download: Arc<ActiveDownload>,
    ) {
        let result = self.do_download(key, url, &mut temp_file, &download).await;

        // Cleanup
        self.active.remove(key);
        metrics::dec_active();

        match result {
            Ok(Some(meta)) => {
                let temp_path = download.temp_path.clone();
                match self
                    .cache
                    .commit_temp_data(key, temp_path.clone(), &meta)
                    .await
                {
                    Ok(_) => {
                        info!(key, size = meta.size, "Download complete, cached");
                        let _ = download.progress_tx.send(DownloadProgress {
                            written: meta.size,
                            content_length: Some(meta.size),
                            headers: download.progress_rx.borrow().headers.clone(),
                            status: ProgressStatus::Complete { from_cache: false },
                        });
                    }
                    Err(e) => {
                        warn!(key, error = %e, "Failed to cache, serving from temp");
                        let _ = download.progress_tx.send(DownloadProgress {
                            written: meta.size,
                            content_length: Some(meta.size),
                            headers: download.progress_rx.borrow().headers.clone(),
                            status: ProgressStatus::Complete { from_cache: false },
                        });
                    }
                }
            }
            Ok(None) => {
                // 304 case handled in do_download
                let _ = tokio::fs::remove_file(&download.temp_path).await;
            }
            Err(e) => {
                warn!(key, error = %e, "Download failed");
                let _ = download.progress_tx.send(DownloadProgress {
                    written: 0,
                    content_length: None,
                    headers: None,
                    status: ProgressStatus::Failed(e.to_string()),
                });
                let _ = tokio::fs::remove_file(&download.temp_path).await;
            }
        }
    }

    async fn do_download(
        &self,
        key: &str,
        url: &str,
        temp_file: &mut File,
        download: &ActiveDownload,
    ) -> Result<Option<Metadata>> {
        debug!(key, url, "Starting download");

        let existing = self.cache.get_metadata(key).await;
        let mut req = self.client.get(url);

        if let Some(ref meta) = existing {
            if let Some(ref etag) = meta.etag {
                req = req.header("If-None-Match", etag);
            }
            if let Some(ref lm) = meta.last_modified {
                req = req.header("If-Modified-Since", lm);
            }
        }

        let resp = req.send().await.map_err(|e| {
            metrics::record_error();
            ProxyError::Upstream(e)
        })?;

        let status = resp.status();

        if status == reqwest::StatusCode::NOT_MODIFIED {
            info!(key, "304 Not Modified");
            metrics::record_304();

            if let Err(e) = self.cache.touch(key).await {
                warn!(key, error = %e, "Failed to touch cache entry");
            }

            // Serve from cache
            if let Some((_, meta)) = self.cache.open(key).await {
                let _ = download.progress_tx.send(DownloadProgress {
                    written: meta.size,
                    content_length: Some(meta.size),
                    headers: Some(Arc::new(meta.headers.clone())),
                    status: ProgressStatus::Complete { from_cache: true },
                });
            }

            return Ok(None);
        }

        if !status.is_success() {
            warn!(key, %status, "Upstream error");
            metrics::record_error();
            return Err(ProxyError::UpstreamStatus(
                axum::http::StatusCode::from_u16(status.as_u16())
                    .unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            ));
        }

        let content_len = resp.content_length();
        if content_len.unwrap_or(0) > MAX_DOWNLOAD_SIZE {
            return Err(ProxyError::Download("File too large".into()));
        }

        let headers = resp.headers().clone();
        let filtered_headers = Self::filter_headers(&headers);

        // Notify that we're downloading with headers
        let _ = download.progress_tx.send(DownloadProgress {
            written: 0,
            content_length: content_len,
            headers: Some(Arc::new(filtered_headers.clone())),
            status: ProgressStatus::Downloading,
        });

        info!(key, size = content_len, "Downloading");

        let mut stream = resp.bytes_stream();
        let mut written: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let bytes = chunk.map_err(ProxyError::Upstream)?;

            written = written.saturating_add(bytes.len() as u64);
            if written > MAX_DOWNLOAD_SIZE {
                return Err(ProxyError::Download("File too large".into()));
            }

            temp_file
                .write_all(&bytes)
                .await
                .map_err(ProxyError::Cache)?;

            // Flush to make data available to readers
            temp_file.flush().await.map_err(ProxyError::Cache)?;

            // Notify progress
            let _ = download.progress_tx.send(DownloadProgress {
                written,
                content_length: content_len,
                headers: Some(Arc::new(filtered_headers.clone())),
                status: ProgressStatus::Downloading,
            });
        }

        temp_file.sync_all().await.map_err(ProxyError::Cache)?;

        if let Some(expected) = content_len {
            if written != expected {
                warn!(key, expected, actual = written, "Size mismatch");
                return Err(ProxyError::Download("Incomplete download".into()));
            }
        }

        metrics::record_download(written);

        let meta = Metadata {
            headers: filtered_headers,
            url: url.into(),
            key: key.into(),
            stored_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            size: written,
            etag: headers
                .get(http::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(Into::into),
            last_modified: headers
                .get(http::header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(Into::into),
        };

        Ok(Some(meta))
    }

    fn filter_headers(headers: &reqwest::header::HeaderMap) -> axum::http::HeaderMap {
        let mut filtered = axum::http::HeaderMap::new();
        for name in ALLOWED_HEADERS {
            if let Some(value) = headers.get(name.as_str()) {
                if let Ok(v) = axum::http::HeaderValue::from_bytes(value.as_bytes()) {
                    filtered.insert(name.clone(), v);
                }
            }
        }
        filtered
    }

    fn response_from_file(file: File, mut meta: Metadata) -> Response {
        if !meta.headers.contains_key(CONTENT_LENGTH) {
            if let Ok(v) = axum::http::HeaderValue::from_str(&meta.size.to_string()) {
                meta.headers.insert(CONTENT_LENGTH, v);
            }
        }

        let stream = ReaderStream::new(file);
        let mut resp = Response::new(Body::from_stream(stream));
        resp.headers_mut().extend(meta.headers);
        resp
    }
}