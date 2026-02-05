use crate::cache::CacheManager;
use crate::error::{ProxyError, Result};
use crate::metrics;
use crate::storage::Metadata;
use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use axum::response::Response;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{watch, Mutex};
use tokio_util::io::ReaderStream;
use tracing::{debug, info, warn};

const TIMEOUT: Duration = Duration::from_secs(300);
const MAX_DOWNLOAD_SIZE: u64 = 2 * 1024 * 1024 * 1024;

const ALLOWED_HEADERS: &[axum::http::HeaderName] = &[
    CONTENT_TYPE,
    CONTENT_LENGTH,
    ETAG,
    LAST_MODIFIED,
    CACHE_CONTROL,
];

#[derive(Clone, Copy, PartialEq)]
enum Status {
    Pending,
    Downloading,
    Done(bool),
}

struct TempFileLease {
    path: PathBuf,
}

impl Drop for TempFileLease {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct LeaseStream<S> {
    inner: S,
    _lease: Arc<TempFileLease>,
}

impl<S> Stream for LeaseStream<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

enum DownloadOutcome {
    Cached,
    Temp { lease: Arc<TempFileLease>, meta: Metadata },
}

struct ActiveDownload {
    tx: watch::Sender<Status>,
    rx: watch::Receiver<Status>,
    outcome: Mutex<Option<Arc<DownloadOutcome>>>,
}

impl ActiveDownload {
    fn new() -> Self {
        let (tx, rx) = watch::channel(Status::Pending);
        Self {
            tx,
            rx,
            outcome: Mutex::new(None),
        }
    }
}

struct DownloadGuard<'a> {
    active: &'a DashMap<String, Arc<ActiveDownload>>,
    key: String,
    download: &'a ActiveDownload,
    completed: bool,
}

impl<'a> DownloadGuard<'a> {
    fn new(
        active: &'a DashMap<String, Arc<ActiveDownload>>,
        key: String,
        download: &'a ActiveDownload,
    ) -> Self {
        Self {
            active,
            key,
            download,
            completed: false,
        }
    }

    fn start(&self) {
        metrics::inc_active();
    }

    fn complete(&mut self, success: bool) {
        self.completed = true;
        let _ = self.download.tx.send(Status::Done(success));
    }
}

impl Drop for DownloadGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.download.tx.send(Status::Done(false));
        }
        self.active.remove(&self.key);
        metrics::dec_active();
    }
}

struct TempPathGuard {
    path: PathBuf,
    keep: bool,
}

impl TempPathGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, keep: false }
    }

    fn keep(mut self) -> Self {
        self.keep = true;
        self
    }
}

impl Drop for TempPathGuard {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(&self.path);
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
        if let Some((file, meta)) = self.cache.open(key).await {
            metrics::record_hit(meta.size);
            debug!(key, size = meta.size, "Cache hit");
            return Ok(self.response_from_file(file, meta));
        }

        metrics::record_miss();

        let (download, is_leader) = self.get_or_create_download(key);

        if !is_leader {
            metrics::record_coalesced();
            debug!(key, "Waiting for existing download");
            return self.wait_for_download(key, download).await;
        }

        self.perform_download(key, url, download).await
    }

    fn get_or_create_download(&self, key: &str) -> (Arc<ActiveDownload>, bool) {
        use dashmap::mapref::entry::Entry;

        match self.active.entry(key.to_string()) {
            Entry::Occupied(e) => (e.get().clone(), false),
            Entry::Vacant(e) => {
                let dl = Arc::new(ActiveDownload::new());
                e.insert(dl.clone());
                (dl, true)
            }
        }
    }

    async fn wait_for_download(&self, key: &str, dl: Arc<ActiveDownload>) -> Result<Response> {
        let mut rx = dl.rx.clone();

        loop {
            let status = *rx.borrow_and_update();

            match status {
                Status::Done(true) => {
                    if let Some((file, meta)) = self.cache.open(key).await {
                        return Ok(self.response_from_file(file, meta));
                    }

                    let outcome = dl.outcome.lock().await.clone();

                    match outcome.as_deref() {
                        Some(DownloadOutcome::Temp { lease, meta }) => {
                            let file =
                                File::open(&lease.path).await.map_err(ProxyError::Cache)?;
                            return Ok(self.response_from_temp_file(file, meta.clone(), lease.clone()));
                        }
                        Some(DownloadOutcome::Cached) | None => {
                            return Err(ProxyError::Download(
                                "Not in cache after download".into(),
                            ));
                        }
                    }
                }
                Status::Done(false) => return Err(ProxyError::Download("Download failed".into())),
                Status::Pending | Status::Downloading => {
                    if rx.changed().await.is_err() {
                        return Err(ProxyError::Download("Download cancelled".into()));
                    }
                }
            }
        }
    }

    async fn perform_download(&self, key: &str, url: &str, dl: Arc<ActiveDownload>) -> Result<Response> {
        let mut guard = DownloadGuard::new(&self.active, key.to_string(), dl.as_ref());
        guard.start();

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

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                metrics::record_error();
                return Err(e.into());
            }
        };

        let status = resp.status();

        if status == reqwest::StatusCode::NOT_MODIFIED {
            info!(key, "304 Not Modified");
            metrics::record_304();

            if let Err(e) = self.cache.touch(key).await {
                warn!(key, error = %e, "Failed to touch cache entry");
            }

            *dl.outcome.lock().await = Some(Arc::new(DownloadOutcome::Cached));
            guard.complete(true);

            if let Some((file, meta)) = self.cache.open(key).await {
                return Ok(self.response_from_file(file, meta));
            }

            return Err(ProxyError::Download("Not in cache after 304".into()));
        }

        if !status.is_success() {
            warn!(key, %status, "Upstream error");
            metrics::record_error();
            guard.complete(false);

            return Err(ProxyError::UpstreamStatus(
                axum::http::StatusCode::from_u16(status.as_u16())
                    .unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            ));
        }

        let _ = dl.tx.send(Status::Downloading);

        let content_len = resp.content_length().unwrap_or(0);
        if content_len > MAX_DOWNLOAD_SIZE {
            warn!(key, size = content_len, max = MAX_DOWNLOAD_SIZE, "File too large");
            guard.complete(false);
            return Err(ProxyError::Download("File too large".into()));
        }

        let headers = resp.headers().clone();
        let filtered_headers = self.filter_headers(&headers);

        let (temp_path, mut temp_file) = self.cache.create_temp_data().await.map_err(|e| {
            metrics::record_error();
            ProxyError::Download(format!("Failed to create temp file: {e}"))
        })?;

        let temp_guard = TempPathGuard::new(temp_path.clone());

        info!(key, size = content_len, "Downloading");

        let mut stream = resp.bytes_stream();
        let mut written: u64 = 0;

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    written = written.saturating_add(bytes.len() as u64);
                    if written > MAX_DOWNLOAD_SIZE {
                        warn!(key, "Download exceeded maximum size");
                        guard.complete(false);
                        return Err(ProxyError::Download("File too large".into()));
                    }
                    if let Err(e) = temp_file.write_all(&bytes).await {
                        metrics::record_error();
                        guard.complete(false);
                        return Err(ProxyError::Cache(e));
                    }
                }
                Err(e) => {
                    metrics::record_error();
                    warn!(key, error = %e, "Download error");
                    guard.complete(false);
                    return Err(e.into());
                }
            }
        }

        if let Err(e) = temp_file.sync_all().await {
            metrics::record_error();
            guard.complete(false);
            return Err(ProxyError::Cache(e));
        }
        drop(temp_file);

        let size = written;

        if content_len > 0 && size != content_len {
            warn!(key, expected = content_len, actual = size, "Size mismatch");
            guard.complete(false);
            return Err(ProxyError::Download("Incomplete download".into()));
        }

        metrics::record_download(size);

        let meta = Metadata {
            headers: filtered_headers,
            url: url.into(),
            key: key.into(),
            stored_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            size,
            etag: headers
                .get(http::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(Into::into),
            last_modified: headers
                .get(http::header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(Into::into),
        };

        match self.cache.commit_temp_data(key, temp_path.clone(), &meta).await {
            Ok(_) => {
                *dl.outcome.lock().await = Some(Arc::new(DownloadOutcome::Cached));
                guard.complete(true);
                info!(key, size, "Download complete");

                if let Some((file, meta)) = self.cache.open(key).await {
                    return Ok(self.response_from_file(file, meta));
                }

                Err(ProxyError::Download("Cached but cannot open".into()))
            }
            Err(e) => {
                warn!(key, error = %e, "Failed to cache");
                let lease = Arc::new(TempFileLease { path: temp_path.clone() });
                *dl.outcome.lock().await = Some(Arc::new(DownloadOutcome::Temp {
                    lease: lease.clone(),
                    meta: meta.clone(),
                }));
                guard.complete(true);

                let _temp_guard = temp_guard.keep();
                let file = File::open(&temp_path).await.map_err(ProxyError::Cache)?;
                Ok(self.response_from_temp_file(file, meta, lease))
            }
        }
    }

    fn filter_headers(&self, headers: &reqwest::header::HeaderMap) -> axum::http::HeaderMap {
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

    fn response_from_file(&self, file: File, mut meta: Metadata) -> Response {
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

    fn response_from_temp_file(&self, file: File, mut meta: Metadata, lease: Arc<TempFileLease>) -> Response {
        if !meta.headers.contains_key(CONTENT_LENGTH) {
            if let Ok(v) = axum::http::HeaderValue::from_str(&meta.size.to_string()) {
                meta.headers.insert(CONTENT_LENGTH, v);
            }
        }

        let stream = LeaseStream {
            inner: ReaderStream::new(file),
            _lease: lease,
        };

        let mut resp = Response::new(Body::from_stream(stream));
        resp.headers_mut().extend(meta.headers);
        resp
    }
}