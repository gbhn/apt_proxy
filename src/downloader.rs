use crate::cache::CacheManager;
use crate::error::{ProxyError, Result};
use crate::metrics;
use crate::storage::Metadata;
use axum::http::header::{
    CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED,
};
use axum::{body::Body, response::Response};
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
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

struct ActiveDownload {
    tx: watch::Sender<Status>,
    rx: watch::Receiver<Status>,
}

impl ActiveDownload {
    fn new() -> Self {
        let (tx, rx) = watch::channel(Status::Pending);
        Self { tx, rx }
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
        metrics::inc_active();
        Self {
            active,
            key,
            download,
            completed: false,
        }
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
        if let Some((data, meta)) = self.cache.get(key).await {
            metrics::record_hit(meta.size);
            debug!(key, size = meta.size, "Cache hit");
            return Ok(self.response_from_data(data, meta));
        }

        metrics::record_miss();

        let (download, is_leader) = self.get_or_create_download(key);

        if !is_leader {
            metrics::record_coalesced();
            debug!(key, "Waiting for existing download");
            return self.wait_for_download(key, &download).await;
        }

        self.perform_download(key, url, &download).await
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

    async fn wait_for_download(&self, key: &str, dl: &ActiveDownload) -> Result<Response> {
        let mut rx = dl.rx.clone();

        loop {
            let status = {
                let borrowed = rx.borrow_and_update();
                *borrowed
            };

            match status {
                Status::Done(true) => {
                    return match self.cache.get(key).await {
                        Some((data, meta)) => Ok(self.response_from_data(data, meta)),
                        None => Err(ProxyError::Download("Not in cache after download".into())),
                    };
                }
                Status::Done(false) => {
                    return Err(ProxyError::Download("Download failed".into()));
                }
                Status::Pending | Status::Downloading => {
                    if rx.changed().await.is_err() {
                        return Err(ProxyError::Download("Download cancelled".into()));
                    }
                }
            }
        }
    }

    async fn perform_download(
        &self,
        key: &str,
        url: &str,
        dl: &ActiveDownload,
    ) -> Result<Response> {
        let mut guard = DownloadGuard::new(&self.active, key.to_string(), dl);

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

        let resp = req.send().await?;
        let status = resp.status();

        if status == reqwest::StatusCode::NOT_MODIFIED {
            info!(key, "304 Not Modified");
            metrics::record_304();
            guard.complete(true);

            return match self.cache.get(key).await {
                Some((data, meta)) => Ok(self.response_from_data(data, meta)),
                None => Err(ProxyError::Download("Not in cache after 304".into())),
            };
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

        info!(key, size = content_len, "Downloading");

        let headers = resp.headers().clone();
        let initial_capacity = (content_len as usize).min(10 * 1024 * 1024);
        let mut data = Vec::with_capacity(initial_capacity);
        let mut stream = resp.bytes_stream();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if (data.len() as u64).saturating_add(bytes.len() as u64) > MAX_DOWNLOAD_SIZE {
                        warn!(key, "Download exceeded maximum size");
                        guard.complete(false);
                        return Err(ProxyError::Download("File too large".into()));
                    }
                    data.extend_from_slice(&bytes);
                }
                Err(e) => {
                    warn!(key, error = %e, "Download error");
                    guard.complete(false);
                    return Err(e.into());
                }
            }
        }

        let size = data.len() as u64;

        if content_len > 0 && size != content_len {
            warn!(key, expected = content_len, actual = size, "Size mismatch");
            guard.complete(false);
            return Err(ProxyError::Download("Incomplete download".into()));
        }

        let filtered_headers = self.filter_headers(&headers);

        let meta = Metadata {
            headers: filtered_headers.clone(),
            url: url.into(),
            key: key.into(),
            stored_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
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

        if let Err(e) = self.cache.put(key, &data, &meta).await {
            warn!(key, error = %e, "Failed to cache");
        }

        metrics::record_download(size);
        guard.complete(true);

        info!(key, size, "Download complete");

        Ok(self.response_from_data(data, meta))
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

    fn response_from_data(&self, data: Vec<u8>, meta: Metadata) -> Response {
        let mut resp = Response::new(Body::from(Bytes::from(data)));
        resp.headers_mut().extend(meta.headers);
        resp
    }
}