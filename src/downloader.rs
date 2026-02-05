use crate::cache::CacheManager;
use crate::error::{ProxyError, Result};
use crate::metrics;
use crate::storage::Metadata;
use axum::{body::Body, response::Response};
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

const TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, Copy, PartialEq)]
enum Status {
    Pending,
    Active,
    Done(bool),
}

struct Download {
    tx: watch::Sender<Status>,
    rx: watch::Receiver<Status>,
}

impl Download {
    fn new() -> Self {
        let (tx, rx) = watch::channel(Status::Pending);
        Self { tx, rx }
    }
}

#[derive(Clone)]
pub struct Downloader {
    client: reqwest::Client,
    cache: CacheManager,
    active: Arc<DashMap<String, Arc<Download>>>,
}

impl Downloader {
    pub fn new(cache: CacheManager) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(concat!("apt-cacher-rs/", env!("CARGO_PKG_VERSION")))
            .timeout(TIMEOUT)
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(16)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            cache,
            active: Arc::new(DashMap::new()),
        }
    }

    pub async fn fetch(&self, key: &str, url: &str) -> Result<Response> {
        // Check cache first
        if let Some((reader, meta)) = self.cache.get(key).await {
            metrics::record_hit(meta.size);
            let stream = tokio_util::io::ReaderStream::with_capacity(reader, 64 * 1024);
            let mut resp = Response::new(Body::from_stream(stream));
            resp.headers_mut().extend(meta.headers);
            return Ok(resp);
        }

        metrics::record_miss();

        // Coalesce concurrent requests for same key
        let (dl, is_leader) = self.get_or_create(key);

        if !is_leader {
            metrics::record_coalesced();
            return self.wait_for_download(key, &dl).await;
        }

        metrics::inc_active();
        let result = self.do_download(key, url, &dl).await;
        metrics::dec_active();
        self.active.remove(key);

        result
    }

    fn get_or_create(&self, key: &str) -> (Arc<Download>, bool) {
        match self.active.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(e) => (e.get().clone(), false),
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let dl = Arc::new(Download::new());
                e.insert(dl.clone());
                (dl, true)
            }
        }
    }

    async fn wait_for_download(&self, key: &str, dl: &Download) -> Result<Response> {
        let mut rx = dl.rx.clone();

        loop {
            let status = *rx.borrow_and_update();
            match status {
                Status::Done(true) => {
                    if let Some((reader, meta)) = self.cache.get(key).await {
                        let stream =
                            tokio_util::io::ReaderStream::with_capacity(reader, 64 * 1024);
                        let mut resp = Response::new(Body::from_stream(stream));
                        resp.headers_mut().extend(meta.headers);
                        return Ok(resp);
                    }
                    return Err(ProxyError::Download("Cache miss after download".into()));
                }
                Status::Done(false) => {
                    return Err(ProxyError::Download("Download failed".into()));
                }
                _ => {
                    if rx.changed().await.is_err() {
                        return Err(ProxyError::Download("Download cancelled".into()));
                    }
                }
            }
        }
    }

    async fn do_download(&self, key: &str, url: &str, dl: &Download) -> Result<Response> {
        debug!(url, "Fetching from upstream");

        // Check for conditional request headers
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

        // Handle 304 Not Modified
        if status == reqwest::StatusCode::NOT_MODIFIED {
            info!(key, "304 Not Modified");
            metrics::record_304();
            let _ = dl.tx.send(Status::Done(true));

            if let Some((reader, meta)) = self.cache.get(key).await {
                let stream = tokio_util::io::ReaderStream::with_capacity(reader, 64 * 1024);
                let mut response = Response::new(Body::from_stream(stream));
                response.headers_mut().extend(meta.headers);
                return Ok(response);
            }
            return Err(ProxyError::Download("Cache miss after 304".into()));
        }

        // Handle error responses
        if !status.is_success() {
            warn!(key, %status, "Upstream error");
            metrics::record_error();
            let _ = dl.tx.send(Status::Done(false));
            return Err(ProxyError::UpstreamStatus(
                axum::http::StatusCode::from_u16(status.as_u16())
                    .unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            ));
        }

        let _ = dl.tx.send(Status::Active);

        let content_len = resp.content_length().unwrap_or(0);
        info!(key, size = content_len, "Downloading");

        let meta = Metadata::from_response(&resp, url, content_len);
        let headers = meta.headers.clone();

        // Download to buffer
        let mut data = Vec::with_capacity(content_len as usize);
        let mut stream = resp.bytes_stream();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(e) => {
                    warn!(key, error = %e, "Download failed");
                    let _ = dl.tx.send(Status::Done(false));
                    return Err(e.into());
                }
            }
        }

        let size = data.len() as u64;
        let final_meta = Metadata { size, ..meta };

        // Cache the response
        if let Err(e) = self.cache.put(key, &data, &final_meta).await {
            warn!(key, error = %e, "Failed to cache");
        }

        metrics::record_download(size);
        let _ = dl.tx.send(Status::Done(true));

        info!(key, size, "Download complete");

        let mut resp = Response::new(Body::from(Bytes::from(data)));
        resp.headers_mut().extend(headers);
        Ok(resp)
    }
}