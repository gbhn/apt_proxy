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

const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SIZE: u64 = 2 << 30;
const BUFFER_SIZE: usize = 64 * 1024;

const FORWARD_HEADERS: &[axum::http::HeaderName] = &[CONTENT_TYPE, CONTENT_LENGTH, ETAG, LAST_MODIFIED, CACHE_CONTROL];

#[derive(Clone, Debug)]
enum State {
    Starting,
    Streaming { written: u64, len: Option<u64>, headers: Arc<axum::http::HeaderMap> },
    Done { cached: bool },
    Failed(String),
}

struct Download {
    tx: watch::Sender<State>,
    rx: watch::Receiver<State>,
    path: PathBuf,
}

impl Download {
    fn new(path: PathBuf) -> Self {
        let (tx, rx) = watch::channel(State::Starting);
        Self { tx, rx, path }
    }
    fn notify(&self, s: State) { let _ = self.tx.send(s); }
    fn subscribe(&self) -> watch::Receiver<State> { self.rx.clone() }
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
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .pool_max_idle_per_host(32)
            .build()
            .expect("HTTP client");

        Self { client, cache, active: Arc::new(DashMap::new()) }
    }

    fn repo(key: &str) -> &str { key.split('/').next().unwrap_or("unknown") }

    pub async fn fetch(&self, key: &str, url: &str) -> Result<Response> {
        let repo = Self::repo(key);

        // Cache hit
        if let Some((file, meta)) = self.cache.open(key).await {
            metrics::record_repo_hit(repo, meta.size);
            return Ok(file_response(file, meta));
        }
        metrics::record_repo_miss(repo);

        // Join existing download
        if let Some(dl) = self.active.get(key) {
            metrics::record_coalesced();
            return self.follow(key, dl.clone()).await;
        }

        // Start new download
        self.start(key, url, repo).await
    }

    async fn start(&self, key: &str, url: &str, repo: &str) -> Result<Response> {
        let (path, file) = self.cache.create_temp_data().await.map_err(|e| {
            ProxyError::download(format!("Temp file: {e}"))
        })?;

        let dl = Arc::new(Download::new(path.clone()));

        // Atomic insert
        use dashmap::mapref::entry::Entry;
        match self.active.entry(key.to_string()) {
            Entry::Occupied(e) => {
                metrics::record_coalesced();
                let existing = e.get().clone();
                drop(e);
                let _ = tokio::fs::remove_file(&path).await;
                return self.follow(key, existing).await;
            }
            Entry::Vacant(v) => { v.insert(dl.clone()); }
        }

        metrics::inc_active();
        metrics::record_download_started();

        let this = self.clone();
        let key_owned = key.to_string();
        let url_owned = url.to_string();
        let repo_owned = repo.to_string();
        let dl2 = dl.clone();

        tokio::spawn(async move {
            this.do_download(&key_owned, &url_owned, &repo_owned, file, &dl2).await;
        });

        self.follow(key, dl).await
    }

    async fn follow(&self, key: impl AsRef<str>, dl: Arc<Download>) -> Result<Response> {
        let key = key.as_ref();
        let mut rx = dl.subscribe();

        // Wait for headers
        let (headers, len) = loop {
            let state = {
                let borrowed = rx.borrow_and_update();
                borrowed.clone()
            };
            
            match state {
                State::Starting => { 
                    if rx.changed().await.is_err() { 
                        return Err(ProxyError::download("Cancelled")); 
                    } 
                }
                State::Streaming { headers, len, .. } => break (headers, len),
                State::Done { cached: true } => return self.cache.open(key).await
                    .map(|(f, m)| file_response(f, m))
                    .ok_or_else(|| ProxyError::download("Cache miss after 304")),
                State::Done { cached: false } => return self.cache.open(key).await
                    .map(|(f, m)| file_response(f, m))
                    .ok_or_else(|| ProxyError::download("Not found after download")),
                State::Failed(e) => return Err(ProxyError::Download(e)),
            }
        };

        let stream = follow_stream(dl.path.clone(), rx);
        let mut resp = Response::new(Body::from_stream(stream));
        resp.headers_mut().extend((*headers).clone());
        if let Some(l) = len {
            if let Ok(v) = axum::http::HeaderValue::from_str(&l.to_string()) {
                resp.headers_mut().insert(CONTENT_LENGTH, v);
            }
        }
        Ok(resp)
    }

    async fn do_download(&self, key: &str, url: &str, repo: &str, mut file: File, dl: &Download) {
        let start = Instant::now();
        let path = dl.path.clone();

        let result = self.download_inner(key, url, repo, &mut file, dl).await;
        drop(file);

        self.active.remove(key);
        metrics::dec_active();
        metrics::clear_download_progress(key);

        match result {
            Ok(Some((meta, bytes))) => {
                match self.cache.commit(key, path.clone(), &meta).await {
                    Ok(_) => {
                        info!(key, size = meta.size, "Downloaded");
                        dl.notify(State::Done { cached: false });
                        metrics::record_download_complete(bytes, start.elapsed(), repo);
                    }
                    Err(e) => {
                        warn!(key, error = %e, "Commit failed");
                        let _ = tokio::fs::remove_file(&path).await;
                        dl.notify(State::Failed(format!("Commit: {e}")));
                        metrics::record_download_failed("commit", repo);
                    }
                }
            }
            Ok(None) => { let _ = tokio::fs::remove_file(&path).await; }
            Err(e) => {
                warn!(key, error = %e, "Download failed");
                dl.notify(State::Failed(e.to_string()));
                let _ = tokio::fs::remove_file(&path).await;
                metrics::record_download_failed("error", repo);
            }
        }
    }

    async fn download_inner(&self, key: &str, url: &str, repo: &str, file: &mut File, dl: &Download) -> Result<Option<(Metadata, u64)>> {
        debug!(key, url, "Downloading");

        let mut req = self.client.get(url);
        if let Some(meta) = self.cache.get_metadata(key).await {
            if let Some(ref etag) = meta.etag { req = req.header("If-None-Match", etag); }
            if let Some(ref lm) = meta.last_modified { req = req.header("If-Modified-Since", lm); }
        }

        let resp = req.send().await.map_err(|e| {
            if e.is_timeout() { metrics::record_upstream_timeout(repo); }
            ProxyError::Upstream(e)
        })?;

        let status = resp.status();

        if status == reqwest::StatusCode::NOT_MODIFIED {
            info!(key, "304 Not Modified");
            metrics::record_304();
            let _ = self.cache.touch(key).await;
            if self.cache.open(key).await.is_some() {
                dl.notify(State::Done { cached: true });
            }
            return Ok(None);
        }

        if !status.is_success() {
            metrics::record_upstream_error(&format!("status_{}", status.as_u16()), repo);
            return Err(ProxyError::UpstreamStatus(
                axum::http::StatusCode::from_u16(status.as_u16()).unwrap_or(axum::http::StatusCode::BAD_GATEWAY)
            ));
        }

        let content_len = resp.content_length();
        if content_len.unwrap_or(0) > MAX_SIZE {
            return Err(ProxyError::download("File too large"));
        }

        // Extract headers BEFORE consuming response with bytes_stream()
        let resp_headers = resp.headers().clone();
        let etag = resp_headers.get(http::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let last_modified = resp_headers.get(http::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        
        let headers = filter_headers(&resp_headers);
        let headers_arc = Arc::new(headers.clone());

        dl.notify(State::Streaming { written: 0, len: content_len, headers: headers_arc.clone() });

        // Now consume the response
        let mut stream = resp.bytes_stream();
        let mut written = 0u64;

        while let Some(chunk) = stream.next().await {
            let bytes = chunk.map_err(ProxyError::Upstream)?;
            written += bytes.len() as u64;
            if written > MAX_SIZE { return Err(ProxyError::download("Size exceeded")); }
            
            file.write_all(&bytes).await.map_err(ProxyError::Cache)?;
            file.flush().await.map_err(ProxyError::Cache)?;
            
            dl.notify(State::Streaming { written, len: content_len, headers: headers_arc.clone() });
            metrics::set_download_progress(key, written);
        }

        file.sync_all().await.map_err(ProxyError::Cache)?;

        if let Some(expected) = content_len {
            if written != expected {
                return Err(ProxyError::download(format!("Incomplete: {written}/{expected}")));
            }
        }

        metrics::record_storage_write(written);

        let meta = Metadata::new(key, url, written, headers)
            .with_conditionals(etag, last_modified);

        Ok(Some((meta, written)))
    }
}

fn filter_headers(h: &reqwest::header::HeaderMap) -> axum::http::HeaderMap {
    let mut out = axum::http::HeaderMap::new();
    for name in FORWARD_HEADERS {
        if let Some(v) = h.get(name.as_str()) {
            if let Ok(v) = axum::http::HeaderValue::from_bytes(v.as_bytes()) {
                out.insert(name.clone(), v);
            }
        }
    }
    out
}

fn file_response(file: File, mut meta: Metadata) -> Response {
    if !meta.headers.contains_key(CONTENT_LENGTH) {
        if let Ok(v) = axum::http::HeaderValue::from_str(&meta.size.to_string()) {
            meta.headers.insert(CONTENT_LENGTH, v);
        }
    }
    let mut resp = Response::new(Body::from_stream(ReaderStream::new(file)));
    resp.headers_mut().extend(meta.headers);
    resp
}

fn follow_stream(path: PathBuf, mut rx: watch::Receiver<State>) -> impl Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send {
    stream! {
        let mut file = match File::open(&path).await {
            Ok(f) => f,
            Err(e) => { yield Err(e); return; }
        };

        let mut pos = 0u64;
        let mut buf = vec![0u8; BUFFER_SIZE];

        loop {
            // Clone state to release the lock before any await
            let (written, done) = {
                let state = rx.borrow_and_update().clone();
                match state {
                    State::Starting => (0, false),
                    State::Streaming { written, .. } => (written, false),
                    State::Done { .. } => (u64::MAX, true),
                    State::Failed(e) => { yield Err(std::io::Error::other(e)); return; }
                }
            };

            while pos < written {
                // Calculate read size before borrowing buf
                let remaining = (written - pos) as usize;
                let buf_len = buf.len();
                let to_read = remaining.min(buf_len);
                
                match file.read(&mut buf[..to_read]).await {
                    Ok(0) => {
                        let _ = file.seek(SeekFrom::Start(pos)).await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        break;
                    }
                    Ok(n) => { 
                        pos += n as u64; 
                        yield Ok(Bytes::copy_from_slice(&buf[..n])); 
                    }
                    Err(e) => { yield Err(e); return; }
                }
            }

            if done {
                loop {
                    match file.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => yield Ok(Bytes::copy_from_slice(&buf[..n])),
                        Err(e) => { yield Err(e); return; }
                    }
                }
                return;
            }

            if rx.changed().await.is_err() {
                yield Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Interrupted"));
                return;
            }
        }
    }
}