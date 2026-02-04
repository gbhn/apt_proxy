use crate::storage::{CacheMetadata, Storage};
use axum::{body::Body, response::Response};
use bytes::Bytes;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncRead, ReadBuf},
    sync::{watch, Notify},
    time::timeout,
};
use tracing::{error, info, warn, debug};

const HEADER_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const FILE_READY_TIMEOUT: Duration = Duration::from_secs(60);
const READ_BUFFER_SIZE: usize = 256 * 1024;
const PROGRESS_LOG_INTERVAL: u64 = 50 * 1024 * 1024; // Log every 50MB

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum DownloadStatus {
    Pending,
    Downloading { status_code: u16 },
    Completed { success: bool },
}

impl DownloadStatus {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Downloading { status_code } => Some(*status_code),
            Self::Completed { success: true } => Some(200),
            Self::Completed { success: false } => Some(502),
            Self::Pending => None,
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed { success: true })
    }
}

struct DownloadState {
    status_tx: watch::Sender<DownloadStatus>,
    status_rx: watch::Receiver<DownloadStatus>,
    notify_file_ready: Notify,
    is_file_ready: AtomicBool,
    notify_data: Notify,
    bytes_written: AtomicU64,
    metadata: tokio::sync::OnceCell<Arc<CacheMetadata>>,
    waiters: AtomicU64, // Track number of concurrent clients
}

impl DownloadState {
    fn new() -> Self {
        let (status_tx, status_rx) = watch::channel(DownloadStatus::Pending);
        Self {
            status_tx,
            status_rx,
            notify_file_ready: Notify::new(),
            is_file_ready: AtomicBool::new(false),
            notify_data: Notify::new(),
            bytes_written: AtomicU64::new(0),
            metadata: tokio::sync::OnceCell::new(),
            waiters: AtomicU64::new(0),
        }
    }

    async fn wait_for_status(&self) -> u16 {
        let mut rx = self.status_rx.clone();
        loop {
            let status = *rx.borrow_and_update();
            if let Some(code) = status.status_code() {
                return code;
            }
            if rx.changed().await.is_err() {
                return 502;
            }
        }
    }

    async fn wait_for_file(&self) -> bool {
        loop {
            if self.is_file_ready.load(Ordering::Acquire) {
                return true;
            }
            if self.status().is_finished() {
                return false;
            }
            self.notify_file_ready.notified().await;
        }
    }

    fn status(&self) -> DownloadStatus {
        *self.status_rx.borrow()
    }

    fn set_status_code(&self, code: u16) {
        let _ = self.status_tx.send(DownloadStatus::Downloading { status_code: code });
    }

    fn mark_file_ready(&self) {
        self.is_file_ready.store(true, Ordering::Release);
        self.notify_file_ready.notify_waiters();
    }

    fn mark_finished(&self, success: bool) {
        let _ = self.status_tx.send(DownloadStatus::Completed { success });
        self.notify_data.notify_waiters();
        self.notify_file_ready.notify_waiters();
    }

    fn notify_progress(&self, bytes: u64) {
        self.bytes_written.store(bytes, Ordering::Release);
        self.notify_data.notify_waiters();
    }

    fn add_waiter(&self) {
        self.waiters.fetch_add(1, Ordering::Relaxed);
    }

    fn remove_waiter(&self) {
        self.waiters.fetch_sub(1, Ordering::Relaxed);
    }

    fn waiter_count(&self) -> u64 {
        self.waiters.load(Ordering::Relaxed)
    }
}

pub struct DownloadManager {
    client: reqwest::Client,
    storage: Arc<Storage>,
    active: Arc<DashMap<Arc<str>, Arc<DownloadState>>>,
}

impl DownloadManager {
    pub fn new(client: reqwest::Client, storage: Arc<Storage>) -> Self {
        Self {
            client,
            storage,
            active: Arc::new(DashMap::with_capacity(128)),
        }
    }

    pub async fn get_or_download(&self, key: &str, upstream_url: String) -> crate::error::Result<Response> {
        let (state, is_new) = self.get_or_create_download(key);
        state.add_waiter();

        if is_new {
            self.spawn_download_task(key.to_string(), upstream_url, state.clone());
        }

        let status_code = timeout(HEADER_WAIT_TIMEOUT, state.wait_for_status())
            .await
            .map_err(|_| crate::error::ProxyError::Download("Header timeout".into()))?;

        if status_code != 200 {
            state.remove_waiter();
            return Err(crate::error::ProxyError::UpstreamError(
                axum::http::StatusCode::from_u16(status_code)
                    .unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            ));
        }

        let file_ready = timeout(FILE_READY_TIMEOUT, state.wait_for_file())
            .await
            .map_err(|_| crate::error::ProxyError::Download("File ready timeout".into()))?;

        if !file_ready {
            state.remove_waiter();
            if state.status().is_success() {
                return self.serve_from_storage(key, &state).await;
            } else {
                return Err(crate::error::ProxyError::Download("Download failed".into()));
            }
        }

        let temp_path = self.storage.path_for(key).with_extension("tmp");
        match File::open(&temp_path).await {
            Ok(file) => {
                let stream = StreamingReader::new(file, state.clone());
                let mut response = Response::new(Body::from_stream(stream));
                if let Some(meta) = state.metadata.get() {
                    response.headers_mut().extend(meta.headers.clone());
                }
                Ok(response)
            }
            Err(_) => {
                state.remove_waiter();
                self.serve_from_storage(key, &state).await
            }
        }
    }

    async fn serve_from_storage(&self, key: &str, state: &Arc<DownloadState>) -> crate::error::Result<Response> {
        if !state.status().is_success() {
            return Err(crate::error::ProxyError::Download("Download failed".into()));
        }

        let stored = self.storage.open(key).await
            .map_err(|e| crate::error::ProxyError::Cache(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .ok_or_else(|| crate::error::ProxyError::Download("Not found in storage".into()))?;

        let stream = tokio_util::io::ReaderStream::with_capacity(stored.file, READ_BUFFER_SIZE);
        let mut response = Response::new(Body::from_stream(stream));
        response.headers_mut().extend(stored.metadata.headers.clone());
        Ok(response)
    }

    fn get_or_create_download(&self, key: &str) -> (Arc<DownloadState>, bool) {
        let key_arc: Arc<str> = Arc::from(key);
        match self.active.entry(key_arc.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                debug!("Joining download: {}", shorten_path(key));
                (entry.get().clone(), false)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                info!("Starting download: {}", shorten_path(key));
                let state = Arc::new(DownloadState::new());
                entry.insert(state.clone());
                (state, true)
            }
        }
    }

    fn spawn_download_task(&self, key: String, upstream_url: String, state: Arc<DownloadState>) {
        let client = self.client.clone();
        let storage = self.storage.clone();
        let active = self.active.clone();
        let key_arc = Arc::from(key.as_str());

        tokio::spawn(async move {
            match download_file(client, upstream_url, &key, storage, state.clone()).await {
                Ok(size) => info!("Download completed: {} ({})", shorten_path(&key), crate::utils::format_size(size)),
                Err(e) => error!("Download failed: {} - {}", shorten_path(&key), e),
            }
            active.remove(&key_arc);
        });
    }

    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}

async fn download_file(
    client: reqwest::Client,
    url: String,
    key: &str,
    storage: Arc<Storage>,
    state: Arc<DownloadState>,
) -> anyhow::Result<u64> {
    debug!("Fetching: {}", url);
    let response = client.get(&url).send().await?;
    let status = response.status().as_u16();

    state.set_status_code(status);

    if !response.status().is_success() {
        warn!("Upstream error: {} returned {}", shorten_path(key), status);
        state.mark_finished(false);
        return Ok(0);
    }

    let content_length = response.content_length().unwrap_or(0);
    let waiters = state.waiter_count();
    
    if content_length > 0 {
        info!("Downloading {} ({}) for {} client(s)", 
            shorten_path(key), 
            crate::utils::format_size(content_length),
            waiters
        );
    } else {
        info!("Downloading {} (unknown size) for {} client(s)", shorten_path(key), waiters);
    }

    let response_headers = response.headers().clone();
    
    let mut metadata = CacheMetadata {
        headers: response_headers,
        original_url: url.clone(),
        stored_at: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs(),
        content_length,
    };

    state.metadata.set(Arc::new(metadata.clone())).ok();

    let mut writer = storage.create(key).await?;
    state.mark_file_ready();

    let mut stream = response.bytes_stream();
    let mut last_log_bytes = 0u64;
    
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        writer.write(&chunk).await?;
        let bytes = writer.bytes_written();
        state.notify_progress(bytes);
        
        // Log progress only for large files at intervals
        if content_length > PROGRESS_LOG_INTERVAL && bytes - last_log_bytes >= PROGRESS_LOG_INTERVAL {
            let progress = if content_length > 0 {
                format!("{}/{} ({:.0}%)", 
                    crate::utils::format_size(bytes),
                    crate::utils::format_size(content_length),
                    (bytes as f64 / content_length as f64) * 100.0
                )
            } else {
                crate::utils::format_size(bytes)
            };
            debug!("Progress {}: {}", shorten_path(key), progress);
            last_log_bytes = bytes;
        }
    }

    let total = writer.bytes_written();
    metadata.content_length = total;

    writer.finalize(&storage, metadata).await?;
    state.mark_finished(true);
    Ok(total)
}

struct StreamingReader {
    file: File,
    state: Arc<DownloadState>,
    buffer: Box<[u8]>,
    position: u64,
}

impl StreamingReader {
    fn new(file: File, state: Arc<DownloadState>) -> Self {
        Self {
            file,
            state,
            buffer: vec![0u8; READ_BUFFER_SIZE].into_boxed_slice(),
            position: 0,
        }
    }
}

impl Stream for StreamingReader {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut read_buf = ReadBuf::new(&mut this.buffer);

        match Pin::new(&mut this.file).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let n = read_buf.filled().len();
                if n > 0 {
                    this.position += n as u64;
                    return Poll::Ready(Some(Ok(Bytes::copy_from_slice(read_buf.filled()))));
                }

                let status = this.state.status();
                if status.is_finished() {
                    // Clean up waiter count when stream ends
                    this.state.remove_waiter();
                    return if status.is_success() {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Download failed"))))
                    };
                }

                let bytes_written = this.state.bytes_written.load(Ordering::Acquire);
                if this.position < bytes_written {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                let waker = cx.waker().clone();
                let state = this.state.clone();
                tokio::spawn(async move {
                    state.notify_data.notified().await;
                    waker.wake();
                });

                Poll::Pending
            }
            Poll::Ready(Err(e)) => {
                this.state.remove_waiter();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for StreamingReader {
    fn drop(&mut self) {
        self.state.remove_waiter();
    }
}

/// Shorten long paths for cleaner logs
fn shorten_path(path: &str) -> &str {
    const MAX_LEN: usize = 80;
    if path.len() <= MAX_LEN {
        path
    } else {
        let end_start = path.len() - MAX_LEN + 3;
        &path[end_start..]
    }
}