use crate::cache_policy::is_not_modified_response;
use crate::config::CacheSettings;
use crate::logging::fields::{self, size};
use crate::metrics;
use crate::storage::{CacheMetadata, Storage};
use async_stream::stream;
use axum::{body::Body, response::Response};
use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::Bytes;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::{watch, RwLock},
    time::timeout,
};
use tracing::{debug, error, info, info_span, warn, Instrument};

const HEADER_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const FILE_READY_TIMEOUT: Duration = Duration::from_secs(60);
const READ_BUFFER_SIZE: usize = 64 * 1024;
const PROGRESS_LOG_INTERVAL: u64 = 50 * 1024 * 1024;
const MAX_RETRIES: u32 = 3;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum DownloadStatus {
    Pending,
    Downloading { status_code: u16 },
    NotModified,
    Completed { success: bool },
}

impl DownloadStatus {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Downloading { status_code } => Some(*status_code),
            Self::NotModified => Some(304),
            Self::Completed { success: true } => Some(200),
            Self::Completed { success: false } => Some(502),
            Self::Pending => None,
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::NotModified)
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed { success: true } | Self::NotModified)
    }
}

struct DownloadState {
    status_tx: watch::Sender<DownloadStatus>,
    status_rx: watch::Receiver<DownloadStatus>,
    notify_file_ready: tokio::sync::Notify,
    is_file_ready: AtomicBool,
    notify_data: tokio::sync::Notify,
    bytes_written: AtomicU64,
    metadata: RwLock<Option<Arc<CacheMetadata>>>,
    waiters: AtomicU64,
    file_size: AtomicU64,
    meta_size: AtomicU64,
}

impl DownloadState {
    fn new() -> Self {
        let (status_tx, status_rx) = watch::channel(DownloadStatus::Pending);
        Self {
            status_tx,
            status_rx,
            notify_file_ready: tokio::sync::Notify::new(),
            is_file_ready: AtomicBool::new(false),
            notify_data: tokio::sync::Notify::new(),
            bytes_written: AtomicU64::new(0),
            metadata: RwLock::new(None),
            waiters: AtomicU64::new(0),
            file_size: AtomicU64::new(0),
            meta_size: AtomicU64::new(0),
        }
    }

    async fn wait_for_status(&self) -> u16 {
        let mut rx = self.status_rx.clone();
        loop {
            {
                let status = rx.borrow_and_update();
                if let Some(code) = status.status_code() {
                    return code;
                }
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
        let status = if is_not_modified_response(code) {
            DownloadStatus::NotModified
        } else {
            DownloadStatus::Downloading { status_code: code }
        };
        let _ = self.status_tx.send(status);
    }

    fn mark_file_ready(&self) {
        self.is_file_ready.store(true, Ordering::Release);
        self.notify_file_ready.notify_waiters();
    }

    fn mark_finished(&self, success: bool, file_size: u64, meta_size: u64) {
        self.file_size.store(file_size, Ordering::Release);
        self.meta_size.store(meta_size, Ordering::Release);
        let _ = self.status_tx.send(DownloadStatus::Completed { success });
        self.notify_data.notify_waiters();
        self.notify_file_ready.notify_waiters();
    }

    fn mark_not_modified(&self, file_size: u64, meta_size: u64) {
        self.file_size.store(file_size, Ordering::Release);
        self.meta_size.store(meta_size, Ordering::Release);
        let _ = self.status_tx.send(DownloadStatus::NotModified);
        self.notify_data.notify_waiters();
        self.notify_file_ready.notify_waiters();
    }

    fn notify_progress(&self, bytes: u64) {
        self.bytes_written.store(bytes, Ordering::Release);
        self.notify_data.notify_waiters();
    }

    async fn set_metadata(&self, metadata: CacheMetadata) {
        *self.metadata.write().await = Some(Arc::new(metadata));
    }

    async fn update_content_length(&self, actual_size: u64) {
        let mut guard = self.metadata.write().await;
        if let Some(ref mut meta_arc) = *guard {
            let mut meta = (**meta_arc).clone();
            meta.content_length = actual_size;
            *meta_arc = Arc::new(meta);
        }
    }

    async fn get_metadata(&self) -> Option<Arc<CacheMetadata>> {
        self.metadata.read().await.clone()
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

    fn get_sizes(&self) -> (u64, u64) {
        (
            self.file_size.load(Ordering::Acquire),
            self.meta_size.load(Ordering::Acquire),
        )
    }
}

pub struct DownloadManager {
    client: reqwest::Client,
    storage: Arc<Storage>,
    settings: Arc<CacheSettings>,
    active: Arc<DashMap<Arc<str>, Arc<DownloadState>>>,
}

impl DownloadManager {
    pub fn new(
        client: reqwest::Client,
        storage: Arc<Storage>,
        settings: Arc<CacheSettings>,
    ) -> Self {
        Self {
            client,
            storage,
            settings,
            active: Arc::new(DashMap::with_capacity(128)),
        }
    }

    /// Создаёт ExponentialBackoff для retry логики
    fn create_backoff() -> ExponentialBackoff {
        ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(5),
            max_elapsed_time: Some(Duration::from_secs(30)),
            multiplier: 2.0,
            ..Default::default()
        }
    }

    pub async fn get_or_download(
        &self,
        key: &str,
        upstream_url: String,
        existing_metadata: Option<CacheMetadata>,
    ) -> crate::error::Result<(Response, u64, u64, CacheMetadata)> {
        let (state, is_new) = self.get_or_create_download(key);
        state.add_waiter();

        if is_new {
            metrics::increment_active_downloads();
            self.spawn_download_task(
                key.to_string(),
                upstream_url,
                state.clone(),
                existing_metadata,
            );
        } else {
            metrics::record_coalesced_request();
        }

        let status_code = timeout(HEADER_WAIT_TIMEOUT, state.wait_for_status())
            .await
            .map_err(|_| crate::error::ProxyError::Timeout("Header timeout".into()))?;

        if status_code == 304 {
            state.remove_waiter();
            metrics::record_304();
            return self.serve_from_storage(key, &state).await;
        }

        if status_code != 200 {
            state.remove_waiter();
            metrics::record_upstream_error();
            return Err(crate::error::ProxyError::UpstreamError(
                axum::http::StatusCode::from_u16(status_code)
                    .unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            ));
        }

        let file_ready = timeout(FILE_READY_TIMEOUT, state.wait_for_file())
            .await
            .map_err(|_| crate::error::ProxyError::Timeout("File ready timeout".into()))?;

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
                let stream = create_streaming_reader(file, state.clone());
                let mut response = Response::new(Body::from_stream(stream));
                if let Some(meta) = state.get_metadata().await {
                    response.headers_mut().extend(meta.headers.clone());
                }

                let (file_size, meta_size) = state.get_sizes();
                let metadata = state
                    .get_metadata()
                    .await
                    .map(|m| (*m).clone())
                    .unwrap_or_else(|| CacheMetadata {
                        headers: Default::default(),
                        original_url: String::new(),
                        key: Some(key.to_string()),
                        stored_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        content_length: file_size,
                        etag: None,
                        last_modified: None,
                    });

                Ok((response, file_size, meta_size, metadata))
            }
            Err(_) => {
                state.remove_waiter();
                self.serve_from_storage(key, &state).await
            }
        }
    }

    async fn serve_from_storage(
        &self,
        key: &str,
        state: &Arc<DownloadState>,
    ) -> crate::error::Result<(Response, u64, u64, CacheMetadata)> {
        if !state.status().is_success() {
            return Err(crate::error::ProxyError::Download("Download failed".into()));
        }

        let stored = self
            .storage
            .open(key)
            .await
            .map_err(|e| {
                crate::error::ProxyError::Cache(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?
            .ok_or_else(|| crate::error::ProxyError::Download("Not found in storage".into()))?;

        let stream = tokio_util::io::ReaderStream::with_capacity(stored.file, READ_BUFFER_SIZE);
        let mut response = Response::new(Body::from_stream(stream));
        response
            .headers_mut()
            .extend(stored.metadata.headers.clone());

        let (file_size, meta_size) = state.get_sizes();
        Ok((response, file_size, meta_size, stored.metadata))
    }

    fn get_or_create_download(&self, key: &str) -> (Arc<DownloadState>, bool) {
        let key_arc: Arc<str> = Arc::from(key);
        match self.active.entry(key_arc.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                debug!(path = %fields::path(key), "Joining existing download");
                (entry.get().clone(), false)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                info!(path = %fields::path(key), "Starting new download");
                let state = Arc::new(DownloadState::new());
                entry.insert(state.clone());
                (state, true)
            }
        }
    }

    fn spawn_download_task(
        &self,
        key: String,
        upstream_url: String,
        state: Arc<DownloadState>,
        existing_metadata: Option<CacheMetadata>,
    ) {
        let client = self.client.clone();
        let storage = self.storage.clone();
        let settings = self.settings.clone();
        let active = self.active.clone();
        let key_arc = Arc::from(key.as_str());

        let span = info_span!("download", path = %fields::path(&key));

        tokio::spawn(
            async move {
                let start = std::time::Instant::now();
                
                // Используем backoff для retry
                let result = download_with_retry(
                    client,
                    upstream_url,
                    &key,
                    storage,
                    settings,
                    state.clone(),
                    existing_metadata,
                )
                .await;

                match result {
                    Ok((downloaded_size, _meta_size)) => {
                        let elapsed = start.elapsed();
                        if downloaded_size > 0 {
                            metrics::record_download(downloaded_size);
                            let speed = downloaded_size as f64 / elapsed.as_secs_f64();
                            info!(
                                size = %size(downloaded_size),
                                time = %fields::duration(elapsed),
                                speed = %format!("{}/s", size(speed as u64)),
                                "Download completed"
                            );
                        } else {
                            info!("Content not modified (304)");
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Download failed after retries");
                    }
                }
                metrics::decrement_active_downloads();
                active.remove(&key_arc);
            }
            .instrument(span),
        );
    }

    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}

/// Download с retry логикой используя backoff crate
async fn download_with_retry(
    client: reqwest::Client,
    url: String,
    key: &str,
    storage: Arc<Storage>,
    settings: Arc<CacheSettings>,
    state: Arc<DownloadState>,
    existing_metadata: Option<CacheMetadata>,
) -> anyhow::Result<(u64, u64)> {
    let mut backoff = DownloadManager::create_backoff();
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        
        match download_file(
            client.clone(),
            url.clone(),
            key,
            storage.clone(),
            settings.clone(),
            state.clone(),
            existing_metadata.clone(),
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(e) => {
                // Проверяем, стоит ли retry
                let is_retryable = matches!(
                    &e.downcast_ref::<reqwest::Error>(),
                    Some(err) if err.is_connect() || err.is_timeout()
                );

                if !is_retryable || attempts >= MAX_RETRIES {
                    return Err(e);
                }

                if let Some(duration) = backoff.next_backoff() {
                    warn!(
                        attempt = attempts,
                        delay = ?duration,
                        error = %e,
                        "Retrying download"
                    );
                    tokio::time::sleep(duration).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

/// Создаёт stream для чтения из файла во время записи
fn create_streaming_reader(
    mut file: File,
    state: Arc<DownloadState>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
    stream! {
        let mut buffer = vec![0u8; READ_BUFFER_SIZE];
        let mut status_rx = state.status_rx.clone();

        loop {
            match file.read(&mut buffer).await {
                Ok(0) => {
                    let status = state.status();
                    if status.is_finished() {
                        state.remove_waiter();
                        if !status.is_success() {
                            yield Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "Download failed",
                            ));
                        }
                        break;
                    }
                    
                    tokio::select! {
                        _ = status_rx.changed() => {}
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                    }
                }
                Ok(n) => {
                    yield Ok(Bytes::copy_from_slice(&buffer[..n]));
                }
                Err(e) => {
                    state.remove_waiter();
                    yield Err(e);
                    break;
                }
            }
        }
    }
}

fn header_to_string(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

async fn download_file(
    client: reqwest::Client,
    url: String,
    key: &str,
    storage: Arc<Storage>,
    settings: Arc<CacheSettings>,
    state: Arc<DownloadState>,
    existing_metadata: Option<CacheMetadata>,
) -> anyhow::Result<(u64, u64)> {
    debug!(url = %url, "Fetching from upstream");

    let mut request = client.get(&url);

    if let Some(ref metadata) = existing_metadata {
        if settings.validation.use_etag {
            if let Some(ref etag) = metadata.etag {
                debug!(etag = %etag, "Adding If-None-Match header");
                request = request.header("If-None-Match", etag);
            }
        }

        if settings.validation.use_last_modified {
            if let Some(ref last_modified) = metadata.last_modified {
                debug!(last_modified = %last_modified, "Adding If-Modified-Since header");
                request = request.header("If-Modified-Since", last_modified);
            }
        }
    }

    let response = request.send().await?;
    let status = response.status().as_u16();

    state.set_status_code(status);

    if status == 304 {
        info!("Upstream returned 304 Not Modified");
        if existing_metadata.is_some() {
            let meta_size = storage.metadata_size(key).await.unwrap_or(0);
            let file_size = storage
                .open(key)
                .await
                .ok()
                .flatten()
                .map(|s| s.size)
                .unwrap_or(0);

            state.mark_not_modified(file_size, meta_size);
            return Ok((0, 0));
        }
    }

    if !response.status().is_success() {
        warn!(status = status, url = %url, "Upstream returned error");
        state.mark_finished(false, 0, 0);
        return Ok((0, 0));
    }

    let content_length = response.content_length().unwrap_or(0);
    let waiters = state.waiter_count();

    info!(
        size = %if content_length > 0 { size(content_length) } else { "unknown".to_string() },
        clients = waiters,
        "Downloading"
    );

    let response_headers = response.headers().clone();

    let etag = header_to_string(&response_headers, reqwest::header::ETAG);
    let last_modified = header_to_string(&response_headers, reqwest::header::LAST_MODIFIED);

    let metadata = CacheMetadata {
        headers: response_headers,
        original_url: url.clone(),
        key: Some(key.to_string()),
        stored_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        content_length,
        etag,
        last_modified,
    };

    state.set_metadata(metadata.clone()).await;

    let mut writer = storage.create(key).await?;
    state.mark_file_ready();

    let mut stream = response.bytes_stream();
    let mut last_log_bytes = 0u64;
    let mut download_error: Option<anyhow::Error> = None;

    while let Some(chunk_result) = stream.next().await {
        match chunk_result {
            Ok(chunk) => {
                if let Err(e) = writer.write(&chunk).await {
                    download_error = Some(e.into());
                    break;
                }
                let bytes = writer.bytes_written();
                state.notify_progress(bytes);

                if content_length > PROGRESS_LOG_INTERVAL
                    && bytes - last_log_bytes >= PROGRESS_LOG_INTERVAL
                {
                    let percent = if content_length > 0 {
                        (bytes as f64 / content_length as f64 * 100.0) as u32
                    } else {
                        0
                    };
                    debug!(
                        downloaded = %size(bytes),
                        total = %size(content_length),
                        percent = percent,
                        "Download progress"
                    );
                    last_log_bytes = bytes;
                }
            }
            Err(e) => {
                download_error = Some(e.into());
                break;
            }
        }
    }

    if let Some(error) = download_error {
        warn!(
            key = %fields::path(key),
            bytes_written = writer.bytes_written(),
            error = %error,
            "Download failed mid-stream, aborting"
        );

        writer.abort().await?;
        state.mark_finished(false, 0, 0);

        return Err(error);
    }

    let total = writer.bytes_written();

    if content_length > 0 && total != content_length {
        warn!(
            key = %fields::path(key),
            expected = content_length,
            actual = total,
            "Content length mismatch"
        );
    }

    state.update_content_length(total).await;

    let final_metadata = state
        .get_metadata()
        .await
        .map(|m| (*m).clone())
        .unwrap_or_else(|| {
            let mut m = metadata;
            m.content_length = total;
            m
        });

    let (file_size, meta_size) = writer.finalize(&storage, final_metadata).await?;
    state.mark_finished(true, file_size, meta_size);
    Ok((file_size, meta_size))
}