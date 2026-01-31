use axum::{body::Body, response::Response};
use bytes::Bytes;
use dashmap::DashMap;
use futures::Stream;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncRead, AsyncWriteExt, ReadBuf},
    sync::{Notify, RwLock},
    time::timeout,
};
use tracing::{error, info, warn};

const FILE_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const HEADER_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const READ_BUFFER_SIZE: usize = 256 * 1024;
const DOWNLOAD_BUFFER_SIZE: usize = 512 * 1024;

pub type ActiveDownloads = Arc<DashMap<Box<str>, Arc<DownloadState>>>;

pub struct Downloader {
    client: reqwest::Client,
    upstream_url: String,
    upstream_path: String,
    active: ActiveDownloads,
}

impl Downloader {
    #[inline]
    pub fn new(
        client: reqwest::Client,
        upstream_url: String,
        upstream_path: String,
        active: ActiveDownloads,
    ) -> Self {
        Self {
            client,
            upstream_url,
            upstream_path,
            active,
        }
    }

    pub async fn fetch_and_stream(
        self,
        path: &str,
        cache: &crate::cache::CacheManager,
    ) -> crate::error::Result<Response> {
        let (state, is_new) = self.get_or_create_download(path);

        if is_new {
            self.spawn_download_task(path.to_string(), cache, state.clone());
        }

        let status_code = timeout(HEADER_WAIT_TIMEOUT, state.wait_for_status())
            .await
            .map_err(|_| crate::error::ProxyError::Download("Header timeout".into()))?;

        if status_code != 200 {
            return Err(crate::error::ProxyError::UpstreamError(
                axum::http::StatusCode::from_u16(status_code)
                    .unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            ));
        }

        let cache_path = cache.cache_path(path);
        let part_path = crate::utils::part_path_for(&cache_path);

        let file = Self::open_downloading_file(&part_path).await?;
        let stream = StreamingReader::new(file, state.clone());

        let body = Body::from_stream(stream);
        let mut response = Response::new(body);

        if let Some(meta) = state.metadata.read().await.as_ref() {
            response.headers_mut().extend(meta.headers.clone());
        }

        Ok(response)
    }

    fn get_or_create_download(&self, path: &str) -> (Arc<DownloadState>, bool) {
        match self.active.entry(path.to_string().into_boxed_str()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                info!("Joining existing download: {}", path);
                (entry.get().clone(), false)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                info!("Starting new download: {}", path);
                let state = Arc::new(DownloadState::new());
                entry.insert(state.clone());
                (state, true)
            }
        }
    }

    fn spawn_download_task(
        &self,
        path: String,
        cache: &crate::cache::CacheManager,
        state: Arc<DownloadState>,
    ) {
        let client = self.client.clone();
        let url = format!(
            "{}/{}",
            self.upstream_url.trim_end_matches('/'),
            self.upstream_path.trim_start_matches('/')
        );
        let cache_path = cache.cache_path(&path);
        let active = self.active.clone();
        let path_key: Box<str> = path.clone().into_boxed_str();

        tokio::spawn(async move {
            let result = download_file(client, url, cache_path, state.clone()).await;

            match &result {
                Ok(_) => info!("Download completed: {}", path),
                Err(e) => error!("Download failed: {} - {}", path, e),
            }

            active.remove(&path_key);
        });
    }

    async fn open_downloading_file(part_path: &std::path::Path) -> crate::error::Result<File> {
        const MAX_ATTEMPTS: u32 = 20;
        let mut delay = Duration::from_millis(50);

        for attempt in 0..MAX_ATTEMPTS {
            match timeout(FILE_OPEN_TIMEOUT, File::open(part_path)).await {
                Ok(Ok(file)) => return Ok(file),
                _ if attempt < MAX_ATTEMPTS - 1 => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(1));
                }
                _ => break,
            }
        }

        Err(crate::error::ProxyError::Download(
            "Failed to open downloading file".into(),
        ))
    }
}

pub struct DownloadState {
    notify_status: Notify,
    notify_data: Notify,
    status_code: AtomicU16,
    is_finished: AtomicBool,
    is_success: AtomicBool,
    bytes_written: AtomicU64,
    pub metadata: RwLock<Option<crate::cache::CacheMetadata>>,
}

impl DownloadState {
    pub fn new() -> Self {
        Self {
            notify_status: Notify::new(),
            notify_data: Notify::new(),
            status_code: AtomicU16::new(0),
            is_finished: AtomicBool::new(false),
            is_success: AtomicBool::new(false),
            bytes_written: AtomicU64::new(0),
            metadata: RwLock::new(None),
        }
    }

    pub async fn wait_for_status(&self) -> u16 {
        loop {
            let code = self.status_code.load(Ordering::Acquire);
            if code != 0 {
                return code;
            }
            if self.is_finished.load(Ordering::Acquire) {
                return 502;
            }
            self.notify_status.notified().await;
        }
    }

    #[inline]
    pub fn mark_finished(&self, success: bool) {
        self.is_finished.store(true, Ordering::Release);
        self.is_success.store(success, Ordering::Release);
        self.notify_data.notify_waiters();
        self.notify_status.notify_waiters();
    }

    #[inline]
    fn notify_progress(&self, bytes: u64) {
        self.bytes_written.store(bytes, Ordering::Release);
        self.notify_data.notify_waiters();
    }
}

impl Default for DownloadState {
    fn default() -> Self {
        Self::new()
    }
}

async fn download_file(
    client: reqwest::Client,
    url: String,
    cache_path: std::path::PathBuf,
    state: Arc<DownloadState>,
) -> anyhow::Result<()> {
    let part_path = crate::utils::part_path_for(&cache_path);

    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    info!("Downloading from: {}", url);
    let response = client.get(&url).send().await?;
    let status = response.status().as_u16();
    info!("Upstream response status: {}", status);

    let metadata = crate::cache::CacheMetadata::from_response(&response, &url);
    *state.metadata.write().await = Some(metadata.clone());

    state.status_code.store(status, Ordering::Release);
    state.notify_status.notify_waiters();

    if !response.status().is_success() {
        warn!("Upstream returned non-success status: {}", status);
        state.mark_finished(false);
        return Ok(());
    }

    let content_length = response.content_length();
    if let Some(len) = content_length {
        info!("Content length: {} bytes ({})", len, crate::utils::format_size(len));
    }

    info!("Writing to temp file: {:?}", part_path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&part_path)
        .await?;

    let mut stream = response.bytes_stream();
    let mut total = 0u64;
    let mut buffer = Vec::with_capacity(DOWNLOAD_BUFFER_SIZE);

    use futures::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buffer.extend_from_slice(&chunk);

        if buffer.len() >= DOWNLOAD_BUFFER_SIZE {
            file.write_all(&buffer).await?;
            total += buffer.len() as u64;
            state.notify_progress(total);
            buffer.clear();
            
            if total % (5 * 1024 * 1024) == 0 {  // Лог каждые 5MB
                info!("Downloaded {} / {:?}", crate::utils::format_size(total), 
                      content_length.map(crate::utils::format_size));
            }
        }
    }

    if !buffer.is_empty() {
        file.write_all(&buffer).await?;
        total += buffer.len() as u64;
        state.notify_progress(total);
    }

    file.flush().await?;
    drop(file);

    info!("Download complete: {} total, renaming to final path", crate::utils::format_size(total));
    fs::rename(&part_path, &cache_path).await?;
    metadata.save(&cache_path).await?;

    state.mark_finished(true);
    info!("File saved successfully to cache");
    Ok(())
}

pub struct StreamingReader {
    file: File,
    state: Arc<DownloadState>,
    buffer: Box<[u8]>,
    position: u64,
}

impl StreamingReader {
    pub fn new(file: File, state: Arc<DownloadState>) -> Self {
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

                // n == 0
                let is_finished = this.state.is_finished.load(Ordering::Acquire);

                if is_finished {
                    return if this.state.is_success.load(Ordering::Acquire) {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "Download failed",
                        ))))
                    };
                }

                let bytes_written = this.state.bytes_written.load(Ordering::Acquire);
                if this.position < bytes_written {
                    // Есть данные - пробуем снова
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                // Ждём новых данных — spawn для ожидания Notify
                // Это редкий путь, поэтому spawn overhead приемлем
                let waker = cx.waker().clone();
                let state = this.state.clone();
                tokio::spawn(async move {
                    state.notify_data.notified().await;
                    waker.wake();
                });
                
                Poll::Pending
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}