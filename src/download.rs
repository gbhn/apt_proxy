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
const READ_BUFFER_SIZE: usize = 131_072;
const DOWNLOAD_BUFFER_SIZE: usize = 262_144;

pub type ActiveDownloads = Arc<DashMap<String, Arc<DownloadState>>>;

pub struct Downloader {
    client: reqwest::Client,
    upstream_url: String,
    upstream_path: String,
    active: ActiveDownloads, 
}

impl Downloader {
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
        let (state, is_new) = self.get_or_create_download(path).await;

        if is_new {
            self.spawn_download_task(path.to_string(), cache, state.clone());
        }

        let status_code = timeout(HEADER_WAIT_TIMEOUT, state.wait_for_status())
            .await
            .map_err(|_| crate::error::ProxyError::Download("Timeout".to_string()))?;

        if status_code != 200 {
            return Err(crate::error::ProxyError::UpstreamError(
                axum::http::StatusCode::from_u16(status_code)
                    .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            ));
        }

        let cache_path = cache.cache_path(path);
        let part_path = crate::utils::part_path_for(&cache_path);
        
        let file = Self::open_downloading_file(&part_path, &cache_path).await?;
        let stream = StreamingReader::new(file, state.clone());
        
        let body = Body::from_stream(stream);
        let mut response = Response::new(body);
        
        if let Some(meta) = state.metadata.read().await.as_ref() {
            response.headers_mut().extend(meta.headers.clone());
        }

        Ok(response)
    }

    async fn get_or_create_download(&self, path: &str) -> (Arc<DownloadState>, bool) {
        match self.active.entry(path.to_string()) {
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

        tokio::spawn(async move {
            let result = download_file(client, url, cache_path, state.clone()).await;
            
            match result {
                Ok(_) => info!("Download completed: {}", path),
                Err(e) => error!("Download failed: {} - {}", path, e),
            }

            active.remove(&path);
        });
    }

    async fn open_downloading_file(
        part_path: &std::path::Path,
        _final_path: &std::path::Path,
    ) -> crate::error::Result<File> {
        let mut delay = Duration::from_millis(50);
        
        for attempt in 0..20 {
            if let Ok(Ok(file)) = timeout(FILE_OPEN_TIMEOUT, File::open(part_path)).await {
                return Ok(file);
            }
            
            if attempt < 19 {
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, Duration::from_secs(1));
            }
        }

        Err(crate::error::ProxyError::Download(
            "Failed to open downloading file".to_string()
        ))
    }
}

pub struct DownloadState {
    pub notify_status: Notify,
    pub notify_data: Notify,
    pub status_code: AtomicU16,
    pub is_finished: AtomicBool,
    pub is_success: AtomicBool,
    pub bytes_written: AtomicU64,
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
            let notified = self.notify_status.notified();
            let code = self.status_code.load(Ordering::Acquire);
            
            if code != 0 {
                return code;
            }
            if self.is_finished.load(Ordering::Acquire) {
                return 502;
            }
            
            notified.await;
        }
    }

    #[inline]
    pub fn mark_finished(&self, success: bool) {
        self.is_finished.store(true, Ordering::Release);
        self.is_success.store(success, Ordering::Release);
        self.notify_data.notify_waiters();
        self.notify_status.notify_waiters();
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

    let response = client.get(&url).send().await?;
    let status = response.status().as_u16();
    
    let metadata = crate::cache::CacheMetadata::from_response(&response, &url);
    *state.metadata.write().await = Some(metadata.clone());
    
    state.status_code.store(status, Ordering::Release);
    state.notify_status.notify_waiters();

    if !response.status().is_success() {
        warn!("Upstream returned {}", status);
        state.mark_finished(false);
        return Ok(());
    }

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
            
            state.bytes_written.store(total, Ordering::Release);
            state.notify_data.notify_waiters();
            
            buffer.clear();
        }
    }

    // Записываем остатки буфера
    if !buffer.is_empty() {
        file.write_all(&buffer).await?;
        total += buffer.len() as u64;
        state.bytes_written.store(total, Ordering::Release);
        state.notify_data.notify_waiters();
    }

    file.flush().await?;
    drop(file);

    fs::rename(&part_path, &cache_path).await?;
    metadata.save(&cache_path).await?;

    state.mark_finished(true);
    Ok(())
}

pub struct StreamingReader {
    file: File,
    state: Arc<DownloadState>,
    buffer: Box<[u8]>,
    last_read_pos: u64,
}

impl StreamingReader {
    pub fn new(file: File, state: Arc<DownloadState>) -> Self {
        Self {
            file,
            state,
            buffer: vec![0u8; READ_BUFFER_SIZE].into_boxed_slice(),
            last_read_pos: 0,
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
                
                if n == 0 {
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
                    if this.last_read_pos >= bytes_written {
                        let waker = cx.waker().clone();
                        let state = this.state.clone();
                        tokio::spawn(async move {
                            state.notify_data.notified().await;
                            waker.wake();
                        });
                        return Poll::Pending;
                    } else {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }

                this.last_read_pos += n as u64;
                Poll::Ready(Some(Ok(Bytes::copy_from_slice(read_buf.filled()))))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}