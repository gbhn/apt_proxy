use axum::{body::Body, http::HeaderMap, response::Response};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{fs, sync::RwLock};
use tracing::{info, warn};

const READ_BUFFER_SIZE: usize = 256 * 1024;

#[derive(Clone)]
pub struct CacheEntry {
    pub data_size: u64,
    pub headers_size: u64,
    pub metadata: Option<Arc<CacheMetadata>>,
}

impl CacheEntry {
    #[inline(always)]
    pub fn total_size(&self) -> u64 {
        self.data_size + self.headers_size
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetadata {
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,
    pub original_url_path: String,
}

impl CacheMetadata {
    #[inline]
    pub fn from_response(response: &reqwest::Response, url_path: &str) -> Self {
        Self {
            headers: response.headers().clone(),
            original_url_path: url_path.to_string(),
        }
    }

    pub async fn save(&self, cache_path: &Path) -> std::io::Result<u64> {
        let headers_path = crate::utils::headers_path_for(cache_path);
        let json = serde_json::to_vec(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let size = json.len() as u64;
        fs::write(headers_path, json).await?;
        Ok(size)
    }

    pub async fn load(cache_path: &Path) -> Option<Self> {
        let headers_path = crate::utils::headers_path_for(cache_path);
        let bytes = fs::read(&headers_path).await.ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}

pub struct CacheManager {
    base_dir: PathBuf,
    lru: Arc<RwLock<lru::LruCache<Box<str>, CacheEntry>>>,
    total_size: Arc<AtomicU64>,
    max_size: u64,
}

impl CacheManager {
    pub async fn new(settings: crate::config::Settings) -> anyhow::Result<Self> {
        fs::create_dir_all(&settings.cache_dir).await?;

        let manager = Self {
            base_dir: settings.cache_dir.clone(),
            lru: Arc::new(RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(settings.max_lru_entries).unwrap(),
            ))),
            total_size: Arc::new(AtomicU64::new(0)),
            max_size: settings.max_cache_size,
        };

        manager.initialize().await?;
        Ok(manager)
    }

    async fn initialize(&self) -> anyhow::Result<()> {
        info!("Initializing cache...");

        self.cleanup_stale_downloads().await?;

        let files = self.scan_directory_iterative().await?;

        let mut lru = self.lru.write().await;
        let mut total = 0u64;

        for (url_path, entry) in files {
            total += entry.total_size();
            lru.put(url_path.into_boxed_str(), entry);
        }

        self.total_size.store(total, Ordering::Release);
        info!(
            "Cache initialized: {} files, {}",
            lru.len(),
            crate::utils::format_size(total)
        );

        Ok(())
    }

    async fn cleanup_stale_downloads(&self) -> anyhow::Result<()> {
        let mut dirs_to_scan = vec![self.base_dir.clone()];
        while let Some(dir) = dirs_to_scan.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.is_dir() {
                    dirs_to_scan.push(path);
                } else if let Some(ext) = path.extension() {
                    if ext == "part" {
                        warn!("Removing stale download file: {:?}", path);
                        let _ = fs::remove_file(path).await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn scan_directory_iterative(&self) -> anyhow::Result<Vec<(String, CacheEntry)>> {
        let mut files: Vec<(String, CacheEntry, std::time::SystemTime)> = Vec::with_capacity(1024);
        let mut dirs_to_scan = vec![self.base_dir.clone()];

        while let Some(dir) = dirs_to_scan.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let file_metadata = match entry.metadata().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                if file_metadata.is_dir() {
                    dirs_to_scan.push(path);
                    continue;
                }

                if !file_metadata.is_file() {
                    continue;
                }

                if let Some(ext) = path.extension() {
                    if ext == "headers" || ext == "part" {
                        continue;
                    }
                }

                // Load metadata and cache it in the entry
                if let Some(cache_meta) = CacheMetadata::load(&path).await {
                    let headers_path = crate::utils::headers_path_for(&path);
                    let headers_size = fs::metadata(&headers_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);

                    let url_path = cache_meta.original_url_path.clone();
                    
                    files.push((
                        url_path,
                        CacheEntry {
                            data_size: file_metadata.len(),
                            headers_size,
                            metadata: Some(Arc::new(cache_meta)),
                        },
                        file_metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                    ));
                }
            }
        }

        // Sort by modification time (oldest first for LRU ordering)
        files.sort_unstable_by_key(|(_, _, mtime)| *mtime);
        
        // Strip mtime from result
        Ok(files.into_iter().map(|(path, entry, _)| (path, entry)).collect())
    }

    pub async fn serve_cached(&self, path: &str) -> crate::error::Result<Option<Response>> {
        let cache_path = self.cache_path(path);

        let file_metadata = match fs::metadata(&cache_path).await {
            Ok(m) if m.is_file() => m,
            _ => return Ok(None),
        };

        // Get or create entry with cached metadata
        let cached_meta = {
            let mut lru = self.lru.write().await;

            if let Some(entry) = lru.get(path) {
                entry.metadata.clone()
            } else {
                // Entry not in LRU - load metadata and add entry
                let meta = CacheMetadata::load(&cache_path).await.map(Arc::new);
                let headers_path = crate::utils::headers_path_for(&cache_path);
                let headers_size = fs::metadata(&headers_path)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);

                let entry = CacheEntry {
                    data_size: file_metadata.len(),
                    headers_size,
                    metadata: meta.clone(),
                };
                self.total_size.fetch_add(entry.total_size(), Ordering::AcqRel);
                lru.put(path.to_string().into_boxed_str(), entry);

                meta
            }
        };

        info!("Cache HIT: {}", path);

        let file = fs::File::open(&cache_path).await?;
        let stream = tokio_util::io::ReaderStream::with_capacity(file, READ_BUFFER_SIZE);
        let body = Body::from_stream(stream);

        let mut response = Response::new(body);
        if let Some(meta) = cached_meta {
            response.headers_mut().extend(meta.headers.clone());
        }

        Ok(Some(response))
    }

    pub async fn store(
        &self,
        path: &str,
        data: &[u8],
        metadata: &CacheMetadata,
    ) -> crate::error::Result<()> {
        let cache_path = self.cache_path(path);

        if let Some(parent) = cache_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let (write_result, headers_result) = tokio::join!(
            fs::write(&cache_path, data),
            metadata.save(&cache_path)
        );

        write_result?;
        let headers_size = headers_result?;

        self.update_stats(path, data.len() as u64, headers_size, Some(Arc::new(metadata.clone()))).await;

        let total = self.total_size.load(Ordering::Acquire);
        if total > self.max_size {
            self.spawn_cleanup();
        }

        Ok(())
    }

    #[inline]
    pub fn cache_path(&self, uri_path: &str) -> PathBuf {
        crate::utils::cache_path_for(&self.base_dir, uri_path)
    }

    async fn update_stats(
        &self,
        path: &str,
        data_size: u64,
        headers_size: u64,
        metadata: Option<Arc<CacheMetadata>>,
    ) {
        let entry = CacheEntry {
            data_size,
            headers_size,
            metadata,
        };
        let mut lru = self.lru.write().await;

        if let Some(old) = lru.put(path.to_string().into_boxed_str(), entry.clone()) {
            let old_size = old.total_size();
            let new_size = entry.total_size();
            if new_size > old_size {
                self.total_size.fetch_add(new_size - old_size, Ordering::AcqRel);
            } else {
                self.total_size.fetch_sub(old_size - new_size, Ordering::AcqRel);
            }
        } else {
            self.total_size.fetch_add(entry.total_size(), Ordering::AcqRel);
        }
    }

    fn spawn_cleanup(&self) {
        let base_dir = self.base_dir.clone();
        let lru = self.lru.clone();
        let total_size = self.total_size.clone();
        let max_size = self.max_size;

        tokio::spawn(async move {
            Self::cleanup_task(base_dir, lru, total_size, max_size).await;
        });
    }

    async fn cleanup_task(
        base_dir: PathBuf,
        lru: Arc<RwLock<lru::LruCache<Box<str>, CacheEntry>>>,
        total_size: Arc<AtomicU64>,
        max_size: u64,
    ) {
        let current = total_size.load(Ordering::Acquire);
        if current <= max_size {
            return;
        }

        info!("Cache cleanup started");
        let target = (max_size as f64 * 0.8) as u64;

        let to_remove: Vec<_> = {
            let mut lru = lru.write().await;
            let mut current = total_size.load(Ordering::Acquire);
            let mut to_remove = Vec::new();

            while current > target {
                let Some((old_path, entry)) = lru.pop_lru() else {
                    break;
                };
                to_remove.push((old_path.to_string(), entry.data_size, entry.headers_size));
                current = current.saturating_sub(entry.total_size());
            }
            to_remove
        };

        for (old_path, data_size, headers_size) in to_remove {
            let cache_path = crate::utils::cache_path_for(&base_dir, &old_path);
            let headers_path = crate::utils::headers_path_for(&cache_path);

            let mut removed = 0u64;
            if fs::remove_file(&cache_path).await.is_ok() {
                removed += data_size;
            }
            if fs::remove_file(headers_path).await.is_ok() {
                removed += headers_size;
            }
            total_size.fetch_sub(removed, Ordering::AcqRel);
        }

        info!("Cache cleanup finished");
    }

    pub async fn get_stats(&self) -> String {
        let size = self.total_size.load(Ordering::Acquire);
        let entries = self.lru.read().await.len();

        format!(
            "Cache Size: {} / {}\nLRU Entries: {}",
            crate::utils::format_size(size),
            crate::utils::format_size(self.max_size),
            entries
        )
    }
}