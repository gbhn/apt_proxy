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
use tracing::info;

const READ_BUFFER_SIZE: usize = 131_072;

#[derive(Clone)]
pub struct CacheEntry {
    pub data_size: u64,
    pub headers_size: u64,
}

impl CacheEntry {
    #[inline]
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
    pub fn from_response(response: &reqwest::Response, url_path: &str) -> Self {
        Self {
            headers: response.headers().clone(),
            original_url_path: url_path.to_string(),
        }
    }

    pub async fn save(&self, cache_path: &Path) -> std::io::Result<u64> {
        let headers_path = crate::utils::headers_path_for(cache_path);
        let json = serde_json::to_vec(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let size = json.len() as u64;
        fs::write(headers_path, json).await?;
        Ok(size)
    }

    pub async fn load(cache_path: &Path) -> Option<Self> {
        let headers_path = crate::utils::headers_path_for(cache_path);
        let json = fs::read_to_string(headers_path).await.ok()?;
        serde_json::from_str(&json).ok()
    }
}

pub struct CacheManager {
    base_dir: PathBuf,
    lru: Arc<RwLock<lru::LruCache<String, CacheEntry>>>,
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
        
        let mut files = Vec::new();
        self.scan_directory(&self.base_dir, &mut files).await?;
        
        files.sort_unstable_by_key(|(_, _, atime)| *atime);

        let mut lru = self.lru.write().await;
        let mut total = 0u64;
        
        for (url_path, entry, _) in files {
            lru.put(url_path, entry.clone());
            total += entry.total_size();
        }
        
        self.total_size.store(total, Ordering::Release);
        info!(
            "Cache initialized: {} files, {}",
            lru.len(),
            crate::utils::format_size(total)
        );
        
        Ok(())
    }

    async fn scan_directory(
        &self,
        dir: &Path,
        files: &mut Vec<(String, CacheEntry, std::time::SystemTime)>,
    ) -> anyhow::Result<()> {
        let mut entries = fs::read_dir(dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;

            if metadata.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "headers" || ext == "part" {
                        continue;
                    }
                }

                if let Some(meta) = CacheMetadata::load(&path).await {
                    let headers_path = crate::utils::headers_path_for(&path);
                    let headers_size = fs::metadata(&headers_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);

                    files.push((
                        meta.original_url_path,
                        CacheEntry {
                            data_size: metadata.len(),
                            headers_size,
                        },
                        metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                    ));
                }
            } else if metadata.is_dir() {
                Box::pin(self.scan_directory(&path, files)).await?;
            }
        }

        Ok(())
    }

    pub async fn serve_cached(&self, path: &str) -> crate::error::Result<Option<Response>> {
        let cache_path = self.cache_path(path);
        
        let metadata = match fs::metadata(&cache_path).await {
            Ok(m) if m.is_file() => m,
            _ => return Ok(None),
        };

        let _needs_lru_update = {
            if let Ok(lru) = self.lru.try_write() {
                drop(lru);
                false
            } else {
                let mut lru = self.lru.write().await;
                let result = lru.get(path).is_none();
                if result {
                    let headers_path = crate::utils::headers_path_for(&cache_path);
                    let headers_size = fs::metadata(&headers_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);
                        
                    lru.put(
                        path.to_string(),
                        CacheEntry {
                            data_size: metadata.len(),
                            headers_size,
                        },
                    );
                }
                result
            }
        };

        info!("Cache HIT: {}", path);

        let file = fs::File::open(&cache_path).await?;
        let stream = tokio_util::io::ReaderStream::with_capacity(file, READ_BUFFER_SIZE);
        let body = Body::from_stream(stream);
        
        let mut response = Response::new(body);
        
        if let Some(meta) = CacheMetadata::load(&cache_path).await {
            response.headers_mut().extend(meta.headers);
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

        let (write_result, headers_size) = tokio::join!(
            fs::write(&cache_path, data),
            metadata.save(&cache_path)
        );
        
        write_result?;
        let headers_size = headers_size?;

        self.update_stats(path, data.len() as u64, headers_size).await;
        
        let total = self.total_size.load(Ordering::Acquire);
        if total > self.max_size {
            let self_clone = self.base_dir.clone();
            let lru_ref = self.lru.clone();
            let total_size_ref = self.total_size.clone();
            let max_size = self.max_size;
            
            tokio::spawn(Self::cleanup_task(
                self_clone,
                lru_ref,
                total_size_ref,
                max_size,
            ));
        }

        Ok(())
    }

    #[inline]
    pub fn cache_path(&self, uri_path: &str) -> PathBuf {
        crate::utils::cache_path_for(&self.base_dir, uri_path)
    }

    async fn update_stats(&self, path: &str, data_size: u64, headers_size: u64) {
        let mut lru = self.lru.write().await;
        let entry = CacheEntry { data_size, headers_size };
        
        if let Some(old) = lru.put(path.to_string(), entry.clone()) {
            self.total_size.fetch_sub(old.total_size(), Ordering::Release);
        }
        
        self.total_size.fetch_add(entry.total_size(), Ordering::Release);
    }

    async fn cleanup_task(
        base_dir: PathBuf,
        lru: Arc<RwLock<lru::LruCache<String, CacheEntry>>>,
        total_size: Arc<AtomicU64>,
        max_size: u64,
    ) {
        let mut current = total_size.load(Ordering::Acquire);
        if current <= max_size {
            return;
        }

        info!("Cache cleanup started (async)");
        let target = (max_size as f64 * 0.8) as u64;
        let mut lru = lru.write().await;

        while current > target {
            let Some((old_path, entry)) = lru.pop_lru() else {
                break;
            };

            let cache_path = crate::utils::cache_path_for(&base_dir, &old_path);
            let mut removed = 0u64;

            let headers_path = crate::utils::headers_path_for(&cache_path);
            let (res1, res2) = tokio::join!(
                fs::remove_file(&cache_path),
                fs::remove_file(headers_path)
            );

            if res1.is_ok() {
                removed += entry.data_size;
            }
            if res2.is_ok() {
                removed += entry.headers_size;
            }

            total_size.fetch_sub(removed, Ordering::Release);
            current -= removed;
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