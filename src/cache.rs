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

const READ_BUFFER_SIZE: usize = 256 * 1024;

#[derive(Clone, Copy)]
pub struct CacheEntry {
    pub data_size: u64,
    pub headers_size: u64,
}

impl CacheEntry {
    #[inline(always)]
    pub const fn total_size(&self) -> u64 {
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

        let files = self.scan_directory_iterative().await?;

        let mut lru = self.lru.write().await;
        let mut total = 0u64;

        for (url_path, entry, _) in files {
            lru.put(url_path.into_boxed_str(), entry);
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

    async fn scan_directory_iterative(
        &self,
    ) -> anyhow::Result<Vec<(String, CacheEntry, std::time::SystemTime)>> {
        let mut files = Vec::with_capacity(1024);
        let mut dirs_to_scan = vec![self.base_dir.clone()];

        while let Some(dir) = dirs_to_scan.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let metadata = match entry.metadata().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                if metadata.is_dir() {
                    dirs_to_scan.push(path);
                    continue;
                }

                if !metadata.is_file() {
                    continue;
                }

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
            }
        }

        files.sort_unstable_by_key(|(_, _, atime)| *atime);
        Ok(files)
    }

    /// Оптимизированная версия - один write lock вместо read+write
    pub async fn serve_cached(&self, path: &str) -> crate::error::Result<Option<Response>> {
        let cache_path = self.cache_path(path);

        // Проверяем файл ДО блокировки LRU
        let metadata = match fs::metadata(&cache_path).await {
            Ok(m) if m.is_file() => m,
            _ => return Ok(None),
        };

        // Один write lock - и проверка, и обновление
        // try_write для неблокирующей попытки
        if let Ok(mut lru) = self.lru.try_write() {
            if lru.get(path).is_none() {
                // Добавляем новую запись
                let headers_path = crate::utils::headers_path_for(&cache_path);
                let headers_size = fs::metadata(&headers_path)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);

                let entry = CacheEntry {
                    data_size: metadata.len(),
                    headers_size,
                };
                self.total_size.fetch_add(entry.total_size(), Ordering::AcqRel);
                lru.put(path.to_string().into_boxed_str(), entry);
            }
            // Если запись есть - get() уже обновил позицию в LRU
        }
        // Если не смогли взять lock - не страшно, LRU обновится при следующем запросе

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

        let (write_result, headers_result) = tokio::join!(
            fs::write(&cache_path, data),
            metadata.save(&cache_path)
        );

        write_result?;
        let headers_size = headers_result?;

        self.update_stats(path, data.len() as u64, headers_size).await;

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

    async fn update_stats(&self, path: &str, data_size: u64, headers_size: u64) {
        let entry = CacheEntry { data_size, headers_size };
        let mut lru = self.lru.write().await;

        if let Some(old) = lru.put(path.to_string().into_boxed_str(), entry) {
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
                to_remove.push((old_path.to_string(), entry));
                current = current.saturating_sub(entry.total_size());
            }
            to_remove
        };

        for (old_path, entry) in to_remove {
            let cache_path = crate::utils::cache_path_for(&base_dir, &old_path);
            let headers_path = crate::utils::headers_path_for(&cache_path);

            let mut removed = 0u64;
            if fs::remove_file(&cache_path).await.is_ok() {
                removed += entry.data_size;
            }
            if fs::remove_file(headers_path).await.is_ok() {
                removed += entry.headers_size;
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