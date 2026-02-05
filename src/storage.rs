//! Storage модуль с использованием cacache для content-addressable caching

use crate::logging::fields::size;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::{
    fs::{self, File},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tracing::{debug, info, warn};

const MAX_CONCURRENT_WRITES: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetadata {
    #[serde(with = "http_serde::header_map")]
    pub headers: axum::http::HeaderMap,
    pub original_url: String,
    #[serde(default)]
    pub key: Option<String>,
    pub stored_at: u64,
    pub content_length: u64,
    #[serde(default)]
    pub etag: Option<String>,
    #[serde(default)]
    pub last_modified: Option<String>,
}

impl CacheMetadata {
    pub fn new(response: &reqwest::Response, url: &str, size: u64) -> Self {
        let headers = response.headers();
        Self {
            headers: headers.clone(),
            original_url: url.to_string(),
            key: None,
            stored_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            content_length: size,
            etag: headers
                .get(http::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            last_modified: headers
                .get(http::header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(String::from),
        }
    }
}

pub struct Storage {
    cache_path: PathBuf,
    temp_dir: PathBuf,
    write_semaphore: Arc<Semaphore>,
}

impl Storage {
    pub async fn new(base_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&base_dir).await?;
        
        let temp_dir = base_dir.join("tmp");
        fs::create_dir_all(&temp_dir).await?;
        
        debug!(path = %base_dir.display(), "Storage directory initialized");
        Ok(Self {
            cache_path: base_dir,
            temp_dir,
            write_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_WRITES)),
        })
    }

    #[inline]
    pub fn path_for(&self, key: &str) -> PathBuf {
        let hash = blake3::hash(key.as_bytes());
        let hex = hash.to_hex();
        let hex_str = hex.as_str();
        self.cache_path
            .join(&hex_str[0..2])
            .join(&hex_str[2..4])
            .join(hex_str)
    }

    /// Возвращает путь к директории для временных файлов
    pub fn temp_dir(&self) -> &Path {
        &self.temp_dir
    }

    pub async fn exists(&self, key: &str) -> bool {
        cacache::exists(&self.cache_path, key).await
    }

    pub async fn metadata_size(&self, key: &str) -> Result<u64> {
        match cacache::metadata(&self.cache_path, key).await {
            Ok(Some(meta)) => {
                Ok(meta.raw_metadata.as_ref().map_or(0, |m| m.len() as u64) + 256)
            }
            _ => Ok(0),
        }
    }

    pub async fn get_metadata(&self, key: &str) -> Result<Option<CacheMetadata>> {
        match cacache::metadata(&self.cache_path, key).await? {
            Some(entry) => {
                if let Some(raw) = entry.raw_metadata {
                    let meta: CacheMetadata = serde_json::from_slice(&raw)?;
                    Ok(Some(meta))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    pub async fn open(&self, key: &str) -> Result<Option<StoredFile>> {
        match cacache::metadata(&self.cache_path, key).await? {
            Some(entry) => {
                let metadata = if let Some(raw) = &entry.raw_metadata {
                    serde_json::from_slice(raw)?
                } else {
                    return Ok(None);
                };

                let file = cacache::Reader::open(&self.cache_path, entry.integrity).await?;
                let size = entry.size;

                Ok(Some(StoredFile {
                    file,
                    metadata,
                    size,
                }))
            }
            None => Ok(None),
        }
    }

    pub async fn create(&self, key: &str) -> Result<StorageWriter> {
        let permit = self.write_semaphore.clone().acquire_owned().await?;
        
        // Используем tempfile crate для безопасного создания временных файлов
        let temp_file = tempfile::Builder::new()
            .prefix("apt-cache-")
            .suffix(".tmp")
            .tempfile_in(&self.temp_dir)?;
        
        let temp_path = temp_file.path().to_path_buf();
        let std_file = temp_file.into_file();
        let file = File::from_std(std_file);

        debug!(key = %key, "Created temp file for streaming write");

        Ok(StorageWriter {
            cache_path: self.cache_path.clone(),
            key: key.to_string(),
            temp_path,
            file: Some(file),
            bytes_written: 0,
            _permit: permit,
            finalized: false,
        })
    }

    pub async fn delete(&self, key: &str) -> Result<u64> {
        let meta = cacache::metadata(&self.cache_path, key).await?;
        let deleted_size = meta.as_ref().map_or(0, |m| m.size);

        cacache::remove(&self.cache_path, key).await?;

        debug!(
            key = %crate::logging::fields::path(key),
            freed = %size(deleted_size),
            "Deleted cache entry"
        );

        Ok(deleted_size)
    }

    pub async fn cleanup_temp_files(&self) -> Result<()> {
        if self.temp_dir.exists() {
            let mut count = 0u32;
            let mut total_size = 0u64;

            let mut entries = fs::read_dir(&self.temp_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                if let Ok(meta) = entry.metadata().await {
                    total_size += meta.len();
                }
                let _ = fs::remove_file(entry.path()).await;
                count += 1;
            }

            if count > 0 {
                info!(
                    files = count,
                    freed = %size(total_size),
                    "Cleaned up stale temp files"
                );
            }
        }

        // GC cacache для удаления orphaned content
        let gc_result = cacache::gc(&self.cache_path).await?;
        if gc_result.entries_removed > 0 || gc_result.content_removed > 0 {
            info!(
                entries = gc_result.entries_removed,
                content = gc_result.content_removed,
                "Ran cacache garbage collection"
            );
        }

        Ok(())
    }

    pub async fn list_all(&self) -> Result<Vec<(String, u64, u64, CacheMetadata)>> {
        let mut files = Vec::with_capacity(1024);
        let cache_path = self.cache_path.clone();

        let entries = cacache::list_sync(&cache_path);

        for entry in entries {
            let entry = entry?;
            let key = entry.key.clone();

            let metadata = if let Some(raw) = &entry.raw_metadata {
                match serde_json::from_slice(raw) {
                    Ok(m) => m,
                    Err(_) => continue,
                }
            } else {
                continue;
            };

            let file_size = entry.size;
            let meta_size = entry.raw_metadata.as_ref().map_or(0, |m| m.len() as u64);

            files.push((key, file_size, meta_size, metadata));
        }

        Ok(files)
    }

    pub async fn stats(&self) -> StorageStats {
        let available_permits = self.write_semaphore.available_permits();
        StorageStats {
            active_writes: MAX_CONCURRENT_WRITES - available_permits,
            max_concurrent_writes: MAX_CONCURRENT_WRITES,
        }
    }
}

pub struct StorageStats {
    pub active_writes: usize,
    pub max_concurrent_writes: usize,
}

pub struct StoredFile {
    pub file: cacache::Reader,
    pub metadata: CacheMetadata,
    pub size: u64,
}

pub struct StorageWriter {
    cache_path: PathBuf,
    key: String,
    temp_path: PathBuf,
    file: Option<File>,
    bytes_written: u64,
    _permit: OwnedSemaphorePermit,
    finalized: bool,
}

impl StorageWriter {
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        if let Some(ref mut file) = self.file {
            file.write_all(data).await?;
            self.bytes_written += data.len() as u64;
        }
        Ok(())
    }

    #[inline]
    pub const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Возвращает путь к временному файлу для чтения во время записи
    pub fn temp_path(&self) -> &Path {
        &self.temp_path
    }

    /// Открывает временный файл для чтения (для streaming во время записи)
    pub async fn open_for_reading(&self) -> Result<File> {
        let file = File::open(&self.temp_path).await?;
        Ok(file)
    }

    pub async fn finalize(
        mut self,
        _storage: &Storage,
        metadata: CacheMetadata,
    ) -> Result<(u64, u64)> {
        self.finalized = true;

        // Закрываем и синхронизируем временный файл
        if let Some(mut file) = self.file.take() {
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
        }

        // Читаем содержимое и записываем в cacache
        let content = fs::read(&self.temp_path).await?;
        let meta_json = serde_json::to_vec(&metadata)?;
        let meta_size = meta_json.len() as u64;

        let integrity = cacache::write_with_opts(
            &self.cache_path,
            &self.key,
            &content,
            cacache::WriteOpts::new().raw_metadata(meta_json),
        )
        .await?;

        // Удаляем временный файл
        let _ = fs::remove_file(&self.temp_path).await;

        debug!(
            key = %self.key,
            size = %size(self.bytes_written),
            integrity = %integrity,
            "Saved to cache"
        );

        Ok((self.bytes_written, meta_size))
    }

    pub async fn abort(mut self) -> Result<()> {
        self.finalized = true;

        if let Some(file) = self.file.take() {
            drop(file);
        }

        if let Err(e) = fs::remove_file(&self.temp_path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    path = %self.temp_path.display(),
                    error = %e,
                    "Failed to remove temp file during abort"
                );
            }
        }

        debug!(
            key = %self.key,
            bytes = self.bytes_written,
            "Aborted write operation"
        );

        Ok(())
    }
}

impl Drop for StorageWriter {
    fn drop(&mut self) {
        if !self.finalized {
            let path = self.temp_path.clone();
            tokio::spawn(async move {
                let _ = fs::remove_file(&path).await;
            });
        }
    }
}