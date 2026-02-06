use crate::metrics;
use anyhow::{Context, Result};
use cacache::WriteOpts;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tracing::{debug, info, warn};

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(with = "http_serde::header_map")]
    pub headers: axum::http::HeaderMap,
    pub url: String,
    pub key: String,
    pub stored_at: u64,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

impl Metadata {
    pub fn new(key: impl Into<String>, url: impl Into<String>, size: u64, headers: axum::http::HeaderMap) -> Self {
        Self { 
            headers, 
            url: url.into(), 
            key: key.into(), 
            stored_at: now_secs(), 
            size, 
            etag: None, 
            last_modified: None 
        }
    }

    pub fn with_conditionals(mut self, etag: Option<String>, last_modified: Option<String>) -> Self {
        self.etag = etag;
        self.last_modified = last_modified;
        self
    }
}

#[derive(Clone)]
pub struct Storage {
    cache_path: PathBuf,
}

impl Storage {
    pub async fn new(base: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&base).await
            .context("Failed to create cache directory")?;
        debug!(path = %base.display(), "Storage initialized with cacache");
        Ok(Self { cache_path: base })
    }

    /// Открыть закэшированный файл
    pub async fn open(&self, key: &str) -> Result<Option<(File, Metadata)>> {
        let start = Instant::now();
        
        let entry = match cacache::index::find_async(&self.cache_path, key).await? {
            Some(e) => e,
            None => return Ok(None),
        };

        let meta: Metadata = match &entry.metadata {
            serde_json::Value::Null => return Ok(None),
            v => serde_json::from_value(v.clone()).context("Failed to parse metadata")?,
        };

        let content_path = cacache::content_path(&self.cache_path, &entry.integrity);
        
        match File::open(&content_path).await {
            Ok(file) => {
                metrics::record_storage_operation("open", start.elapsed(), true);
                metrics::record_storage_read(meta.size);
                Ok(Some((file, meta)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                warn!(key, "Content missing for index entry, cleaning up");
                let _ = cacache::remove_async(&self.cache_path, key).await;
                metrics::record_storage_error("content_missing");
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Получить только metadata
    pub async fn get_metadata(&self, key: &str) -> Result<Option<Metadata>> {
        match cacache::index::find_async(&self.cache_path, key).await? {
            Some(entry) if entry.metadata != serde_json::Value::Null => {
                let meta: Metadata = serde_json::from_value(entry.metadata)?;
                Ok(Some(meta))
            }
            _ => Ok(None),
        }
    }

    /// Создать writer для streaming записи
    pub async fn create_writer(&self, key: &str, meta: &Metadata) -> Result<CacheWriter> {
        let start = Instant::now();
        let meta_json = serde_json::to_value(meta)?;
        
        let writer = WriteOpts::new()
            .algorithm(cacache::Algorithm::Sha256)
            .metadata(meta_json)
            .open_async(&self.cache_path, key)
            .await
            .context("Failed to create cache writer")?;
        
        metrics::record_storage_operation("create_writer", start.elapsed(), true);
        metrics::inc_temp_files();
        
        Ok(CacheWriter { 
            inner: writer,
            key: key.to_string(),
            size: 0,
        })
    }

    /// Записать данные из AsyncRead
    pub async fn put_from_reader<R: AsyncRead + Unpin>(
        &self, 
        key: &str, 
        reader: R, 
        meta: &Metadata
    ) -> Result<ssri::Integrity> {
        let start = Instant::now();
        let meta_json = serde_json::to_value(meta)?;
        
        let integrity = cacache::copy_async(&self.cache_path, key, reader).await
            .context("Failed to write to cache")?;

        // Update metadata
        cacache::index::insert_async(
            &self.cache_path, 
            key, 
            WriteOpts::new()
                .integrity(integrity.clone())
                .metadata(meta_json)
                .size(meta.size as usize)
        ).await?;
        
        metrics::record_storage_operation("put", start.elapsed(), true);
        metrics::record_storage_write(meta.size);
        debug!(key, size = meta.size, hash = %integrity, "Cached");
        
        Ok(integrity)
    }

    /// Обновить timestamp (touch) для TTL refresh
    pub async fn touch(&self, key: &str) -> Result<bool> {
        let entry = match cacache::index::find_async(&self.cache_path, key).await? {
            Some(e) => e,
            None => return Ok(false),
        };

        let mut meta: Metadata = match serde_json::from_value(entry.metadata) {
            Ok(m) => m,
            Err(_) => return Ok(false),
        };
        
        meta.stored_at = now_secs();
        let meta_json = serde_json::to_value(&meta)?;

        cacache::index::insert_async(
            &self.cache_path, 
            key, 
            WriteOpts::new()
                .integrity(entry.integrity)
                .metadata(meta_json)
                .size(entry.size)
        ).await?;

        debug!(key, "Touched");
        Ok(true)
    }

    /// Удалить запись
    pub async fn delete(&self, key: &str) -> Result<()> {
        let start = Instant::now();
        cacache::remove_async(&self.cache_path, key).await?;
        metrics::record_storage_operation("delete", start.elapsed(), true);
        debug!(key, "Deleted");
        Ok(())
    }

    /// Garbage collection - удаляет unreferenced content
    pub async fn gc(&self) -> Result<usize> {
        let start = Instant::now();
        let cache_path = self.cache_path.clone();
        
        // cacache gc is sync, run in blocking task
        let removed = tokio::task::spawn_blocking(move || {
            cacache::clear_sync(&cache_path)
        }).await??;
        
        if removed > 0 {
            info!(removed, "GC completed");
            metrics::record_orphans_removed(removed as u64);
        }
        metrics::record_maintenance_run("gc", start.elapsed());
        
        Ok(removed)
    }

    /// Список всех записей (для загрузки индекса)
    pub async fn list_entries(&self) -> Result<Vec<Metadata>> {
        let cache_path = self.cache_path.clone();
        
        let entries = tokio::task::spawn_blocking(move || {
            cacache::list_sync(&cache_path)
                .filter_map(|entry| {
                    serde_json::from_value::<Metadata>(entry.metadata).ok()
                })
                .collect::<Vec<_>>()
        }).await?;
        
        Ok(entries)
    }

    /// Проверить целостность content
    pub async fn verify(&self, key: &str) -> Result<bool> {
        let entry = match cacache::index::find_async(&self.cache_path, key).await? {
            Some(e) => e,
            None => return Ok(false),
        };

        let content_path = cacache::content_path(&self.cache_path, &entry.integrity);
        
        match tokio::fs::metadata(&content_path).await {
            Ok(m) => Ok(m.len() == entry.size as u64),
            Err(_) => Ok(false),
        }
    }

    // Legacy compatibility - теперь пустые операции
    pub async fn cleanup_temp(&self) -> Result<()> { Ok(()) }
    pub async fn cleanup_orphans(&self) -> Result<usize> { self.gc().await }
}

/// Streaming cache writer с автоматическим commit
pub struct CacheWriter {
    inner: cacache::Writer,
    key: String,
    size: u64,
}

impl CacheWriter {
    pub async fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.inner.write_all(data).await?;
        self.size += data.len() as u64;
        Ok(data.len())
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush().await
    }

    pub fn written(&self) -> u64 {
        self.size
    }

    pub async fn commit(self) -> Result<(u64, ssri::Integrity)> {
        let integrity = self.inner.commit().await
            .context("Failed to commit cache write")?;
        metrics::dec_temp_files();
        metrics::record_storage_write(self.size);
        debug!(key = %self.key, size = self.size, hash = %integrity, "Committed");
        Ok((self.size, integrity))
    }
}

impl Drop for CacheWriter {
    fn drop(&mut self) {
    }
}