use crate::logging::fields::size;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
};
use tracing::{debug, info, warn};

const WRITE_BUFFER_SIZE: usize = 512 * 1024;
const MAX_CONCURRENT_WRITES: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetadata {
    #[serde(with = "http_serde::header_map")]
    pub headers: axum::http::HeaderMap,
    pub original_url: String,
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
    base_dir: PathBuf,
    write_semaphore: tokio::sync::Semaphore,
}

impl Storage {
    pub async fn new(base_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&base_dir).await?;
        debug!(
            path = %base_dir.display(),
            "Storage directory initialized"
        );
        Ok(Self {
            base_dir,
            write_semaphore: tokio::sync::Semaphore::new(MAX_CONCURRENT_WRITES),
        })
    }

    #[inline]
    pub fn path_for(&self, key: &str) -> PathBuf {
        let hash = blake3::hash(key.as_bytes());
        let hex = hash.to_hex();
        let hex_str = hex.as_str();
        self.base_dir
            .join(&hex_str[0..2])
            .join(&hex_str[2..4])
            .join(hex_str)
    }

    #[inline]
    fn temp_path_for(&self, key: &str) -> PathBuf {
        self.path_for(key).with_extension("tmp")
    }

    #[inline]
    fn metadata_path_for(&self, cache_path: &Path) -> PathBuf {
        cache_path.with_extension("meta")
    }

    pub async fn exists(&self, key: &str) -> bool {
        let path = self.path_for(key);
        fs::try_exists(&path).await.unwrap_or(false)
    }

    pub async fn open(&self, key: &str) -> Result<Option<StoredFile>> {
        let path = self.path_for(key);

        let file = match File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let file_size = file.metadata().await?.len();
        let metadata = self.load_metadata(&path).await?;

        Ok(Some(StoredFile {
            file,
            metadata,
            size: file_size,
        }))
    }

    pub async fn create(&self, key: &str) -> Result<StorageWriter> {
        let _permit = self.write_semaphore.acquire().await?;
        
        let final_path = self.path_for(key);
        let temp_path = self.temp_path_for(key);

        if let Some(parent) = temp_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
            .await?;

        debug!(path = %temp_path.display(), "Created temp file");

        Ok(StorageWriter {
            writer: BufWriter::with_capacity(WRITE_BUFFER_SIZE, file),
            temp_path,
            final_path,
            bytes_written: 0,
        })
    }

    pub async fn delete(&self, key: &str) -> Result<u64> {
        let path = self.path_for(key);
        let metadata_path = self.metadata_path_for(&path);
        let mut deleted = 0u64;

        if let Ok(meta) = fs::metadata(&path).await {
            deleted += meta.len();
            fs::remove_file(&path).await.ok();
        }

        if let Ok(meta) = fs::metadata(&metadata_path).await {
            deleted += meta.len();
            fs::remove_file(&metadata_path).await.ok();
        }

        debug!(
            key = %crate::logging::fields::path(key),
            freed = %size(deleted),
            "Deleted cache entry"
        );

        Ok(deleted)
    }

    async fn save_metadata(&self, cache_path: &Path, metadata: &CacheMetadata) -> Result<u64> {
        let meta_path = self.metadata_path_for(cache_path);
        let json = serde_json::to_vec(metadata)?;
        let meta_size = json.len() as u64;
        fs::write(meta_path, json).await?;
        Ok(meta_size)
    }

    async fn load_metadata(&self, cache_path: &Path) -> Result<CacheMetadata> {
        let meta_path = self.metadata_path_for(cache_path);
        let bytes = fs::read(&meta_path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn cleanup_temp_files(&self) -> Result<()> {
        let mut dirs = vec![self.base_dir.clone()];
        let mut count = 0u32;
        let mut total_size = 0u64;

        while let Some(dir) = dirs.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path);
                } else if let Some(ext) = path.extension() {
                    if ext == "tmp" || ext == "part" {
                        if let Ok(meta) = fs::metadata(&path).await {
                            total_size += meta.len();
                        }
                        debug!(
                            file = %path.file_name().unwrap_or_default().to_string_lossy(),
                            "Removing stale temp file"
                        );
                        let _ = fs::remove_file(path).await;
                        count += 1;
                    }
                }
            }
        }

        if count > 0 {
            info!(
                files = count,
                freed = %size(total_size),
                "Cleaned up stale temp files"
            );
        }
        Ok(())
    }

    pub async fn list_all(&self) -> Result<Vec<(String, u64, u64, CacheMetadata)>> {
        let mut files = Vec::with_capacity(1024);
        let mut dirs = vec![self.base_dir.clone()];

        while let Some(dir) = dirs.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let meta = match entry.metadata().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                if meta.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if !meta.is_file() {
                    continue;
                }

                if let Some(ext) = path.extension() {
                    if ext == "meta" || ext == "tmp" || ext == "part" {
                        continue;
                    }
                }

                if let Ok(cache_meta) = self.load_metadata(&path).await {
                    let meta_path = self.metadata_path_for(&path);
                    let meta_size = fs::metadata(&meta_path).await.map(|m| m.len()).unwrap_or(0);

                    files.push((
                        cache_meta.original_url.clone(),
                        meta.len(),
                        meta_size,
                        cache_meta,
                    ));
                }
            }
        }
        Ok(files)
    }
    
    /// Get storage statistics
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
    pub file: File,
    pub metadata: CacheMetadata,
    pub size: u64,
}

pub struct StorageWriter {
    writer: BufWriter<File>,
    temp_path: PathBuf,
    final_path: PathBuf,
    bytes_written: u64,
}

impl StorageWriter {
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write_all(data).await?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    #[inline]
    pub const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub async fn finalize(
        mut self,
        storage: &Storage,
        metadata: CacheMetadata,
    ) -> Result<(u64, u64)> {
        self.writer.flush().await?;
        drop(self.writer);

        fs::rename(&self.temp_path, &self.final_path).await?;
        debug!(
            path = %self.final_path.file_name().unwrap_or_default().to_string_lossy(),
            size = %size(self.bytes_written),
            "Saved to cache"
        );

        let meta_size = storage.save_metadata(&self.final_path, &metadata).await?;
        Ok((self.bytes_written, meta_size))
    }

    pub async fn abort(self) -> Result<()> {
        drop(self.writer);
        fs::remove_file(&self.temp_path).await.ok();
        warn!(
            path = %self.temp_path.display(),
            "Aborted write operation"
        );
        Ok(())
    }
}