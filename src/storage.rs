use crate::metrics;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

const HASH_PREFIX_LEN: usize = 2;

static INSTANCE_ID: OnceLock<u64> = OnceLock::new();
static COUNTER: AtomicU64 = AtomicU64::new(0);

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs()
}

fn instance_id() -> u64 {
    *INSTANCE_ID.get_or_init(|| {
        std::process::id() as u64 ^ SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos() as u64
    })
}

fn unique_id() -> String {
    format!("{:016x}_{:016x}", instance_id(), COUNTER.fetch_add(1, Ordering::Relaxed))
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
        Self { headers, url: url.into(), key: key.into(), stored_at: now_secs(), size, etag: None, last_modified: None }
    }

    pub fn with_conditionals(mut self, etag: Option<String>, last_modified: Option<String>) -> Self {
        self.etag = etag;
        self.last_modified = last_modified;
        self
    }
}

/// Timed storage operation helper
macro_rules! timed_op {
    ($op:literal, $body:expr) => {{
        let start = Instant::now();
        let result = $body;
        let success = result.is_ok();
        metrics::record_storage_operation($op, start.elapsed(), success);
        result
    }};
}

#[derive(Clone)]
pub struct Storage {
    base: PathBuf,
    temp: PathBuf,
}

impl Storage {
    pub async fn new(base: PathBuf) -> Result<Self> {
        let temp = base.join(".tmp");
        fs::create_dir_all(&base).await.context("Failed to create cache dir")?;
        fs::create_dir_all(&temp).await.context("Failed to create temp dir")?;
        debug!(path = %base.display(), "Storage initialized");
        Ok(Self { base, temp })
    }

    fn data_path(&self, key: &str) -> PathBuf {
        let hash = blake3::hash(key.as_bytes()).to_hex();
        let h = hash.as_str();
        self.base.join(&h[..HASH_PREFIX_LEN]).join(&h[HASH_PREFIX_LEN..HASH_PREFIX_LEN * 2]).join(h)
    }

    fn meta_path(&self, key: &str) -> PathBuf { self.data_path(key).with_extension("json") }
    fn temp_path(&self, ext: &str) -> PathBuf { self.temp.join(format!("{}.{}", unique_id(), ext)) }

    pub async fn cleanup_temp(&self) -> Result<()> {
        timed_op!("cleanup_temp", async {
            let mut entries = match fs::read_dir(&self.temp).await {
                Ok(e) => e,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e.into()),
            };
            let mut count = 0u64;
            while let Ok(Some(entry)) = entries.next_entry().await {
                if fs::remove_file(entry.path()).await.is_ok() { count += 1; }
            }
            if count > 0 { metrics::record_temp_cleanup(count); }
            Ok(())
        }.await)
    }

    pub async fn list_metadata_paths(&self) -> Result<Vec<PathBuf>> {
        let mut result = Vec::new();
        let mut stack = vec![self.base.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await { Ok(e) => e, Err(_) => continue };
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let Ok(ft) = entry.file_type().await else { continue };
                if ft.is_dir() && !path.file_name().is_some_and(|n| n.to_str().is_some_and(|s| s.starts_with('.'))) {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "json") {
                    result.push(path);
                }
            }
        }
        Ok(result)
    }

    pub async fn read_metadata_from_path(&self, path: &Path) -> Result<Option<Metadata>> {
        match fs::read(path).await {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(meta) => Ok(Some(meta)),
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "Corrupted metadata");
                    metrics::record_metadata_parse_error();
                    let _ = fs::remove_file(path).await;
                    Ok(None)
                }
            },
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_temp_data(&self) -> Result<(PathBuf, File)> {
        let path = self.temp_path("data");
        let file = File::create(&path).await.context("Failed to create temp file")?;
        metrics::record_temp_file_created();
        Ok((path, file))
    }

    pub async fn open(&self, key: &str) -> Result<Option<(File, Metadata)>> {
        let meta_path = self.meta_path(key);
        let data_path = self.data_path(key);

        let meta: Metadata = match self.read_metadata_from_path(&meta_path).await? {
            Some(m) => m,
            None => return Ok(None),
        };

        let data_size = match fs::metadata(&data_path).await {
            Ok(m) => m.len(),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                warn!(key, "Data file missing");
                let _ = fs::remove_file(&meta_path).await;
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        if data_size != meta.size {
            warn!(key, expected = meta.size, actual = data_size, "Size mismatch");
            let _ = self.delete(key).await;
            return Ok(None);
        }

        match File::open(&data_path).await {
            Ok(file) => {
                metrics::record_storage_read(meta.size);
                Ok(Some((file, meta)))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_metadata(&self, key: &str) -> Result<Option<Metadata>> {
        self.read_metadata_from_path(&self.meta_path(key)).await
    }

    pub async fn put_from_temp_data(&self, key: &str, temp_data: PathBuf, meta: &Metadata) -> Result<u64> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        if let Some(parent) = data_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let temp_meta = self.temp_path("json");
        let json = serde_json::to_vec(meta)?;
        let mut file = File::create(&temp_meta).await?;
        file.write_all(&json).await?;
        file.sync_all().await?;

        if let Err(e) = fs::rename(&temp_data, &data_path).await {
            let _ = fs::remove_file(&temp_data).await;
            let _ = fs::remove_file(&temp_meta).await;
            return Err(e.into());
        }

        if let Err(e) = fs::rename(&temp_meta, &meta_path).await {
            let _ = fs::remove_file(&data_path).await;
            let _ = fs::remove_file(&temp_meta).await;
            return Err(e.into());
        }

        metrics::record_storage_write(meta.size);
        debug!(key, size = meta.size, "Cached");
        Ok(meta.size)
    }

    pub async fn touch(&self, key: &str) -> Result<bool> {
        let meta_path = self.meta_path(key);
        let mut meta: Metadata = match self.read_metadata_from_path(&meta_path).await? {
            Some(m) => m,
            None => return Ok(false),
        };

        meta.stored_at = now_secs();

        let temp_meta = self.temp_path("json");
        let json = serde_json::to_vec(&meta)?;
        let mut file = File::create(&temp_meta).await?;
        file.write_all(&json).await?;
        file.sync_all().await?;
        fs::rename(&temp_meta, &meta_path).await?;

        debug!(key, "Touched");
        Ok(true)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let _ = fs::remove_file(self.data_path(key)).await;
        let _ = fs::remove_file(self.meta_path(key)).await;
        debug!(key, "Deleted");
        Ok(())
    }

    pub async fn cleanup_orphans(&self) -> Result<usize> {
        let mut stack = vec![self.base.clone()];
        let mut removed = 0usize;

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await { Ok(e) => e, Err(_) => continue };
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let Ok(ft) = entry.file_type().await else { continue };

                if ft.is_dir() {
                    if !path.file_name().is_some_and(|n| n.to_str().is_some_and(|s| s.starts_with('.'))) {
                        stack.push(path);
                    }
                    continue;
                }

                let is_orphan = match path.extension().and_then(|e| e.to_str()) {
                    Some("json") => !fs::try_exists(path.with_extension("")).await.unwrap_or(true),
                    None if path.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.len() == 64) => {
                        !fs::try_exists(path.with_extension("json")).await.unwrap_or(true)
                    }
                    _ => false,
                };

                if is_orphan {
                    let _ = fs::remove_file(&path).await;
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            debug!(removed, "Removed orphans");
            metrics::record_orphans_removed(removed as u64);
        }
        Ok(removed)
    }
}