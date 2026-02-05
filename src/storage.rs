use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

static INSTANCE_ID: OnceLock<u64> = OnceLock::new();
static COUNTER: AtomicU64 = AtomicU64::new(0);

fn get_instance_id() -> u64 {
    *INSTANCE_ID.get_or_init(|| {
        let pid = std::process::id() as u64;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        pid ^ timestamp
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(with = "http_serde::header_map")]
    pub headers: axum::http::HeaderMap,
    pub url: String,
    pub key: String,
    pub stored_at: u64,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl Metadata {
    pub fn age(&self) -> u64 {
        now_secs().saturating_sub(self.stored_at)
    }

    pub fn remaining_ttl(&self, max_ttl: u64) -> u64 {
        max_ttl.saturating_sub(self.age())
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unique_id() -> String {
    let instance = get_instance_id();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}_{:016x}", instance, counter)
}

struct TempFileGuard {
    paths: Vec<PathBuf>,
    committed: bool,
}

impl TempFileGuard {
    fn new() -> Self {
        Self {
            paths: Vec::new(),
            committed: false,
        }
    }

    fn add(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.committed {
            for path in &self.paths {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[derive(Clone)]
pub struct Storage {
    base_path: PathBuf,
    temp_dir: PathBuf,
}

impl Storage {
    pub async fn new(path: PathBuf) -> Result<Self> {
        let temp_dir = path.join(".tmp");
        fs::create_dir_all(&path).await?;
        fs::create_dir_all(&temp_dir).await?;
        debug!(path = %path.display(), "Storage initialized");
        Ok(Self {
            base_path: path,
            temp_dir,
        })
    }

    fn data_path(&self, key: &str) -> PathBuf {
        let hash = blake3::hash(key.as_bytes());
        let hex = hash.to_hex();
        let h = hex.as_str();
        self.base_path.join(&h[0..2]).join(&h[2..4]).join(h)
    }

    fn meta_path(&self, key: &str) -> PathBuf {
        let mut p = self.data_path(key);
        p.set_extension("json");
        p
    }

    /// Quick cleanup of temp files only - for fast startup
    pub async fn cleanup_temp(&self) -> Result<()> {
        if let Ok(mut entries) = fs::read_dir(&self.temp_dir).await {
            while let Ok(Some(e)) = entries.next_entry().await {
                let _ = fs::remove_file(e.path()).await;
            }
        }
        debug!("Temp directory cleaned");
        Ok(())
    }

    /// List all metadata file paths without reading contents
    pub async fn list_metadata_paths(&self) -> Result<Vec<PathBuf>> {
        let mut result = Vec::new();
        let mut stack = vec![self.base_path.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let file_type = match entry.file_type().await {
                    Ok(ft) => ft,
                    Err(_) => continue,
                };

                if file_type.is_dir() {
                    if !path
                        .file_name()
                        .map(|n| n.to_string_lossy().starts_with('.'))
                        .unwrap_or(false)
                    {
                        stack.push(path);
                    }
                } else if path.extension().map(|e| e == "json").unwrap_or(false) {
                    result.push(path);
                }
            }
        }

        Ok(result)
    }

    /// Read metadata from a specific path
    pub async fn read_metadata_from_path(&self, path: &PathBuf) -> Result<Option<Metadata>> {
        let bytes = match fs::read(path).await {
            Ok(b) => b,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        match serde_json::from_slice(&bytes) {
            Ok(meta) => Ok(Some(meta)),
            Err(e) => {
                warn!(path = %path.display(), error = %e, "Corrupted metadata, removing");
                let _ = fs::remove_file(path).await;
                Ok(None)
            }
        }
    }

    pub async fn create_temp_data(&self) -> Result<(PathBuf, File)> {
        let id = unique_id();
        let temp_data = self.temp_dir.join(format!("{}.data", id));
        let file = File::create(&temp_data).await?;
        Ok((temp_data, file))
    }

    pub async fn open(&self, key: &str) -> Result<Option<(File, Metadata)>> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        let meta_bytes = match fs::read(&meta_path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let meta: Metadata = match serde_json::from_slice(&meta_bytes) {
            Ok(m) => m,
            Err(e) => {
                warn!(key, error = %e, "Corrupted metadata, removing");
                let _ = self.delete(key).await;
                return Ok(None);
            }
        };

        let data_meta = match fs::metadata(&data_path).await {
            Ok(m) => m,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                warn!(key, "Data file missing, removing metadata");
                let _ = fs::remove_file(&meta_path).await;
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        if data_meta.len() != meta.size {
            warn!(
                key,
                expected = meta.size,
                actual = data_meta.len(),
                "Size mismatch, removing"
            );
            let _ = self.delete(key).await;
            return Ok(None);
        }

        let file = match File::open(&data_path).await {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        Ok(Some((file, meta)))
    }

    pub async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Metadata)>> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        let meta_bytes = match fs::read(&meta_path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let meta: Metadata = match serde_json::from_slice(&meta_bytes) {
            Ok(m) => m,
            Err(e) => {
                warn!(key, error = %e, "Corrupted metadata, removing");
                let _ = self.delete(key).await;
                return Ok(None);
            }
        };

        let data = match fs::read(&data_path).await {
            Ok(d) => d,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                warn!(key, "Data file missing, removing metadata");
                let _ = fs::remove_file(&meta_path).await;
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        if data.len() as u64 != meta.size {
            warn!(
                key,
                expected = meta.size,
                actual = data.len(),
                "Size mismatch, removing"
            );
            let _ = self.delete(key).await;
            return Ok(None);
        }

        Ok(Some((data, meta)))
    }

    pub async fn get_metadata(&self, key: &str) -> Result<Option<Metadata>> {
        let meta_path = self.meta_path(key);

        let meta_bytes = match fs::read(&meta_path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        match serde_json::from_slice(&meta_bytes) {
            Ok(meta) => Ok(Some(meta)),
            Err(e) => {
                warn!(key, error = %e, "Corrupted metadata");
                let _ = self.delete(key).await;
                Ok(None)
            }
        }
    }

    pub async fn put(&self, key: &str, data: &[u8], meta: &Metadata) -> Result<u64> {
        let (temp_data, mut file) = self.create_temp_data().await?;
        let mut guard = TempFileGuard::new();
        guard.add(temp_data.clone());

        file.write_all(data).await?;
        file.sync_all().await?;
        drop(file);

        self.put_from_temp_data(key, temp_data, meta).await?;
        guard.commit();
        Ok(data.len() as u64)
    }

    pub async fn put_from_temp_data(
        &self,
        key: &str,
        temp_data: PathBuf,
        meta: &Metadata,
    ) -> Result<u64> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        if let Some(parent) = data_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut guard = TempFileGuard::new();

        let id = unique_id();
        let temp_meta = self.temp_dir.join(format!("{}.json", id));
        guard.add(temp_meta.clone());

        let meta_json = serde_json::to_vec(meta)?;
        let mut file = File::create(&temp_meta).await?;
        file.write_all(&meta_json).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_data, &data_path).await?;
        fs::rename(&temp_meta, &meta_path).await?;

        guard.commit();

        debug!(key, size = meta.size, "Cached");
        Ok(meta.size)
    }

    pub async fn touch(&self, key: &str) -> Result<bool> {
        let meta_path = self.meta_path(key);

        let meta_bytes = match fs::read(&meta_path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };

        let mut meta: Metadata = match serde_json::from_slice(&meta_bytes) {
            Ok(m) => m,
            Err(_) => return Ok(false),
        };

        meta.stored_at = now_secs();

        let meta_json = serde_json::to_vec(&meta)?;

        let mut guard = TempFileGuard::new();
        let id = unique_id();
        let temp_meta = self.temp_dir.join(format!("{}.json", id));
        guard.add(temp_meta.clone());

        let mut file = File::create(&temp_meta).await?;
        file.write_all(&meta_json).await?;
        file.sync_all().await?;
        drop(file);

        fs::rename(&temp_meta, &meta_path).await?;
        guard.commit();

        debug!(key, "Touched");
        Ok(true)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        let _ = fs::remove_file(&data_path).await;
        let _ = fs::remove_file(&meta_path).await;

        debug!(key, "Deleted from cache");
        Ok(())
    }

    pub async fn cleanup(&self) -> Result<()> {
        self.cleanup_temp().await?;
        self.cleanup_orphans().await?;
        debug!("Cleanup completed");
        Ok(())
    }

    pub async fn cleanup_orphans(&self) -> Result<()> {
        let mut stack = vec![self.base_path.clone()];
        let mut removed = 0usize;

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let file_type = match entry.file_type().await {
                    Ok(ft) => ft,
                    Err(_) => continue,
                };

                if file_type.is_dir() {
                    if path
                        .file_name()
                        .map(|n| n.to_string_lossy().starts_with('.'))
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    stack.push(path);
                    continue;
                }

                let ext = path.extension().and_then(|e| e.to_str());

                if ext == Some("json") {
                    let data_path = path.with_extension("");
                    if !fs::try_exists(&data_path).await.unwrap_or(false) {
                        debug!(path = %path.display(), "Removing orphaned metadata");
                        let _ = fs::remove_file(&path).await;
                        removed += 1;
                    }
                    continue;
                }

                if ext.is_none() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        let looks_like_hash =
                            name.len() == 64 && name.chars().all(|c| c.is_ascii_hexdigit());
                        if looks_like_hash {
                            let meta_path = path.with_extension("json");
                            if !fs::try_exists(&meta_path).await.unwrap_or(false) {
                                debug!(path = %path.display(), "Removing orphaned data");
                                let _ = fs::remove_file(&path).await;
                                removed += 1;
                            }
                        }
                    }
                }
            }
        }

        if removed > 0 {
            debug!(removed, "Removed orphaned cache files");
        }

        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<(String, Metadata)>> {
        let mut result = Vec::new();
        let mut stack = vec![self.base_path.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let file_type = match entry.file_type().await {
                    Ok(ft) => ft,
                    Err(_) => continue,
                };

                if file_type.is_dir() {
                    if path
                        .file_name()
                        .map(|n| n.to_string_lossy().starts_with('.'))
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    stack.push(path);
                } else if path.extension().map(|e| e == "json").unwrap_or(false) {
                    if let Ok(bytes) = fs::read(&path).await {
                        if let Ok(meta) = serde_json::from_slice::<Metadata>(&bytes) {
                            result.push((meta.key.clone(), meta));
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}