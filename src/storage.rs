use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

/// Hash prefix length for directory sharding (2 hex chars = 256 buckets)
const HASH_PREFIX_LEN: usize = 2;

static INSTANCE_ID: OnceLock<u64> = OnceLock::new();
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Time utilities
mod time {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn now_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos() as u64
    }

    pub fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
    }
}

/// Generates a unique instance ID based on PID and timestamp
fn instance_id() -> u64 {
    *INSTANCE_ID.get_or_init(|| std::process::id() as u64 ^ time::now_nanos())
}

/// Generates a unique identifier for temp files
fn unique_id() -> String {
    format!(
        "{:016x}_{:016x}",
        instance_id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// File metadata stored alongside cached data
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
    /// Returns age in seconds since stored
    #[inline]
    pub fn age(&self) -> u64 {
        time::now_secs().saturating_sub(self.stored_at)
    }

    /// Creates new metadata with current timestamp
    pub fn new(
        key: impl Into<String>,
        url: impl Into<String>,
        size: u64,
        headers: axum::http::HeaderMap,
    ) -> Self {
        Self {
            headers,
            url: url.into(),
            key: key.into(),
            stored_at: time::now_secs(),
            size,
            etag: None,
            last_modified: None,
        }
    }

    /// Sets conditional request headers from response
    pub fn with_conditionals(
        mut self,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Self {
        self.etag = etag;
        self.last_modified = last_modified;
        self
    }
}

/// RAII guard for temporary files cleanup on error
struct TempGuard {
    paths: Vec<PathBuf>,
    committed: bool,
}

impl TempGuard {
    fn new() -> Self {
        Self {
            paths: Vec::with_capacity(2),
            committed: false,
        }
    }

    fn track(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for TempGuard {
    fn drop(&mut self) {
        if !self.committed {
            for path in &self.paths {
                // Use blocking remove since we're in Drop
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

/// Content-addressable file storage with atomic writes
#[derive(Clone)]
pub struct Storage {
    base: PathBuf,
    temp: PathBuf,
}

impl Storage {
    /// Creates a new storage instance at the given base path
    pub async fn new(base: PathBuf) -> Result<Self> {
        let temp = base.join(".tmp");

        fs::create_dir_all(&base)
            .await
            .with_context(|| format!("Failed to create cache directory: {}", base.display()))?;

        fs::create_dir_all(&temp)
            .await
            .with_context(|| format!("Failed to create temp directory: {}", temp.display()))?;

        debug!(path = %base.display(), "Storage initialized");
        Ok(Self { base, temp })
    }

    /// Computes content-addressed path for a key using BLAKE3 hash
    fn data_path(&self, key: &str) -> PathBuf {
        let hash = blake3::hash(key.as_bytes()).to_hex();
        let h = hash.as_str();
        self.base
            .join(&h[..HASH_PREFIX_LEN])
            .join(&h[HASH_PREFIX_LEN..HASH_PREFIX_LEN * 2])
            .join(h)
    }

    fn meta_path(&self, key: &str) -> PathBuf {
        self.data_path(key).with_extension("json")
    }

    fn temp_path(&self, ext: &str) -> PathBuf {
        self.temp.join(format!("{}.{}", unique_id(), ext))
    }

    // ========== Public API ==========

    /// Cleans up temporary files from previous runs
    pub async fn cleanup_temp(&self) -> Result<()> {
        let mut entries = match fs::read_dir(&self.temp).await {
            Ok(e) => e,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e).context("Failed to read temp directory"),
        };

        let mut cleaned = 0u32;
        while let Ok(Some(entry)) = entries.next_entry().await {
            if fs::remove_file(entry.path()).await.is_ok() {
                cleaned += 1;
            }
        }

        if cleaned > 0 {
            debug!(count = cleaned, "Cleaned temp files");
        }
        Ok(())
    }

    /// Lists all metadata file paths for bulk loading
    pub async fn list_metadata_paths(&self) -> Result<Vec<PathBuf>> {
        let mut result = Vec::new();
        let mut stack = vec![self.base.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let Ok(ft) = entry.file_type().await else {
                    continue;
                };

                if ft.is_dir() {
                    // Skip hidden directories (e.g., .tmp)
                    if !is_hidden_path(&path) {
                        stack.push(path);
                    }
                } else if path.extension().is_some_and(|e| e == "json") {
                    result.push(path);
                }
            }
        }

        Ok(result)
    }

    /// Reads metadata from a specific path
    pub async fn read_metadata_from_path(&self, path: &Path) -> Result<Option<Metadata>> {
        match self.read_json(path).await {
            Ok(meta) => Ok(Some(meta)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) if is_parse_error(&e) => {
                warn!(path = %path.display(), error = %e, "Corrupted metadata, removing");
                let _ = fs::remove_file(path).await;
                Ok(None)
            }
            Err(e) => Err(e).context("Failed to read metadata"),
        }
    }

    /// Creates a new temporary file for writing
    pub async fn create_temp_data(&self) -> Result<(PathBuf, File)> {
        let path = self.temp_path("data");
        let file = File::create(&path)
            .await
            .with_context(|| format!("Failed to create temp file: {}", path.display()))?;
        Ok((path, file))
    }

    /// Opens a cached file by key, returns file handle and metadata
    pub async fn open(&self, key: &str) -> Result<Option<(File, Metadata)>> {
        let meta_path = self.meta_path(key);
        let data_path = self.data_path(key);

        let meta: Metadata = match self.read_json(&meta_path).await {
            Ok(m) => m,
            Err(e) if is_not_found(&e) => return Ok(None),
            Err(e) if is_parse_error(&e) => {
                warn!(key, error = %e, "Corrupted metadata, removing");
                let _ = self.delete(key).await;
                return Ok(None);
            }
            Err(e) => return Err(e).context("Failed to read metadata"),
        };

        // Verify data file exists and matches expected size
        let data_size = match fs::metadata(&data_path).await {
            Ok(m) => m.len(),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                warn!(key, "Data file missing, removing metadata");
                let _ = fs::remove_file(&meta_path).await;
                return Ok(None);
            }
            Err(e) => return Err(e).context("Failed to stat data file"),
        };

        if data_size != meta.size {
            warn!(
                key,
                expected = meta.size,
                actual = data_size,
                "Size mismatch, removing"
            );
            let _ = self.delete(key).await;
            return Ok(None);
        }

        match File::open(&data_path).await {
            Ok(file) => Ok(Some((file, meta))),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context("Failed to open data file"),
        }
    }

    /// Returns metadata for a key without opening the data file
    pub async fn get_metadata(&self, key: &str) -> Result<Option<Metadata>> {
        match self.read_json(&self.meta_path(key)).await {
            Ok(meta) => Ok(Some(meta)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) if is_parse_error(&e) => {
                warn!(key, error = %e, "Corrupted metadata");
                let _ = self.delete(key).await;
                Ok(None)
            }
            Err(e) => Err(e).context("Failed to read metadata"),
        }
    }

    /// Commits a temporary data file to permanent storage
    pub async fn put_from_temp_data(
        &self,
        key: &str,
        temp_data: PathBuf,
        meta: &Metadata,
    ) -> Result<u64> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        if let Some(parent) = data_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create cache subdirectory")?;
        }

        let mut guard = TempGuard::new();

        // Write metadata to temp file first
        let temp_meta = self.temp_path("json");
        guard.track(temp_meta.clone());
        self.write_json(&temp_meta, meta)
            .await
            .context("Failed to write metadata")?;

        // Atomic rename both files
        fs::rename(&temp_data, &data_path)
            .await
            .context("Failed to commit data file")?;
        fs::rename(&temp_meta, &meta_path)
            .await
            .context("Failed to commit metadata")?;

        guard.commit();
        debug!(key, size = meta.size, "Cached");
        Ok(meta.size)
    }

    /// Updates the stored_at timestamp for a key (for 304 responses)
    pub async fn touch(&self, key: &str) -> Result<bool> {
        let meta_path = self.meta_path(key);

        let mut meta: Metadata = match self.read_json(&meta_path).await {
            Ok(m) => m,
            Err(e) if is_not_found(&e) => return Ok(false),
            Err(_) => return Ok(false),
        };

        meta.stored_at = time::now_secs();

        let mut guard = TempGuard::new();
        let temp_meta = self.temp_path("json");
        guard.track(temp_meta.clone());

        self.write_json(&temp_meta, &meta).await?;
        fs::rename(&temp_meta, &meta_path).await?;

        guard.commit();
        debug!(key, "Touched");
        Ok(true)
    }

    /// Deletes both data and metadata files for a key
    pub async fn delete(&self, key: &str) -> Result<()> {
        let data_result = fs::remove_file(self.data_path(key)).await;
        let meta_result = fs::remove_file(self.meta_path(key)).await;

        // Only log if at least one file existed
        if data_result.is_ok() || meta_result.is_ok() {
            debug!(key, "Deleted");
        }
        Ok(())
    }

    /// Removes orphaned files (data without metadata or vice versa)
    pub async fn cleanup_orphans(&self) -> Result<usize> {
        let mut stack = vec![self.base.clone()];
        let mut removed = 0usize;

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let Ok(ft) = entry.file_type().await else {
                    continue;
                };

                if ft.is_dir() {
                    if !is_hidden_path(&path) {
                        stack.push(path);
                    }
                    continue;
                }

                if self.is_orphan(&path).await {
                    debug!(path = %path.display(), "Removing orphan");
                    let _ = fs::remove_file(&path).await;
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            debug!(removed, "Removed orphaned files");
        }
        Ok(removed)
    }

    // ========== Private Helpers ==========

    async fn read_json<T: serde::de::DeserializeOwned>(&self, path: &Path) -> Result<T> {
        let bytes = fs::read(path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn write_json<T: serde::Serialize>(&self, path: &Path, data: &T) -> Result<()> {
        let json = serde_json::to_vec(data)?;
        let mut file = File::create(path).await?;
        file.write_all(&json).await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn is_orphan(&self, path: &Path) -> bool {
        let ext = path.extension().and_then(|e| e.to_str());

        match ext {
            Some("json") => {
                // Metadata file - check if data file exists
                let data_path = path.with_extension("");
                !fs::try_exists(&data_path).await.unwrap_or(true)
            }
            None => {
                // Data file - verify it's a valid hash and check for metadata
                let is_valid_hash = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.len() == 64 && n.chars().all(|c| c.is_ascii_hexdigit()));

                if !is_valid_hash {
                    return false;
                }

                let meta_path = path.with_extension("json");
                !fs::try_exists(&meta_path).await.unwrap_or(true)
            }
            _ => false,
        }
    }
}

// ========== Helper Functions ==========

fn is_hidden_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with('.'))
}

fn is_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<std::io::Error>()
        .is_some_and(|io| io.kind() == ErrorKind::NotFound)
}

fn is_parse_error(e: &anyhow::Error) -> bool {
    e.downcast_ref::<serde_json::Error>().is_some()
}