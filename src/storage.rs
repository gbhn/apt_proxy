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

/// Generates a unique instance ID based on PID and timestamp
fn instance_id() -> u64 {
    *INSTANCE_ID.get_or_init(|| {
        std::process::id() as u64 ^ now_nanos()
    })
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unique_id() -> String {
    format!("{:016x}_{:016x}", instance_id(), COUNTER.fetch_add(1, Ordering::Relaxed))
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
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl Metadata {
    /// Returns age in seconds since stored
    pub fn age(&self) -> u64 {
        now_secs().saturating_sub(self.stored_at)
    }
}

/// RAII guard for temporary files cleanup on error
struct TempGuard {
    paths: Vec<PathBuf>,
    disarmed: bool,
}

impl TempGuard {
    fn new() -> Self {
        Self { paths: Vec::with_capacity(2), disarmed: false }
    }

    fn track(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    fn disarm(mut self) {
        self.disarmed = true;
    }
}

impl Drop for TempGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            for path in &self.paths {
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
    const HASH_PREFIX_LEN: usize = 2;

    pub async fn new(base: PathBuf) -> Result<Self> {
        let temp = base.join(".tmp");
        fs::create_dir_all(&base).await?;
        fs::create_dir_all(&temp).await?;
        debug!(path = %base.display(), "Storage initialized");
        Ok(Self { base, temp })
    }

    /// Computes content-addressed path for a key
    fn data_path(&self, key: &str) -> PathBuf {
        let hash = blake3::hash(key.as_bytes()).to_hex();
        let h = hash.as_str();
        self.base
            .join(&h[..Self::HASH_PREFIX_LEN])
            .join(&h[Self::HASH_PREFIX_LEN..Self::HASH_PREFIX_LEN * 2])
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
            Err(e) => return Err(e.into()),
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let _ = fs::remove_file(entry.path()).await;
        }
        debug!("Temp directory cleaned");
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
                let Ok(ft) = entry.file_type().await else { continue };

                if ft.is_dir() {
                    // Skip hidden directories
                    let dominated = path
                        .file_name()
                        .is_some_and(|n| n.to_string_lossy().starts_with('.'));
                    if !dominated {
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
    pub async fn read_metadata_from_path(&self, path: &PathBuf) -> Result<Option<Metadata>> {
        match self.read_json(path).await {
            Ok(meta) => Ok(Some(meta)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) if is_parse_error(&e) => {
                warn!(path = %path.display(), error = %e, "Corrupted metadata, removing");
                let _ = fs::remove_file(path).await;
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Creates a new temporary file for writing
    pub async fn create_temp_data(&self) -> Result<(PathBuf, File)> {
        let path = self.temp_path("data");
        let file = File::create(&path).await?;
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
            Err(e) => return Err(e),
        };

        // Verify data file exists and matches expected size
        let data_size = match fs::metadata(&data_path).await {
            Ok(m) => m.len(),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                warn!(key, "Data file missing, removing metadata");
                let _ = fs::remove_file(&meta_path).await;
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        if data_size != meta.size {
            warn!(key, expected = meta.size, actual = data_size, "Size mismatch, removing");
            let _ = self.delete(key).await;
            return Ok(None);
        }

        match File::open(&data_path).await {
            Ok(file) => Ok(Some((file, meta))),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
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
            Err(e) => Err(e),
        }
    }

    /// Commits a temporary data file to permanent storage
    pub async fn put_from_temp_data(&self, key: &str, temp_data: PathBuf, meta: &Metadata) -> Result<u64> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        if let Some(parent) = data_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut guard = TempGuard::new();

        // Write metadata to temp file first
        let temp_meta = self.temp_path("json");
        guard.track(temp_meta.clone());
        self.write_json(&temp_meta, meta).await?;

        // Atomic rename both files
        fs::rename(&temp_data, &data_path).await?;
        fs::rename(&temp_meta, &meta_path).await?;

        guard.disarm();
        debug!(key, size = meta.size, "Cached");
        Ok(meta.size)
    }

    /// Updates the stored_at timestamp for a key
    pub async fn touch(&self, key: &str) -> Result<bool> {
        let meta_path = self.meta_path(key);

        let mut meta: Metadata = match self.read_json(&meta_path).await {
            Ok(m) => m,
            Err(e) if is_not_found(&e) => return Ok(false),
            Err(_) => return Ok(false),
        };

        meta.stored_at = now_secs();

        let mut guard = TempGuard::new();
        let temp_meta = self.temp_path("json");
        guard.track(temp_meta.clone());

        self.write_json(&temp_meta, &meta).await?;
        fs::rename(&temp_meta, &meta_path).await?;

        guard.disarm();
        debug!(key, "Touched");
        Ok(true)
    }

    /// Deletes both data and metadata files for a key
    pub async fn delete(&self, key: &str) -> Result<()> {
        let _ = fs::remove_file(self.data_path(key)).await;
        let _ = fs::remove_file(self.meta_path(key)).await;
        debug!(key, "Deleted");
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
                let Ok(ft) = entry.file_type().await else { continue };

                if ft.is_dir() {
                    if !path.file_name().is_some_and(|n| n.to_string_lossy().starts_with('.')) {
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

    async fn read_json<T: serde::de::DeserializeOwned>(&self, path: &PathBuf) -> Result<T> {
        let bytes = fs::read(path).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn write_json<T: serde::Serialize>(&self, path: &PathBuf, data: &T) -> Result<()> {
        let json = serde_json::to_vec(data)?;
        let mut file = File::create(path).await?;
        file.write_all(&json).await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn is_orphan(&self, path: &PathBuf) -> bool {
        let ext = path.extension().and_then(|e| e.to_str());

        match ext {
            Some("json") => {
                // Metadata without data file
                !fs::try_exists(path.with_extension("")).await.unwrap_or(true)
            }
            None => {
                // Data file - check if it looks like a hash
                let is_hash = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.len() == 64 && n.chars().all(|c| c.is_ascii_hexdigit()));

                is_hash && !fs::try_exists(path.with_extension("json")).await.unwrap_or(true)
            }
            _ => false,
        }
    }
}

fn is_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<std::io::Error>()
        .is_some_and(|io| io.kind() == ErrorKind::NotFound)
}

fn is_parse_error(e: &anyhow::Error) -> bool {
    e.downcast_ref::<serde_json::Error>().is_some()
}