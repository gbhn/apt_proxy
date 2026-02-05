use crate::config::CacheConfig;
use crate::storage::{Metadata, Storage};
use futures::StreamExt;
use moka::future::Cache;
use moka::notification::RemovalCause;
use moka::Expiry;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tracing::{debug, info, warn};

/// Parallel I/O concurrency for cache index loading
const LOAD_CONCURRENCY: usize = 64;

/// Minimum weight for cache entries (1 KB)
const MIN_ENTRY_WEIGHT_KB: u32 = 1;

/// Cache entry tracking size for LRU eviction
#[derive(Clone, Copy, Debug)]
struct CacheEntry {
    size: u64,
}

impl CacheEntry {
    fn new(size: u64) -> Self {
        Self { size }
    }

    /// Calculates weight in KB for cache capacity management
    fn weight(&self) -> u32 {
        (self.size / 1024).clamp(MIN_ENTRY_WEIGHT_KB as u64, u32::MAX as u64) as u32
    }
}

/// TTL expiration policy based on file path patterns
struct TtlPolicy {
    config: Arc<CacheConfig>,
}

impl Expiry<String, CacheEntry> for TtlPolicy {
    fn expire_after_create(
        &self,
        key: &String,
        _value: &CacheEntry,
        _current_time: Instant,
    ) -> Option<Duration> {
        Some(Duration::from_secs(self.config.ttl_for(key)))
    }

    fn expire_after_update(
        &self,
        key: &String,
        _value: &CacheEntry,
        _current_time: Instant,
        _current_duration: Option<Duration>,
    ) -> Option<Duration> {
        Some(Duration::from_secs(self.config.ttl_for(key)))
    }

    fn expire_after_read(
        &self,
        _key: &String,
        _value: &CacheEntry,
        _current_time: Instant,
        current_duration: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        // Don't extend TTL on read
        current_duration
    }
}

/// In-memory cache index backed by persistent storage
#[derive(Clone)]
pub struct CacheManager {
    storage: Arc<Storage>,
    index: Cache<String, CacheEntry>,
    loading: Arc<AtomicBool>,
}

impl CacheManager {
    /// Creates a new cache manager with the given storage and configuration
    pub async fn new(
        storage: Arc<Storage>,
        config: Arc<CacheConfig>,
        max_size: u64,
    ) -> anyhow::Result<Self> {
        storage.cleanup_temp().await?;

        let index = Self::build_index(storage.clone(), config, max_size);
        let loading = Arc::new(AtomicBool::new(true));

        let manager = Self {
            storage: storage.clone(),
            index: index.clone(),
            loading: loading.clone(),
        };

        // Background index loading
        tokio::spawn(Self::load_index_background(storage, index, loading));

        info!("Cache manager started, loading index in background");
        Ok(manager)
    }

    fn build_index(
        storage: Arc<Storage>,
        config: Arc<CacheConfig>,
        max_size: u64,
    ) -> Cache<String, CacheEntry> {
        // Convert to KB for capacity (moka uses weight-based eviction)
        let max_capacity_kb = (max_size / 1024).max(1);

        Cache::builder()
            .max_capacity(max_capacity_kb)
            .weigher(|_, entry: &CacheEntry| entry.weight())
            .expire_after(TtlPolicy { config })
            .eviction_listener(move |key: Arc<String>, _, cause| {
                if matches!(cause, RemovalCause::Expired | RemovalCause::Size) {
                    let storage = storage.clone();
                    let key = key.to_string();
                    tokio::spawn(async move {
                        if let Err(e) = storage.delete(&key).await {
                            debug!(key, error = %e, "Failed to delete evicted entry");
                        }
                    });
                }
            })
            .build()
    }

    async fn load_index_background(
        storage: Arc<Storage>,
        index: Cache<String, CacheEntry>,
        loading: Arc<AtomicBool>,
    ) {
        let start = Instant::now();

        match Self::load_entries(&storage, &index).await {
            Ok((loaded, total_size)) => {
                info!(
                    entries = loaded,
                    size_mb = total_size / (1024 * 1024),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "Cache index loaded"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to load cache entries");
            }
        }

        // Cleanup orphaned files after loading
        if let Err(e) = storage.cleanup_orphans().await {
            warn!(error = %e, "Failed to cleanup orphans");
        }

        loading.store(false, Ordering::Release);
    }

    async fn load_entries(
        storage: &Storage,
        index: &Cache<String, CacheEntry>,
    ) -> anyhow::Result<(usize, u64)> {
        let paths = storage.list_metadata_paths().await?;
        if paths.is_empty() {
            return Ok((0, 0));
        }

        let results: Vec<_> = futures::stream::iter(paths)
            .map(|path| {
                let storage = storage.clone();
                async move { storage.read_metadata_from_path(&path).await.ok().flatten() }
            })
            .buffer_unordered(LOAD_CONCURRENCY)
            .collect()
            .await;

        let mut loaded = 0usize;
        let mut total_size = 0u64;

        for meta in results.into_iter().flatten() {
            total_size = total_size.saturating_add(meta.size);
            index
                .insert(meta.key.clone(), CacheEntry::new(meta.size))
                .await;
            loaded += 1;
        }

        // Process any pending evictions
        index.run_pending_tasks().await;

        Ok((loaded, total_size))
    }

    /// Returns true if the cache index is still loading
    pub fn is_loading(&self) -> bool {
        self.loading.load(Ordering::Acquire)
    }

    /// Opens a cached file, returning file handle and metadata
    pub async fn open(&self, key: &str) -> Option<(File, Metadata)> {
        // Fast path: check index
        if self.index.get(key).await.is_some() {
            return self.open_from_storage(key).await;
        }

        // During loading, fallback to direct storage check
        if self.is_loading() {
            if let Some(result) = self.open_from_storage(key).await {
                // Add to index if found
                self.index
                    .insert(key.to_owned(), CacheEntry::new(result.1.size))
                    .await;
                return Some(result);
            }
        }

        None
    }

    async fn open_from_storage(&self, key: &str) -> Option<(File, Metadata)> {
        match self.storage.open(key).await {
            Ok(Some(v)) => Some(v),
            Ok(None) => {
                // File doesn't exist, remove from index
                self.index.invalidate(key).await;
                None
            }
            Err(e) => {
                debug!(key, error = %e, "Failed to open cached file");
                self.index.invalidate(key).await;
                None
            }
        }
    }

    /// Returns metadata without opening the file
    pub async fn get_metadata(&self, key: &str) -> Option<Metadata> {
        self.storage.get_metadata(key).await.ok().flatten()
    }

    /// Creates a temporary file for download
    pub async fn create_temp_data(&self) -> anyhow::Result<(PathBuf, File)> {
        self.storage.create_temp_data().await
    }

    /// Commits a downloaded file to cache
    pub async fn commit(
        &self,
        key: &str,
        temp_data: PathBuf,
        meta: &Metadata,
    ) -> anyhow::Result<()> {
        self.storage.put_from_temp_data(key, temp_data, meta).await?;
        self.index
            .insert(key.to_owned(), CacheEntry::new(meta.size))
            .await;
        Ok(())
    }

    /// Updates the timestamp for conditional requests (304 Not Modified)
    pub async fn touch(&self, key: &str) -> anyhow::Result<bool> {
        let touched = self.storage.touch(key).await?;
        if touched {
            // Refresh cache entry to reset TTL
            if let Some(meta) = self.get_metadata(key).await {
                self.index
                    .insert(key.to_owned(), CacheEntry::new(meta.size))
                    .await;
            }
        }
        Ok(touched)
    }

    /// Runs background maintenance tasks (eviction processing)
    pub async fn run_maintenance(&self) {
        self.index.run_pending_tasks().await;
    }

    /// Returns approximate number of entries in the cache
    pub fn entry_count(&self) -> u64 {
        self.index.entry_count()
    }

    /// Returns approximate weighted size of the cache in KB
    pub fn weighted_size(&self) -> u64 {
        self.index.weighted_size()
    }
}
