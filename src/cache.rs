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
use tracing::info;

#[derive(Clone)]
struct Entry {
    size: u64,
}

struct TtlExpiry {
    config: Arc<CacheConfig>,
}

impl TtlExpiry {
    fn new(config: Arc<CacheConfig>) -> Self {
        Self { config }
    }
}

impl Expiry<String, Entry> for TtlExpiry {
    fn expire_after_create(
        &self,
        key: &String,
        _value: &Entry,
        _current_time: Instant,
    ) -> Option<Duration> {
        Some(Duration::from_secs(self.config.ttl_for(key)))
    }

    fn expire_after_update(
        &self,
        key: &String,
        _value: &Entry,
        _current_time: Instant,
        _current_duration: Option<Duration>,
    ) -> Option<Duration> {
        Some(Duration::from_secs(self.config.ttl_for(key)))
    }

    fn expire_after_read(
        &self,
        _key: &String,
        _value: &Entry,
        _current_time: Instant,
        duration: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        duration
    }
}

#[derive(Clone)]
pub struct CacheManager {
    storage: Arc<Storage>,
    index: Cache<String, Entry>,
    #[allow(dead_code)]
    loading: Arc<AtomicBool>,
}

impl CacheManager {
    pub async fn new(
        storage: Arc<Storage>,
        config: Arc<CacheConfig>,
        max_size: u64,
    ) -> anyhow::Result<Self> {
        // Quick cleanup of temp files only - don't block on orphan cleanup
        storage.cleanup_temp().await?;

        let max_capacity_kb = (max_size / 1024).max(1);

        let storage_for_listener = storage.clone();
        let index: Cache<String, Entry> = Cache::builder()
            .max_capacity(max_capacity_kb)
            .weigher(|_, e: &Entry| {
                let size_kb = e.size / 1024;
                size_kb.max(1).min(u32::MAX as u64) as u32
            })
            .expire_after(TtlExpiry::new(config.clone()))
            .eviction_listener(move |k: Arc<String>, _v: Entry, cause: RemovalCause| {
                if !matches!(cause, RemovalCause::Expired | RemovalCause::Size) {
                    return;
                }
                let storage = storage_for_listener.clone();
                let key = k.as_str().to_owned();
                tokio::spawn(async move {
                    let _ = storage.delete(&key).await;
                });
            })
            .build();

        let loading = Arc::new(AtomicBool::new(true));

        let manager = Self {
            storage: storage.clone(),
            index: index.clone(),
            loading: loading.clone(),
        };

        // Spawn background loading task
        let bg_storage = storage.clone();
        let bg_config = config.clone();
        let bg_index = index.clone();

        tokio::spawn(async move {
            let start = std::time::Instant::now();

            match Self::load_entries_parallel(&bg_storage, &bg_config, &bg_index).await {
                Ok((loaded, expired, total_size)) => {
                    info!(
                        loaded,
                        expired,
                        size_mb = total_size / 1024 / 1024,
                        elapsed_ms = start.elapsed().as_millis() as u64,
                        "Cache index loaded"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to load cache entries");
                }
            }

            // Orphan cleanup in background after loading
            let _ = bg_storage.cleanup_orphans().await;

            loading.store(false, Ordering::Release);
        });

        info!("Cache manager started, loading index in background");
        Ok(manager)
    }

    async fn load_entries_parallel(
        storage: &Storage,
        config: &CacheConfig,
        index: &Cache<String, Entry>,
    ) -> anyhow::Result<(usize, usize, u64)> {
        // Get list of metadata file paths (fast directory scan)
        let paths = storage.list_metadata_paths().await?;
        let total_files = paths.len();

        if total_files == 0 {
            return Ok((0, 0, 0));
        }

        // Process files with parallel I/O
        const CONCURRENCY: usize = 64;

        let results: Vec<Option<Metadata>> = futures::stream::iter(paths)
            .map(|path| {
                let storage = storage.clone();
                async move { storage.read_metadata_from_path(&path).await.ok().flatten() }
            })
            .buffer_unordered(CONCURRENCY)
            .collect()
            .await;

        // Batch insert into index
        let mut loaded = 0usize;
        let mut expired = 0usize;
        let mut total_size = 0u64;

        // Collect valid entries first
        let mut valid_entries = Vec::with_capacity(results.len());

        for meta in results.into_iter().flatten() {
            let ttl = config.ttl_for(&meta.key);
            if meta.age() < ttl {
                valid_entries.push((meta.key, meta.size));
                loaded += 1;
            } else {
                expired += 1;
            }
        }

        // Batch insert - moka handles this efficiently
        for (key, size) in valid_entries {
            total_size += size;
            index.insert(key, Entry { size }).await;
        }

        // Run pending tasks to ensure insertions are processed
        index.run_pending_tasks().await;

        Ok((loaded, expired, total_size))
    }

    pub async fn open(&self, key: &str) -> Option<(File, Metadata)> {
        // Check index first (fast path)
        if self.index.get(key).await.is_none() {
            // If still loading, also check storage directly
            if self.loading.load(Ordering::Acquire) {
                // Fallback to direct storage check during loading
                if let Ok(Some((file, meta))) = self.storage.open(key).await {
                    // Insert into index for future lookups
                    self.index
                        .insert(key.to_owned(), Entry { size: meta.size })
                        .await;
                    return Some((file, meta));
                }
            }
            return None;
        }

        match self.storage.open(key).await {
            Ok(Some(v)) => Some(v),
            Ok(None) => {
                self.index.invalidate(key).await;
                None
            }
            Err(_) => {
                self.index.invalidate(key).await;
                None
            }
        }
    }

    pub async fn get_metadata(&self, key: &str) -> Option<Metadata> {
        self.storage.get_metadata(key).await.ok().flatten()
    }

    pub async fn create_temp_data(&self) -> anyhow::Result<(PathBuf, File)> {
        Ok(self.storage.create_temp_data().await?)
    }

    pub async fn commit_temp_data(
        &self,
        key: &str,
        temp_data: PathBuf,
        meta: &Metadata,
    ) -> anyhow::Result<()> {
        self.storage.put_from_temp_data(key, temp_data, meta).await?;
        self.index
            .insert(key.to_owned(), Entry { size: meta.size })
            .await;
        Ok(())
    }

    pub async fn touch(&self, key: &str) -> anyhow::Result<bool> {
        let touched = self.storage.touch(key).await?;
        if touched {
            if let Some(meta) = self.storage.get_metadata(key).await.ok().flatten() {
                self.index
                    .insert(key.to_owned(), Entry { size: meta.size })
                    .await;
            }
        }
        Ok(touched)
    }

    pub async fn run_maintenance(&self) {
        self.index.run_pending_tasks().await;
    }
}