use crate::config::CacheConfig;
use crate::metrics;
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

const LOAD_CONCURRENCY: usize = 32;

#[derive(Clone, Copy, Debug)]
struct CacheEntry { size: u64 }

impl CacheEntry {
    fn weight(&self) -> u32 { 
        ((self.size / 1024).min(u32::MAX as u64).max(1)) as u32 
    }
}

struct TtlPolicy { config: Arc<CacheConfig> }

impl Expiry<String, CacheEntry> for TtlPolicy {
    fn expire_after_create(&self, key: &String, _: &CacheEntry, _: Instant) -> Option<Duration> {
        let ttl = self.config.ttl_for(key);
        metrics::record_ttl_applied(ttl, self.config.pattern_for(key));
        Some(Duration::from_secs(ttl))
    }

    fn expire_after_update(&self, key: &String, _: &CacheEntry, _: Instant, _: Option<Duration>) -> Option<Duration> {
        Some(Duration::from_secs(self.config.ttl_for(key)))
    }

    fn expire_after_read(&self, _: &String, _: &CacheEntry, _: Instant, dur: Option<Duration>, _: Instant) -> Option<Duration> {
        dur
    }
}

#[derive(Clone)]
pub struct CacheManager {
    storage: Arc<Storage>,
    index: Cache<String, CacheEntry>,
    loading: Arc<AtomicBool>,
}

impl CacheManager {
    pub async fn new(storage: Arc<Storage>, config: Arc<CacheConfig>, max_size: u64) -> anyhow::Result<Self> {
        storage.cleanup_temp().await?;
        
        let storage_clone = storage.clone();
        let index = Cache::builder()
            .max_capacity(max_size / 1024)
            .weigher(|_, e: &CacheEntry| e.weight())
            .expire_after(TtlPolicy { config })
            .eviction_listener(move |key: Arc<String>, entry, cause| {
                let reason = match cause {
                    RemovalCause::Expired => { metrics::record_ttl_expiration(); "expired" }
                    RemovalCause::Size => "size",
                    RemovalCause::Explicit => "explicit",
                    RemovalCause::Replaced => "replaced",
                };
                metrics::record_eviction(reason);
                debug!(key = key.as_str(), size = entry.size, reason, "Eviction");

                if matches!(cause, RemovalCause::Expired | RemovalCause::Size) {
                    let storage = storage_clone.clone();
                    let key = key.to_string();
                    tokio::spawn(async move { let _ = storage.delete(&key).await; });
                }
            })
            .build();

        let loading = Arc::new(AtomicBool::new(true));
        metrics::set_cache_loading(true);

        let manager = Self { storage: storage.clone(), index: index.clone(), loading: loading.clone() };
        
        tokio::spawn(async move {
            let start = Instant::now();
            match Self::load_entries(&storage, &index).await {
                Ok((count, size)) => {
                    info!(entries = count, size_mb = size / (1024 * 1024), "Index loaded");
                    metrics::set_cache_stats(size, count as u64, size / 1024);
                }
                Err(e) => warn!(error = %e, "Index load failed"),
            }
            let _ = storage.cleanup_orphans().await;
            loading.store(false, Ordering::Release);
            metrics::set_cache_loading(false);
            metrics::record_maintenance_run("index_load", start.elapsed());
        });

        Ok(manager)
    }

    async fn load_entries(storage: &Storage, index: &Cache<String, CacheEntry>) -> anyhow::Result<(usize, u64)> {
        let paths = storage.list_metadata_paths().await?;
        let results: Vec<_> = futures::stream::iter(paths)
            .map(|p| {
                let s = storage.clone();
                async move { s.read_metadata_from_path(&p).await.ok().flatten() }
            })
            .buffer_unordered(LOAD_CONCURRENCY)
            .collect()
            .await;

        let mut count = 0;
        let mut size = 0u64;
        for meta in results.into_iter().flatten() {
            size += meta.size;
            index.insert(meta.key.clone(), CacheEntry { size: meta.size }).await;
            count += 1;
        }
        index.run_pending_tasks().await;
        Ok((count, size))
    }

    pub fn is_loading(&self) -> bool { self.loading.load(Ordering::Acquire) }

    pub async fn open(&self, key: &str) -> Option<(File, Metadata)> {
        let start = Instant::now();
        
        let found = if self.index.get(key).await.is_some() {
            self.storage.open(key).await.ok().flatten()
        } else if self.is_loading() {
            let result = self.storage.open(key).await.ok().flatten();
            if let Some((_, ref m)) = result {
                self.index.insert(key.to_owned(), CacheEntry { size: m.size }).await;
            }
            result
        } else {
            None
        };

        metrics::record_cache_lookup(found.is_some(), start.elapsed());
        found
    }

    pub async fn get_metadata(&self, key: &str) -> Option<Metadata> {
        self.storage.get_metadata(key).await.ok().flatten()
    }

    pub async fn create_temp_data(&self) -> anyhow::Result<(PathBuf, File)> {
        self.storage.create_temp_data().await
    }

    pub async fn commit(&self, key: &str, temp_data: PathBuf, meta: &Metadata) -> anyhow::Result<()> {
        let index_clone = self.index.clone();
        tokio::spawn(async move {
            index_clone.run_pending_tasks().await;
        });

        self.storage.put_from_temp_data(key, temp_data, meta).await?;
        self.index.insert(key.to_owned(), CacheEntry { size: meta.size }).await;
        metrics::record_cache_operation("commit");
        self.update_stats().await;
        Ok(())
    }

    pub async fn touch(&self, key: &str) -> anyhow::Result<bool> {
        let touched = self.storage.touch(key).await?;
        if touched {
            if let Some(meta) = self.get_metadata(key).await {
                self.index.insert(key.to_owned(), CacheEntry { size: meta.size }).await;
            }
            metrics::record_ttl_refresh();
        }
        Ok(touched)
    }

    pub async fn run_maintenance(&self) {
        let start = Instant::now();
        self.index.run_pending_tasks().await;
        self.update_stats().await;
        let _ = self.storage.cleanup_temp().await;
        metrics::record_maintenance_run("maintenance", start.elapsed());
    }

    async fn update_stats(&self) {
        let kb = self.index.weighted_size();
        metrics::set_cache_stats(kb * 1024, self.index.entry_count(), kb);
        metrics::set_storage_space_used(kb * 1024);
    }

    pub fn entry_count(&self) -> u64 { self.index.entry_count() }
    pub fn weighted_size(&self) -> u64 { self.index.weighted_size() }
}