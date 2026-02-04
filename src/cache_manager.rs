use crate::logging::fields::{self, size};
use crate::storage::Storage;
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;
use tracing::{debug, info, info_span, warn, Instrument};

#[derive(Clone)]
struct CacheEntry {
    data_size: u64,
    meta_size: u64,
}

impl CacheEntry {
    #[inline]
    fn total_size(&self) -> u64 {
        self.data_size + self.meta_size
    }
}

pub struct CacheManager {
    storage: Arc<Storage>,
    lru: Arc<RwLock<lru::LruCache<Arc<str>, CacheEntry>>>,
    total_size: Arc<AtomicU64>,
    max_size: u64,
}

impl CacheManager {
    pub async fn new(
        storage: Arc<Storage>,
        max_size: u64,
        max_entries: usize,
    ) -> anyhow::Result<Self> {
        let manager = Self {
            storage,
            lru: Arc::new(RwLock::new(lru::LruCache::new(
                NonZeroUsize::new(max_entries).unwrap(),
            ))),
            total_size: Arc::new(AtomicU64::new(0)),
            max_size,
        };
        manager.initialize().await?;
        Ok(manager)
    }

    async fn initialize(&self) -> anyhow::Result<()> {
        let span = info_span!("cache_init");
        async {
            info!("Initializing cache index...");
            self.storage.cleanup_temp_files().await?;

            let files = self.storage.list_all().await?;
            let mut lru = self.lru.write().await;
            let mut total = 0u64;

            for (key, data_size, meta_size, _metadata) in files {
                let entry = CacheEntry {
                    data_size,
                    meta_size,
                };
                total += entry.total_size();
                lru.put(Arc::from(key.as_str()), entry);
            }

            self.total_size.store(total, Ordering::Release);
            info!(
                entries = lru.len(),
                size = %size(total),
                max_size = %size(self.max_size),
                "Cache initialized"
            );
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub async fn contains(&self, key: &str) -> bool {
        if self.lru.read().await.contains(&Arc::from(key)) {
            return true;
        }
        self.storage.open(key).await.ok().flatten().is_some()
    }

    pub async fn mark_used(&self, key: &str, data_size: u64, meta_size: u64) {
        let entry = CacheEntry {
            data_size,
            meta_size,
        };
        let mut lru = self.lru.write().await;

        if let Some(old) = lru.put(Arc::from(key), entry.clone()) {
            let diff = entry.total_size() as i64 - old.total_size() as i64;
            if diff > 0 {
                self.total_size.fetch_add(diff as u64, Ordering::AcqRel);
            } else {
                self.total_size
                    .fetch_sub(diff.unsigned_abs(), Ordering::AcqRel);
            }
        } else {
            self.total_size
                .fetch_add(entry.total_size(), Ordering::AcqRel);
        }
    }

    pub fn needs_cleanup(&self) -> bool {
        self.total_size.load(Ordering::Acquire) > self.max_size
    }

    pub fn spawn_cleanup(&self) {
        let storage = self.storage.clone();
        let lru = self.lru.clone();
        let total_size = self.total_size.clone();
        let max_size = self.max_size;

        tokio::spawn(
            async move {
                Self::cleanup_task(storage, lru, total_size, max_size).await;
            }
            .instrument(info_span!("cache_cleanup")),
        );
    }

    async fn cleanup_task(
        storage: Arc<Storage>,
        lru: Arc<RwLock<lru::LruCache<Arc<str>, CacheEntry>>>,
        total_size: Arc<AtomicU64>,
        max_size: u64,
    ) {
        let current = total_size.load(Ordering::Acquire);
        if current <= max_size {
            return;
        }

        let target = (max_size as f64 * 0.8) as u64;
        info!(
            current = %size(current),
            target = %size(target),
            "Starting cache cleanup"
        );

        let to_remove: Vec<_> = {
            let mut lru = lru.write().await;
            let mut current = total_size.load(Ordering::Acquire);
            let mut list = Vec::new();

            while current > target {
                if let Some((key, entry)) = lru.pop_lru() {
                    list.push((key.to_string(), entry.total_size()));
                    current = current.saturating_sub(entry.total_size());
                } else {
                    break;
                }
            }
            list
        };

        let count = to_remove.len();
        let mut actually_removed = 0u64;
        let mut errors = 0u32;

        for (key, expected_size) in to_remove {
            match storage.delete(&key).await {
                Ok(removed) => {
                    actually_removed += removed;
                    if removed != expected_size {
                        debug!(
                            key = %fields::path(&key),
                            expected = %size(expected_size),
                            actual = %size(removed),
                            "Size mismatch during cleanup"
                        );
                    }
                }
                Err(e) => {
                    errors += 1;
                    warn!(
                        key = %fields::path(&key),
                        error = %e,
                        "Failed to delete cache entry"
                    );
                }
            }
        }

        total_size.fetch_sub(actually_removed, Ordering::AcqRel);
        info!(
            removed = count,
            freed = %size(actually_removed),
            errors = errors,
            "Cache cleanup completed"
        );
    }

    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.total_size.load(Ordering::Acquire),
            max_size: self.max_size,
            entries: self.lru.read().await.len(),
        }
    }
}

pub struct CacheStats {
    pub size: u64,
    pub max_size: u64,
    pub entries: usize,
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let usage_percent = (self.size as f64 / self.max_size as f64 * 100.0) as u32;
        write!(
            f,
            "Cache: {}/{} ({}%) │ {} entries",
            size(self.size),
            size(self.max_size),
            usage_percent,
            self.entries
        )
    }
}