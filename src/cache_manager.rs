use crate::storage::Storage;
use std::{
    num::NonZeroUsize,
    sync::{atomic::{AtomicU64, Ordering}, Arc},
};
use tokio::sync::RwLock;
use tracing::{info, warn};

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
        info!("Initializing cache...");
        self.storage.cleanup_temp_files().await?;

        let files = self.storage.list_all().await?;
        let mut lru = self.lru.write().await;
        let mut total = 0u64;

        for (key, data_size, meta_size, _metadata) in files {
            let entry = CacheEntry { data_size, meta_size };
            total += entry.total_size();
            lru.put(Arc::from(key.as_str()), entry);
        }

        self.total_size.store(total, Ordering::Release);
        info!("Cache initialized: {} files, {}", lru.len(), crate::utils::format_size(total));
        Ok(())
    }

    pub async fn contains(&self, key: &str) -> bool {
        if self.lru.read().await.contains(&Arc::from(key)) {
            return true;
        }
        // Fallback: Check disk if not in LRU (e.g., just restarted)
        self.storage.open(key).await.ok().flatten().is_some()
    }

    pub async fn mark_used(&self, key: &str, data_size: u64, meta_size: u64) {
        let entry = CacheEntry { data_size, meta_size };
        let mut lru = self.lru.write().await;

        if let Some(old) = lru.put(Arc::from(key), entry.clone()) {
            let diff = entry.total_size() as i64 - old.total_size() as i64;
            if diff > 0 {
                self.total_size.fetch_add(diff as u64, Ordering::AcqRel);
            } else {
                self.total_size.fetch_sub(diff.abs() as u64, Ordering::AcqRel);
            }
        } else {
            self.total_size.fetch_add(entry.total_size(), Ordering::AcqRel);
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

        tokio::spawn(async move {
            Self::cleanup_task(storage, lru, total_size, max_size).await;
        });
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

        info!("Cache cleanup started: {} / {}", crate::utils::format_size(current), crate::utils::format_size(max_size));
        let target = (max_size as f64 * 0.8) as u64;

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

        let mut actually_removed = 0u64;
        for (key, expected_size) in to_remove {
            match storage.delete(&key).await {
                Ok(removed) => {
                    actually_removed += removed;
                    if removed != expected_size {
                        info!("Size mismatch for {}: expected {}, removed {}", key, expected_size, removed);
                    }
                }
                Err(e) => warn!("Failed to delete {}: {}", key, e),
            }
        }

        total_size.fetch_sub(actually_removed, Ordering::AcqRel);
        info!("Cleanup finished: removed {}", crate::utils::format_size(actually_removed));
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
        write!(
            f,
            "Cache Size: {} / {}\nLRU Entries: {}",
            crate::utils::format_size(self.size),
            crate::utils::format_size(self.max_size),
            self.entries
        )
    }
}