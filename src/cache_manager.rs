use crate::cache_policy::{is_cache_valid, CachedEntry};
use crate::config::CacheSettings;
use crate::logging::fields::{self, size};
use crate::storage::{CacheMetadata, Storage};
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
    metadata: Arc<CacheMetadata>,
}

impl CacheEntry {
    #[inline]
    fn total_size(&self) -> u64 {
        self.data_size + self.meta_size
    }
}

pub struct CacheManager {
    storage: Arc<Storage>,
    settings: Arc<CacheSettings>,
    lru: Arc<RwLock<lru::LruCache<Arc<str>, CacheEntry>>>,
    total_size: Arc<AtomicU64>,
    max_size: u64,
}

impl CacheManager {
    pub async fn new(
        storage: Arc<Storage>,
        settings: Arc<CacheSettings>,
        max_size: u64,
        max_entries: usize,
    ) -> anyhow::Result<Self> {
        let manager = Self {
            storage,
            settings,
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
            let mut expired = 0usize;

            for (key, data_size, meta_size, metadata) in files {
                if !is_cache_valid(&metadata, &self.settings) {
                    debug!(
                        key = %fields::path(&key),
                        "Removing expired entry during initialization"
                    );
                    if let Err(e) = self.storage.delete(&key).await {
                        warn!(
                            key = %fields::path(&key),
                            error = %e,
                            "Failed to delete expired entry"
                        );
                    }
                    expired += 1;
                    continue;
                }

                let entry = CacheEntry {
                    data_size,
                    meta_size,
                    metadata: Arc::new(metadata),
                };
                total += entry.total_size();
                lru.put(Arc::from(key.as_str()), entry);
            }

            self.total_size.store(total, Ordering::Release);
            info!(
                entries = lru.len(),
                expired = expired,
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
        if let Some(entry) = self.lru.read().await.peek(&Arc::from(key)) {
            if is_cache_valid(&entry.metadata, &self.settings) {
                return true;
            }
            debug!(
                key = %fields::path(key),
                "Entry found but expired"
            );
            return false;
        }

        if let Ok(Some(stored)) = self.storage.open(key).await {
            if is_cache_valid(&stored.metadata, &self.settings) {
                return true;
            }
        }

        false
    }

    pub async fn mark_used(&self, key: &str, data_size: u64, meta_size: u64, metadata: CacheMetadata) {
        let entry = CacheEntry {
            data_size,
            meta_size,
            metadata: Arc::new(metadata),
        };
        let mut lru = self.lru.write().await;
        let key_arc = Arc::from(key);

        if let Some(existing) = lru.get(&key_arc) {
            let diff = entry.total_size() as i64 - existing.total_size() as i64;
            if diff != 0 {
                lru.put(key_arc, entry);
                if diff > 0 {
                    self.total_size.fetch_add(diff as u64, Ordering::AcqRel);
                } else {
                    self.total_size.fetch_sub(diff.unsigned_abs(), Ordering::AcqRel);
                }
            }
        } else {
            lru.put(key_arc, entry.clone());
            self.total_size.fetch_add(entry.total_size(), Ordering::AcqRel);
        }
    }

    pub fn needs_cleanup(&self) -> bool {
        self.total_size.load(Ordering::Acquire) > self.max_size
    }

    pub fn spawn_cleanup(&self) {
        let storage = self.storage.clone();
        let settings = self.settings.clone();
        let lru = self.lru.clone();
        let total_size = self.total_size.clone();
        let max_size = self.max_size;

        tokio::spawn(
            async move {
                Self::cleanup_task(storage, settings, lru, total_size, max_size).await;
            }
            .instrument(info_span!("cache_cleanup")),
        );
    }

    async fn cleanup_task(
        storage: Arc<Storage>,
        settings: Arc<CacheSettings>,
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
            let mut current_size = total_size.load(Ordering::Acquire);
            let mut list = Vec::new();

            let mut expired_keys = Vec::new();
            for (key, entry) in lru.iter() {
                if !is_cache_valid(&entry.metadata, &settings) {
                    expired_keys.push((key.clone(), entry.total_size()));
                }
            }

            for (key, size) in expired_keys {
                lru.pop(&key);
                list.push((key.to_string(), size));
                current_size = current_size.saturating_sub(size);
            }

            debug!(
                expired = list.len(),
                "Removed expired entries"
            );

            while current_size > target {
                if let Some((key, entry)) = lru.pop_lru() {
                    let entry_size = entry.total_size();
                    list.push((key.to_string(), entry_size));
                    current_size = current_size.saturating_sub(entry_size);
                } else {
                    break;
                }
            }

            info!(
                to_remove = list.len(),
                estimated_freed = %size(current.saturating_sub(current_size)),
                "Prepared cleanup list"
            );

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

        let final_size = total_size.load(Ordering::Acquire);
        info!(
            removed = count,
            freed = %size(actually_removed),
            errors = errors,
            final_size = %size(final_size),
            usage_percent = (final_size as f64 / max_size as f64 * 100.0) as u32,
            "Cache cleanup completed"
        );
    }

    pub fn spawn_expiry_checker(&self, interval_secs: u64) {
        let storage = self.storage.clone();
        let settings = self.settings.clone();
        let lru = self.lru.clone();
        let total_size = self.total_size.clone();

        tokio::spawn(
            async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
                loop {
                    interval.tick().await;
                    Self::check_expired_entries(storage.clone(), settings.clone(), lru.clone(), total_size.clone()).await;
                }
            }
            .instrument(info_span!("expiry_checker")),
        );
    }

    async fn check_expired_entries(
        storage: Arc<Storage>,
        settings: Arc<CacheSettings>,
        lru: Arc<RwLock<lru::LruCache<Arc<str>, CacheEntry>>>,
        total_size: Arc<AtomicU64>,
    ) {
        let expired_keys: Vec<_> = {
            let lru = lru.read().await;
            lru.iter()
                .filter(|(_, entry)| !is_cache_valid(&entry.metadata, &settings))
                .map(|(key, entry)| (key.clone(), entry.total_size()))
                .collect()
        };

        if expired_keys.is_empty() {
            return;
        }

        debug!(count = expired_keys.len(), "Found expired entries");

        let mut removed_size = 0u64;
        {
            let mut lru = lru.write().await;
            for (key, _size) in &expired_keys {
                lru.pop(key);
                if let Ok(deleted) = storage.delete(&key.to_string()).await {
                    removed_size += deleted;
                }
            }
        }

        total_size.fetch_sub(removed_size, Ordering::AcqRel);

        info!(
            expired = expired_keys.len(),
            freed = %size(removed_size),
            "Cleaned up expired entries"
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