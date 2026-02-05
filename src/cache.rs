use crate::config::CacheConfig;
use crate::storage::{Metadata, Storage};
use moka::future::Cache;
use moka::Expiry;
use std::sync::Arc;
use std::time::{Duration, Instant};
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

impl Expiry<Arc<str>, Entry> for TtlExpiry {
    fn expire_after_create(
        &self,
        key: &Arc<str>,
        _value: &Entry,
        _current_time: Instant,
    ) -> Option<Duration> {
        Some(Duration::from_secs(self.config.ttl_for(key)))
    }

    fn expire_after_update(
        &self,
        key: &Arc<str>,
        _value: &Entry,
        _current_time: Instant,
        _current_duration: Option<Duration>,
    ) -> Option<Duration> {
        Some(Duration::from_secs(self.config.ttl_for(key)))
    }

    fn expire_after_read(
        &self,
        _key: &Arc<str>,
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
    index: Cache<Arc<str>, Entry>,
}

impl CacheManager {
    pub async fn new(
        storage: Arc<Storage>,
        config: Arc<CacheConfig>,
        max_size: u64,
    ) -> anyhow::Result<Self> {
        storage.cleanup().await?;

        // Ensure minimum capacity of 1KB
        let max_capacity_kb = (max_size / 1024).max(1);

        let index: Cache<Arc<str>, Entry> = Cache::builder()
            .max_capacity(max_capacity_kb)
            .weigher(|_, e: &Entry| {
                let size_kb = e.size / 1024;
                size_kb.max(1).min(u32::MAX as u64) as u32
            })
            .expire_after(TtlExpiry::new(config.clone()))
            .build();

        let entries = storage.list().await?;
        let mut total = 0u64;
        let mut loaded = 0usize;
        let mut expired = 0usize;

        for (key, meta) in entries {
            let ttl = config.ttl_for(&key);
            let age = meta.age();

            if age < ttl {
                index
                    .insert(Arc::from(key.as_str()), Entry { size: meta.size })
                    .await;
                total += meta.size;
                loaded += 1;
            } else {
                let _ = storage.delete(&key).await;
                expired += 1;
            }
        }

        info!(
            loaded,
            expired,
            size_mb = total / 1024 / 1024,
            "Cache initialized"
        );

        Ok(Self { storage, index })
    }

    pub async fn get(&self, key: &str) -> Option<(Vec<u8>, Metadata)> {
        let key_arc = Arc::from(key);

        // Check index first for fast path
        if self.index.get(&key_arc).await.is_none() {
            return None;
        }

        // Fetch from storage - handle race condition gracefully
        match self.storage.get(key).await {
            Ok(Some(result)) => Some(result),
            Ok(None) => {
                // Storage doesn't have it, remove from index
                self.index.invalidate(&key_arc).await;
                None
            }
            Err(_) => {
                // Error reading, invalidate to be safe
                self.index.invalidate(&key_arc).await;
                None
            }
        }
    }

    pub async fn get_metadata(&self, key: &str) -> Option<Metadata> {
        self.storage.get_metadata(key).await.ok().flatten()
    }

    pub async fn put(&self, key: &str, data: &[u8], meta: &Metadata) -> anyhow::Result<()> {
        self.storage.put(key, data, meta).await?;
        self.index
            .insert(Arc::from(key), Entry { size: meta.size })
            .await;
        Ok(())
    }

    pub async fn touch(&self, key: &str) -> anyhow::Result<bool> {
        let touched = self.storage.touch(key).await?;
        if touched {
            // Re-insert to reset TTL in the index
            if let Some(meta) = self.storage.get_metadata(key).await.ok().flatten() {
                self.index
                    .insert(Arc::from(key), Entry { size: meta.size })
                    .await;
            }
        }
        Ok(touched)
    }

    pub async fn run_maintenance(&self) {
        self.index.run_pending_tasks().await;
    }
}