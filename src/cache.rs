use crate::config::CacheConfig;
use crate::storage::{Metadata, Storage};
use moka::future::Cache;
use moka::Expiry;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

struct Entry {
    size: u64,
    stored_at: u64,
}

struct TtlExpiry(Arc<CacheConfig>);

impl Expiry<Arc<str>, Entry> for TtlExpiry {
    fn expire_after_create(
        &self,
        key: &Arc<str>,
        _value: &Entry,
        _current_time: Instant,
    ) -> Option<Duration> {
        Some(Duration::from_secs(self.0.ttl_for(key)))
    }

    fn expire_after_update(
        &self,
        key: &Arc<str>,
        _value: &Entry,
        _current_time: Instant,
        _current_duration: Option<Duration>,
    ) -> Option<Duration> {
        Some(Duration::from_secs(self.0.ttl_for(key)))
    }

    fn expire_after_read(
        &self,
        _key: &Arc<str>,
        _value: &Entry,
        _current_time: Instant,
        current_duration: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        current_duration
    }
}

#[derive(Clone)]
pub struct CacheManager {
    storage: Arc<Storage>,
    config: Arc<CacheConfig>,
    index: Cache<Arc<str>, Entry>,
    max_size: u64,
}

impl CacheManager {
    pub async fn new(
        storage: Arc<Storage>,
        config: Arc<CacheConfig>,
        max_size: u64,
    ) -> anyhow::Result<Self> {
        storage.cleanup().await?;

        let index: Cache<Arc<str>, Entry> = Cache::builder()
            .max_capacity(100_000)
            .weigher(|_, e: &Entry| (e.size / 1024).min(u32::MAX as u64) as u32)
            .expire_after(TtlExpiry(config.clone()))
            .build();

        let entries = storage.list().await?;
        let mut total = 0u64;
        let mut loaded = 0usize;

        for (key, meta) in entries {
            let ttl = config.ttl_for(&key);
            if meta.age() < ttl {
                index
                    .insert(
                        Arc::from(key.as_str()),
                        Entry {
                            size: meta.size,
                            stored_at: meta.stored_at,
                        },
                    )
                    .await;
                total += meta.size;
                loaded += 1;
            } else {
                let _ = storage.delete(&key).await;
            }
        }

        info!(entries = loaded, size_mb = total / 1024 / 1024, "Cache initialized");

        Ok(Self {
            storage,
            config,
            index,
            max_size,
        })
    }

    pub async fn get(&self, key: &str) -> Option<(cacache::Reader, Metadata)> {
        if self.index.get(&Arc::from(key)).await.is_some() {
            if let Ok(Some(result)) = self.storage.get(key).await {
                return Some(result);
            }
            self.index.invalidate(&Arc::from(key)).await;
        }
        None
    }

    pub async fn get_metadata(&self, key: &str) -> Option<Metadata> {
        self.storage.get_metadata(key).await.ok().flatten()
    }

    pub async fn put(&self, key: &str, data: &[u8], meta: &Metadata) -> anyhow::Result<()> {
        self.storage.put(key, data, meta).await?;
        self.index
            .insert(
                Arc::from(key),
                Entry {
                    size: meta.size,
                    stored_at: meta.stored_at,
                },
            )
            .await;
        self.maybe_cleanup().await;
        Ok(())
    }

    async fn maybe_cleanup(&self) {
        self.index.run_pending_tasks().await;
        let size = self.index.weighted_size() * 1024;
        if size > self.max_size {
            debug!(
                size_mb = size / 1024 / 1024,
                max_mb = self.max_size / 1024 / 1024,
                "Cache exceeds limit, running GC"
            );
            let _ = self.storage.cleanup().await;
        }
    }
}