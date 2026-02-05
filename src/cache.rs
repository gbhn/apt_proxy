use crate::config::CacheConfig;
use crate::storage::{Metadata, Storage};
use moka::future::Cache;
use moka::notification::RemovalCause;
use moka::Expiry;
use std::path::PathBuf;
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
}

impl CacheManager {
    pub async fn new(
        storage: Arc<Storage>,
        config: Arc<CacheConfig>,
        max_size: u64,
    ) -> anyhow::Result<Self> {
        storage.cleanup().await?;

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

        let entries = storage.list().await?;
        let mut total = 0u64;
        let mut loaded = 0usize;
        let mut expired = 0usize;

        for (key, meta) in entries {
            let ttl = config.ttl_for(&key);
            let age = meta.age();

            if age < ttl {
                index.insert(key.clone(), Entry { size: meta.size }).await;
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

    pub async fn open(&self, key: &str) -> Option<(File, Metadata)> {
        if self.index.get(key).await.is_none() {
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