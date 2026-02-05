use crate::cache_policy::is_cache_valid;
use crate::config::CacheSettings;
use crate::storage::{CacheMetadata, Storage};
use crate::utils::format_size;
use moka::future::Cache;
use moka::Expiry;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tracing::{debug, info, info_span, warn, Instrument};

#[derive(Clone)]
struct CacheEntry {
    data_size: u64,
    meta_size: u64,
    metadata: Arc<CacheMetadata>,
    created_at: Instant,
}

impl CacheEntry {
    fn new(data_size: u64, meta_size: u64, metadata: CacheMetadata) -> Self {
        Self {
            data_size,
            meta_size,
            metadata: Arc::new(metadata),
            created_at: Instant::now(),
        }
    }

    #[inline]
    fn total_size(&self) -> u64 {
        self.data_size + self.meta_size
    }
}

/// Custom expiry based on cache settings and file type
struct CacheExpiry {
    settings: Arc<CacheSettings>,
}

impl Expiry<Arc<str>, CacheEntry> for CacheExpiry {
    fn expire_after_create(
        &self,
        key: &Arc<str>,
        _value: &CacheEntry,
        _current_time: Instant,
    ) -> Option<Duration> {
        let ttl_secs = self.settings.get_ttl_for_path(key);
        Some(Duration::from_secs(ttl_secs))
    }

    fn expire_after_read(
        &self,
        _key: &Arc<str>,
        _value: &CacheEntry,
        _current_time: Instant,
        _current_duration: Option<Duration>,
        _last_modified_at: Instant,
    ) -> Option<Duration> {
        // Не меняем TTL при чтении
        None
    }

    fn expire_after_update(
        &self,
        key: &Arc<str>,
        _value: &CacheEntry,
        _current_time: Instant,
        _current_duration: Option<Duration>,
    ) -> Option<Duration> {
        // Обновляем TTL при перезаписи
        let ttl_secs = self.settings.get_ttl_for_path(key);
        Some(Duration::from_secs(ttl_secs))
    }
}

#[derive(Clone)]
pub struct CacheManager {
    storage: Arc<Storage>,
    settings: Arc<CacheSettings>,
    cache: Cache<Arc<str>, CacheEntry>,
    max_size: u64,
    cleanup_running: Arc<AtomicBool>,
}

impl CacheManager {
    pub async fn new(
        storage: Arc<Storage>,
        settings: Arc<CacheSettings>,
        max_size: u64,
        max_entries: usize,
    ) -> anyhow::Result<Self> {
        let expiry = CacheExpiry {
            settings: settings.clone(),
        };

        // moka cache с size-based eviction и custom TTL
        let cache: Cache<Arc<str>, CacheEntry> = Cache::builder()
            .max_capacity(max_entries as u64)
            .weigher(|_key, entry: &CacheEntry| {
                // Weigher возвращает u32, масштабируем до KB
                (entry.total_size() / 1024).min(u32::MAX as u64) as u32
            })
            .expire_after(expiry)
            .eviction_listener(|key, entry, cause| {
                debug!(
                    key = %crate::logging::fields::path(&key),
                    size = %format_size(entry.total_size()),
                    cause = ?cause,
                    "Entry evicted from cache"
                );
            })
            .build();

        let manager = Self {
            storage,
            settings,
            cache,
            max_size,
            cleanup_running: Arc::new(AtomicBool::new(false)),
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
            let mut loaded = 0usize;
            let mut expired = 0usize;
            let mut total_size = 0u64;

            for (key, data_size, meta_size, metadata) in files {
                if !is_cache_valid(&metadata, &self.settings) {
                    debug!(
                        key = %crate::logging::fields::path(&key),
                        "Removing expired entry during initialization"
                    );
                    if let Err(e) = self.storage.delete(&key).await {
                        warn!(
                            key = %crate::logging::fields::path(&key),
                            error = %e,
                            "Failed to delete expired entry"
                        );
                    }
                    expired += 1;
                    continue;
                }

                let entry = CacheEntry::new(data_size, meta_size, metadata);
                total_size += entry.total_size();
                self.cache.insert(Arc::from(key.as_str()), entry).await;
                loaded += 1;
            }

            info!(
                entries = loaded,
                expired = expired,
                size = %format_size(total_size),
                max_size = %format_size(self.max_size),
                "Cache initialized"
            );
            Ok(())
        }
        .instrument(span)
        .await
    }

    pub async fn contains(&self, key: &str) -> bool {
        let key_arc = Arc::from(key);

        // moka автоматически проверяет TTL
        if let Some(entry) = self.cache.get(&key_arc).await {
            if is_cache_valid(&entry.metadata, &self.settings) {
                return true;
            }
            // Запись устарела по нашим правилам, удаляем
            self.cache.invalidate(&key_arc).await;
        }

        // Проверяем storage и восстанавливаем если найдено
        if let Ok(Some(stored)) = self.storage.open(key).await {
            if is_cache_valid(&stored.metadata, &self.settings) {
                let meta_size = self.storage.metadata_size(key).await.unwrap_or(0);
                let entry = CacheEntry::new(stored.size, meta_size, stored.metadata);
                self.cache.insert(key_arc, entry).await;

                debug!(
                    key = %crate::logging::fields::path(key),
                    "Restored entry to cache from storage"
                );
                return true;
            }
        }

        false
    }

    pub async fn mark_used(
        &self,
        key: &str,
        data_size: u64,
        meta_size: u64,
        metadata: CacheMetadata,
    ) {
        let entry = CacheEntry::new(data_size, meta_size, metadata);
        self.cache.insert(Arc::from(key), entry).await;
    }

    pub async fn get_or_load(&self, key: &str) -> Option<Arc<CacheMetadata>> {
        let key_arc = Arc::from(key);

        // Быстрый путь через moka
        if let Some(entry) = self.cache.get(&key_arc).await {
            if is_cache_valid(&entry.metadata, &self.settings) {
                return Some(entry.metadata.clone());
            }
        }

        // Загружаем из storage
        let stored = self.storage.open(key).await.ok()??;

        if !is_cache_valid(&stored.metadata, &self.settings) {
            return None;
        }

        let meta_size = self.storage.metadata_size(key).await.unwrap_or(0);
        let entry = CacheEntry::new(stored.size, meta_size, stored.metadata);
        let metadata = entry.metadata.clone();
        self.cache.insert(key_arc, entry).await;

        Some(metadata)
    }

    pub fn needs_cleanup(&self) -> bool {
        self.weighted_size() > self.max_size / 1024 // weigher считает в KB
    }

    /// Текущий размер кэша в байтах (приблизительно)
    fn weighted_size(&self) -> u64 {
        self.cache.weighted_size() * 1024
    }

    pub fn spawn_cleanup(&self) {
        if self
            .cleanup_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let storage = self.storage.clone();
        let settings = self.settings.clone();
        let cache = self.cache.clone();
        let max_size = self.max_size;
        let cleanup_running = self.cleanup_running.clone();

        tokio::spawn(
            async move {
                Self::cleanup_task(storage, settings, cache, max_size).await;
                cleanup_running.store(false, Ordering::Release);
            }
            .instrument(info_span!("cache_cleanup")),
        );
    }

    async fn cleanup_task(
        storage: Arc<Storage>,
        settings: Arc<CacheSettings>,
        cache: Cache<Arc<str>, CacheEntry>,
        max_size: u64,
    ) {
        // moka уже делает eviction автоматически, но мы можем принудительно удалить expired
        cache.run_pending_tasks().await;

        let current = cache.weighted_size() * 1024;
        if current <= max_size {
            return;
        }

        let target = (max_size as f64 * 0.8) as u64;
        info!(
            current = %format_size(current),
            target = %format_size(target),
            "Starting cache cleanup"
        );

        // Собираем ключи для удаления - сначала expired
        let mut to_remove: Vec<(Arc<str>, u64)> = Vec::new();
        let mut current_size = current;

        // Итерируемся по кэшу (moka поддерживает это)
        for (key, entry) in cache.iter() {
            if !is_cache_valid(&entry.metadata, &settings) {
                to_remove.push((key.clone(), entry.total_size()));
                current_size = current_size.saturating_sub(entry.total_size());
            }
        }

        // Если всё ещё превышаем лимит, удаляем по LRU (moka делает это сам при insert)
        // Принудительно вызываем sync для применения eviction
        cache.run_pending_tasks().await;

        // Удаляем файлы
        let count = to_remove.len();
        let mut actually_removed = 0u64;

        for (key, _expected_size) in &to_remove {
            cache.invalidate(key).await;
            if let Ok(removed) = storage.delete(key).await {
                actually_removed += removed;
            }
        }

        let final_size = cache.weighted_size() * 1024;
        info!(
            removed = count,
            freed = %format_size(actually_removed),
            final_size = %format_size(final_size),
            "Cache cleanup completed"
        );
    }

    pub fn spawn_expiry_checker(&self, interval_secs: u64) {
        let storage = self.storage.clone();
        let settings = self.settings.clone();
        let cache = self.cache.clone();

        tokio::spawn(
            async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(interval_secs));
                loop {
                    interval.tick().await;
                    Self::check_expired_entries(storage.clone(), settings.clone(), cache.clone())
                        .await;
                }
            }
            .instrument(info_span!("expiry_checker")),
        );
    }

    async fn check_expired_entries(
        storage: Arc<Storage>,
        settings: Arc<CacheSettings>,
        cache: Cache<Arc<str>, CacheEntry>,
    ) {
        // Запускаем pending tasks для применения TTL eviction
        cache.run_pending_tasks().await;

        let mut expired_keys: Vec<(Arc<str>, u64)> = Vec::new();

        for (key, entry) in cache.iter() {
            if !is_cache_valid(&entry.metadata, &settings) {
                expired_keys.push((key.clone(), entry.total_size()));
            }
        }

        if expired_keys.is_empty() {
            return;
        }

        debug!(count = expired_keys.len(), "Found expired entries");

        let mut removed_size = 0u64;
        for (key, _size) in &expired_keys {
            cache.invalidate(key).await;
            if let Ok(deleted) = storage.delete(key).await {
                removed_size += deleted;
            }
        }

        info!(
            expired = expired_keys.len(),
            freed = %format_size(removed_size),
            "Cleaned up expired entries"
        );
    }

    pub async fn stats(&self) -> CacheStats {
        // Запускаем pending tasks для актуальных данных
        self.cache.run_pending_tasks().await;

        CacheStats {
            size: self.cache.weighted_size() * 1024,
            max_size: self.max_size,
            entries: self.cache.entry_count() as usize,
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
        let usage_percent = if self.max_size > 0 {
            (self.size as f64 / self.max_size as f64 * 100.0) as u32
        } else {
            0
        };
        write!(
            f,
            "Cache: {}/{} ({}%) │ {} entries",
            format_size(self.size),
            format_size(self.max_size),
            usage_percent,
            self.entries
        )
    }
}