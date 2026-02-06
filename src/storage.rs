use crate::metrics;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tracing::{debug, info};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(with = "http_serde::header_map")]
    pub headers: axum::http::HeaderMap,
    pub url: String,
    pub key: String,
    pub stored_at: u64,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

impl Metadata {
    pub fn new(
        key: impl Into<String>,
        url: impl Into<String>,
        size: u64,
        headers: axum::http::HeaderMap,
    ) -> Self {
        Self {
            headers,
            url: url.into(),
            key: key.into(),
            stored_at: now_secs(),
            size,
            etag: None,
            last_modified: None,
        }
    }

    pub fn with_conditionals(mut self, etag: Option<String>, last_modified: Option<String>) -> Self {
        self.etag = etag;
        self.last_modified = last_modified;
        self
    }
}

#[derive(Clone)]
pub struct Storage {
    cache_path: PathBuf,
}

impl Storage {
    pub async fn new(base: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&base)
            .await
            .context("Failed to create cache directory")?;
        debug!(path = %base.display(), "Storage initialized with cacache");
        Ok(Self { cache_path: base })
    }

    /// Открыть закэшированный файл
    pub async fn open(&self, key: &str) -> Result<Option<(File, Metadata)>> {
        let start = Instant::now();
        let cache_path = self.cache_path.clone();
        let key_owned = key.to_string();

        let result = tokio::task::spawn_blocking(move || -> Result<Option<(Vec<u8>, Metadata)>> {
            // Читаем через cacache
            let data = match cacache::read_sync(&cache_path, &key_owned) {
                Ok(d) => d,
                Err(cacache::Error::EntryNotFound(_, _)) => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            // Получаем метаданные из индекса
            let entry = match cacache::index::find(&cache_path, &key_owned)? {
                Some(e) => e,
                None => return Ok(None),
            };

            let meta: Metadata = match &entry.metadata {
                serde_json::Value::Null => return Ok(None),
                v => serde_json::from_value(v.clone()).context("Failed to parse metadata")?,
            };

            Ok(Some((data, meta)))
        })
        .await??;

        match result {
            Some((data, meta)) => {
                // Записываем во временный файл и открываем
                let temp_path =
                    std::env::temp_dir().join(format!("apt-cache-{}.tmp", uuid::Uuid::new_v4()));
                tokio::fs::write(&temp_path, &data).await?;
                let file = File::open(&temp_path).await?;
                // Удаляем temp файл после открытия (он останется пока file handle открыт на Unix)
                let _ = tokio::fs::remove_file(&temp_path).await;

                metrics::record_storage_operation("open", start.elapsed(), true);
                metrics::record_storage_read(meta.size);
                Ok(Some((file, meta)))
            }
            None => Ok(None),
        }
    }

    /// Получить только metadata
    pub async fn get_metadata(&self, key: &str) -> Result<Option<Metadata>> {
        let cache_path = self.cache_path.clone();
        let key_owned = key.to_string();

        tokio::task::spawn_blocking(move || -> Result<Option<Metadata>> {
            match cacache::index::find(&cache_path, &key_owned)? {
                Some(entry) if entry.metadata != serde_json::Value::Null => {
                    let meta: Metadata = serde_json::from_value(entry.metadata)?;
                    Ok(Some(meta))
                }
                _ => Ok(None),
            }
        })
        .await?
    }

    /// Записать данные в кэш
    pub async fn put(&self, key: &str, data: &[u8], meta: &Metadata) -> Result<ssri::Integrity> {
        let start = Instant::now();
        let cache_path = self.cache_path.clone();
        let key_owned = key.to_string();
        let data = data.to_vec();
        let meta_json = serde_json::to_value(meta)?;
        let size = meta.size;

        let integrity = tokio::task::spawn_blocking(move || -> Result<ssri::Integrity> {
            // Сначала записываем данные
            let sri = cacache::write_sync(&cache_path, &key_owned, &data)?;

            // Затем обновляем метаданные в индексе
            cacache::index::insert(
                &cache_path,
                &key_owned,
                cacache::WriteOpts::new()
                    .integrity(sri.clone())
                    .metadata(meta_json)
                    .size(data.len()),
            )?;

            Ok(sri)
        })
        .await??;

        metrics::record_storage_operation("put", start.elapsed(), true);
        metrics::record_storage_write(size);
        debug!(key, size, hash = %integrity, "Cached");

        Ok(integrity)
    }

    /// Обновить timestamp (touch) для TTL refresh
    pub async fn touch(&self, key: &str) -> Result<bool> {
        let cache_path = self.cache_path.clone();
        let key_owned = key.to_string();

        tokio::task::spawn_blocking(move || -> Result<bool> {
            let entry = match cacache::index::find(&cache_path, &key_owned)? {
                Some(e) => e,
                None => return Ok(false),
            };

            let mut meta: Metadata = match serde_json::from_value(entry.metadata) {
                Ok(m) => m,
                Err(_) => return Ok(false),
            };

            meta.stored_at = now_secs();
            let meta_json = serde_json::to_value(&meta)?;

            // Обновляем только индекс с новыми метаданными
            cacache::index::insert(
                &cache_path,
                &key_owned,
                cacache::WriteOpts::new()
                    .integrity(entry.integrity)
                    .metadata(meta_json)
                    .size(entry.size),
            )?;

            Ok(true)
        })
        .await?
    }

    /// Удалить запись
    pub async fn delete(&self, key: &str) -> Result<()> {
        let start = Instant::now();
        let cache_path = self.cache_path.clone();
        let key_owned = key.to_string();
        let key_for_log = key.to_string();

        tokio::task::spawn_blocking(move || -> Result<()> {
            // ВАЖНО: обычный remove_sync удаляет только запись в индексе,
            // content на диске может остаться. Нужно remove_fully(true).
            match cacache::index::RemoveOpts::new()
                .remove_fully(true)
                .remove_sync(&cache_path, &key_owned)
            {
                Ok(()) => Ok(()),
                Err(cacache::Error::EntryNotFound(_, _)) => Ok(()), // идемпотентный delete
                Err(e) => Err(e.into()),
            }
        })
        .await??;

        metrics::record_storage_operation("delete", start.elapsed(), true);
        debug!(key = key_for_log, "Deleted");
        Ok(())
    }

    /// Garbage collection
    pub async fn gc(&self) -> Result<usize> {
        let start = Instant::now();
        let cache_path = self.cache_path.clone();

        let removed = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut removed = 0usize;

            // Итерируем по всем записям и проверяем их целостность
            for entry_result in cacache::list_sync(&cache_path) {
                let entry = match entry_result {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                // Проверяем что content существует и читается
                if cacache::read_sync(&cache_path, &entry.key).is_err() {
                    // Удаляем полностью (индекс + content), иначе будет утечка диска
                    let _ = cacache::index::RemoveOpts::new()
                        .remove_fully(true)
                        .remove_sync(&cache_path, &entry.key);
                    removed += 1;
                }
            }

            Ok(removed)
        })
        .await??;

        if removed > 0 {
            info!(removed, "GC completed");
            metrics::record_orphans_removed(removed as u64);
        }
        metrics::record_maintenance_run("gc", start.elapsed());

        Ok(removed)
    }

    /// Список всех записей
    pub async fn list_entries(&self) -> Result<Vec<Metadata>> {
        let cache_path = self.cache_path.clone();

        tokio::task::spawn_blocking(move || -> Result<Vec<Metadata>> {
            let entries: Vec<Metadata> = cacache::list_sync(&cache_path)
                .filter_map(|entry_result| {
                    entry_result.ok().and_then(|entry| serde_json::from_value::<Metadata>(entry.metadata).ok())
                })
                .collect();
            Ok(entries)
        })
        .await?
    }

    /// Проверить целостность
    pub async fn verify(&self, key: &str) -> Result<bool> {
        let cache_path = self.cache_path.clone();
        let key_owned = key.to_string();

        tokio::task::spawn_blocking(move || -> Result<bool> {
            match cacache::read_sync(&cache_path, &key_owned) {
                Ok(_) => Ok(true),
                Err(cacache::Error::EntryNotFound(_, _)) => Ok(false),
                Err(cacache::Error::IntegrityError(_)) => Ok(false),
                Err(e) => Err(e.into()),
            }
        })
        .await?
    }

    // Legacy compatibility
    pub async fn cleanup_temp(&self) -> Result<()> {
        Ok(())
    }

    pub async fn cleanup_orphans(&self) -> Result<usize> {
        self.gc().await
    }
}
