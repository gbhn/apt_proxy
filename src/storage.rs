use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tracing::debug;

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(with = "http_serde::header_map")]
    pub headers: axum::http::HeaderMap,
    pub url: String,
    pub key: String,
    pub stored_at: u64,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl Metadata {
    pub fn from_response(resp: &reqwest::Response, url: &str, key: &str, size: u64) -> Self {
        let h = resp.headers();
        Self {
            headers: h.clone(),
            url: url.into(),
            key: key.into(),
            stored_at: now_secs(),
            size,
            etag: header_str(h, http::header::ETAG),
            last_modified: header_str(h, http::header::LAST_MODIFIED),
        }
    }

    pub fn age(&self) -> u64 {
        now_secs().saturating_sub(self.stored_at)
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn header_str(h: &reqwest::header::HeaderMap, name: http::header::HeaderName) -> Option<String> {
    h.get(name).and_then(|v| v.to_str().ok()).map(Into::into)
}

fn unique_id() -> u64 {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub struct Storage {
    base_path: PathBuf,
    temp_dir: PathBuf,
}

impl Storage {
    pub async fn new(path: PathBuf) -> Result<Self> {
        let temp_dir = path.join(".tmp");
        fs::create_dir_all(&path).await?;
        fs::create_dir_all(&temp_dir).await?;
        debug!(path = %path.display(), "Storage initialized");
        Ok(Self { base_path: path, temp_dir })
    }

    /// Генерирует путь к файлу данных
    fn data_path(&self, key: &str) -> PathBuf {
        let hash = blake3::hash(key.as_bytes());
        let hex = hash.to_hex();
        let h = hex.as_str();
        self.base_path.join(&h[0..2]).join(&h[2..4]).join(h)
    }

    /// Путь к файлу метаданных
    fn meta_path(&self, key: &str) -> PathBuf {
        let mut p = self.data_path(key);
        p.set_extension("json");
        p
    }

    pub async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Metadata)>> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        // Проверяем существование обоих файлов
        if !fs::try_exists(&data_path).await.unwrap_or(false) {
            return Ok(None);
        }
        if !fs::try_exists(&meta_path).await.unwrap_or(false) {
            return Ok(None);
        }

        let meta_bytes = fs::read(&meta_path).await?;
        let meta: Metadata = serde_json::from_slice(&meta_bytes)?;
        let data = fs::read(&data_path).await?;

        Ok(Some((data, meta)))
    }

    pub async fn get_metadata(&self, key: &str) -> Result<Option<Metadata>> {
        let meta_path = self.meta_path(key);

        if !fs::try_exists(&meta_path).await.unwrap_or(false) {
            return Ok(None);
        }

        let meta_bytes = fs::read(&meta_path).await?;
        let meta: Metadata = serde_json::from_slice(&meta_bytes)?;
        Ok(Some(meta))
    }

    pub async fn put(&self, key: &str, data: &[u8], meta: &Metadata) -> Result<u64> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        // Создаём директорию
        if let Some(parent) = data_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Атомарная запись через временные файлы
        let id = unique_id();
        let temp_data = self.temp_dir.join(format!("{}.data", id));
        let temp_meta = self.temp_dir.join(format!("{}.json", id));

        // Записываем данные
        let mut file = File::create(&temp_data).await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        drop(file);

        // Записываем метаданные
        let meta_json = serde_json::to_vec(meta)?;
        let mut file = File::create(&temp_meta).await?;
        file.write_all(&meta_json).await?;
        file.sync_all().await?;
        drop(file);

        // Атомарное переименование
        fs::rename(&temp_data, &data_path).await?;
        fs::rename(&temp_meta, &meta_path).await?;

        debug!(key, size = data.len(), "Cached");
        Ok(data.len() as u64)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let data_path = self.data_path(key);
        let meta_path = self.meta_path(key);

        let _ = fs::remove_file(&data_path).await;
        let _ = fs::remove_file(&meta_path).await;

        debug!(key, "Deleted from cache");
        Ok(())
    }

    pub async fn cleanup(&self) -> Result<()> {
        // Очищаем временные файлы
        if let Ok(mut entries) = fs::read_dir(&self.temp_dir).await {
            while let Ok(Some(e)) = entries.next_entry().await {
                let _ = fs::remove_file(e.path()).await;
            }
        }
        debug!("Temp files cleaned");
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<(String, Metadata)>> {
        let mut result = Vec::new();
        let mut stack = vec![self.base_path.clone()];

        while let Some(dir) = stack.pop() {
            let mut entries = match fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let file_type = match entry.file_type().await {
                    Ok(ft) => ft,
                    Err(_) => continue,
                };

                if file_type.is_dir() {
                    // Пропускаем .tmp
                    if path.file_name().map(|n| n.to_string_lossy().starts_with('.')).unwrap_or(false) {
                        continue;
                    }
                    stack.push(path);
                } else if path.extension().map(|e| e == "json").unwrap_or(false) {
                    if let Ok(bytes) = fs::read(&path).await {
                        if let Ok(meta) = serde_json::from_slice::<Metadata>(&bytes) {
                            result.push((meta.key.clone(), meta));
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Возвращает общий размер кэша
    pub async fn total_size(&self) -> u64 {
        let entries = match self.list().await {
            Ok(e) => e,
            Err(_) => return 0,
        };
        entries.iter().map(|(_, m)| m.size).sum()
    }
}