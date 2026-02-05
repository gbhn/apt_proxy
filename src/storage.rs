use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs;
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(with = "http_serde::header_map")]
    pub headers: axum::http::HeaderMap,
    pub url: String,
    pub stored_at: u64,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl Metadata {
    pub fn from_response(resp: &reqwest::Response, url: &str, size: u64) -> Self {
        let h = resp.headers();
        Self {
            headers: h.clone(),
            url: url.into(),
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

fn header_str(
    h: &reqwest::header::HeaderMap,
    name: http::header::HeaderName,
) -> Option<String> {
    h.get(name).and_then(|v| v.to_str().ok()).map(Into::into)
}

pub struct Storage {
    path: PathBuf,
    temp_dir: PathBuf,
}

impl Storage {
    pub async fn new(path: PathBuf) -> Result<Self> {
        let temp_dir = path.join("tmp");
        fs::create_dir_all(&path).await?;
        fs::create_dir_all(&temp_dir).await?;
        debug!(path = %path.display(), "Storage initialized");
        Ok(Self { path, temp_dir })
    }

    pub async fn get(&self, key: &str) -> Result<Option<(cacache::Reader, Metadata)>> {
        let Some(entry) = cacache::metadata(&self.path, key).await? else {
            return Ok(None);
        };
        let Some(raw) = entry.raw_metadata else {
            return Ok(None);
        };
        let meta: Metadata = serde_json::from_slice(&raw)?;
        let reader = cacache::Reader::open(&self.path, entry.integrity).await?;
        Ok(Some((reader, meta)))
    }

    pub async fn get_metadata(&self, key: &str) -> Result<Option<Metadata>> {
        let Some(entry) = cacache::metadata(&self.path, key).await? else {
            return Ok(None);
        };
        entry
            .raw_metadata
            .map(|raw| serde_json::from_slice(&raw))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn put(&self, key: &str, data: &[u8], meta: &Metadata) -> Result<u64> {
        let meta_json = serde_json::to_vec(meta)?;
        cacache::write_with_opts(
            &self.path,
            key,
            data,
            cacache::WriteOpts::new().raw_metadata(meta_json),
        )
        .await?;
        debug!(key, size = data.len(), "Cached");
        Ok(data.len() as u64)
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        cacache::remove(&self.path, key).await?;
        debug!(key, "Deleted");
        Ok(())
    }

    pub async fn cleanup(&self) -> Result<()> {
        // Clean temp files
        if let Ok(mut entries) = fs::read_dir(&self.temp_dir).await {
            while let Ok(Some(e)) = entries.next_entry().await {
                let _ = fs::remove_file(e.path()).await;
            }
        }
        // Run cacache GC
        cacache::gc(&self.path).await?;
        debug!("Cleanup completed");
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<(String, Metadata)>> {
        let mut result = Vec::new();
        for entry in cacache::list_sync(&self.path) {
            let entry = entry?;
            if let Some(raw) = entry.raw_metadata {
                if let Ok(meta) = serde_json::from_slice(&raw) {
                    result.push((entry.key, meta));
                }
            }
        }
        Ok(result)
    }
}