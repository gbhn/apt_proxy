use humansize::{SizeFormatter, BINARY};
use std::path::{Path, PathBuf};

/// Валидирует путь запроса
///
/// # Errors
/// Возвращает ошибку если путь содержит небезопасные компоненты
#[inline]
pub fn validate_path(path: &str) -> Result<(), crate::error::ProxyError> {
    let len = path.len();
    if len == 0 || len > 2048 {
        return Err(crate::error::ProxyError::InvalidPath(
            "Path length invalid".into(),
        ));
    }

    if memchr::memchr(0, path.as_bytes()).is_some() {
        return Err(crate::error::ProxyError::InvalidPath(
            "Path contains null byte".into(),
        ));
    }

    if path.contains("..") {
        return Err(crate::error::ProxyError::InvalidPath(
            "Path contains '..'".into(),
        ));
    }

    if path.contains("//") || path.starts_with('/') {
        return Err(crate::error::ProxyError::InvalidPath(
            "Invalid path format".into(),
        ));
    }

    Ok(())
}

/// Форматирует размер в человекочитаемый формат используя humansize crate
#[inline]
pub fn format_size(bytes: u64) -> String {
    SizeFormatter::new(bytes, BINARY).to_string()
}

/// Форматирует длительность в человекочитаемый формат используя humantime crate
#[inline]
pub fn format_duration(duration: std::time::Duration) -> String {
    humantime::format_duration(duration).to_string()
}

/// Форматирует секунды в человекочитаемый формат
#[inline]
pub fn format_duration_secs(seconds: u64) -> String {
    format_duration(std::time::Duration::from_secs(seconds))
}

/// Вычисляет путь в кэше для заданного URI.
///
/// Использует blake3 для хэширования.
#[inline]
pub fn cache_path_for(base_dir: &Path, uri_path: &str) -> PathBuf {
    let hash = blake3::hash(uri_path.as_bytes());
    let hex = hash.to_hex();
    let hex_str = hex.as_str();
    base_dir
        .join(&hex_str[0..2])
        .join(&hex_str[2..4])
        .join(hex_str)
}

/// Асинхронная версия cache_path_for для очень длинных путей
#[allow(dead_code)]
pub async fn cache_path_for_async(base_dir: PathBuf, uri_path: String) -> PathBuf {
    tokio::task::spawn_blocking(move || cache_path_for(&base_dir, &uri_path))
        .await
        .expect("blake3 hashing task panicked")
}

#[inline]
pub fn meta_path_for(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("meta")
}

#[inline]
pub fn encode_cache_key(key: &str) -> String {
    percent_encoding::utf8_percent_encode(key, percent_encoding::NON_ALPHANUMERIC).to_string()
}

/// Строит полный URL из базового URL и пути
#[inline]
pub fn build_upstream_url(base_url: &str, path: &str) -> Result<String, url::ParseError> {
    let base = url::Url::parse(base_url)?;
    let full = base.join(path)?;
    Ok(full.to_string())
}

/// Генерирует уникальный идентификатор (UUID v4)
#[inline]
pub fn generate_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Генерирует короткий уникальный идентификатор
#[inline]
pub fn generate_short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}