use std::path::{Path, PathBuf};

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

#[inline]
pub fn format_size(bytes: u64) -> String {
    const UNITS: &[(u64, &str)] = &[
        (1_099_511_627_776, "TB"),
        (1_073_741_824, "GB"),
        (1_048_576, "MB"),
        (1024, "KB"),
    ];

    for &(threshold, unit) in UNITS {
        if bytes >= threshold {
            return format!("{:.2}{}", bytes as f64 / threshold as f64, unit);
        }
    }
    format!("{}B", bytes)
}

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

#[inline]
pub fn meta_path_for(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("meta")
}

#[inline]
pub fn encode_cache_key(key: &str) -> String {
    percent_encoding::utf8_percent_encode(key, percent_encoding::NON_ALPHANUMERIC).to_string()
}