use std::path::{Path, PathBuf};

/// Validates a URL path for security issues
#[inline]
pub fn validate_path(path: &str) -> Result<(), crate::error::ProxyError> {
    let len = path.len();
    if len == 0 || len > 2048 {
        return Err(crate::error::ProxyError::InvalidPath(
            "Path length invalid".into(),
        ));
    }

    // Check for null bytes
    if memchr::memchr(0, path.as_bytes()).is_some() {
        return Err(crate::error::ProxyError::InvalidPath(
            "Path contains null byte".into(),
        ));
    }

    // Check for path traversal
    if path.contains("..") {
        return Err(crate::error::ProxyError::InvalidPath(
            "Path contains '..'".into(),
        ));
    }

    // Check for other dangerous patterns
    if path.contains("//") || path.starts_with('/') {
        return Err(crate::error::ProxyError::InvalidPath(
            "Invalid path format".into(),
        ));
    }

    Ok(())
}

/// Formats bytes into human-readable size
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

/// Generates cache path from URI using BLAKE3 hash
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
pub fn headers_path_for(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("headers")
}

#[inline]
pub fn part_path_for(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("part")
}

/// URL-safe encoding for cache keys
#[inline]
pub fn encode_cache_key(key: &str) -> String {
    percent_encoding::utf8_percent_encode(key, percent_encoding::NON_ALPHANUMERIC).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_path_valid() {
        assert!(validate_path("ubuntu/pool/main/package.deb").is_ok());
        assert!(validate_path("debian/dists/stable/Release").is_ok());
    }

    #[test]
    fn test_validate_path_invalid() {
        assert!(validate_path("").is_err());
        assert!(validate_path("../etc/passwd").is_err());
        assert!(validate_path("path/../secret").is_err());
        assert!(validate_path("/absolute/path").is_err());
        assert!(validate_path("double//slash").is_err());
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0B");
        assert_eq!(format_size(512), "512B");
        assert_eq!(format_size(1024), "1.00KB");
        assert_eq!(format_size(1536), "1.50KB");
        assert_eq!(format_size(1_048_576), "1.00MB");
        assert_eq!(format_size(1_073_741_824), "1.00GB");
        assert_eq!(format_size(1_099_511_627_776), "1.00TB");
    }
}