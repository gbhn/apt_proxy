use std::path::{Path, PathBuf};

pub fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "apt_proxy=info,tower_http=info".to_string()),
        )
        .init();
}

pub fn validate_path(path: &str) -> Result<(), crate::error::ProxyError> {
    if path.is_empty() || path.len() > 2048 {
        return Err(crate::error::ProxyError::InvalidPath(
            "Path length invalid".to_string(),
        ));
    }
    if path.contains('\0') || path.contains("..") {
        return Err(crate::error::ProxyError::InvalidPath(
            "Path contains invalid characters".to_string(),
        ));
    }
    Ok(())
}

pub fn format_size(bytes: u64) -> String {
    const UNITS: [(u64, &str); 4] = [
        (1_073_741_824, "GB"),
        (1_048_576, "MB"),
        (1024, "KB"),
        (1, "B"),
    ];

    for (divisor, unit) in UNITS {
        if bytes >= divisor {
            return if divisor == 1 {
                format!("{} {}", bytes, unit)
            } else {
                format!("{:.2} {}", bytes as f64 / divisor as f64, unit)
            };
        }
    }
    format!("{} B", bytes)
}

pub fn cache_path_for(base_dir: &Path, uri_path: &str) -> PathBuf {
    let hash = blake3::hash(uri_path.as_bytes());
    let hex = hash.to_hex();
    base_dir
        .join(&hex[0..2])
        .join(&hex[2..4])
        .join(hex.as_str())
}

pub fn headers_path_for(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("headers")
}

pub fn part_path_for(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("part")
}