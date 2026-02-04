use std::path::{Path, PathBuf};
use tracing_subscriber::{fmt, EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("apt_cacher_rs=info,tower_http=info,hyper=warn"));
    
    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_line_number(false)
                .with_level(true)
                .compact()
        )
        .init();
    
    tracing::info!("APT Cacher RS started");
}

#[inline]
pub fn validate_path(path: &str) -> Result<(), crate::error::ProxyError> {
    let len = path.len();
    if len == 0 || len > 2048 {
        return Err(crate::error::ProxyError::InvalidPath("Path length invalid".into()));
    }
    
    if memchr::memchr(0, path.as_bytes()).is_some() {
        return Err(crate::error::ProxyError::InvalidPath("Path contains null byte".into()));
    }
    
    if path.contains("..") {
        return Err(crate::error::ProxyError::InvalidPath("Path contains '..'".into()));
    }
    Ok(())
}

#[inline]
pub fn format_size(bytes: u64) -> String {
    const GB: u64 = 1_073_741_824;
    const MB: u64 = 1_048_576;
    const KB: u64 = 1024;

    if bytes >= GB {
        format!("{:.2}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

#[inline]
pub fn cache_path_for(base_dir: &Path, uri_path: &str) -> PathBuf {
    let hash = blake3::hash(uri_path.as_bytes());
    let hex = hash.to_hex();
    base_dir
        .join(&hex.as_str()[0..2])
        .join(&hex.as_str()[2..4])
        .join(hex.as_str())
}

#[inline]
pub fn headers_path_for(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("headers")
}

#[inline]
pub fn part_path_for(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("part")
}