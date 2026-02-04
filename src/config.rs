use crate::logging::fields::size;
use clap::Parser;
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf};
use tracing::{info, warn};

const DEFAULT_PORT: u16 = 3142;
const DEFAULT_CACHE_DIR: &str = "./apt_cache";
const DEFAULT_MAX_CACHE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
const DEFAULT_MAX_LRU_ENTRIES: usize = 100_000;

#[derive(Parser, Clone)]
#[command(author, version, about = "High-performance APT caching proxy")]
pub struct Args {
    /// Path to configuration file
    #[arg(long, short)]
    pub config: Option<PathBuf>,

    /// TCP port to listen on
    #[arg(long)]
    pub port: Option<u16>,

    /// Unix socket path (overrides TCP port)
    #[arg(long)]
    pub socket: Option<PathBuf>,

    /// Cache directory path
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,

    /// Maximum cache size in bytes
    #[arg(long)]
    pub max_cache_size: Option<u64>,

    /// Maximum LRU cache entries
    #[arg(long)]
    pub max_lru_entries: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    pub port: Option<u16>,
    pub socket: Option<PathBuf>,
    pub repositories: Option<HashMap<String, String>>,
    pub cache_dir: Option<PathBuf>,
    pub max_cache_size: Option<u64>,
    #[serde(default)]
    pub max_cache_size_human: Option<String>,
    pub max_lru_entries: Option<usize>,
}

impl ConfigFile {
    pub fn parse_size(s: &str) -> Option<u64> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }

        if let Ok(n) = s.parse::<u64>() {
            return Some(n);
        }

        let bytes = s.as_bytes();
        let num_end = bytes
            .iter()
            .position(|&b| !b.is_ascii_digit() && b != b' ')
            .unwrap_or(bytes.len());

        let num: u64 = s[..num_end].trim().parse().ok()?;
        let suffix = s[num_end..].trim().to_ascii_uppercase();

        let multiplier = match suffix.as_str() {
            "GB" | "G" => 1_073_741_824,
            "MB" | "M" => 1_048_576,
            "KB" | "K" => 1024,
            "B" | "" => 1,
            _ => return None,
        };

        Some(num * multiplier)
    }

    #[inline]
    fn max_cache_size(&self) -> Option<u64> {
        self.max_cache_size_human
            .as_ref()
            .and_then(|s| Self::parse_size(s))
            .or(self.max_cache_size)
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub port: u16,
    pub socket: Option<PathBuf>,
    pub repositories: HashMap<String, String>,
    pub cache_dir: PathBuf,
    pub max_cache_size: u64,
    pub max_lru_entries: usize,
}

impl Settings {
    pub async fn load(args: Args) -> anyhow::Result<Self> {
        let config = Self::load_config_file(&args.config).await?;
        let config_max_cache_size = config.max_cache_size();

        Ok(Self {
            port: args.port.or(config.port).unwrap_or(DEFAULT_PORT),
            socket: args.socket.or(config.socket),
            repositories: config.repositories.unwrap_or_default(),
            cache_dir: args
                .cache_dir
                .or(config.cache_dir)
                .unwrap_or_else(|| DEFAULT_CACHE_DIR.into()),
            max_cache_size: args
                .max_cache_size
                .or(config_max_cache_size)
                .unwrap_or(DEFAULT_MAX_CACHE_SIZE),
            max_lru_entries: args
                .max_lru_entries
                .or(config.max_lru_entries)
                .unwrap_or(DEFAULT_MAX_LRU_ENTRIES),
        })
    }

    async fn load_config_file(path: &Option<PathBuf>) -> anyhow::Result<ConfigFile> {
        if let Some(path) = path {
            let content = tokio::fs::read_to_string(path).await?;
            info!(path = %path.display(), "Loaded configuration file");
            return Ok(serde_yaml::from_str(&content)?);
        }

        const CONFIG_PATHS: &[&str] = &["/etc/apt-cacher/config.yaml", "./config.yaml"];
        for path in CONFIG_PATHS {
            if let Ok(content) = tokio::fs::read_to_string(path).await {
                if let Ok(config) = serde_yaml::from_str(&content) {
                    info!(path = %path, "Loaded configuration file");
                    return Ok(config);
                }
            }
        }

        info!("Using default configuration");
        Ok(ConfigFile::default())
    }

    pub fn display_info(&self) {
        info!(
            path = %self.cache_dir.display(),
            max_size = %size(self.max_cache_size),
            max_entries = self.max_lru_entries,
            "Cache configuration"
        );

        if self.repositories.is_empty() {
            warn!("No repositories configured - all requests will fail!");
        } else {
            for (name, url) in &self.repositories {
                info!(name = %name, url = %url, "Repository configured");
            }
        }

        if let Some(ref socket) = self.socket {
            info!(socket = %socket.display(), "Unix socket mode");
        } else {
            info!(port = self.port, "TCP mode");
        }
    }
}