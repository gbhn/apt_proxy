use clap::Parser;
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf};
use tracing::info;

const DEFAULT_PORT: u16 = 3142;
const DEFAULT_CACHE_DIR: &str = "./apt_cache";
const DEFAULT_MAX_CACHE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
const DEFAULT_MAX_LRU_ENTRIES: usize = 100_000;

#[derive(Parser, Clone)]
#[command(author, version, about)]
pub struct Args {
    #[arg(long, short)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub port: Option<u16>,
    #[arg(long)]
    pub socket: Option<PathBuf>,
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,
    #[arg(long)]
    pub max_cache_size: Option<u64>,
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
        let s = s.trim().to_uppercase();
        let units = [("GB", 1_073_741_824), ("MB", 1_048_576), ("KB", 1024), ("B", 1)];
        
        units
            .iter()
            .find_map(|(suffix, mult)| {
                s.strip_suffix(suffix)
                    .and_then(|n| n.trim().parse::<u64>().ok())
                    .map(|n| n * mult)
            })
            .or_else(|| s.parse().ok())
    }

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
            return Ok(serde_yaml::from_str(&content)?);
        }

        for path in ["/etc/apt-cacher/config.yaml", "./config.yaml"] {
            if let Ok(content) = tokio::fs::read_to_string(path).await {
                if let Ok(config) = serde_yaml::from_str(&content) {
                    info!("Loaded config from {}", path);
                    return Ok(config);
                }
            }
        }

        Ok(ConfigFile::default())
    }

    pub fn display_info(&self) {
        use crate::utils::format_size;
        
        info!("Configuration:");
        info!("  Cache directory: {:?}", self.cache_dir);
        info!("  Max cache size: {}", format_size(self.max_cache_size));
        
        if self.repositories.is_empty() {
            tracing::warn!("No repositories configured - all requests will return 404");
        } else {
            info!("  Repositories: {}", self.repositories.len());
            for (key, url) in &self.repositories {
                tracing::debug!("    /{} -> {}", key, url);
            }
        }
    }
}