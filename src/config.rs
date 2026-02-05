use crate::logging::fields::size;
use clap::Parser;
use regex::Regex;
use serde::Deserialize;
use serde_yml as serde_yaml;
use std::{collections::HashMap, path::PathBuf};
use tracing::{info, warn};

const DEFAULT_PORT: u16 = 3142;
const DEFAULT_CACHE_DIR: &str = "./apt_cache";
const DEFAULT_MAX_CACHE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
const DEFAULT_MAX_LRU_ENTRIES: usize = 100_000;
const DEFAULT_TTL: u64 = 86400;
const DEFAULT_MIN_TTL: u64 = 3600;
const DEFAULT_MAX_TTL: u64 = 604800;
const DEFAULT_STALE_WHILE_REVALIDATE: u64 = 3600;

#[derive(Parser, Clone)]
#[command(author, version, about = "High-performance APT caching proxy")]
pub struct Args {
    #[arg(long, short, env = "APT_CACHER_CONFIG")]
    pub config: Option<PathBuf>,

    #[arg(long, env = "APT_CACHER_PORT")]
    pub port: Option<u16>,

    #[arg(long, env = "APT_CACHER_SOCKET")]
    pub socket: Option<PathBuf>,

    #[arg(long, env = "APT_CACHER_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    #[arg(long, env = "APT_CACHER_MAX_CACHE_SIZE")]
    pub max_cache_size: Option<u64>,

    #[arg(long, env = "APT_CACHER_MAX_LRU_ENTRIES")]
    pub max_lru_entries: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TtlOverride {
    pub pattern: String,
    pub ttl: u64,
    #[serde(skip)]
    pub regex: Option<Regex>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ValidationSettings {
    #[serde(default = "default_true")]
    pub use_etag: bool,
    #[serde(default = "default_true")]
    pub use_last_modified: bool,
    #[serde(default)]
    pub always_revalidate: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CacheSettings {
    #[serde(default = "default_ttl")]
    pub default_ttl: u64,
    #[serde(default = "default_min_ttl")]
    pub min_ttl: u64,
    #[serde(default = "default_max_ttl")]
    pub max_ttl: u64,
    #[serde(default)]
    pub ignore_cache_control: bool,
    #[serde(default = "default_stale_while_revalidate")]
    pub stale_while_revalidate: u64,
    #[serde(default)]
    pub ttl_overrides: Vec<TtlOverride>,
    #[serde(default)]
    pub validation: ValidationSettings,
}

fn default_true() -> bool {
    true
}

fn default_ttl() -> u64 {
    DEFAULT_TTL
}

fn default_min_ttl() -> u64 {
    DEFAULT_MIN_TTL
}

fn default_max_ttl() -> u64 {
    DEFAULT_MAX_TTL
}

fn default_stale_while_revalidate() -> u64 {
    DEFAULT_STALE_WHILE_REVALIDATE
}

impl CacheSettings {
    pub fn get_ttl_for_path(&self, path: &str) -> u64 {
        for override_rule in &self.ttl_overrides {
            if let Some(regex) = &override_rule.regex {
                if regex.is_match(path) {
                    return self.clamp_ttl(override_rule.ttl);
                }
            }
        }
        self.clamp_ttl(self.default_ttl)
    }

    #[inline]
    fn clamp_ttl(&self, ttl: u64) -> u64 {
        ttl.clamp(self.min_ttl, self.max_ttl)
    }

    pub fn compile_patterns(&mut self) -> anyhow::Result<()> {
        for override_rule in &mut self.ttl_overrides {
            match Regex::new(&override_rule.pattern) {
                Ok(regex) => {
                    override_rule.regex = Some(regex);
                    info!(
                        pattern = %override_rule.pattern,
                        ttl = override_rule.ttl,
                        "Compiled TTL override pattern"
                    );
                }
                Err(e) => {
                    warn!(
                        pattern = %override_rule.pattern,
                        error = %e,
                        "Failed to compile TTL pattern, skipping"
                    );
                }
            }
        }
        Ok(())
    }
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
    #[serde(default)]
    pub cache: CacheSettings,
}

impl ConfigFile {
    pub fn parse_size(s: &str) -> Option<u64> {
        let s = s.trim();
        if s.is_empty() { return None; }
        if let Ok(n) = s.parse::<u64>() { return Some(n); }
        let end = s.find(|c: char| !c.is_ascii_digit() && c != ' ').unwrap_or(s.len());
        let (num_str, suffix) = s.split_at(end);
        let num: u64 = num_str.trim().parse().ok()?;
        let mul = match suffix.trim().to_ascii_uppercase().as_str() {
            "GB" | "G" | "GIB" => 1 << 30,
            "MB" | "M" | "MIB" => 1 << 20,
            "KB" | "K" | "KIB" => 1 << 10,
            "B" | "" => 1,
            _ => return None,
        };
        Some(num * mul)
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
    pub cache: CacheSettings,
}

impl Settings {
    pub async fn load(args: Args) -> anyhow::Result<Self> {
        let mut config = Self::load_config_file(&args.config).await?;
        let config_max_cache_size = config.max_cache_size();

        config.cache.compile_patterns()?;

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
            cache: config.cache,
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

        info!(
            default_ttl = %format_duration(self.cache.default_ttl),
            min_ttl = %format_duration(self.cache.min_ttl),
            max_ttl = %format_duration(self.cache.max_ttl),
            stale_while_revalidate = %format_duration(self.cache.stale_while_revalidate),
            ignore_cache_control = self.cache.ignore_cache_control,
            "TTL settings"
        );

        info!(
            use_etag = self.cache.validation.use_etag,
            use_last_modified = self.cache.validation.use_last_modified,
            always_revalidate = self.cache.validation.always_revalidate,
            "Validation settings"
        );

        if !self.cache.ttl_overrides.is_empty() {
            info!(
                count = self.cache.ttl_overrides.len(),
                "TTL overrides configured"
            );
        }

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

fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86400 {
        format!("{}h", seconds / 3600)
    } else {
        format!("{}d", seconds / 86400)
    }
}