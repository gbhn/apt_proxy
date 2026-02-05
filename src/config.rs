use anyhow::{Context, Result};
use bytesize::ByteSize;
use clap::Parser;
use figment::{providers::{Env, Format, Serialized, Yaml}, Figment};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tracing::{info, warn};

const DEFAULT_PORT: u16 = 3142;
const DEFAULT_MAX_CACHE_SIZE: u64 = 10 << 30;
const DEFAULT_TTL: Duration = Duration::from_secs(86400);
const DEFAULT_MIN_TTL: Duration = Duration::from_secs(3600);
const DEFAULT_MAX_TTL: Duration = Duration::from_secs(604800);

#[derive(Parser, Clone, Debug)]
#[command(author, version, about = "High-performance APT caching proxy")]
pub struct Args {
    #[arg(long, short, env = "APT_CACHER_CONFIG")]
    pub config: Option<PathBuf>,
    #[arg(long, env = "APT_CACHER_PORT")]
    pub port: Option<u16>,
    #[arg(long, env = "APT_CACHER_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,
    #[arg(long, env = "APT_CACHER_PROMETHEUS", default_value_t = false)]
    pub prometheus: bool,
    // FIXED: Bug #14 - Added flag to allow starting without repositories
    #[arg(long, env = "APT_CACHER_ALLOW_EMPTY", default_value_t = false)]
    pub allow_empty: bool,
}

#[derive(Debug, Clone)]
pub struct CompiledTtlRule {
    pub pattern: Regex,
    pub pattern_str: String,
    pub ttl: Duration,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TtlRule {
    pub pattern: String,
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub default_ttl: Duration,
    pub min_ttl: Duration,
    pub max_ttl: Duration,
    rules: Vec<CompiledTtlRule>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: DEFAULT_TTL,
            min_ttl: DEFAULT_MIN_TTL,
            max_ttl: DEFAULT_MAX_TTL,
            rules: Vec::new(),
        }
    }
}

impl CacheConfig {
    pub fn ttl_for(&self, path: &str) -> u64 {
        let ttl = self.rules.iter()
            .find(|r| r.pattern.is_match(path))
            .map(|r| r.ttl.as_secs())
            .unwrap_or(self.default_ttl.as_secs());
        ttl.clamp(self.min_ttl.as_secs(), self.max_ttl.as_secs())
    }

    pub fn pattern_for(&self, path: &str) -> &str {
        self.rules.iter()
            .find(|r| r.pattern.is_match(path))
            .map(|r| r.pattern_str.as_str())
            .unwrap_or("default")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct RawConfig {
    port: Option<u16>,
    #[serde(default)]
    repositories: HashMap<String, String>,
    cache_dir: Option<PathBuf>,
    max_cache_size: Option<ByteSize>,
    #[serde(default)]
    cache: RawCacheConfig,
    prometheus: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RawCacheConfig {
    #[serde(default = "default_ttl", with = "humantime_serde")]
    default_ttl: Duration,
    #[serde(default = "default_min_ttl", with = "humantime_serde")]
    min_ttl: Duration,
    #[serde(default = "default_max_ttl", with = "humantime_serde")]
    max_ttl: Duration,
    #[serde(default)]
    ttl_rules: Vec<TtlRule>,
}

fn default_ttl() -> Duration { DEFAULT_TTL }
fn default_min_ttl() -> Duration { DEFAULT_MIN_TTL }
fn default_max_ttl() -> Duration { DEFAULT_MAX_TTL }

impl Default for RawCacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: DEFAULT_TTL,
            min_ttl: DEFAULT_MIN_TTL,
            max_ttl: DEFAULT_MAX_TTL,
            ttl_rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub port: u16,
    pub repositories: HashMap<String, String>,
    pub cache_dir: PathBuf,
    pub max_cache_size: u64,
    pub cache: CacheConfig,
    pub prometheus: bool,
}

impl Settings {
    pub fn load(args: Args) -> Result<Self> {
        let config_path = args.config.clone().or_else(|| {
            ["/etc/apt-cacher/config.yaml", "./config.yaml"]
                .iter()
                .map(PathBuf::from)
                .find(|p| p.exists())
        });

        let mut figment = Figment::new().merge(Serialized::defaults(RawConfig::default()));

        if let Some(ref path) = config_path {
            info!(path = %path.display(), "Loading config");
            figment = figment.merge(Yaml::file(path));
        }

        let mut config: RawConfig = figment
            .merge(Env::prefixed("APT_CACHER_").split("__"))
            .extract()
            .context("Failed to parse configuration")?;

        // CLI overrides
        if let Some(port) = args.port { config.port = Some(port); }
        if let Some(ref dir) = args.cache_dir { config.cache_dir = Some(dir.clone()); }
        if args.prometheus { config.prometheus = Some(true); }

        // FIXED: Bug #14 - Return error if no repositories configured and not allowed
        if config.repositories.is_empty() {
            if !args.allow_empty {
                return Err(anyhow::anyhow!(
                    "No repositories configured. Add repositories to config or use --allow-empty flag."
                ));
            } else {
                warn!("No repositories configured - proxy will reject all requests");
            }
        }

        let cache = Self::build_cache_config(config.cache);

        Ok(Self {
            port: config.port.unwrap_or(DEFAULT_PORT),
            repositories: config.repositories,
            cache_dir: config.cache_dir.unwrap_or_else(|| "./apt_cache".into()),
            max_cache_size: config.max_cache_size.map(|b| b.as_u64()).unwrap_or(DEFAULT_MAX_CACHE_SIZE),
            cache,
            prometheus: config.prometheus.unwrap_or(false),
        })
    }

    fn build_cache_config(raw: RawCacheConfig) -> CacheConfig {
        let rules: Vec<_> = raw.ttl_rules.iter().filter_map(|r| {
            Regex::new(&r.pattern).ok().map(|re| CompiledTtlRule {
                pattern: re,
                pattern_str: r.pattern.clone(),
                ttl: r.ttl,
            })
        }).collect();

        let (min, max) = if raw.min_ttl > raw.max_ttl {
            warn!("min_ttl > max_ttl; swapping");
            (raw.max_ttl, raw.min_ttl)
        } else {
            (raw.min_ttl, raw.max_ttl)
        };

        info!(rules = rules.len(), "Cache config loaded");
        
        CacheConfig {
            default_ttl: raw.default_ttl,
            min_ttl: min,
            max_ttl: max,
            rules,
        }
    }
}