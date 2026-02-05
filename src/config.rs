use anyhow::{Context, Result};
use bytesize::ByteSize;
use clap::Parser;
use figment::{
    providers::{Env, Format, Serialized, Yaml},
    Figment,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tracing::{info, warn};

/// Default configuration values
mod defaults {
    use std::time::Duration;

    pub const PORT: u16 = 3142;
    pub const MAX_CACHE_SIZE: u64 = 10 << 30; // 10 GB

    pub const TTL: Duration = Duration::from_secs(86400);      // 1 day
    pub const MIN_TTL: Duration = Duration::from_secs(3600);   // 1 hour
    pub const MAX_TTL: Duration = Duration::from_secs(604800); // 7 days
}

#[derive(Parser, Clone, Debug)]
#[command(author, version, about = "High-performance APT caching proxy")]
pub struct Args {
    /// Path to configuration file
    #[arg(long, short, env = "APT_CACHER_CONFIG")]
    pub config: Option<PathBuf>,

    /// Port to listen on
    #[arg(long, env = "APT_CACHER_PORT")]
    pub port: Option<u16>,

    /// Cache directory path
    #[arg(long, env = "APT_CACHER_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    /// Enable Prometheus metrics
    #[arg(long, env = "APT_CACHER_PROMETHEUS", default_value_t = false)]
    pub prometheus: bool,
}

/// Compiled TTL rule with validated regex pattern
#[derive(Debug, Clone)]
pub struct CompiledTtlRule {
    pub pattern: Regex,
    pub pattern_str: String,
    pub ttl: Duration,
}

/// Raw TTL rule from configuration file
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TtlRule {
    pub pattern: String,
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
}

impl TtlRule {
    /// Attempts to compile the pattern into a regex
    fn compile(&self) -> Option<CompiledTtlRule> {
        match Regex::new(&self.pattern) {
            Ok(regex) => Some(CompiledTtlRule {
                pattern: regex,
                pattern_str: self.pattern.clone(),
                ttl: self.ttl,
            }),
            Err(e) => {
                warn!(pattern = %self.pattern, error = %e, "Invalid TTL pattern, skipping");
                None
            }
        }
    }
}

/// Cache configuration with TTL policies
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub default_ttl: Duration,
    pub min_ttl: Duration,
    pub max_ttl: Duration,
    compiled_rules: Vec<CompiledTtlRule>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: defaults::TTL,
            min_ttl: defaults::MIN_TTL,
            max_ttl: defaults::MAX_TTL,
            compiled_rules: Vec::new(),
        }
    }
}

impl CacheConfig {
    /// Returns TTL for a path, clamped to min/max bounds
    pub fn ttl_for(&self, path: &str) -> u64 {
        let ttl = self
            .compiled_rules
            .iter()
            .find(|rule| rule.pattern.is_match(path))
            .map(|rule| rule.ttl.as_secs())
            .unwrap_or(self.default_ttl.as_secs());

        ttl.clamp(self.min_ttl.as_secs(), self.max_ttl.as_secs())
    }

    /// Returns the pattern name that matched the path (for metrics)
    pub fn pattern_for(&self, path: &str) -> &str {
        self.compiled_rules
            .iter()
            .find(|rule| rule.pattern.is_match(path))
            .map(|rule| rule.pattern_str.as_str())
            .unwrap_or("default")
    }

    /// Creates a new CacheConfig from raw configuration
    fn from_raw(raw: RawCacheConfig) -> Self {
        let mut config = Self {
            default_ttl: raw.default_ttl,
            min_ttl: raw.min_ttl,
            max_ttl: raw.max_ttl,
            compiled_rules: raw.ttl_rules.iter().filter_map(TtlRule::compile).collect(),
        };

        // Ensure min_ttl <= max_ttl
        if config.min_ttl > config.max_ttl {
            warn!(
                min = ?config.min_ttl,
                max = ?config.max_ttl,
                "min_ttl > max_ttl; swapping"
            );
            std::mem::swap(&mut config.min_ttl, &mut config.max_ttl);
        }

        info!(
            default_ttl = ?config.default_ttl,
            min_ttl = ?config.min_ttl,
            max_ttl = ?config.max_ttl,
            rules = config.compiled_rules.len(),
            "Cache TTL configuration loaded"
        );

        config
    }
}

/// Raw cache config for deserialization
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

fn default_ttl() -> Duration { defaults::TTL }
fn default_min_ttl() -> Duration { defaults::MIN_TTL }
fn default_max_ttl() -> Duration { defaults::MAX_TTL }

impl Default for RawCacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: defaults::TTL,
            min_ttl: defaults::MIN_TTL,
            max_ttl: defaults::MAX_TTL,
            ttl_rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct ConfigFile {
    port: Option<u16>,
    repositories: HashMap<String, String>,
    cache_dir: Option<PathBuf>,
    max_cache_size: Option<ByteSize>,
    #[serde(default)]
    cache: RawCacheConfig,
    prometheus: Option<bool>,
}

/// Application settings with validated configuration
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
    const CONFIG_PATHS: &'static [&'static str] = &[
        "/etc/apt-cacher/config.yaml",
        "./config.yaml",
    ];

    /// Loads settings from config file, environment, and CLI args
    pub fn load(args: Args) -> Result<Self> {
        let config_path = args.config.clone().or_else(|| {
            Self::CONFIG_PATHS.iter().map(PathBuf::from).find(|p| p.exists())
        });

        let mut figment = Figment::new().merge(Serialized::defaults(ConfigFile::default()));

        if let Some(ref path) = config_path {
            info!(path = %path.display(), "Loading config");
            figment = figment.merge(Yaml::file(path));
        }

        figment = figment.merge(Env::prefixed("APT_CACHER_").split("__"));

        let mut config: ConfigFile = figment
            .extract()
            .context("Failed to parse configuration")?;

        // CLI args override config file
        Self::apply_args(&mut config, &args);

        if config.repositories.is_empty() {
            warn!("No repositories configured");
        }

        Ok(Self {
            port: config.port.unwrap_or(defaults::PORT),
            repositories: config.repositories,
            cache_dir: config.cache_dir.unwrap_or_else(|| "./apt_cache".into()),
            max_cache_size: config
                .max_cache_size
                .map(|b| b.as_u64())
                .unwrap_or(defaults::MAX_CACHE_SIZE),
            cache: CacheConfig::from_raw(config.cache),
            prometheus: config.prometheus.unwrap_or(false),
        })
    }

    fn apply_args(config: &mut ConfigFile, args: &Args) {
        if let Some(port) = args.port {
            config.port = Some(port);
        }
        if let Some(ref dir) = args.cache_dir {
            config.cache_dir = Some(dir.clone());
        }
        if args.prometheus {
            config.prometheus = Some(true);
        }
    }
}