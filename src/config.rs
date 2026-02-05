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

const DEFAULT_PORT: u16 = 3142;
const DEFAULT_PROMETHEUS_PORT: u16 = 9090;
const DEFAULT_MAX_CACHE_SIZE: u64 = 10 << 30; // 10 GB

const DEFAULT_TTL_SECS: u64 = 86400;      // 1 day
const DEFAULT_MIN_TTL_SECS: u64 = 3600;   // 1 hour
const DEFAULT_MAX_TTL_SECS: u64 = 604800; // 7 days

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

    /// Prometheus metrics port
    #[arg(long, env = "APT_CACHER_PROMETHEUS_PORT")]
    pub prometheus_port: Option<u16>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TtlRule {
    pub pattern: String,
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
    #[serde(skip)]
    regex: Option<Regex>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    #[serde(default = "default_ttl", with = "humantime_serde")]
    pub default_ttl: Duration,

    #[serde(default = "default_min_ttl", with = "humantime_serde")]
    pub min_ttl: Duration,

    #[serde(default = "default_max_ttl", with = "humantime_serde")]
    pub max_ttl: Duration,

    #[serde(default)]
    pub ttl_rules: Vec<TtlRule>,
}

fn default_ttl() -> Duration { Duration::from_secs(DEFAULT_TTL_SECS) }
fn default_min_ttl() -> Duration { Duration::from_secs(DEFAULT_MIN_TTL_SECS) }
fn default_max_ttl() -> Duration { Duration::from_secs(DEFAULT_MAX_TTL_SECS) }

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: default_ttl(),
            min_ttl: default_min_ttl(),
            max_ttl: default_max_ttl(),
            ttl_rules: Vec::new(),
        }
    }
}

impl CacheConfig {
    /// Returns TTL for a path, clamped to min/max bounds
    pub fn ttl_for(&self, path: &str) -> u64 {
        let ttl = self.ttl_rules
            .iter()
            .find(|rule| rule.regex.as_ref().is_some_and(|re| re.is_match(path)))
            .map(|rule| rule.ttl.as_secs())
            .unwrap_or(self.default_ttl.as_secs());

        ttl.clamp(self.min_ttl.as_secs(), self.max_ttl.as_secs())
    }

    /// Compiles regex patterns, removing invalid ones
    pub fn compile_patterns(&mut self) {
        self.ttl_rules.retain_mut(|rule| {
            match Regex::new(&rule.pattern) {
                Ok(re) => {
                    rule.regex = Some(re);
                    true
                }
                Err(e) => {
                    warn!(pattern = %rule.pattern, error = %e, "Invalid TTL pattern");
                    false
                }
            }
        });
    }

    /// Ensures min_ttl <= max_ttl
    pub fn validate(&mut self) {
        if self.min_ttl > self.max_ttl {
            warn!(min = ?self.min_ttl, max = ?self.max_ttl, "min_ttl > max_ttl; swapping");
            std::mem::swap(&mut self.min_ttl, &mut self.max_ttl);
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
    cache: CacheConfig,
    prometheus: Option<bool>,
    prometheus_port: Option<u16>,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub port: u16,
    pub repositories: HashMap<String, String>,
    pub cache_dir: PathBuf,
    pub max_cache_size: u64,
    pub cache: CacheConfig,
    pub prometheus: bool,
    pub prometheus_port: u16,
}

impl Settings {
    const CONFIG_PATHS: &'static [&'static str] = &["/etc/apt-cacher/config.yaml", "./config.yaml"];

    pub fn load(args: Args) -> anyhow::Result<Self> {
        let config_path = args.config.clone().or_else(|| {
            Self::CONFIG_PATHS.iter().map(PathBuf::from).find(|p| p.exists())
        });

        let mut figment = Figment::new().merge(Serialized::defaults(ConfigFile::default()));

        if let Some(ref path) = config_path {
            info!(path = %path.display(), "Loading config");
            figment = figment.merge(Yaml::file(path));
        }

        figment = figment.merge(Env::prefixed("APT_CACHER_").split("__"));

        let mut config: ConfigFile = figment.extract()?;

        // CLI args override config file
        Self::apply_args(&mut config, &args);

        // Validate and compile
        config.cache.compile_patterns();
        config.cache.validate();

        if config.repositories.is_empty() {
            warn!("No repositories configured");
        }

        Ok(Self {
            port: config.port.unwrap_or(DEFAULT_PORT),
            repositories: config.repositories,
            cache_dir: config.cache_dir.unwrap_or_else(|| "./apt_cache".into()),
            max_cache_size: config.max_cache_size.map(|b| b.as_u64()).unwrap_or(DEFAULT_MAX_CACHE_SIZE),
            cache: config.cache,
            prometheus: config.prometheus.unwrap_or(false),
            prometheus_port: config.prometheus_port.unwrap_or(DEFAULT_PROMETHEUS_PORT),
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
        if let Some(port) = args.prometheus_port {
            config.prometheus_port = Some(port);
        }
    }
}