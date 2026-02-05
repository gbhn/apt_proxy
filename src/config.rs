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

#[derive(Parser, Clone, Debug)]
#[command(author, version, about = "High-performance APT caching proxy")]
pub struct Args {
    #[arg(long, short, env = "APT_CACHER_CONFIG")]
    pub config: Option<PathBuf>,

    #[arg(long, env = "APT_CACHER_PORT")]
    pub port: Option<u16>,

    #[arg(long, env = "APT_CACHER_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    #[arg(long, env = "APT_CACHER_PROMETHEUS", default_value = "false")]
    pub prometheus: bool,

    #[arg(long, env = "APT_CACHER_PROMETHEUS_PORT", default_value = "9090")]
    pub prometheus_port: u16,
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

fn default_ttl() -> Duration {
    Duration::from_secs(86400)
}

fn default_min_ttl() -> Duration {
    Duration::from_secs(3600)
}

fn default_max_ttl() -> Duration {
    Duration::from_secs(604800)
}

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
    pub fn ttl_for(&self, path: &str) -> u64 {
        for rule in &self.ttl_rules {
            if let Some(ref re) = rule.regex {
                if re.is_match(path) {
                    return self.clamp(rule.ttl.as_secs());
                }
            }
        }
        self.clamp(self.default_ttl.as_secs())
    }

    fn clamp(&self, ttl: u64) -> u64 {
        ttl.clamp(self.min_ttl.as_secs(), self.max_ttl.as_secs())
    }

    pub fn compile_patterns(&mut self) {
        self.ttl_rules.retain_mut(|rule| match Regex::new(&rule.pattern) {
            Ok(re) => {
                rule.regex = Some(re);
                true
            }
            Err(e) => {
                warn!(pattern = %rule.pattern, error = %e, "Invalid TTL pattern");
                false
            }
        });
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
    pub async fn load(args: Args) -> anyhow::Result<Self> {
        let config_path = args.config.clone().or_else(|| {
            ["/etc/apt-cacher/config.yaml", "./config.yaml"]
                .iter()
                .map(PathBuf::from)
                .find(|p| p.exists())
        });

        let mut figment = Figment::new().merge(Serialized::defaults(ConfigFile::default()));

        if let Some(path) = config_path {
            info!(path = %path.display(), "Loading config");
            figment = figment.merge(Yaml::file(path));
        }

        figment = figment.merge(Env::prefixed("APT_CACHER_").split("_"));

        let mut config: ConfigFile = figment.extract()?;

        if let Some(port) = args.port {
            config.port = Some(port);
        }
        if let Some(dir) = args.cache_dir {
            config.cache_dir = Some(dir);
        }
        if args.prometheus {
            config.prometheus = Some(true);
        }

        config.cache.compile_patterns();

        if config.repositories.is_empty() {
            warn!("No repositories configured");
        }

        Ok(Self {
            port: config.port.unwrap_or(3142),
            repositories: config.repositories,
            cache_dir: config.cache_dir.unwrap_or_else(|| "./apt_cache".into()),
            max_cache_size: config.max_cache_size.map(|b| b.as_u64()).unwrap_or(10 << 30),
            cache: config.cache,
            prometheus: config.prometheus.unwrap_or(false),
            prometheus_port: config.prometheus_port.unwrap_or(args.prometheus_port),
        })
    }
}