use crate::utils::{format_duration_secs, format_size};
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
const DEFAULT_CACHE_DIR: &str = "./apt_cache";
const DEFAULT_MAX_CACHE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
const DEFAULT_MAX_LRU_ENTRIES: usize = 100_000;
const DEFAULT_TTL: u64 = 86400;
const DEFAULT_MIN_TTL: u64 = 3600;
const DEFAULT_MAX_TTL: u64 = 604800;
const DEFAULT_STALE_WHILE_REVALIDATE: u64 = 3600;

#[derive(Parser, Clone, Debug)]
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

    #[arg(long, env = "APT_CACHER_STRICT_PATTERNS", default_value = "false")]
    pub strict_patterns: bool,

    /// Enable Prometheus metrics endpoint
    #[arg(long, env = "APT_CACHER_PROMETHEUS", default_value = "false")]
    pub prometheus: bool,

    /// Prometheus metrics port
    #[arg(long, env = "APT_CACHER_PROMETHEUS_PORT", default_value = "9090")]
    pub prometheus_port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TtlOverride {
    pub pattern: String,
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
    #[serde(skip)]
    pub regex: Option<Regex>,
}

impl TtlOverride {
    pub fn ttl_secs(&self) -> u64 {
        self.ttl.as_secs()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationSettings {
    #[serde(default = "default_true")]
    pub use_etag: bool,
    #[serde(default = "default_true")]
    pub use_last_modified: bool,
    #[serde(default)]
    pub always_revalidate: bool,
}

impl Default for ValidationSettings {
    fn default() -> Self {
        Self {
            use_etag: true,
            use_last_modified: true,
            always_revalidate: false,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_ttl() -> Duration {
    Duration::from_secs(DEFAULT_TTL)
}

fn default_min_ttl() -> Duration {
    Duration::from_secs(DEFAULT_MIN_TTL)
}

fn default_max_ttl() -> Duration {
    Duration::from_secs(DEFAULT_MAX_TTL)
}

fn default_stale_while_revalidate() -> Duration {
    Duration::from_secs(DEFAULT_STALE_WHILE_REVALIDATE)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CacheSettings {
    #[serde(default = "default_ttl", with = "humantime_serde")]
    pub default_ttl: Duration,
    #[serde(default = "default_min_ttl", with = "humantime_serde")]
    pub min_ttl: Duration,
    #[serde(default = "default_max_ttl", with = "humantime_serde")]
    pub max_ttl: Duration,
    #[serde(default)]
    pub ignore_cache_control: bool,
    #[serde(default = "default_stale_while_revalidate", with = "humantime_serde")]
    pub stale_while_revalidate: Duration,
    #[serde(default)]
    pub ttl_overrides: Vec<TtlOverride>,
    #[serde(default)]
    pub validation: ValidationSettings,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            default_ttl: default_ttl(),
            min_ttl: default_min_ttl(),
            max_ttl: default_max_ttl(),
            ignore_cache_control: false,
            stale_while_revalidate: default_stale_while_revalidate(),
            ttl_overrides: Vec::new(),
            validation: ValidationSettings::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PatternCompilationResult {
    pub successful: usize,
    pub failed: Vec<(String, String)>,
}

impl CacheSettings {
    #[inline]
    pub fn default_ttl_secs(&self) -> u64 {
        self.default_ttl.as_secs()
    }

    #[inline]
    pub fn min_ttl_secs(&self) -> u64 {
        self.min_ttl.as_secs()
    }

    #[inline]
    pub fn max_ttl_secs(&self) -> u64 {
        self.max_ttl.as_secs()
    }

    #[inline]
    pub fn stale_while_revalidate_secs(&self) -> u64 {
        self.stale_while_revalidate.as_secs()
    }

    pub fn get_ttl_for_path(&self, path: &str) -> u64 {
        for override_rule in &self.ttl_overrides {
            if let Some(regex) = &override_rule.regex {
                if regex.is_match(path) {
                    return self.clamp_ttl(override_rule.ttl_secs());
                }
            }
        }
        self.clamp_ttl(self.default_ttl_secs())
    }

    #[inline]
    pub fn clamp_ttl(&self, ttl: u64) -> u64 {
        ttl.clamp(self.min_ttl_secs(), self.max_ttl_secs())
    }

    pub fn compile_patterns(&mut self) -> PatternCompilationResult {
        let mut successful = 0;
        let mut failed = Vec::new();

        for override_rule in &mut self.ttl_overrides {
            match Regex::new(&override_rule.pattern) {
                Ok(regex) => {
                    override_rule.regex = Some(regex);
                    info!(
                        pattern = %override_rule.pattern,
                        ttl = %format_duration_secs(override_rule.ttl_secs()),
                        "Compiled TTL override pattern"
                    );
                    successful += 1;
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    warn!(
                        pattern = %override_rule.pattern,
                        error = %error_msg,
                        "Failed to compile TTL pattern"
                    );
                    failed.push((override_rule.pattern.clone(), error_msg));
                }
            }
        }

        self.ttl_overrides.retain(|r| r.regex.is_some());

        if !failed.is_empty() {
            warn!(
                failed = failed.len(),
                "Some TTL patterns failed to compile and were skipped"
            );
        }

        PatternCompilationResult { successful, failed }
    }

    pub fn compile_patterns_strict(&mut self) -> anyhow::Result<()> {
        for override_rule in &mut self.ttl_overrides {
            let regex = Regex::new(&override_rule.pattern).map_err(|e| {
                anyhow::anyhow!("Invalid TTL pattern '{}': {}", override_rule.pattern, e)
            })?;
            override_rule.regex = Some(regex);
            info!(
                pattern = %override_rule.pattern,
                ttl = %format_duration_secs(override_rule.ttl_secs()),
                "Compiled TTL override pattern"
            );
        }
        Ok(())
    }
}

/// Конфигурация из файла, совместимая с figment
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub socket: Option<PathBuf>,
    #[serde(default)]
    pub repositories: HashMap<String, String>,
    #[serde(default)]
    pub cache_dir: Option<PathBuf>,
    #[serde(default)]
    pub max_cache_size: Option<ByteSize>,
    #[serde(default)]
    pub max_lru_entries: Option<usize>,
    #[serde(default)]
    pub cache: CacheSettings,
    #[serde(default)]
    pub prometheus: Option<bool>,
    #[serde(default)]
    pub prometheus_port: Option<u16>,
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
    pub prometheus: bool,
    pub prometheus_port: u16,
}

impl Settings {
    /// Загружает конфигурацию с использованием figment
    /// Приоритет: CLI args > ENV > config file > defaults
    pub async fn load(args: Args) -> anyhow::Result<Self> {
        // Определяем путь к конфигу
        let config_paths: Vec<PathBuf> = if let Some(ref path) = args.config {
            vec![path.clone()]
        } else {
            vec![
                PathBuf::from("/etc/apt-cacher/config.yaml"),
                PathBuf::from("./config.yaml"),
            ]
        };

        // Строим figment с приоритетами
        let mut figment = Figment::new()
            // Defaults
            .merge(Serialized::defaults(ConfigFile::default()));

        // Добавляем конфиг файлы (первый найденный)
        for path in &config_paths {
            if path.exists() {
                info!(path = %path.display(), "Loading configuration file");
                figment = figment.merge(Yaml::file(path));
                break;
            }
        }

        // ENV переменные с префиксом APT_CACHER_
        figment = figment.merge(Env::prefixed("APT_CACHER_").split("_"));

        // Извлекаем конфигурацию
        let mut config: ConfigFile = figment.extract()?;

        // CLI args override (применяем явно заданные аргументы)
        if args.port.is_some() {
            config.port = args.port;
        }
        if args.socket.is_some() {
            config.socket = args.socket;
        }
        if args.cache_dir.is_some() {
            config.cache_dir = args.cache_dir;
        }
        if args.max_cache_size.is_some() {
            config.max_cache_size = args.max_cache_size.map(ByteSize);
        }
        if args.max_lru_entries.is_some() {
            config.max_lru_entries = args.max_lru_entries;
        }
        if args.prometheus {
            config.prometheus = Some(true);
        }

        // Компиляция паттернов
        if args.strict_patterns {
            config.cache.compile_patterns_strict()?;
        } else {
            let result = config.cache.compile_patterns();
            if !result.failed.is_empty() {
                info!(
                    successful = result.successful,
                    failed = result.failed.len(),
                    "Pattern compilation completed with some failures"
                );
            }
        }

        Ok(Self {
            port: config.port.unwrap_or(DEFAULT_PORT),
            socket: config.socket,
            repositories: config.repositories,
            cache_dir: config.cache_dir.unwrap_or_else(|| DEFAULT_CACHE_DIR.into()),
            max_cache_size: config
                .max_cache_size
                .map(|bs| bs.as_u64())
                .unwrap_or(DEFAULT_MAX_CACHE_SIZE),
            max_lru_entries: config.max_lru_entries.unwrap_or(DEFAULT_MAX_LRU_ENTRIES),
            cache: config.cache,
            prometheus: config.prometheus.unwrap_or(false),
            prometheus_port: config.prometheus_port.unwrap_or(args.prometheus_port),
        })
    }

    pub fn display_info(&self) {
        info!(
            path = %self.cache_dir.display(),
            max_size = %format_size(self.max_cache_size),
            max_entries = self.max_lru_entries,
            "Cache configuration"
        );

        info!(
            default_ttl = %format_duration_secs(self.cache.default_ttl_secs()),
            min_ttl = %format_duration_secs(self.cache.min_ttl_secs()),
            max_ttl = %format_duration_secs(self.cache.max_ttl_secs()),
            stale_while_revalidate = %format_duration_secs(self.cache.stale_while_revalidate_secs()),
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

        if self.prometheus {
            info!(port = self.prometheus_port, "Prometheus metrics enabled");
        }

        if let Some(ref socket) = self.socket {
            info!(socket = %socket.display(), "Unix socket mode");
        } else {
            info!(port = self.port, "TCP mode");
        }
    }
}