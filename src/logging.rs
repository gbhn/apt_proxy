//! Упрощённый модуль логирования на основе стандартных форматов tracing-subscriber

use std::{io::IsTerminal, path::Path};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub level: String,
    pub format: LogFormat,
    pub colors: Option<bool>,
    pub file: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum LogFormat {
    #[default]
    Pretty,
    Compact,
    Json,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Pretty,
            colors: None,
            file: None,
        }
    }
}

pub struct LogGuard {
    _file_guard: Option<WorkerGuard>,
}

pub fn init(config: LogConfig) -> LogGuard {
    let use_colors = config
        .colors
        .unwrap_or_else(|| std::io::stderr().is_terminal());

    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry().with(filter);

    match (&config.file, config.format) {
        (Some(path), LogFormat::Json) => {
            let (file_writer, guard) = create_file_appender(path);
            registry
                .with(
                    fmt::layer()
                        .json()
                        .with_writer(file_writer)
                        .with_ansi(false),
                )
                .with(fmt::layer().json().with_ansi(false))
                .init();
            LogGuard {
                _file_guard: Some(guard),
            }
        }
        (Some(path), LogFormat::Compact) => {
            let (file_writer, guard) = create_file_appender(path);
            registry
                .with(
                    fmt::layer()
                        .compact()
                        .with_writer(file_writer)
                        .with_ansi(false),
                )
                .with(fmt::layer().compact().with_ansi(use_colors))
                .init();
            LogGuard {
                _file_guard: Some(guard),
            }
        }
        (Some(path), LogFormat::Pretty) => {
            let (file_writer, guard) = create_file_appender(path);
            registry
                .with(fmt::layer().with_writer(file_writer).with_ansi(false))
                .with(
                    fmt::layer()
                        .pretty()
                        .with_ansi(use_colors)
                        .with_span_events(FmtSpan::CLOSE),
                )
                .init();
            LogGuard {
                _file_guard: Some(guard),
            }
        }
        (None, LogFormat::Json) => {
            registry.with(fmt::layer().json().with_ansi(false)).init();
            LogGuard { _file_guard: None }
        }
        (None, LogFormat::Compact) => {
            registry
                .with(fmt::layer().compact().with_ansi(use_colors))
                .init();
            LogGuard { _file_guard: None }
        }
        (None, LogFormat::Pretty) => {
            registry
                .with(
                    fmt::layer()
                        .pretty()
                        .with_ansi(use_colors)
                        .with_span_events(FmtSpan::CLOSE),
                )
                .init();
            LogGuard { _file_guard: None }
        }
    }
}

fn create_file_appender(
    path: &Path,
) -> (tracing_appender::non_blocking::NonBlocking, WorkerGuard) {
    let file_appender = tracing_appender::rolling::daily(
        path.parent().unwrap_or(Path::new(".")),
        path.file_name().unwrap_or_default(),
    );
    tracing_appender::non_blocking(file_appender)
}

pub fn print_banner() {
    let is_terminal = std::io::stderr().is_terminal();
    let banner = format!(
        r#"
╔════════════════════════════════════════════════════════╗
║                                                        ║
║         High-Performance APT Caching Proxy v{}         ║
║                                                        ║
╚════════════════════════════════════════════════════════╝
"#,
        VERSION
    );

    if is_terminal {
        eprintln!("\x1b[36m{}\x1b[0m", banner);
    } else {
        eprintln!("{}", banner);
    }
}

/// Вспомогательные функции для форматирования полей логов
pub mod fields {
    use crate::utils;

    #[inline]
    pub fn size(bytes: u64) -> String {
        utils::format_size(bytes)
    }

    #[inline]
    pub fn path(p: &str) -> &str {
        shorten_path(p, 60)
    }

    #[inline]
    pub fn shorten_path(path: &str, max_len: usize) -> &str {
        if path.len() <= max_len {
            path
        } else {
            let start = path.len() - max_len + 3;
            &path[start..]
        }
    }

    #[inline]
    pub fn duration(d: std::time::Duration) -> String {
        utils::format_duration(d)
    }
}

pub use fields::shorten_path;