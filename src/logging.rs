use nu_ansi_term::{Color, Style};
use std::{fmt, io::IsTerminal, path::Path};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    fmt::{
        format::{self, FormatEvent, FormatFields},
        FmtContext, FormattedFields,
    },
    layer::SubscriberExt,
    registry::LookupSpan,
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
            let file_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_writer(file_writer)
                .with_ansi(false);
            let console_layer = tracing_subscriber::fmt::layer().json().with_ansi(false);

            registry.with(console_layer).with(file_layer).init();
            LogGuard {
                _file_guard: Some(guard),
            }
        }
        (Some(path), LogFormat::Compact) => {
            let (file_writer, guard) = create_file_appender(path);
            let file_layer = tracing_subscriber::fmt::layer()
                .compact()
                .with_writer(file_writer)
                .with_ansi(false);
            let console_layer = tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(use_colors);

            registry.with(console_layer).with(file_layer).init();
            LogGuard {
                _file_guard: Some(guard),
            }
        }
        (Some(path), LogFormat::Pretty) => {
            let (file_writer, guard) = create_file_appender(path);
            let file_layer = tracing_subscriber::fmt::layer()
                .with_writer(file_writer)
                .with_ansi(false);
            let console_layer = tracing_subscriber::fmt::layer()
                .event_format(PrettyFormatter::new(use_colors))
                .with_ansi(use_colors);

            registry.with(console_layer).with(file_layer).init();
            LogGuard {
                _file_guard: Some(guard),
            }
        }
        (None, LogFormat::Json) => {
            let console_layer = tracing_subscriber::fmt::layer().json().with_ansi(false);

            registry.with(console_layer).init();
            LogGuard { _file_guard: None }
        }
        (None, LogFormat::Compact) => {
            let console_layer = tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(use_colors);

            registry.with(console_layer).init();
            LogGuard { _file_guard: None }
        }
        (None, LogFormat::Pretty) => {
            let console_layer = tracing_subscriber::fmt::layer()
                .event_format(PrettyFormatter::new(use_colors))
                .with_ansi(use_colors);

            registry.with(console_layer).init();
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

struct PrettyFormatter {
    colors: bool,
}

impl PrettyFormatter {
    fn new(colors: bool) -> Self {
        Self { colors }
    }

    fn style_level(&self, level: Level) -> StyledLevel {
        StyledLevel {
            level,
            colors: self.colors,
        }
    }

    fn dim(&self, text: &str) -> String {
        if self.colors {
            Style::new().dimmed().paint(text).to_string()
        } else {
            text.to_string()
        }
    }

    fn cyan(&self, text: &str) -> String {
        if self.colors {
            Color::Cyan.paint(text).to_string()
        } else {
            text.to_string()
        }
    }
}

struct StyledLevel {
    level: Level,
    colors: bool,
}

impl fmt::Display for StyledLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.colors {
            let (color, text) = match self.level {
                Level::ERROR => (Color::Red, "ERROR"),
                Level::WARN => (Color::Yellow, "WARN "),
                Level::INFO => (Color::Green, "INFO "),
                Level::DEBUG => (Color::Blue, "DEBUG"),
                Level::TRACE => (Color::Purple, "TRACE"),
            };
            write!(f, "{}", color.bold().paint(text))
        } else {
            let text = match self.level {
                Level::ERROR => "ERROR",
                Level::WARN => "WARN ",
                Level::INFO => "INFO ",
                Level::DEBUG => "DEBUG",
                Level::TRACE => "TRACE",
            };
            write!(f, "{}", text)
        }
    }
}

impl<S, N> FormatEvent<S, N> for PrettyFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let level = *metadata.level();

        let now = chrono::Local::now();
        write!(
            writer,
            "{} ",
            self.dim(&now.format("%H:%M:%S%.3f").to_string())
        )?;

        write!(writer, "{} ", self.style_level(level))?;

        let target = metadata.target();
        let short_target = if target.starts_with("apt_cacher_rs") {
            target.strip_prefix("apt_cacher_rs::").unwrap_or(target)
        } else if target.len() > 20 {
            &target[target.len() - 20..]
        } else {
            target
        };
        write!(writer, "{} ", self.cyan(&format!("{:>12}", short_target)))?;

        write!(writer, "{} ", self.dim("│"))?;

        if let Some(scope) = ctx.event_scope() {
            let mut seen = false;
            for span in scope.from_root() {
                let exts = span.extensions();
                if let Some(fields) = exts.get::<FormattedFields<N>>() {
                    if !fields.is_empty() {
                        if seen {
                            write!(writer, "{}", self.dim(":"))?;
                        }
                        write!(writer, "{}", fields)?;
                        seen = true;
                    }
                }
            }
            if seen {
                write!(writer, " ")?;
            }
        }

        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

pub fn print_banner() {
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

    if std::io::stderr().is_terminal() {
        eprintln!("{}", Color::Cyan.paint(banner));
    } else {
        eprintln!("{}", banner);
    }
}

pub mod fields {
    #[inline]
    pub fn size(bytes: u64) -> String {
        crate::utils::format_size(bytes)
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

    pub fn duration(d: std::time::Duration) -> String {
        let ms = d.as_millis();
        if ms < 1000 {
            format!("{}ms", ms)
        } else if ms < 60_000 {
            format!("{:.2}s", d.as_secs_f64())
        } else {
            format!("{}m{}s", ms / 60_000, (ms % 60_000) / 1000)
        }
    }
}

pub use fields::shorten_path;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shorten_path() {
        assert_eq!(shorten_path("short", 10), "short");
        assert_eq!(shorten_path("a/very/long/path/to/file.deb", 20).len(), 20);
    }

    #[test]
    fn test_size_format() {
        assert_eq!(fields::size(1024), "1.00KB");
        assert_eq!(fields::size(1_048_576), "1.00MB");
        assert_eq!(fields::size(1_073_741_824), "1.00GB");
    }
}