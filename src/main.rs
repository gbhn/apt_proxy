use anyhow::Result;
use apt_cacher_rs::{
    build_router,
    config::{Args, Settings},
    logging::{self, LogConfig, LogFormat},
    metrics,
    server, AppState,
};
use clap::Parser;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let log_format = std::env::var("LOG_FORMAT")
        .ok()
        .and_then(|s| match s.to_lowercase().as_str() {
            "json" => Some(LogFormat::Json),
            "compact" => Some(LogFormat::Compact),
            "pretty" => Some(LogFormat::Pretty),
            _ => None,
        })
        .unwrap_or(LogFormat::Pretty);

    let log_config = LogConfig {
        level: std::env::var("RUST_LOG")
            .unwrap_or_else(|_| "apt_cacher_rs=info,tower_http=info,hyper=warn".to_string()),
        format: log_format,
        colors: None,
        file: std::env::var("LOG_FILE").ok().map(Into::into),
    };

    let _guard = logging::init(log_config);

    if log_format == LogFormat::Pretty {
        logging::print_banner();
    }

    let settings = Settings::load(args.clone()).await?;
    settings.display_info();

    // Инициализируем метрики (с или без Prometheus)
    if settings.prometheus {
        metrics::init_with_prometheus(settings.prometheus_port)?;
    } else {
        metrics::init();
    }

    let state = Arc::new(AppState::new(settings.clone()).await?);
    let app = build_router(state.clone());

    info!(
        repositories = state.settings.repositories.len(),
        cache_dir = %state.settings.cache_dir.display(),
        max_size = %logging::fields::size(state.settings.max_cache_size),
        "Server ready"
    );

    match state.settings.socket {
        Some(ref socket_path) => server::serve_unix(app, socket_path).await,
        None => server::serve_tcp(app, state.settings.port).await,
    }
}