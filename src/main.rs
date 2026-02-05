use anyhow::{Context, Result};
use apt_cacher_rs::{
    config::{Args, Settings},
    metrics, router, server, App,
};
use clap::Parser;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let settings = Settings::load(Args::parse()).context("Failed to load configuration")?;

    info!(
        port = settings.port,
        cache_dir = %settings.cache_dir.display(),
        max_size_gb = settings.max_cache_size / (1024 * 1024 * 1024),
        repos = settings.repositories.len(),
        prometheus = settings.prometheus,
        "Starting apt-cacher-rs v{}",
        env!("CARGO_PKG_VERSION")
    );

    metrics::init(settings.prometheus.then_some(settings.prometheus_port))
        .context("Failed to initialize metrics")?;

    let app = Arc::new(
        App::new(settings.clone())
            .await
            .context("Failed to initialize application")?,
    );

    server::serve(router(app), settings.port).await
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("apt_cacher_rs=info,tower_http=info")
    });

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}