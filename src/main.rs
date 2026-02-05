use anyhow::Result;
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
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("apt_cacher_rs=info,tower_http=info")),
        )
        .init();

    let settings = Settings::load(args).await?;

    info!(
        port = settings.port,
        cache_dir = %settings.cache_dir.display(),
        max_size_gb = settings.max_cache_size / 1024 / 1024 / 1024,
        repos = settings.repositories.len(),
        "Starting apt-cacher-rs"
    );

    metrics::init(settings.prometheus.then_some(settings.prometheus_port))?;

    let app = Arc::new(App::new(settings.clone()).await?);
    server::serve(router(app), settings.port).await
}