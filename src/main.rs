use anyhow::Result;
use apt_cacher_rs::{config::{Args, Settings}, metrics, router, server, App};
use clap::Parser;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "apt_cacher_rs=info".into()))
        .with_target(false)
        .init();

    let start = Instant::now();
    let settings = Settings::load(Args::parse())?;

    info!(port = settings.port, repos = settings.repositories.len(), "Starting v{}", env!("CARGO_PKG_VERSION"));

    metrics::init(settings.prometheus)?;
    
    let app = Arc::new(App::new(settings.clone()).await?);

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            metrics::update_uptime(start);
        }
    });

    server::serve(router(app), settings.port).await
}