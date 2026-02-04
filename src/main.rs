use clap::Parser;
use std::sync::Arc;
use apt_cacher_rs::{build_router, config::{Args, Settings}, server, utils, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::init_logging();

    let args = Args::parse();
    let settings = Settings::load(args).await?;

    settings.display_info();

    let state = Arc::new(AppState::new(settings.clone()).await?);
    let app = build_router(state.clone());

    match state.settings.socket {
        Some(ref socket_path) => server::serve_unix(app, socket_path).await,
        None => server::serve_tcp(app, state.settings.port).await,
    }
}