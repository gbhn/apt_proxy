use clap::Parser;
use std::sync::Arc;

use apt_cacher_rs::{
    build_router, cache::CacheManager, config::{Args, Settings}, server, utils, AppState
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::init_logging();

    let args = Args::parse();
    let settings = Settings::load(args).await?;
    
    settings.display_info();

    let cache = CacheManager::new(settings.clone()).await?;
    let state = Arc::new(AppState::new(settings, cache));

    let app = build_router(state.clone());
    
    if let Some(ref socket_path) = state.settings.socket {
        server::serve_unix(app, socket_path).await
    } else {
        server::serve_tcp(app, state.settings.port).await
    }
}