//! Prometheus metrics for monitoring cache performance

use metrics::{counter, describe_counter, describe_gauge, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use tracing::info;

static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();
static ACTIVE_DOWNLOADS: AtomicU64 = AtomicU64::new(0);

/// Metric names as constants for consistency
mod names {
    pub const CACHE_HITS: &str = "cache_hits_total";
    pub const CACHE_MISSES: &str = "cache_misses_total";
    pub const BYTES_FROM_CACHE: &str = "bytes_from_cache_total";
    pub const BYTES_DOWNLOADED: &str = "bytes_downloaded_total";
    pub const REQUESTS_COALESCED: &str = "requests_coalesced_total";
    pub const UPSTREAM_ERRORS: &str = "upstream_errors_total";
    pub const NOT_MODIFIED: &str = "not_modified_total";
    pub const ACTIVE_DOWNLOADS: &str = "active_downloads";
}

/// Initializes the metrics system with optional Prometheus endpoint
pub fn init(prometheus_port: Option<u16>) -> anyhow::Result<()> {
    if let Some(port) = prometheus_port {
        let handle = PrometheusBuilder::new()
            .with_http_listener(([0, 0, 0, 0], port))
            .install_recorder()?;
        let _ = PROMETHEUS.set(handle);
        info!(port, "Prometheus metrics enabled");
    }

    // Register metric descriptions
    describe_counter!(names::CACHE_HITS, "Total cache hits");
    describe_counter!(names::CACHE_MISSES, "Total cache misses");
    describe_counter!(names::BYTES_FROM_CACHE, "Total bytes served from cache");
    describe_counter!(names::BYTES_DOWNLOADED, "Total bytes downloaded from upstream");
    describe_counter!(names::REQUESTS_COALESCED, "Requests that joined existing downloads");
    describe_counter!(names::UPSTREAM_ERRORS, "Total upstream request errors");
    describe_counter!(names::NOT_MODIFIED, "304 Not Modified responses received");
    describe_gauge!(names::ACTIVE_DOWNLOADS, "Currently active downloads");

    Ok(())
}

/// Records a cache hit with the number of bytes served
#[inline]
pub fn record_hit(bytes: u64) {
    counter!(names::CACHE_HITS).increment(1);
    counter!(names::BYTES_FROM_CACHE).increment(bytes);
}

/// Records a cache miss
#[inline]
pub fn record_miss() {
    counter!(names::CACHE_MISSES).increment(1);
}

/// Records bytes downloaded from upstream
#[inline]
pub fn record_download(bytes: u64) {
    counter!(names::BYTES_DOWNLOADED).increment(bytes);
}

/// Records a coalesced request (joined existing download)
#[inline]
pub fn record_coalesced() {
    counter!(names::REQUESTS_COALESCED).increment(1);
}

/// Records a 304 Not Modified response
#[inline]
pub fn record_304() {
    counter!(names::NOT_MODIFIED).increment(1);
}

/// Records an upstream error
#[inline]
pub fn record_error() {
    counter!(names::UPSTREAM_ERRORS).increment(1);
}

/// Increments the active downloads counter
pub fn inc_active() {
    let val = ACTIVE_DOWNLOADS.fetch_add(1, Ordering::Relaxed) + 1;
    gauge!(names::ACTIVE_DOWNLOADS).set(val as f64);
}

/// Decrements the active downloads counter
pub fn dec_active() {
    let val = ACTIVE_DOWNLOADS
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        })
        .unwrap_or(1)
        .saturating_sub(1);
    gauge!(names::ACTIVE_DOWNLOADS).set(val as f64);
}

/// Renders metrics in Prometheus text format
pub fn render() -> Option<String> {
    PROMETHEUS.get().map(|h| h.render())
}

/// Returns the current number of active downloads
pub fn active_downloads() -> u64 {
    ACTIVE_DOWNLOADS.load(Ordering::Relaxed)
}