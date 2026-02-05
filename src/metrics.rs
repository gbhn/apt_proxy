use metrics::{counter, describe_counter, describe_gauge, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use tracing::info;

static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();
static ACTIVE_DOWNLOADS: AtomicU64 = AtomicU64::new(0);

pub fn init(prometheus_port: Option<u16>) -> anyhow::Result<()> {
    if let Some(port) = prometheus_port {
        let handle = PrometheusBuilder::new()
            .with_http_listener(([0, 0, 0, 0], port))
            .install_recorder()?;
        let _ = PROMETHEUS.set(handle);
        info!(port, "Prometheus metrics enabled");
    }

    // Register metric descriptions
    describe_counter!("cache_hits_total", "Cache hits");
    describe_counter!("cache_misses_total", "Cache misses");
    describe_counter!("bytes_from_cache_total", "Bytes served from cache");
    describe_counter!("bytes_downloaded_total", "Bytes downloaded from upstream");
    describe_counter!("requests_coalesced_total", "Coalesced concurrent requests");
    describe_counter!("upstream_errors_total", "Upstream errors");
    describe_counter!("not_modified_total", "304 Not Modified responses");
    describe_gauge!("active_downloads", "Active downloads");

    Ok(())
}

#[inline]
pub fn record_hit(bytes: u64) {
    counter!("cache_hits_total").increment(1);
    counter!("bytes_from_cache_total").increment(bytes);
}

#[inline]
pub fn record_miss() {
    counter!("cache_misses_total").increment(1);
}

#[inline]
pub fn record_download(bytes: u64) {
    counter!("bytes_downloaded_total").increment(bytes);
}

#[inline]
pub fn record_coalesced() {
    counter!("requests_coalesced_total").increment(1);
}

#[inline]
pub fn record_304() {
    counter!("not_modified_total").increment(1);
}

#[inline]
pub fn record_error() {
    counter!("upstream_errors_total").increment(1);
}

pub fn inc_active() {
    let val = ACTIVE_DOWNLOADS.fetch_add(1, Ordering::Relaxed) + 1;
    gauge!("active_downloads").set(val as f64);
}

pub fn dec_active() {
    let val = ACTIVE_DOWNLOADS
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
        .unwrap_or(1)
        .saturating_sub(1);
    gauge!("active_downloads").set(val as f64);
}

pub fn render() -> Option<String> {
    PROMETHEUS.get().map(|h| h.render())
}