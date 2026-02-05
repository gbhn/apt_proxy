//! Метрики с поддержкой Prometheus exporter

use metrics::{counter, describe_counter, describe_gauge, gauge, Counter, Gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;
use tracing::info;

static METRICS: OnceLock<AppMetrics> = OnceLock::new();
static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

struct AppMetrics {
    cache_hits: Counter,
    cache_misses: Counter,
    bytes_served_from_cache: Counter,
    bytes_downloaded: Counter,
    requests_coalesced: Counter,
    upstream_errors: Counter,
    validation_304s: Counter,
    active_downloads: Gauge,
}

/// Инициализирует метрики без Prometheus
pub fn init() {
    init_metrics();
}

/// Инициализирует метрики с Prometheus exporter на указанном порту
pub fn init_with_prometheus(port: u16) -> anyhow::Result<()> {
    let builder = PrometheusBuilder::new();
    let handle = builder
        .with_http_listener(([0, 0, 0, 0], port))
        .install_recorder()?;

    let _ = PROMETHEUS_HANDLE.set(handle);
    info!(port = port, endpoint = "/metrics", "Prometheus exporter started");

    init_metrics();
    Ok(())
}

fn init_metrics() {
    describe_counter!("apt_cacher_cache_hits_total", "Total number of cache hits");
    describe_counter!(
        "apt_cacher_cache_misses_total",
        "Total number of cache misses"
    );
    describe_counter!(
        "apt_cacher_bytes_served_from_cache_total",
        "Total bytes served from cache"
    );
    describe_counter!(
        "apt_cacher_bytes_downloaded_total",
        "Total bytes downloaded from upstream"
    );
    describe_counter!(
        "apt_cacher_requests_coalesced_total",
        "Total requests that were coalesced"
    );
    describe_counter!("apt_cacher_upstream_errors_total", "Total upstream errors");
    describe_counter!(
        "apt_cacher_validation_304s_total",
        "Total 304 responses from upstream"
    );
    describe_gauge!(
        "apt_cacher_active_downloads",
        "Number of currently active downloads"
    );

    let _ = METRICS.set(AppMetrics {
        cache_hits: counter!("apt_cacher_cache_hits_total"),
        cache_misses: counter!("apt_cacher_cache_misses_total"),
        bytes_served_from_cache: counter!("apt_cacher_bytes_served_from_cache_total"),
        bytes_downloaded: counter!("apt_cacher_bytes_downloaded_total"),
        requests_coalesced: counter!("apt_cacher_requests_coalesced_total"),
        upstream_errors: counter!("apt_cacher_upstream_errors_total"),
        validation_304s: counter!("apt_cacher_validation_304s_total"),
        active_downloads: gauge!("apt_cacher_active_downloads"),
    });
}

fn metrics() -> &'static AppMetrics {
    METRICS.get().expect("Metrics not initialized")
}

/// Получить Prometheus handle для рендеринга метрик
pub fn prometheus_handle() -> Option<&'static PrometheusHandle> {
    PROMETHEUS_HANDLE.get()
}

#[inline]
pub fn record_cache_hit(bytes: u64) {
    let m = metrics();
    m.cache_hits.increment(1);
    m.bytes_served_from_cache.increment(bytes);
}

#[inline]
pub fn record_cache_miss() {
    metrics().cache_misses.increment(1);
}

#[inline]
pub fn record_download(bytes: u64) {
    metrics().bytes_downloaded.increment(bytes);
}

#[inline]
pub fn record_coalesced_request() {
    metrics().requests_coalesced.increment(1);
}

#[inline]
pub fn record_304() {
    metrics().validation_304s.increment(1);
}

#[inline]
pub fn record_upstream_error() {
    metrics().upstream_errors.increment(1);
}

#[inline]
pub fn set_active_downloads(count: usize) {
    metrics().active_downloads.set(count as f64);
}

#[inline]
pub fn increment_active_downloads() {
    metrics().active_downloads.increment(1.0);
}

#[inline]
pub fn decrement_active_downloads() {
    metrics().active_downloads.decrement(1.0);
}

/// Рендерит метрики в формате Prometheus
pub fn render_prometheus() -> Option<String> {
    prometheus_handle().map(|h| h.render())
}

/// Снимок метрик для отображения
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bytes_served_from_cache: u64,
    pub bytes_downloaded: u64,
    pub requests_coalesced: u64,
    pub upstream_errors: u64,
    pub validation_304s: u64,
}

impl MetricsSnapshot {
    pub fn hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total > 0 {
            self.cache_hits as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Hits: {} | Misses: {} | Hit Rate: {:.1}% | Downloaded: {} | Served: {} | Coalesced: {} | 304s: {} | Errors: {}",
            self.cache_hits,
            self.cache_misses,
            self.hit_rate(),
            crate::utils::format_size(self.bytes_downloaded),
            crate::utils::format_size(self.bytes_served_from_cache),
            self.requests_coalesced,
            self.validation_304s,
            self.upstream_errors,
        )
    }
}