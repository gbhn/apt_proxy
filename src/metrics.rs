//! Модуль метрик использует стандартный `metrics` crate
//!
//! Преимущества:
//! - Стандартный интерфейс, совместимый с Prometheus, StatsD и др.
//! - Низкий overhead
//! - Атомарные операции под капотом

use metrics::{counter, gauge, describe_counter, describe_gauge, Counter, Gauge};
use std::sync::OnceLock;

/// Глобальные метрики
static METRICS: OnceLock<AppMetrics> = OnceLock::new();

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

/// Инициализирует метрики с описаниями
pub fn init() {
    describe_counter!("apt_cacher_cache_hits_total", "Total number of cache hits");
    describe_counter!("apt_cacher_cache_misses_total", "Total number of cache misses");
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
    describe_counter!(
        "apt_cacher_upstream_errors_total",
        "Total upstream errors"
    );
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

/// Снимок метрик для отображения (для совместимости)
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

/// Совместимый API для существующего кода
#[derive(Default)]
pub struct Metrics;

impl Metrics {
    pub fn new() -> Self {
        Self
    }

    #[inline]
    pub fn record_cache_hit(&self, bytes: u64) {
        record_cache_hit(bytes);
    }

    #[inline]
    pub fn record_cache_miss(&self) {
        record_cache_miss();
    }

    #[inline]
    pub fn record_download(&self, bytes: u64) {
        record_download(bytes);
    }

    #[inline]
    pub fn record_coalesced_request(&self) {
        record_coalesced_request();
    }

    #[inline]
    pub fn record_304(&self) {
        record_304();
    }

    #[inline]
    pub fn record_upstream_error(&self) {
        record_upstream_error();
    }

    /// Snapshot для отображения - в данной реализации недоступен
    /// так как metrics crate не предоставляет способ прочитать значения
    /// Для полноценного решения нужен metrics-exporter
    pub fn snapshot(&self) -> MetricsSnapshot {
        // С metrics crate мы не можем напрямую читать значения
        // Для этого нужен recorder (например metrics-exporter-prometheus)
        MetricsSnapshot::default()
    }
}