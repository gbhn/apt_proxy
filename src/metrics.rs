use std::sync::atomic::{AtomicU64, Ordering};

/// Метрики производительности кэша
#[derive(Default)]
pub struct Metrics {
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub bytes_served_from_cache: AtomicU64,
    pub bytes_downloaded: AtomicU64,
    pub requests_coalesced: AtomicU64,
    pub upstream_errors: AtomicU64,
    pub validation_304s: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn record_cache_hit(&self, bytes: u64) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        self.bytes_served_from_cache.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_download(&self, bytes: u64) {
        self.bytes_downloaded.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_coalesced_request(&self) {
        self.requests_coalesced.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_304(&self) {
        self.validation_304s.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_upstream_error(&self) {
        self.upstream_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            bytes_served_from_cache: self.bytes_served_from_cache.load(Ordering::Relaxed),
            bytes_downloaded: self.bytes_downloaded.load(Ordering::Relaxed),
            requests_coalesced: self.requests_coalesced.load(Ordering::Relaxed),
            upstream_errors: self.upstream_errors.load(Ordering::Relaxed),
            validation_304s: self.validation_304s.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
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