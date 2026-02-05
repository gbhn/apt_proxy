//! Prometheus metrics - simplified

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();

// ============ Atomic Gauge Macro ============

macro_rules! atomic_gauge {
    ($static:ident, $metric:literal, $inc:ident, $dec:ident, $get:ident) => {
        static $static: AtomicU64 = AtomicU64::new(0);

        pub fn $inc() {
            let val = $static.fetch_add(1, Ordering::Relaxed) + 1;
            gauge!($metric).set(val as f64);
        }

        pub fn $dec() {
            let val = $static
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
                .unwrap_or(1)
                .saturating_sub(1);
            gauge!($metric).set(val as f64);
        }

        pub fn $get() -> u64 {
            $static.load(Ordering::Relaxed)
        }
    };
}

atomic_gauge!(ACTIVE_DOWNLOADS, "downloads_active", inc_active, dec_active, active_downloads);
atomic_gauge!(REQUESTS_IN_FLIGHT, "http_requests_in_flight", inc_requests_in_flight, dec_requests_in_flight, requests_in_flight);
atomic_gauge!(ACTIVE_CONNECTIONS, "http_active_connections", inc_connections, dec_connections, active_connections);
atomic_gauge!(PENDING_FOLLOWERS, "downloads_pending_followers", inc_followers, dec_followers, _pending_followers);
atomic_gauge!(TEMP_FILES_ACTIVE, "storage_temp_files_active", inc_temp_files, dec_temp_files, _temp_files_active);

// ============ Histogram Buckets ============

const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
];

const FAST_BUCKETS: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

const SIZE_BUCKETS: &[f64] = &[
    1024.0, 16384.0, 262144.0, 1048576.0, 16777216.0, 67108864.0, 268435456.0, 1073741824.0,
];

// ============ Initialization ============

pub fn init(enabled: bool) -> anyhow::Result<()> {
    if enabled {
        let handle = PrometheusBuilder::new()
            .set_buckets_for_metric(Matcher::Suffix("_duration_seconds".into()), DURATION_BUCKETS)?
            .set_buckets_for_metric(Matcher::Full("cache_lookup_duration_seconds".into()), FAST_BUCKETS)?
            .set_buckets_for_metric(Matcher::Suffix("_size_bytes".into()), SIZE_BUCKETS)?
            .install_recorder()?;
        let _ = PROMETHEUS.set(handle);
        info!("Prometheus metrics enabled");
    }

    // Record static metrics
    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    gauge!("process_start_time_seconds").set(start_time);
    gauge!("build_info", "version" => env!("CARGO_PKG_VERSION")).set(1.0);
    gauge!("health_check_status").set(1.0);

    Ok(())
}

pub fn render() -> Option<String> {
    PROMETHEUS.get().map(|h| h.render())
}

// ============ Cache Metrics ============

#[inline]
pub fn record_cache_lookup(hit: bool, duration: Duration) {
    let result = if hit { "hit" } else { "miss" };
    counter!("cache_lookups_total", "result" => result).increment(1);
    histogram!("cache_lookup_duration_seconds", "result" => result).record(duration.as_secs_f64());
    
    if hit {
        gauge!("last_cache_hit_timestamp").set(now_secs());
    }
}

#[inline]
pub fn set_cache_stats(size_bytes: u64, entries: u64, weighted_kb: u64) {
    gauge!("cache_size_bytes").set(size_bytes as f64);
    gauge!("cache_entries").set(entries as f64);
    gauge!("cache_weighted_size_kb").set(weighted_kb as f64);
}

#[inline]
pub fn set_cache_loading(loading: bool) {
    gauge!("cache_index_loading").set(if loading { 1.0 } else { 0.0 });
}

#[inline]
pub fn record_eviction(reason: &str) {
    counter!("cache_evictions_total", "reason" => reason.to_string()).increment(1);
}

#[inline]
pub fn record_cache_operation(op: &str) {
    counter!("cache_operations_total", "operation" => op.to_string()).increment(1);
}

// ============ Repository Metrics ============

#[inline]
pub fn record_repo_request(repo: &str) {
    counter!("repository_requests_total", "repo" => repo.to_string()).increment(1);
}

#[inline]
pub fn record_repo_hit(repo: &str, bytes: u64) {
    counter!("cache_hits_total").increment(1);
    counter!("cache_bytes_served_total").increment(bytes);
    counter!("repository_cache_hits_total", "repo" => repo.to_string()).increment(1);
    counter!("repository_bytes_served_total", "repo" => repo.to_string()).increment(bytes);
}

#[inline]
pub fn record_repo_miss(repo: &str) {
    counter!("cache_misses_total").increment(1);
    counter!("repository_cache_misses_total", "repo" => repo.to_string()).increment(1);
}

#[inline]
pub fn record_repo_error(repo: &str, error_type: &str) {
    counter!("repository_errors_total", "repo" => repo.to_string(), "type" => error_type.to_string()).increment(1);
}

#[inline]
pub fn record_repo_bytes_served(repo: &str, bytes: u64) {
    counter!("repository_bytes_served_total", "repo" => repo.to_string()).increment(bytes);
}

// ============ Download Metrics ============

#[inline]
pub fn record_download_started() {
    counter!("downloads_started_total").increment(1);
}

#[inline]
pub fn record_download_complete(bytes: u64, duration: Duration, repo: &str) {
    counter!("downloads_total", "status" => "success", "repo" => repo.to_string()).increment(1);
    counter!("download_bytes_total").increment(bytes);
    histogram!("download_duration_seconds", "repo" => repo.to_string()).record(duration.as_secs_f64());
    histogram!("download_size_bytes", "repo" => repo.to_string()).record(bytes as f64);
    
    if duration.as_secs_f64() > 0.0 {
        histogram!("download_speed_bytes_per_second").record(bytes as f64 / duration.as_secs_f64());
    }
    gauge!("last_successful_download_timestamp").set(now_secs());
}

#[inline]
pub fn record_download_failed(reason: &str, repo: &str) {
    counter!("downloads_total", "status" => "failed", "reason" => reason.to_string(), "repo" => repo.to_string()).increment(1);
}

#[inline]
pub fn record_coalesced() {
    counter!("downloads_coalesced_total").increment(1);
}

#[inline]
pub fn set_download_progress(key: &str, bytes: u64) {
    gauge!("download_progress_bytes", "key" => key.to_string()).set(bytes as f64);
}

#[inline]
pub fn clear_download_progress(key: &str) {
    gauge!("download_progress_bytes", "key" => key.to_string()).set(0.0);
}

// ============ Upstream Metrics ============

#[inline]
pub fn record_304() {
    counter!("upstream_not_modified_total").increment(1);
}

#[inline]
pub fn record_upstream_error(category: &str, repo: &str) {
    counter!("upstream_errors_total", "category" => category.to_string(), "repo" => repo.to_string()).increment(1);
}

#[inline]
pub fn record_upstream_timeout(repo: &str) {
    counter!("upstream_timeouts_total", "repo" => repo.to_string()).increment(1);
}

#[inline]
pub fn record_upstream_request(status: u16, duration: Duration, size: Option<u64>, repo: &str) {
    counter!("upstream_requests_total", "status" => status.to_string(), "repo" => repo.to_string()).increment(1);
    histogram!("upstream_request_duration_seconds", "repo" => repo.to_string()).record(duration.as_secs_f64());
    if let Some(s) = size {
        histogram!("upstream_response_size_bytes", "repo" => repo.to_string()).record(s as f64);
    }
}

#[inline]
pub fn record_upstream_connect_time(duration: Duration) {
    histogram!("upstream_connect_duration_seconds").record(duration.as_secs_f64());
}

#[inline]
pub fn record_upstream_ttfb(duration: Duration) {
    histogram!("upstream_time_to_first_byte_seconds").record(duration.as_secs_f64());
}

// ============ Storage Metrics ============

#[inline]
pub fn record_storage_operation(op: &str, duration: Duration, success: bool) {
    let status = if success { "success" } else { "error" };
    counter!("storage_operations_total", "operation" => op.to_string(), "status" => status).increment(1);
    histogram!("storage_operation_duration_seconds", "operation" => op.to_string()).record(duration.as_secs_f64());
}

#[inline]
pub fn record_storage_error(error_type: &str) {
    counter!("storage_errors_total", "type" => error_type.to_string()).increment(1);
}

#[inline]
pub fn record_storage_read(bytes: u64) {
    counter!("storage_bytes_read_total").increment(bytes);
}

#[inline]
pub fn record_storage_write(bytes: u64) {
    counter!("storage_bytes_written_total").increment(bytes);
}

#[inline]
pub fn set_storage_space_used(bytes: u64) {
    gauge!("storage_space_used_bytes").set(bytes as f64);
}

#[inline]
pub fn record_temp_file_created() {
    counter!("storage_temp_files_created_total").increment(1);
    inc_temp_files();
}

#[inline]
pub fn record_temp_cleanup(count: u64) {
    counter!("cleanup_temp_files_total").increment(count);
}

#[inline]
pub fn record_orphans_removed(count: u64) {
    counter!("storage_orphans_removed_total").increment(count);
}

#[inline]
pub fn record_metadata_parse_error() {
    counter!("storage_metadata_parse_errors_total").increment(1);
}

// ============ TTL Metrics ============

#[inline]
pub fn record_ttl_applied(ttl_seconds: u64, pattern: &str) {
    histogram!("ttl_applied_seconds", "pattern" => pattern.to_string()).record(ttl_seconds as f64);
}

#[inline]
pub fn record_ttl_expiration() {
    counter!("ttl_expirations_total").increment(1);
}

#[inline]
pub fn record_ttl_refresh() {
    counter!("ttl_refreshes_total").increment(1);
}

// ============ Request Metrics ============

#[inline]
pub fn record_request(method: &str, status: u16, repo: &str, duration: Duration, _req_size: u64, _resp_size: u64) {
    counter!("http_requests_total", 
        "method" => method.to_string(),
        "status" => status.to_string(),
        "repo" => repo.to_string()
    ).increment(1);
    histogram!("http_request_duration_seconds",
        "method" => method.to_string(),
        "repo" => repo.to_string()
    ).record(duration.as_secs_f64());
}

#[inline]
pub fn record_path_validation(success: bool, reason: Option<&str>) {
    let result = if success { "success" } else { "failure" };
    counter!("path_validations_total", "result" => result).increment(1);
    if let Some(r) = reason {
        counter!("path_validation_failures_total", "reason" => r.to_string()).increment(1);
    }
}

// ============ Maintenance Metrics ============

#[inline]
pub fn record_maintenance_run(task: &str, duration: Duration) {
    counter!("maintenance_runs_total", "task" => task.to_string()).increment(1);
    histogram!("maintenance_duration_seconds", "task" => task.to_string()).record(duration.as_secs_f64());
}

// ============ Health & Status ============

#[inline]
pub fn set_health_status(healthy: bool) {
    gauge!("health_check_status").set(if healthy { 1.0 } else { 0.0 });
}

#[inline]
pub fn update_uptime(start_time: Instant) {
    gauge!("process_uptime_seconds").set(start_time.elapsed().as_secs_f64());
}

// ============ Helpers ============

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64()
}

// ============ RAII Guards ============

/// Guard for tracking followers
pub struct FollowerGuard;

impl FollowerGuard {
    pub fn new() -> Self {
        inc_followers();
        Self
    }
}

impl Drop for FollowerGuard {
    fn drop(&mut self) {
        dec_followers();
    }
}

/// Guard for tracking temp files
pub struct TempFileGuard;

impl TempFileGuard {
    pub fn new() -> Self {
        record_temp_file_created();
        Self
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        dec_temp_files();
    }
}