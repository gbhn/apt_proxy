//! Comprehensive Prometheus metrics for monitoring cache performance

use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

static PROMETHEUS: OnceLock<PrometheusHandle> = OnceLock::new();
static ACTIVE_DOWNLOADS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static PENDING_FOLLOWERS: AtomicU64 = AtomicU64::new(0);

/// Metric names as constants for consistency
pub mod names {
    // ============ HTTP Request Metrics ============
    pub const REQUESTS_TOTAL: &str = "http_requests_total";
    pub const REQUEST_DURATION_SECONDS: &str = "http_request_duration_seconds";
    pub const REQUEST_SIZE_BYTES: &str = "http_request_size_bytes";
    pub const RESPONSE_SIZE_BYTES: &str = "http_response_size_bytes";
    pub const ACTIVE_CONNECTIONS: &str = "http_active_connections";
    pub const REQUESTS_IN_FLIGHT: &str = "http_requests_in_flight";

    // ============ Cache Metrics ============
    pub const CACHE_HITS: &str = "cache_hits_total";
    pub const CACHE_MISSES: &str = "cache_misses_total";
    pub const CACHE_SIZE_BYTES: &str = "cache_size_bytes";
    pub const CACHE_ENTRIES: &str = "cache_entries";
    pub const CACHE_WEIGHTED_SIZE_KB: &str = "cache_weighted_size_kb";
    pub const CACHE_EVICTIONS: &str = "cache_evictions_total";
    pub const CACHE_INDEX_LOADING: &str = "cache_index_loading";
    pub const CACHE_LOOKUPS: &str = "cache_lookups_total";
    pub const CACHE_LOOKUP_DURATION: &str = "cache_lookup_duration_seconds";
    pub const BYTES_FROM_CACHE: &str = "cache_bytes_served_total";
    pub const CACHE_OPERATIONS: &str = "cache_operations_total";

    // ============ Download Metrics ============
    pub const BYTES_DOWNLOADED: &str = "download_bytes_total";
    pub const DOWNLOADS_TOTAL: &str = "downloads_total";
    pub const DOWNLOADS_STARTED: &str = "downloads_started_total";
    pub const DOWNLOAD_DURATION_SECONDS: &str = "download_duration_seconds";
    pub const DOWNLOAD_SIZE_BYTES: &str = "download_size_bytes";
    pub const DOWNLOAD_SPEED_BYTES: &str = "download_speed_bytes_per_second";
    pub const REQUESTS_COALESCED: &str = "downloads_coalesced_total";
    pub const ACTIVE_DOWNLOADS: &str = "downloads_active";
    pub const PENDING_FOLLOWERS: &str = "downloads_pending_followers";
    pub const DOWNLOAD_PROGRESS_BYTES: &str = "download_progress_bytes";

    // ============ Upstream Metrics ============
    pub const UPSTREAM_REQUESTS: &str = "upstream_requests_total";
    pub const UPSTREAM_ERRORS: &str = "upstream_errors_total";
    pub const UPSTREAM_DURATION_SECONDS: &str = "upstream_request_duration_seconds";
    pub const UPSTREAM_CONNECT_DURATION: &str = "upstream_connect_duration_seconds";
    pub const UPSTREAM_TTFB_SECONDS: &str = "upstream_time_to_first_byte_seconds";
    pub const NOT_MODIFIED: &str = "upstream_not_modified_total";
    pub const UPSTREAM_RESPONSE_SIZE: &str = "upstream_response_size_bytes";
    pub const UPSTREAM_STATUS: &str = "upstream_response_status_total";
    pub const UPSTREAM_RETRIES: &str = "upstream_retries_total";
    pub const UPSTREAM_TIMEOUTS: &str = "upstream_timeouts_total";

    // ============ Storage Metrics ============
    pub const STORAGE_OPERATIONS: &str = "storage_operations_total";
    pub const STORAGE_OPERATION_DURATION: &str = "storage_operation_duration_seconds";
    pub const STORAGE_BYTES_READ: &str = "storage_bytes_read_total";
    pub const STORAGE_BYTES_WRITTEN: &str = "storage_bytes_written_total";
    pub const STORAGE_ERRORS: &str = "storage_errors_total";
    pub const TEMP_FILES_CREATED: &str = "storage_temp_files_created_total";
    pub const TEMP_FILES_ACTIVE: &str = "storage_temp_files_active";
    pub const ORPHANS_REMOVED: &str = "storage_orphans_removed_total";
    pub const METADATA_PARSE_ERRORS: &str = "storage_metadata_parse_errors_total";
    pub const STORAGE_SPACE_USED_BYTES: &str = "storage_space_used_bytes";

    // ============ TTL Metrics ============
    pub const TTL_APPLIED_SECONDS: &str = "ttl_applied_seconds";
    pub const TTL_EXPIRATIONS: &str = "ttl_expirations_total";
    pub const TTL_REFRESHES: &str = "ttl_refreshes_total";

    // ============ Path Validation Metrics ============
    pub const PATH_VALIDATIONS: &str = "path_validations_total";
    pub const PATH_VALIDATION_FAILURES: &str = "path_validation_failures_total";

    // ============ Repository Metrics ============
    pub const REPO_REQUESTS: &str = "repository_requests_total";
    pub const REPO_BYTES_SERVED: &str = "repository_bytes_served_total";
    pub const REPO_CACHE_HITS: &str = "repository_cache_hits_total";
    pub const REPO_CACHE_MISSES: &str = "repository_cache_misses_total";
    pub const REPO_ERRORS: &str = "repository_errors_total";

    // ============ Process Metrics ============
    pub const PROCESS_START_TIME: &str = "process_start_time_seconds";
    pub const BUILD_INFO: &str = "build_info";
    pub const PROCESS_UPTIME_SECONDS: &str = "process_uptime_seconds";

    // ============ Maintenance Metrics ============
    pub const MAINTENANCE_RUNS: &str = "maintenance_runs_total";
    pub const MAINTENANCE_DURATION: &str = "maintenance_duration_seconds";
    pub const CLEANUP_TEMP_FILES: &str = "cleanup_temp_files_total";
    pub const CLEANUP_ORPHANS: &str = "cleanup_orphans_total";

    // ============ Health Metrics ============
    pub const HEALTH_CHECK_STATUS: &str = "health_check_status";
    pub const LAST_SUCCESSFUL_DOWNLOAD: &str = "last_successful_download_timestamp";
    pub const LAST_CACHE_HIT: &str = "last_cache_hit_timestamp";
}

/// Histogram bucket configurations
mod buckets {
    /// Duration buckets in seconds (1ms to 10min)
    pub const DURATION: &[f64] = &[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
        30.0, 60.0, 120.0, 300.0, 600.0,
    ];

    /// Fast operation buckets (100µs to 1s) for cache lookups
    pub const FAST_DURATION: &[f64] = &[
        0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
    ];

    /// Size buckets in bytes (1KB to 4GB)
    pub const SIZE: &[f64] = &[
        1024.0,            // 1 KB
        4096.0,            // 4 KB
        16384.0,           // 16 KB
        65536.0,           // 64 KB
        262144.0,          // 256 KB
        1048576.0,         // 1 MB
        4194304.0,         // 4 MB
        16777216.0,        // 16 MB
        67108864.0,        // 64 MB
        268435456.0,       // 256 MB
        1073741824.0,      // 1 GB
        4294967296.0,      // 4 GB
    ];

    /// Speed buckets in bytes/second (1 KB/s to 1 GB/s)
    pub const SPEED: &[f64] = &[
        1024.0,          // 1 KB/s
        10240.0,         // 10 KB/s
        102400.0,        // 100 KB/s
        1048576.0,       // 1 MB/s
        10485760.0,      // 10 MB/s
        52428800.0,      // 50 MB/s
        104857600.0,     // 100 MB/s
        524288000.0,     // 500 MB/s
        1073741824.0,    // 1 GB/s
    ];

    /// TTL buckets in seconds (1 hour to 30 days)
    pub const TTL: &[f64] = &[
        3600.0,    // 1 hour
        7200.0,    // 2 hours
        14400.0,   // 4 hours
        28800.0,   // 8 hours
        43200.0,   // 12 hours
        86400.0,   // 1 day
        172800.0,  // 2 days
        604800.0,  // 7 days
        1209600.0, // 14 days
        2592000.0, // 30 days
    ];
}

/// Initializes the metrics system with optional Prometheus endpoint
pub fn init(prometheus_port: Option<u16>) -> anyhow::Result<()> {
    if let Some(port) = prometheus_port {
        let handle = PrometheusBuilder::new()
            // Duration histograms
            .set_buckets_for_metric(Matcher::Suffix("_duration_seconds".to_string()), buckets::DURATION)?
            .set_buckets_for_metric(Matcher::Full(names::CACHE_LOOKUP_DURATION.to_string()), buckets::FAST_DURATION)?
            .set_buckets_for_metric(Matcher::Full(names::UPSTREAM_CONNECT_DURATION.to_string()), buckets::FAST_DURATION)?
            .set_buckets_for_metric(Matcher::Full(names::UPSTREAM_TTFB_SECONDS.to_string()), buckets::FAST_DURATION)?
            // Size histograms
            .set_buckets_for_metric(Matcher::Suffix("_size_bytes".to_string()), buckets::SIZE)?
            .set_buckets_for_metric(Matcher::Suffix("_bytes".to_string()), buckets::SIZE)?
            // Speed histograms
            .set_buckets_for_metric(Matcher::Suffix("_bytes_per_second".to_string()), buckets::SPEED)?
            // TTL histograms
            .set_buckets_for_metric(Matcher::Full(names::TTL_APPLIED_SECONDS.to_string()), buckets::TTL)?
            .with_http_listener(([0, 0, 0, 0], port))
            .install_recorder()?;
        let _ = PROMETHEUS.set(handle);
        info!(port, "Prometheus metrics enabled");
    }

    register_descriptions();
    record_static_metrics();

    Ok(())
}

fn register_descriptions() {
    // ============ HTTP Request Metrics ============
    describe_counter!(names::REQUESTS_TOTAL, "Total HTTP requests processed");
    describe_histogram!(names::REQUEST_DURATION_SECONDS, "HTTP request processing duration in seconds");
    describe_histogram!(names::REQUEST_SIZE_BYTES, "HTTP request body size in bytes");
    describe_histogram!(names::RESPONSE_SIZE_BYTES, "HTTP response body size in bytes");
    describe_gauge!(names::ACTIVE_CONNECTIONS, "Currently active HTTP connections");
    describe_gauge!(names::REQUESTS_IN_FLIGHT, "Currently processing HTTP requests");

    // ============ Cache Metrics ============
    describe_counter!(names::CACHE_HITS, "Total cache hits");
    describe_counter!(names::CACHE_MISSES, "Total cache misses");
    describe_gauge!(names::CACHE_SIZE_BYTES, "Current total cache size in bytes");
    describe_gauge!(names::CACHE_ENTRIES, "Current number of cache entries");
    describe_gauge!(names::CACHE_WEIGHTED_SIZE_KB, "Current weighted cache size in KB");
    describe_counter!(names::CACHE_EVICTIONS, "Total cache evictions by reason");
    describe_gauge!(names::CACHE_INDEX_LOADING, "Whether cache index is loading (1=loading, 0=ready)");
    describe_counter!(names::CACHE_LOOKUPS, "Total cache lookups");
    describe_histogram!(names::CACHE_LOOKUP_DURATION, "Cache lookup duration in seconds");
    describe_counter!(names::BYTES_FROM_CACHE, "Total bytes served from cache");
    describe_counter!(names::CACHE_OPERATIONS, "Total cache operations by type");

    // ============ Download Metrics ============
    describe_counter!(names::BYTES_DOWNLOADED, "Total bytes downloaded from upstream");
    describe_counter!(names::DOWNLOADS_TOTAL, "Total downloads by status");
    describe_counter!(names::DOWNLOADS_STARTED, "Total downloads started");
    describe_histogram!(names::DOWNLOAD_DURATION_SECONDS, "Download duration in seconds");
    describe_histogram!(names::DOWNLOAD_SIZE_BYTES, "Downloaded file size in bytes");
    describe_histogram!(names::DOWNLOAD_SPEED_BYTES, "Download speed in bytes per second");
    describe_counter!(names::REQUESTS_COALESCED, "Requests that joined existing downloads");
    describe_gauge!(names::ACTIVE_DOWNLOADS, "Currently active downloads");
    describe_gauge!(names::PENDING_FOLLOWERS, "Clients waiting for active downloads");
    describe_gauge!(names::DOWNLOAD_PROGRESS_BYTES, "Current download progress in bytes");

    // ============ Upstream Metrics ============
    describe_counter!(names::UPSTREAM_REQUESTS, "Total requests to upstream servers");
    describe_counter!(names::UPSTREAM_ERRORS, "Total upstream request errors by category");
    describe_histogram!(names::UPSTREAM_DURATION_SECONDS, "Upstream request total duration");
    describe_histogram!(names::UPSTREAM_CONNECT_DURATION, "Upstream connection establishment time");
    describe_histogram!(names::UPSTREAM_TTFB_SECONDS, "Time to first byte from upstream");
    describe_counter!(names::NOT_MODIFIED, "304 Not Modified responses received");
    describe_histogram!(names::UPSTREAM_RESPONSE_SIZE, "Upstream response size in bytes");
    describe_counter!(names::UPSTREAM_STATUS, "Upstream responses by status code");
    describe_counter!(names::UPSTREAM_RETRIES, "Upstream request retries");
    describe_counter!(names::UPSTREAM_TIMEOUTS, "Upstream request timeouts");

    // ============ Storage Metrics ============
    describe_counter!(names::STORAGE_OPERATIONS, "Total storage operations by type");
    describe_histogram!(names::STORAGE_OPERATION_DURATION, "Storage operation duration");
    describe_counter!(names::STORAGE_BYTES_READ, "Total bytes read from storage");
    describe_counter!(names::STORAGE_BYTES_WRITTEN, "Total bytes written to storage");
    describe_counter!(names::STORAGE_ERRORS, "Storage errors by type");
    describe_counter!(names::TEMP_FILES_CREATED, "Total temporary files created");
    describe_gauge!(names::TEMP_FILES_ACTIVE, "Currently active temporary files");
    describe_counter!(names::ORPHANS_REMOVED, "Total orphaned files removed");
    describe_counter!(names::METADATA_PARSE_ERRORS, "Metadata parsing errors");
    describe_gauge!(names::STORAGE_SPACE_USED_BYTES, "Total storage space used");

    // ============ TTL Metrics ============
    describe_histogram!(names::TTL_APPLIED_SECONDS, "TTL values applied to cache entries");
    describe_counter!(names::TTL_EXPIRATIONS, "Cache entries expired by TTL");
    describe_counter!(names::TTL_REFRESHES, "TTL refreshes via touch operation");

    // ============ Path Validation Metrics ============
    describe_counter!(names::PATH_VALIDATIONS, "Total path validation attempts");
    describe_counter!(names::PATH_VALIDATION_FAILURES, "Path validation failures by reason");

    // ============ Repository Metrics ============
    describe_counter!(names::REPO_REQUESTS, "Requests per repository");
    describe_counter!(names::REPO_BYTES_SERVED, "Bytes served per repository");
    describe_counter!(names::REPO_CACHE_HITS, "Cache hits per repository");
    describe_counter!(names::REPO_CACHE_MISSES, "Cache misses per repository");
    describe_counter!(names::REPO_ERRORS, "Errors per repository");

    // ============ Process Metrics ============
    describe_gauge!(names::PROCESS_START_TIME, "Unix timestamp when process started");
    describe_gauge!(names::BUILD_INFO, "Build information with version labels");
    describe_gauge!(names::PROCESS_UPTIME_SECONDS, "Process uptime in seconds");

    // ============ Maintenance Metrics ============
    describe_counter!(names::MAINTENANCE_RUNS, "Total maintenance task runs");
    describe_histogram!(names::MAINTENANCE_DURATION, "Maintenance task duration");
    describe_counter!(names::CLEANUP_TEMP_FILES, "Temp files cleaned up");
    describe_counter!(names::CLEANUP_ORPHANS, "Orphan files cleaned up");

    // ============ Health Metrics ============
    describe_gauge!(names::HEALTH_CHECK_STATUS, "Health check status (1=healthy, 0=unhealthy)");
    describe_gauge!(names::LAST_SUCCESSFUL_DOWNLOAD, "Timestamp of last successful download");
    describe_gauge!(names::LAST_CACHE_HIT, "Timestamp of last cache hit");
}

fn record_static_metrics() {
    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    gauge!(names::PROCESS_START_TIME).set(start_time);

    gauge!(
        names::BUILD_INFO,
        "version" => env!("CARGO_PKG_VERSION"),
        "name" => env!("CARGO_PKG_NAME")
    )
    .set(1.0);

    gauge!(names::HEALTH_CHECK_STATUS).set(1.0);
}

// ============ HTTP Request Metrics ============

static REQUESTS_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);

/// Records an HTTP request with full details
#[inline]
pub fn record_request(
    method: &str,
    status: u16,
    repo: &str,
    duration: Duration,
    request_size: u64,
    response_size: u64,
) {
    let status_class = format!("{}xx", status / 100);

    counter!(
        names::REQUESTS_TOTAL,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "status_class" => status_class.clone(),
        "repo" => repo.to_string()
    )
    .increment(1);

    histogram!(
        names::REQUEST_DURATION_SECONDS,
        "method" => method.to_string(),
        "status_class" => status_class,
        "repo" => repo.to_string()
    )
    .record(duration.as_secs_f64());

    if request_size > 0 {
        histogram!(names::REQUEST_SIZE_BYTES).record(request_size as f64);
    }

    histogram!(names::RESPONSE_SIZE_BYTES, "repo" => repo.to_string()).record(response_size as f64);
}

/// Increments requests in flight
pub fn inc_requests_in_flight() {
    let val = REQUESTS_IN_FLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
    gauge!(names::REQUESTS_IN_FLIGHT).set(val as f64);
}

/// Decrements requests in flight
pub fn dec_requests_in_flight() {
    let val = REQUESTS_IN_FLIGHT
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
        .unwrap_or(1)
        .saturating_sub(1);
    gauge!(names::REQUESTS_IN_FLIGHT).set(val as f64);
}

/// Increments active connections
pub fn inc_connections() {
    let val = ACTIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
    gauge!(names::ACTIVE_CONNECTIONS).set(val as f64);
}

/// Decrements active connections
pub fn dec_connections() {
    let val = ACTIVE_CONNECTIONS
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
        .unwrap_or(1)
        .saturating_sub(1);
    gauge!(names::ACTIVE_CONNECTIONS).set(val as f64);
}

// ============ Cache Metrics ============

/// Records a cache hit with the number of bytes served
#[inline]
pub fn record_hit(bytes: u64) {
    counter!(names::CACHE_HITS).increment(1);
    counter!(names::BYTES_FROM_CACHE).increment(bytes);
    
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    gauge!(names::LAST_CACHE_HIT).set(now);
}

/// Records a cache hit for a specific repository
#[inline]
pub fn record_repo_hit(repo: &str, bytes: u64) {
    record_hit(bytes);
    counter!(names::REPO_CACHE_HITS, "repo" => repo.to_string()).increment(1);
    counter!(names::REPO_BYTES_SERVED, "repo" => repo.to_string()).increment(bytes);
}

/// Records a cache miss
#[inline]
pub fn record_miss() {
    counter!(names::CACHE_MISSES).increment(1);
}

/// Records a cache miss for a specific repository
#[inline]
pub fn record_repo_miss(repo: &str) {
    record_miss();
    counter!(names::REPO_CACHE_MISSES, "repo" => repo.to_string()).increment(1);
}

/// Updates cache size and entry count gauges
#[inline]
pub fn set_cache_stats(size_bytes: u64, entry_count: u64, weighted_size_kb: u64) {
    gauge!(names::CACHE_SIZE_BYTES).set(size_bytes as f64);
    gauge!(names::CACHE_ENTRIES).set(entry_count as f64);
    gauge!(names::CACHE_WEIGHTED_SIZE_KB).set(weighted_size_kb as f64);
}

/// Records a cache eviction
#[inline]
pub fn record_eviction(reason: &str) {
    counter!(names::CACHE_EVICTIONS, "reason" => reason.to_string()).increment(1);
}

/// Sets the cache index loading state
#[inline]
pub fn set_cache_loading(loading: bool) {
    gauge!(names::CACHE_INDEX_LOADING).set(if loading { 1.0 } else { 0.0 });
}

/// Records a cache lookup
#[inline]
pub fn record_cache_lookup(hit: bool, duration: Duration) {
    let result = if hit { "hit" } else { "miss" };
    counter!(names::CACHE_LOOKUPS, "result" => result).increment(1);
    histogram!(names::CACHE_LOOKUP_DURATION, "result" => result).record(duration.as_secs_f64());
}

/// Records a cache operation
#[inline]
pub fn record_cache_operation(operation: &str) {
    counter!(names::CACHE_OPERATIONS, "operation" => operation.to_string()).increment(1);
}

// ============ Download Metrics ============

/// Records bytes downloaded from upstream
#[inline]
pub fn record_download(bytes: u64) {
    counter!(names::BYTES_DOWNLOADED).increment(bytes);
}

/// Records a download started
#[inline]
pub fn record_download_started() {
    counter!(names::DOWNLOADS_STARTED).increment(1);
}

/// Records a completed download with full details
#[inline]
pub fn record_download_complete(bytes: u64, duration: Duration, repo: &str) {
    counter!(names::DOWNLOADS_TOTAL, "status" => "success", "repo" => repo.to_string()).increment(1);
    counter!(names::BYTES_DOWNLOADED).increment(bytes);
    histogram!(names::DOWNLOAD_DURATION_SECONDS, "repo" => repo.to_string()).record(duration.as_secs_f64());
    histogram!(names::DOWNLOAD_SIZE_BYTES, "repo" => repo.to_string()).record(bytes as f64);

    if duration.as_secs_f64() > 0.0 {
        let speed = bytes as f64 / duration.as_secs_f64();
        histogram!(names::DOWNLOAD_SPEED_BYTES, "repo" => repo.to_string()).record(speed);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64();
    gauge!(names::LAST_SUCCESSFUL_DOWNLOAD).set(now);
}

/// Records a failed download
#[inline]
pub fn record_download_failed(reason: &str, repo: &str) {
    counter!(
        names::DOWNLOADS_TOTAL,
        "status" => "failed",
        "reason" => reason.to_string(),
        "repo" => repo.to_string()
    )
    .increment(1);
    counter!(names::REPO_ERRORS, "repo" => repo.to_string(), "type" => "download").increment(1);
}

/// Records a coalesced request (joined existing download)
#[inline]
pub fn record_coalesced() {
    counter!(names::REQUESTS_COALESCED).increment(1);
}

/// Increments the active downloads counter
pub fn inc_active() {
    let val = ACTIVE_DOWNLOADS.fetch_add(1, Ordering::Relaxed) + 1;
    gauge!(names::ACTIVE_DOWNLOADS).set(val as f64);
}

/// Decrements the active downloads counter
pub fn dec_active() {
    let val = ACTIVE_DOWNLOADS
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
        .unwrap_or(1)
        .saturating_sub(1);
    gauge!(names::ACTIVE_DOWNLOADS).set(val as f64);
}

/// Increments pending followers
pub fn inc_followers() {
    let val = PENDING_FOLLOWERS.fetch_add(1, Ordering::Relaxed) + 1;
    gauge!(names::PENDING_FOLLOWERS).set(val as f64);
}

/// Decrements pending followers
pub fn dec_followers() {
    let val = PENDING_FOLLOWERS
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
        .unwrap_or(1)
        .saturating_sub(1);
    gauge!(names::PENDING_FOLLOWERS).set(val as f64);
}

/// Updates download progress
pub fn set_download_progress(key: &str, bytes: u64) {
    gauge!(names::DOWNLOAD_PROGRESS_BYTES, "key" => key.to_string()).set(bytes as f64);
}

/// Clears download progress
pub fn clear_download_progress(key: &str) {
    gauge!(names::DOWNLOAD_PROGRESS_BYTES, "key" => key.to_string()).set(0.0);
}

// ============ Upstream Metrics ============

/// Records a 304 Not Modified response
#[inline]
pub fn record_304() {
    counter!(names::NOT_MODIFIED).increment(1);
    counter!(names::UPSTREAM_STATUS, "status" => "304").increment(1);
}

/// Records an upstream error
#[inline]
pub fn record_error() {
    counter!(names::UPSTREAM_ERRORS, "category" => "general").increment(1);
}

/// Records an upstream error with category
#[inline]
pub fn record_upstream_error(category: &str, repo: &str) {
    counter!(names::UPSTREAM_ERRORS, "category" => category.to_string(), "repo" => repo.to_string()).increment(1);
    counter!(names::REPO_ERRORS, "repo" => repo.to_string(), "type" => "upstream").increment(1);
}

/// Records a completed upstream request
#[inline]
pub fn record_upstream_request(
    status: u16,
    duration: Duration,
    size: Option<u64>,
    repo: &str,
) {
    counter!(
        names::UPSTREAM_REQUESTS,
        "status" => status.to_string(),
        "repo" => repo.to_string()
    )
    .increment(1);
    counter!(names::UPSTREAM_STATUS, "status" => status.to_string()).increment(1);
    histogram!(
        names::UPSTREAM_DURATION_SECONDS,
        "repo" => repo.to_string()
    )
    .record(duration.as_secs_f64());

    if let Some(s) = size {
        histogram!(names::UPSTREAM_RESPONSE_SIZE, "repo" => repo.to_string()).record(s as f64);
    }
}

/// Records upstream connection time
#[inline]
pub fn record_upstream_connect_time(duration: Duration) {
    histogram!(names::UPSTREAM_CONNECT_DURATION).record(duration.as_secs_f64());
}

/// Records time to first byte from upstream
#[inline]
pub fn record_upstream_ttfb(duration: Duration) {
    histogram!(names::UPSTREAM_TTFB_SECONDS).record(duration.as_secs_f64());
}

/// Records an upstream retry
#[inline]
pub fn record_upstream_retry(repo: &str) {
    counter!(names::UPSTREAM_RETRIES, "repo" => repo.to_string()).increment(1);
}

/// Records an upstream timeout
#[inline]
pub fn record_upstream_timeout(repo: &str) {
    counter!(names::UPSTREAM_TIMEOUTS, "repo" => repo.to_string()).increment(1);
}

// ============ Storage Metrics ============

static TEMP_FILES_ACTIVE: AtomicU64 = AtomicU64::new(0);

/// Records a storage operation
#[inline]
pub fn record_storage_operation(operation: &str, duration: Duration, success: bool) {
    let status = if success { "success" } else { "error" };
    counter!(
        names::STORAGE_OPERATIONS,
        "operation" => operation.to_string(),
        "status" => status
    )
    .increment(1);
    histogram!(
        names::STORAGE_OPERATION_DURATION,
        "operation" => operation.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Records bytes read from storage
#[inline]
pub fn record_storage_read(bytes: u64) {
    counter!(names::STORAGE_BYTES_READ).increment(bytes);
}

/// Records bytes written to storage
#[inline]
pub fn record_storage_write(bytes: u64) {
    counter!(names::STORAGE_BYTES_WRITTEN).increment(bytes);
}

/// Records a storage error
#[inline]
pub fn record_storage_error(error_type: &str) {
    counter!(names::STORAGE_ERRORS, "type" => error_type.to_string()).increment(1);
}

/// Records a temporary file creation
#[inline]
pub fn record_temp_file_created() {
    counter!(names::TEMP_FILES_CREATED).increment(1);
    let val = TEMP_FILES_ACTIVE.fetch_add(1, Ordering::Relaxed) + 1;
    gauge!(names::TEMP_FILES_ACTIVE).set(val as f64);
}

/// Records a temporary file removal
#[inline]
pub fn record_temp_file_removed() {
    let val = TEMP_FILES_ACTIVE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
        .unwrap_or(1)
        .saturating_sub(1);
    gauge!(names::TEMP_FILES_ACTIVE).set(val as f64);
}

/// Records orphaned files removed
#[inline]
pub fn record_orphans_removed(count: u64) {
    counter!(names::ORPHANS_REMOVED).increment(count);
    counter!(names::CLEANUP_ORPHANS).increment(count);
}

/// Records a metadata parse error
#[inline]
pub fn record_metadata_parse_error() {
    counter!(names::METADATA_PARSE_ERRORS).increment(1);
}

/// Updates storage space used
#[inline]
pub fn set_storage_space_used(bytes: u64) {
    gauge!(names::STORAGE_SPACE_USED_BYTES).set(bytes as f64);
}

// ============ TTL Metrics ============

/// Records a TTL value applied
#[inline]
pub fn record_ttl_applied(ttl_seconds: u64, path_pattern: &str) {
    histogram!(names::TTL_APPLIED_SECONDS, "pattern" => path_pattern.to_string())
        .record(ttl_seconds as f64);
}

/// Records a TTL expiration
#[inline]
pub fn record_ttl_expiration() {
    counter!(names::TTL_EXPIRATIONS).increment(1);
    record_eviction("expired");
}

/// Records a TTL refresh
#[inline]
pub fn record_ttl_refresh() {
    counter!(names::TTL_REFRESHES).increment(1);
}

// ============ Path Validation Metrics ============

/// Records a path validation attempt
#[inline]
pub fn record_path_validation(success: bool, failure_reason: Option<&str>) {
    counter!(names::PATH_VALIDATIONS, "result" => if success { "success" } else { "failure" }).increment(1);
    if let Some(reason) = failure_reason {
        counter!(names::PATH_VALIDATION_FAILURES, "reason" => reason.to_string()).increment(1);
    }
}

// ============ Repository Metrics ============

/// Records a request to a repository
#[inline]
pub fn record_repo_request(repo: &str) {
    counter!(names::REPO_REQUESTS, "repo" => repo.to_string()).increment(1);
}

/// Records bytes served for a repository
#[inline]
pub fn record_repo_bytes_served(repo: &str, bytes: u64) {
    counter!(names::REPO_BYTES_SERVED, "repo" => repo.to_string()).increment(bytes);
}

/// Records a repository error
#[inline]
pub fn record_repo_error(repo: &str, error_type: &str) {
    counter!(
        names::REPO_ERRORS,
        "repo" => repo.to_string(),
        "type" => error_type.to_string()
    )
    .increment(1);
}

// ============ Maintenance Metrics ============

/// Records a maintenance run
#[inline]
pub fn record_maintenance_run(task: &str, duration: Duration) {
    counter!(names::MAINTENANCE_RUNS, "task" => task.to_string()).increment(1);
    histogram!(names::MAINTENANCE_DURATION, "task" => task.to_string()).record(duration.as_secs_f64());
}

/// Records temp files cleaned up
#[inline]
pub fn record_temp_cleanup(count: u64) {
    counter!(names::CLEANUP_TEMP_FILES).increment(count);
}

// ============ Health Metrics ============

/// Sets health check status
#[inline]
pub fn set_health_status(healthy: bool) {
    gauge!(names::HEALTH_CHECK_STATUS).set(if healthy { 1.0 } else { 0.0 });
}

// ============ Uptime ============

/// Updates process uptime gauge
pub fn update_uptime(start_time: Instant) {
    gauge!(names::PROCESS_UPTIME_SECONDS).set(start_time.elapsed().as_secs_f64());
}

// ============ Utility Functions ============

/// Renders metrics in Prometheus text format
pub fn render() -> Option<String> {
    PROMETHEUS.get().map(|h| h.render())
}

/// Returns the current number of active downloads
pub fn active_downloads() -> u64 {
    ACTIVE_DOWNLOADS.load(Ordering::Relaxed)
}

/// Returns the current number of active connections
pub fn active_connections() -> u64 {
    ACTIVE_CONNECTIONS.load(Ordering::Relaxed)
}

/// Returns the current number of requests in flight
pub fn requests_in_flight() -> u64 {
    REQUESTS_IN_FLIGHT.load(Ordering::Relaxed)
}

// ============ RAII Guards ============

/// RAII guard for timing storage operations
pub struct StorageTimer {
    start: Instant,
    operation: &'static str,
    success: bool,
}

impl StorageTimer {
    pub fn new(operation: &'static str) -> Self {
        Self {
            start: Instant::now(),
            operation,
            success: true,
        }
    }

    pub fn set_error(&mut self) {
        self.success = false;
    }

    pub fn success(mut self) {
        self.success = true;
    }
}

impl Drop for StorageTimer {
    fn drop(&mut self) {
        record_storage_operation(self.operation, self.start.elapsed(), self.success);
    }
}

/// RAII guard for tracking download lifecycle
pub struct DownloadGuard {
    start: Instant,
    bytes: u64,
    repo: String,
    completed: bool,
}

impl DownloadGuard {
    pub fn new(repo: &str) -> Self {
        inc_active();
        record_download_started();
        Self {
            start: Instant::now(),
            bytes: 0,
            repo: repo.to_string(),
            completed: false,
        }
    }

    pub fn set_bytes(&mut self, bytes: u64) {
        self.bytes = bytes;
    }

    pub fn complete(mut self) {
        self.completed = true;
        record_download_complete(self.bytes, self.start.elapsed(), &self.repo);
    }

    pub fn complete_304(mut self) {
        self.completed = true;
        record_304();
    }

    pub fn fail(mut self, reason: &str) {
        self.completed = true;
        record_download_failed(reason, &self.repo);
    }
}

impl Drop for DownloadGuard {
    fn drop(&mut self) {
        dec_active();
        if !self.completed {
            record_download_failed("dropped", &self.repo);
        }
    }
}

/// RAII guard for tracking request lifecycle
pub struct RequestGuard {
    start: Instant,
    method: String,
    repo: String,
    request_size: u64,
}

impl RequestGuard {
    pub fn new(method: &str, repo: &str) -> Self {
        inc_requests_in_flight();
        record_repo_request(repo);
        Self {
            start: Instant::now(),
            method: method.to_string(),
            repo: repo.to_string(),
            request_size: 0,
        }
    }

    pub fn set_request_size(&mut self, size: u64) {
        self.request_size = size;
    }

    pub fn complete(self, status: u16, response_size: u64) {
        dec_requests_in_flight();
        record_request(
            &self.method,
            status,
            &self.repo,
            self.start.elapsed(),
            self.request_size,
            response_size,
        );
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        // Handled by complete() - this is a fallback for panics
    }
}

/// RAII guard for connection tracking
pub struct ConnectionGuard;

impl ConnectionGuard {
    pub fn new() -> Self {
        inc_connections();
        Self
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        dec_connections();
    }
}

/// RAII guard for follower tracking
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

/// RAII guard for temp file tracking
pub struct TempFileGuard {
    removed: bool,
}

impl TempFileGuard {
    pub fn new() -> Self {
        record_temp_file_created();
        Self { removed: false }
    }

    pub fn removed(mut self) {
        self.removed = true;
        record_temp_file_removed();
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.removed {
            record_temp_file_removed();
        }
    }
}

/// RAII guard for cache lookup timing
pub struct CacheLookupTimer {
    start: Instant,
    hit: bool,
}

impl CacheLookupTimer {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            hit: false,
        }
    }

    pub fn set_hit(&mut self, hit: bool) {
        self.hit = hit;
    }

    pub fn complete(self, hit: bool) -> Self {
        record_cache_lookup(hit, self.start.elapsed());
        Self { hit, ..self }
    }
}

impl Drop for CacheLookupTimer {
    fn drop(&mut self) {
        // Metrics recorded in complete()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_counters() {
        // Test that atomic operations work correctly
        inc_active();
        assert_eq!(active_downloads(), 1);
        inc_active();
        assert_eq!(active_downloads(), 2);
        dec_active();
        assert_eq!(active_downloads(), 1);
        dec_active();
        assert_eq!(active_downloads(), 0);
        // Should not go below 0
        dec_active();
        assert_eq!(active_downloads(), 0);
    }

    #[test]
    fn test_connection_guard() {
        {
            let _guard = ConnectionGuard::new();
            assert_eq!(active_connections(), 1);
        }
        // Guard dropped, counter should be decremented
        // Note: In tests without init(), the gauge might not work
    }
}