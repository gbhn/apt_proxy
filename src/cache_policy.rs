use crate::config::CacheSettings;
use crate::storage::CacheMetadata;
use http_cache_semantics::CachePolicy;
use std::time::{Duration, SystemTime};
use tracing::debug;
use http::{Request, Response};

#[derive(Clone)]
pub struct CachedEntry {
    pub metadata: CacheMetadata,
    pub policy: CachePolicy,
    pub stored_at: SystemTime,
}

impl CachedEntry {
    pub fn new(metadata: CacheMetadata) -> Option<Self> {
        let request = Request::builder()
            .method("GET")
            .uri(&metadata.original_url)
            .body(())
            .ok()?;

        let mut response_builder = Response::builder()
            .status(200);

        if let Some(headers) = response_builder.headers_mut() {
            *headers = metadata.headers.clone();
        }

        let response = response_builder.body(()).ok()?;
        let policy = CachePolicy::new(&request, &response);

        Some(Self {
            metadata,
            policy,
            stored_at: SystemTime::UNIX_EPOCH + Duration::from_secs(metadata.stored_at),
        })
    }

    pub fn is_fresh(&self) -> bool {
        !self.policy.is_stale(SystemTime::now())
    }

    pub fn can_serve_stale(&self, settings: &CacheSettings) -> bool {
        if self.is_fresh() {
            return true;
        }

        let now = SystemTime::now();
        let age = now.duration_since(self.stored_at).unwrap_or_default();
        
        let ttl_secs = calculate_ttl(
            &self.metadata.headers,
            self.metadata.key.as_deref().unwrap_or(""),
            settings
        );
        let ttl = Duration::from_secs(ttl_secs);

        let stale_duration = age.saturating_sub(ttl);
        stale_duration.as_secs() <= settings.stale_while_revalidate
    }

    pub fn ttl(&self) -> Duration {
        self.policy.time_to_live(SystemTime::now())
    }

    pub fn needs_revalidation(&self, settings: &CacheSettings) -> bool {
        if settings.validation.always_revalidate {
            return true;
        }
        !self.is_fresh()
    }

    pub fn get_validation_headers(&self, settings: &CacheSettings) -> Vec<(String, String)> {
        let mut headers = Vec::new();

        if settings.validation.use_etag {
            if let Some(etag) = &self.metadata.etag {
                headers.push(("if-none-match".to_string(), etag.clone()));
            }
        }

        if settings.validation.use_last_modified {
            if let Some(last_modified) = &self.metadata.last_modified {
                headers.push(("if-modified-since".to_string(), last_modified.clone()));
            }
        }

        headers
    }
}

pub fn calculate_ttl(
    headers: &axum::http::HeaderMap,
    path: &str,
    settings: &CacheSettings,
) -> u64 {
    let override_ttl = settings.get_ttl_for_path(path);

    if settings.ignore_cache_control {
        debug!(
            path = %crate::logging::fields::path(path),
            ttl = override_ttl,
            "Using override TTL (ignoring upstream headers)"
        );
        return override_ttl;
    }

    let request = Request::builder()
        .method("GET")
        .uri("http://localhost")
        .body(())
        .unwrap();

    let mut response_builder = Response::builder()
        .status(200);
        
    if let Some(h) = response_builder.headers_mut() {
        *h = headers.clone();
    }
    let response = response_builder.body(()).unwrap();

    let policy = CachePolicy::new(&request, &response);
    let upstream_ttl = policy.time_to_live(SystemTime::now()).as_secs();

    let ttl = if upstream_ttl > 0 {
        settings.clamp_ttl(upstream_ttl)
    } else {
        override_ttl
    };

    debug!(
        path = %crate::logging::fields::path(path),
        upstream_ttl = upstream_ttl,
        final_ttl = ttl,
        "Calculated TTL"
    );

    ttl
}

pub fn is_cache_valid(
    metadata: &CacheMetadata,
    settings: &CacheSettings,
) -> bool {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let age = now.saturating_sub(metadata.stored_at);
    let ttl = calculate_ttl(&metadata.headers, metadata.key.as_deref().unwrap_or(""), settings);

    age < ttl
}

pub fn is_not_modified_response(status: u16) -> bool {
    status == 304
}

pub fn parse_http_date(date_str: &str) -> Option<SystemTime> {
    httpdate::parse_http_date(date_str).ok()
}

pub fn format_http_date(time: SystemTime) -> String {
    httpdate::fmt_http_date(time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheSettings;

    #[test]
    fn test_calculate_ttl_with_cache_control() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "cache-control",
            "max-age=3600".parse().unwrap(),
        );

        let settings = CacheSettings::default();
        let ttl = calculate_ttl(&headers, "test/file.deb", &settings);

        assert!(ttl >= settings.min_ttl);
    }

    #[test]
    fn test_calculate_ttl_pattern_override() {
        let headers = axum::http::HeaderMap::new();
        let mut settings = CacheSettings::default();
        
        settings.ttl_overrides.push(crate::config::TtlOverride {
            pattern: r"\.deb$".to_string(),
            ttl: 2592000,
            regex: Some(regex::Regex::new(r"\.deb$").unwrap()),
        });

        let ttl = calculate_ttl(&headers, "ubuntu/pool/main/test_1.0_amd64.deb", &settings);
        
        assert_eq!(ttl, settings.max_ttl.min(2592000));
    }
}