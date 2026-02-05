use crate::config::CacheSettings;
use crate::storage::CacheMetadata;
use http::{Request, Response};
use http_cache_semantics::CachePolicy;
use std::time::{Duration, SystemTime};
use tracing::debug;

/// Единая структура для проверки валидности кэша
pub struct CacheValidity<'a> {
    metadata: &'a CacheMetadata,
    settings: &'a CacheSettings,
    now: u64,
}

impl<'a> CacheValidity<'a> {
    pub fn new(metadata: &'a CacheMetadata, settings: &'a CacheSettings) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self { metadata, settings, now }
    }

    /// Возраст записи в секундах
    #[inline]
    pub fn age(&self) -> u64 {
        self.now.saturating_sub(self.metadata.stored_at)
    }

    /// TTL для этой записи
    #[inline]
    pub fn ttl(&self) -> u64 {
        calculate_ttl(
            &self.metadata.headers,
            self.metadata.key.as_deref().unwrap_or(""),
            self.settings,
        )
    }

    /// Запись ещё свежая (не истёк TTL)
    #[inline]
    pub fn is_fresh(&self) -> bool {
        self.age() < self.ttl()
    }

    /// Запись устарела, но может быть использована (stale-while-revalidate)
    #[inline]
    pub fn is_stale_but_usable(&self) -> bool {
        if self.is_fresh() {
            return true;
        }
        let stale_duration = self.age().saturating_sub(self.ttl());
        stale_duration <= self.settings.stale_while_revalidate
    }

    /// Требуется ревалидация
    #[inline]
    pub fn needs_revalidation(&self) -> bool {
        if self.settings.validation.always_revalidate {
            return true;
        }
        !self.is_fresh()
    }

    /// Заголовки для условного запроса
    pub fn validation_headers(&self) -> Vec<(String, String)> {
        let mut headers = Vec::new();

        if self.settings.validation.use_etag {
            if let Some(ref etag) = self.metadata.etag {
                headers.push(("If-None-Match".to_string(), etag.clone()));
            }
        }

        if self.settings.validation.use_last_modified {
            if let Some(ref last_modified) = self.metadata.last_modified {
                headers.push(("If-Modified-Since".to_string(), last_modified.clone()));
            }
        }

        headers
    }

    /// Создаёт CachePolicy для более сложной логики HTTP кэширования
    pub fn as_policy(&self) -> Option<CachePolicy> {
        let request = Request::builder()
            .method("GET")
            .uri(&self.metadata.original_url)
            .body(())
            .ok()?;

        let mut response_builder = Response::builder().status(200);
        if let Some(headers) = response_builder.headers_mut() {
            *headers = self.metadata.headers.clone();
        }
        let response = response_builder.body(()).ok()?;

        Some(CachePolicy::new(&request, &response))
    }

    /// Оставшееся время жизни
    pub fn time_to_live(&self) -> Duration {
        let ttl = self.ttl();
        let age = self.age();
        if age >= ttl {
            Duration::ZERO
        } else {
            Duration::from_secs(ttl - age)
        }
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

    let mut response_builder = Response::builder().status(200);

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

/// Проверяет, валидна ли запись кэша
#[inline]
pub fn is_cache_valid(metadata: &CacheMetadata, settings: &CacheSettings) -> bool {
    CacheValidity::new(metadata, settings).is_fresh()
}

/// Проверяет, можно ли использовать устаревшую запись
#[inline]
pub fn can_serve_stale(metadata: &CacheMetadata, settings: &CacheSettings) -> bool {
    CacheValidity::new(metadata, settings).is_stale_but_usable()
}

/// Проверяет, требуется ли ревалидация
#[inline]
pub fn needs_revalidation(metadata: &CacheMetadata, settings: &CacheSettings) -> bool {
    CacheValidity::new(metadata, settings).needs_revalidation()
}

/// Получает заголовки для условного запроса
#[inline]
pub fn get_validation_headers(metadata: &CacheMetadata, settings: &CacheSettings) -> Vec<(String, String)> {
    CacheValidity::new(metadata, settings).validation_headers()
}

#[inline]
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
        headers.insert("cache-control", "max-age=3600".parse().unwrap());

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

    #[test]
    fn test_cache_validity() {
        let settings = CacheSettings::default();
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metadata = CacheMetadata {
            headers: axum::http::HeaderMap::new(),
            original_url: "http://example.com/test".to_string(),
            key: Some("test".to_string()),
            stored_at: now - 100, // 100 seconds ago
            content_length: 1000,
            etag: Some("\"abc123\"".to_string()),
            last_modified: None,
        };

        let validity = CacheValidity::new(&metadata, &settings);
        assert!(validity.is_fresh()); // default_ttl is 86400
        assert_eq!(validity.age(), 100);
    }
}