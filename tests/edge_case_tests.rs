use apt_cacher_rs::*;
use axum::{body::Body, http::{Request, StatusCode}};
use std::collections::HashMap;
use tempfile::TempDir;
use tower::util::ServiceExt;


#[test]
fn test_validate_path_exact_limit() {
    use apt_cacher_rs::utils::validate_path;
    
    let path_2048 = "a".repeat(2048);
    assert!(validate_path(&path_2048).is_ok());
    
    let path_2049 = "a".repeat(2049);
    assert!(validate_path(&path_2049).is_err());
}

#[test]
fn test_validate_path_unicode() {
    use apt_cacher_rs::utils::validate_path;
    
    assert!(validate_path("путь/файл.txt").is_ok());
    assert!(validate_path("路径/文件.txt").is_ok());
    assert!(validate_path("🚀/file.txt").is_ok());
}

#[test]
fn test_validate_path_special_characters() {
    use apt_cacher_rs::utils::validate_path;
    
    assert!(validate_path("file-name.txt").is_ok());
    assert!(validate_path("file_name.txt").is_ok());
    assert!(validate_path("file+name.txt").is_ok());
    assert!(validate_path("file@name.txt").is_ok());
    assert!(validate_path("file#name.txt").is_ok());
}

#[test]
fn test_format_size_zero() {
    use apt_cacher_rs::utils::format_size;
    assert_eq!(format_size(0), "0 B");
}

#[test]
fn test_format_size_edge_values() {
    use apt_cacher_rs::utils::format_size;
    
    assert_eq!(format_size(1023), "1023 B");
    assert_eq!(format_size(1024), "1.00 KB");
    assert_eq!(format_size(1_048_575), "1024.00 KB");
    assert_eq!(format_size(1_048_576), "1.00 MB");
}

#[test]
fn test_format_size_max_u64() {
    use apt_cacher_rs::utils::format_size;
    
    let max_size = u64::MAX;
    let result = format_size(max_size);
    
    assert!(result.contains("GB"));
}

#[test]
fn test_cache_path_empty_input() {
    use apt_cacher_rs::utils::cache_path_for;
    use std::path::Path;
    
    let base = Path::new("/cache");
    let path = cache_path_for(base, "");
    
    assert!(path.starts_with(base));
    assert!(path.components().count() >= 4);
}

#[test]
fn test_cache_path_very_long_input() {
    use apt_cacher_rs::utils::cache_path_for;
    use std::path::Path;
    
    let base = Path::new("/cache");
    let long_path = "a".repeat(10000);
    let path = cache_path_for(base, &long_path);
    
    assert!(path.starts_with(base));
}


#[tokio::test]
async fn test_config_parse_size_edge_cases() {
    use apt_cacher_rs::config::ConfigFile;
    
    assert_eq!(ConfigFile::parse_size("0GB"), Some(0));
    assert_eq!(ConfigFile::parse_size("0 MB"), Some(0));
    assert_eq!(ConfigFile::parse_size("1B"), Some(1));
    
    assert_eq!(
        ConfigFile::parse_size("1024GB"),
        Some(1024 * 1_073_741_824)
    );
}

#[tokio::test]
async fn test_config_parse_size_whitespace() {
    use apt_cacher_rs::config::ConfigFile;
    
    assert_eq!(ConfigFile::parse_size("  10GB  "), Some(10_737_418_240));
    assert_eq!(ConfigFile::parse_size("10  GB"), Some(10_737_418_240));
    assert_eq!(ConfigFile::parse_size("  10  GB  "), Some(10_737_418_240));
}

#[tokio::test]
async fn test_settings_empty_repositories() {
    use apt_cacher_rs::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    
    let mut temp_file = NamedTempFile::new().unwrap();
    write!(temp_file, "repositories: {{}}").unwrap();
    temp_file.flush().unwrap();
    
    let args = Args {
        config: Some(temp_file.path().to_path_buf()),
        port: None,
        socket: None,
        cache_dir: None,
        max_cache_size: None,
        max_lru_entries: None,
    };
    
    let settings = Settings::load(args).await.unwrap();
    assert!(settings.repositories.is_empty());
}


#[tokio::test]
async fn test_cache_zero_max_size() {
    use tempfile::TempDir;
    
    let temp_dir = TempDir::new().unwrap();
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 0, 
        max_lru_entries: 100,
    };
    
    let result = cache::CacheManager::new(settings).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_cache_one_lru_entry() {
    use tempfile::TempDir;
    use axum::http::HeaderMap;
    
    let temp_dir = TempDir::new().unwrap();
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 1024 * 1024,
        max_lru_entries: 1, 
    };
    
    let cache = cache::CacheManager::new(settings).await.unwrap();
    
    for i in 0..2 {
        let data = vec![0u8; 100];
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        
        let meta = cache::CacheMetadata {
            headers,
            original_url_path: format!("test/file{}.txt", i),
        };
        
        cache.store(&format!("test/file{}.txt", i), &data, &meta)
            .await
            .unwrap();
    }
    
    let stats = cache.get_stats().await;
    assert!(stats.contains("LRU Entries: 1"));
}

#[tokio::test]
async fn test_cache_metadata_with_binary_values() {
    use tempfile::TempDir;
    use axum::http::HeaderMap;
    
    let temp_dir = TempDir::new().unwrap();
    let cache_path = temp_dir.path().join("binary_test");
    
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-custom",
        "value\x01\x02\x03".parse().unwrap_or("fallback".parse().unwrap()),
    );
    
    let meta = cache::CacheMetadata {
        headers,
        original_url_path: "test/binary".to_string(),
    };
    
    let result = meta.save(&cache_path).await;
    assert!(result.is_ok());
}


#[tokio::test]
async fn test_download_connection_reset() {
    use tempfile::TempDir;
    use dashmap::DashMap;
    use std::sync::Arc;
    
    let temp_dir = TempDir::new().unwrap();
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = cache::CacheManager::new(settings).await.unwrap();
    
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(100))
        .build()
        .unwrap();
    
    let downloader = download::Downloader::new(
        client,
        "http://192.0.2.1".to_string(), 
        "file".to_string(),
        Arc::new(DashMap::new()),
    );
    
    let result = downloader
        .fetch_and_stream("test/file", &cache)
        .await;
    
    assert!(result.is_err());
}

#[tokio::test]
async fn test_download_very_slow_response() {
    use tempfile::TempDir;
    use dashmap::DashMap;
    use std::sync::Arc;
    
    let temp_dir = TempDir::new().unwrap();
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = cache::CacheManager::new(settings).await.unwrap();
    
    let mut mock_server = mockito::Server::new_async().await;
    let _mock = mock_server
        .mock("GET", "/slow")
        .with_status(200)
        .with_chunked_body(|w| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            w.write_all(b"data")
        })
        .create_async()
        .await;
    
    let client = reqwest::Client::new();
    let downloader = download::Downloader::new(
        client,
        mock_server.url(),
        "slow".to_string(),
        Arc::new(DashMap::new()),
    );
    
    let result = downloader
        .fetch_and_stream("test/slow", &cache)
        .await;
    
    assert!(result.is_ok());
}


#[tokio::test]
async fn test_app_state_resolve_upstream_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let mut repositories = HashMap::new();
    repositories.insert("test".to_string(), "http://example.com".to_string());
    
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories,
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = cache::CacheManager::new(settings.clone())
        .await
        .unwrap();
    
    let state = AppState::new(settings, cache);
    
    assert!(state.resolve_upstream("test").is_none());
    
    assert!(state.resolve_upstream("/").is_none());
    
    assert!(state.resolve_upstream("").is_none());
    
    let result = state.resolve_upstream("test/file");
    assert!(result.is_some());
    let (url, path) = result.unwrap();
    assert_eq!(url, "http://example.com");
    assert_eq!(path, "file");
}

#[tokio::test]
async fn test_proxy_with_very_long_url() {
    let temp_dir = TempDir::new().unwrap();
    let mut repositories = HashMap::new();
    repositories.insert("test".to_string(), "http://example.com".to_string());
    
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories,
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = cache::CacheManager::new(settings.clone())
        .await
        .unwrap();
    
    let state = std::sync::Arc::new(AppState::new(settings, cache));
    let app = build_router(state);
    
    let long_path = format!("/test/{}", "a".repeat(3000));
    
    let response = app
        .oneshot(
            Request::builder()
                .uri(&long_path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_proxy_multiple_slashes() {
    let temp_dir = TempDir::new().unwrap();
    let mut repositories = HashMap::new();
    repositories.insert("test".to_string(), "http://example.com".to_string());
    
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories,
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = cache::CacheManager::new(settings.clone())
        .await
        .unwrap();
    
    let state = std::sync::Arc::new(AppState::new(settings, cache));
    let app = build_router(state);
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test///file")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert!(response.status().is_client_error() || response.status().is_server_error());
}

#[tokio::test]
async fn test_concurrent_health_checks() {
    let temp_dir = TempDir::new().unwrap();
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories: HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = cache::CacheManager::new(settings.clone())
        .await
        .unwrap();
    
    let state = std::sync::Arc::new(AppState::new(settings, cache));
    let app = build_router(state);
    
    let mut handles = vec![];
    for _ in 0..100 {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            app_clone
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        });
        handles.push(handle);
    }
    
    for handle in handles {
        let response = handle.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn test_stats_during_download() {
    let temp_dir = TempDir::new().unwrap();
    let mut mock_server = mockito::Server::new_async().await;
    
    let mut repositories = HashMap::new();
    repositories.insert("test".to_string(), mock_server.url());
    
    let settings = config::Settings {
        port: 3142,
        socket: None,
        repositories,
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = cache::CacheManager::new(settings.clone())
        .await
        .unwrap();
    
    let state = std::sync::Arc::new(AppState::new(settings, cache));
    let app = build_router(state);
    
    let _mock = mock_server
        .mock("GET", "/file")
        .with_status(200)
        .with_chunked_body(|w| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            w.write_all(b"data")
        })
        .create_async()
        .await;
    
    let app_clone = app.clone();
    let _download_handle = tokio::spawn(async move {
        app_clone
            .oneshot(
                Request::builder()
                    .uri("/test/file")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    let stats_response = app
        .oneshot(
            Request::builder()
                .uri("/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(stats_response.status(), StatusCode::OK);
}


#[test]
fn test_error_display_formatting() {
    use apt_cacher_rs::error::ProxyError;
    
    let errors = vec![
        ProxyError::InvalidPath("test".to_string()),
        ProxyError::RepositoryNotFound,
        ProxyError::Download("timeout".to_string()),
        ProxyError::UpstreamError(StatusCode::NOT_FOUND),
    ];
    
    for err in errors {
        let display = format!("{}", err);
        assert!(!display.is_empty());
        
        let debug = format!("{:?}", err);
        assert!(!debug.is_empty());
    }
}

#[test]
fn test_error_chain_preservation() {
    use apt_cacher_rs::error::ProxyError;
    use std::io;
    
    let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
    let proxy_err: ProxyError = io_err.into();
    
    let err_string = proxy_err.to_string();
    assert!(err_string.contains("Cache error"));
}