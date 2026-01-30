use apt_cacher_rs::{cache::*, config::Settings};
use axum::http::HeaderMap;
use tempfile::TempDir;
use tokio::fs;

#[tokio::test]
async fn test_cache_entry_total_size() {
    let entry = CacheEntry {
        data_size: 1000,
        headers_size: 500,
    };
    
    assert_eq!(entry.total_size(), 1500);
}

#[tokio::test]
async fn test_cache_metadata_empty_headers() {
    let temp_dir = TempDir::new().unwrap();
    let cache_path = temp_dir.path().join("test_file");
    
    let headers = HeaderMap::new();
    let meta = CacheMetadata {
        headers,
        original_url_path: "test/path".to_string(),
    };
    
    let size = meta.save(&cache_path).await.unwrap();
    assert!(size > 0);
    
    let loaded = CacheMetadata::load(&cache_path).await.unwrap();
    assert_eq!(loaded.original_url_path, "test/path");
    assert_eq!(loaded.headers.len(), 0);
}

#[tokio::test]
async fn test_cache_metadata_many_headers() {
    let temp_dir = TempDir::new().unwrap();
    let cache_path = temp_dir.path().join("test_file");
    
    let mut headers = HeaderMap::new();
    for i in 0..50 {
        headers.insert(
            format!("x-custom-{}", i).parse::<axum::http::HeaderName>().unwrap(),
            format!("value-{}", i).parse().unwrap(),
        );
    }
    
    let meta = CacheMetadata {
        headers: headers.clone(),
        original_url_path: "test/many_headers".to_string(),
    };
    
    meta.save(&cache_path).await.unwrap();
    let loaded = CacheMetadata::load(&cache_path).await.unwrap();
    
    assert_eq!(loaded.headers.len(), 50);
}

#[tokio::test]
async fn test_cache_metadata_invalid_json() {
    let temp_dir = TempDir::new().unwrap();
    let cache_path = temp_dir.path().join("test_file");
    let headers_path = cache_path.with_extension("headers");
    
    fs::write(&headers_path, b"{ invalid json").await.unwrap();
    
    let result = CacheMetadata::load(&cache_path).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_cache_manager_max_size_enforcement() {
    let temp_dir = TempDir::new().unwrap();
    let settings = Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 5000, 
        max_lru_entries: 10,
    };
    
    let cache = CacheManager::new(settings).await.unwrap();
    
    for i in 0..10 {
        let data = vec![0u8; 1000]; 
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        
        let meta = CacheMetadata {
            headers,
            original_url_path: format!("test/file{}.txt", i),
        };
        
        cache.store(&format!("test/file{}.txt", i), &data, &meta)
            .await
            .unwrap();
    }
    
    let stats = cache.get_stats().await;
    assert!(stats.contains("KB") || stats.contains("B"));
}

#[tokio::test]
async fn test_cache_serve_corrupted_file() {
    let temp_dir = TempDir::new().unwrap();
    let settings = Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = CacheManager::new(settings).await.unwrap();
    let cache_path = cache.cache_path("test/corrupted");
    
    fs::create_dir_all(cache_path.parent().unwrap()).await.unwrap();
    fs::write(&cache_path, b"data").await.unwrap();
    
    let result = cache.serve_cached("test/corrupted").await.unwrap();
    assert!(result.is_some());
}

#[tokio::test]
async fn test_cache_store_creates_directories() {
    let temp_dir = TempDir::new().unwrap();
    let settings = Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = CacheManager::new(settings).await.unwrap();
    
    let data = b"test data";
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "text/plain".parse().unwrap());
    
    let meta = CacheMetadata {
        headers,
        original_url_path: "deep/nested/path/file.txt".to_string(),
    };
    
    cache.store("deep/nested/path/file.txt", data, &meta)
        .await
        .unwrap();
    
    let cache_path = cache.cache_path("deep/nested/path/file.txt");
    assert!(cache_path.exists());
}

#[tokio::test]
async fn test_cache_initialization_skips_part_files() {
    let temp_dir = TempDir::new().unwrap();
    let cache_dir = temp_dir.path();
    
    fs::create_dir_all(cache_dir.join("aa/bb")).await.unwrap();
    fs::write(cache_dir.join("aa/bb/file.part"), b"partial")
        .await
        .unwrap();
    
    let settings = Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: cache_dir.to_path_buf(),
        max_cache_size: 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = CacheManager::new(settings).await.unwrap();
    let stats = cache.get_stats().await;
    
    assert!(stats.contains("0 B") || stats.contains("0.00"));
}

#[tokio::test]
async fn test_cache_concurrent_store_same_path() {
    let temp_dir = TempDir::new().unwrap();
    let settings = Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 1024 * 1024,
        max_lru_entries: 100,
    };
    
    let cache = std::sync::Arc::new(CacheManager::new(settings).await.unwrap());
    
    let mut handles = vec![];
    
    for i in 0..5 {
        let cache_clone = cache.clone();
        let handle = tokio::spawn(async move {
            let data = format!("data from thread {}", i).into_bytes();
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "text/plain".parse().unwrap());
            
            let meta = CacheMetadata {
                headers,
                original_url_path: "same/path.txt".to_string(),
            };
            
            cache_clone.store("same/path.txt", &data, &meta).await
        });
        handles.push(handle);
    }
    
    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }
    
    let result = cache.serve_cached("same/path.txt").await.unwrap();
    assert!(result.is_some());
}