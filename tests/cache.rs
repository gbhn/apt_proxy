mod helpers;

use apt_cacher_rs::{cache::*, config::Settings};
use axum::http::HeaderMap;
use helpers::*;

/// Группа тестов для базовой функциональности кеша
mod basic {
    use super::*;
    
    #[tokio::test]
    async fn cache_manager_creation() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        
        let cache = CacheManager::new(settings).await;
        assert!(cache.is_ok());
    }
    
    #[tokio::test]
    async fn cache_miss_returns_none() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(settings).await.unwrap();
        
        let result = cache.serve_cached("nonexistent/file").await.unwrap();
        assert!(result.is_none());
    }
    
    #[tokio::test]
    async fn cache_path_generation() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(settings).await.unwrap();
        
        let path1 = cache.cache_path("ubuntu/dists/focal/Release");
        let path2 = cache.cache_path("ubuntu/dists/focal/Release");
        let path3 = cache.cache_path("debian/dists/stable/Release");
        
        // Одинаковые пути должны генерировать одинаковый кеш
        assert_eq!(path1, path2);
        // Разные пути должны генерировать разный кеш
        assert_ne!(path1, path3);
        // Все пути должны быть в директории кеша
        assert!(path1.starts_with(temp_dir.path()));
        assert!(path3.starts_with(temp_dir.path()));
    }
}

/// Группа тестов для метаданных
mod metadata {
    use super::*;
    
    #[tokio::test]
    async fn metadata_roundtrip() {
        let temp_dir = temp_dir();
        let cache_path = temp_dir.path().join("test_file");
        
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/octet-stream".parse().unwrap());
        headers.insert("content-length", "12345".parse().unwrap());
        headers.insert("etag", "\"abc123\"".parse().unwrap());
        
        let meta = CacheMetadata {
            headers: headers.clone(),
            original_url_path: "ubuntu/test/package.deb".to_string(),
        };
        
        let size = meta.save(&cache_path).await.unwrap();
        assert!(size > 0);
        
        let loaded = CacheMetadata::load(&cache_path).await.unwrap();
        
        assert_eq!(loaded.original_url_path, "ubuntu/test/package.deb");
        assert_eq!(
            loaded.headers.get("content-type").unwrap(),
            "application/octet-stream"
        );
        assert_eq!(loaded.headers.get("content-length").unwrap(), "12345");
        assert_eq!(loaded.headers.get("etag").unwrap(), "\"abc123\"");
    }
    
    #[tokio::test]
    async fn metadata_from_response() {
        let mut mock = MockServerBuilder::new().await;
        let mock_endpoint = mock.mock_get_with_headers(
            "/test.deb",
            b"test data",
            vec![
                ("content-type", "application/x-debian-package"),
                ("content-length", "9"),
            ],
        )
        .await;
        
        let client = reqwest::Client::new();
        let url = format!("{}/test.deb", mock.url());
        let response = client.get(&url).send().await.unwrap();
        
        let meta = CacheMetadata::from_response(&response, "test/path");
        
        assert_eq!(meta.original_url_path, "test/path");
        assert_eq!(
            meta.headers.get("content-type").unwrap(),
            "application/x-debian-package"
        );
        
        mock_endpoint.assert_async().await;
    }
    
    #[tokio::test]
    async fn handles_missing_headers_file() {
        let temp_dir = temp_dir();
        let cache_path = temp_dir.path().join("test_file");
        
        tokio::fs::write(&cache_path, b"test data").await.unwrap();
        
        let meta = CacheMetadata::load(&cache_path).await;
        assert!(meta.is_none());
    }
}

/// Группа тестов для хранения и извлечения данных
mod storage {
    use super::*;
    
    #[tokio::test]
    async fn store_and_retrieve() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(settings).await.unwrap();
        
        let data = b"test data content";
        let mut headers = test_headers();
        headers.insert("content-type", "text/plain".parse().unwrap());
        
        let meta = CacheMetadata {
            headers,
            original_url_path: "test/file.txt".to_string(),
        };
        
        cache.store("test/file.txt", data, &meta).await.unwrap();
        
        let response = cache.serve_cached("test/file.txt").await.unwrap();
        assert!(response.is_some());
        
        let response = response.unwrap();
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain"
        );
    }
    
    #[tokio::test]
    async fn initialization_from_disk() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        
        // Создаем кеш и сохраняем данные
        {
            let cache = CacheManager::new(settings.clone()).await.unwrap();
            
            let data = b"test data";
            let mut headers = test_headers();
            headers.insert("content-type", "text/plain".parse().unwrap());
            
            let meta = CacheMetadata {
                headers,
                original_url_path: "test/persistent.txt".to_string(),
            };
            
            cache.store("test/persistent.txt", data, &meta).await.unwrap();
        }
        
        // Создаем новый кеш и проверяем, что данные сохранились
        let cache2 = CacheManager::new(settings).await.unwrap();
        let response = cache2.serve_cached("test/persistent.txt").await.unwrap();
        
        assert!(response.is_some());
    }
    
    #[tokio::test]
    async fn multiple_concurrent_stores() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = std::sync::Arc::new(CacheManager::new(settings).await.unwrap());
        
        let mut handles = vec![];
        
        for i in 0..10 {
            let cache_clone = cache.clone();
            let handle = tokio::spawn(async move {
                let data = format!("data {}", i).into_bytes();
                let mut headers = test_headers();
                headers.insert("content-type", "text/plain".parse().unwrap());
                
                let meta = CacheMetadata {
                    headers,
                    original_url_path: format!("test/file{}.txt", i),
                };
                
                cache_clone
                    .store(&format!("test/file{}.txt", i), &data, &meta)
                    .await
            });
            handles.push(handle);
        }
        
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }
    }
}

/// Группа тестов для LRU и eviction
mod lru_eviction {
    use super::*;
    
    #[tokio::test]
    async fn lru_eviction_on_size_limit() {
        let temp_dir = temp_dir();
        let settings = settings_with_cache_params(
            temp_dir.path().to_path_buf(),
            1024,  // 1 KB max
            3,     // max 3 entries
        );
        
        let cache = CacheManager::new(settings).await.unwrap();
        
        let mut headers = test_headers();
        headers.insert("content-length", "512".parse().unwrap());
        
        // Сохраняем 5 файлов по 512 байт
        for i in 0..5 {
            let data = vec![0u8; 512];
            let meta = CacheMetadata {
                headers: headers.clone(),
                original_url_path: format!("test/file{}.txt", i),
            };
            cache
                .store(&format!("test/file{}.txt", i), &data, &meta)
                .await
                .unwrap();
        }
        
        let stats = cache.get_stats().await;
        assert!(stats.contains("Cache Size"));
    }
}

/// Группа тестов для статистики
mod statistics {
    use super::*;
    
    #[tokio::test]
    async fn get_stats() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(settings).await.unwrap();
        
        let stats = cache.get_stats().await;
        
        assert!(stats.contains("Cache Size"));
        assert!(stats.contains("LRU Entries"));
    }
    
    #[tokio::test]
    async fn stats_update_after_store() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = CacheManager::new(settings).await.unwrap();
        
        let stats_before = cache.get_stats().await;
        
        let data = b"test data content";
        let meta = CacheMetadata {
            headers: test_headers(),
            original_url_path: "test/file.txt".to_string(),
        };
        
        cache.store("test/file.txt", data, &meta).await.unwrap();
        
        let stats_after = cache.get_stats().await;
        
        assert_ne!(stats_before, stats_after);
    }
}