mod helpers;

use apt_cacher_rs::{cache::*, download::*, error::ProxyError};
use dashmap::DashMap;
use helpers::*;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Группа тестов для создания и инициализации Downloader
mod creation {
    use super::*;
    
    #[tokio::test]
    async fn downloader_creation() {
        let client = reqwest::Client::new();
        let downloader = Downloader::new(
            client,
            "http://example.com".to_string(),
            "path/to/file".to_string(),
            Arc::new(DashMap::new()),
        );
        
        drop(downloader);
    }
}

/// Группа тестов для DownloadState
mod download_state {
    use super::*;
    
    #[tokio::test]
    async fn initial_state() {
        let state = Arc::new(DownloadState::new());
        
        assert_eq!(state.status_code.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(!state.is_finished.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!state.is_success.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(state.bytes_written.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    
    #[tokio::test]
    async fn mark_finished_success() {
        let state = Arc::new(DownloadState::new());
        
        state.mark_finished(true);
        
        assert!(state.is_finished.load(std::sync::atomic::Ordering::SeqCst));
        assert!(state.is_success.load(std::sync::atomic::Ordering::SeqCst));
    }
    
    #[tokio::test]
    async fn mark_finished_failure() {
        let state = Arc::new(DownloadState::new());
        
        state.mark_finished(false);
        
        assert!(state.is_finished.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!state.is_success.load(std::sync::atomic::Ordering::SeqCst));
    }
    
    #[tokio::test]
    async fn wait_for_status() {
        let state = Arc::new(DownloadState::new());
        let state_clone = state.clone();
        
        let handle = tokio::spawn(async move {
            state_clone.wait_for_status().await
        });
        
        sleep(Duration::from_millis(10)).await;
        
        state.status_code.store(200, std::sync::atomic::Ordering::SeqCst);
        state.notify_status.notify_waiters();
        
        let result = handle.await.unwrap();
        assert_eq!(result, 200);
    }
    
    #[tokio::test]
    async fn wait_for_status_on_finish() {
        let state = Arc::new(DownloadState::new());
        let state_clone = state.clone();
        
        let handle = tokio::spawn(async move {
            state_clone.wait_for_status().await
        });
        
        sleep(Duration::from_millis(10)).await;
        
        state.mark_finished(false);
        
        let result = handle.await.unwrap();
        assert_eq!(result, 502);
    }
}

/// Группа тестов для успешных загрузок
mod successful_downloads {
    use super::*;
    
    #[tokio::test]
    async fn basic_download() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = Arc::new(CacheManager::new(settings).await.unwrap());
        
        let test_data = generate_test_data(1024);
        let mut mock = MockServerBuilder::new().await;
        let _m = mock.mock_get("/test.deb", &test_data).await;
        
        let client = reqwest::Client::new();
        // Correctly split URL components: base URL and file path
        let url = mock.url();
        let upstream_path = "test.deb";
        
        let path = "test/package.deb";
        let downloader = Downloader::new(
            client,
            url,
            upstream_path.to_string(),
            Arc::new(DashMap::new()),
        );
        
        let result = downloader.fetch_and_stream(path, &cache).await;
        assert!(result.is_ok());
        
        // Ждем файл
        let cache_path = cache.cache_path(path);
        assert!(wait_for_file(&cache_path, 2000).await);

        // Проверяем, что файл сохранен
        let cached = cache.serve_cached(path).await.unwrap();
        assert!(cached.is_some());
    }
    
    #[tokio::test]
    async fn download_debian_package() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = Arc::new(CacheManager::new(settings).await.unwrap());
        
        let package_data = generate_test_data(2048);
        let mut mock = MockServerBuilder::new().await;
        let _m = mock.mock_debian_package("/package.deb", &package_data).await;
        
        let client = reqwest::Client::new();
        let url = mock.url();
        let upstream_path = "package.deb";
        
        let path = "ubuntu/pool/main/p/package.deb";
        let downloader = Downloader::new(
            client,
            url,
            upstream_path.to_string(),
            Arc::new(DashMap::new()),
        );
        
        let result = downloader.fetch_and_stream(path, &cache).await;
        assert!(result.is_ok());
        
        // Проверяем размер
        let cache_path = cache.cache_path(path);
        assert!(wait_for_file(&cache_path, 2000).await);
        assert_file_size(&cache_path, 2048).await;
    }
}

/// Группа тестов для параллельных загрузок
mod concurrent_downloads {
    use super::*;
    
    #[tokio::test]
    async fn multiple_parallel_downloads() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = Arc::new(CacheManager::new(settings).await.unwrap());
        
        let mut mock = MockServerBuilder::new().await;
        
        // Создаем несколько эндпоинтов
        let mut handles = vec![];
        for i in 0..5 {
            let data = generate_test_data(512);
            mock.mock_get(&format!("/file{}.deb", i), &data).await;
        }
        
        let base_url = mock.url();
        
        // Загружаем параллельно
        for i in 0..5 {
            let cache_clone = cache.clone();
            let url = base_url.clone();
            let client = reqwest::Client::new();
            
            let handle = tokio::spawn(async move {
                let path = format!("test/file{}.deb", i);
                let upstream_path = format!("file{}.deb", i);
                
                let downloader = Downloader::new(
                    client,
                    url,
                    upstream_path,
                    Arc::new(DashMap::new()),
                );
                
                match downloader.fetch_and_stream(&path, &cache_clone).await {
                    Ok(_) => {
                        let cache_path = cache_clone.cache_path(&path);
                        if wait_for_file(&cache_path, 2000).await {
                            Ok(())
                        } else {
                            Err(ProxyError::Download("Timeout waiting for file".into()))
                        }
                    },
                    Err(e) => Err(e)
                }
            });
            
            handles.push(handle);
        }
        
        // Ждем все загрузки
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }
        
        // Проверяем, что все файлы в кеше
        for i in 0..5 {
            let cached = cache.serve_cached(&format!("test/file{}.deb", i))
                .await
                .unwrap();
            assert!(cached.is_some());
        }
    }
    
    #[tokio::test]
    async fn deduplication_of_same_file() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = Arc::new(CacheManager::new(settings).await.unwrap());
        
        let test_data = generate_test_data(1024);
        let mut mock = MockServerBuilder::new().await;
        let m = mock.mock_get("/same.deb", &test_data).await;
        
        let base_url = mock.url();
        let in_progress = Arc::new(DashMap::new());
        
        // Запускаем 3 параллельные загрузки одного файла
        let mut handles = vec![];
        for _ in 0..3 {
            let cache_clone = cache.clone();
            let url = base_url.clone();
            let client = reqwest::Client::new();
            let in_progress_clone = in_progress.clone();
            
            let handle = tokio::spawn(async move {
                let path = "test/same.deb";
                let upstream_path = "same.deb";
                
                let downloader = Downloader::new(
                    client,
                    url,
                    upstream_path.to_string(),
                    in_progress_clone,
                );
                match downloader.fetch_and_stream(path, &cache_clone).await {
                    Ok(_) => {
                        let cache_path = cache_clone.cache_path(path);
                        if wait_for_file(&cache_path, 2000).await {
                            Ok(())
                        } else {
                            Err(ProxyError::Download("Timeout".into()))
                        }
                    },
                    Err(e) => Err(e)
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }
        
        let cache_path = cache.cache_path("test/same.deb");
        assert!(wait_for_file(&cache_path, 2000).await);
        
        // Мок должен быть вызван только один раз (дедупликация)
        m.assert_async().await;
    }
}

/// Группа тестов для обработки ошибок
mod error_handling {
    use super::*;
    
    #[tokio::test]
    async fn handle_404_error() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = Arc::new(CacheManager::new(settings).await.unwrap());
        
        let mut mock = MockServerBuilder::new().await;
        let _m = mock.mock_get_status("/notfound.deb", 404).await;
        
        let client = reqwest::Client::new();
        let url = mock.url();
        let upstream_path = "notfound.deb";
        
        let path = "test/notfound.deb";
        let downloader = Downloader::new(
            client,
            url,
            upstream_path.to_string(),
            Arc::new(DashMap::new()),
        );
        
        let result = downloader.fetch_and_stream(path, &cache).await;
        assert!(result.is_err());
    }
    
    #[tokio::test]
    async fn handle_server_error() {
        let temp_dir = temp_dir();
        let settings = basic_settings(temp_dir.path().to_path_buf());
        let cache = Arc::new(CacheManager::new(settings).await.unwrap());
        
        let mut mock = MockServerBuilder::new().await;
        let _m = mock.mock_get_status("/error.deb", 500).await;
        
        let client = reqwest::Client::new();
        let url = mock.url();
        let upstream_path = "error.deb";
        
        let path = "test/error.deb";
        let downloader = Downloader::new(
            client,
            url,
            upstream_path.to_string(),
            Arc::new(DashMap::new()),
        );
        
        let result = downloader.fetch_and_stream(path, &cache).await;
        assert!(result.is_err());
    }
}