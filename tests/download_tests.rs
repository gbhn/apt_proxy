use apt_cacher_rs::{cache::*, config::Settings, download::*};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};
use dashmap::DashMap; 

async fn create_test_cache(temp_dir: &TempDir) -> CacheManager {
    let settings = Settings {
        port: 3142,
        socket: None,
        repositories: std::collections::HashMap::new(),
        cache_dir: temp_dir.path().to_path_buf(),
        max_cache_size: 10 * 1024 * 1024,
        max_lru_entries: 100,
    };
    
    CacheManager::new(settings).await.unwrap()
}

#[tokio::test]
async fn test_downloader_creation() {
    let client = reqwest::Client::new();
    let downloader = Downloader::new(
        client,
        "http://example.com".to_string(),
        "path/to/file".to_string(),
        Arc::new(DashMap::new()),
    );
    
    drop(downloader);
}

#[tokio::test]
async fn test_download_state_initial() {
    let state = Arc::new(DownloadState::new());
    
    assert_eq!(state.status_code.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(!state.is_finished.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!state.is_success.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(state.bytes_written.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_download_state_mark_finished_success() {
    let state = Arc::new(DownloadState::new());
    
    state.mark_finished(true);
    
    assert!(state.is_finished.load(std::sync::atomic::Ordering::SeqCst));
    assert!(state.is_success.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn test_download_state_mark_finished_failure() {
    let state = Arc::new(DownloadState::new());
    
    state.mark_finished(false);
    
    assert!(state.is_finished.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!state.is_success.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn test_download_state_wait_for_status() {
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
async fn test_download_state_wait_for_status_on_finish() {
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

#[tokio::test]
async fn test_successful_download() {
    let temp_dir = TempDir::new().unwrap();
    let cache = create_test_cache(&temp_dir).await;
    
    let mut mock_server = mockito::Server::new_async().await;
    let mock = mock_server
        .mock("GET", "/test.deb")
        .with_status(200)
        .with_header("content-type", "application/x-debian-package")
        .with_body("test package content")
        .create_async()
        .await;
    
    let client = reqwest::Client::new();
    let downloader = Downloader::new(
        client,
        mock_server.url(),
        "test.deb".to_string(),
        Arc::new(DashMap::new()), 
    );
    
    let result = downloader
        .fetch_and_stream("ubuntu/test.deb", &cache)
        .await;
    
    assert!(result.is_ok());
    mock.assert_async().await;
}

#[tokio::test]
async fn test_download_404() {
    let temp_dir = TempDir::new().unwrap();
    let cache = create_test_cache(&temp_dir).await;
    
    let mut mock_server = mockito::Server::new_async().await;
    let mock = mock_server
        .mock("GET", "/notfound.deb")
        .with_status(404)
        .create_async()
        .await;
    
    let client = reqwest::Client::new();
    let downloader = Downloader::new(
        client,
        mock_server.url(),
        "notfound.deb".to_string(),
        Arc::new(DashMap::new()), 
    );
    
    let result = downloader
        .fetch_and_stream("ubuntu/notfound.deb", &cache)
        .await;
    
    assert!(result.is_err());
    mock.assert_async().await;
}

#[tokio::test]
async fn test_download_preserves_headers() {
    let temp_dir = TempDir::new().unwrap();
    let cache = create_test_cache(&temp_dir).await;
    
    let mut mock_server = mockito::Server::new_async().await;
    let mock = mock_server
        .mock("GET", "/package.deb")
        .with_status(200)
        .with_header("content-type", "application/x-debian-package")
        .with_header("content-length", "20")
        .with_header("etag", "\"abc123\"")
        .with_body("test package content")
        .create_async()
        .await;
    
    let client = reqwest::Client::new();
    let downloader = Downloader::new(
        client,
        mock_server.url(),
        "package.deb".to_string(),
        Arc::new(DashMap::new()), 
    );
    
    let response = downloader
        .fetch_and_stream("ubuntu/package.deb", &cache)
        .await
        .unwrap();
    
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-debian-package"
    );
    assert_eq!(response.headers().get("etag").unwrap(), "\"abc123\"");
    
    mock.assert_async().await;
}

#[tokio::test]
async fn test_concurrent_downloads_deduplicated() {
    let temp_dir = TempDir::new().unwrap();
    let cache = Arc::new(create_test_cache(&temp_dir).await);
    
    let mut mock_server = mockito::Server::new_async().await;
    
    let mock = mock_server
        .mock("GET", "/shared.deb")
        .with_status(200)
        .with_body("shared content")
        .expect(1)
        .create_async()
        .await;
    
    let client = Arc::new(reqwest::Client::new());
    let active_downloads = Arc::new(DashMap::new());
    
    let mut handles = vec![];
    for _ in 0..5 {
        let cache_clone = cache.clone();
        let client_clone = client.clone();
        let url = mock_server.url();
        let active_downloads = active_downloads.clone();
        
        let handle = tokio::spawn(async move {
            let downloader = Downloader::new(
                (*client_clone).clone(),
                url,
                "shared.deb".to_string(),
                active_downloads, 
            );
            
            downloader
                .fetch_and_stream("ubuntu/shared.deb", &cache_clone)
                .await
        });
        
        handles.push(handle);
        
    }
    
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Download should succeed");
    }
    
    mock.assert_async().await;
}

#[tokio::test]
async fn test_download_timeout() {
    let temp_dir = TempDir::new().unwrap();
    let cache = create_test_cache(&temp_dir).await;
    
    let _mock_server = mockito::Server::new_async().await;
    
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    
    let downloader = Downloader::new(
        client,
        "http://10.255.255.1".to_string(), 
        "timeout.deb".to_string(),
        Arc::new(DashMap::new()), 
    );
    
    let result = downloader
        .fetch_and_stream("ubuntu/timeout.deb", &cache)
        .await;
    
    assert!(result.is_err());
}

#[tokio::test]
async fn test_download_large_file() {
    let temp_dir = TempDir::new().unwrap();
    let cache = create_test_cache(&temp_dir).await;
    
    let mut mock_server = mockito::Server::new_async().await;
    
    let large_data = vec![b'X'; 1024 * 1024];
    
    let mock = mock_server
        .mock("GET", "/large.deb")
        .with_status(200)
        .with_body(&large_data)
        .create_async()
        .await;
    
    let client = reqwest::Client::new();
    let downloader = Downloader::new(
        client,
        mock_server.url(),
        "large.deb".to_string(),
        Arc::new(DashMap::new()), 
    );
    
    let result = downloader
        .fetch_and_stream("ubuntu/large.deb", &cache)
        .await;
    
    assert!(result.is_ok());
    mock.assert_async().await;
}

#[tokio::test]
async fn test_streaming_reader_completion() {
    let state = Arc::new(DownloadState::new());
    
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test_file");
    tokio::fs::write(&file_path, b"test data").await.unwrap();
    
    let file = tokio::fs::File::open(&file_path).await.unwrap();
    
    state.mark_finished(true);
    state.is_success.store(true, std::sync::atomic::Ordering::SeqCst);
    
    let mut stream = StreamingReader::new(file, state.clone());
    
    use futures::StreamExt;
    
    let mut all_data = Vec::new();
    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        all_data.extend_from_slice(&chunk);
    }
    
    assert_eq!(&all_data, b"test data");
}