use apt_cacher_rs::*;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::collections::HashMap;
use tempfile::TempDir;
use tower::util::ServiceExt; 

async fn create_test_app() -> (axum::Router, TempDir, mockito::ServerGuard) {
    let temp_dir = TempDir::new().unwrap();
    let mock_server = mockito::Server::new_async().await;
    
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
    
    (app, temp_dir, mock_server)
}

#[tokio::test]
async fn test_health_check() {
    let (app, _temp, _mock) = create_test_app().await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body[..], b"OK");
}

#[tokio::test]
async fn test_stats_endpoint() {
    let (app, _temp, _mock) = create_test_app().await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();
    
    assert!(body_str.contains("Cache Size"));
    assert!(body_str.contains("LRU Entries"));
}

#[tokio::test]
async fn test_proxy_cache_miss_then_hit() {
    let (app, _temp, mut mock_server) = create_test_app().await;
    
    let mock = mock_server
        .mock("GET", "/dists/focal/Release")
        .with_status(200)
        .with_header("content-type", "text/plain")
        .with_body("Release file content")
        .create_async()
        .await;
    
    let response1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/test/dists/focal/Release")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response1.status(), StatusCode::OK);
    mock.assert_async().await;
    
    let body1 = axum::body::to_bytes(response1.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body1[..], b"Release file content");
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let response2 = app
        .oneshot(
            Request::builder()
                .uri("/test/dists/focal/Release")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response2.status(), StatusCode::OK);
    
    let body2 = axum::body::to_bytes(response2.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(&body2[..], b"Release file content");
    
    mock.assert_async().await;
}

#[tokio::test]
async fn test_proxy_repository_not_found() {
    let (app, _temp, _mock) = create_test_app().await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/nonexistent/dists/focal/Release")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_proxy_invalid_path() {
    let (app, _temp, _mock) = create_test_app().await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test/../etc/passwd")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_proxy_upstream_404() {
    let (app, _temp, mut mock_server) = create_test_app().await;
    
    let mock = mock_server
        .mock("GET", "/nonexistent.deb")
        .with_status(404)
        .create_async()
        .await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test/nonexistent.deb")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    mock.assert_async().await;
}

#[tokio::test]
async fn test_proxy_upstream_500() {
    let (app, _temp, mut mock_server) = create_test_app().await;
    
    let mock = mock_server
        .mock("GET", "/error.deb")
        .with_status(500)
        .create_async()
        .await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test/error.deb")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    mock.assert_async().await;
}

#[tokio::test]
async fn test_proxy_headers_preserved() {
    let (app, _temp, mut mock_server) = create_test_app().await;
    
    let mock = mock_server
        .mock("GET", "/package.deb")
        .with_status(200)
        .with_header("content-type", "application/x-debian-package")
        .with_header("etag", "\"abc123\"")
        .with_header("last-modified", "Mon, 01 Jan 2024 00:00:00 GMT")
        .with_body("package data")
        .create_async()
        .await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test/package.deb")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-debian-package"
    );
    assert_eq!(response.headers().get("etag").unwrap(), "\"abc123\"");
    
    mock.assert_async().await;
}

#[tokio::test]
async fn test_concurrent_downloads_same_file() {
    let (app, _temp, mut mock_server) = create_test_app().await;
    
    let mock = mock_server
        .mock("GET", "/shared.deb")
        .with_status(200)
        .with_body("shared content")
        .expect(1)
        .create_async()
        .await;
    
    let mut handles = vec![];
    for _ in 0..5 {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            app_clone
                .oneshot(
                    Request::builder()
                        .uri("/test/shared.deb")
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
    
    mock.assert_async().await;
}

#[tokio::test]
async fn test_large_file_streaming() {
    let (app, _temp, mut mock_server) = create_test_app().await;
    
    let large_data = vec![b'X'; 1024 * 1024];
    
    let mock = mock_server
        .mock("GET", "/large.deb")
        .with_status(200)
        .with_header("content-type", "application/x-debian-package")
        .with_body(&large_data)
        .create_async()
        .await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test/large.deb")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response.status(), StatusCode::OK);
    
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    
    assert_eq!(body.len(), 1024 * 1024);
    assert_eq!(&body[..100], &large_data[..100]);
    
    mock.assert_async().await;
}

#[tokio::test]
async fn test_multiple_repositories() {
    let temp_dir = TempDir::new().unwrap();
    let mut mock_server1 = mockito::Server::new_async().await;
    let mut mock_server2 = mockito::Server::new_async().await;
    
    let mut repositories = HashMap::new();
    repositories.insert("ubuntu".to_string(), mock_server1.url());
    repositories.insert("debian".to_string(), mock_server2.url());
    
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
    
    let mock1 = mock_server1
        .mock("GET", "/ubuntu-file")
        .with_status(200)
        .with_body("ubuntu content")
        .create_async()
        .await;
    
    let mock2 = mock_server2
        .mock("GET", "/debian-file")
        .with_status(200)
        .with_body("debian content")
        .create_async()
        .await;
    
    let response1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ubuntu/ubuntu-file")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response1.status(), StatusCode::OK);
    mock1.assert_async().await;
    
    let response2 = app
        .oneshot(
            Request::builder()
                .uri("/debian/debian-file")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert_eq!(response2.status(), StatusCode::OK);
    mock2.assert_async().await;
}

#[tokio::test]
async fn test_empty_path() {
    let (app, _temp, _mock) = create_test_app().await;
    
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    
    assert!(response.status().is_client_error());
}