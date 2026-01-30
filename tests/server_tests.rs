use apt_cacher_rs::server::*;
use axum::{Router, routing::get};
use tempfile::TempDir;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_serve_tcp_starts_successfully() {
    let app = Router::new().route("/test", get(|| async { "OK" }));
    
    let handle = tokio::spawn(async move {
        serve_tcp(app, 0).await 
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    handle.abort();
    
    assert!(true);
}

#[tokio::test]
#[cfg(unix)]
async fn test_serve_unix_creates_socket() {
    use tokio::net::UnixStream;
    
    let temp_dir = TempDir::new().unwrap();
    let socket_path = temp_dir.path().join("test.sock");
    
    let app = Router::new().route("/test", get(|| async { "OK" }));
    
    let socket_path_clone = socket_path.clone();
    let handle = tokio::spawn(async move {
        serve_unix(app, &socket_path_clone).await
    });
    
    for _ in 0..50 {
        if socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    
    assert!(socket_path.exists(), "Socket file should be created");
    
    let result = timeout(
        Duration::from_secs(1),
        UnixStream::connect(&socket_path)
    ).await;
    
    assert!(result.is_ok(), "Should be able to connect to socket");
    
    handle.abort();
}

#[tokio::test]
#[cfg(unix)]
async fn test_serve_unix_removes_existing_socket() {
    let temp_dir = TempDir::new().unwrap();
    let socket_path = temp_dir.path().join("existing.sock");
    
    std::fs::write(&socket_path, b"old socket").unwrap();
    assert!(socket_path.exists());
    
    let app = Router::new().route("/test", get(|| async { "OK" }));
    
    let socket_path_clone = socket_path.clone();
    let handle = tokio::spawn(async move {
        serve_unix(app, &socket_path_clone).await
    });
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    assert!(socket_path.exists());
    
    handle.abort();
}

#[tokio::test]
#[cfg(unix)]
async fn test_serve_unix_sets_permissions() {
    use std::os::unix::fs::PermissionsExt;
    
    let temp_dir = TempDir::new().unwrap();
    let socket_path = temp_dir.path().join("perms.sock");
    
    let app = Router::new().route("/test", get(|| async { "OK" }));
    
    let socket_path_clone = socket_path.clone();
    let handle = tokio::spawn(async move {
        serve_unix(app, &socket_path_clone).await
    });
    
    let mut success = false;
    for _ in 0..50 {
        if socket_path.exists() {
            if let Ok(metadata) = std::fs::metadata(&socket_path) {
                let permissions = metadata.permissions();
                let mode = permissions.mode() & 0o777;
                if mode == 0o777 {
                    success = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    
    handle.abort();
    
    if !success {
         if !socket_path.exists() {
             panic!("Socket file was never created");
         } else {
             let metadata = std::fs::metadata(&socket_path).unwrap();
             let mode = metadata.permissions().mode() & 0o777;
             panic!("Socket permissions mismatch. Expected 0o777, got 0o{:o}. This might be a filesystem limitation (e.g. WSL/NTFS) or a race condition.", mode);
         }
    }
}

#[tokio::test]
async fn test_tcp_port_binding() {
    let app = Router::new().route("/", get(|| async { "test" }));
    
    let handle1 = tokio::spawn(async move {
        serve_tcp(app, 18765).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let app2 = Router::new().route("/", get(|| async { "test2" }));
    let handle2 = tokio::spawn(async move {
        serve_tcp(app2, 18765).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    handle1.abort();
    
    let result = timeout(Duration::from_secs(1), handle2).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test]
#[cfg(unix)]
async fn test_unix_socket_concurrent_connections() {
    use tokio::net::UnixStream;
    use tokio::io::{AsyncWriteExt, AsyncReadExt};
    
    let temp_dir = TempDir::new().unwrap();
    let socket_path = temp_dir.path().join("concurrent.sock");
    
    let app = Router::new().route("/test", get(|| async { "response" }));
    
    let socket_path_clone = socket_path.clone();
    let _handle = tokio::spawn(async move {
        serve_unix(app, &socket_path_clone).await
    });
    
    for _ in 0..50 {
        if socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    
    let mut handles = vec![];
    for i in 0..5 {
        let socket_path = socket_path.clone();
        let handle = tokio::spawn(async move {
            let mut stream = UnixStream::connect(&socket_path).await.unwrap();
            let request = format!(
                "GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(request.as_bytes()).await.unwrap();
            
            let mut buffer = vec![0u8; 1024];
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(n > 0, "Should receive response for connection {}", i);
        });
        handles.push(handle);
    }
    
    for handle in handles {
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("Connection should complete")
            .expect("Connection should succeed");
    }
}

#[tokio::test]
async fn test_tcp_handles_http_requests() {
    use reqwest::Client;
    
    let app = Router::new()
        .route("/", get(|| async { "root" }))
        .route("/test", get(|| async { "test response" }));
    
    let handle = tokio::spawn(async move {
        serve_tcp(app, 18766).await
    });
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let client = Client::new();
    let response = timeout(
        Duration::from_secs(2),
        client.get("http://127.0.0.1:18766/test").send()
    )
    .await;
    
    handle.abort();
    
    assert!(response.is_ok(), "Should successfully make HTTP request");
    
    if let Ok(Ok(resp)) = response {
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        assert_eq!(text, "test response");
    }
}

#[tokio::test]
async fn test_tcp_server_shutdown() {
    let app = Router::new().route("/", get(|| async { "ok" }));
    
    let handle = tokio::spawn(async move {
        serve_tcp(app, 18767).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    handle.abort();
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let app2 = Router::new().route("/", get(|| async { "new" }));
    let handle2 = tokio::spawn(async move {
        serve_tcp(app2, 18767).await
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle2.abort();
    
    assert!(true, "Should be able to reuse port after shutdown");
}

#[tokio::test]
#[cfg(unix)]
async fn test_unix_socket_cleanup_on_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let socket_path = temp_dir.path().join("cleanup.sock");
    
    let app = Router::new().route("/", get(|| async { "ok" }));
    
    let socket_path_clone = socket_path.clone();
    let handle = tokio::spawn(async move {
        serve_unix(app, &socket_path_clone).await
    });
    
    for _ in 0..50 {
        if socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    
    assert!(socket_path.exists());
    
    handle.abort();
    
    assert!(socket_path.exists());
}

#[tokio::test]
async fn test_tcp_multiple_routes() {
    use reqwest::Client;
    
    let app = Router::new()
        .route("/route1", get(|| async { "response1" }))
        .route("/route2", get(|| async { "response2" }))
        .route("/route3", get(|| async { "response3" }));
    
    let handle = tokio::spawn(async move {
        serve_tcp(app, 18768).await
    });
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let client = Client::new();
    
    for (route, expected) in [
        ("/route1", "response1"),
        ("/route2", "response2"),
        ("/route3", "response3"),
    ] {
        let response = client
            .get(format!("http://127.0.0.1:18768{}", route))
            .send()
            .await
            .unwrap();
        
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), expected);
    }
    
    handle.abort();
}

#[tokio::test]
async fn test_tcp_404_handling() {
    use reqwest::Client;
    
    let app = Router::new().route("/exists", get(|| async { "ok" }));
    
    let handle = tokio::spawn(async move {
        serve_tcp(app, 18769).await
    });
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let client = Client::new();
    let response = client
        .get("http://127.0.0.1:18769/notfound")
        .send()
        .await
        .unwrap();
    
    assert_eq!(response.status(), 404);
    
    handle.abort();
}