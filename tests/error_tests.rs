use apt_cacher_rs::error::*;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::io;

#[test]
fn test_invalid_path_error() {
    let err = ProxyError::InvalidPath("test path".to_string());
    assert_eq!(err.to_string(), "Invalid path: test path");
}

#[test]
fn test_repository_not_found_error() {
    let err = ProxyError::RepositoryNotFound;
    assert_eq!(err.to_string(), "Repository not found for path");
}

#[test]
fn test_io_error_conversion() {
    let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
    let proxy_err: ProxyError = io_err.into();
    
    assert!(matches!(proxy_err, ProxyError::Cache(_)));
    assert!(proxy_err.to_string().contains("Cache error"));
}

#[test]
fn test_reqwest_error_conversion() {
    let reqwest_err = reqwest::Client::new()
        .get("http://[invalid")
        .build()
        .unwrap_err();
    
    let proxy_err: ProxyError = reqwest_err.into();
    assert!(matches!(proxy_err, ProxyError::Http(_)));
}

#[test]
fn test_error_to_response_invalid_path() {
    let err = ProxyError::InvalidPath("bad".to_string());
    let response = err.into_response();
    
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn test_error_to_response_not_found() {
    let err = ProxyError::RepositoryNotFound;
    let response = err.into_response();
    
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[test]
fn test_error_to_response_upstream_error() {
    let err = ProxyError::UpstreamError(StatusCode::SERVICE_UNAVAILABLE);
    let response = err.into_response();
    
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn test_error_to_response_http_error() {
    let reqwest_err = reqwest::Client::new()
        .get("http://[invalid")
        .build()
        .unwrap_err();
    
    let err = ProxyError::Http(reqwest_err);
    let response = err.into_response();
    
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[test]
fn test_error_to_response_download_error() {
    let err = ProxyError::Download("timeout".to_string());
    let response = err.into_response();
    
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[test]
fn test_error_to_response_cache_error() {
    let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "denied");
    let err = ProxyError::Cache(io_err);
    let response = err.into_response();
    
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn test_result_type_alias() {
    fn returns_result() -> Result<String> {
        Ok("success".to_string())
    }
    
    fn returns_error() -> Result<String> {
        Err(ProxyError::RepositoryNotFound)
    }
    
    assert!(returns_result().is_ok());
    assert!(returns_error().is_err());
}

#[test]
fn test_error_chain() {
    
    fn wrapper() -> Result<()> {
        let _ = std::fs::read("/nonexistent")?;
        Ok(())
    }
    
    let result = wrapper();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ProxyError::Cache(_)));
}

#[test]
fn test_multiple_error_types() {
    let errors = vec![
        ProxyError::InvalidPath("test".to_string()),
        ProxyError::RepositoryNotFound,
        ProxyError::Download("failed".to_string()),
        ProxyError::UpstreamError(StatusCode::NOT_FOUND),
    ];
    
    for err in errors {
        let response = err.into_response();
        assert!(response.status().as_u16() >= 400);
    }
}

#[tokio::test]
async fn test_error_response_body() {
    let err = ProxyError::InvalidPath("test/path".to_string());
    let response = err.into_response();
    
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
    
    assert!(body_str.contains("Invalid path"));
    assert!(body_str.contains("test/path"));
}