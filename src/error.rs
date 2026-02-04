use axum::{http::StatusCode, response::IntoResponse};
use thiserror::Error;
use tracing::{warn, error};

pub type Result<T> = std::result::Result<T, ProxyError>;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    
    #[error("Repository not found for path")]
    RepositoryNotFound,
    
    #[error("Cache error: {0}")]
    Cache(#[from] std::io::Error),
    
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    
    #[error("Upstream returned {0}")]
    UpstreamError(StatusCode),
    
    #[error("Download failed: {0}")]
    Download(String),
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Self::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Self::RepositoryNotFound => StatusCode::NOT_FOUND,
            Self::UpstreamError(code) => *code,
            Self::Http(_) | Self::Download(_) => StatusCode::BAD_GATEWAY,
            Self::Cache(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        
        match status {
            StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => warn!("Request failed: {}", self),
            _ => error!("Request failed: {}", self),
        }
        
        (status, self.to_string()).into_response()
    }
}