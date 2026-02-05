use axum::{http::StatusCode, response::IntoResponse};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ProxyError>;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Repository not configured: {0}")]
    RepositoryNotFound(String),

    #[error("Cache I/O error: {0}")]
    Cache(#[source] std::io::Error),

    #[error("Upstream request failed: {0}")]
    Upstream(#[source] reqwest::Error),

    #[error("Upstream returned {0}")]
    UpstreamStatus(StatusCode),

    #[error("Download failed: {0}")]
    Download(String),

    #[error("Request timed out")]
    Timeout,
}

impl ProxyError {
    pub fn download(msg: impl std::fmt::Display) -> Self {
        Self::Download(msg.to_string())
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Self::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Self::RepositoryNotFound(_) => StatusCode::NOT_FOUND,
            Self::UpstreamStatus(code) => *code,
            Self::Upstream(_) | Self::Download(_) => StatusCode::BAD_GATEWAY,
            Self::Cache(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Timeout => StatusCode::GATEWAY_TIMEOUT,
        };
        
        let body = match &self {
            Self::Cache(_) => "Internal server error".to_string(),
            other => other.to_string(),
        };
        
        (status, body).into_response()
    }
}

impl From<std::io::Error> for ProxyError {
    fn from(err: std::io::Error) -> Self { Self::Cache(err) }
}

impl From<reqwest::Error> for ProxyError {
    fn from(err: reqwest::Error) -> Self { Self::Upstream(err) }
}