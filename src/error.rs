use axum::{http::StatusCode, response::IntoResponse};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ProxyError>;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Repository not configured")]
    RepositoryNotFound,

    #[error("Cache error: {0}")]
    Cache(#[from] std::io::Error),

    #[error("Upstream error: {0}")]
    Upstream(#[from] reqwest::Error),

    #[error("Upstream returned {0}")]
    UpstreamStatus(StatusCode),

    #[error("Download failed: {0}")]
    Download(String),

    #[error("Timeout: {0}")]
    Timeout(String),
}

impl ProxyError {
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Self::RepositoryNotFound => StatusCode::NOT_FOUND,
            Self::UpstreamStatus(code) => *code,
            Self::Upstream(_) | Self::Download(_) => StatusCode::BAD_GATEWAY,
            Self::Cache(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> axum::response::Response {
        (self.status_code(), self.to_string()).into_response()
    }
}

impl From<tokio::time::error::Elapsed> for ProxyError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout("Operation timed out".into())
    }
}