use axum::{http::StatusCode, response::IntoResponse};
use std::io;
use thiserror::Error;
use tracing::{error, warn};

pub type Result<T> = std::result::Result<T, ProxyError>;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Repository not configured")]
    RepositoryNotFound,

    #[error("Cache I/O error: {0}")]
    Cache(#[from] io::Error),

    #[error("Upstream connection error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Upstream returned {0}")]
    UpstreamError(StatusCode),

    #[error("Download error: {0}")]
    Download(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

impl ProxyError {
    /// Returns true if this error is retryable
    #[inline]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Http(_) | Self::Timeout(_) | Self::UpstreamError(_)
        )
    }

    /// Returns the appropriate HTTP status code for this error
    #[inline]
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Self::RepositoryNotFound => StatusCode::NOT_FOUND,
            Self::UpstreamError(code) => *code,
            Self::Http(_) | Self::Download(_) => StatusCode::BAD_GATEWAY,
            Self::Cache(_) | Self::Config(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();

        match &self {
            Self::RepositoryNotFound => {
                warn!(error = %self, "Repository not found");
            }
            Self::InvalidPath(path) => {
                warn!(path = %path, "Invalid path requested");
            }
            Self::Http(e) => {
                error!(error = %e, "Upstream connection failed");
            }
            Self::Download(msg) => {
                error!(message = %msg, "Download failed");
            }
            Self::Cache(e) => {
                error!(error = %e, "Cache I/O error");
            }
            Self::UpstreamError(code) => {
                warn!(status = %code, "Upstream error");
            }
            Self::Timeout(msg) => {
                warn!(message = %msg, "Request timeout");
            }
            Self::Config(msg) => {
                error!(message = %msg, "Configuration error");
            }
        }

        (status, self.to_string()).into_response()
    }
}

impl From<tokio::time::error::Elapsed> for ProxyError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout("Operation timed out".into())
    }
}