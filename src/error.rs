use axum::{http::StatusCode, response::IntoResponse};
use std::fmt;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ProxyError>;

/// Wrapper for cache keys providing type safety
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey(String);

impl CacheKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for CacheKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for CacheKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for CacheKey {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

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

    #[error("Internal error: {0}")]
    Internal(String),
}

impl ProxyError {
    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Self::RepositoryNotFound(_) => StatusCode::NOT_FOUND,
            Self::UpstreamStatus(code) => *code,
            Self::Upstream(_) | Self::Download(_) => StatusCode::BAD_GATEWAY,
            Self::Cache(_) | Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Timeout => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    /// Creates a download error with formatted message
    pub fn download(msg: impl fmt::Display) -> Self {
        Self::Download(msg.to_string())
    }

    /// Creates an internal error with formatted message
    pub fn internal(msg: impl fmt::Display) -> Self {
        Self::Internal(msg.to_string())
    }

    /// Returns error category for metrics
    pub fn category(&self) -> &'static str {
        match self {
            Self::InvalidPath(_) => "invalid_path",
            Self::RepositoryNotFound(_) => "repo_not_found",
            Self::Cache(_) => "cache_io",
            Self::Upstream(_) => "upstream",
            Self::UpstreamStatus(_) => "upstream_status",
            Self::Download(_) => "download",
            Self::Timeout => "timeout",
            Self::Internal(_) => "internal",
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        let body = match &self {
            // Don't expose internal details for security
            Self::Cache(_) | Self::Internal(_) => "Internal server error".to_string(),
            other => other.to_string(),
        };
        (status, body).into_response()
    }
}

impl From<std::io::Error> for ProxyError {
    fn from(err: std::io::Error) -> Self {
        Self::Cache(err)
    }
}

impl From<reqwest::Error> for ProxyError {
    fn from(err: reqwest::Error) -> Self {
        Self::Upstream(err)
    }
}

impl From<tokio::time::error::Elapsed> for ProxyError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}