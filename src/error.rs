use axum::{http::StatusCode, response::IntoResponse};
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
    Cache(#[from] std::io::Error),

    #[error("Upstream connection error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Upstream returned {0}")]
    UpstreamError(StatusCode),

    #[error("Download error: {0}")]
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
        }

        (status, self.to_string()).into_response()
    }
}