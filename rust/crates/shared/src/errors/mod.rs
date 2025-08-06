use thiserror::Error;

/// Centralized error type for shared crate
#[derive(Error, Debug)]
pub enum SharedError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON (de)serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Utility error: {0}")]
    Utility(String),
}

/// Alias for fallible operations in the shared crate
pub type SharedResult<T> = Result<T, SharedError>;