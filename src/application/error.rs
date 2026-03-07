use crate::domain::error::DomainError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),

    #[error("missing configuration: {0}")]
    MissingConfig(String),

    #[error("operation failed: {0}")]
    OperationFailed(String),
}
