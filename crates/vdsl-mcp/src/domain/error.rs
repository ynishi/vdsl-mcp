#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("API key not configured: set VDSL_RUNPOD_API_KEY environment variable")]
    ApiKeyMissing,

    #[error("runpod-cli execution failed: {0}")]
    CliExecution(String),

    #[error("runpod-cli returned error (exit {code}): {message}")]
    CliError { code: i32, message: String },

    #[error("failed to parse CLI output: {reason}\nraw: {raw}")]
    ParseError { reason: String, raw: String },

    #[error("ComfyUI connection failed: {0}")]
    ComfyUiConnection(String),

    #[error("command timed out after {seconds}s")]
    ExecTimeout { seconds: u64 },

    #[error("unknown model dir key: {0}")]
    ModelTypeParse(String),
}
