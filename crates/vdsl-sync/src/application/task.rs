//! Background task types for long-running sync operations.
//!
//! Provides shared types (`TaskId`, `TaskStatus`) used by interface layers
//! (MCP, Lua) to manage background sync tasks. The actual task registry
//! lives in the interface layer — Store itself is synchronous.
//!
//! # Design
//!
//! - `TaskId`: opaque UUID string identifying a spawned task
//! - `TaskStatus`: Pending | Running(phase) | Completed(T) | Failed(String)
//!
//! # Progress reporting
//!
//! `Running(String)` carries a human-readable phase description so that
//! poll() callers can display what the task is currently doing.
//! Example phases: "scanning 5000 files", "recovering 12 failed transfers",
//! "transferring 45/200 queued".

use uuid::Uuid;

/// Opaque task identifier (UUID v4).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct TaskId(String);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Reconstruct a TaskId from a string (e.g., from Lua poll call).
    pub fn parse(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Status of a background task.
///
/// # Serde output
///
/// ```json
/// {"status":"pending"}
/// {"status":"running","result":"scanning 5000 files..."}
/// {"status":"completed","result":{"scanned":5000,...}}
/// {"status":"failed","result":"rclone: exit code 1"}
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", content = "result")]
pub enum TaskStatus<T: Clone> {
    /// Task is spawned but not yet started processing.
    #[serde(rename = "pending")]
    Pending,
    /// Task is actively running. The String describes the current phase.
    ///
    /// Example phases:
    /// - `"scanning 5000 files..."`
    /// - `"recovering 12 failed transfers..."`
    /// - `"transferring 45/200 queued..."`
    #[serde(rename = "running")]
    Running(String),
    /// Task completed successfully with a result.
    #[serde(rename = "completed")]
    Completed(T),
    /// Task failed with an error message.
    #[serde(rename = "failed")]
    Failed(String),
}

impl<T: Clone> TaskStatus<T> {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed(_) | Self::Failed(_))
    }
}
