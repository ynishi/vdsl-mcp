//! Repository domain models: Workspace > Run > Generation.

use serde::{Deserialize, Serialize};

/// A workspace groups related runs (e.g. "gravure_klimt").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub created_at: String,
}

/// A run represents a single script execution within a workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub workspace_id: String,
    pub script: Option<String>,
    pub created_at: String,
}

/// A generation record — one output image from a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Generation {
    pub id: String,
    pub run_id: String,
    pub seed: Option<i64>,
    pub model: Option<String>,
    pub output: Option<String>,
    pub created_at: String,
    pub recipe: Option<String>,
    pub meta: Option<String>,
}

/// Filter for querying generations.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GenerationFilter {
    pub model: Option<String>,
    pub script: Option<String>,
    pub workspace: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
}

/// Query options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOpts {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl Default for QueryOpts {
    fn default() -> Self {
        Self {
            limit: Some(50),
            offset: None,
        }
    }
}

/// Generation with joined workspace/run info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationRow {
    #[serde(flatten)]
    pub gen: Generation,
    pub script: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_name: Option<String>,
}

/// Stats row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatRow {
    pub group: String,
    pub count: i64,
}

/// Repository trait — abstracts storage backend.
pub trait Repository: Send + Sync {
    fn ensure_workspace(&self, name: &str) -> Result<Workspace, RepositoryError>;
    fn list_workspaces(&self) -> Result<Vec<Workspace>, RepositoryError>;

    fn create_run(&self, workspace_id: &str, script: Option<&str>) -> Result<Run, RepositoryError>;
    fn find_runs_by_workspace(
        &self,
        workspace_id: &str,
        limit: u32,
    ) -> Result<Vec<Run>, RepositoryError>;

    fn save_generation(&self, gen: &Generation) -> Result<(), RepositoryError>;
    fn find_generation(&self, id: &str) -> Result<Option<Generation>, RepositoryError>;
    fn find_by_run(&self, run_id: &str) -> Result<Vec<Generation>, RepositoryError>;
    fn query_generations(
        &self,
        filter: &GenerationFilter,
        opts: &QueryOpts,
    ) -> Result<Vec<GenerationRow>, RepositoryError>;
    fn stats(&self, group_by: &str) -> Result<Vec<StatRow>, RepositoryError>;

    fn get_meta(&self, gen_id: &str) -> Result<Option<String>, RepositoryError>;
    fn set_meta(&self, gen_id: &str, meta_json: &str) -> Result<(), RepositoryError>;
}

/// Repository errors.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error("database error: {0}")]
    Database(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}
