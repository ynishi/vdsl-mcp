//! Generation registration helper — ComfyUI domain logic.
//!
//! Registers output image + optional recipe file via [`Store::put`].
//! This is ComfyUI-specific domain knowledge that does not belong in
//! the generic `vdsl-sync` crate.

use std::path::Path;

use vdsl_sync::{FileType, InfraError, PutOptions, Store, SyncError, TrackedFile};

/// Register a generation's output files (by absolute paths).
///
/// - `output`: absolute path to the generated image
/// - `recipe`: optional absolute path to the recipe JSON
///
/// Files that don't exist are skipped with a warning (not an error),
/// since ComfyUI may not always produce both outputs.
pub async fn register_generation(
    db: &Store,
    gen_id: &str,
    output: &str,
    recipe: Option<&str>,
) -> Result<Vec<TrackedFile>, SyncError> {
    let mut entries = Vec::new();

    match check_file_exists(Path::new(output)).await {
        Ok(()) => {
            let result = db
                .put(
                    output,
                    FileType::Image,
                    PutOptions {
                        embedded_id: Some(gen_id.to_string()),
                        ..Default::default()
                    },
                )
                .await?;
            entries.push(result.file);
        }
        Err(SyncError::Infra(InfraError::FileNotFound(_))) => {
            tracing::warn!(path = %output, "output file not found, skipping");
        }
        Err(e) => return Err(e),
    }

    if let Some(recipe_path) = recipe {
        match check_file_exists(Path::new(recipe_path)).await {
            Ok(()) => {
                let result = db
                    .put(
                        recipe_path,
                        FileType::Asset,
                        PutOptions {
                            embedded_id: Some(gen_id.to_string()),
                            ..Default::default()
                        },
                    )
                    .await?;
                entries.push(result.file);
            }
            Err(SyncError::Infra(InfraError::FileNotFound(_))) => {
                tracing::warn!(path = %recipe_path, "recipe file not found, skipping");
            }
            Err(e) => return Err(e),
        }
    }

    Ok(entries)
}

/// Check that a file exists, returning a typed error.
async fn check_file_exists(path: &Path) -> Result<(), SyncError> {
    match tokio::fs::try_exists(path).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(InfraError::FileNotFound(path.to_path_buf()).into()),
        Err(e) => Err(SyncError::from(e)),
    }
}
