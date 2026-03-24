//! Generation registration helper — ComfyUI domain logic.
//!
//! Registers output image + optional recipe file via [`SyncService::notify`].
//! This is ComfyUI-specific domain knowledge that does not belong in
//! the generic `vdsl-sync` crate.

use std::path::Path;

use vdsl_sync::{FileType, SyncError, SyncService, TrackedFile};

/// Register a generation's output files (by absolute paths).
///
/// - `output`: absolute path to the generated image
/// - `recipe`: optional absolute path to the recipe JSON
///
/// Files that don't exist are skipped with a warning (not an error),
/// since ComfyUI may not always produce both outputs.
pub async fn register_generation(
    svc: &SyncService,
    gen_id: &str,
    output: &str,
    recipe: Option<&str>,
) -> Result<Vec<TrackedFile>, SyncError> {
    let mut entries = Vec::new();

    match check_file_exists(Path::new(output)).await {
        Ok(()) => {
            let result = svc.notify(output, FileType::Image, Some(gen_id)).await?;
            entries.push(result.file);
        }
        Err(SyncError::FileNotFound(_)) => {
            eprintln!("[WARN] output file not found, skipping: {output}");
        }
        Err(e) => return Err(e),
    }

    if let Some(recipe_path) = recipe {
        match check_file_exists(Path::new(recipe_path)).await {
            Ok(()) => {
                let result = svc
                    .notify(recipe_path, FileType::Recipe, Some(gen_id))
                    .await?;
                entries.push(result.file);
            }
            Err(SyncError::FileNotFound(_)) => {
                eprintln!("[WARN] recipe file not found, skipping: {recipe_path}");
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
        Ok(false) => Err(SyncError::FileNotFound(path.to_path_buf())),
        Err(e) => Err(SyncError::Io(e)),
    }
}
