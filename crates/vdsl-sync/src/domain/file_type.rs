//! File type classification for sync tracking.
//!
//! vdsl-sync is a generic distributed file storage engine.
//! FileType classifies tracked files at the storage level only.
//!
//! - **Image** — generated images (PNG, JPG, etc.). First-class entity
//!   that may embed generation origin (Recipe/Workflow) in metadata.
//! - **Asset** — all other files (JSON, text, config, raw recipes, etc.).
//!
//! Domain-specific semantics (e.g., "this JSON is a ComfyUI recipe")
//! belong in the consuming crate (vdsl-mcp), not here.

use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::DomainError;

/// Type of file tracked by the sync engine.
///
/// Intentionally minimal — only distinguishes files that require
/// different storage-level handling (e.g., content hashing strategy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    /// Generated image (PNG, JPG, etc.).
    /// May embed generation origin in metadata (PNG tEXt, EXIF, etc.).
    Image,
    /// General asset file (JSON, text, config, raw recipe, DB, etc.).
    Asset,
}

impl FileType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Asset => "asset",
        }
    }

    /// Infer file type from a file extension (case-insensitive).
    ///
    /// Returns `Asset` for unrecognized extensions.
    pub fn from_extension(ext: &str) -> Self {
        match ext.to_ascii_lowercase().as_str() {
            "png" | "jpg" | "jpeg" | "webp" | "bmp" | "gif" | "tiff" | "tif" => Self::Image,
            _ => Self::Asset,
        }
    }
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for FileType {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "image" => Ok(Self::Image),
            "asset" => Ok(Self::Asset),
            other => Err(DomainError::InvalidFileType(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for ft in [FileType::Image, FileType::Asset] {
            let s = ft.as_str();
            let parsed: FileType = s.parse().expect("should parse");
            assert_eq!(parsed, ft);
        }
    }

    #[test]
    fn invalid_type() {
        let result: Result<FileType, _> = "video".parse();
        assert!(result.is_err());
    }

    #[test]
    fn invalid_legacy_recipe() {
        let result: Result<FileType, _> = "recipe".parse();
        assert!(result.is_err(), "legacy 'recipe' must not parse");
    }

    #[test]
    fn invalid_legacy_db() {
        let result: Result<FileType, _> = "db".parse();
        assert!(result.is_err(), "legacy 'db' must not parse");
    }

    #[test]
    fn display() {
        assert_eq!(format!("{}", FileType::Image), "image");
        assert_eq!(format!("{}", FileType::Asset), "asset");
    }

    #[test]
    fn from_extension_images() {
        for ext in &[
            "png", "jpg", "jpeg", "webp", "bmp", "gif", "tiff", "tif", "PNG", "Jpg",
        ] {
            assert_eq!(FileType::from_extension(ext), FileType::Image);
        }
    }

    #[test]
    fn from_extension_assets() {
        for ext in &["json", "txt", "db", "sqlite", "toml", "yaml", "csv"] {
            assert_eq!(FileType::from_extension(ext), FileType::Asset);
        }
    }

    #[test]
    fn serde_roundtrip() {
        let ft = FileType::Asset;
        let json = serde_json::to_string(&ft).unwrap();
        assert_eq!(json, "\"asset\"");
        let back: FileType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ft);
    }
}
