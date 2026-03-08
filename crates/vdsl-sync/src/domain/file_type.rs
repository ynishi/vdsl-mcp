//! File type classification for sync tracking.

use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::SyncError;

/// Type of file tracked by the sync engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    /// Generated image (PNG).
    Image,
    /// Generation recipe (JSON).
    Recipe,
    /// General asset file.
    Asset,
    /// Database file.
    Db,
}

impl FileType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Recipe => "recipe",
            Self::Asset => "asset",
            Self::Db => "db",
        }
    }

    /// Infer file type from a file extension (case-insensitive).
    ///
    /// Returns `Asset` for unrecognized extensions.
    pub fn from_extension(ext: &str) -> Self {
        match ext.to_ascii_lowercase().as_str() {
            "png" | "jpg" | "jpeg" | "webp" | "bmp" | "gif" | "tiff" | "tif" => Self::Image,
            "json" => Self::Recipe,
            "db" | "sqlite" | "sqlite3" => Self::Db,
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
    type Err = SyncError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "image" => Ok(Self::Image),
            "recipe" => Ok(Self::Recipe),
            "asset" => Ok(Self::Asset),
            "db" => Ok(Self::Db),
            other => Err(SyncError::InvalidFileType(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for ft in [
            FileType::Image,
            FileType::Recipe,
            FileType::Asset,
            FileType::Db,
        ] {
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
    fn display() {
        assert_eq!(format!("{}", FileType::Image), "image");
    }

    #[test]
    fn serde_roundtrip() {
        let ft = FileType::Recipe;
        let json = serde_json::to_string(&ft).unwrap();
        assert_eq!(json, "\"recipe\"");
        let back: FileType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ft);
    }
}
