//! Location identifiers and per-location sync state.
//!
//! Locations are string-based for N-remote extensibility.
//! "local" is reserved as the origin location (developer machine).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use super::error::SyncError;

// =============================================================================
// LocationId
// =============================================================================

/// Identifier for a sync location.
///
/// String-based to support arbitrary remotes: "pod", "cloud", "staging-pod",
/// "nas", "s3-archive", etc. `"local"` is reserved as the origin.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct LocationId(String);

impl<'de> Deserialize<'de> for LocationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

impl LocationId {
    /// Reserved ID for the local (origin) location.
    pub const LOCAL: &str = "local";

    /// Create a new LocationId. Empty strings are rejected.
    pub fn new(id: impl Into<String>) -> Result<Self, SyncError> {
        let id = id.into();
        if id.is_empty() {
            return Err(SyncError::InvalidLocation("empty location id".into()));
        }
        // Enforce lowercase alphanumeric + hyphens for consistency
        if !id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        {
            return Err(SyncError::InvalidLocation(format!(
                "location id must be lowercase alphanumeric with hyphens/underscores: {id}"
            )));
        }
        Ok(Self(id))
    }

    /// The canonical local location.
    pub fn local() -> Self {
        Self("local".into())
    }

    /// Whether this is the local (origin) location.
    pub fn is_local(&self) -> bool {
        self.0 == Self::LOCAL
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for LocationId {
    type Err = SyncError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

// =============================================================================
// LocationState
// =============================================================================

/// Sync state of a file at a specific location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocationState {
    /// File confirmed present at this location.
    Present,
    /// File needs to be synced to this location.
    Pending,
    /// Transfer in progress.
    Syncing,
    /// State not yet determined.
    Unknown,
    /// File confirmed absent at this location.
    Absent,
}

impl LocationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Pending => "pending",
            Self::Syncing => "syncing",
            Self::Unknown => "unknown",
            Self::Absent => "absent",
        }
    }

    /// Whether this state indicates the file needs sync action.
    ///
    /// `Unknown` is treated as needing sync because unverified locations
    /// should be resolved by attempting a push (fail-safe: sync rather than skip).
    pub fn needs_sync(&self) -> bool {
        matches!(self, Self::Pending | Self::Unknown)
    }
}

impl fmt::Display for LocationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for LocationState {
    type Err = SyncError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "present" => Ok(Self::Present),
            "pending" => Ok(Self::Pending),
            "syncing" => Ok(Self::Syncing),
            "unknown" => Ok(Self::Unknown),
            "absent" => Ok(Self::Absent),
            other => Err(SyncError::InvalidLocationState(other.to_string())),
        }
    }
}

// =============================================================================
// LocationSummary / SyncSummary
// =============================================================================

/// Per-location count of files by state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocationSummary {
    pub present: usize,
    pub pending: usize,
    pub syncing: usize,
    pub unknown: usize,
    pub absent: usize,
}

impl LocationSummary {
    pub fn total(&self) -> usize {
        self.present
            .saturating_add(self.pending)
            .saturating_add(self.syncing)
            .saturating_add(self.unknown)
            .saturating_add(self.absent)
    }

    pub fn increment(&mut self, state: LocationState) {
        match state {
            LocationState::Present => self.present = self.present.saturating_add(1),
            LocationState::Pending => self.pending = self.pending.saturating_add(1),
            LocationState::Syncing => self.syncing = self.syncing.saturating_add(1),
            LocationState::Unknown => self.unknown = self.unknown.saturating_add(1),
            LocationState::Absent => self.absent = self.absent.saturating_add(1),
        }
    }
}

/// Aggregated sync status across all locations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncSummary {
    pub locations: HashMap<LocationId, LocationSummary>,
    pub total_entries: usize,
    pub total_errors: usize,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_id_valid() {
        assert!(LocationId::new("pod").is_ok());
        assert!(LocationId::new("cloud").is_ok());
        assert!(LocationId::new("staging-pod").is_ok());
        assert!(LocationId::new("s3_archive").is_ok());
        assert!(LocationId::new("nas2").is_ok());
    }

    #[test]
    fn location_id_empty_rejected() {
        assert!(LocationId::new("").is_err());
    }

    #[test]
    fn location_id_invalid_chars_rejected() {
        assert!(LocationId::new("Pod").is_err()); // uppercase
        assert!(LocationId::new("my pod").is_err()); // space
        assert!(LocationId::new("cloud/b2").is_err()); // slash
    }

    #[test]
    fn location_id_local() {
        let loc = LocationId::local();
        assert!(loc.is_local());
        assert_eq!(loc.as_str(), "local");
    }

    #[test]
    fn location_id_non_local() {
        let loc = LocationId::new("pod").unwrap();
        assert!(!loc.is_local());
    }

    #[test]
    fn location_state_roundtrip() {
        for state in [
            LocationState::Present,
            LocationState::Pending,
            LocationState::Syncing,
            LocationState::Unknown,
            LocationState::Absent,
        ] {
            let s = state.as_str();
            let parsed: LocationState = s.parse().expect("should parse");
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn location_state_needs_sync() {
        assert!(LocationState::Pending.needs_sync());
        assert!(LocationState::Unknown.needs_sync());
        assert!(!LocationState::Present.needs_sync());
        assert!(!LocationState::Syncing.needs_sync());
        assert!(!LocationState::Absent.needs_sync());
    }

    #[test]
    fn location_summary_increment() {
        let mut s = LocationSummary::default();
        s.increment(LocationState::Present);
        s.increment(LocationState::Present);
        s.increment(LocationState::Pending);
        assert_eq!(s.present, 2);
        assert_eq!(s.pending, 1);
        assert_eq!(s.total(), 3);
    }

    #[test]
    fn location_id_serde() {
        let loc = LocationId::new("pod").unwrap();
        let json = serde_json::to_string(&loc).unwrap();
        assert_eq!(json, "\"pod\"");
        let back: LocationId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, loc);
    }

    #[test]
    fn location_id_serde_rejects_invalid() {
        // Empty string
        let r: Result<LocationId, _> = serde_json::from_str("\"\"");
        assert!(r.is_err(), "empty string must be rejected via serde");

        // Uppercase
        let r: Result<LocationId, _> = serde_json::from_str("\"Pod\"");
        assert!(r.is_err(), "uppercase must be rejected via serde");

        // Slash
        let r: Result<LocationId, _> = serde_json::from_str("\"cloud/b2\"");
        assert!(r.is_err(), "slash must be rejected via serde");

        // Space
        let r: Result<LocationId, _> = serde_json::from_str("\"my pod\"");
        assert!(r.is_err(), "space must be rejected via serde");
    }
}
