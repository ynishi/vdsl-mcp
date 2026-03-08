//! SyncEntry — core entity representing a tracked file.
//!
//! Contains domain logic for state transitions, duplicate detection,
//! and metadata updates.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::file_type::FileType;
use super::location::{LocationId, LocationState};

/// A tracked file's synchronization state across all locations.
///
/// Paths are stored as **relative paths** from the sync root.
/// Each Location (local or remote) has its own root, and the full path is
/// resolved as `location_root.join(&entry.relative_path)`.
///
/// # Hash model
///
/// - `file_hash` — DJB2 of entire file bytes. **Required** for all files.
///   Used for change detection and generic duplicate detection.
/// - `content_hash` — format-specific semantic hash (e.g. DJB2 of PNG IHDR+IDAT).
///   Used for high-precision duplicate detection that ignores metadata.
///   `None` for non-PNG files.
///
/// # Field visibility
///
/// All fields are `pub` to allow direct clearing (e.g. `entry.content_hash = None`).
/// **Warning**: Modifying `file_hash` directly does NOT automatically mark remotes
/// as pending. Use [`update_metadata`](Self::update_metadata) for hash changes
/// so that remote locations are correctly transitioned to `Pending`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEntry {
    /// Unique identifier (UUID).
    pub id: String,
    /// Relative path from the sync root (unique key).
    pub relative_path: String,
    /// Classification of the file.
    pub file_type: FileType,
    /// DJB2 hash of entire file content. Always present.
    pub file_hash: String,
    /// Format-specific semantic hash (e.g. PNG pixel identity).
    pub content_hash: Option<String>,
    /// File size in bytes.
    pub file_size: Option<u64>,
    /// VDSL generation ID (links to generation record).
    pub gen_id: Option<String>,
    /// Per-location sync state.
    pub locations: HashMap<LocationId, LocationState>,
    /// Last sync error message.
    pub error: Option<String>,
    /// Timestamp of last successful sync.
    pub synced_at: Option<DateTime<Utc>>,
    /// Timestamp of last state change.
    pub updated_at: DateTime<Utc>,
}

impl SyncEntry {
    // =========================================================================
    // Factory
    // =========================================================================

    /// Create a new SyncEntry with local=present and all given remotes=pending.
    pub fn new(
        relative_path: String,
        file_type: FileType,
        file_hash: String,
        content_hash: Option<String>,
        file_size: Option<u64>,
        gen_id: Option<String>,
        remote_locations: &[LocationId],
    ) -> Self {
        let mut locations = HashMap::new();
        locations.insert(LocationId::local(), LocationState::Present);
        for loc in remote_locations {
            locations.insert(loc.clone(), LocationState::Pending);
        }
        Self::with_locations(
            relative_path,
            file_type,
            file_hash,
            content_hash,
            file_size,
            gen_id,
            locations,
        )
    }

    /// Create a new SyncEntry with caller-specified initial locations.
    ///
    /// Use this when the caller needs full control over location states
    /// (e.g. pull_file registering both local and source as present).
    pub fn with_locations(
        relative_path: String,
        file_type: FileType,
        file_hash: String,
        content_hash: Option<String>,
        file_size: Option<u64>,
        gen_id: Option<String>,
        locations: HashMap<LocationId, LocationState>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            relative_path,
            file_type,
            file_hash,
            content_hash,
            file_size,
            gen_id,
            locations,
            error: None,
            synced_at: None,
            updated_at: Utc::now(),
        }
    }

    // =========================================================================
    // Domain logic — state transitions
    // =========================================================================

    /// Update file metadata. If file_hash changed, marks all non-local locations as pending.
    ///
    /// `file_hash` is always present, so change detection is straightforward:
    /// any difference in file_hash triggers re-sync.
    ///
    /// # None semantics
    ///
    /// Optional fields (`content_hash`, `file_size`, `gen_id`) use **patch semantics**:
    /// - `Some(value)` → overwrite with new value
    /// - `None` → preserve existing value (no-op)
    ///
    /// To explicitly clear a field, assign directly: `entry.content_hash = None`.
    /// All fields are `pub` for this purpose.
    pub fn update_metadata(
        &mut self,
        file_type: FileType,
        file_hash: String,
        content_hash: Option<String>,
        file_size: Option<u64>,
        gen_id: Option<String>,
    ) {
        let hash_changed = file_hash != self.file_hash;

        self.file_type = file_type;
        self.file_hash = file_hash;
        if content_hash.is_some() {
            self.content_hash = content_hash;
        }
        if file_size.is_some() {
            self.file_size = file_size;
        }
        if gen_id.is_some() {
            self.gen_id = gen_id;
        }
        self.updated_at = Utc::now();

        if hash_changed {
            self.mark_remotes_pending();
        }
    }

    /// Mark all non-local locations as pending (e.g. after content change).
    pub fn mark_remotes_pending(&mut self) {
        for (loc, state) in self.locations.iter_mut() {
            if !loc.is_local() {
                *state = LocationState::Pending;
            }
        }
    }

    // =========================================================================
    // Queries
    // =========================================================================

    /// Get the sync state at a specific location.
    pub fn location_state(&self, loc: &LocationId) -> LocationState {
        self.locations
            .get(loc)
            .copied()
            .unwrap_or(LocationState::Unknown)
    }

    /// The best hash for duplicate detection: content_hash if available, else file_hash.
    pub fn identity_hash(&self) -> &str {
        self.content_hash.as_deref().unwrap_or(&self.file_hash)
    }

    /// Whether this file has a sync error.
    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    /// Whether this file needs sync to any non-local location.
    pub fn needs_any_sync(&self) -> bool {
        self.locations
            .iter()
            .any(|(loc, state)| !loc.is_local() && state.needs_sync())
    }

    /// Locations where sync is needed.
    pub fn pending_locations(&self) -> Vec<&LocationId> {
        self.locations
            .iter()
            .filter(|(loc, state)| !loc.is_local() && state.needs_sync())
            .map(|(loc, _)| loc)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(locations: Vec<(&str, LocationState)>) -> SyncEntry {
        let locs: HashMap<LocationId, LocationState> = locations
            .into_iter()
            .map(|(id, state)| (LocationId::new(id).unwrap(), state))
            .collect();
        SyncEntry {
            id: "test-id".into(),
            relative_path: "output/test.png".into(),
            file_type: FileType::Image,
            file_hash: "abc123".into(),
            content_hash: Some("def456".into()),
            file_size: Some(1024_u64),
            gen_id: Some("gen-001".into()),
            locations: locs,
            error: None,
            synced_at: None,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn factory_creates_with_local_present_and_remotes_pending() {
        let entry = SyncEntry::new(
            "output/test.png".into(),
            FileType::Image,
            "hash123".into(),
            Some("content123".into()),
            Some(1024),
            Some("gen-1".into()),
            &[
                LocationId::new("cloud").unwrap(),
                LocationId::new("pod").unwrap(),
            ],
        );
        assert_eq!(
            entry.location_state(&LocationId::local()),
            LocationState::Present
        );
        assert_eq!(
            entry.location_state(&LocationId::new("cloud").unwrap()),
            LocationState::Pending
        );
        assert_eq!(
            entry.location_state(&LocationId::new("pod").unwrap()),
            LocationState::Pending
        );
        assert!(!entry.id.is_empty());
    }

    #[test]
    fn update_metadata_marks_pending_on_hash_change() {
        let mut entry = make_entry(vec![
            ("local", LocationState::Present),
            ("cloud", LocationState::Present),
        ]);

        entry.update_metadata(FileType::Image, "new_hash".into(), None, None, None);

        assert_eq!(entry.file_hash, "new_hash");
        assert_eq!(
            entry.location_state(&LocationId::new("cloud").unwrap()),
            LocationState::Pending,
        );
        assert_eq!(
            entry.location_state(&LocationId::local()),
            LocationState::Present,
        );
    }

    #[test]
    fn update_metadata_no_change_without_hash_change() {
        let mut entry = make_entry(vec![
            ("local", LocationState::Present),
            ("cloud", LocationState::Present),
        ]);

        entry.update_metadata(
            FileType::Image,
            "abc123".into(), // same as initial
            None,
            None,
            None,
        );

        assert_eq!(
            entry.location_state(&LocationId::new("cloud").unwrap()),
            LocationState::Present,
        );
    }

    #[test]
    fn location_state_present() {
        let entry = make_entry(vec![
            ("local", LocationState::Present),
            ("cloud", LocationState::Pending),
        ]);
        assert_eq!(
            entry.location_state(&LocationId::local()),
            LocationState::Present
        );
        assert_eq!(
            entry.location_state(&LocationId::new("cloud").unwrap()),
            LocationState::Pending
        );
    }

    #[test]
    fn location_state_unknown_for_missing() {
        let entry = make_entry(vec![("local", LocationState::Present)]);
        assert_eq!(
            entry.location_state(&LocationId::new("pod").unwrap()),
            LocationState::Unknown
        );
    }

    #[test]
    fn identity_hash_prefers_content_hash() {
        let entry = make_entry(vec![]);
        assert_eq!(entry.identity_hash(), "def456");
    }

    #[test]
    fn identity_hash_falls_back_to_file_hash() {
        let mut entry = make_entry(vec![]);
        entry.content_hash = None;
        assert_eq!(entry.identity_hash(), "abc123");
    }

    #[test]
    fn needs_any_sync() {
        let entry = make_entry(vec![
            ("local", LocationState::Present),
            ("cloud", LocationState::Pending),
        ]);
        assert!(entry.needs_any_sync());
    }

    #[test]
    fn no_sync_needed_all_present() {
        let entry = make_entry(vec![
            ("local", LocationState::Present),
            ("cloud", LocationState::Present),
            ("pod", LocationState::Present),
        ]);
        assert!(!entry.needs_any_sync());
    }

    #[test]
    fn pending_locations() {
        let entry = make_entry(vec![
            ("local", LocationState::Present),
            ("cloud", LocationState::Pending),
            ("pod", LocationState::Present),
            ("nas", LocationState::Unknown),
        ]);
        let pending = entry.pending_locations();
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn has_error() {
        let mut entry = make_entry(vec![]);
        assert!(!entry.has_error());
        entry.error = Some("connection refused".into());
        assert!(entry.has_error());
    }
}
