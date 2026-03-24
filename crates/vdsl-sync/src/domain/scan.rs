//! Scan outcome — per-location scan result.
//!
//! Tracks whether each location was successfully scanned, failed, or skipped.
//! This is a **domain concept**: for a sync product, knowing the scan status
//! of each source location is a primary concern, not a side-effect.

use serde::Serialize;
use std::collections::HashMap;
use std::fmt;

use super::location::LocationId;

/// Per-location scan outcome.
///
/// Represents what happened when attempting to scan a single source location.
/// Carries enough information for operators to diagnose sync gaps.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ScanOutcome {
    /// Successfully scanned. `entries` files detected, `errors` per-file failures.
    Scanned { entries: usize, errors: usize },
    /// Source was reachable but scan command failed (e.g. SSH find returned non-zero).
    Failed { error: String },
    /// Source was not reachable (connection refused, timeout, shell exec error).
    Unreachable { error: String },
}

impl ScanOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Scanned { .. })
    }

    pub fn is_failure(&self) -> bool {
        !self.is_success()
    }

    pub fn entries(&self) -> usize {
        match self {
            Self::Scanned { entries, .. } => *entries,
            _ => 0,
        }
    }
}

impl fmt::Display for ScanOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scanned { entries, errors } => {
                write!(f, "scanned ({entries} entries, {errors} errors)")
            }
            Self::Failed { error } => write!(f, "failed: {error}"),
            Self::Unreachable { error } => write!(f, "unreachable: {error}"),
        }
    }
}

/// Aggregated scan report across all source locations.
///
/// Each source location that was attempted during `scan_and_register`
/// gets an entry. Missing entries mean the location had no route as src.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ScanReport {
    pub outcomes: HashMap<LocationId, ScanOutcome>,
}

impl ScanReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, location: LocationId, outcome: ScanOutcome) {
        self.outcomes.insert(location, outcome);
    }

    /// Any location that failed or was unreachable.
    pub fn has_failures(&self) -> bool {
        self.outcomes.values().any(|o| o.is_failure())
    }

    /// Total entries detected across all successful scans.
    pub fn total_entries(&self) -> usize {
        self.outcomes.values().map(|o| o.entries()).sum()
    }

    /// Location IDs that failed or were unreachable.
    pub fn failed_locations(&self) -> Vec<&LocationId> {
        self.outcomes
            .iter()
            .filter(|(_, o)| o.is_failure())
            .map(|(id, _)| id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanned_is_success() {
        let o = ScanOutcome::Scanned {
            entries: 10,
            errors: 0,
        };
        assert!(o.is_success());
        assert_eq!(o.entries(), 10);
    }

    #[test]
    fn failed_is_failure() {
        let o = ScanOutcome::Failed {
            error: "exit code 1".into(),
        };
        assert!(o.is_failure());
        assert_eq!(o.entries(), 0);
    }

    #[test]
    fn unreachable_is_failure() {
        let o = ScanOutcome::Unreachable {
            error: "connection refused".into(),
        };
        assert!(o.is_failure());
    }

    #[test]
    fn report_tracks_failures() {
        let mut report = ScanReport::new();
        report.record(
            LocationId::local(),
            ScanOutcome::Scanned {
                entries: 100,
                errors: 2,
            },
        );
        report.record(
            LocationId::new("pod").unwrap(),
            ScanOutcome::Unreachable {
                error: "ssh timeout".into(),
            },
        );

        assert!(report.has_failures());
        assert_eq!(report.total_entries(), 100);
        assert_eq!(report.failed_locations().len(), 1);
        assert_eq!(report.failed_locations()[0].as_str(), "pod");
    }

    #[test]
    fn report_no_failures() {
        let mut report = ScanReport::new();
        report.record(
            LocationId::local(),
            ScanOutcome::Scanned {
                entries: 50,
                errors: 0,
            },
        );
        assert!(!report.has_failures());
    }

    #[test]
    fn display_format() {
        assert_eq!(
            ScanOutcome::Scanned {
                entries: 10,
                errors: 2
            }
            .to_string(),
            "scanned (10 entries, 2 errors)"
        );
        assert_eq!(
            ScanOutcome::Failed {
                error: "exit 1".into()
            }
            .to_string(),
            "failed: exit 1"
        );
        assert_eq!(
            ScanOutcome::Unreachable {
                error: "timeout".into()
            }
            .to_string(),
            "unreachable: timeout"
        );
    }
}
