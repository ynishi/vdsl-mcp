//! Human-readable formatting for sync state output.

use crate::domain::location::SyncSummary;

/// Format a sync summary as human-readable text.
pub fn format_summary(summary: &SyncSummary) -> String {
    let mut out = String::from("# Sync Summary\n\n");

    out.push_str(&format!(
        "Total: {} entries, {} errors\n\n",
        summary.total_entries, summary.total_errors
    ));

    let mut locs: Vec<_> = summary.locations.iter().collect();
    locs.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));

    for (loc_id, loc) in &locs {
        out.push_str(&format!(
            "{}: {} present, {} pending, {} syncing, {} absent\n",
            loc_id, loc.present, loc.pending, loc.syncing, loc.absent,
        ));
    }

    out
}

/// Format a file size as a human-readable string.
///
/// NOTE: Uses `f64` arithmetic. For files larger than 2^53 bytes (~9 PB)
/// the displayed value may lose precision in the fractional part.
/// This is acceptable for display purposes.
pub fn format_size(sz: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1_048_576;
    const GB: u64 = 1_073_741_824;
    const TB: u64 = 1_099_511_627_776;

    if sz >= TB {
        format!("{:.1}TB", sz as f64 / TB as f64)
    } else if sz >= GB {
        format!("{:.1}GB", sz as f64 / GB as f64)
    } else if sz >= MB {
        format!("{:.1}MB", sz as f64 / MB as f64)
    } else if sz >= KB {
        format!("{:.1}KB", sz as f64 / KB as f64)
    } else {
        format!("{sz}B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::location::{LocationId, LocationSummary};
    use std::collections::HashMap;

    #[test]
    fn format_summary_output() {
        let mut locations = HashMap::new();
        locations.insert(
            LocationId::local(),
            LocationSummary {
                present: 5,
                ..Default::default()
            },
        );
        locations.insert(
            LocationId::new("cloud").unwrap(),
            LocationSummary {
                present: 3,
                pending: 2,
                ..Default::default()
            },
        );
        let summary = SyncSummary {
            locations,
            total_entries: 5,
            total_errors: 0,
            error_entries: Vec::new(),
            pending_entries: Vec::new(),
        };
        let text = format_summary(&summary);
        assert!(text.contains("5 entries"));
        assert!(text.contains("cloud: 3 present, 2 pending, 0 syncing, 0 absent"));
    }

    #[test]
    fn format_size_ranges() {
        assert_eq!(super::format_size(0), "0B");
        assert_eq!(super::format_size(512), "512B");
        assert_eq!(super::format_size(1024), "1.0KB");
        assert_eq!(super::format_size(1_048_576), "1.0MB");
        assert_eq!(super::format_size(1_073_741_824), "1.0GB");
        assert_eq!(super::format_size(1_099_511_627_776), "1.0TB");
        assert_eq!(super::format_size(2_199_023_255_552), "2.0TB");
    }
}
