//! Human-readable formatting for sync state output.

use crate::domain::entry::SyncEntry;
use crate::domain::location::{LocationId, SyncSummary};

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
            "{}: {} present, {} pending, {} syncing, {} unknown, {} absent\n",
            loc_id, loc.present, loc.pending, loc.syncing, loc.unknown, loc.absent,
        ));
    }

    out
}

/// Format a file size as a human-readable string.
///
/// NOTE: Uses `f64` arithmetic. For files larger than 2^53 bytes (~9 PB)
/// the displayed value may lose precision in the fractional part.
/// This is acceptable for display purposes.
fn format_size(sz: u64) -> String {
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

/// Format a list of SyncEntry as human-readable text.
pub fn format_entry_list(entries: &[SyncEntry]) -> String {
    if entries.is_empty() {
        return "No tracked files.".to_string();
    }

    let mut out = format!("# Tracked Files ({})\n\n", entries.len());
    for (i, e) in entries.iter().enumerate() {
        let hash_str = if e.file_hash.len() > 12 {
            format!("{}...", &e.file_hash[..12])
        } else if e.file_hash.is_empty() {
            "(empty)".to_string()
        } else {
            e.file_hash.clone()
        };

        let size_str = e
            .file_size
            .map(format_size)
            .unwrap_or_else(|| "?".to_string());

        let err_str = e
            .error
            .as_deref()
            .map(|err| format!(" ERR:{err}"))
            .unwrap_or_default();

        out.push_str(&format!(
            "{}. [{}] {} ({}, gen:{}, hash:{})\n",
            i + 1,
            e.file_type,
            e.relative_path,
            size_str,
            e.gen_id.as_deref().unwrap_or("-"),
            hash_str,
        ));

        // Location states
        let mut locs: Vec<_> = e.locations.iter().collect();
        locs.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
        let loc_strs: Vec<String> = locs
            .iter()
            .map(|(id, state)| format!("{}:{}", id, state))
            .collect();
        out.push_str(&format!("   {}{}\n", loc_strs.join(" "), err_str));
    }
    out
}

/// Format pending files for a specific destination.
pub fn format_pending(dest: &LocationId, entries: &[SyncEntry]) -> String {
    if entries.is_empty() {
        return format!("No files pending sync to {}.", dest);
    }

    let mut out = format!("# Pending -> {} ({})\n\n", dest, entries.len());
    for (i, e) in entries.iter().enumerate() {
        out.push_str(&format!(
            "{}. {} [{}] (gen:{})\n",
            i + 1,
            e.relative_path,
            e.file_type,
            e.gen_id.as_deref().unwrap_or("-"),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::file_type::FileType;
    use crate::domain::location::{LocationState, LocationSummary};
    use std::collections::HashMap;

    fn sample_entry() -> SyncEntry {
        let mut locations = HashMap::new();
        locations.insert(LocationId::local(), LocationState::Present);
        locations.insert(LocationId::new("cloud").unwrap(), LocationState::Pending);
        SyncEntry {
            id: "id-1".into(),
            relative_path: "/output/test.png".into(),
            file_type: FileType::Image,
            file_hash: "abc123def456".into(),
            content_hash: None,
            file_size: Some(1_500_000_u64),
            gen_id: Some("gen-001".into()),
            locations,
            error: None,
            synced_at: None,
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn format_empty_list() {
        assert_eq!(format_entry_list(&[]), "No tracked files.");
    }

    #[test]
    fn format_list_includes_info() {
        let text = format_entry_list(&[sample_entry()]);
        assert!(text.contains("/output/test.png"));
        assert!(text.contains("[image]"));
        assert!(text.contains("1.4MB"));
        assert!(text.contains("gen-001"));
        assert!(text.contains("cloud:pending"));
        assert!(text.contains("local:present"));
    }

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
        };
        let text = format_summary(&summary);
        assert!(text.contains("5 entries"));
        assert!(text.contains("cloud: 3 present, 2 pending"));
    }

    #[test]
    fn format_pending_empty() {
        let loc = LocationId::new("cloud").unwrap();
        let text = format_pending(&loc, &[]);
        assert!(text.contains("No files pending"));
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
