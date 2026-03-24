//! Scanner — スキャン結果とDB状態を比較して FileDelta を生成する。
//!
//! infra層（ファイルリスト取得・ハッシュ計算）とdomain層（FileDelta）の橋渡し。
//! DB書き込みやTransfer作成は行わない。
//!
//! # フロー
//!
//! ```text
//! infra (list_src_files, hash) → ScannedEntry[]
//! scanner::compute_deltas(scanned, db_state) → FileDelta[]
//! plan::plan_all(deltas, graph, presence) → PlannedTransfer[]
//! ```

use std::collections::{HashMap, HashSet};

use crate::domain::delta::{AddedFile, FileDelta, ModifiedFile, RemovedFile};
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::location::LocationId;
use crate::domain::tracked_file::TrackedFile;

/// infra層から受け取るスキャン結果の1エントリ。
///
/// ファイルリスト取得・ハッシュ計算後の「検出された事実」を表す。
/// TrackedFileとは独立した値であり、DBに依存しない。
#[deprecated(note = "use LocationFile — ScannedEntry compares cross-location hashes")]
#[derive(Debug, Clone)]
pub struct ScannedEntry {
    pub relative_path: String,
    pub file_type: FileType,
    pub fingerprint: FileFingerprint,
    pub origin: LocationId,
    pub embedded_id: Option<String>,
}

/// スキャン結果とDB状態を比較し、FileDelta を生成する。
///
/// # 引数
///
/// - `scanned` — 特定の origin location で検出されたファイル群
/// - `db_files` — DB上の TrackedFile を relative_path でインデックスしたもの
///
/// # ルール
///
/// - scanned にあり DB にない → `Added`
/// - scanned にあり DB にあるが fingerprint が変化 → `Modified`
/// - scanned にあり DB にあるが fingerprint 同一 → スキップ（delta なし）
/// - DB にあり scanned にない → この関数では処理しない（`detect_removals()` で別途）
#[deprecated(note = "use TopologyStore pipeline — compute_deltas compares cross-location hashes")]
pub fn compute_deltas(
    scanned: &[ScannedEntry],
    db_files: &HashMap<String, &TrackedFile>,
) -> Vec<FileDelta> {
    let mut deltas = Vec::new();

    for entry in scanned {
        match db_files.get(&entry.relative_path) {
            None => {
                // DB未登録 → Added
                deltas.push(FileDelta::Added(AddedFile {
                    id: uuid::Uuid::new_v4().to_string(),
                    relative_path: entry.relative_path.clone(),
                    file_type: entry.file_type,
                    fingerprint: entry.fingerprint.clone(),
                    origin: entry.origin.clone(),
                    embedded_id: entry.embedded_id.clone(),
                }));
            }
            Some(existing) => {
                // deleted済みファイルが再検出された場合もAddedとして扱う
                if existing.is_deleted() {
                    deltas.push(FileDelta::Added(AddedFile {
                        id: uuid::Uuid::new_v4().to_string(),
                        relative_path: entry.relative_path.clone(),
                        file_type: entry.file_type,
                        fingerprint: entry.fingerprint.clone(),
                        origin: entry.origin.clone(),
                        embedded_id: entry.embedded_id.clone(),
                    }));
                    continue;
                }

                // fingerprint比較で変更検知
                if existing.has_changed(&entry.fingerprint) {
                    deltas.push(FileDelta::Modified(ModifiedFile {
                        file_id: existing.id().to_string(),
                        relative_path: entry.relative_path.clone(),
                        file_type: entry.file_type,
                        old_fingerprint: existing.fingerprint(),
                        new_fingerprint: entry.fingerprint.clone(),
                        origin: entry.origin.clone(),
                        embedded_id: entry.embedded_id.clone(),
                    }));
                }
                // fingerprint一致 → 変更なし、deltaなし
            }
        }
    }

    deltas
}

/// DB上に存在するがスキャン結果に含まれないファイルを Removed として検出する。
///
/// # 引数
///
/// - `db_files` — DB上のTrackedFile（deletedでないもの）を relative_path でインデックス
/// - `scanned_paths` — スキャンで検出されたファイルの relative_path 集合
/// - `origin` — 削除が検出された location
///
/// # 注意
///
/// この関数はlocal scanの場合のみ使用を想定。Cloud/Remote の削除検出は
/// リスト取得のコスト・タイミングが異なるため、別途対応が必要。
#[deprecated(note = "use TopologyStore pipeline")]
pub fn detect_removals(
    db_files: &HashMap<String, &TrackedFile>,
    scanned_paths: &HashSet<String>,
    origin: &LocationId,
) -> Vec<FileDelta> {
    db_files
        .iter()
        .filter(|(path, file)| {
            // deleted済みは対象外、スキャンに存在するものも対象外
            !file.is_deleted() && !scanned_paths.contains(*path)
        })
        .map(|(_, file)| {
            FileDelta::Removed(RemovedFile {
                file_id: file.id().to_string(),
                relative_path: file.relative_path().to_string(),
                origin: origin.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn local() -> LocationId {
        LocationId::local()
    }

    fn cloud() -> LocationId {
        LocationId::new("cloud").unwrap()
    }

    fn local_fp(hash: &str, size: u64) -> FileFingerprint {
        FileFingerprint {
            file_hash: Some(hash.to_string()),
            content_hash: None,
            meta_hash: None,
            size,
            modified_at: None,
        }
    }

    fn cloud_fp(size: u64) -> FileFingerprint {
        FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: None,
            size,
            modified_at: Some(Utc::now()),
        }
    }

    fn make_tracked(path: &str, hash: &str, size: u64) -> TrackedFile {
        TrackedFile::from_scan(
            path.to_string(),
            FileType::Image,
            hash.to_string(),
            None,
            size,
            None,
        )
        .unwrap()
    }

    fn make_entry(path: &str, fp: FileFingerprint, origin: LocationId) -> ScannedEntry {
        ScannedEntry {
            relative_path: path.to_string(),
            file_type: FileType::Image,
            fingerprint: fp,
            origin,
            embedded_id: None,
        }
    }

    // =========================================================================
    // compute_deltas — Added
    // =========================================================================

    #[test]
    fn new_file_produces_added_delta() {
        let scanned = vec![make_entry("new.png", local_fp("abc", 1024), local())];
        let db: HashMap<String, &TrackedFile> = HashMap::new();

        let deltas = compute_deltas(&scanned, &db);

        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].is_added());
        assert_eq!(deltas[0].relative_path(), "new.png");
    }

    #[test]
    fn deleted_file_re_detected_produces_added_delta() {
        let mut existing = make_tracked("gone.png", "old_hash", 512);
        existing.mark_deleted();

        let db: HashMap<String, &TrackedFile> =
            [("gone.png".to_string(), &existing)].into_iter().collect();

        let scanned = vec![make_entry("gone.png", local_fp("new_hash", 512), local())];
        let deltas = compute_deltas(&scanned, &db);

        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].is_added(), "deleted + re-detected = Added");
    }

    // =========================================================================
    // compute_deltas — Modified
    // =========================================================================

    #[test]
    fn changed_hash_produces_modified_delta() {
        let existing = make_tracked("img.png", "old_hash", 1024);
        let db: HashMap<String, &TrackedFile> =
            [("img.png".to_string(), &existing)].into_iter().collect();

        let scanned = vec![make_entry("img.png", local_fp("new_hash", 2048), local())];
        let deltas = compute_deltas(&scanned, &db);

        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].is_modified());
        if let FileDelta::Modified(m) = &deltas[0] {
            assert_eq!(m.file_id, existing.id());
        }
    }

    #[test]
    fn unchanged_file_produces_no_delta() {
        let existing = make_tracked("same.png", "hash1", 1024);
        let db: HashMap<String, &TrackedFile> =
            [("same.png".to_string(), &existing)].into_iter().collect();

        let scanned = vec![make_entry("same.png", local_fp("hash1", 1024), local())];
        let deltas = compute_deltas(&scanned, &db);

        assert_eq!(deltas.len(), 0, "unchanged file should not produce delta");
    }

    // =========================================================================
    // compute_deltas — Cloud (metadata-based)
    // =========================================================================

    #[test]
    fn cloud_new_file_produces_added() {
        let scanned = vec![make_entry("remote/photo.png", cloud_fp(4096), cloud())];
        let db: HashMap<String, &TrackedFile> = HashMap::new();

        let deltas = compute_deltas(&scanned, &db);

        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].is_added());
        assert_eq!(deltas[0].origin(), &cloud());
    }

    #[test]
    fn cloud_same_size_unchanged() {
        let ts = Utc::now();
        let existing = TrackedFile::from_cloud_scan(
            "remote/photo.png".to_string(),
            FileType::Image,
            4096,
            Some(ts),
        )
        .unwrap();

        let db: HashMap<String, &TrackedFile> = [("remote/photo.png".to_string(), &existing)]
            .into_iter()
            .collect();

        // 同一タイムスタンプを使用（実cloudでは同一ファイルなら同じ値）
        let fp = FileFingerprint {
            file_hash: None,
            content_hash: None,
            meta_hash: None,
            size: 4096,
            modified_at: Some(ts),
        };
        let scanned = vec![make_entry("remote/photo.png", fp, cloud())];
        let deltas = compute_deltas(&scanned, &db);

        assert_eq!(deltas.len(), 0, "same size+mtime cloud file = no change");
    }

    #[test]
    fn cloud_different_size_produces_modified() {
        let existing = TrackedFile::from_cloud_scan(
            "remote/photo.png".to_string(),
            FileType::Image,
            4096,
            Some(Utc::now()),
        )
        .unwrap();

        let db: HashMap<String, &TrackedFile> = [("remote/photo.png".to_string(), &existing)]
            .into_iter()
            .collect();

        let scanned = vec![make_entry("remote/photo.png", cloud_fp(8192), cloud())];
        let deltas = compute_deltas(&scanned, &db);

        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].is_modified());
    }

    // =========================================================================
    // compute_deltas — mixed batch
    // =========================================================================

    #[test]
    fn batch_produces_correct_mix() {
        let existing_unchanged = make_tracked("keep.png", "h1", 100);
        let existing_changed = make_tracked("update.png", "old", 200);

        let db: HashMap<String, &TrackedFile> = [
            ("keep.png".to_string(), &existing_unchanged),
            ("update.png".to_string(), &existing_changed),
        ]
        .into_iter()
        .collect();

        let scanned = vec![
            make_entry("keep.png", local_fp("h1", 100), local()), // unchanged
            make_entry("update.png", local_fp("new", 300), local()), // changed
            make_entry("brand_new.png", local_fp("x", 50), local()), // new
        ];

        let deltas = compute_deltas(&scanned, &db);

        assert_eq!(deltas.len(), 2, "1 modified + 1 added");
        let modified_count = deltas.iter().filter(|d| d.is_modified()).count();
        let added_count = deltas.iter().filter(|d| d.is_added()).count();
        assert_eq!(modified_count, 1);
        assert_eq!(added_count, 1);
    }

    // =========================================================================
    // detect_removals
    // =========================================================================

    #[test]
    fn missing_file_produces_removed_delta() {
        let existing = make_tracked("deleted.png", "h1", 1024);
        let db: HashMap<String, &TrackedFile> = [("deleted.png".to_string(), &existing)]
            .into_iter()
            .collect();

        let scanned_paths: HashSet<String> = HashSet::new(); // 何もスキャンされなかった

        let removals = detect_removals(&db, &scanned_paths, &local());

        assert_eq!(removals.len(), 1);
        assert!(removals[0].is_removed());
        assert_eq!(removals[0].file_id(), existing.id());
    }

    #[test]
    fn present_file_not_removed() {
        let existing = make_tracked("alive.png", "h1", 1024);
        let db: HashMap<String, &TrackedFile> =
            [("alive.png".to_string(), &existing)].into_iter().collect();

        let scanned_paths: HashSet<String> = ["alive.png".to_string()].into_iter().collect();

        let removals = detect_removals(&db, &scanned_paths, &local());

        assert_eq!(removals.len(), 0, "file exists in scan, not removed");
    }

    #[test]
    fn already_deleted_file_not_removed_again() {
        let mut existing = make_tracked("old.png", "h1", 1024);
        existing.mark_deleted();

        let db: HashMap<String, &TrackedFile> =
            [("old.png".to_string(), &existing)].into_iter().collect();

        let scanned_paths: HashSet<String> = HashSet::new();

        let removals = detect_removals(&db, &scanned_paths, &local());

        assert_eq!(
            removals.len(),
            0,
            "already deleted file should not produce removal delta"
        );
    }

    #[test]
    fn removals_use_correct_origin() {
        let existing = make_tracked("x.png", "h1", 1024);
        let db: HashMap<String, &TrackedFile> =
            [("x.png".to_string(), &existing)].into_iter().collect();

        let scanned_paths: HashSet<String> = HashSet::new();
        let removals = detect_removals(&db, &scanned_paths, &cloud());

        assert_eq!(removals.len(), 1);
        assert_eq!(removals[0].origin(), &cloud());
    }
}
