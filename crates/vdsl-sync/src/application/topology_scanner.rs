//! TopologyScanner — LocationScanner[] → TopologyDelta[] のオーケストレーター。
//!
//! 各Locationのスキャンは `LocationScanner` トレイト（infra層）が担当する。
//! 本モジュールはスキャン結果をDB上のTopologyFile/LocationFileと突合し、
//! TopologyDeltaを生成するApplication層のオーケストレーションのみを行う。
//!
//! # フロー
//!
//! ```text
//! LocationScanner[].scan() → ScannedFile[]
//!     ↓
//! compute_deltas(TopologyFile[], ScannedFile[]) → TopologyDelta[]
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::application::error::SyncError;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::location::LocationId;
use crate::domain::scan::{ScanOutcome, ScanReport};
use crate::domain::topology_delta::{
    ContentChangedFile, DiscoveredFile, TopologyDelta, VanishedFile,
};
use crate::domain::topology_file::{ScanMatch, TopologyFile};
use crate::infra::location_file_store::LocationFileStore;
use crate::infra::location_scanner::{LocationScanner, ScannedFile};
use crate::infra::topology_file_store::TopologyFileStore;

// =============================================================================
// Scan Result
// =============================================================================

/// スキャンエラー（非致命的）。
#[derive(Debug, Clone)]
pub struct TopologyScanError {
    pub path: String,
    pub error: String,
}

/// スキャン+delta生成の出力。
pub struct ScanResult {
    pub deltas: Vec<TopologyDelta>,
    pub scanned: usize,
    pub scan_errors: Vec<TopologyScanError>,
    pub scan_report: ScanReport,
}

// =============================================================================
// TopologyScanner
// =============================================================================

/// LocationScanner群 → TopologyDelta生成のオーケストレーター。
///
/// 各LocationのスキャンはLocationScannerトレイト（infra層）に委譲する。
/// 本構造体はスキャン結果 × DB上のTopologyFile/LocationFile の突合のみ担当。
pub struct TopologyScanner {
    topology_files: Arc<dyn TopologyFileStore>,
    location_files: Arc<dyn LocationFileStore>,
    scanners: Vec<Arc<dyn LocationScanner>>,
}

impl TopologyScanner {
    pub fn new(
        topology_files: Arc<dyn TopologyFileStore>,
        location_files: Arc<dyn LocationFileStore>,
        scanners: Vec<Arc<dyn LocationScanner>>,
    ) -> Self {
        Self {
            topology_files,
            location_files,
            scanners,
        }
    }

    /// 全Locationをスキャンし、TopologyDelta群を生成する。
    ///
    /// # フロー
    ///
    /// 1. LocationScanner毎にscan() → ScannedFile[]
    /// 2. DB上のTopologyFile/LocationFileを取得
    /// 3. ScannedFile × TopologyFile マッチング → TopologyDelta生成
    pub async fn scan_all(&self, excludes: &[glob::Pattern]) -> Result<ScanResult, SyncError> {
        let mut all_scanned: Vec<ScannedFile> = Vec::new();
        let mut all_errors: Vec<TopologyScanError> = Vec::new();
        let mut scan_report = ScanReport::new();

        let location_total = self.scanners.len();
        tracing::info!(locations = location_total, "topology_scan: starting");

        // Phase 1: Location毎にスキャン
        for (idx, scanner) in self.scanners.iter().enumerate() {
            let loc_id = scanner.location_id().clone();
            tracing::info!(
                location = %loc_id,
                index = idx,
                total = location_total,
                "topology_scan: scanning location"
            );

            match scanner.scan(excludes).await {
                Ok(result) => {
                    let entry_count = result.files.len();
                    let error_count = result.errors.len();
                    tracing::info!(
                        location = %loc_id,
                        entries = entry_count,
                        errors = error_count,
                        "topology_scan: location done"
                    );
                    scan_report.record(
                        loc_id.clone(),
                        ScanOutcome::Scanned {
                            entries: entry_count,
                            errors: error_count,
                        },
                    );
                    all_scanned.extend(result.files);
                    all_errors.extend(result.errors.into_iter().map(|e| TopologyScanError {
                        path: e.path,
                        error: e.error,
                    }));
                }
                Err(e) => {
                    tracing::error!(
                        location = %loc_id,
                        error = %e,
                        "topology_scan: location failed"
                    );
                    scan_report.record(
                        loc_id,
                        ScanOutcome::Failed {
                            error: e.to_string(),
                        },
                    );
                }
            }
        }

        let scanned = all_scanned.len();

        // Phase 2: TopologyDelta生成
        let deltas = self.compute_topology_deltas(&all_scanned).await?;

        tracing::info!(
            scanned,
            deltas = deltas.len(),
            errors = all_errors.len(),
            "topology_scan: delta generation complete"
        );

        Ok(ScanResult {
            deltas,
            scanned,
            scan_errors: all_errors,
            scan_report,
        })
    }

    // =========================================================================
    // Delta generation
    // =========================================================================

    /// ScannedFile群をDB上のTopologyFileとマッチングし、TopologyDeltaを生成する。
    ///
    /// # マッチングルール (TopologyFile::matches_scan)
    ///
    /// 1. canonical_hash一致 → ByHash (rename検出対応)
    /// 2. relative_path一致 → ByPath
    /// 3. 全不一致 → Discovered (新規)
    ///
    /// # Vanished検出
    ///
    /// DB上のLocationFileがActiveだが、スキャン結果に存在しない → Vanished
    async fn compute_topology_deltas(
        &self,
        scanned: &[ScannedFile],
    ) -> Result<Vec<TopologyDelta>, SyncError> {
        let mut deltas = Vec::new();

        // DB上の全TopologyFileを取得（マッチング用）
        let all_tfs = self.topology_files.list_active(None, None).await?;

        // origin毎にスキャン結果をグループ化
        let mut by_origin: HashMap<&LocationId, Vec<&ScannedFile>> = HashMap::new();
        for entry in scanned {
            by_origin.entry(&entry.origin).or_default().push(entry);
        }

        // origin毎に処理
        for (origin, entries) in &by_origin {
            let mut matched_tf_ids = std::collections::HashSet::new();

            for entry in entries {
                let matched = match_and_classify(entry, &all_tfs, &mut matched_tf_ids);
                if let Some(delta) = matched {
                    deltas.push(delta);
                }
            }

            // Vanished検出: このoriginにActiveなLocationFileがあるが、
            // スキャン結果にマッチしなかったTopologyFile
            let origin_lfs = self.location_files.list_by_location(origin).await?;
            let scanned_paths: std::collections::HashSet<&str> =
                entries.iter().map(|e| e.relative_path.as_str()).collect();

            for lf in &origin_lfs {
                if !lf.state().is_source_eligible() {
                    continue;
                }
                let tf_id = lf.file_id();
                if matched_tf_ids.contains(tf_id) {
                    continue;
                }
                let lf_path = lf.relative_path();
                if !scanned_paths.contains(lf_path) {
                    deltas.push(TopologyDelta::Vanished(VanishedFile {
                        topology_file_id: tf_id.to_string(),
                        relative_path: lf_path.to_string(),
                        origin: (*origin).clone(),
                    }));
                }
            }
        }

        Ok(deltas)
    }
}

// =============================================================================
// Domain純粋関数 — delta分類
// =============================================================================

/// 単一ScannedFileをTopologyFile群とマッチングし、適切なTopologyDeltaを返す。
///
/// TopologyFile::matches_scan (Domain) を使った分類ロジック。
/// &self不要の純粋関数。
fn match_and_classify(
    entry: &ScannedFile,
    all_tfs: &[TopologyFile],
    matched_tf_ids: &mut std::collections::HashSet<String>,
) -> Option<TopologyDelta> {
    for tf in all_tfs {
        match tf.matches_scan(&entry.relative_path, &entry.fingerprint) {
            ScanMatch::ByHash => {
                matched_tf_ids.insert(tf.id().to_string());
                if tf.relative_path() != entry.relative_path {
                    return Some(TopologyDelta::Renamed(
                        crate::domain::topology_delta::RenamedFile {
                            topology_file_id: tf.id().to_string(),
                            old_path: tf.relative_path().to_string(),
                            new_path: entry.relative_path.clone(),
                            file_type: entry.file_type,
                            fingerprint: entry.fingerprint.clone(),
                            origin: entry.origin.clone(),
                            embedded_id: entry.embedded_id.clone(),
                        },
                    ));
                }
                return None;
            }
            ScanMatch::ByPath => {
                matched_tf_ids.insert(tf.id().to_string());
                if fingerprint_changed(tf, &entry.fingerprint) {
                    return Some(TopologyDelta::ContentChanged(ContentChangedFile {
                        topology_file_id: tf.id().to_string(),
                        relative_path: entry.relative_path.clone(),
                        file_type: entry.file_type,
                        old_fingerprint: extract_tf_fingerprint(tf),
                        new_fingerprint: entry.fingerprint.clone(),
                        origin: entry.origin.clone(),
                        embedded_id: entry.embedded_id.clone(),
                    }));
                }
                return None;
            }
            ScanMatch::NoMatch => continue,
        }
    }

    Some(TopologyDelta::Discovered(DiscoveredFile {
        id: uuid::Uuid::new_v4().to_string(),
        relative_path: entry.relative_path.clone(),
        file_type: entry.file_type,
        fingerprint: entry.fingerprint.clone(),
        origin: entry.origin.clone(),
        embedded_id: entry.embedded_id.clone(),
    }))
}

/// TopologyFileのcanonical_hashとスキャンfingerprintを比較。
fn fingerprint_changed(tf: &TopologyFile, scan_fp: &FileFingerprint) -> bool {
    let scan_canonical = scan_fp
        .content_hash
        .as_deref()
        .or(scan_fp.file_hash.as_deref());
    match (tf.canonical_hash(), scan_canonical) {
        (Some(db), Some(scan)) => db != scan,
        (None, Some(_)) => true,
        (Some(_), None) => false,
        (None, None) => true,
    }
}

/// TopologyFileからfingerprint近似値を構築（ContentChangedのold_fingerprint用）。
fn extract_tf_fingerprint(tf: &TopologyFile) -> FileFingerprint {
    FileFingerprint {
        file_hash: tf.canonical_hash().map(|s| s.to_string()),
        content_hash: None,
        meta_hash: None,
        size: 0,
        modified_at: None,
    }
}
