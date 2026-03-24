//! LocationFile — 特定locationにおけるファイルの実体情報。
//!
//! 同じファイルでもlocation毎にhashアルゴリズムが異なる:
//! - local: DJB2 (16文字hex)
//! - pod/remote: SHA-256 (64文字hex)
//! - cloud: hashなし (sizeのみ)
//!
//! `compute_deltas`は同一locationのLocationFile同士を比較するため、
//! アルゴリズム不一致による誤検知が発生しない。
//!
//! # relative_path
//!
//! 各locationでのファイルパスを保持する。通常はTopologyFileと同一だが、
//! 将来のrename対応でlocation毎に異なるパスを持つケースに対応する。
//!
//! # 状態遷移
//!
//! ```text
//! Active ← Syncing (Transfer完了)
//!   ↓           ↑
//! Stale ────────┘ (Transfer作成)
//!   ↓
//! Active (スキャンでfingerprint更新)
//!
//! Active → Missing (スキャンでファイル未検出)
//! Missing → Active (再スキャンで復帰)
//!
//! Active → Archived (ユーザー操作 / ポリシー)
//! Archived → Active (ユーザー操作 / 復元)
//!
//! ※ ArchivedなファイルはDistributeフェーズの転送対象から除外される
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::DomainError;
use super::fingerprint::FileFingerprint;
use super::location::LocationId;

/// このLocationにおけるファイルの状態。
///
/// Transfer状態マシンと連動して遷移する。
///
/// # Distribute除外ルール
///
/// - `Archived` なLocationFileは転送対象から除外される
/// - `Missing` なLocationFileは転送ソースとして使用不可
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocationFileState {
    /// ファイルが存在し、fingerprintが最新。
    Active,
    /// 転送中。このLocationへのSync Transferがin-flight。
    Syncing,
    /// ファイルは存在するがfingerprintが古い。更新転送待ち。
    Stale,
    /// スキャンでファイルが検出されなかった。一時的な欠落。
    Missing,
    /// アーカイブ済み。コールドストレージ等。転送対象外。
    Archived,
}

impl LocationFileState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Syncing => "syncing",
            Self::Stale => "stale",
            Self::Missing => "missing",
            Self::Archived => "archived",
        }
    }

    /// 転送ソースとして使用可能か。
    pub fn is_source_eligible(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Distributeフェーズの転送対象か。
    pub fn is_distribute_target(&self) -> bool {
        !matches!(self, Self::Archived)
    }

    /// ファイルがこのLocationに物理的に存在すると期待できるか。
    pub fn is_present(&self) -> bool {
        matches!(self, Self::Active | Self::Stale | Self::Archived)
    }
}

impl fmt::Display for LocationFileState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for LocationFileState {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "syncing" => Ok(Self::Syncing),
            "stale" => Ok(Self::Stale),
            "missing" => Ok(Self::Missing),
            "archived" => Ok(Self::Archived),
            other => Err(DomainError::Validation {
                field: "location_file_state".into(),
                reason: format!("unknown state: {other}"),
            }),
        }
    }
}

/// 特定locationにおけるファイルの実体情報。
///
/// # 設計原則
///
/// - `(file_id, location_id)` が一意キー
/// - `relative_path` はこのlocationでの実パス
/// - fingerprintはlocation固有のhashアルゴリズムで計算された値
/// - 同一ファイルでもlocation間でfingerprintは異なる（DJB2 vs SHA-256等）
/// - 変更検知は同一location内でのみ比較する
/// - `state` がTransfer連動でこのLocationでのファイル状態を管理する
///
/// # ライフサイクル
///
/// 1. スキャンで検出 → `TopologyFile::materialize()` で作成 (state=Active)
/// 2. 他Locationで変更検出 → `mark_stale()` (state=Stale)
/// 3. Transfer作成 → `mark_syncing()` (state=Syncing)
/// 4. Transfer完了 → `mark_active()` + fingerprint更新 (state=Active)
/// 5. スキャンでファイル未検出 → `mark_missing()` (state=Missing)
/// 6. アーカイブ → `archive()` (state=Archived, 転送対象外)
/// 7. アーカイブ解除 → `unarchive()` (state=Active)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationFile {
    file_id: String,
    location_id: LocationId,
    /// このlocationでのrelative_path。
    /// 通常はTopologyFile.relative_pathと同一。
    relative_path: String,
    fingerprint: FileFingerprint,
    state: LocationFileState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    embedded_id: Option<String>,
    updated_at: DateTime<Utc>,
}

impl LocationFile {
    // =========================================================================
    // Factory
    // =========================================================================

    /// スキャン結果から新規作成。state = Active。
    ///
    /// # Errors
    ///
    /// - `file_id` が空文字列の場合
    /// - `relative_path` が空文字列の場合
    pub fn new(
        file_id: String,
        location_id: LocationId,
        relative_path: String,
        fingerprint: FileFingerprint,
        embedded_id: Option<String>,
    ) -> Result<Self, DomainError> {
        if file_id.is_empty() {
            return Err(DomainError::Validation {
                field: "file_id".into(),
                reason: "must not be empty".into(),
            });
        }
        if relative_path.is_empty() {
            return Err(DomainError::Validation {
                field: "relative_path".into(),
                reason: "must not be empty".into(),
            });
        }
        Ok(Self {
            file_id,
            location_id,
            relative_path,
            fingerprint,
            state: LocationFileState::Active,
            embedded_id,
            updated_at: Utc::now(),
        })
    }

    /// DB復元用。
    pub(crate) fn reconstitute(
        file_id: String,
        location_id: LocationId,
        relative_path: String,
        fingerprint: FileFingerprint,
        state: LocationFileState,
        embedded_id: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            file_id,
            location_id,
            relative_path,
            fingerprint,
            state,
            embedded_id,
            updated_at,
        }
    }

    // =========================================================================
    // Commands — fingerprint
    // =========================================================================

    /// スキャン結果でfingerprintを更新。変更があればtrue。
    /// state も Active に戻す（スキャンで最新データが確認されたため）。
    ///
    /// **同一locationのスキャン結果のみ**渡すこと。
    /// 異なるlocationのfingerprintを渡すとhashアルゴリズム不一致で
    /// 常にtrue（変更あり）を返す。
    pub fn update_fingerprint(
        &mut self,
        new_fingerprint: FileFingerprint,
        new_embedded_id: Option<String>,
    ) -> bool {
        let changed = !self.fingerprint.matches_within_location(&new_fingerprint);
        self.fingerprint = new_fingerprint;
        self.embedded_id = new_embedded_id;
        self.state = LocationFileState::Active;
        if changed {
            self.updated_at = Utc::now();
        }
        changed
    }

    /// スキャン結果と比較し、変更があればtrue。
    pub fn has_changed(&self, scan_fingerprint: &FileFingerprint) -> bool {
        !self.fingerprint.matches_within_location(scan_fingerprint)
    }

    // =========================================================================
    // Commands — state transitions
    // =========================================================================

    /// 他Locationで変更検出 → このLocationのファイルは古い。
    ///
    /// Active → Stale。Archived は遷移しない（アーカイブは保護される）。
    ///
    /// # Returns
    ///
    /// 遷移した場合true。
    pub fn mark_stale(&mut self) -> bool {
        if self.state == LocationFileState::Archived {
            return false;
        }
        if self.state != LocationFileState::Stale {
            self.state = LocationFileState::Stale;
            self.updated_at = Utc::now();
            true
        } else {
            false
        }
    }

    /// Transfer作成 → このLocationへの転送が開始された。
    ///
    /// Stale/Missing → Syncing。
    /// Active からの遷移は許可しない（既に最新なら転送不要）。
    ///
    /// # Errors
    ///
    /// Active, Archived, Syncing からの遷移はエラー。
    pub fn mark_syncing(&mut self) -> Result<(), DomainError> {
        match self.state {
            LocationFileState::Stale | LocationFileState::Missing => {
                self.state = LocationFileState::Syncing;
                self.updated_at = Utc::now();
                Ok(())
            }
            other => Err(DomainError::InvalidStateTransition {
                from: other.as_str().to_string(),
                to: "syncing".to_string(),
            }),
        }
    }

    /// Transfer完了 → このLocationのファイルが最新になった。
    ///
    /// Syncing → Active。fingerprintも同時に更新される想定
    /// （update_fingerprintが別途呼ばれる）。
    ///
    /// # Errors
    ///
    /// Syncing以外からの遷移はエラー。
    pub fn mark_active(&mut self) -> Result<(), DomainError> {
        if self.state != LocationFileState::Syncing {
            return Err(DomainError::InvalidStateTransition {
                from: self.state.as_str().to_string(),
                to: "active".to_string(),
            });
        }
        self.state = LocationFileState::Active;
        self.updated_at = Utc::now();
        Ok(())
    }

    /// スキャンでファイルが検出されなかった。
    ///
    /// Active/Stale → Missing。Archived は遷移しない。
    ///
    /// # Returns
    ///
    /// 遷移した場合true。
    pub fn mark_missing(&mut self) -> bool {
        if self.state == LocationFileState::Archived {
            return false;
        }
        if self.state != LocationFileState::Missing {
            self.state = LocationFileState::Missing;
            self.updated_at = Utc::now();
            true
        } else {
            false
        }
    }

    /// アーカイブ。転送対象から除外される。
    ///
    /// 任意の状態から遷移可能（Syncing中でもユーザー指示で中断→アーカイブ）。
    /// 冪等。
    pub fn archive(&mut self) {
        if self.state != LocationFileState::Archived {
            self.state = LocationFileState::Archived;
            self.updated_at = Utc::now();
        }
    }

    /// アーカイブ解除。Active に復帰。
    ///
    /// # Errors
    ///
    /// Archived以外からの遷移はエラー。
    pub fn unarchive(&mut self) -> Result<(), DomainError> {
        if self.state != LocationFileState::Archived {
            return Err(DomainError::InvalidStateTransition {
                from: self.state.as_str().to_string(),
                to: "active (unarchive)".to_string(),
            });
        }
        self.state = LocationFileState::Active;
        self.updated_at = Utc::now();
        Ok(())
    }

    // =========================================================================
    // Queries
    // =========================================================================

    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    pub fn location_id(&self) -> &LocationId {
        &self.location_id
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn fingerprint(&self) -> &FileFingerprint {
        &self.fingerprint
    }

    pub fn state(&self) -> LocationFileState {
        self.state
    }

    pub fn embedded_id(&self) -> Option<&str> {
        self.embedded_id.as_deref()
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_loc() -> LocationId {
        LocationId::local()
    }

    fn pod_loc() -> LocationId {
        LocationId::new("pod").unwrap()
    }

    fn djb2_fp(hash: &str, size: u64) -> FileFingerprint {
        use super::super::digest::ByteDigest;
        FileFingerprint {
            byte_digest: Some(ByteDigest::Djb2(hash.to_string())),
            content_digest: None,
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    fn sha256_fp(hash: &str, size: u64) -> FileFingerprint {
        use super::super::digest::ByteDigest;
        FileFingerprint {
            byte_digest: Some(ByteDigest::Sha256(hash.to_string())),
            content_digest: None,
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    fn cloud_fp(size: u64) -> FileFingerprint {
        FileFingerprint {
            byte_digest: None,
            content_digest: None,
            meta_digest: None,
            size,
            modified_at: None,
        }
    }

    fn make_lf() -> LocationFile {
        LocationFile::new(
            "f1".into(),
            local_loc(),
            "a.png".into(),
            djb2_fp("abc123", 1024),
            None,
        )
        .unwrap()
    }

    // =========================================================================
    // Factory
    // =========================================================================

    #[test]
    fn new_sets_fields_and_active_state() {
        let lf = LocationFile::new(
            "file-1".into(),
            local_loc(),
            "output/gen-001.png".into(),
            djb2_fp("abc123", 1024),
            Some("gen-001".into()),
        )
        .unwrap();
        assert_eq!(lf.file_id(), "file-1");
        assert_eq!(lf.location_id(), &local_loc());
        assert_eq!(lf.relative_path(), "output/gen-001.png");
        assert_eq!(
            lf.fingerprint().byte_digest.as_ref().map(|d| d.as_str()),
            Some("abc123")
        );
        assert_eq!(lf.embedded_id(), Some("gen-001"));
        assert_eq!(lf.state(), LocationFileState::Active);
    }

    #[test]
    fn new_rejects_empty_file_id() {
        let result = LocationFile::new(
            "".into(),
            local_loc(),
            "a.png".into(),
            djb2_fp("abc", 100),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn new_rejects_empty_relative_path() {
        let result = LocationFile::new(
            "file-1".into(),
            local_loc(),
            "".into(),
            djb2_fp("abc", 100),
            None,
        );
        assert!(result.is_err());
    }

    // =========================================================================
    // has_changed — 同一location内での比較（正常系）
    // =========================================================================

    #[test]
    fn same_hash_not_changed() {
        let lf = make_lf();
        assert!(!lf.has_changed(&djb2_fp("abc123", 1024)));
    }

    #[test]
    fn different_hash_is_changed() {
        let lf = make_lf();
        assert!(lf.has_changed(&djb2_fp("def456", 1024)));
    }

    // =========================================================================
    // has_changed — cloud (hashなし、size比較)
    // =========================================================================

    #[test]
    fn cloud_same_size_not_changed() {
        let lf = LocationFile::new(
            "f1".into(),
            LocationId::new("cloud").unwrap(),
            "cloud/photo.png".into(),
            cloud_fp(2048),
            None,
        )
        .unwrap();
        assert!(!lf.has_changed(&cloud_fp(2048)));
    }

    #[test]
    fn cloud_different_size_is_changed() {
        let lf = LocationFile::new(
            "f1".into(),
            LocationId::new("cloud").unwrap(),
            "cloud/photo.png".into(),
            cloud_fp(2048),
            None,
        )
        .unwrap();
        assert!(lf.has_changed(&cloud_fp(4096)));
    }

    // =========================================================================
    // update_fingerprint
    // =========================================================================

    #[test]
    fn update_returns_true_on_change() {
        let mut lf = make_lf();
        let changed = lf.update_fingerprint(djb2_fp("new", 2048), None);
        assert!(changed);
        assert_eq!(
            lf.fingerprint().byte_digest.as_ref().map(|d| d.as_str()),
            Some("new")
        );
        assert_eq!(lf.state(), LocationFileState::Active);
    }

    #[test]
    fn update_returns_false_on_same() {
        let mut lf = make_lf();
        let changed = lf.update_fingerprint(djb2_fp("abc123", 1024), None);
        assert!(!changed);
    }

    #[test]
    fn update_fingerprint_resets_state_to_active() {
        let mut lf = make_lf();
        lf.mark_stale();
        assert_eq!(lf.state(), LocationFileState::Stale);
        lf.update_fingerprint(djb2_fp("new", 2048), None);
        assert_eq!(lf.state(), LocationFileState::Active);
    }

    // =========================================================================
    // State transitions — mark_stale
    // =========================================================================

    #[test]
    fn mark_stale_from_active() {
        let mut lf = make_lf();
        assert!(lf.mark_stale());
        assert_eq!(lf.state(), LocationFileState::Stale);
    }

    #[test]
    fn mark_stale_idempotent() {
        let mut lf = make_lf();
        lf.mark_stale();
        let ts = lf.updated_at();
        assert!(!lf.mark_stale());
        assert_eq!(lf.updated_at(), ts);
    }

    #[test]
    fn mark_stale_skips_archived() {
        let mut lf = make_lf();
        lf.archive();
        assert!(!lf.mark_stale());
        assert_eq!(lf.state(), LocationFileState::Archived);
    }

    // =========================================================================
    // State transitions — mark_syncing
    // =========================================================================

    #[test]
    fn mark_syncing_from_stale() {
        let mut lf = make_lf();
        lf.mark_stale();
        lf.mark_syncing().unwrap();
        assert_eq!(lf.state(), LocationFileState::Syncing);
    }

    #[test]
    fn mark_syncing_from_missing() {
        let mut lf = make_lf();
        lf.mark_missing();
        lf.mark_syncing().unwrap();
        assert_eq!(lf.state(), LocationFileState::Syncing);
    }

    #[test]
    fn mark_syncing_from_active_fails() {
        let mut lf = make_lf();
        assert!(lf.mark_syncing().is_err());
    }

    #[test]
    fn mark_syncing_from_archived_fails() {
        let mut lf = make_lf();
        lf.archive();
        assert!(lf.mark_syncing().is_err());
    }

    #[test]
    fn mark_syncing_from_syncing_fails() {
        let mut lf = make_lf();
        lf.mark_stale();
        lf.mark_syncing().unwrap();
        assert!(lf.mark_syncing().is_err());
    }

    // =========================================================================
    // State transitions — mark_active
    // =========================================================================

    #[test]
    fn mark_active_from_syncing() {
        let mut lf = make_lf();
        lf.mark_stale();
        lf.mark_syncing().unwrap();
        lf.mark_active().unwrap();
        assert_eq!(lf.state(), LocationFileState::Active);
    }

    #[test]
    fn mark_active_from_stale_fails() {
        let mut lf = make_lf();
        lf.mark_stale();
        assert!(lf.mark_active().is_err());
    }

    // =========================================================================
    // State transitions — mark_missing
    // =========================================================================

    #[test]
    fn mark_missing_from_active() {
        let mut lf = make_lf();
        assert!(lf.mark_missing());
        assert_eq!(lf.state(), LocationFileState::Missing);
    }

    #[test]
    fn mark_missing_from_stale() {
        let mut lf = make_lf();
        lf.mark_stale();
        assert!(lf.mark_missing());
        assert_eq!(lf.state(), LocationFileState::Missing);
    }

    #[test]
    fn mark_missing_idempotent() {
        let mut lf = make_lf();
        lf.mark_missing();
        assert!(!lf.mark_missing());
    }

    #[test]
    fn mark_missing_skips_archived() {
        let mut lf = make_lf();
        lf.archive();
        assert!(!lf.mark_missing());
        assert_eq!(lf.state(), LocationFileState::Archived);
    }

    // =========================================================================
    // State transitions — archive / unarchive
    // =========================================================================

    #[test]
    fn archive_from_active() {
        let mut lf = make_lf();
        lf.archive();
        assert_eq!(lf.state(), LocationFileState::Archived);
    }

    #[test]
    fn archive_idempotent() {
        let mut lf = make_lf();
        lf.archive();
        let ts = lf.updated_at();
        lf.archive();
        assert_eq!(lf.updated_at(), ts);
    }

    #[test]
    fn archive_from_syncing() {
        // ユーザー操作でSyncing中でもアーカイブ可能
        let mut lf = make_lf();
        lf.mark_stale();
        lf.mark_syncing().unwrap();
        lf.archive();
        assert_eq!(lf.state(), LocationFileState::Archived);
    }

    #[test]
    fn unarchive_from_archived() {
        let mut lf = make_lf();
        lf.archive();
        lf.unarchive().unwrap();
        assert_eq!(lf.state(), LocationFileState::Active);
    }

    #[test]
    fn unarchive_from_active_fails() {
        let mut lf = make_lf();
        assert!(lf.unarchive().is_err());
    }

    // =========================================================================
    // LocationFileState — query methods
    // =========================================================================

    #[test]
    fn active_is_source_eligible() {
        assert!(LocationFileState::Active.is_source_eligible());
        assert!(!LocationFileState::Stale.is_source_eligible());
        assert!(!LocationFileState::Syncing.is_source_eligible());
        assert!(!LocationFileState::Missing.is_source_eligible());
        assert!(!LocationFileState::Archived.is_source_eligible());
    }

    #[test]
    fn archived_not_distribute_target() {
        assert!(LocationFileState::Active.is_distribute_target());
        assert!(LocationFileState::Stale.is_distribute_target());
        assert!(LocationFileState::Syncing.is_distribute_target());
        assert!(LocationFileState::Missing.is_distribute_target());
        assert!(!LocationFileState::Archived.is_distribute_target());
    }

    #[test]
    fn is_present_reflects_physical_existence() {
        assert!(LocationFileState::Active.is_present());
        assert!(LocationFileState::Stale.is_present());
        assert!(LocationFileState::Archived.is_present());
        assert!(!LocationFileState::Missing.is_present());
        assert!(!LocationFileState::Syncing.is_present());
    }

    // =========================================================================
    // LocationFileState — str roundtrip
    // =========================================================================

    #[test]
    fn state_str_roundtrip() {
        for state in [
            LocationFileState::Active,
            LocationFileState::Syncing,
            LocationFileState::Stale,
            LocationFileState::Missing,
            LocationFileState::Archived,
        ] {
            let s = state.as_str();
            let parsed: LocationFileState = s.parse().unwrap();
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn state_display() {
        assert_eq!(LocationFileState::Active.to_string(), "active");
        assert_eq!(LocationFileState::Archived.to_string(), "archived");
    }

    #[test]
    fn state_invalid_str() {
        let result: Result<LocationFileState, _> = "invalid".parse();
        assert!(result.is_err());
    }

    // =========================================================================
    // 異アルゴリズムbyte_digest → フォールバック動作確認
    // =========================================================================

    #[test]
    fn cross_algo_byte_digest_falls_back_to_size() {
        // matches_within_location: Djb2 vs Sha256 → Err → sizeフォールバック
        // 同一sizeなので「同一」と判定（has_changed=false）
        let local_lf = make_lf(); // Djb2, size=1024
        let pod_fp = sha256_fp(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            1024,
        );
        assert!(
            !local_lf.has_changed(&pod_fp),
            "same size → fallback matches"
        );

        // sizeが異なれば「変更」と判定
        let pod_fp_diff = sha256_fp(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            2048,
        );
        assert!(
            local_lf.has_changed(&pod_fp_diff),
            "different size → changed"
        );
    }

    // =========================================================================
    // reconstitute
    // =========================================================================

    #[test]
    fn reconstitute_preserves_all() {
        let now = Utc::now();
        let lf = LocationFile::reconstitute(
            "f-1".into(),
            pod_loc(),
            "output/file.png".into(),
            sha256_fp("deadbeef", 512),
            LocationFileState::Stale,
            Some("emb".into()),
            now,
        );
        assert_eq!(lf.file_id(), "f-1");
        assert_eq!(lf.location_id(), &pod_loc());
        assert_eq!(lf.relative_path(), "output/file.png");
        assert_eq!(lf.state(), LocationFileState::Stale);
        assert_eq!(lf.updated_at(), now);
    }

    // =========================================================================
    // serde
    // =========================================================================

    #[test]
    fn serde_roundtrip() {
        let lf = LocationFile::new(
            "f1".into(),
            local_loc(),
            "output/gen.png".into(),
            djb2_fp("hash", 100),
            Some("emb".into()),
        )
        .unwrap();
        let json = serde_json::to_value(&lf).unwrap();
        let restored: LocationFile = serde_json::from_value(json).unwrap();
        assert_eq!(restored.file_id(), lf.file_id());
        assert_eq!(restored.location_id(), lf.location_id());
        assert_eq!(restored.relative_path(), lf.relative_path());
        assert_eq!(
            restored
                .fingerprint()
                .byte_digest
                .as_ref()
                .map(|d| d.as_str()),
            lf.fingerprint().byte_digest.as_ref().map(|d| d.as_str()),
        );
        assert_eq!(restored.state(), LocationFileState::Active);
    }

    #[test]
    fn serde_roundtrip_archived() {
        let mut lf = make_lf();
        lf.archive();
        let json = serde_json::to_value(&lf).unwrap();
        let restored: LocationFile = serde_json::from_value(json).unwrap();
        assert_eq!(restored.state(), LocationFileState::Archived);
    }
}
