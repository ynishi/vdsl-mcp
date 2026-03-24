//! SyncObserver — tracing::Span相当のトレーサビリティを提供するtrait。
//!
//! sync pipeline全体の入口/出口/進捗をコールバックで通知する。
//! 各メソッドにはデフォルト実装（no-op）があり、必要なイベントだけを
//! オーバーライドすれば良い。
//!
//! # 設計方針
//!
//! - tracing::Spanが使えない環境でSpan相当のコンテキスト伝搬を実現
//! - 全メソッドが `&self` で呼ばれる（`Send + Sync`）
//! - 各メソッドは処理をブロックしてはならない（ログ書き込み or Arc<Mutex> 更新のみ）
//! - フェーズ名・location・件数・経過時間を構造化して渡す

use crate::domain::location::LocationId;

/// scan_local_entriesの1ファイルhash完了時に渡す進捗。
#[derive(Debug, Clone)]
pub struct HashProgress {
    /// hash計算済みファイル数（cached reuse含まない）。
    pub hashed: usize,
    /// incremental scan でcacheから再利用したファイル数。
    pub cached: usize,
    /// このlocationの総ファイル数。
    pub total: usize,
}

/// compute_deltas の結果サマリ。
#[derive(Debug, Clone)]
pub struct DeltaSummary {
    pub added: usize,
    pub modified: usize,
    pub removed: usize,
}

/// execute_target / execute_all の1 transfer完了時に渡す進捗。
#[derive(Debug, Clone)]
pub struct TransferProgress {
    /// 転送元。
    pub src: LocationId,
    /// 転送先。
    pub dest: LocationId,
    /// 完了した転送数（成功+失敗）。
    pub completed: usize,
    /// このバッチの総転送数。
    pub total: usize,
    /// 最後に完了したファイルのrelative_path（成功時）。
    pub last_path: Option<String>,
}

/// recovery_executorの結果通知用。
#[derive(Debug, Clone)]
pub struct RecoveryProgress {
    pub retried: usize,
    pub resolved: usize,
    pub requeued: usize,
    pub skipped: usize,
    pub total: usize,
}

/// Sync pipeline全体のobserver trait。
///
/// tracing::Spanに相当するコンテキスト伝搬を提供する。
/// 全メソッドにデフォルト実装（no-op）があるため、必要なイベントだけ
/// オーバーライドすれば良い。
///
/// # スレッド安全性
///
/// `Send + Sync` を要求。内部状態の更新は `std::sync::Mutex` 等で行うこと。
/// async contextから呼ばれるが、メソッド自体はsync（tokio::Mutex不可）。
pub trait SyncObserver: Send + Sync {
    // =====================================================================
    // Phase 0: sync() entry
    // =====================================================================

    /// sync() 開始。reset_inflight完了後、scan開始前に呼ばれる。
    fn on_sync_start(&self, _reset_inflight: usize) {}

    // =====================================================================
    // Phase 1: Scan
    // =====================================================================

    /// scan_and_register() 開始。DB state fetch前に呼ばれる。
    fn on_scan_start(&self, _location_count: usize) {}

    /// DB state fetch完了。
    fn on_db_state_fetched(&self, _db_file_count: usize) {}

    /// 個別locationのscan開始。
    fn on_location_scan_start(&self, _location: &LocationId, _index: usize, _total: usize) {}

    /// 個別locationのファイルリスト取得完了。
    /// hash計算前（local）またはlsf完了（cloud）。
    fn on_location_listed(&self, _location: &LocationId, _file_count: usize) {}

    /// local scan: hash進捗（N件ごとに呼ばれる）。
    fn on_hash_progress(&self, _location: &LocationId, _progress: &HashProgress) {}

    /// 個別locationのscan完了。
    fn on_location_scan_done(&self, _location: &LocationId, _entries: usize, _errors: usize) {}

    /// 個別locationのscanが失敗。
    fn on_location_scan_failed(&self, _location: &LocationId, _error: &str) {}

    // =====================================================================
    // Phase 2: Diff
    // =====================================================================

    /// compute_deltas完了。
    fn on_diff_computed(&self, _summary: &DeltaSummary) {}

    // =====================================================================
    // Phase 2.5: mtime backfill
    // =====================================================================

    /// mtime backfill完了。
    fn on_mtime_backfill(&self, _backfilled: usize) {}

    // =====================================================================
    // Phase 3: Apply deltas
    // =====================================================================

    /// apply deltas開始。
    fn on_apply_start(&self, _delta_count: usize) {}

    /// apply deltas完了。
    fn on_apply_done(&self, _registered: usize, _errors: usize) {}

    // =====================================================================
    // Phase 4: Deletion detection
    // =====================================================================

    /// deletion detection開始。
    fn on_deletion_scan_start(&self, _db_path_count: usize) {}

    /// deletion detection完了。
    fn on_deletion_scan_done(&self, _deleted: usize) {}

    // =====================================================================
    // Phase 5: Recovery
    // =====================================================================

    /// recovery開始。
    fn on_recovery_start(&self, _failed_count: usize) {}

    /// recovery 1件処理完了。
    fn on_recovery_progress(
        &self,
        _processed: usize,
        _total: usize,
        _src: &LocationId,
        _dest: &LocationId,
        _action: &str,
        _file_path: &str,
    ) {
    }

    /// recovery完了。
    fn on_recovery_done(&self, _progress: &RecoveryProgress) {}

    // =====================================================================
    // Phase 6: Transfer execution
    // =====================================================================

    /// execute_all開始。
    fn on_transfer_start(&self, _queued: usize, _target_count: usize) {}

    /// 個別dest宛の転送バッチ開始。srcsは同一destへ転送する全src一覧。
    fn on_target_start(&self, _srcs: &[LocationId], _dest: &LocationId, _queued: usize) {}

    /// 個別transfer完了（成功 or 失敗、N件ごと）。
    fn on_transfer_progress(&self, _progress: &TransferProgress) {}

    /// 個別dest宛の転送バッチ完了。
    fn on_target_done(
        &self,
        _srcs: &[LocationId],
        _dest: &LocationId,
        _total: usize,
        _transferred: usize,
        _failed: usize,
    ) {
    }

    /// execute_all完了。
    fn on_transfer_done(&self, _transferred: usize, _failed: usize) {}

    // =====================================================================
    // Phase 7: sync() 完了
    // =====================================================================

    /// sync() 完了。
    fn on_sync_done(&self) {}

    /// sync() 失敗。
    fn on_sync_failed(&self, _error: &str) {}
}

/// No-op observer。テストや progress 不要な場面で使用。
pub struct NullObserver;

impl SyncObserver for NullObserver {}

/// Format multiple srcs for display: "cloud+local" or single "cloud".
fn format_srcs(srcs: &[LocationId]) -> String {
    if srcs.is_empty() {
        "?".to_string()
    } else if srcs.len() == 1 {
        srcs[0].to_string()
    } else {
        srcs.iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("+")
    }
}

/// ProgressFn互換のブリッジobserver。
///
/// 既存の `ProgressFn`（`Arc<dyn Fn(String)>`）を `SyncObserver` に変換する。
/// 遷移期間の後方互換用。各イベントを人間可読な文字列に変換して `ProgressFn` に委譲する。
pub struct ProgressFnBridge {
    f: std::sync::Arc<dyn Fn(String) + Send + Sync>,
}

impl ProgressFnBridge {
    pub fn new(f: std::sync::Arc<dyn Fn(String) + Send + Sync>) -> Self {
        Self { f }
    }
}

impl SyncObserver for ProgressFnBridge {
    fn on_sync_start(&self, reset_inflight: usize) {
        if reset_inflight > 0 {
            (self.f)(format!("reset {} orphaned transfers", reset_inflight));
        }
    }

    fn on_scan_start(&self, location_count: usize) {
        (self.f)(format!("scan: starting ({} locations)", location_count));
    }

    fn on_db_state_fetched(&self, db_file_count: usize) {
        (self.f)(format!("scan: loaded {} files from DB", db_file_count));
    }

    fn on_location_scan_start(&self, location: &LocationId, index: usize, total: usize) {
        (self.f)(format!(
            "scan: {} ({}/{}) listing files...",
            location,
            index + 1,
            total,
        ));
    }

    fn on_location_listed(&self, location: &LocationId, file_count: usize) {
        (self.f)(format!("scan: {} listed {} files", location, file_count,));
    }

    fn on_hash_progress(&self, location: &LocationId, progress: &HashProgress) {
        (self.f)(format!(
            "scan: {} hashing {}/{} ({} cached)",
            location, progress.hashed, progress.total, progress.cached,
        ));
    }

    fn on_location_scan_done(&self, location: &LocationId, entries: usize, errors: usize) {
        if errors > 0 {
            (self.f)(format!(
                "scan: {} done ({} entries, {} errors)",
                location, entries, errors,
            ));
        }
    }

    fn on_location_scan_failed(&self, location: &LocationId, error: &str) {
        (self.f)(format!("scan: {} FAILED: {}", location, error));
    }

    fn on_diff_computed(&self, summary: &DeltaSummary) {
        (self.f)(format!(
            "diff: +{} added, ~{} modified, -{} removed",
            summary.added, summary.modified, summary.removed,
        ));
    }

    fn on_apply_start(&self, delta_count: usize) {
        (self.f)(format!("apply: processing {} deltas...", delta_count));
    }

    fn on_apply_done(&self, registered: usize, errors: usize) {
        (self.f)(format!(
            "apply: {} registered, {} errors",
            registered, errors,
        ));
    }

    fn on_deletion_scan_start(&self, db_path_count: usize) {
        (self.f)(format!(
            "delete: checking {} DB paths against filesystem...",
            db_path_count,
        ));
    }

    fn on_deletion_scan_done(&self, deleted: usize) {
        if deleted > 0 {
            (self.f)(format!("delete: {} files removed", deleted));
        }
    }

    fn on_recovery_start(&self, failed_count: usize) {
        (self.f)(format!(
            "recovery: {failed_count} failed transfers to process"
        ));
    }

    fn on_recovery_progress(
        &self,
        processed: usize,
        total: usize,
        src: &LocationId,
        dest: &LocationId,
        action: &str,
        file_path: &str,
    ) {
        (self.f)(format!(
            "recovery: {src}→{dest} {processed}/{total} {action} ({file_path})"
        ));
    }

    fn on_recovery_done(&self, progress: &RecoveryProgress) {
        (self.f)(format!(
            "recovery: done {}/{} (retried:{} resolved:{} requeued:{} skipped:{})",
            progress.retried + progress.resolved + progress.requeued + progress.skipped,
            progress.total,
            progress.retried,
            progress.resolved,
            progress.requeued,
            progress.skipped,
        ));
    }

    fn on_transfer_start(&self, queued: usize, target_count: usize) {
        (self.f)(format!(
            "transfer: {} queued across {} destinations",
            queued, target_count,
        ));
    }

    fn on_target_start(&self, srcs: &[LocationId], dest: &LocationId, queued: usize) {
        let src_label = format_srcs(srcs);
        (self.f)(format!(
            "transfer: {src_label}→{dest} executing {queued} files (progress: N/A — batch)",
        ));
    }

    fn on_transfer_progress(&self, progress: &TransferProgress) {
        let path_info = progress.last_path.as_deref().unwrap_or("...");
        (self.f)(format!(
            "transfer: {}→{} {}/{} ({})",
            progress.src, progress.dest, progress.completed, progress.total, path_info,
        ));
    }

    fn on_target_done(
        &self,
        srcs: &[LocationId],
        dest: &LocationId,
        total: usize,
        transferred: usize,
        failed: usize,
    ) {
        let src_label = format_srcs(srcs);
        (self.f)(format!(
            "transfer: {src_label}→{dest} done {transferred}/{total} ok, {failed} failed",
        ));
    }

    fn on_transfer_done(&self, transferred: usize, failed: usize) {
        (self.f)(format!(
            "transfer: all done ({} ok, {} failed)",
            transferred, failed,
        ));
    }
}
