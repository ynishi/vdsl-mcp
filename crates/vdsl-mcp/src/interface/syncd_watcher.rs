//! FSEvents watcher — notify + notify-debouncer-full によるローカルファイル監視。
//!
//! # 概要
//!
//! work_dir 以下のファイル変更を監視し、debounce 後の差分を SDK に反映する。
//! 反映後に coalesced auto sync を発動して全拠点へ transfer を伝播する。
//!
//! # exclude フィルタ
//!
//! `LocalScanner` と同一のフィルタを適用する:
//! 1. `glob::Pattern` による excludes パターンマッチ (work_dir 相対パス)
//! 2. basename が `.` 開始のファイルをハードコードで除外 (hidden file)
//!
//! # Rename ハンドリング
//!
//! `notify-debouncer-full 0.6` は Rename イベントを以下に分類する:
//! - `RenameMode::Both` → paths[0] を delete、paths[1] を upsert
//! - `RenameMode::From` → delete (To が debounce 内に来なかった場合)
//! - `RenameMode::To`   → upsert

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use notify::event::{ModifyKind, RenameMode};
use notify::{EventKind, RecursiveMode};
use notify_debouncer_full::{new_debouncer, DebouncedEvent, Debouncer, FileIdMap};
use tracing::{error, info, warn};

use crate::interface::syncd_http::SyncdState;

// =============================================================================
// WatcherHandle
// =============================================================================

/// watcher の生存を管理する RAII ハンドル。
///
/// `Drop` 時に `Debouncer` が停止し、notify スレッドが終了する。
/// `syncd.rs::run()` のローカル変数として保持し、shutdown 時に `drop` することで
/// graceful stop を実現する。
pub struct WatcherHandle {
    /// Debouncer を保持することで notify watcher の生存を管理する。
    /// Drop で notify スレッドが停止する。
    _debouncer: Debouncer<notify::RecommendedWatcher, FileIdMap>,
}

// =============================================================================
// spawn_watcher
// =============================================================================

/// watcher を起動し、`WatcherHandle` を返す。
///
/// # Parameters
///
/// - `work_dir`: 監視するルートディレクトリ (絶対パス)
/// - `debounce`: デバウンス時間
/// - `excludes`: 除外 glob パターン (`LocalScanner` と同じ形式)
/// - `state`: syncd 共有状態 (`Arc<SyncdState>`)
///
/// # Errors
///
/// - `work_dir` が存在しない場合
/// - `local_root` が `None` の場合 (watcher は local location 前提)
/// - notify の初期化に失敗した場合
pub fn spawn_watcher(
    work_dir: PathBuf,
    debounce: Duration,
    excludes: Vec<glob::Pattern>,
    state: Arc<SyncdState>,
) -> anyhow::Result<WatcherHandle> {
    // local_root が None なら watcher 起動不可
    let local_root = state
        .sdk
        .local_root()
        .ok_or_else(|| anyhow::anyhow!("syncd: local_root is None — cannot start watcher"))?
        .to_path_buf();

    info!(
        work_dir = %work_dir.display(),
        local_root = %local_root.display(),
        debounce_ms = debounce.as_millis(),
        excludes = excludes.len(),
        "syncd_watcher: starting"
    );

    // channel: debounced event バッチを tokio task に送る
    // capacity 64: 大量イベント時のバックプレッシャー
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<DebouncedEvent>>(64);

    // notify-debouncer-full 0.6 のコールバックは同期コンテキストで呼ばれる。
    // tokio async を直接呼ぶと deadlock リスクがあるため、blocking_send で channel に流す。
    let tx_for_cb = tx.clone();
    let mut debouncer = new_debouncer(debounce, None, move |result| match result {
        Ok(events) => {
            if let Err(e) = tx_for_cb.blocking_send(events) {
                warn!(error = %e, "syncd_watcher: event channel send failed (receiver dropped?)");
            }
        }
        Err(errors) => {
            for e in &errors {
                warn!(error = ?e, "syncd_watcher: notify error");
            }
        }
    })
    .map_err(|e| anyhow::anyhow!("syncd_watcher: failed to create debouncer: {e}"))?;

    debouncer
        .watch(&work_dir, RecursiveMode::Recursive)
        .map_err(|e| {
            anyhow::anyhow!("syncd_watcher: failed to watch {}: {e}", work_dir.display())
        })?;

    // 処理ループを tokio task に spawn
    tokio::spawn(async move {
        info!("syncd_watcher: event loop started");
        while let Some(events) = rx.recv().await {
            match apply_events(&state, &excludes, &local_root, events).await {
                Ok(applied) => {
                    if applied > 0 {
                        trigger_auto_sync(&state).await;
                    }
                }
                Err(e) => {
                    error!(error = ?e, "syncd_watcher: apply_events failed");
                }
            }
        }
        info!("syncd_watcher: event loop stopped");
    });

    Ok(WatcherHandle {
        _debouncer: debouncer,
    })
}

// =============================================================================
// apply_events
// =============================================================================

/// デバウンス済みイベントを SDK に反映する。
///
/// `LocalScanner` と同じ exclude フィルタを適用してから `sdk.put` / `sdk.delete` を呼ぶ。
async fn apply_events(
    state: &Arc<SyncdState>,
    excludes: &[glob::Pattern],
    local_root: &Path,
    events: Vec<DebouncedEvent>,
) -> anyhow::Result<usize> {
    let mut applied = 0usize;
    for ev in events {
        let ops = classify(&ev);
        for op in ops {
            match op {
                FileOp::Upsert(path) => {
                    if is_excluded(&path, local_root, excludes) {
                        continue;
                    }
                    if !path.exists() {
                        // ファイルが既に消えている場合はスキップ (競合状態)
                        continue;
                    }
                    let rel = match path.strip_prefix(local_root) {
                        Ok(r) => r.to_string_lossy().to_string(),
                        Err(_) => {
                            warn!(path = %path.display(), "syncd_watcher: path not under local_root, skipping");
                            continue;
                        }
                    };
                    match compute_fingerprint(&path).await {
                        Ok((fp, ft)) => {
                            let origin = match state.sdk.local_root() {
                                Some(root) => {
                                    let root_str = root.to_string_lossy().into_owned();
                                    vdsl_sync::LocationId::new(root_str).unwrap_or_else(|_| {
                                        vdsl_sync::LocationId::new("local").expect("static str")
                                    })
                                }
                                None => {
                                    warn!("syncd_watcher: local_root gone during upsert, skipping");
                                    continue;
                                }
                            };
                            if let Err(e) = state.sdk.put(&rel, ft, fp, &origin, None).await {
                                warn!(path = %path.display(), error = ?e, "syncd_watcher: sdk.put failed");
                            } else {
                                applied += 1;
                            }
                        }
                        Err(e) => {
                            warn!(path = %path.display(), error = ?e, "syncd_watcher: compute_fingerprint failed");
                        }
                    }
                }
                FileOp::Delete(path) => {
                    if is_excluded(&path, local_root, excludes) {
                        continue;
                    }
                    let rel = match path.strip_prefix(local_root) {
                        Ok(r) => r.to_string_lossy().to_string(),
                        Err(_) => {
                            warn!(path = %path.display(), "syncd_watcher: path not under local_root, skipping");
                            continue;
                        }
                    };
                    if let Err(e) = state.sdk.delete(&rel).await {
                        warn!(path = %path.display(), error = ?e, "syncd_watcher: sdk.delete failed");
                    } else {
                        applied += 1;
                    }
                }
            }
        }
    }
    Ok(applied)
}

// =============================================================================
// イベント分類
// =============================================================================

/// apply_events 内での個別操作。
enum FileOp {
    Upsert(PathBuf),
    Delete(PathBuf),
}

/// `DebouncedEvent` を `Vec<FileOp>` に変換する。
///
/// Rename の From/To/Both を適切に分解する。
fn classify(ev: &DebouncedEvent) -> Vec<FileOp> {
    match &ev.event.kind {
        EventKind::Create(_) | EventKind::Modify(ModifyKind::Data(_)) => ev
            .event
            .paths
            .iter()
            .map(|p| FileOp::Upsert(p.clone()))
            .collect(),

        EventKind::Modify(ModifyKind::Name(mode)) => match mode {
            RenameMode::Both => {
                // paths[0] = from (削除), paths[1] = to (作成)
                let mut ops = Vec::new();
                if let Some(from) = ev.event.paths.first() {
                    ops.push(FileOp::Delete(from.clone()));
                }
                if let Some(to) = ev.event.paths.get(1) {
                    ops.push(FileOp::Upsert(to.clone()));
                }
                ops
            }
            RenameMode::From => ev
                .event
                .paths
                .iter()
                .map(|p| FileOp::Delete(p.clone()))
                .collect(),
            RenameMode::To | RenameMode::Any => ev
                .event
                .paths
                .iter()
                .map(|p| FileOp::Upsert(p.clone()))
                .collect(),
            RenameMode::Other => vec![],
        },

        EventKind::Remove(_) => ev
            .event
            .paths
            .iter()
            .map(|p| FileOp::Delete(p.clone()))
            .collect(),

        // Access / Other / Any / Modify(Metadata) / Modify(Other) はスキップ
        EventKind::Access(_)
        | EventKind::Other
        | EventKind::Any
        | EventKind::Modify(ModifyKind::Metadata(_))
        | EventKind::Modify(ModifyKind::Other)
        | EventKind::Modify(ModifyKind::Any) => vec![],
    }
}

// =============================================================================
// exclude フィルタ
// =============================================================================

/// ファイルパスが除外対象かを判定する。
///
/// `LocalScanner` と同一の 2 段階フィルタを適用:
/// 1. basename が `.` 開始 (hidden file)
/// 2. `local_root` 相対パスが `excludes` パターンにマッチ
///
/// `path` は絶対パスを前提とする (notify 8.x の `Event::paths` は常に絶対パス)。
pub fn is_excluded(path: &Path, local_root: &Path, excludes: &[glob::Pattern]) -> bool {
    // 1. hidden file チェック (basename が `.` 開始)
    if path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with('.'))
        .unwrap_or(false)
    {
        return true;
    }

    // 2. glob パターンマッチ (local_root 相対パス文字列)
    let rel = match path.strip_prefix(local_root) {
        Ok(r) => r.to_string_lossy().to_string(),
        Err(_) => return false,
    };

    excludes.iter().any(|p| p.matches(&rel))
}

// =============================================================================
// fingerprint 計算
// =============================================================================

/// ローカルファイルのフィンガープリントを計算する。
///
/// `Djb2Hasher` を使って file_hash / content_hash を取得し、
/// ファイルサイズと mtime と合わせて `FileFingerprint` を構築する。
///
/// blocking I/O は `tokio::task::spawn_blocking` でオフロードする。
pub async fn compute_fingerprint(
    abs_path: &Path,
) -> anyhow::Result<(vdsl_sync::FileFingerprint, vdsl_sync::FileType)> {
    let path = abs_path.to_path_buf();
    let ext = abs_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_string();

    let (hash_result, file_size) =
        tokio::task::spawn_blocking(move || -> anyhow::Result<(vdsl_sync::HashResult, u64)> {
            let hasher = vdsl_sync::Djb2Hasher;
            let hr =
                vdsl_sync::ContentHasher::hash_file(&hasher, &path).map_err(anyhow::Error::from)?;
            let size = std::fs::metadata(&path).map_err(anyhow::Error::from)?.len();
            Ok((hr, size))
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking panicked: {e}"))??;

    let file_type = vdsl_sync::FileType::from_extension(&ext);

    let mtime = tokio::fs::metadata(abs_path)
        .await
        .ok()
        .and_then(|m| m.modified().ok())
        .map(chrono::DateTime::<chrono::Utc>::from);

    let fingerprint = vdsl_sync::FileFingerprint::from_local_hash(
        hash_result.file_hash,
        hash_result.content_hash,
        file_size,
        mtime,
    );

    Ok((fingerprint, file_type))
}

// =============================================================================
// auto sync trigger (coalesce)
// =============================================================================

/// 差分反映後に auto sync を 1 バッチにつき 1 回に coalesced して発動する。
///
/// - `auto_sync_running` が `true` の間は `auto_sync_pending` フラグのみ立てて return
/// - `false` なら `running=true` にして `spawn_sync` を実行
/// - sync 完了後に `pending` が立っていれば追加 run
pub async fn trigger_auto_sync(state: &Arc<SyncdState>) {
    // 既に running なら pending フラグだけ立てて return (coalesce)
    if state
        .auto_sync_running
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        state.auto_sync_pending.store(true, Ordering::SeqCst);
        return;
    }

    let state2 = Arc::clone(state);
    tokio::spawn(async move {
        loop {
            match state2.task_mgr.spawn_sync(&state2.sdk).await {
                Ok(task_id) => {
                    info!(task_id = %task_id, "syncd_watcher: auto sync spawned");
                }
                Err(e) => {
                    // spawn_sync は先行 sync が running の場合 busy エラーを返す。
                    // ここで tight loop を避けるため一定時間待機してから次の coalesce チェックへ。
                    // pending は立てず、loop を抜けて running=false にする。
                    // 次の watcher event で再 trigger される。
                    warn!(error = %e, "syncd_watcher: auto sync busy, skip (next watcher event retries)");
                    break;
                }
            }

            // pending が再度立っていたら追加 run (coalesce によるキューを消化)
            if !state2.auto_sync_pending.swap(false, Ordering::SeqCst) {
                break;
            }
            // 立て続けに spawn_sync を叩くと task_mgr 側の状態遷移と競合するので小休止。
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        state2.auto_sync_running.store(false, Ordering::SeqCst);
    });
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
    use notify::{Event, EventKind};
    use notify_debouncer_full::DebouncedEvent;

    fn make_event(kind: EventKind, paths: Vec<PathBuf>) -> DebouncedEvent {
        DebouncedEvent {
            event: Event {
                kind,
                paths,
                attrs: Default::default(),
            },
            time: std::time::Instant::now(),
        }
    }

    // =========================================================================
    // classify
    // =========================================================================

    #[test]
    fn classify_create_is_upsert() {
        let path = PathBuf::from("/tmp/test/file.txt");
        let ev = make_event(EventKind::Create(CreateKind::File), vec![path.clone()]);
        let ops = classify(&ev);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], FileOp::Upsert(p) if p == &path));
    }

    #[test]
    fn classify_remove_is_delete() {
        let path = PathBuf::from("/tmp/test/file.txt");
        let ev = make_event(EventKind::Remove(RemoveKind::File), vec![path.clone()]);
        let ops = classify(&ev);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], FileOp::Delete(p) if p == &path));
    }

    #[test]
    fn classify_rename_both_is_delete_then_upsert() {
        let from = PathBuf::from("/tmp/test/old.txt");
        let to = PathBuf::from("/tmp/test/new.txt");
        let ev = make_event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            vec![from.clone(), to.clone()],
        );
        let ops = classify(&ev);
        assert_eq!(ops.len(), 2);
        assert!(matches!(&ops[0], FileOp::Delete(p) if p == &from));
        assert!(matches!(&ops[1], FileOp::Upsert(p) if p == &to));
    }

    #[test]
    fn classify_rename_from_is_delete() {
        let from = PathBuf::from("/tmp/test/old.txt");
        let ev = make_event(
            EventKind::Modify(ModifyKind::Name(RenameMode::From)),
            vec![from.clone()],
        );
        let ops = classify(&ev);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], FileOp::Delete(p) if p == &from));
    }

    #[test]
    fn classify_rename_to_is_upsert() {
        let to = PathBuf::from("/tmp/test/new.txt");
        let ev = make_event(
            EventKind::Modify(ModifyKind::Name(RenameMode::To)),
            vec![to.clone()],
        );
        let ops = classify(&ev);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], FileOp::Upsert(p) if p == &to));
    }

    #[test]
    fn classify_access_is_ignored() {
        let path = PathBuf::from("/tmp/test/file.txt");
        let ev = make_event(
            EventKind::Access(notify::event::AccessKind::Read),
            vec![path],
        );
        let ops = classify(&ev);
        assert!(ops.is_empty());
    }

    // =========================================================================
    // is_excluded
    // =========================================================================

    #[test]
    fn hidden_file_excluded() {
        let root = PathBuf::from("/work");
        let path = PathBuf::from("/work/images/.hidden.png");
        assert!(is_excluded(&path, &root, &[]));
    }

    #[test]
    fn hidden_dir_entry_not_excluded_by_basename_alone() {
        // `.git/config` の basename は `config` (`.` で始まらない) なので
        // basename チェックでは除外されない。`.git/**` の glob パターンで除外する。
        let root = PathBuf::from("/work");
        let path = PathBuf::from("/work/.git/config");
        assert!(!is_excluded(&path, &root, &[]));
    }

    #[test]
    fn hidden_dir_entry_excluded_by_glob_pattern() {
        let root = PathBuf::from("/work");
        let path = PathBuf::from("/work/.git/config");
        let pattern = glob::Pattern::new(".git/**").unwrap();
        assert!(is_excluded(&path, &root, &[pattern]));
    }

    #[test]
    fn normal_file_not_excluded() {
        let root = PathBuf::from("/work");
        let path = PathBuf::from("/work/images/photo.png");
        assert!(!is_excluded(&path, &root, &[]));
    }

    #[test]
    fn glob_pattern_excluded() {
        let root = PathBuf::from("/work");
        let path = PathBuf::from("/work/images/foo.partial");
        let pattern = glob::Pattern::new("**/*.partial").unwrap();
        assert!(is_excluded(&path, &root, &[pattern]));
    }

    #[test]
    fn glob_pattern_not_matching_not_excluded() {
        let root = PathBuf::from("/work");
        let path = PathBuf::from("/work/images/photo.png");
        let pattern = glob::Pattern::new("**/*.partial").unwrap();
        assert!(!is_excluded(&path, &root, &[pattern]));
    }

    #[test]
    fn path_outside_root_not_excluded() {
        let root = PathBuf::from("/work");
        let path = PathBuf::from("/other/file.txt");
        assert!(!is_excluded(&path, &root, &[]));
    }

    // =========================================================================
    // compute_fingerprint
    // =========================================================================

    #[tokio::test]
    async fn compute_fingerprint_returns_valid_data() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.txt");
        std::fs::write(&file, b"hello world").unwrap();

        let (fp, ft) = compute_fingerprint(&file).await.unwrap();
        assert_eq!(fp.size, 11);
        assert!(fp.byte_digest.is_some());
        assert!(matches!(ft, vdsl_sync::FileType::Asset));
    }

    #[tokio::test]
    async fn compute_fingerprint_nonexistent_file_returns_error() {
        let path = PathBuf::from("/nonexistent/path/file.txt");
        let result = compute_fingerprint(&path).await;
        assert!(result.is_err());
    }
}
