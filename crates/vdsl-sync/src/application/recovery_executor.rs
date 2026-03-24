//! RecoveryExecutor — Failed Transfer の復帰実行。
//!
//! ドメイン層の [`RecoveryStrategy`] が「何をするか」を決定し、
//! このモジュールが「どう実行するか」を担当する。
//!
//! Store から分離することで:
//! - sync / force_full_rewrite で戦略を差し替え可能
//! - テストでstrategyをモックして実行パスを検証可能
//! - 判定ロジック（domain）と実行（application）が明確に分離
//!
//! # retry_failed() 統合
//!
//! 旧 `Store::retry_failed()` のロジックは `RecoveryAction::Retry` として
//! このモジュールに統合されている。retryable判定はドメイン層の
//! `RecoveryStrategy::decide()` が行い、実行はここで `transfer.retry()` を呼ぶ。

use crate::application::error::SyncError;
use crate::application::observer::{RecoveryProgress, SyncObserver};
use crate::domain::recovery::{FailedContext, RecoveryAction, RecoveryStrategy};
use crate::domain::retry::RetryPolicy;
use crate::domain::transfer::{Transfer, TransferKind};
use crate::infra::file_store::FileStore;
use crate::infra::transfer_store::TransferStore;

use super::transfer_engine::TransferEngine;

/// Recovery実行の結果。
#[derive(Debug, Default)]
pub struct RecoveryResult {
    /// retry() で attempt+1 の新Transfer を生成した数。
    pub retried: usize,
    /// resolve() で Completed に遷移した数。
    pub resolved: usize,
    /// 新規 Transfer(attempt=1) を生成して再キューした数。
    pub requeued: usize,
    /// skip した数。
    pub skipped: usize,
}

/// Failed Transfer の復帰を実行する。
///
/// 1. failed_transfers() で全Failed Transferを取得
/// 2. retryable判定はstrategyに委譲（Retry / dest存在チェック不要で即判定）
/// 3. exhaustedなものはdest存在チェック後にstrategyで判定
/// 4. RecoveryAction に基づいて実行
#[allow(dead_code)] // Used by tests
pub async fn execute_recovery(
    strategy: &dyn RecoveryStrategy,
    policy: &RetryPolicy,
    engine: &TransferEngine,
    file_store: &dyn FileStore,
    transfer_store: &dyn TransferStore,
) -> Result<RecoveryResult, SyncError> {
    let failed = transfer_store.failed_transfers().await?;
    let mut result = RecoveryResult::default();

    for t in failed {
        // retryable はdest存在チェック不要 — strategyがRetryを返すかで判定
        // dest_exists = None でcontextを作り、strategyがRetryを返せば即実行
        let needs_dest_check = !t.is_retryable(policy);

        let dest_exists = if needs_dest_check {
            check_dest_exists(engine, file_store, &t).await
        } else {
            None // retryableならdest_existsは不要
        };

        let ctx = FailedContext::from_transfer(&t, dest_exists, policy);
        let action = strategy.decide(&ctx);

        match action {
            RecoveryAction::Retry => {
                let new_transfer = t.retry()?;
                transfer_store.insert_transfer(&new_transfer).await?;
                result.retried += 1;
            }
            RecoveryAction::Resolve => {
                let mut transfer = t;
                transfer.resolve()?;
                transfer_store.update_transfer(&transfer).await?;
                log_recovery("resolved", &transfer, dest_exists);
                result.resolved += 1;
            }
            RecoveryAction::Requeue => {
                requeue_transfer(engine, transfer_store, &t).await?;
                log_recovery("requeued", &t, dest_exists);
                result.requeued += 1;
            }
            RecoveryAction::Skip => {
                result.skipped += 1;
            }
        }
    }

    Ok(result)
}

/// Observer付きrecovery実行。
pub async fn execute_recovery_with_observer(
    strategy: &dyn RecoveryStrategy,
    policy: &RetryPolicy,
    engine: &TransferEngine,
    file_store: &dyn FileStore,
    transfer_store: &dyn TransferStore,
    observer: &dyn SyncObserver,
) -> Result<RecoveryResult, SyncError> {
    let failed = transfer_store.failed_transfers().await?;
    let total = failed.len();
    observer.on_recovery_start(total);

    let mut result = RecoveryResult::default();
    let mut processed: usize = 0;

    for t in failed {
        let needs_dest_check = !t.is_retryable(policy);

        let dest_exists = if needs_dest_check {
            check_dest_exists(engine, file_store, &t).await
        } else {
            None
        };

        let ctx = FailedContext::from_transfer(&t, dest_exists, policy);
        let action = strategy.decide(&ctx);

        let action_str = match &action {
            RecoveryAction::Retry => "retry",
            RecoveryAction::Resolve => "resolve",
            RecoveryAction::Requeue => "requeue",
            RecoveryAction::Skip => "skip",
        };

        // file_storeからrelative_pathを取得（取得失敗時はfile_idをフォールバック）
        let file_path = match file_store.get_file_by_id(t.file_id()).await {
            Ok(Some(f)) => f.relative_path().to_string(),
            _ => t.file_id().to_string(),
        };

        processed += 1;
        observer.on_recovery_progress(processed, total, t.src(), t.dest(), action_str, &file_path);

        match action {
            RecoveryAction::Retry => {
                let new_transfer = t.retry()?;
                transfer_store.insert_transfer(&new_transfer).await?;
                result.retried += 1;
            }
            RecoveryAction::Resolve => {
                let mut transfer = t;
                transfer.resolve()?;
                transfer_store.update_transfer(&transfer).await?;
                log_recovery("resolved", &transfer, dest_exists);
                result.resolved += 1;
            }
            RecoveryAction::Requeue => {
                requeue_transfer(engine, transfer_store, &t).await?;
                log_recovery("requeued", &t, dest_exists);
                result.requeued += 1;
            }
            RecoveryAction::Skip => {
                result.skipped += 1;
            }
        }
    }

    observer.on_recovery_done(&RecoveryProgress {
        retried: result.retried,
        resolved: result.resolved,
        requeued: result.requeued,
        skipped: result.skipped,
        total,
    });

    Ok(result)
}

/// dest側のファイル存在チェック。
///
/// route が見つからない / file が見つからない / チェック失敗 → None。
async fn check_dest_exists(
    engine: &TransferEngine,
    file_store: &dyn FileStore,
    transfer: &Transfer,
) -> Option<bool> {
    let route = engine.find_route(transfer.src(), transfer.dest())?;
    let file = file_store
        .get_file_by_id(transfer.file_id())
        .await
        .ok()
        .flatten()?;
    route.dest_file_exists(file.relative_path()).await.ok()
}

/// Failed Transfer を新しいQueued Transferとして再キュー。
///
/// attempt=1 の新Transferを生成（retry()ではなく完全リセット）。
/// Delete の場合は new_delete()、Sync の場合は new()。
async fn requeue_transfer(
    engine: &TransferEngine,
    transfer_store: &dyn TransferStore,
    failed: &Transfer,
) -> Result<(), SyncError> {
    if engine.find_route(failed.src(), failed.dest()).is_none() {
        tracing::warn!(
            transfer_id = %failed.id(),
            src = %failed.src(),
            dest = %failed.dest(),
            "skipping requeue: route no longer exists"
        );
        return Ok(());
    }

    let new_transfer = match failed.kind() {
        TransferKind::Delete => Transfer::new_delete(
            failed.file_id().to_string(),
            failed.src().clone(),
            failed.dest().clone(),
        )?,
        TransferKind::Sync => Transfer::new(
            failed.file_id().to_string(),
            failed.src().clone(),
            failed.dest().clone(),
        )?,
    };
    transfer_store.insert_transfer(&new_transfer).await?;
    Ok(())
}

fn log_recovery(action: &str, transfer: &Transfer, dest_exists: Option<bool>) {
    tracing::info!(
        action = action,
        transfer_id = %transfer.id(),
        file_id = transfer.file_id(),
        kind = %transfer.kind(),
        src = %transfer.src(),
        dest = %transfer.dest(),
        dest_exists = ?dest_exists,
        "recovery executor: {action}"
    );
}

#[cfg(test)]
#[cfg(feature = "sqlite")]
mod tests {
    use super::*;
    use crate::application::route::TransferRoute;
    use crate::domain::location::LocationId;
    use crate::domain::recovery::{DefaultRecovery, ForceRecovery};
    use crate::domain::retry::{RetryPolicy, TransferErrorKind};
    use crate::domain::tracked_file::TrackedFile;
    use crate::domain::transfer::TransferState;
    use crate::infra::backend::memory::InMemoryBackend;
    use crate::infra::file_store::FileStore;
    use crate::infra::sqlite::SqliteSyncStore;
    use crate::infra::transfer_store::TransferStore;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn loc(s: &str) -> LocationId {
        LocationId::new(s).unwrap()
    }

    /// Build TransferEngine + stores + backend for tests.
    /// Route: local → cloud (push direction).
    async fn test_setup(
        dir: &std::path::Path,
    ) -> (TransferEngine, Arc<SqliteSyncStore>, Arc<InMemoryBackend>) {
        let store = Arc::new(SqliteSyncStore::open_in_memory().await.unwrap());
        let backend = Arc::new(InMemoryBackend::default());

        let routes = vec![TransferRoute::new(
            LocationId::local(),
            loc("cloud"),
            dir.to_path_buf(),
            PathBuf::from("remote/output"),
            Box::new(Arc::clone(&backend)),
        )];

        let engine = TransferEngine::new(routes, 8);
        (engine, store, backend)
    }

    /// Register a TrackedFile in the store. Returns the file_id.
    async fn seed_file(store: &dyn FileStore, relative_path: &str) -> String {
        let file = TrackedFile::from_scan(
            relative_path.to_string(),
            crate::domain::file_type::FileType::Image,
            "hash123".to_string(),
            None,
            1024,
            None,
        )
        .unwrap();
        let id = file.id().to_string();
        store.upsert_file(&file).await.unwrap();
        id
    }

    /// Insert a Failed transfer (Delete kind) into the store.
    async fn seed_failed_delete(
        store: &dyn TransferStore,
        file_id: &str,
        attempt: u32,
        error_kind: TransferErrorKind,
    ) -> String {
        let t = Transfer::reconstitute(
            uuid::Uuid::new_v4().to_string(),
            file_id.to_string(),
            LocationId::local(),
            loc("cloud"),
            TransferKind::Delete,
            TransferState::Failed,
            Some("mock error".into()),
            Some(error_kind),
            attempt,
            chrono::Utc::now(),
            Some(chrono::Utc::now()),
            Some(chrono::Utc::now()),
        );
        let id = t.id().to_string();
        store.insert_transfer(&t).await.unwrap();
        id
    }

    /// Insert a Failed transfer (Sync kind) into the store.
    async fn seed_failed_sync(
        store: &dyn TransferStore,
        file_id: &str,
        attempt: u32,
        error_kind: TransferErrorKind,
    ) -> String {
        let t = Transfer::reconstitute(
            uuid::Uuid::new_v4().to_string(),
            file_id.to_string(),
            LocationId::local(),
            loc("cloud"),
            TransferKind::Sync,
            TransferState::Failed,
            Some("mock error".into()),
            Some(error_kind),
            attempt,
            chrono::Utc::now(),
            Some(chrono::Utc::now()),
            Some(chrono::Utc::now()),
        );
        let id = t.id().to_string();
        store.insert_transfer(&t).await.unwrap();
        id
    }

    // =======================================================================
    // Scenario 1: retryable → Retry (attempt+1 new Transfer)
    // =======================================================================

    #[tokio::test]
    async fn retryable_transient_produces_retry() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, _backend) = test_setup(dir.path()).await;
        let policy = RetryPolicy::new(3);

        let file_id = seed_file(store.as_ref(), "img/001.png").await;
        // attempt=1, Transient → retryable (1 < 3)
        seed_failed_sync(store.as_ref(), &file_id, 1, TransferErrorKind::Transient).await;

        let result = execute_recovery(
            &DefaultRecovery,
            &policy,
            &engine,
            store.as_ref() as &dyn FileStore,
            store.as_ref() as &dyn TransferStore,
        )
        .await
        .unwrap();

        assert_eq!(result.retried, 1);
        assert_eq!(result.resolved, 0);
        assert_eq!(result.requeued, 0);
        assert_eq!(result.skipped, 0);

        // Verify: new queued transfer exists with attempt=2
        let queued = store.queued_transfers(&loc("cloud")).await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].attempt(), 2);
        assert_eq!(queued[0].state(), TransferState::Queued);
    }

    // =======================================================================
    // Scenario 2: exhausted Delete + dest absent → Resolve (both strategies)
    // =======================================================================

    #[tokio::test]
    async fn exhausted_delete_dest_absent_resolves_default() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, _backend) = test_setup(dir.path()).await;
        let policy = RetryPolicy::new(3);

        let file_id = seed_file(store.as_ref(), "img/002.png").await;
        // attempt=3, Transient → exhausted (3 >= 3)
        let transfer_id =
            seed_failed_delete(store.as_ref(), &file_id, 3, TransferErrorKind::Transient).await;
        // dest file does NOT exist in backend → dest_exists = false

        let result = execute_recovery(
            &DefaultRecovery,
            &policy,
            &engine,
            store.as_ref() as &dyn FileStore,
            store.as_ref() as &dyn TransferStore,
        )
        .await
        .unwrap();

        assert_eq!(result.resolved, 1);
        assert_eq!(result.retried, 0);
        assert_eq!(result.requeued, 0);

        // Verify: the original transfer is now Completed
        let transfers = store.latest_transfers_by_file(&file_id).await.unwrap();
        let resolved = transfers.iter().find(|t| t.id() == transfer_id).unwrap();
        assert_eq!(resolved.state(), TransferState::Completed);
    }

    #[tokio::test]
    async fn exhausted_delete_dest_absent_resolves_force() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, _backend) = test_setup(dir.path()).await;
        let policy = RetryPolicy::new(3);

        let file_id = seed_file(store.as_ref(), "img/003.png").await;
        seed_failed_delete(store.as_ref(), &file_id, 3, TransferErrorKind::Transient).await;

        let result = execute_recovery(
            &ForceRecovery,
            &policy,
            &engine,
            store.as_ref() as &dyn FileStore,
            store.as_ref() as &dyn TransferStore,
        )
        .await
        .unwrap();

        assert_eq!(result.resolved, 1);
        assert_eq!(result.requeued, 0);
    }

    // =======================================================================
    // Scenario 3: exhausted Delete + dest present → DefaultRecovery skips,
    //             ForceRecovery requeues
    // =======================================================================

    #[tokio::test]
    async fn exhausted_delete_dest_present_default_skips() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, backend) = test_setup(dir.path()).await;
        let policy = RetryPolicy::new(3);

        let file_id = seed_file(store.as_ref(), "img/004.png").await;
        seed_failed_delete(store.as_ref(), &file_id, 3, TransferErrorKind::Transient).await;

        // Put file in backend → dest_exists = true
        backend
            .files
            .lock()
            .await
            .insert("remote/output/img/004.png".into(), b"data".to_vec());

        let result = execute_recovery(
            &DefaultRecovery,
            &policy,
            &engine,
            store.as_ref() as &dyn FileStore,
            store.as_ref() as &dyn TransferStore,
        )
        .await
        .unwrap();

        assert_eq!(result.skipped, 1);
        assert_eq!(result.requeued, 0);
        assert_eq!(result.resolved, 0);
        assert_eq!(result.retried, 0);
    }

    #[tokio::test]
    async fn exhausted_delete_dest_present_force_requeues() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, backend) = test_setup(dir.path()).await;
        let policy = RetryPolicy::new(3);

        let file_id = seed_file(store.as_ref(), "img/005.png").await;
        seed_failed_delete(store.as_ref(), &file_id, 3, TransferErrorKind::Transient).await;

        // Put file in backend → dest_exists = true
        backend
            .files
            .lock()
            .await
            .insert("remote/output/img/005.png".into(), b"data".to_vec());

        let result = execute_recovery(
            &ForceRecovery,
            &policy,
            &engine,
            store.as_ref() as &dyn FileStore,
            store.as_ref() as &dyn TransferStore,
        )
        .await
        .unwrap();

        assert_eq!(result.requeued, 1);
        assert_eq!(result.resolved, 0);
        assert_eq!(result.skipped, 0);

        // Verify: new Delete transfer queued (attempt=1, fresh)
        let queued = store.queued_transfers(&loc("cloud")).await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].kind(), TransferKind::Delete);
        assert_eq!(queued[0].attempt(), 1);
        assert_eq!(queued[0].state(), TransferState::Queued);
    }

    // =======================================================================
    // Scenario 4: exhausted Sync + dest absent → ForceRecovery requeues
    // =======================================================================

    #[tokio::test]
    async fn exhausted_sync_dest_absent_force_requeues() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, _backend) = test_setup(dir.path()).await;
        let policy = RetryPolicy::new(3);

        let file_id = seed_file(store.as_ref(), "img/006.png").await;
        seed_failed_sync(store.as_ref(), &file_id, 3, TransferErrorKind::Transient).await;
        // dest file does NOT exist in backend

        let result = execute_recovery(
            &ForceRecovery,
            &policy,
            &engine,
            store.as_ref() as &dyn FileStore,
            store.as_ref() as &dyn TransferStore,
        )
        .await
        .unwrap();

        assert_eq!(result.requeued, 1);

        let queued = store.queued_transfers(&loc("cloud")).await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].kind(), TransferKind::Sync);
        assert_eq!(queued[0].attempt(), 1);
    }

    // =======================================================================
    // Scenario 5: Permanent error → exhausted immediately, skipped
    // =======================================================================

    #[tokio::test]
    async fn permanent_error_skipped_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, _backend) = test_setup(dir.path()).await;
        let policy = RetryPolicy::new(3);

        let file_id = seed_file(store.as_ref(), "img/007.png").await;
        // Permanent, attempt=1 → exhausted (Permanent is always exhausted)
        seed_failed_sync(store.as_ref(), &file_id, 1, TransferErrorKind::Permanent).await;

        let result = execute_recovery(
            &DefaultRecovery,
            &policy,
            &engine,
            store.as_ref() as &dyn FileStore,
            store.as_ref() as &dyn TransferStore,
        )
        .await
        .unwrap();

        // DefaultRecovery: Sync + exhausted → Skip (regardless of dest)
        assert_eq!(result.skipped, 1);
        assert_eq!(result.retried, 0);
        assert_eq!(result.resolved, 0);
        assert_eq!(result.requeued, 0);
    }

    // =======================================================================
    // Scenario 6: mixed — multiple transfers, each takes different path
    // =======================================================================

    #[tokio::test]
    async fn mixed_transfers_each_action_path() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, backend) = test_setup(dir.path()).await;
        let policy = RetryPolicy::new(3);

        // File A: retryable sync (attempt=1, Transient) → Retry
        let file_a = seed_file(store.as_ref(), "a.png").await;
        seed_failed_sync(store.as_ref(), &file_a, 1, TransferErrorKind::Transient).await;

        // File B: exhausted delete, dest absent → Resolve
        let file_b = seed_file(store.as_ref(), "b.png").await;
        seed_failed_delete(store.as_ref(), &file_b, 3, TransferErrorKind::Transient).await;

        // File C: exhausted delete, dest present → Requeue (ForceRecovery)
        let file_c = seed_file(store.as_ref(), "c.png").await;
        seed_failed_delete(store.as_ref(), &file_c, 3, TransferErrorKind::Transient).await;
        backend
            .files
            .lock()
            .await
            .insert("remote/output/c.png".into(), b"data".to_vec());

        // File D: Permanent sync, dest present → Skip
        // (ForceRecovery: exhausted Sync + dest exists → Skip)
        let file_d = seed_file(store.as_ref(), "d.png").await;
        seed_failed_sync(store.as_ref(), &file_d, 1, TransferErrorKind::Permanent).await;
        backend
            .files
            .lock()
            .await
            .insert("remote/output/d.png".into(), b"data".to_vec());

        let result = execute_recovery(
            &ForceRecovery,
            &policy,
            &engine,
            store.as_ref() as &dyn FileStore,
            store.as_ref() as &dyn TransferStore,
        )
        .await
        .unwrap();

        assert_eq!(result.retried, 1, "File A should be retried");
        assert_eq!(result.resolved, 1, "File B should be resolved");
        assert_eq!(result.requeued, 1, "File C should be requeued");
        assert_eq!(
            result.skipped, 1,
            "File D should be skipped (dest present, Sync)"
        );
    }
}
