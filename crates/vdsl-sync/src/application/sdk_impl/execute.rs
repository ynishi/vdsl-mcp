//! BFS実行 / バッチ処理 / 結果永続化。
//!
//! `sync()` / `sync_route()` の Execute フェーズ実装を集約する。

use tracing::{debug, error, info, trace, warn};

use super::SdkImpl;
use crate::application::error::SyncError;
use crate::application::sdk::SyncReportError;
use crate::application::transfer_engine::{PreparedTransfer, TransferOutcome};
use crate::domain::location::LocationId;
use crate::domain::transfer::{Transfer, TransferState};

impl SdkImpl {
    /// BFS順でTransfer実行 + DB永続化。
    ///
    /// Engine.execute_prepared()で純粋なroute I/Oを実行し、
    /// 結果をtransfer_storeに永続化 + unblock_dependentsでチェーン転送を解放する。
    pub(super) async fn execute_bfs(
        &self,
        skip_locations: &std::collections::HashSet<LocationId>,
    ) -> Result<(usize, usize, Vec<SyncReportError>), SyncError> {
        let mut total_transferred = 0usize;
        let mut total_failed = 0usize;
        let mut all_errors: Vec<SyncReportError> = Vec::new();

        let targets = self.engine.all_targets_ordered();
        debug!(targets = ?targets, "execute_bfs: BFS target order");

        // Re-iterate BFS targets until no progress: chain transfers (e.g. pod→cloud)
        // become Queued only after their parent (local→pod) completes via
        // `unblock_dependents`. A single pass would miss them when the dependent
        // target was visited before the parent.
        let max_passes = targets.len().saturating_add(1).max(2);
        for pass in 0..max_passes {
            let mut progress = false;
            for target in &targets {
                if skip_locations.contains(target) {
                    if pass == 0 {
                        warn!(
                            target = %target,
                            "execute_bfs: skipping target (ensure failed)"
                        );
                    }
                    continue;
                }
                let queued = self.transfer_store.queued_transfers(target).await?;
                if queued.is_empty() {
                    debug!(target = %target, pass, "execute_bfs: no queued transfers, skip");
                    continue;
                }
                progress = true;
                info!(target = %target, pass, queued = queued.len(), "execute_bfs: processing target");
                self.process_target_batch(
                    target,
                    queued,
                    &mut total_transferred,
                    &mut total_failed,
                    &mut all_errors,
                )
                .await?;
            }
            if !progress {
                debug!(pass, "execute_bfs: no progress, exiting");
                break;
            }
        }

        Ok((total_transferred, total_failed, all_errors))
    }

    /// 1ターゲット分のqueued転送をprepare→sync→delete→permitの順で実行する。
    pub(super) async fn process_target_batch(
        &self,
        target: &LocationId,
        queued: Vec<Transfer>,
        total_transferred: &mut usize,
        total_failed: &mut usize,
        all_errors: &mut Vec<SyncReportError>,
    ) -> Result<(), SyncError> {
        info!(target = %target, queued = queued.len(), "execute_bfs: processing target");
        self.report_progress(&format!("target {target}: {} queued", queued.len()));

        // Prepare: resolve relative_path from topology_files
        let mut prepared = Vec::with_capacity(queued.len());
        let mut resolve_miss = 0usize;
        for transfer in queued {
            match self.topology_files.get_by_id(transfer.file_id()).await {
                Ok(Some(file)) => {
                    trace!(
                        file_id = %transfer.file_id(),
                        path = %file.relative_path(),
                        src = %transfer.src(),
                        dest = %transfer.dest(),
                        "execute_bfs: prepared"
                    );
                    prepared.push(PreparedTransfer {
                        transfer,
                        relative_path: file.relative_path().to_string(),
                    });
                }
                Ok(None) => {
                    resolve_miss += 1;
                    error!(
                        file_id = %transfer.file_id(),
                        src = %transfer.src(),
                        dest = %transfer.dest(),
                        "execute_bfs: topology_file not found — transfer skipped"
                    );
                    *total_failed += 1;
                    all_errors.push(SyncReportError {
                        path: transfer.file_id().to_string(),
                        error: format!("file {} not found in store", transfer.file_id()),
                    });
                }
                Err(e) => {
                    resolve_miss += 1;
                    error!(
                        file_id = %transfer.file_id(),
                        src = %transfer.src(),
                        dest = %transfer.dest(),
                        err = %e,
                        "execute_bfs: topology_file lookup error — transfer skipped"
                    );
                    *total_failed += 1;
                    all_errors.push(SyncReportError {
                        path: transfer.file_id().to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }
        // Partition: sync / delete を分離して段階実行
        // sync完了→DB永続化→delete実行→DB永続化 の2段階。
        // delete がハング/失敗しても sync 結果がDBに反映される。
        let (sync_prepared, delete_prepared): (Vec<_>, Vec<_>) =
            prepared.into_iter().partition(|p| !p.transfer.is_delete());

        debug!(
            target = %target,
            sync = sync_prepared.len(),
            delete = delete_prepared.len(),
            resolve_miss = resolve_miss,
            "execute_bfs: preparation done"
        );

        // Phase A: Sync transfers → execute → DB persist
        if !sync_prepared.is_empty() {
            self.report_progress(&format!(
                "target {target}: syncing {} files",
                sync_prepared.len()
            ));
            info!(
                target = %target,
                count = sync_prepared.len(),
                "execute_bfs: executing sync transfers"
            );
            let sync_outcomes = self.engine.execute_prepared(sync_prepared).await;
            self.report_progress(&format!(
                "target {target}: sync done, persisting {}",
                sync_outcomes.len()
            ));
            info!(
                target = %target,
                outcomes = sync_outcomes.len(),
                "execute_bfs: sync execution done, persisting"
            );
            self.persist_outcomes(&sync_outcomes, total_transferred, total_failed, all_errors)
                .await?;
        }

        // Phase B: Delete transfers → execute → DB persist
        if !delete_prepared.is_empty() {
            self.report_progress(&format!(
                "target {target}: deleting {} files",
                delete_prepared.len()
            ));
            info!(
                target = %target,
                count = delete_prepared.len(),
                "execute_bfs: executing delete transfers"
            );
            let delete_outcomes = self.engine.execute_prepared(delete_prepared).await;
            self.report_progress(&format!(
                "target {target}: delete done, persisting {}",
                delete_outcomes.len()
            ));
            info!(
                target = %target,
                outcomes = delete_outcomes.len(),
                "execute_bfs: delete execution done, persisting"
            );
            self.persist_outcomes(
                &delete_outcomes,
                total_transferred,
                total_failed,
                all_errors,
            )
            .await?;
        }

        info!(
            target = %target,
            transferred = *total_transferred,
            failed = *total_failed,
            "execute_bfs: target batch done"
        );

        Ok(())
    }

    /// TransferOutcome群をDB永続化する共通ヘルパー。
    ///
    /// sync/delete の2段階実行で共通化するために抽出。
    pub(super) async fn persist_outcomes(
        &self,
        outcomes: &[TransferOutcome],
        total_transferred: &mut usize,
        total_failed: &mut usize,
        all_errors: &mut Vec<SyncReportError>,
    ) -> Result<(), SyncError> {
        for outcome in outcomes {
            let is_completed = outcome.transfer.state() == TransferState::Completed;
            self.transfer_store
                .update_transfer(&outcome.transfer)
                .await?;

            if is_completed {
                self.transfer_store
                    .unblock_dependents(outcome.transfer.id())
                    .await?;

                if outcome.transfer.is_delete() {
                    // Delete完了 = dest側にファイルが存在しない → LocationFile削除
                    let deleted = self
                        .location_files
                        .delete(outcome.transfer.file_id(), outcome.transfer.dest())
                        .await?;
                    trace!(
                        file_id = %outcome.transfer.file_id(),
                        dest = %outcome.transfer.dest(),
                        deleted = deleted,
                        "execute_bfs: delete transfer → LocationFile removed"
                    );
                    // 全LF削除済みならTFを物理削除（list_deleted肥大化防止）
                    let remaining = self
                        .location_files
                        .list_by_file(outcome.transfer.file_id())
                        .await?;
                    if remaining.is_empty() {
                        let purged = self
                            .topology_files
                            .hard_delete(outcome.transfer.file_id())
                            .await?;
                        if purged {
                            debug!(
                                file_id = %outcome.transfer.file_id(),
                                "execute_bfs: all LFs gone → TopologyFile hard-deleted"
                            );
                        }
                    }
                } else {
                    // Sync完了 = dest側にファイルが存在 → LocationFile作成
                    if let Ok(Some(tf)) = self
                        .topology_files
                        .get_by_id(outcome.transfer.file_id())
                        .await
                    {
                        let src_lf = self
                            .location_files
                            .get(outcome.transfer.file_id(), outcome.transfer.src())
                            .await?;
                        if let Some(src_lf) = src_lf {
                            trace!(
                                file_id = %outcome.transfer.file_id(),
                                src = %outcome.transfer.src(),
                                dest = %outcome.transfer.dest(),
                                path = %outcome.relative_path,
                                "persist_outcomes: creating dest LocationFile from src"
                            );
                            let dest_lf = tf
                                .materialize(
                                    outcome.transfer.dest().clone(),
                                    outcome.relative_path.clone(),
                                    src_lf.fingerprint().clone(),
                                    src_lf.embedded_id().map(|s| s.to_string()),
                                )
                                .map_err(SyncError::Domain)?;
                            self.location_files.upsert(&dest_lf).await?;
                        } else {
                            warn!(
                                file_id = %outcome.transfer.file_id(),
                                src = %outcome.transfer.src(),
                                "persist_outcomes: src LocationFile not found, cannot create dest LF"
                            );
                        }
                    } else {
                        warn!(
                            file_id = %outcome.transfer.file_id(),
                            "persist_outcomes: TopologyFile not found for completed transfer"
                        );
                    }
                }

                *total_transferred += 1;
                info!(
                    id = %outcome.transfer.id(),
                    src = %outcome.transfer.src(),
                    dest = %outcome.transfer.dest(),
                    path = %outcome.relative_path,
                    kind = ?outcome.transfer.kind(),
                    "execute_bfs: transfer completed"
                );
            } else {
                *total_failed += 1;
                let err_msg = outcome
                    .transfer
                    .error()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown error".to_string());
                error!(
                    id = %outcome.transfer.id(),
                    src = %outcome.transfer.src(),
                    dest = %outcome.transfer.dest(),
                    path = %outcome.relative_path,
                    err = %err_msg,
                    "execute_bfs: transfer FAILED"
                );
                all_errors.push(SyncReportError {
                    path: outcome.relative_path.clone(),
                    error: err_msg,
                });
            }
        }
        Ok(())
    }
}
