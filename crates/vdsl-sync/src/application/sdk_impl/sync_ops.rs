//! `SyncStoreSdk` trait の `SdkImpl` 実装。
//!
//! 公開API面（sync / sync_route / put / delete / restore / get / list /
//! status / errors / pending / locations / all_edges / local_root /
//! set_progress_callback）を集約する。

use std::path::Path;

use async_trait::async_trait;
use tracing::{error, info, trace, warn};

use super::SdkImpl;
use crate::application::error::SyncError;
use crate::application::sdk::{PutReport, SyncReport, SyncReportError, SyncStoreSdk};
use crate::application::topology_store::TopologyFileView;
use crate::application::transfer_engine::PreparedTransfer;
use crate::domain::file_type::FileType;
use crate::domain::fingerprint::FileFingerprint;
use crate::domain::location::{LocationId, SyncSummary};
use crate::domain::view::{ErrorEntry, PendingEntry, PresenceState};
use crate::infra::backend::ProgressFn;

#[async_trait]
impl SyncStoreSdk for SdkImpl {
    // =========================================================================
    // UseCase — 同期操作
    // =========================================================================

    async fn sync(&self) -> Result<SyncReport, SyncError> {
        info!("sdk_impl::sync: pipeline start");
        self.report_progress("ensure: checking locations");

        // Phase 0a: Ensure — 全拠点の到達確認 + 外部ツール確保
        // 失敗したLocationはスキャン/転送対象から除外し、syncは続行する。
        let location_ids: Vec<String> =
            self.locations.iter().map(|l| l.id().to_string()).collect();
        info!(
            location_count = self.locations.len(),
            locations = %location_ids.join(", "),
            "sdk_impl::sync: ensure start"
        );
        let mut failed_locations: std::collections::HashSet<LocationId> =
            std::collections::HashSet::new();
        for loc in &self.locations {
            info!(
                location = %loc.id(),
                kind = ?loc.kind(),
                "sdk_impl::sync: ensure checking"
            );
            match loc.ensure().await {
                Ok(()) => {
                    info!(location = %loc.id(), "sdk_impl::sync: ensure ok");
                }
                Err(e) => {
                    error!(
                        location = %loc.id(),
                        kind = ?loc.kind(),
                        error = %e,
                        "sdk_impl::sync: ensure FAILED — this location will be excluded from sync"
                    );
                    failed_locations.insert(loc.id().clone());
                }
            }
        }
        if failed_locations.is_empty() {
            info!("sdk_impl::sync: ensure done — all locations reachable");
        } else {
            let excluded: Vec<String> =
                failed_locations.iter().map(|l| l.to_string()).collect();
            warn!(
                excluded = %excluded.join(", "),
                "sdk_impl::sync: ensure done — {} location(s) excluded due to ensure failure",
                failed_locations.len()
            );
        }

        // Phase 0b: InFlight孤児の終端化（プロセスクラッシュ復帰）
        let cancelled = self.transfer_store.cancel_orphaned_inflight().await?;
        if cancelled > 0 {
            info!(
                cancelled_count = cancelled,
                "sdk_impl::sync: cancelled orphaned InFlight transfers"
            );
        }

        // Phase 1: Scan → TopologyDelta[]
        self.report_progress("scan: scanning locations");
        info!("sdk_impl::sync: phase1 scan start");
        let progress_cb = self.progress.lock().ok().and_then(|g| g.clone());
        let scan_result = self
            .scanner
            .scan_all(&self.scan_excludes, &failed_locations, progress_cb.as_ref())
            .await?;
        info!(
            scanned = scan_result.scanned,
            deltas = scan_result.deltas.len(),
            scan_errors = scan_result.scan_errors.len(),
            "sdk_impl::sync: phase1 scan done"
        );
        // delta詳細をtrace出力
        for delta in &scan_result.deltas {
            trace!(delta = ?delta, "sdk_impl::sync: delta");
        }

        // Phase 2: Plan — Apply → Distribute → Route → Transfer作成
        self.report_progress(&format!(
            "plan: {} files scanned, {} deltas",
            scan_result.scanned,
            scan_result.deltas.len()
        ));
        info!(
            delta_count = scan_result.deltas.len(),
            "sdk_impl::sync: phase2 plan start"
        );
        let plan_result = self.topology.sync(&scan_result.deltas).await?;
        info!(
            transfers_created = plan_result.transfers_created,
            conflicts = plan_result.conflicts.len(),
            "sdk_impl::sync: phase2 plan done"
        );

        // Phase 3: Execute — BFS順でTransfer実行 + DB永続化
        // Propagate progress callback to all route backends for chunk-level reporting.
        if let Ok(guard) = self.progress.lock() {
            self.engine.set_progress_callback(guard.clone());
        }
        self.report_progress(&format!(
            "execute: {} transfers queued",
            plan_result.transfers_created
        ));
        info!("sdk_impl::sync: phase3 execute start");
        let (transferred, failed, errors) = self.execute_bfs(&failed_locations).await?;
        // Clear backend callbacks after execution.
        self.engine.set_progress_callback(None);
        info!(
            transferred = transferred,
            failed = failed,
            error_count = errors.len(),
            "sdk_impl::sync: phase3 execute done"
        );

        Ok(SyncReport {
            scanned: scan_result.scanned,
            scan_errors: scan_result
                .scan_errors
                .iter()
                .map(|e| SyncReportError {
                    path: e.path.clone(),
                    error: e.error.clone(),
                })
                .collect(),
            transfers_created: plan_result.transfers_created,
            transferred,
            failed,
            errors,
            conflicts: plan_result
                .conflicts
                .iter()
                .map(crate::application::sdk::SyncReportConflict::from)
                .collect(),
        })
    }

    async fn sync_route(
        &self,
        src: &LocationId,
        dest: &LocationId,
    ) -> Result<SyncReport, SyncError> {
        // Phase 0: InFlight孤児の終端化
        let cancelled = self.transfer_store.cancel_orphaned_inflight().await?;
        if cancelled > 0 {
            info!(
                cancelled_count = cancelled,
                "sync_route: cancelled orphaned InFlight transfers"
            );
        }

        // Phase 1: Plan — sync_routeはdelta生成なし、Distribute + Route のみ
        self.report_progress(&format!("plan: route {src} → {dest}"));
        let plan_result = self.topology.sync_route(src, dest).await?;

        // Phase 2: Execute — dest宛のQueued Transferをsrcでフィルタして実行
        // Propagate progress callback to all route backends.
        if let Ok(guard) = self.progress.lock() {
            self.engine.set_progress_callback(guard.clone());
        }
        let queued = self.transfer_store.queued_transfers(dest).await?;
        let eligible: Vec<_> = queued.into_iter().filter(|t| t.src() == src).collect();

        let mut prepared = Vec::with_capacity(eligible.len());
        let mut total_failed = 0usize;
        let mut all_errors: Vec<SyncReportError> = Vec::new();

        for transfer in eligible {
            match self.topology_files.get_by_id(transfer.file_id()).await {
                Ok(Some(file)) => {
                    prepared.push(PreparedTransfer {
                        transfer,
                        relative_path: file.relative_path().to_string(),
                    });
                }
                Ok(None) => {
                    total_failed += 1;
                    all_errors.push(SyncReportError {
                        path: transfer.file_id().to_string(),
                        error: format!("file {} not found in store", transfer.file_id()),
                    });
                }
                Err(e) => {
                    total_failed += 1;
                    all_errors.push(SyncReportError {
                        path: transfer.file_id().to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }

        self.report_progress(&format!(
            "execute: {} transfers ({src} → {dest})",
            prepared.len()
        ));
        let outcomes = self.engine.execute_prepared(prepared).await;
        // Clear backend callbacks after execution.
        self.engine.set_progress_callback(None);
        let mut total_transferred = 0usize;

        self.persist_outcomes(
            &outcomes,
            &mut total_transferred,
            &mut total_failed,
            &mut all_errors,
        )
        .await?;

        Ok(SyncReport {
            scanned: 0,
            scan_errors: Vec::new(),
            transfers_created: plan_result.transfers_created,
            transferred: total_transferred,
            failed: total_failed,
            errors: all_errors,
            conflicts: plan_result
                .conflicts
                .iter()
                .map(crate::application::sdk::SyncReportConflict::from)
                .collect(),
        })
    }

    // =========================================================================
    // Command — ファイル操作
    // =========================================================================

    async fn put(
        &self,
        path: &str,
        file_type: FileType,
        fingerprint: FileFingerprint,
        origin: &LocationId,
        embedded_id: Option<String>,
    ) -> Result<PutReport, SyncError> {
        let result = self
            .topology
            .put(path, file_type, fingerprint, origin, embedded_id)
            .await?;
        Ok(PutReport {
            file_id: result.topology_file_id,
            is_new: result.is_new,
            transfers_created: result.transfers_created,
        })
    }

    async fn delete(&self, path: &str) -> Result<usize, SyncError> {
        self.topology.delete(path).await
    }

    async fn restore(&self, path: &str, revision: &str) -> Result<(), SyncError> {
        info!(path = %path, revision = %revision, "sdk_impl::restore: start");

        // 1. archive_root を持つルート（cloud宛）を engine から1件取得
        let route = self.engine.archive_route().ok_or_else(|| -> SyncError {
            crate::infra::error::InfraError::Transfer {
                reason: "restore: no route with archive_root configured".into(),
            }
            .into()
        })?;

        // 2. 物理復元: cloud archive → cloud original
        route.restore_from_archive(path, revision).await?;
        info!(path = %path, "sdk_impl::restore: physical restore done");

        // 3. 削除済みTopologyFileを取得して unmark
        //    delete transfers 完走後は TF が hard-delete されている (commit c8213ce)
        //    ため見つからないケースがある。物理 restore は既に完了しているので
        //    次回 full sync で cloud から再発見させれば整合が取れる。
        let deleted_tfs = self.topology_files.list_deleted().await?;
        match deleted_tfs.into_iter().find(|t| t.relative_path() == path) {
            Some(mut tf) => {
                tf.unmark_deleted();
                self.topology_files.upsert(&tf).await?;
                info!(path = %path, file_id = %tf.id(), "sdk_impl::restore: TopologyFile unmarked");
            }
            None => {
                warn!(
                    path = %path,
                    "sdk_impl::restore: TopologyFile not in deleted list (likely hard-deleted after delete transfers). Physical restore succeeded — next full sync will re-register."
                );
            }
        }

        Ok(())
    }

    // =========================================================================
    // Query — 読み取り
    // =========================================================================

    async fn get(&self, path: &str) -> Result<Option<TopologyFileView>, SyncError> {
        self.topology.get(path).await
    }

    async fn list(
        &self,
        file_type: Option<FileType>,
        limit: Option<usize>,
    ) -> Result<Vec<TopologyFileView>, SyncError> {
        self.topology.list(file_type, limit).await
    }

    async fn status(&self) -> Result<SyncSummary, SyncError> {
        use crate::domain::location::LocationSummary;
        use crate::domain::transfer::TransferState;
        use std::collections::HashMap;

        let retry_policy = self.config.retry_policy();
        let total_files = self.topology.file_count().await?;
        let stats = self.transfer_store.transfer_stats().await?;
        let present_counts = self.transfer_store.present_counts_by_location().await?;
        let failed = self.transfer_store.failed_transfers().await?;
        let pending = self.transfer_store.all_pending_transfers().await?;

        let mut locations: HashMap<LocationId, LocationSummary> = HashMap::new();
        let mut total_errors = 0usize;

        for (loc, count) in &present_counts {
            let summary = locations.entry(loc.clone()).or_default();
            summary.present = *count;
        }

        for row in &stats {
            if row.state == TransferState::Completed || row.state == TransferState::Cancelled {
                continue;
            }
            let dest_state = match row.state {
                TransferState::Blocked | TransferState::Queued => PresenceState::Pending,
                TransferState::InFlight => PresenceState::Syncing,
                TransferState::Failed => {
                    let exhausted = match row.error_kind.as_deref() {
                        Some("permanent") => true,
                        _ => row.attempt >= retry_policy.max_attempts(),
                    };
                    if exhausted {
                        PresenceState::Failed
                    } else {
                        PresenceState::Pending
                    }
                }
                TransferState::Completed | TransferState::Cancelled => PresenceState::Absent,
            };

            let dest_summary = locations.entry(row.dest.clone()).or_default();
            match dest_state {
                PresenceState::Pending => {
                    dest_summary.pending = dest_summary.pending.saturating_add(row.file_count);
                }
                PresenceState::Syncing => {
                    dest_summary.syncing = dest_summary.syncing.saturating_add(row.file_count);
                }
                PresenceState::Failed => {
                    dest_summary.failed = dest_summary.failed.saturating_add(row.file_count);
                    total_errors = total_errors.saturating_add(row.file_count);
                }
                PresenceState::Absent => {
                    dest_summary.absent = dest_summary.absent.saturating_add(row.file_count);
                }
                PresenceState::Present => {}
            }
        }

        let error_entries: Vec<ErrorEntry> = failed
            .iter()
            .filter(|t| {
                let state = PresenceState::from_transfer(t, &retry_policy);
                state == PresenceState::Failed
            })
            .map(ErrorEntry::from_transfer)
            .collect();

        let mut pending_entries: Vec<PendingEntry> =
            pending.iter().map(PendingEntry::from_transfer).collect();
        for t in &failed {
            let state = PresenceState::from_transfer(t, &retry_policy);
            if state == PresenceState::Pending {
                pending_entries.push(PendingEntry::from_transfer(t));
            }
        }

        Ok(SyncSummary {
            locations,
            total_entries: total_files,
            total_errors,
            error_entries,
            pending_entries,
        })
    }

    async fn errors(&self) -> Result<Vec<ErrorEntry>, SyncError> {
        let retry_policy = self.config.retry_policy();
        let failed = self.transfer_store.failed_transfers().await?;
        Ok(failed
            .iter()
            .filter(|t| {
                let state = PresenceState::from_transfer(t, &retry_policy);
                state == PresenceState::Failed
            })
            .map(ErrorEntry::from_transfer)
            .collect())
    }

    async fn pending(&self, dest: &LocationId) -> Result<Vec<PendingEntry>, SyncError> {
        let retry_policy = self.config.retry_policy();

        // Queued/Blocked/InFlight transfers for the target dest
        let all_pending = self.transfer_store.all_pending_transfers().await?;
        let mut entries: Vec<PendingEntry> = all_pending
            .iter()
            .filter(|t| t.dest() == dest)
            .map(PendingEntry::from_transfer)
            .collect();

        // Failed but retryable transfers also count as pending
        let failed = self.transfer_store.failed_transfers().await?;
        for t in &failed {
            if t.dest() == dest {
                let state = PresenceState::from_transfer(t, &retry_policy);
                if state == PresenceState::Pending {
                    entries.push(PendingEntry::from_transfer(t));
                }
            }
        }

        Ok(entries)
    }

    // =========================================================================
    // Topology — 読み取り専用
    // =========================================================================

    fn locations(&self) -> Vec<LocationId> {
        self.topology.locations().to_vec()
    }

    fn all_edges(&self) -> Vec<(LocationId, LocationId)> {
        self.engine.all_edges()
    }

    fn local_root(&self) -> Option<&Path> {
        self.engine.local_root()
    }

    fn set_progress_callback(&self, callback: Option<ProgressFn>) {
        if let Ok(mut guard) = self.progress.lock() {
            *guard = callback;
        }
    }
}
