//! SQLite implementation of file/transfer/task stores.
//!
//! Uses normalized schema: `topology_files` + `location_files` + `transfers` + `sync_tasks`.
//! Schema versioning via `PRAGMA user_version` (see [`schema`]).
//! Designed for single-writer (sync engine), concurrent readers OK.
//!
//! Uses `tokio-rusqlite` for non-blocking async access — each connection
//! runs on a dedicated background thread with mpsc channel dispatch.

mod location_file_store_impl;
mod mapping;
mod schema;
mod task_store_impl;
mod topology_file_store_impl;
mod transfer_store_impl;

use std::path::Path;

use crate::infra::error::InfraError;

/// SQLite-backed sync store.
///
/// Uses `tokio_rusqlite::Connection` — a handle that dispatches closures
/// to a dedicated background thread via mpsc channel. Does not block
/// the async runtime.
pub struct SqliteSyncStore {
    conn: tokio_rusqlite::Connection,
}

impl SqliteSyncStore {
    /// Open (or create) a sync database at the given path.
    pub async fn open(path: &Path) -> Result<Self, InfraError> {
        let path = path.to_path_buf();
        let conn =
            tokio_rusqlite::Connection::open(&path)
                .await
                .map_err(|e| InfraError::Store {
                    op: "sqlite",
                    reason: format!("open failed: {e}"),
                })?;
        conn.call(schema::init_connection)
            .await
            .map_err(map_call_err)?;
        Ok(Self { conn })
    }

    /// Open an in-memory database (for testing).
    pub async fn open_in_memory() -> Result<Self, InfraError> {
        let conn = tokio_rusqlite::Connection::open_in_memory()
            .await
            .map_err(|e| InfraError::Store {
                op: "sqlite",
                reason: format!("open_in_memory failed: {e}"),
            })?;
        conn.call(schema::init_connection)
            .await
            .map_err(map_call_err)?;
        Ok(Self { conn })
    }
}

// =============================================================================
// Error mapping
// =============================================================================

/// Convert `tokio_rusqlite::Error<InfraError>` → `InfraError`.
fn map_call_err(e: tokio_rusqlite::Error<InfraError>) -> InfraError {
    match e {
        tokio_rusqlite::Error::Error(infra_err) => infra_err,
        tokio_rusqlite::Error::ConnectionClosed => InfraError::Store {
            op: "sqlite",
            reason: "sqlite connection closed".into(),
        },
        tokio_rusqlite::Error::Close((_, e)) => InfraError::Store {
            op: "sqlite",
            reason: format!("sqlite close error: {e}"),
        },
        other => InfraError::Store {
            op: "sqlite",
            reason: format!("tokio-rusqlite: {other:?}"),
        },
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use rusqlite::params;

    use crate::domain::file_type::FileType;
    use crate::domain::location::LocationId;
    use crate::domain::topology_file::TopologyFile;
    use crate::domain::transfer::Transfer;
    use crate::infra::topology_file_store::TopologyFileStore;
    use crate::infra::transfer_store::TransferStore;

    fn loc(s: &str) -> LocationId {
        LocationId::new(s).expect("valid test location")
    }

    /// Create a test TopologyFile and insert it into the store.
    /// Returns the TopologyFile (for use as FK target in transfers).
    async fn insert_test_topology_file(store: &SqliteSyncStore, path: &str) -> TopologyFile {
        let tf =
            TopologyFile::new(path.to_string(), FileType::Image).expect("valid test topology file");
        TopologyFileStore::upsert(store, &tf)
            .await
            .expect("insert topology file");
        tf
    }

    // =========================================================================
    // TransferStore tests
    // =========================================================================

    #[tokio::test]
    async fn insert_and_query_transfer() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = insert_test_topology_file(&store, "output/t.png").await;

        let transfer =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        store
            .insert_transfer(&transfer)
            .await
            .expect("insert transfer");

        let queued = store.queued_transfers(&loc("cloud")).await.expect("queued");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].file_id(), file.id());
        assert_eq!(queued[0].dest(), &loc("cloud"));
    }

    #[tokio::test]
    async fn update_transfer_state() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = insert_test_topology_file(&store, "output/s.png").await;

        let mut transfer =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        store
            .insert_transfer(&transfer)
            .await
            .expect("insert transfer");

        transfer.start().expect("start");
        store
            .update_transfer(&transfer)
            .await
            .expect("update transfer");

        let queued = store.queued_transfers(&loc("cloud")).await.expect("queued");
        assert_eq!(queued.len(), 0);

        transfer.complete().expect("complete");
        store
            .update_transfer(&transfer)
            .await
            .expect("update transfer");

        let latest = store
            .latest_transfers_by_file(file.id())
            .await
            .expect("latest");
        assert_eq!(latest.len(), 1);
        assert_eq!(
            latest[0].state(),
            crate::domain::transfer::TransferState::Completed
        );
    }

    #[tokio::test]
    async fn failed_transfers_query() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = insert_test_topology_file(&store, "output/f.png").await;

        let mut transfer =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        transfer.start().expect("start");
        transfer
            .fail(
                "timeout".into(),
                crate::domain::retry::TransferErrorKind::Transient,
            )
            .expect("fail");
        store
            .insert_transfer(&transfer)
            .await
            .expect("insert transfer");

        let failed = store.failed_transfers().await.expect("failed");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].error(), Some("timeout"));
        assert_eq!(
            failed[0].error_kind(),
            Some(crate::domain::retry::TransferErrorKind::Transient)
        );
    }

    #[tokio::test]
    async fn failed_transfers_excludes_retried() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = insert_test_topology_file(&store, "output/retry.png").await;

        // T1: Failed (attempt=1)
        let mut t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        t1.start().expect("start");
        t1.fail(
            "net error".into(),
            crate::domain::retry::TransferErrorKind::Transient,
        )
        .expect("fail");
        store.insert_transfer(&t1).await.expect("insert t1");

        // T2: retry of T1 → Queued (attempt=2), then fails again
        let mut t2 = t1.retry().expect("retry");
        t2.start().expect("start");
        t2.fail(
            "net error again".into(),
            crate::domain::retry::TransferErrorKind::Transient,
        )
        .expect("fail");
        store.insert_transfer(&t2).await.expect("insert t2");

        // failed_transfers should return only T2 (latest), not T1
        let failed = store.failed_transfers().await.expect("failed");
        assert_eq!(
            failed.len(),
            1,
            "should return only the latest failed transfer"
        );
        assert_eq!(failed[0].error(), Some("net error again"));
        assert_eq!(failed[0].attempt(), 2);
    }

    #[tokio::test]
    async fn latest_transfers_by_file_returns_latest_per_dest() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = insert_test_topology_file(&store, "output/r.png").await;

        let mut t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        t1.start().expect("start");
        t1.fail(
            "err".into(),
            crate::domain::retry::TransferErrorKind::Transient,
        )
        .expect("fail");
        store.insert_transfer(&t1).await.expect("insert t1");

        let t2 = t1.retry().expect("retry");
        store.insert_transfer(&t2).await.expect("insert t2");

        let mut t3 = Transfer::new(file.id().to_string(), loc("local"), loc("pod")).expect("valid");
        t3.start().expect("start");
        t3.complete().expect("complete");
        store.insert_transfer(&t3).await.expect("insert t3");

        let latest = store
            .latest_transfers_by_file(file.id())
            .await
            .expect("latest");
        assert_eq!(latest.len(), 2);

        let cloud = latest
            .iter()
            .find(|t| t.dest() == &loc("cloud"))
            .expect("cloud");
        assert_eq!(
            cloud.state(),
            crate::domain::transfer::TransferState::Queued
        );
        assert_eq!(cloud.attempt(), 2);

        let pod = latest
            .iter()
            .find(|t| t.dest() == &loc("pod"))
            .expect("pod");
        assert_eq!(
            pod.state(),
            crate::domain::transfer::TransferState::Completed
        );
    }

    #[tokio::test]
    async fn queued_returns_only_latest_per_file_dest() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = insert_test_topology_file(&store, "output/q.png").await;

        let mut t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid");
        t1.start().expect("start");
        t1.fail(
            "err".into(),
            crate::domain::retry::TransferErrorKind::Transient,
        )
        .expect("fail");
        store.insert_transfer(&t1).await.expect("insert t1");

        let t2 = t1.retry().expect("retry");
        store.insert_transfer(&t2).await.expect("insert t2");

        let queued = store.queued_transfers(&loc("cloud")).await.expect("queued");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].attempt(), 2);
    }

    // =========================================================================
    // unblock_dependents tests
    // =========================================================================

    #[tokio::test]
    async fn unblock_dependents_transitions_blocked_to_queued() {
        use crate::domain::transfer::TransferKind;

        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = insert_test_topology_file(&store, "output/chain.png").await;

        // T1: local→cloud (Queued — 先行transfer)
        let t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid t1");
        let t1_id = t1.id().to_string();
        store.insert_transfer(&t1).await.expect("insert t1");

        // T2: cloud→pod (Blocked, depends_on=T1)
        let t2 = Transfer::with_dependency(
            file.id().to_string(),
            loc("cloud"),
            loc("pod"),
            TransferKind::Sync,
            t1_id.clone(),
        )
        .expect("valid t2");
        let t2_id = t2.id().to_string();
        store.insert_transfer(&t2).await.expect("insert t2");

        // Before unblock: T2 should NOT appear in queued_transfers
        let queued_before = store.queued_transfers(&loc("pod")).await.expect("queued");
        assert_eq!(
            queued_before.len(),
            0,
            "blocked transfer must not appear in queued"
        );

        // Simulate T1 completion → unblock dependents
        let unblocked = store.unblock_dependents(&t1_id).await.expect("unblock");
        assert_eq!(unblocked, 1, "exactly one transfer should be unblocked");

        // After unblock: T2 should appear in queued_transfers
        let queued_after = store.queued_transfers(&loc("pod")).await.expect("queued");
        assert_eq!(
            queued_after.len(),
            1,
            "unblocked transfer must appear in queued"
        );
        assert_eq!(queued_after[0].id(), t2_id);
    }

    #[tokio::test]
    async fn unblock_dependents_ignores_non_blocked_state() {
        use crate::domain::transfer::TransferKind;

        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file = insert_test_topology_file(&store, "output/nonblock.png").await;

        // T1: local→cloud (Queued)
        let t1 =
            Transfer::new(file.id().to_string(), loc("local"), loc("cloud")).expect("valid t1");
        let t1_id = t1.id().to_string();
        store.insert_transfer(&t1).await.expect("insert t1");

        // T2: depends on T1, but manually set to in_flight (not blocked)
        let t2 = Transfer::with_dependency(
            file.id().to_string(),
            loc("cloud"),
            loc("pod"),
            TransferKind::Sync,
            t1_id.clone(),
        )
        .expect("valid t2");
        // with_dependency creates Blocked. Insert as-is, then manually
        // update via SQL to simulate a non-blocked state (race condition).
        store.insert_transfer(&t2).await.expect("insert t2");

        // Manually update T2 to in_flight via SQL (simulating a race)
        let t2_id_clone = t2.id().to_string();
        store
            .conn
            .call(move |conn| {
                conn.execute(
                    "UPDATE transfers SET state = 'in_flight' WHERE id = ?",
                    params![t2_id_clone],
                )
                .map_err(|e| InfraError::Store {
                    op: "sqlite",
                    reason: format!("{e}"),
                })
            })
            .await
            .expect("manual update");

        // unblock should NOT touch in_flight transfers
        let unblocked = store.unblock_dependents(&t1_id).await.expect("unblock");
        assert_eq!(unblocked, 0, "in_flight transfer must not be unblocked");
    }

    #[tokio::test]
    async fn unblock_dependents_multiple_dependents() {
        use crate::domain::transfer::TransferKind;

        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let file_a = insert_test_topology_file(&store, "output/multi_a.png").await;
        let file_b = insert_test_topology_file(&store, "output/multi_b.png").await;

        // T1: local→cloud (shared dependency)
        let t1 =
            Transfer::new(file_a.id().to_string(), loc("local"), loc("cloud")).expect("valid t1");
        let t1_id = t1.id().to_string();
        store.insert_transfer(&t1).await.expect("insert t1");

        // T2: cloud→pod for file_a (Blocked, depends_on=T1)
        let t2 = Transfer::with_dependency(
            file_a.id().to_string(),
            loc("cloud"),
            loc("pod"),
            TransferKind::Sync,
            t1_id.clone(),
        )
        .expect("valid t2");
        store.insert_transfer(&t2).await.expect("insert t2");

        // T3: cloud→nas for file_b (Blocked, depends_on=T1)
        let t3 = Transfer::with_dependency(
            file_b.id().to_string(),
            loc("cloud"),
            loc("nas"),
            TransferKind::Sync,
            t1_id.clone(),
        )
        .expect("valid t3");
        store.insert_transfer(&t3).await.expect("insert t3");

        // Unblock both at once
        let unblocked = store.unblock_dependents(&t1_id).await.expect("unblock");
        assert_eq!(unblocked, 2, "both blocked transfers should be unblocked");

        // Verify both are now queued
        let pod_queued = store.queued_transfers(&loc("pod")).await.expect("pod");
        assert_eq!(pod_queued.len(), 1);
        let nas_queued = store.queued_transfers(&loc("nas")).await.expect("nas");
        assert_eq!(nas_queued.len(), 1);
    }

    #[tokio::test]
    async fn unblock_dependents_no_dependents_returns_zero() {
        let store = SqliteSyncStore::open_in_memory().await.expect("open");

        // No transfers at all — should return 0 without error
        let unblocked = store
            .unblock_dependents("nonexistent-id")
            .await
            .expect("unblock");
        assert_eq!(unblocked, 0);
    }

    // =========================================================================
    // E2E: 2回sync → list_deleted が0件であることを検証
    //
    // 再現対象: delete 3514件問題
    // 仮説: 2回目syncでDiscoveredが新IDで生成され、
    //        upsert path conflict retireで既存TFがdeleted化
    // =========================================================================

    #[tokio::test]
    async fn two_syncs_should_not_create_deleted_topology_files() {
        use crate::application::topology_store::TopologyStore;
        use crate::domain::digest::{ByteDigest, ContentDigest};
        use crate::domain::fingerprint::FileFingerprint;
        use crate::domain::graph::RouteGraph;
        use crate::domain::topology_delta::{DiscoveredFile, TopologyDelta};
        use crate::infra::location_file_store::LocationFileStore;
        use crate::infra::topology_file_store::TopologyFileStore;

        let store = SqliteSyncStore::open_in_memory().await.expect("open");
        let store = std::sync::Arc::new(store);

        let local = loc("local");
        let cloud = loc("cloud");
        let mut graph = RouteGraph::new();
        graph.add(local.clone(), cloud.clone());
        graph.add(cloud.clone(), local.clone());
        let locations = vec![local.clone(), cloud.clone()];

        let topo = TopologyStore::new(
            store.clone() as std::sync::Arc<dyn TopologyFileStore>,
            store.clone() as std::sync::Arc<dyn LocationFileStore>,
            store.clone() as std::sync::Arc<dyn crate::infra::transfer_store::TransferStore>,
            graph,
            locations,
        );

        // 10ファイル分のDiscovered delta
        let make_deltas = |origin: &LocationId| -> Vec<TopologyDelta> {
            (0..10)
                .map(|i| {
                    TopologyDelta::Discovered(DiscoveredFile {
                        relative_path: format!("output/img-{i:04}.png"),
                        file_type: FileType::Image,
                        fingerprint: FileFingerprint {
                            byte_digest: Some(ByteDigest::Djb2(format!("hash-{i}"))),
                            content_digest: Some(ContentDigest(format!("hash-{i}"))),
                            meta_digest: None,
                            size: 1024,
                            modified_at: None,
                        },
                        origin: origin.clone(),
                        embedded_id: None,
                    })
                })
                .collect()
        };

        // 1回目sync
        let deltas1 = make_deltas(&local);
        let result1 = topo.sync(&deltas1).await.expect("sync1");
        assert_eq!(result1.ingested, 10, "sync1: 10 deltas ingested");

        let deleted_after_sync1 = TopologyFileStore::list_deleted(&*store)
            .await
            .expect("list_deleted");
        assert_eq!(
            deleted_after_sync1.len(),
            0,
            "sync1: no deleted TFs expected"
        );

        // 2回目sync — 同じファイル群、同じfingerprint
        // match_and_classify で ByPath → fingerprint unchanged → Skip となるはず
        // → delta 0件で sync に渡される
        // しかし実際は compute_topology_deltas を経由するので、
        // ここでは apply_ingest に渡す Discovered を直接構築して
        // 「2回目にDiscoveredが再生成される」シナリオをテストする
        let deltas2 = make_deltas(&local);
        let _result2 = topo.sync(&deltas2).await.expect("sync2");

        let deleted_after_sync2 = TopologyFileStore::list_deleted(&*store)
            .await
            .expect("list_deleted");
        assert_eq!(
            deleted_after_sync2.len(),
            0,
            "sync2: no deleted TFs expected, but got {} (path conflict retire?)",
            deleted_after_sync2.len()
        );

        // activeは10件のまま
        let active = TopologyFileStore::count_active(&*store)
            .await
            .expect("count_active");
        assert_eq!(active, 10, "active TFs should remain 10");
    }

    /// Discovered → Vanished → 再Discovered で deleted TF が蓄積しないことを検証
    #[tokio::test]
    async fn discovered_vanished_rediscovered_no_stale_deleted() {
        use crate::application::topology_store::TopologyStore;
        use crate::domain::digest::{ByteDigest, ContentDigest};
        use crate::domain::fingerprint::FileFingerprint;
        use crate::domain::graph::RouteGraph;
        use crate::domain::topology_delta::{DiscoveredFile, TopologyDelta, VanishedFile};
        use crate::infra::location_file_store::LocationFileStore;
        use crate::infra::topology_file_store::TopologyFileStore;

        let store = std::sync::Arc::new(SqliteSyncStore::open_in_memory().await.expect("open"));

        let local = loc("local");
        let cloud = loc("cloud");
        let mut graph = RouteGraph::new();
        graph.add(local.clone(), cloud.clone());
        graph.add(cloud.clone(), local.clone());

        let topo = TopologyStore::new(
            store.clone() as std::sync::Arc<dyn TopologyFileStore>,
            store.clone() as std::sync::Arc<dyn LocationFileStore>,
            store.clone() as std::sync::Arc<dyn crate::infra::transfer_store::TransferStore>,
            graph,
            vec![local.clone(), cloud.clone()],
        );

        let fp = FileFingerprint {
            byte_digest: Some(ByteDigest::Djb2("abc".into())),
            content_digest: Some(ContentDigest("abc".into())),
            meta_digest: None,
            size: 1024,
            modified_at: None,
        };

        // sync1: Discovered
        let d1 = vec![TopologyDelta::Discovered(DiscoveredFile {
            relative_path: "output/test.png".into(),
            file_type: FileType::Image,
            fingerprint: fp.clone(),
            origin: local.clone(),
            embedded_id: None,
        })];
        topo.sync(&d1).await.expect("sync1");

        let tf_id = {
            let tfs = TopologyFileStore::list_active(&*store, None, None)
                .await
                .expect("list");
            assert_eq!(tfs.len(), 1);
            tfs[0].id().to_string()
        };

        // sync2: Vanished（ファイル消失）
        let d2 = vec![TopologyDelta::Vanished(VanishedFile {
            topology_file_id: tf_id.clone(),
            relative_path: "output/test.png".into(),
            origin: local.clone(),
        })];
        topo.sync(&d2).await.expect("sync2");

        // Vanished後: TF自体はdeleted化しない（mark_deletedは撤回済み）
        let deleted_after_vanish = TopologyFileStore::list_deleted(&*store)
            .await
            .expect("list_deleted");
        assert_eq!(
            deleted_after_vanish.len(),
            0,
            "Vanished should not mark_deleted TF"
        );

        // sync3: 再Discovered（同じファイルが戻ってきた）
        let d3 = vec![TopologyDelta::Discovered(DiscoveredFile {
            relative_path: "output/test.png".into(),
            file_type: FileType::Image,
            fingerprint: fp.clone(),
            origin: local.clone(),
            embedded_id: None,
        })];
        topo.sync(&d3).await.expect("sync3");

        let deleted_after_rediscovery = TopologyFileStore::list_deleted(&*store)
            .await
            .expect("list_deleted");
        assert_eq!(
            deleted_after_rediscovery.len(),
            0,
            "Re-Discovered should reuse existing TF, not create path conflict. Got {} deleted.",
            deleted_after_rediscovery.len()
        );
    }

    /// 複数Location（local + cloud）から同一ファイルをDiscoveredで報告 → 2回sync
    #[tokio::test]
    async fn two_syncs_multi_origin_no_deleted() {
        use crate::application::topology_store::TopologyStore;
        use crate::domain::digest::{ByteDigest, ContentDigest};
        use crate::domain::fingerprint::FileFingerprint;
        use crate::domain::graph::RouteGraph;
        use crate::domain::topology_delta::{DiscoveredFile, TopologyDelta};
        use crate::infra::location_file_store::LocationFileStore;
        use crate::infra::topology_file_store::TopologyFileStore;

        let store = std::sync::Arc::new(SqliteSyncStore::open_in_memory().await.expect("open"));

        let local = loc("local");
        let cloud = loc("cloud");
        let mut graph = RouteGraph::new();
        graph.add(local.clone(), cloud.clone());
        graph.add(cloud.clone(), local.clone());

        let topo = TopologyStore::new(
            store.clone() as std::sync::Arc<dyn TopologyFileStore>,
            store.clone() as std::sync::Arc<dyn LocationFileStore>,
            store.clone() as std::sync::Arc<dyn crate::infra::transfer_store::TransferStore>,
            graph,
            vec![local.clone(), cloud.clone()],
        );

        let fp = |i: usize| FileFingerprint {
            byte_digest: Some(ByteDigest::Djb2(format!("h-{i}"))),
            content_digest: Some(ContentDigest(format!("h-{i}"))),
            meta_digest: None,
            size: 2048,
            modified_at: None,
        };

        // sync1: local + cloud の両方から同一5ファイルをDiscovered
        let mut deltas1 = Vec::new();
        for i in 0..5 {
            for origin in [&local, &cloud] {
                deltas1.push(TopologyDelta::Discovered(DiscoveredFile {
                    relative_path: format!("output/multi-{i:04}.png"),
                    file_type: FileType::Image,
                    fingerprint: fp(i),
                    origin: origin.clone(),
                    embedded_id: None,
                }));
            }
        }
        topo.sync(&deltas1).await.expect("sync1");

        let deleted1 = TopologyFileStore::list_deleted(&*store)
            .await
            .expect("list_deleted");
        assert_eq!(deleted1.len(), 0, "sync1: no deleted TFs");

        // sync2: 同じデルタ
        topo.sync(&deltas1).await.expect("sync2");

        let deleted2 = TopologyFileStore::list_deleted(&*store)
            .await
            .expect("list_deleted");
        assert_eq!(
            deleted2.len(),
            0,
            "sync2: no deleted TFs, got {} — paths: {:?}",
            deleted2.len(),
            deleted2
                .iter()
                .map(|t| t.relative_path())
                .collect::<Vec<_>>()
        );
    }
}
