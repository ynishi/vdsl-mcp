//! E2E test: local → pod, local → cloud three-location synchronization.
//!
//! Verifies the complete sync lifecycle with the Store API:
//!
//! 1. **put** — register a local file, remotes become `pending`
//! 2. **sync_route(local, pod)** — transfer to pod, pod becomes `present`
//! 3. **sync_route(local, cloud)** — transfer to cloud, cloud becomes `present`
//! 4. **status** — verify all three locations show `present`
//! 5. **file modification** — re-put marks remotes as `pending` again
//! 6. **sync()** — topology-wide transfer to ALL remotes
//! 7. **duplicate detection** — same content at different path is detected
//! 8. **put output + recipe** — generation registration pattern
//! 9. **error recovery** — backend failure → retry via sync()
//!
//! Uses InMemoryBackend (no real network). Runs entirely in-process.
//!
//! ```sh
//! cargo run --example e2e_three_location_sync --features test-utils
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use vdsl_sync::infra::backend::memory::InMemoryBackend;
use vdsl_sync::infra::remote_store::RemoteStore;
use vdsl_sync::infra::sqlite::SqliteSyncStore;
use vdsl_sync::infra::store::RemoteConfig;
use vdsl_sync::{
    FileStore, FileType, LocationId, PresenceState, PutOptions, Store, StoreBuilder, TransferRoute,
    TransferStore,
};

/// Build a Store with pod + cloud routes using InMemoryBackends.
async fn build_store(
    local_root: &std::path::Path,
) -> (Store, Arc<InMemoryBackend>, Arc<InMemoryBackend>) {
    let store = SqliteSyncStore::open_in_memory().await.unwrap();

    let pod_backend = Arc::new(InMemoryBackend::default());
    let cloud_backend = Arc::new(InMemoryBackend::default());

    // Register remotes in store
    for id in &["pod", "cloud"] {
        RemoteStore::register_remote(
            &store,
            &RemoteConfig {
                location_id: LocationId::new(*id).unwrap(),
                backend: "memory".into(),
                config: serde_json::json!({}),
                created_at: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    }

    let store = Arc::new(store);
    let db = StoreBuilder::new(
        store.clone() as Arc<dyn FileStore>,
        store.clone() as Arc<dyn TransferStore>,
        store.clone() as Arc<dyn RemoteStore>,
    )
    .route(TransferRoute::new(
        LocationId::local(),
        LocationId::new("pod").unwrap(),
        local_root.to_path_buf(),
        PathBuf::from("workspace/comfyui/output"),
        Box::new(Arc::clone(&pod_backend)),
    ))
    .route(TransferRoute::new(
        LocationId::local(),
        LocationId::new("cloud").unwrap(),
        local_root.to_path_buf(),
        PathBuf::from("vdsl/output"),
        Box::new(Arc::clone(&cloud_backend)),
    ))
    .build()
    .await
    .unwrap();

    (db, pod_backend, cloud_backend)
}

/// Build a minimal valid PNG with given IDAT data and optional tEXt chunks.
fn build_test_png(idat_data: &[u8], text_chunks: &[(&str, &str)]) -> Vec<u8> {
    let mut buf = Vec::new();
    // PNG signature
    buf.extend_from_slice(&[137, 80, 78, 71, 13, 10, 26, 10]);
    // IHDR (1x1 RGB)
    let ihdr = [0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0, 0, 0];
    buf.extend_from_slice(&(ihdr.len() as u32).to_be_bytes());
    buf.extend_from_slice(b"IHDR");
    buf.extend_from_slice(&ihdr);
    buf.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder
                                          // tEXt chunks
    for (keyword, text) in text_chunks {
        let data: Vec<u8> = [keyword.as_bytes(), &[0], text.as_bytes()].concat();
        buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
        buf.extend_from_slice(b"tEXt");
        buf.extend_from_slice(&data);
        buf.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder
    }
    // IDAT
    buf.extend_from_slice(&(idat_data.len() as u32).to_be_bytes());
    buf.extend_from_slice(b"IDAT");
    buf.extend_from_slice(idat_data);
    buf.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder
                                          // IEND
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(b"IEND");
    buf.extend_from_slice(&[0, 0, 0, 0]); // CRC placeholder
    buf
}

/// Assert a location's presence state via FileView.
fn assert_presence(view: &vdsl_sync::FileView, loc: &str, expected: PresenceState) {
    let loc_id = if loc == "local" {
        LocationId::local()
    } else {
        LocationId::new(loc).unwrap()
    };
    let actual = view.presence_state(&loc_id);
    assert_eq!(
        actual,
        Some(expected),
        "expected {loc}={expected}, got {loc}={actual:?} for '{}'",
        view.file.relative_path()
    );
}

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let (db, pod_backend, _cloud_backend) = build_store(dir.path()).await;

    let pod_id = LocationId::new("pod").unwrap();
    let cloud_id = LocationId::new("cloud").unwrap();

    // =========================================================================
    // 1. put — register a local file
    // =========================================================================
    let img_path = dir.path().join("output/gen-001.png");
    std::fs::create_dir_all(img_path.parent().unwrap()).unwrap();
    std::fs::write(&img_path, build_test_png(b"PIXEL_DATA_V1", &[])).unwrap();

    let result = db
        .put(
            "output/gen-001.png",
            FileType::Image,
            PutOptions {
                embedded_id: Some("gen-001".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(!result.is_duplicate, "first file should not be duplicate");
    assert_eq!(result.file.relative_path(), "output/gen-001.png");
    assert_eq!(result.transfers_created, 2); // local→pod, local→cloud

    let view = db.get("output/gen-001.png").await.unwrap().unwrap();
    assert_presence(&view, "local", PresenceState::Present);
    assert_presence(&view, "pod", PresenceState::Pending);
    assert_presence(&view, "cloud", PresenceState::Pending);
    eprintln!("[PASS] 1. put — local=present, pod=pending, cloud=pending");

    // =========================================================================
    // 2. sync_route(local, pod) — transfer to pod only
    // =========================================================================
    let batch = db.sync_route(&LocationId::local(), &pod_id).await.unwrap();
    assert_eq!(batch.transferred, 1, "should transfer 1 file to pod");
    assert_eq!(batch.failed, 0, "no failures expected");

    let view = db.get("output/gen-001.png").await.unwrap().unwrap();
    assert_presence(&view, "local", PresenceState::Present);
    assert_presence(&view, "pod", PresenceState::Present);
    assert_presence(&view, "cloud", PresenceState::Pending);

    // Verify pod backend received correct remote path
    {
        let log = pod_backend.log.lock().await;
        assert_eq!(log.len(), 1);
        match &log[0] {
            vdsl_sync::infra::backend::memory::Op::Push { remote, .. } => {
                assert_eq!(remote, "workspace/comfyui/output/output/gen-001.png");
            }
            other => panic!("expected Push, got {:?}", other),
        }
    }
    eprintln!("[PASS] 2. sync_route(local, pod) — pod=present, cloud still pending");

    // =========================================================================
    // 3. sync_route(local, cloud) — transfer to cloud
    // =========================================================================
    let batch = db
        .sync_route(&LocationId::local(), &cloud_id)
        .await
        .unwrap();
    assert_eq!(batch.transferred, 1);

    let view = db.get("output/gen-001.png").await.unwrap().unwrap();
    assert_presence(&view, "local", PresenceState::Present);
    assert_presence(&view, "pod", PresenceState::Present);
    assert_presence(&view, "cloud", PresenceState::Present);
    eprintln!("[PASS] 3. sync_route(local, cloud) — all three locations present");

    // =========================================================================
    // 4. status — verify aggregated summary
    // =========================================================================
    let summary = db.status().await.unwrap();
    assert_eq!(summary.total_entries, 1);
    let local_summary = summary.locations.get(&LocationId::local()).unwrap();
    assert_eq!(local_summary.present, 1);
    let pod_summary = summary.locations.get(&pod_id).unwrap();
    assert_eq!(pod_summary.present, 1);
    let cloud_summary = summary.locations.get(&cloud_id).unwrap();
    assert_eq!(cloud_summary.present, 1);
    eprintln!("[PASS] 4. status — 1 entry, all locations present");

    // =========================================================================
    // 5. file modification — re-put marks remotes pending
    // =========================================================================
    std::fs::write(&img_path, build_test_png(b"PIXEL_DATA_V2_MODIFIED", &[])).unwrap();

    let result = db
        .put(
            "output/gen-001.png",
            FileType::Image,
            PutOptions {
                embedded_id: Some("gen-001".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(!result.is_duplicate);
    assert_eq!(result.transfers_created, 2); // new transfers for modified file

    let view = db.get("output/gen-001.png").await.unwrap().unwrap();
    assert_presence(&view, "local", PresenceState::Present);
    assert_presence(&view, "pod", PresenceState::Pending);
    assert_presence(&view, "cloud", PresenceState::Pending);
    eprintln!("[PASS] 5. file modification — remotes back to pending");

    // =========================================================================
    // 6. sync() — topology-wide trigger, transfer ALL pending
    // =========================================================================
    let result = db.sync().await.unwrap();
    assert_eq!(
        result.batch.transferred, 2,
        "should transfer to both pod and cloud"
    );
    assert_eq!(result.batch.failed, 0);

    let view = db.get("output/gen-001.png").await.unwrap().unwrap();
    assert_presence(&view, "local", PresenceState::Present);
    assert_presence(&view, "pod", PresenceState::Present);
    assert_presence(&view, "cloud", PresenceState::Present);
    eprintln!("[PASS] 6. sync() — all remotes synced via topology trigger");

    // =========================================================================
    // 7. duplicate detection — same content, different path
    // =========================================================================
    let dup_path = dir.path().join("output/gen-001-copy.png");
    std::fs::copy(&img_path, &dup_path).unwrap();

    let result = db
        .put(
            "output/gen-001-copy.png",
            FileType::Image,
            PutOptions::default(),
        )
        .await
        .unwrap();

    assert!(
        result.is_duplicate,
        "identical file should be detected as duplicate"
    );
    assert_eq!(
        result.duplicate_of.as_deref(),
        Some("output/gen-001.png"),
        "should reference original"
    );
    eprintln!("[PASS] 7. duplicate detection — same content identified");

    // =========================================================================
    // 8. put output + recipe (generation registration pattern)
    // =========================================================================
    let gen_output = dir.path().join("output/gen-002.png");
    let gen_recipe = dir.path().join("output/gen-002_recipe.json");
    std::fs::write(&gen_output, build_test_png(b"GEN002_PIXELS", &[])).unwrap();
    std::fs::write(&gen_recipe, br#"{"prompt":"test"}"#).unwrap();

    let output_result = db
        .put(
            "output/gen-002.png",
            FileType::Image,
            PutOptions {
                embedded_id: Some("gen-002".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let recipe_result = db
        .put(
            "output/gen-002_recipe.json",
            FileType::Asset,
            PutOptions {
                embedded_id: Some("gen-002".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(output_result.file.file_type(), FileType::Image);
    assert_eq!(recipe_result.file.file_type(), FileType::Asset);
    assert_eq!(output_result.file.embedded_id(), Some("gen-002"));
    assert_eq!(recipe_result.file.embedded_id(), Some("gen-002"));
    eprintln!("[PASS] 8. put output + recipe registered");

    // =========================================================================
    // 9. error recovery — backend failure + retry via sync()
    // =========================================================================
    // First, sync all pending files to pod so only gen-003 will be pending
    let batch = db.sync_route(&LocationId::local(), &pod_id).await.unwrap();
    assert_eq!(batch.failed, 0, "pre-cleanup should succeed");

    let err_path = dir.path().join("output/gen-003.png");
    std::fs::write(&err_path, build_test_png(b"GEN003_FAIL_FIRST", &[])).unwrap();

    db.put(
        "output/gen-003.png",
        FileType::Image,
        PutOptions {
            embedded_id: Some("gen-003".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Make pod backend fail on next transfer (only gen-003 is pending for pod)
    *pod_backend.fail_next.lock().await = true;

    let batch = db.sync_route(&LocationId::local(), &pod_id).await.unwrap();
    assert_eq!(batch.failed, 1, "gen-003 should fail");
    assert_eq!(batch.transferred, 0, "nothing should succeed");

    // Verify error is recorded — Failed + Transient → Pending (retryable)
    let view = db.get("output/gen-003.png").await.unwrap().unwrap();
    let pod_presence = view.presence(&pod_id).unwrap();
    assert_eq!(
        pod_presence.state,
        PresenceState::Pending,
        "transient failure within retry limit → Pending"
    );
    assert!(pod_presence.error.is_some(), "error should be recorded");

    // Verify retryable failure appears in pending_entries (not error_entries)
    let summary = db.status().await.unwrap();
    assert!(
        summary
            .pending_entries
            .iter()
            .any(|e| e.dest == pod_id && e.file_id.contains("gen-003")),
        "retryable transient failure should appear as pending"
    );
    assert!(
        summary.error_entries.is_empty(),
        "retryable failure should NOT be in error_entries"
    );

    // Retry via sync() — retries failed transfers, then executes
    // (fail_next was auto-reset by InMemoryBackend)
    let result = db.sync().await.unwrap();
    // gen-003 to pod should be retried + succeed
    // gen-003 to cloud should also be pushed (was still queued)
    assert!(result.batch.transferred >= 1, "retry should succeed");
    assert_eq!(result.batch.failed, 0);

    let view = db.get("output/gen-003.png").await.unwrap().unwrap();
    assert_presence(&view, "pod", PresenceState::Present);
    eprintln!("[PASS] 9. error recovery — failure recorded, retry via sync() succeeds");

    // =========================================================================
    // 10. get() accepts absolute paths
    // =========================================================================
    let abs_view = db.get(img_path.to_str().unwrap()).await.unwrap();
    assert!(abs_view.is_some(), "get() should accept absolute paths");
    eprintln!("[PASS] 10. get() accepts absolute path");

    // =========================================================================
    // Final summary
    // =========================================================================
    let summary = db.status().await.unwrap();
    eprintln!();
    eprintln!("=== Final Status ===");
    eprintln!(
        "Total entries: {}, errors: {}",
        summary.total_entries, summary.total_errors
    );
    for (loc, s) in &summary.locations {
        eprintln!(
            "  {loc}: present={}, pending={}, syncing={}, absent={}",
            s.present, s.pending, s.syncing, s.absent
        );
    }
    eprintln!();
    eprintln!("All 10 E2E scenarios passed.");
}
