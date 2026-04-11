//! `SdkImplBuilder` — 外部crateからの構築用。
//!
//! Location（拠点）を `location()` で登録し、ルートを `connect()` で宣言する。
//! Location からスキャナーが自動導出され、ルートコストは `LocationKind` の
//! 組み合わせから自動推定される。

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use super::SdkImpl;
use crate::application::error::SyncError;
use crate::application::route::{TransferDirection, TransferRoute};
use crate::application::topology_scanner::TopologyScanner;
use crate::application::topology_store::TopologyStore;
use crate::application::transfer_engine::TransferEngine;
use crate::domain::config::SyncConfig;
use crate::domain::graph::{EdgeCost, RouteGraph};
use crate::domain::location::LocationId;
use crate::infra::backend::StorageBackend;
use crate::infra::location::{Location, LocationKind};
use crate::infra::location_file_store::LocationFileStore;
use crate::infra::location_scanner::LocationScanner;
use crate::infra::shell::RemoteShell;
use crate::infra::topology_file_store::TopologyFileStore;
use crate::infra::transfer_store::TransferStore;

/// ルート接続の中間表現。
///
/// `connect()` で登録され、`build()` 時に Location の `file_root()` で
/// TransferRoute に変換される。コストは `LocationKind` の組み合わせから自動推定。
struct PendingRoute {
    src: LocationId,
    dest: LocationId,
    backend: Box<dyn StorageBackend>,
    src_shell: Option<Box<dyn RemoteShell>>,
    direction: TransferDirection,
}

/// SdkImplのビルダー。
///
/// Location（拠点）を `location()` で登録し、ルートを `connect()` で宣言する。
/// Location からスキャナーが自動導出され、ルートコストは `LocationKind` の
/// 組み合わせから自動推定される。
///
/// # 使用例
///
/// ```ignore
/// let sdk = SdkImplBuilder::new(topology_files, location_files, transfer_store)
///     .location(Arc::new(LocalLocation::new(root, hasher)))
///     .location(Arc::new(SshLocation::new(pod_id, pod_root, shell)))
///     .location(Arc::new(CloudLocation::new(cloud_id, cloud_root, backend)))
///     .connect(&local_id, &cloud_id, rclone_backend)
///     .connect_with_shell(&pod_id, &cloud_id, pod_rclone, pod_shell)
///     .connect_pull(&cloud_id, &local_id, rclone_pull)
///     .exclude(".git")
///     .build()?;
/// ```
pub struct SdkImplBuilder {
    topology_files: Arc<dyn TopologyFileStore>,
    location_files: Arc<dyn LocationFileStore>,
    transfer_store: Arc<dyn TransferStore>,
    locations: Vec<Arc<dyn Location>>,
    pending_routes: Vec<PendingRoute>,
    config: Option<SyncConfig>,
    scan_excludes: Vec<glob::Pattern>,
    /// Per-destination archive root: dest LocationId → archive path.
    /// When set, all routes whose dest matches get archive-on-delete.
    archive_roots: HashMap<LocationId, std::path::PathBuf>,
}

impl SdkImplBuilder {
    /// 必須3ストアでビルダーを作成。
    pub fn new(
        topology_files: Arc<dyn TopologyFileStore>,
        location_files: Arc<dyn LocationFileStore>,
        transfer_store: Arc<dyn TransferStore>,
    ) -> Self {
        Self {
            topology_files,
            location_files,
            transfer_store,
            locations: Vec::new(),
            pending_routes: Vec::new(),
            config: None,
            scan_excludes: Vec::new(),
            archive_roots: HashMap::new(),
        }
    }

    /// Enable archive-on-delete for routes targeting `dest`.
    ///
    /// All routes whose destination is `dest` will move deleted files to
    /// `{archive_root}/{ISO8601_ts}/{relative_path}` instead of hard-deleting.
    /// The backend must implement `archive_move` (e.g. RcloneBackend via
    /// `rclone moveto`).
    pub fn archive_route_to(mut self, dest: &LocationId, archive_root: std::path::PathBuf) -> Self {
        self.archive_roots.insert(dest.clone(), archive_root);
        self
    }

    /// Location（拠点）追加。
    ///
    /// Location trait 実装からスキャナーが自動導出される。
    /// 同じLocationIdの二重登録は無視される。
    pub fn location(mut self, loc: Arc<dyn Location>) -> Self {
        if !self.locations.iter().any(|l| l.id() == loc.id()) {
            self.locations.push(loc);
        }
        self
    }

    /// Push方向のルート接続を宣言。
    ///
    /// `src` → `dest` への転送ルートを登録する。
    /// `file_root` は `build()` 時に Location から自動解決される。
    /// コストは `LocationKind` の組み合わせから自動推定される。
    pub fn connect(
        mut self,
        src: &LocationId,
        dest: &LocationId,
        backend: Box<dyn StorageBackend>,
    ) -> Self {
        self.pending_routes.push(PendingRoute {
            src: src.clone(),
            dest: dest.clone(),
            backend,
            src_shell: None,
            direction: TransferDirection::Push,
        });
        self
    }

    /// Push方向 + ソース側Shell付きのルート接続。
    ///
    /// リモートホスト（Pod等）がソースの場合、ソース側でのファイル操作
    /// （存在確認、ハッシュ計算）に `src_shell` を使用する。
    pub fn connect_with_shell(
        mut self,
        src: &LocationId,
        dest: &LocationId,
        backend: Box<dyn StorageBackend>,
        src_shell: Box<dyn RemoteShell>,
    ) -> Self {
        self.pending_routes.push(PendingRoute {
            src: src.clone(),
            dest: dest.clone(),
            backend,
            src_shell: Some(src_shell),
            direction: TransferDirection::Push,
        });
        self
    }

    /// Pull方向のルート接続。
    ///
    /// Cloud → Local, Cloud → Pod のように、リモートからローカル方向への
    /// 転送ルートを登録する。`backend.pull()` が使用される。
    pub fn connect_pull(
        mut self,
        src: &LocationId,
        dest: &LocationId,
        backend: Box<dyn StorageBackend>,
    ) -> Self {
        self.pending_routes.push(PendingRoute {
            src: src.clone(),
            dest: dest.clone(),
            backend,
            src_shell: None,
            direction: TransferDirection::Pull,
        });
        self
    }

    /// SyncConfig設定。
    pub fn config(mut self, config: SyncConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// スキャン除外パターン追加。
    pub fn exclude(mut self, pattern: &str) -> Self {
        match glob::Pattern::new(pattern) {
            Ok(p) => self.scan_excludes.push(p),
            Err(e) => {
                tracing::warn!(pattern = pattern, error = %e, "invalid exclude glob pattern, skipped");
            }
        }
        self
    }

    /// SdkImplを構築。
    ///
    /// 1. Location から Scanner を自動導出
    /// 2. PendingRoute → TransferRoute に変換（file_root 自動解決 + コスト自動推定）
    /// 3. TransferRoute から RouteGraph + TransferEngine を構築
    /// 4. TopologyStore + TopologyScanner を構築
    pub fn build(self) -> Result<SdkImpl, SyncError> {
        let config = self.config.unwrap_or_default();

        // Location map: LocationId → Arc<dyn Location>
        let loc_map: HashMap<LocationId, &Arc<dyn Location>> =
            self.locations.iter().map(|l| (l.id().clone(), l)).collect();

        // Scanner 導出
        let scanners: Vec<Arc<dyn LocationScanner>> =
            self.locations.iter().map(|loc| loc.scanner()).collect();

        let archive_roots = self.archive_roots;

        // PendingRoute → TransferRoute 変換
        let routes: Vec<TransferRoute> = self
            .pending_routes
            .into_iter()
            .filter_map(|pr| {
                let src_loc = loc_map.get(&pr.src)?;
                let dest_loc = loc_map.get(&pr.dest)?;

                let cost = match estimate_route_cost(src_loc.kind(), dest_loc.kind()) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(src = ?src_loc.kind(), dest = ?dest_loc.kind(), error = %e, "skipping route: invalid cost");
                        return None;
                    }
                };

                let archive_root_for_dest = archive_roots.get(&pr.dest).cloned();

                let mut route = TransferRoute::new(
                    pr.src,
                    pr.dest.clone(),
                    src_loc.file_root().to_path_buf(),
                    dest_loc.file_root().to_path_buf(),
                    pr.backend,
                )
                .with_cost(cost.time_per_gb, cost.priority);

                if let Some(archive_root) = archive_root_for_dest {
                    // Archive-on-delete is only meaningful for Push direction:
                    // Pull routes delete from local filesystem and can't archive
                    // to a remote. Silently ignore for Pull.
                    if pr.direction == TransferDirection::Push {
                        route = route.with_archive_root(archive_root);
                    }
                }

                if pr.direction == TransferDirection::Pull {
                    route = route.direction(TransferDirection::Pull);
                }
                if let Some(shell) = pr.src_shell {
                    route = route.with_src_shell(shell);
                }

                Some(route)
            })
            .collect();

        // Location一覧
        let location_ids: Vec<LocationId> =
            self.locations.iter().map(|loc| loc.id().clone()).collect();

        // RouteGraph構築（1回のみ。TopologyStore / TransferEngine で共有）
        let mut graph = RouteGraph::new();
        for r in &routes {
            graph.add_with_cost(
                r.src().clone(),
                r.dest().clone(),
                EdgeCost::new(r.time_per_gb(), r.priority())?,
            );
        }

        let topology = TopologyStore::new(
            self.topology_files.clone(),
            self.location_files.clone(),
            self.transfer_store.clone(),
            graph.clone(),
            location_ids,
        );

        let engine = TransferEngine::new(graph, routes, config.concurrency);

        let scanner = TopologyScanner::new(
            self.topology_files.clone(),
            self.location_files.clone(),
            scanners,
        );

        Ok(SdkImpl {
            scanner,
            topology,
            engine,
            topology_files: self.topology_files,
            location_files: self.location_files,
            transfer_store: self.transfer_store,
            locations: self.locations,
            config,
            scan_excludes: self.scan_excludes,
            progress: StdMutex::new(None),
        })
    }
}

/// LocationKind の組み合わせからルートコストを自動推定する。
///
/// optimal_tree（Dijkstra）がこのコストで最適経路を計算する。
/// 例: Local→Pod(1.0) + Pod→Cloud(2.0) = 3.0 < Local→Cloud(5.0)
/// → Pod経由チェーンが自動的に選択される。MCP層での条件分岐は不要。
fn estimate_route_cost(
    src: LocationKind,
    dest: LocationKind,
) -> Result<EdgeCost, crate::domain::error::DomainError> {
    let (time_per_gb, priority) = match (src, dest) {
        // LAN/SSH: 低コスト（ローカルネットワーク、低レイテンシ）
        (LocationKind::Local, LocationKind::Remote) => (1.0, 10),
        (LocationKind::Remote, LocationKind::Local) => (1.0, 10),

        // DC帯域: 中コスト（データセンター内 or DC→Cloud）
        (LocationKind::Remote, LocationKind::Cloud) => (2.0, 50),
        (LocationKind::Cloud, LocationKind::Remote) => (2.0, 50),

        // WAN: 高コスト（家庭回線アップロード/ダウンロード）
        (LocationKind::Local, LocationKind::Cloud) => (5.0, 100),
        (LocationKind::Cloud, LocationKind::Local) => (5.0, 100),

        // 同種間: 中立（通常は発生しないが安全なフォールバック）
        _ => (1.0, 100),
    };
    EdgeCost::new(time_per_gb, priority)
}
