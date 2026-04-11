//! SdkImpl — SyncStoreSdk の本実装。
//!
//! scan→delta→plan→execute の全パイプラインを内部完結させる。
//! インターフェース層（MCP, Lua）は `Arc<dyn SyncStoreSdk>` 経由でのみ使用する。
//!
//! # 構成
//!
//! ```text
//! SdkImpl
//!   ├── scanner: TopologyScanner  — scan → TopologyDelta[]
//!   ├── topology: TopologyStore   — Apply → Distribute → Route → Transfer作成
//!   ├── engine: TransferEngine    — Transfer実行
//!   ├── transfer_store            — Transfer永続化（execute時に必要）
//!   ├── topology_files            — TopologyFile参照（execute時に必要）
//!   ├── config: SyncConfig        — リトライ/並行数
//!   └── scan_excludes             — globパターン
//! ```
//!
//! # ファイル分割
//!
//! - [`builder`] — `SdkImplBuilder` と `estimate_route_cost`
//! - [`execute`] — BFS実行 / バッチ処理 / 結果永続化（private impl）
//! - [`sync_ops`] — `SyncStoreSdk` trait 実装（private impl）

mod builder;
mod execute;
mod sync_ops;

pub use builder::SdkImplBuilder;

use std::sync::{Arc, Mutex as StdMutex};

use crate::application::topology_scanner::TopologyScanner;
use crate::application::topology_store::TopologyStore;
use crate::application::transfer_engine::TransferEngine;
use crate::domain::config::SyncConfig;
use crate::infra::backend::ProgressFn;
use crate::infra::location::Location;
use crate::infra::location_file_store::LocationFileStore;
use crate::infra::topology_file_store::TopologyFileStore;
use crate::infra::transfer_store::TransferStore;

/// SyncStoreSdkの本実装。
///
/// scan→delta→plan→execute を一貫して実行する。
/// インターフェース層は `Arc<dyn SyncStoreSdk>` として保持する。
pub struct SdkImpl {
    pub(super) scanner: TopologyScanner,
    pub(super) topology: TopologyStore,
    pub(super) engine: TransferEngine,
    pub(super) topology_files: Arc<dyn TopologyFileStore>,
    pub(super) location_files: Arc<dyn LocationFileStore>,
    pub(super) transfer_store: Arc<dyn TransferStore>,
    pub(super) locations: Vec<Arc<dyn Location>>,
    pub(super) config: SyncConfig,
    pub(super) scan_excludes: Vec<glob::Pattern>,
    /// Progress callback for reporting phase/chunk progress.
    pub(super) progress: StdMutex<Option<ProgressFn>>,
}

impl SdkImpl {
    /// Report progress via the stored callback (if set).
    pub(super) fn report_progress(&self, msg: &str) {
        if let Ok(guard) = self.progress.lock() {
            if let Some(cb) = guard.as_ref() {
                cb(msg);
            }
        }
    }
}
