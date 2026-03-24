//! SyncTaskManager — interface層のバックグラウンドタスク管理。
//!
//! SyncStoreSdk（Application層）から実行管理の責務を分離。
//! MCP tool と Lua runtime で共通使用する。
//!
//! # 競合条件の回避
//!
//! エントリ登録を `tokio::spawn` の前に行い、spawned Future が
//! 先に lock を取得しても `get_mut` が `None` を返さないことを保証する。
//!
//! # Progress reporting
//!
//! `sdk.status()` でDB SELECTベースのサマリーを取得する。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use vdsl_sync::{LocationId, SyncReport, SyncStoreSdk, TaskId, TaskStatus};

/// バックグラウンドタスクエントリ。
struct TaskEntry {
    status: TaskStatus<SyncReport>,
    _handle: tokio::task::JoinHandle<()>,
}

/// interface層のバックグラウンドSync操作管理。
///
/// SyncStoreSdk は `sync()`, `sync_route()` を
/// `async fn → Result` で提供する。本構造体がそれらを
/// `tokio::spawn` でバックグラウンド化し、TaskId/poll で管理する。
pub struct SyncTaskManager {
    tasks: Arc<Mutex<HashMap<TaskId, TaskEntry>>>,
}

impl Default for SyncTaskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncTaskManager {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// `SyncStoreSdk::sync()` をバックグラウンド実行。TaskId を即座に返す。
    pub async fn spawn_sync(&self, sdk: &Arc<dyn SyncStoreSdk>) -> TaskId {
        let sdk = Arc::clone(sdk);
        self.spawn_inner(move || async move { sdk.sync().await.map_err(|e| e.to_string()) })
            .await
    }

    /// `SyncStoreSdk::sync_route()` をバックグラウンド実行。TaskId を即座に返す。
    pub async fn spawn_sync_route(
        &self,
        sdk: &Arc<dyn SyncStoreSdk>,
        src: LocationId,
        dest: LocationId,
    ) -> TaskId {
        let sdk = Arc::clone(sdk);
        self.spawn_inner(move || async move {
            sdk.sync_route(&src, &dest).await.map_err(|e| e.to_string())
        })
        .await
    }

    /// タスクの現在のステータスを取得。不明な TaskId には `None` を返す。
    pub async fn poll(&self, id: &TaskId) -> Option<TaskStatus<SyncReport>> {
        let map = self.tasks.lock().await;
        map.get(id).map(|e| e.status.clone())
    }

    /// 内部共通の spawn ロジック。
    ///
    /// 1. TaskId 生成 + Pending エントリ登録（lock内）
    /// 2. tokio::spawn で非同期実行開始
    /// 3. Future 内で Running → Completed/Failed に遷移
    ///
    /// エントリ登録が spawn より先に完了するため、競合条件は発生しない。
    async fn spawn_inner<F, Fut>(&self, make_future: F) -> TaskId
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<SyncReport, String>> + Send + 'static,
    {
        let id = TaskId::new();
        let tasks = Arc::clone(&self.tasks);
        let id_for_task = id.clone();

        // ダミーの JoinHandle を持つ Pending エントリを先に登録。
        let placeholder_handle = tokio::spawn(async {});
        let entry = TaskEntry {
            status: TaskStatus::Pending,
            _handle: placeholder_handle,
        };

        {
            let mut map = self.tasks.lock().await;
            map.insert(id.clone(), entry);
        }

        let handle = tokio::spawn(async move {
            // Running に遷移
            {
                let mut map = tasks.lock().await;
                if let Some(entry) = map.get_mut(&id_for_task) {
                    entry.status = TaskStatus::Running(String::new());
                }
            }

            let result = make_future().await;

            // Completed/Failed に遷移
            {
                let mut map = tasks.lock().await;
                if let Some(entry) = map.get_mut(&id_for_task) {
                    entry.status = match &result {
                        Ok(val) => TaskStatus::Completed(val.clone()),
                        Err(msg) => TaskStatus::Failed(msg.clone()),
                    };
                }
            }
        });

        // 実際の handle で上書き
        {
            let mut map = self.tasks.lock().await;
            if let Some(entry) = map.get_mut(&id) {
                entry._handle = handle;
            }
        }

        id
    }
}
