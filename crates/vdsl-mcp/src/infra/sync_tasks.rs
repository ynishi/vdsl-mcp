//! SyncTaskManager — interface層のバックグラウンドタスク管理。
//!
//! SyncStoreSdk（Application層）から実行管理の責務を分離。
//! MCP tool と Lua runtime で共通使用する。
//!
//! # 永続化
//!
//! タスクステータスは SQLite（`sync_tasks` テーブル）に永続化される。
//! インメモリ HashMap はセッション内のキャッシュ + JoinHandle 管理用。
//! `poll()` はインメモリ → DB の順で参照する。
//!
//! # 起動時リカバリ
//!
//! `set_store()` 初回呼び出し時に、DB上の `running` タスクを `failed` に
//! 遷移させる（プロセス異常終了でステータスが更新されなかったゾンビを回収）。
//!
//! # 排他制御
//!
//! 同一宛先への多重sync起動を防止する。
//! - `spawn_sync`（全体sync）: 他のsyncが実行中なら拒否
//! - `spawn_sync_route`（route sync）: 同一destのsyncまたは全体syncが実行中なら拒否
//!
//! タスク完了時にロックは自動解放される。
//!
//! # 競合条件の回避
//!
//! エントリ登録を `tokio::spawn` の前に行い、spawned Future が
//! 先に lock を取得しても `get_mut` が `None` を返さないことを保証する。

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use tokio::sync::{Mutex, OnceCell};
use tracing::{info, warn};
use vdsl_sync::{
    LocationId, ProgressFn, SqliteSyncStore, SyncReport, SyncStoreSdk, TaskId, TaskStatus,
};

/// バックグラウンドタスクエントリ。
struct TaskEntry {
    status: TaskStatus<SyncReport>,
    _handle: tokio::task::JoinHandle<()>,
}

/// sync 排他制御のロックキー。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SyncLockKey {
    /// 全体 sync — 全 dest をロック。
    FullSync,
    /// 特定 route の sync — dest をロック。
    Route(String),
}

impl fmt::Display for SyncLockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FullSync => write!(f, "full_sync"),
            Self::Route(dest) => write!(f, "route(→{dest})"),
        }
    }
}

/// spawn 排他エラー。
#[derive(Debug)]
pub struct SyncBusyError {
    /// 競合しているロックキーの説明。
    pub reason: String,
}

impl fmt::Display for SyncBusyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sync busy: {}", self.reason)
    }
}

/// interface層のバックグラウンドSync操作管理。
///
/// SyncStoreSdk は `sync()`, `sync_route()` を
/// `async fn → Result` で提供する。本構造体がそれらを
/// `tokio::spawn` でバックグラウンド化し、TaskId/poll で管理する。
pub struct SyncTaskManager {
    tasks: Arc<Mutex<HashMap<TaskId, TaskEntry>>>,
    /// DB永続化用。OnceCell で lazy 初期化。
    /// SDK構築時に `set_store()` でセットされる。
    store: OnceCell<Arc<SqliteSyncStore>>,
    /// アクティブな sync のロック集合。タスク完了時に自動解放。
    active_locks: Arc<Mutex<HashSet<SyncLockKey>>>,
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
            store: OnceCell::new(),
            active_locks: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// DB store を設定する。初回呼び出し時に stale running タスクを recover する。
    ///
    /// SDK は lazy 初期化のため、サーバー起動時には store が存在しない。
    /// `ensure_sync_sdk()` で SDK 構築時にこのメソッドを呼ぶ。
    /// 2回目以降の呼び出しは無視される（OnceCell）。
    pub async fn set_store(&self, store: Arc<SqliteSyncStore>) {
        self.store
            .get_or_init(|| async {
                let recovered = store.recover_stale_running().await;
                match recovered {
                    Ok(0) => {}
                    Ok(n) => {
                        info!(recovered = n, "sync_tasks: recovered stale running tasks")
                    }
                    Err(e) => {
                        warn!(error = %e, "sync_tasks: failed to recover stale running tasks")
                    }
                }
                store
            })
            .await;
    }

    /// `SyncStoreSdk::sync()` をバックグラウンド実行。TaskId を即座に返す。
    ///
    /// 他の sync（全体 or route）が実行中の場合は `SyncBusyError` を返す。
    pub async fn spawn_sync(&self, sdk: &Arc<dyn SyncStoreSdk>) -> Result<TaskId, SyncBusyError> {
        let lock_key = SyncLockKey::FullSync;
        self.acquire_lock(&lock_key).await?;

        let sdk_clone = Arc::clone(sdk);
        let locks = Arc::clone(&self.active_locks);
        let key_for_release = lock_key.clone();

        Ok(self
            .spawn_inner(
                sdk,
                move || async move { sdk_clone.sync().await.map_err(|e| e.to_string()) },
                move || {
                    let locks = locks;
                    let key = key_for_release;
                    async move {
                        let mut set = locks.lock().await;
                        set.remove(&key);
                    }
                },
            )
            .await)
    }

    /// `SyncStoreSdk::sync_route()` をバックグラウンド実行。TaskId を即座に返す。
    ///
    /// 同一 dest への sync または全体 sync が実行中の場合は `SyncBusyError` を返す。
    pub async fn spawn_sync_route(
        &self,
        sdk: &Arc<dyn SyncStoreSdk>,
        src: LocationId,
        dest: LocationId,
    ) -> Result<TaskId, SyncBusyError> {
        let lock_key = SyncLockKey::Route(dest.as_str().to_string());
        self.acquire_lock(&lock_key).await?;

        let sdk_clone = Arc::clone(sdk);
        let locks = Arc::clone(&self.active_locks);
        let key_for_release = lock_key.clone();

        Ok(self
            .spawn_inner(
                sdk,
                move || async move {
                    sdk_clone
                        .sync_route(&src, &dest)
                        .await
                        .map_err(|e| e.to_string())
                },
                move || {
                    let locks = locks;
                    let key = key_for_release;
                    async move {
                        let mut set = locks.lock().await;
                        set.remove(&key);
                    }
                },
            )
            .await)
    }

    /// タスクの現在のステータスを取得。
    ///
    /// 1. インメモリ HashMap を参照（セッション内タスク）
    /// 2. なければ DB を参照（前セッションのタスク）
    /// 3. どちらにもなければ None
    pub async fn poll(&self, id: &TaskId) -> Option<TaskStatus<SyncReport>> {
        // 1. インメモリ
        {
            let map = self.tasks.lock().await;
            if let Some(entry) = map.get(id) {
                return Some(entry.status.clone());
            }
        }

        // 2. DB fallback
        if let Some(store) = self.store.get() {
            match store.load_task(id).await {
                Ok(status) => return status,
                Err(e) => {
                    warn!(task_id = %id, error = %e, "sync_tasks: DB load failed, returning None");
                }
            }
        }

        None
    }

    /// ロック取得。競合する場合は `SyncBusyError` を返す。
    ///
    /// FullSync は他の全ロックと競合する。
    /// Route(dest) は FullSync および同一 Route(dest) と競合する。
    async fn acquire_lock(&self, key: &SyncLockKey) -> Result<(), SyncBusyError> {
        let mut set = self.active_locks.lock().await;

        match key {
            SyncLockKey::FullSync => {
                // 全体 sync: 他に何かあれば拒否
                if let Some(existing) = set.iter().next() {
                    return Err(SyncBusyError {
                        reason: format!("full sync requested but {existing} is already running"),
                    });
                }
            }
            SyncLockKey::Route(dest) => {
                // route sync: FullSync または同一 dest と競合
                if set.contains(&SyncLockKey::FullSync) {
                    return Err(SyncBusyError {
                        reason: "full sync is already running".into(),
                    });
                }
                let route_key = SyncLockKey::Route(dest.clone());
                if set.contains(&route_key) {
                    return Err(SyncBusyError {
                        reason: format!("sync to dest '{dest}' is already running"),
                    });
                }
            }
        }

        set.insert(key.clone());
        Ok(())
    }

    /// 内部共通の spawn ロジック。
    ///
    /// 1. TaskId 生成 + Pending エントリ登録（lock内 + DB）
    /// 2. tokio::spawn で非同期実行開始
    /// 3. Future 内で Running → Completed/Failed に遷移（HashMap + DB）
    /// 4. `on_complete` でロック解放
    ///
    /// エントリ登録が spawn より先に完了するため、競合条件は発生しない。
    /// Build a progress callback that updates both in-memory HashMap and DB.
    fn make_progress_callback(
        tasks: &Arc<Mutex<HashMap<TaskId, TaskEntry>>>,
        store: &Option<Arc<SqliteSyncStore>>,
        task_id: &TaskId,
    ) -> ProgressFn {
        let tasks = Arc::clone(tasks);
        let store = store.clone();
        let id = task_id.clone();
        Arc::new(move |phase: &str| {
            let tasks = Arc::clone(&tasks);
            let store = store.clone();
            let id = id.clone();
            let phase = phase.to_string();
            // Fire-and-forget: spawn a task to update async state.
            tokio::spawn(async move {
                {
                    let mut map = tasks.lock().await;
                    if let Some(entry) = map.get_mut(&id) {
                        entry.status = TaskStatus::Running(phase.clone());
                    }
                }
                if let Some(s) = &store {
                    let _ = s.update_task_running(&id, &phase).await;
                }
            });
        })
    }

    async fn spawn_inner<F, Fut, C, CFut>(
        &self,
        sdk: &Arc<dyn SyncStoreSdk>,
        make_future: F,
        on_complete: C,
    ) -> TaskId
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<SyncReport, String>> + Send + 'static,
        C: FnOnce() -> CFut + Send + 'static,
        CFut: std::future::Future<Output = ()> + Send + 'static,
    {
        let id = TaskId::new();
        let tasks = Arc::clone(&self.tasks);
        let id_for_task = id.clone();
        let store = self.store.get().cloned();

        // Set progress callback on the SDK before spawning.
        let progress_cb = Self::make_progress_callback(&tasks, &store, &id);
        sdk.set_progress_callback(Some(progress_cb));

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

        // DB に Pending を永続化
        if let Some(s) = &store {
            if let Err(e) = s.insert_task(&id).await {
                warn!(task_id = %id, error = %e, "sync_tasks: DB insert_task failed");
            }
        }

        let sdk_for_cleanup = Arc::clone(sdk);
        let handle = tokio::spawn(async move {
            // Running に遷移
            {
                let mut map = tasks.lock().await;
                if let Some(entry) = map.get_mut(&id_for_task) {
                    entry.status = TaskStatus::Running(String::new());
                }
            }
            if let Some(s) = &store {
                let _ = s.update_task_running(&id_for_task, "").await;
            }

            let result = make_future().await;

            // Clear progress callback after execution.
            sdk_for_cleanup.set_progress_callback(None);

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
            if let Some(s) = &store {
                match &result {
                    Ok(report) => {
                        let _ = s.update_task_completed(&id_for_task, report).await;
                    }
                    Err(msg) => {
                        let _ = s.update_task_failed(&id_for_task, msg).await;
                    }
                }
            }

            // ロック解放
            on_complete().await;
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
