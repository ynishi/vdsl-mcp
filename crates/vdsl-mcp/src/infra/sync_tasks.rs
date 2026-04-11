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
//!
//! # ロック解放 (RAII)
//!
//! sync ロックは `LockGuard` の `Drop` で解放される。Future が panic しても
//! cancel によって abort されても、タスクスタックの unwind 過程で guard が
//! drop されるため、ロックリークは発生しない。

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{info, warn};
use vdsl_sync::{
    LocationId, ProgressFn, SqliteSyncStore, SyncReport, SyncStoreSdk, TaskId, TaskStatus,
};

/// バックグラウンドタスクエントリ。
struct TaskEntry {
    status: TaskStatus<SyncReport>,
    handle: tokio::task::JoinHandle<()>,
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

/// sync ロックの RAII guard。Drop 時に対応するキーを `active_locks` から除去する。
///
/// spawn された Future に move され、タスクの正常完了 / panic / abort いずれの
/// ケースでも unwind 過程で drop が実行される。これによりロックは確実に解放される。
#[derive(Debug)]
struct LockGuard {
    locks: Arc<std::sync::Mutex<HashSet<SyncLockKey>>>,
    key: Option<SyncLockKey>,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            // std::sync::Mutex: 保持期間は O(1) な set 操作のみで極めて短い。
            // poisoned (他スレッドが panic) の場合も無視して解放を試みる。
            let mut set = match self.locks.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            set.remove(&key);
        }
    }
}

/// interface層のバックグラウンドSync操作管理。
///
/// SyncStoreSdk は `sync()`, `sync_route()` を
/// `async fn → Result` で提供する。本構造体がそれらを
/// `tokio::spawn` でバックグラウンド化し、TaskId/poll で管理する。
pub struct SyncTaskManager {
    tasks: Arc<Mutex<HashMap<TaskId, TaskEntry>>>,
    /// DB永続化用。SyncDb.ensure() で取得した store を毎回セットする。
    /// DB再構築時に差し替わるため RwLock。
    store: Arc<tokio::sync::RwLock<Option<Arc<SqliteSyncStore>>>>,
    /// 初回 recovery 済みフラグ。
    recovered: std::sync::atomic::AtomicBool,
    /// アクティブな sync のロック集合。`LockGuard` の Drop で自動解放される。
    ///
    /// `std::sync::Mutex` を使うのは、Drop (同期 context) からロック解放するため。
    /// 保持時間は set の insert/remove のみで極短なので blocking は実質起きない。
    active_locks: Arc<std::sync::Mutex<HashSet<SyncLockKey>>>,
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
            store: Arc::new(tokio::sync::RwLock::new(None)),
            recovered: std::sync::atomic::AtomicBool::new(false),
            active_locks: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    /// DB store を設定する。初回呼び出し時に stale running タスクを recover する。
    ///
    /// SyncDb.ensure() で取得した store を毎回渡すこと。
    /// DB再構築時は新しい store に差し替わる。
    ///
    /// # Deprecation note
    ///
    /// 新規コードは `set_store_for_syncd` または `set_store_no_recover` を使用すること。
    /// - syncd 起動時: `set_store_for_syncd` (recover を実行)
    /// - mcp fallback 経路: `set_store_no_recover` (recover しない)
    ///
    /// 本メソッドは既存の mcp モード (`Command::Mcp`) の互換性維持のため残している。
    /// mcp モードは syncd プロセスが存在しない状態で起動するため、recover は安全。
    pub async fn set_store(&self, store: Arc<SqliteSyncStore>) {
        self.set_store_for_syncd(store).await;
    }

    /// DB store を設定し、起動時 stale recover を実行する。
    ///
    /// syncd プロセス起動時の専用メソッド。
    /// `AtomicBool` で初回のみ recover が実行されるため、
    /// 複数回呼んでも recover は 1 回のみ。
    ///
    /// mcp と syncd が別プロセスで同時に起動した場合に
    /// 互いのタスクを failed 化する問題を避けるため、
    /// recover の呼び出しはこのメソッドを通じてのみ行う。
    pub async fn set_store_for_syncd(&self, store: Arc<SqliteSyncStore>) {
        // 初回のみ recovery を実行
        if !self.recovered.load(std::sync::atomic::Ordering::Acquire) {
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
            self.recovered
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let mut guard = self.store.write().await;
        *guard = Some(store);
    }

    /// DB store を設定する。recover は実行しない。
    ///
    /// mcp fallback 経路 (syncd spawn に失敗した場合) 専用。
    /// syncd プロセスが既に recover を実行している状態では、
    /// mcp 側が再度 recover すると syncd の running タスクを
    /// failed 化してしまう。このメソッドはその問題を回避する。
    pub async fn set_store_no_recover(&self, store: Arc<SqliteSyncStore>) {
        let mut guard = self.store.write().await;
        *guard = Some(store);
    }

    /// `SyncStoreSdk::sync()` をバックグラウンド実行。TaskId を即座に返す。
    ///
    /// 他の sync（全体 or route）が実行中の場合は `SyncBusyError` を返す。
    pub async fn spawn_sync(&self, sdk: &Arc<dyn SyncStoreSdk>) -> Result<TaskId, SyncBusyError> {
        let guard = self.acquire_lock(SyncLockKey::FullSync)?;
        let sdk_clone = Arc::clone(sdk);
        Ok(self
            .spawn_inner(
                sdk,
                move || async move { sdk_clone.sync().await.map_err(|e| e.to_string()) },
                guard,
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
        let guard = self.acquire_lock(SyncLockKey::Route(dest.as_str().to_string()))?;
        let sdk_clone = Arc::clone(sdk);
        Ok(self
            .spawn_inner(
                sdk,
                move || async move {
                    sdk_clone
                        .sync_route(&src, &dest)
                        .await
                        .map_err(|e| e.to_string())
                },
                guard,
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
        {
            let guard = self.store.read().await;
            if let Some(ref store) = *guard {
                match store.load_task(id).await {
                    Ok(status) => return status,
                    Err(e) => {
                        warn!(task_id = %id, error = %e, "sync_tasks: DB load failed, returning None");
                    }
                }
            }
        }

        None
    }

    /// 実行中タスクをキャンセルする。
    ///
    /// - Pending/Running → abort + Failed("cancelled") に遷移
    /// - Completed/Failed/不明 → false を返す（何もしない）
    ///
    /// abort() は tokio タスクを即座に中断する。rclone 等の外部プロセスは
    /// Drop 実装で kill される。ロックは spawn 時に move した `LockGuard` の
    /// `Drop` 経由で自動解放される (タスクスタック unwind 時)。
    pub async fn cancel(&self, id: &TaskId) -> bool {
        let mut map = self.tasks.lock().await;
        let entry = match map.get_mut(id) {
            Some(e) => e,
            None => return false,
        };

        match &entry.status {
            TaskStatus::Pending | TaskStatus::Running(_) => {
                entry.handle.abort();
                entry.status = TaskStatus::Failed("cancelled by user".to_string());

                // DB 更新
                {
                    let guard = self.store.read().await;
                    if let Some(ref store) = *guard {
                        let _ = store.update_task_failed(id, "cancelled by user").await;
                    }
                }

                info!(task_id = %id, "sync_tasks: task cancelled");
                true
            }
            _ => false,
        }
    }

    /// ロック取得。競合する場合は `SyncBusyError` を返す。
    ///
    /// FullSync は他の全ロックと競合する。
    /// Route(dest) は FullSync および同一 Route(dest) と競合する。
    ///
    /// 成功時は `LockGuard` を返す。guard が drop されるとロックは解放される。
    fn acquire_lock(&self, key: SyncLockKey) -> Result<LockGuard, SyncBusyError> {
        let mut set = match self.active_locks.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };

        match &key {
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
        Ok(LockGuard {
            locks: Arc::clone(&self.active_locks),
            key: Some(key),
        })
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

    async fn spawn_inner<F, Fut>(
        &self,
        sdk: &Arc<dyn SyncStoreSdk>,
        make_future: F,
        lock_guard: LockGuard,
    ) -> TaskId
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<SyncReport, String>> + Send + 'static,
    {
        let id = TaskId::new();
        let tasks = Arc::clone(&self.tasks);
        let id_for_task = id.clone();
        let store = self.store.read().await.clone();

        // Set progress callback on the SDK before spawning.
        let progress_cb = Self::make_progress_callback(&tasks, &store, &id);
        sdk.set_progress_callback(Some(progress_cb));

        // ダミーの JoinHandle を持つ Pending エントリを先に登録。
        let placeholder_handle = tokio::spawn(async {});
        let entry = TaskEntry {
            status: TaskStatus::Pending,
            handle: placeholder_handle,
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
            // `lock_guard` はこのスコープ内で保持され、タスク正常完了・panic・
            // JoinHandle::abort のいずれのケースでも Drop でロックが解放される。
            let _lock_guard = lock_guard;

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
            // _lock_guard が drop されロックが解放される。
        });

        // 実際の handle で上書き
        {
            let mut map = self.tasks.lock().await;
            if let Some(entry) = map.get_mut(&id) {
                entry.handle = handle;
            }
        }

        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_mgr() -> SyncTaskManager {
        SyncTaskManager::new()
    }

    fn lock_count(mgr: &SyncTaskManager) -> usize {
        mgr.active_locks.lock().unwrap().len()
    }

    #[test]
    fn lock_guard_releases_on_drop() {
        let mgr = fresh_mgr();
        {
            let _g = mgr
                .acquire_lock(SyncLockKey::FullSync)
                .expect("first lock should succeed");
            assert_eq!(lock_count(&mgr), 1);
        }
        assert_eq!(lock_count(&mgr), 0, "lock should be released on drop");
    }

    #[test]
    fn lock_guard_releases_on_panic() {
        let mgr = fresh_mgr();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = mgr.acquire_lock(SyncLockKey::FullSync).unwrap();
            assert_eq!(lock_count(&mgr), 1);
            panic!("simulated sync failure");
        }));
        assert!(result.is_err(), "panic expected");
        assert_eq!(
            lock_count(&mgr),
            0,
            "lock must be released even when holder panics"
        );
    }

    #[test]
    fn full_sync_conflicts_with_existing_full_sync() {
        let mgr = fresh_mgr();
        let _g1 = mgr.acquire_lock(SyncLockKey::FullSync).unwrap();
        let err = mgr
            .acquire_lock(SyncLockKey::FullSync)
            .expect_err("second full sync should conflict");
        assert!(err.reason.contains("already running"));
    }

    #[test]
    fn route_sync_allows_disjoint_dests() {
        let mgr = fresh_mgr();
        let _g1 = mgr
            .acquire_lock(SyncLockKey::Route("cloud".into()))
            .unwrap();
        let _g2 = mgr
            .acquire_lock(SyncLockKey::Route("pod".into()))
            .expect("different dest should be allowed");
        assert_eq!(lock_count(&mgr), 2);
    }

    #[test]
    fn route_sync_blocks_same_dest_and_full_sync() {
        let mgr = fresh_mgr();
        let _g1 = mgr
            .acquire_lock(SyncLockKey::Route("cloud".into()))
            .unwrap();
        mgr.acquire_lock(SyncLockKey::Route("cloud".into()))
            .expect_err("same dest should conflict");
        mgr.acquire_lock(SyncLockKey::FullSync)
            .expect_err("full sync should conflict with any route");
    }

    #[test]
    fn full_sync_blocks_route_sync() {
        let mgr = fresh_mgr();
        let _g1 = mgr.acquire_lock(SyncLockKey::FullSync).unwrap();
        mgr.acquire_lock(SyncLockKey::Route("cloud".into()))
            .expect_err("route should conflict while full sync is active");
    }
}
