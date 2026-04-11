//! syncd HTTP サーバー — axum router と handler 群。
//!
//! エンドポイント:
//! - `GET  /healthz`              — 生存確認
//! - `POST /v1/sync`              — 全体 sync トリガ
//! - `POST /v1/sync_route`        — route sync トリガ
//! - `GET  /v1/tasks/:id`         — タスク状態 poll
//! - `POST /v1/tasks/:id/cancel`  — タスクキャンセル
//! - `POST /v1/delete`            — ファイル削除マーク
//! - `POST /v1/restore`           — ファイルリストア

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tracing::warn;
use vdsl_sync::SyncStoreSdk;

use crate::infra::config::SyncdConfig;
use crate::infra::sync_tasks::SyncTaskManager;
use crate::infra::syncd_token;

// =============================================================================
// State
// =============================================================================

/// syncd HTTP server が保持する共有状態。
///
/// `Arc<SyncdState>` として各 handler に渡される。
pub struct SyncdState {
    pub cfg: SyncdConfig,
    pub sdk: Arc<dyn SyncStoreSdk>,
    pub task_mgr: Arc<SyncTaskManager>,
    pub started_at: Instant,
    /// watcher による auto sync が実行中かを示すフラグ。
    /// `trigger_auto_sync` で coalesce 制御に使用する。
    pub auto_sync_running: Arc<AtomicBool>,
    /// auto sync 完了後に追加 run が必要かを示すフラグ。
    /// running 中に新たなイベントが来た場合に立てる。
    pub auto_sync_pending: Arc<AtomicBool>,
    /// Shared-secret bearer token for HTTP auth. Read from `cfg.token_file`
    /// at startup. `/healthz` は auth 例外。
    pub auth_token: String,
    /// 起動時に env `VDSL_SYNCD_POD_ID` から読んだ pod_id。
    /// SDK 構築時に pod Location として登録済み (Bug #4)。
    /// frontend が pod 切替時に mismatch を検知して syncd を再起動するために `/healthz`
    /// で公開する。
    pub pod_id: Option<String>,
}

// =============================================================================
// Router
// =============================================================================

/// axum Router を構築して返す。
///
/// `/healthz` は認証例外。その他の `/v1/*` 経路は `Authorization: Bearer <token>`
/// を必須とする (`cfg.token_file` の値と一致すること)。トークンはループバックの
/// 同 UID プロセスだけが `0600` ファイルを読める前提で配布する。
pub fn router(state: Arc<SyncdState>) -> Router {
    let authed = Router::new()
        .route("/v1/sync", post(post_sync))
        .route("/v1/sync_route", post(post_sync_route))
        .route("/v1/tasks/{id}", get(get_task))
        .route("/v1/tasks/{id}/cancel", post(post_cancel))
        .route("/v1/delete", post(post_delete))
        .route("/v1/restore", post(post_restore))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_token,
        ));

    Router::new()
        .route("/healthz", get(healthz))
        .merge(authed)
        .with_state(state)
}

/// `Authorization: Bearer <token>` を検証する middleware。
///
/// header が無い / 形式不正 / トークン不一致のいずれも `401 Unauthorized`。
/// 比較は constant-time で行う。
async fn require_bearer_token(
    State(state): State<Arc<SyncdState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let header = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v: &HeaderValue| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token = header
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::UNAUTHORIZED)?
        .trim();

    if !syncd_token::constant_time_eq(token, &state.auth_token) {
        warn!("syncd: auth rejected — token mismatch");
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(req).await)
}

// =============================================================================
// Error type
// =============================================================================

/// handler が返す汎用エラー。
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: msg.into(),
        }
    }

    fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: msg.into(),
        }
    }

    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "bad_request",
            message: msg.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct ErrorBody {
            error: ErrorDetail,
        }
        #[derive(Serialize)]
        struct ErrorDetail {
            code: String,
            message: String,
        }
        let body = ErrorBody {
            error: ErrorDetail {
                code: self.code.to_string(),
                message: self.message,
            },
        };
        (self.status, Json(body)).into_response()
    }
}

// =============================================================================
// Request / Response 型
// =============================================================================

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    uptime_sec: u64,
    work_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pod_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct TaskIdResponse {
    task_id: String,
}

#[derive(Debug, Deserialize)]
struct SyncRouteRequest {
    src: String,
    dest: String,
}

#[derive(Debug, Serialize)]
struct TaskStatusResponse {
    id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct CancelResponse {
    ok: bool,
}

#[derive(Debug, Deserialize)]
struct DeleteRequest {
    path: String,
}

#[derive(Debug, Serialize)]
struct DeleteResponse {
    created: usize,
}

#[derive(Debug, Deserialize)]
struct RestoreRequest {
    path: String,
    revision: String,
}

#[derive(Debug, Serialize)]
struct OkResponse {
    ok: bool,
}

// =============================================================================
// Handlers
// =============================================================================

/// `GET /healthz` — 生存確認。
async fn healthz(State(state): State<Arc<SyncdState>>) -> impl IntoResponse {
    let uptime_sec = state.started_at.elapsed().as_secs();
    let work_dir = match state.cfg.resolved_work_dir() {
        Ok(p) => p.display().to_string(),
        Err(_) => "(unknown)".to_string(),
    };
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        uptime_sec,
        work_dir,
        pod_id: state.pod_id.clone(),
    })
}

/// `POST /v1/sync` — 全体 sync をバックグラウンド実行。
async fn post_sync(
    State(state): State<Arc<SyncdState>>,
) -> Result<(StatusCode, Json<TaskIdResponse>), ApiError> {
    let task_id = state
        .task_mgr
        .spawn_sync(&state.sdk)
        .await
        .map_err(|e| ApiError::bad_request(format!("sync busy: {e}")))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(TaskIdResponse {
            task_id: task_id.to_string(),
        }),
    ))
}

/// `POST /v1/sync_route` — route sync をバックグラウンド実行。
async fn post_sync_route(
    State(state): State<Arc<SyncdState>>,
    Json(req): Json<SyncRouteRequest>,
) -> Result<(StatusCode, Json<TaskIdResponse>), ApiError> {
    let src = vdsl_sync::LocationId::new(&req.src)
        .map_err(|e| ApiError::bad_request(format!("invalid src: {e}")))?;
    let dest = vdsl_sync::LocationId::new(&req.dest)
        .map_err(|e| ApiError::bad_request(format!("invalid dest: {e}")))?;

    let task_id = state
        .task_mgr
        .spawn_sync_route(&state.sdk, src, dest)
        .await
        .map_err(|e| ApiError::bad_request(format!("sync busy: {e}")))?;

    Ok((
        StatusCode::ACCEPTED,
        Json(TaskIdResponse {
            task_id: task_id.to_string(),
        }),
    ))
}

/// `GET /v1/tasks/:id` — タスクステータス poll。
async fn get_task(
    State(state): State<Arc<SyncdState>>,
    Path(id): Path<String>,
) -> Result<Json<TaskStatusResponse>, ApiError> {
    let task_id = vdsl_sync::TaskId::parse(&id);
    let status = state.task_mgr.poll(&task_id).await;

    match status {
        None => Err(ApiError::not_found(format!("unknown task_id: {id}"))),
        Some(vdsl_sync::TaskStatus::Pending) => Ok(Json(TaskStatusResponse {
            id,
            status: "pending".to_string(),
            phase: None,
            error: None,
            result: None,
        })),
        Some(vdsl_sync::TaskStatus::Running(phase)) => Ok(Json(TaskStatusResponse {
            id,
            status: "running".to_string(),
            phase: Some(if phase.is_empty() {
                "starting".to_string()
            } else {
                phase
            }),
            error: None,
            result: None,
        })),
        Some(vdsl_sync::TaskStatus::Failed(msg)) => Ok(Json(TaskStatusResponse {
            id,
            status: "failed".to_string(),
            phase: None,
            error: Some(msg),
            result: None,
        })),
        Some(vdsl_sync::TaskStatus::Completed(report)) => {
            let result = serde_json::to_value(&report)
                .map_err(|e| {
                    warn!(error = %e, "syncd: failed to serialize SyncReport");
                    ApiError::internal("failed to serialize task result")
                })
                .ok();
            Ok(Json(TaskStatusResponse {
                id,
                status: "done".to_string(),
                phase: None,
                error: None,
                result,
            }))
        }
    }
}

/// `POST /v1/tasks/:id/cancel` — タスクキャンセル。
async fn post_cancel(
    State(state): State<Arc<SyncdState>>,
    Path(id): Path<String>,
) -> Json<CancelResponse> {
    let task_id = vdsl_sync::TaskId::parse(&id);
    let ok = state.task_mgr.cancel(&task_id).await;
    Json(CancelResponse { ok })
}

/// `POST /v1/delete` — ファイル削除マークを作成。
async fn post_delete(
    State(state): State<Arc<SyncdState>>,
    Json(req): Json<DeleteRequest>,
) -> Result<Json<DeleteResponse>, ApiError> {
    let created = state
        .sdk
        .delete(&req.path)
        .await
        .map_err(|e| ApiError::internal(format!("delete failed: {e}")))?;
    Ok(Json(DeleteResponse { created }))
}

/// `POST /v1/restore` — アーカイブからファイルをリストア。
async fn post_restore(
    State(state): State<Arc<SyncdState>>,
    Json(req): Json<RestoreRequest>,
) -> Result<Json<OkResponse>, ApiError> {
    state
        .sdk
        .restore(&req.path, &req.revision)
        .await
        .map_err(|e| ApiError::internal(format!("restore failed: {e}")))?;
    Ok(Json(OkResponse { ok: true }))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Instant;

    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt as _;
    use vdsl_sync::SyncStoreSdk;

    use crate::infra::config::SyncdConfig;
    use crate::infra::sync_tasks::SyncTaskManager;

    /// テスト用のダミー SDK。全メソッドが即座に Ok を返す。
    struct NoopSdk;

    #[async_trait::async_trait]
    impl SyncStoreSdk for NoopSdk {
        async fn sync(&self) -> Result<vdsl_sync::SyncReport, vdsl_sync::SyncError> {
            Ok(vdsl_sync::SyncReport::default())
        }
        async fn sync_route(
            &self,
            _src: &vdsl_sync::LocationId,
            _dest: &vdsl_sync::LocationId,
        ) -> Result<vdsl_sync::SyncReport, vdsl_sync::SyncError> {
            Ok(vdsl_sync::SyncReport::default())
        }
        async fn put(
            &self,
            _path: &str,
            _file_type: vdsl_sync::FileType,
            _fingerprint: vdsl_sync::FileFingerprint,
            _origin: &vdsl_sync::LocationId,
            _embedded_id: Option<String>,
        ) -> Result<vdsl_sync::PutReport, vdsl_sync::SyncError> {
            Ok(vdsl_sync::PutReport {
                file_id: String::new(),
                is_new: false,
                transfers_created: 0,
            })
        }
        async fn delete(&self, _path: &str) -> Result<usize, vdsl_sync::SyncError> {
            Ok(0)
        }
        async fn restore(&self, _path: &str, _revision: &str) -> Result<(), vdsl_sync::SyncError> {
            Ok(())
        }
        async fn get(
            &self,
            _path: &str,
        ) -> Result<Option<vdsl_sync::TopologyFileView>, vdsl_sync::SyncError> {
            Ok(None)
        }
        async fn list(
            &self,
            _file_type: Option<vdsl_sync::FileType>,
            _limit: Option<usize>,
        ) -> Result<Vec<vdsl_sync::TopologyFileView>, vdsl_sync::SyncError> {
            Ok(vec![])
        }
        async fn status(&self) -> Result<vdsl_sync::SyncSummary, vdsl_sync::SyncError> {
            Ok(vdsl_sync::SyncSummary::default())
        }
        async fn errors(&self) -> Result<Vec<vdsl_sync::ErrorEntry>, vdsl_sync::SyncError> {
            Ok(vec![])
        }
        async fn pending(
            &self,
            _dest: &vdsl_sync::LocationId,
        ) -> Result<Vec<vdsl_sync::PendingEntry>, vdsl_sync::SyncError> {
            Ok(vec![])
        }
        fn locations(&self) -> Vec<vdsl_sync::LocationId> {
            vec![]
        }
        fn all_edges(&self) -> Vec<(vdsl_sync::LocationId, vdsl_sync::LocationId)> {
            vec![]
        }
        fn local_root(&self) -> Option<&std::path::Path> {
            None
        }
    }

    const TEST_TOKEN: &str = "00000000000000000000000000000000";

    fn test_state() -> Arc<SyncdState> {
        let cfg = SyncdConfig {
            port: 7823,
            pid_file: PathBuf::from("/tmp/test_syncd.pid"),
            token_file: PathBuf::from("/tmp/test_syncd.token"),
            work_dir: None,
            debounce_ms: 500,
            log_level: "info".to_string(),
        };
        let sdk: Arc<dyn SyncStoreSdk> = Arc::new(NoopSdk);
        Arc::new(SyncdState {
            cfg,
            sdk,
            task_mgr: Arc::new(SyncTaskManager::new()),
            started_at: Instant::now(),
            auto_sync_running: Arc::new(AtomicBool::new(false)),
            auto_sync_pending: Arc::new(AtomicBool::new(false)),
            auth_token: TEST_TOKEN.to_string(),
            pod_id: None,
        })
    }

    #[tokio::test]
    async fn healthz_returns_200() {
        let state = test_state();
        let app = router(state);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_does_not_require_auth() {
        let state = test_state();
        let app = router(state);
        // No Authorization header
        let req = Request::builder()
            .method(Method::GET)
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn v1_endpoint_without_token_returns_401() {
        let state = test_state();
        let app = router(state);
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/sync")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn v1_endpoint_with_wrong_token_returns_401() {
        let state = test_state();
        let app = router(state);
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/sync")
            .header("authorization", "Bearer deadbeefdeadbeefdeadbeefdeadbeef")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn v1_endpoint_with_correct_token_passes_auth() {
        let state = test_state();
        let app = router(state);
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/sync")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        // NoopSdk returns empty SyncReport → 200
        assert_ne!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "auth should pass when token matches"
        );
    }
}
