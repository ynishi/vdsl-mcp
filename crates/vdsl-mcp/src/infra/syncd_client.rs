//! SyncdClient — mcp 側から syncd プロセスへの HTTP クライアント。
//!
//! syncd の各エンドポイントに対する操作をカプセル化する。
//! probe / delegate_sync / delegate_sync_route / delegate_poll /
//! delegate_cancel / delegate_delete / delegate_restore
//!
//! 全メソッドは `anyhow::Result<T>` を返す。
//! 呼び出し元の mcp tool 層で `McpError` に変換すること。

use std::time::Duration;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::infra::config::SyncdConfig;
use crate::infra::syncd_token;

// =============================================================================
// Request / Response 型
// =============================================================================

/// POST /v1/sync のレスポンス。
#[derive(Debug, Deserialize)]
pub struct SyncTaskResponse {
    pub task_id: String,
}

/// POST /v1/sync_route のリクエスト。
#[derive(Debug, Serialize)]
pub struct SyncRouteRequest {
    pub src: String,
    pub dest: String,
}

/// POST /v1/delete のリクエスト。
#[derive(Debug, Serialize)]
pub struct DeleteRequest {
    pub path: String,
}

/// POST /v1/delete のレスポンス。
#[derive(Debug, Deserialize)]
pub struct DeleteResponse {
    pub created: u64,
}

/// POST /v1/restore のリクエスト。
#[derive(Debug, Serialize)]
pub struct RestoreRequest {
    pub path: String,
    pub revision: String,
}

/// GET /v1/tasks/{id} のレスポンス。
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TaskStatusResponse {
    pub id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

/// POST /v1/tasks/{id}/cancel のレスポンス。
#[derive(Debug, Deserialize)]
pub struct CancelResponse {
    pub ok: bool,
}

/// GET /healthz のレスポンス。
#[derive(Debug, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    /// syncd が起動時に読んだ pod_id。frontend が pod mismatch 検知に使う。
    #[serde(default)]
    pub pod_id: Option<String>,
}

// =============================================================================
// SyncdClient
// =============================================================================

/// mcp 側から syncd HTTP サーバーへの操作クライアント。
///
/// `from_config` で構築し、`probe()` で生存確認、
/// `delegate_*` メソッドで各操作を委譲する。
#[derive(Clone)]
pub struct SyncdClient {
    base_url: String,
    /// reqwest::Client は内部で Arc を使用しており Clone が安価。
    http: reqwest::Client,
    /// Bearer token — 起動時に `cfg.token_file` から読み込む。
    /// 未設定 (None) の場合は Authorization header を付与しない。
    token: Option<String>,
    /// token 再読込用に保持。
    token_file: std::path::PathBuf,
}

impl SyncdClient {
    /// SyncdConfig から SyncdClient を構築する。
    ///
    /// token file が存在すれば読み込み、全リクエストに `Authorization: Bearer` を付与する。
    /// 存在しない場合は probe だけを期待した状態で構築する (syncd 未起動時の探索用)。
    pub fn from_config(cfg: &SyncdConfig) -> anyhow::Result<Self> {
        // timeout は個別リクエストで設定するため、ここではデフォルト (no timeout) のまま。
        let http = reqwest::Client::builder()
            .build()
            .context("reqwest::Client build failed — TLS library initialization error")?;
        let token = syncd_token::read_only(&cfg.token_file).ok().flatten();
        Ok(Self {
            base_url: format!("http://127.0.0.1:{}", cfg.port),
            http,
            token,
            token_file: cfg.token_file.clone(),
        })
    }

    /// 認証付き GET ビルダ。token が無ければ auth header なし (`/healthz` 用)。
    fn http_get(&self, url: &str) -> reqwest::RequestBuilder {
        let mut b = self.http.get(url);
        if let Some(t) = self.token.as_deref() {
            b = b.bearer_auth(t);
        }
        b
    }

    /// 認証付き POST ビルダ。
    fn http_post(&self, url: &str) -> reqwest::RequestBuilder {
        let mut b = self.http.post(url);
        if let Some(t) = self.token.as_deref() {
            b = b.bearer_auth(t);
        }
        b
    }

    /// syncd 起動直後に token file を書いたあと、クライアント側でも再読込する。
    pub fn refresh_token(&mut self) {
        self.token = syncd_token::read_only(&self.token_file).ok().flatten();
    }

    /// 非 2xx レスポンスを詳細なエラー (status + body snippet) に変換する。
    ///
    /// `reqwest::Response::error_for_status` は body を捨てるため、syncd が返す
    /// `ApiError` JSON (例: `sync busy: ...`) が呼び出し側に届かない。
    /// このヘルパは body 先頭 512 バイトまでを取り込んでエラー文に含める。
    async fn check_ok(resp: reqwest::Response, label: &str) -> anyhow::Result<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<body read failed: {e}>"));
        let snippet: String = body.chars().take(512).collect();
        anyhow::bail!("{label}: HTTP {status} — {snippet}")
    }

    /// syncd の生存を確認する。
    ///
    /// `GET /healthz` に最大 300ms timeout で問い合わせ、
    /// 200 OK が返れば `true`、それ以外 (ConnectionRefused, timeout 等) は `false`。
    pub async fn probe(&self) -> bool {
        self.fetch_health().await.is_some()
    }

    /// `/healthz` の body を取得する。pod_id mismatch 検知用。
    pub async fn fetch_health(&self) -> Option<HealthResponse> {
        let url = format!("{}/healthz", self.base_url);
        let resp = self
            .http
            .get(&url)
            .timeout(Duration::from_millis(300))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<HealthResponse>().await.ok()
    }

    /// `POST /v1/sync` — 全体 sync を syncd に委譲する。
    pub async fn delegate_sync(&self) -> anyhow::Result<SyncTaskResponse> {
        let url = format!("{}/v1/sync", self.base_url);
        let raw = self
            .http_post(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .context("POST /v1/sync failed")?;
        let resp = Self::check_ok(raw, "POST /v1/sync").await?;
        resp.json::<SyncTaskResponse>()
            .await
            .context("POST /v1/sync: failed to parse response")
    }

    /// `POST /v1/sync_route` — route sync を syncd に委譲する。
    pub async fn delegate_sync_route(
        &self,
        src: &str,
        dest: &str,
    ) -> anyhow::Result<SyncTaskResponse> {
        let url = format!("{}/v1/sync_route", self.base_url);
        let body = SyncRouteRequest {
            src: src.to_string(),
            dest: dest.to_string(),
        };
        let raw = self
            .http_post(&url)
            .timeout(Duration::from_secs(10))
            .json(&body)
            .send()
            .await
            .context("POST /v1/sync_route failed")?;
        let resp = Self::check_ok(raw, "POST /v1/sync_route").await?;
        resp.json::<SyncTaskResponse>()
            .await
            .context("POST /v1/sync_route: failed to parse response")
    }

    /// `GET /v1/tasks/{task_id}` — タスクステータスを poll する。
    pub async fn delegate_poll(&self, task_id: &str) -> anyhow::Result<TaskStatusResponse> {
        let url = format!("{}/v1/tasks/{}", self.base_url, task_id);
        let raw = self
            .http_get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .context("GET /v1/tasks/{id} failed")?;
        let resp = Self::check_ok(raw, "GET /v1/tasks/{id}").await?;
        resp.json::<TaskStatusResponse>()
            .await
            .context("GET /v1/tasks/{id}: failed to parse response")
    }

    /// `POST /v1/tasks/{task_id}/cancel` — タスクをキャンセルする。
    pub async fn delegate_cancel(&self, task_id: &str) -> anyhow::Result<bool> {
        let url = format!("{}/v1/tasks/{}/cancel", self.base_url, task_id);
        let raw = self
            .http_post(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .context("POST /v1/tasks/{id}/cancel failed")?;
        let resp = Self::check_ok(raw, "POST /v1/tasks/{id}/cancel").await?;
        let cancel_resp = resp
            .json::<CancelResponse>()
            .await
            .context("POST /v1/tasks/{id}/cancel: failed to parse response")?;
        Ok(cancel_resp.ok)
    }

    /// `POST /v1/delete` — ファイル削除マークを syncd に委譲する。
    pub async fn delegate_delete(&self, path: &str) -> anyhow::Result<u64> {
        let url = format!("{}/v1/delete", self.base_url);
        let body = DeleteRequest {
            path: path.to_string(),
        };
        let raw = self
            .http_post(&url)
            .timeout(Duration::from_secs(10))
            .json(&body)
            .send()
            .await
            .context("POST /v1/delete failed")?;
        let resp = Self::check_ok(raw, "POST /v1/delete").await?;
        let del_resp = resp
            .json::<DeleteResponse>()
            .await
            .context("POST /v1/delete: failed to parse response")?;
        Ok(del_resp.created)
    }

    /// `POST /v1/restore` — ファイルリストアを syncd に委譲する。
    pub async fn delegate_restore(&self, path: &str, revision: &str) -> anyhow::Result<()> {
        let url = format!("{}/v1/restore", self.base_url);
        let body = RestoreRequest {
            path: path.to_string(),
            revision: revision.to_string(),
        };
        let raw = self
            .http_post(&url)
            .timeout(Duration::from_secs(30))
            .json(&body)
            .send()
            .await
            .context("POST /v1/restore failed")?;
        Self::check_ok(raw, "POST /v1/restore").await?;
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::config::SyncdConfig;
    use std::path::PathBuf;

    fn test_config(port: u16) -> SyncdConfig {
        SyncdConfig {
            port,
            pid_file: PathBuf::from("/tmp/test_syncd.pid"),
            token_file: PathBuf::from("/tmp/test_syncd.token"),
            work_dir: None,
            debounce_ms: 500,
            log_level: "info".to_string(),
        }
    }

    /// probe() が閉塞ポートに対して false を返すことを確認する。
    #[tokio::test]
    async fn probe_returns_false_when_not_running() {
        // ポート 19999 は通常使われていないため、syncd は起動していない前提。
        let cfg = test_config(19999);
        let client = SyncdClient::from_config(&cfg).expect("client build should succeed");
        let result = client.probe().await;
        assert!(
            !result,
            "probe should return false when syncd is not running"
        );
    }

    #[test]
    fn from_config_constructs_correct_base_url() {
        let cfg = test_config(7823);
        let client = SyncdClient::from_config(&cfg).expect("client build should succeed");
        assert_eq!(client.base_url, "http://127.0.0.1:7823");
    }
}
