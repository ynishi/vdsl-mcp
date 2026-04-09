//! SyncDb — Sync用SQLite DBのライフサイクル管理。
//!
//! DBパスの一元管理と、接続の健全性保証を担う。
//! `ensure()` 呼び出し時にDBファイルの存在を検証し、
//! 消失していれば再構築する。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use tokio::sync::RwLock;
use tracing::warn;
use vdsl_sync::SqliteSyncStore;

/// Sync用SQLite DB管理。
///
/// work_dir/.vdsl/sync.db のパスを一元管理し、
/// `ensure()` で接続の取得とファイル存在の保証を行う。
pub struct SyncDb {
    db_path: PathBuf,
    store: RwLock<Option<Arc<SqliteSyncStore>>>,
}

impl SyncDb {
    /// work_dir から SyncDb を構築する。この時点ではDBを開かない。
    pub fn new(work_dir: &Path) -> Self {
        Self {
            db_path: work_dir.join(".vdsl").join("sync.db"),
            store: RwLock::new(None),
        }
    }

    /// DBパスを返す。
    pub fn path(&self) -> &Path {
        &self.db_path
    }

    /// DB接続を返す。未接続 or ファイル消失時は (再)構築する。
    ///
    /// 呼び出し元は毎回これを通してstoreを取得すること。
    /// キャッシュ済みConnectionが健全ならそのまま返す。
    pub async fn ensure(&self) -> anyhow::Result<Arc<SqliteSyncStore>> {
        // Fast path: 既存接続 + ファイル存在
        {
            let guard = self.store.read().await;
            if let Some(ref s) = *guard {
                if self.db_path.exists() {
                    return Ok(Arc::clone(s));
                }
                warn!(path = %self.db_path.display(), "sync DB file missing — rebuilding");
            }
        }

        // Slow path: (再)構築
        let mut guard = self.store.write().await;

        // Double-check: 別タスクが先に構築した可能性
        if let Some(ref s) = *guard {
            if self.db_path.exists() {
                return Ok(Arc::clone(s));
            }
        }

        // ディレクトリ作成 + open
        if let Some(parent) = self.db_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context("failed to create .vdsl dir")?;
        }
        let store = Arc::new(
            SqliteSyncStore::open(&self.db_path)
                .await
                .context("failed to open sync DB")?,
        );
        *guard = Some(Arc::clone(&store));
        Ok(store)
    }
}
