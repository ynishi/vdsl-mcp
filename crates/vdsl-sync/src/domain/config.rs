//! SyncConfig — 同期動作の設定値オブジェクト。
//!
//! ランタイムで変更されない設定をまとめる。
//! Store / TransferEngine に分散していた定数をConfigに集約。
//!
//! # Serde対応
//!
//! `Serialize` / `Deserialize` を実装しているため、
//! TOML / JSON / 環境変数 等からの読み込みが可能。

use serde::{Deserialize, Serialize};

use super::retry::RetryPolicy;

/// 同期動作の設定。
///
/// 全フィールドにデフォルト値があり、`SyncConfig::default()` で
/// 合理的なデフォルトが得られる。
///
/// # Planned extensions
///
/// Currently holds only `max_attempts` and `concurrency`.
/// The following fields are planned for incremental migration:
/// - `scan_excludes` — glob patterns currently held directly by `Store`
/// - `backoff` — retry interval strategy (fixed / exponential)
/// - `prune_retention` — retention period for completed transfers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// 転送リトライの最大試行回数（初回を含む）。
    /// デフォルト: 3。
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// 1ルートあたりの最大並行転送数。
    /// デフォルト: 8。
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

fn default_max_attempts() -> u32 {
    RetryPolicy::DEFAULT_MAX_ATTEMPTS
}

fn default_concurrency() -> usize {
    8
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            concurrency: default_concurrency(),
        }
    }
}

impl SyncConfig {
    /// RetryPolicy を導出。
    pub fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::new(self.max_attempts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let cfg = SyncConfig::default();
        assert_eq!(cfg.max_attempts, 3);
        assert_eq!(cfg.concurrency, 8);
    }

    #[test]
    fn retry_policy_derived() {
        let cfg = SyncConfig {
            max_attempts: 5,
            ..Default::default()
        };
        assert_eq!(cfg.retry_policy().max_attempts(), 5);
    }

    #[test]
    fn serde_roundtrip() {
        let cfg = SyncConfig {
            max_attempts: 7,
            concurrency: 16,
        };
        let json = serde_json::to_string(&cfg).expect("serialize");
        let restored: SyncConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.max_attempts, 7);
        assert_eq!(restored.concurrency, 16);
    }

    #[test]
    fn serde_defaults_on_missing_fields() {
        let json = "{}";
        let cfg: SyncConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(cfg.max_attempts, 3);
        assert_eq!(cfg.concurrency, 8);
    }

    #[test]
    fn zero_max_attempts_clamped() {
        let cfg = SyncConfig {
            max_attempts: 0,
            ..Default::default()
        };
        // RetryPolicy::new() clamps to 1
        assert_eq!(cfg.retry_policy().max_attempts(), 1);
    }
}
