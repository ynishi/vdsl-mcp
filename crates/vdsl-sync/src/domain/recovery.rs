//! Recovery Strategy — Failed Transfer の復帰判定ドメインロジック。
//!
//! Transfer が Failed になった後の復帰方針を決定する。
//! 判定はドメイン層で行い、実行（dest存在チェック等）はapplication層に委譲。
//!
//! # Strategy 一覧
//!
//! - [`DefaultRecovery`] — 通常sync用。retry → resolve → skip。
//! - [`ForceRecovery`] — force用。retry → resolve → requeue → skip。
//!
//! # 判定フロー
//!
//! ```text
//! FailedContext { kind, dest_exists, is_retryable, is_exhausted }
//!   → Strategy::decide()
//!   → RecoveryAction { Retry | Resolve | Requeue | Skip }
//! ```

use super::retry::RetryPolicy;
use super::transfer::{Transfer, TransferKind};

/// Recovery判定に必要なコンテキスト。
///
/// application層が収集した情報をドメイン層に渡す。
/// Transfer自体への参照は持たない（判定に必要な情報のみ抽出）。
#[derive(Debug, Clone)]
pub struct FailedContext {
    /// 配送の種類（Sync / Delete）。
    pub kind: TransferKind,
    /// dest側にファイルが存在するか。
    /// `None` = 存在チェックが失敗した（ネットワーク障害等）。
    pub dest_exists: Option<bool>,
    /// RetryPolicy に基づくリトライ可能性。
    pub is_retryable: bool,
    /// RetryPolicy に基づく上限到達。
    pub is_exhausted: bool,
}

impl FailedContext {
    /// Transfer + dest存在チェック結果 + RetryPolicy からコンテキストを構築。
    pub fn from_transfer(
        transfer: &Transfer,
        dest_exists: Option<bool>,
        policy: &RetryPolicy,
    ) -> Self {
        Self {
            kind: transfer.kind(),
            dest_exists,
            is_retryable: transfer.is_retryable(policy),
            is_exhausted: transfer.is_exhausted(policy),
        }
    }
}

/// Recovery判定の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// attempt+1 で再試行（Transient + retryable）。
    Retry,
    /// 事後条件が満たされている → Failed → Completed。
    Resolve,
    /// 再実行が必要 → 新しいTransfer(attempt=1)を生成。
    Requeue,
    /// 何もしない（判断不能 or 対象外）。
    Skip,
}

/// Failed Transfer の復帰戦略。
pub trait RecoveryStrategy: Send + Sync {
    /// 判定: このFailed Transferに対してどのアクションを取るか。
    fn decide(&self, ctx: &FailedContext) -> RecoveryAction;
}

/// 通常sync用の復帰戦略。
///
/// - retryable → Retry（attempt+1で再試行）
/// - Delete + exhausted + dest不在 → Resolve
/// - それ以外 → Skip
#[derive(Debug, Default)]
pub struct DefaultRecovery;

impl RecoveryStrategy for DefaultRecovery {
    fn decide(&self, ctx: &FailedContext) -> RecoveryAction {
        if ctx.is_retryable {
            return RecoveryAction::Retry;
        }

        if ctx.kind == TransferKind::Delete && ctx.is_exhausted {
            match ctx.dest_exists {
                Some(false) => return RecoveryAction::Resolve,
                Some(true) | None => return RecoveryAction::Skip,
            }
        }

        RecoveryAction::Skip
    }
}

/// Force用の復帰戦略。
///
/// DefaultRecoveryとの差分:
/// - Delete + exhausted + dest存在 → Requeue（再実行を試みる）
/// - Sync + exhausted + dest不在 → Requeue（再転送を試みる）
#[derive(Debug, Default)]
pub struct ForceRecovery;

impl RecoveryStrategy for ForceRecovery {
    fn decide(&self, ctx: &FailedContext) -> RecoveryAction {
        if ctx.is_retryable {
            return RecoveryAction::Retry;
        }

        if !ctx.is_exhausted {
            return RecoveryAction::Skip;
        }

        match ctx.kind {
            TransferKind::Delete => match ctx.dest_exists {
                Some(false) => RecoveryAction::Resolve,
                Some(true) => RecoveryAction::Requeue,
                None => RecoveryAction::Skip,
            },
            TransferKind::Sync => match ctx.dest_exists {
                Some(false) => RecoveryAction::Requeue,
                Some(true) => RecoveryAction::Skip,
                None => RecoveryAction::Skip,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- FailedContext builders for tests ---

    fn delete_exhausted(dest_exists: Option<bool>) -> FailedContext {
        FailedContext {
            kind: TransferKind::Delete,
            dest_exists,
            is_retryable: false,
            is_exhausted: true,
        }
    }

    fn delete_retryable() -> FailedContext {
        FailedContext {
            kind: TransferKind::Delete,
            dest_exists: Some(true),
            is_retryable: true,
            is_exhausted: false,
        }
    }

    fn sync_exhausted(dest_exists: Option<bool>) -> FailedContext {
        FailedContext {
            kind: TransferKind::Sync,
            dest_exists,
            is_retryable: false,
            is_exhausted: true,
        }
    }

    fn sync_retryable() -> FailedContext {
        FailedContext {
            kind: TransferKind::Sync,
            dest_exists: Some(false),
            is_retryable: true,
            is_exhausted: false,
        }
    }

    // =======================================================================
    // DefaultRecovery
    // =======================================================================

    #[test]
    fn default_retryable_returns_retry() {
        let s = DefaultRecovery;
        assert_eq!(s.decide(&delete_retryable()), RecoveryAction::Retry);
        assert_eq!(s.decide(&sync_retryable()), RecoveryAction::Retry);
    }

    #[test]
    fn default_delete_exhausted_dest_absent_resolves() {
        let s = DefaultRecovery;
        assert_eq!(
            s.decide(&delete_exhausted(Some(false))),
            RecoveryAction::Resolve
        );
    }

    #[test]
    fn default_delete_exhausted_dest_present_skips() {
        let s = DefaultRecovery;
        assert_eq!(
            s.decide(&delete_exhausted(Some(true))),
            RecoveryAction::Skip
        );
    }

    #[test]
    fn default_delete_exhausted_check_failed_skips() {
        let s = DefaultRecovery;
        assert_eq!(s.decide(&delete_exhausted(None)), RecoveryAction::Skip);
    }

    #[test]
    fn default_sync_exhausted_skips() {
        let s = DefaultRecovery;
        assert_eq!(s.decide(&sync_exhausted(Some(false))), RecoveryAction::Skip);
        assert_eq!(s.decide(&sync_exhausted(Some(true))), RecoveryAction::Skip);
    }

    // =======================================================================
    // ForceRecovery
    // =======================================================================

    #[test]
    fn force_retryable_returns_retry() {
        let s = ForceRecovery;
        assert_eq!(s.decide(&delete_retryable()), RecoveryAction::Retry);
        assert_eq!(s.decide(&sync_retryable()), RecoveryAction::Retry);
    }

    #[test]
    fn force_delete_exhausted_dest_absent_resolves() {
        let s = ForceRecovery;
        assert_eq!(
            s.decide(&delete_exhausted(Some(false))),
            RecoveryAction::Resolve
        );
    }

    #[test]
    fn force_delete_exhausted_dest_present_requeues() {
        let s = ForceRecovery;
        assert_eq!(
            s.decide(&delete_exhausted(Some(true))),
            RecoveryAction::Requeue
        );
    }

    #[test]
    fn force_delete_exhausted_check_failed_skips() {
        let s = ForceRecovery;
        assert_eq!(s.decide(&delete_exhausted(None)), RecoveryAction::Skip);
    }

    #[test]
    fn force_sync_exhausted_dest_absent_requeues() {
        let s = ForceRecovery;
        assert_eq!(
            s.decide(&sync_exhausted(Some(false))),
            RecoveryAction::Requeue
        );
    }

    #[test]
    fn force_sync_exhausted_dest_present_skips() {
        let s = ForceRecovery;
        assert_eq!(s.decide(&sync_exhausted(Some(true))), RecoveryAction::Skip);
    }

    #[test]
    fn force_sync_exhausted_check_failed_skips() {
        let s = ForceRecovery;
        assert_eq!(s.decide(&sync_exhausted(None)), RecoveryAction::Skip);
    }
}
