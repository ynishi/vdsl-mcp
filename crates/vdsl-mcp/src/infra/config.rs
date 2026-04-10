use serde::Deserialize;
use std::path::{Path, PathBuf};

const DEFAULT_PORT: u16 = 7823;
const DEFAULT_DEBOUNCE_MS: u64 = 500;

/// Application-wide config. Loaded from `~/.vdsl/config.toml` (or `--config` / `VDSL_CONFIG`),
/// with env overrides (`VDSL_SYNCD__*`) and CLI overrides applied on top.
///
/// Sample `~/.vdsl/config.toml`:
/// ```toml
/// [syncd]
/// port = 7823
/// pid_file = "~/.vdsl/syncd.pid"
/// # work_dir = "/Users/you/project"   # 省略時は VDSL_WORK_DIR env → cwd の順で解決
/// debounce_ms = 500
/// log_level = "info"
/// ```
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub syncd: SyncdConfig,
}

/// syncd デーモンの設定。
#[derive(Debug, Clone, Deserialize)]
pub struct SyncdConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_pid_file")]
    pub pid_file: PathBuf,
    /// None の場合は `VDSL_WORK_DIR` env → `std::env::current_dir()` の順で解決する。
    #[serde(default)]
    pub work_dir: Option<PathBuf>,
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

impl Default for SyncdConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            pid_file: default_pid_file(),
            work_dir: None,
            debounce_ms: default_debounce_ms(),
            log_level: default_log_level(),
        }
    }
}

fn default_port() -> u16 {
    DEFAULT_PORT
}

fn default_pid_file() -> PathBuf {
    expand_tilde("~/.vdsl/syncd.pid")
}

fn default_debounce_ms() -> u64 {
    DEFAULT_DEBOUNCE_MS
}

fn default_log_level() -> String {
    "info".to_string()
}

/// CLI からの上書き値。`None` の場合は config / env の値を維持する。
#[derive(Debug, Default)]
pub struct SyncdCliOverrides {
    pub port: Option<u16>,
    pub work_dir: Option<PathBuf>,
    pub pid_file: Option<PathBuf>,
    pub debounce_ms: Option<u64>,
    pub log_level: Option<String>,
}

impl SyncdConfig {
    /// CLI 引数で指定された値を上書きする。`None` フィールドは無視する。
    pub fn merge_cli(mut self, cli: SyncdCliOverrides) -> Self {
        if let Some(v) = cli.port {
            self.port = v;
        }
        if let Some(v) = cli.work_dir {
            self.work_dir = Some(v);
        }
        if let Some(v) = cli.pid_file {
            self.pid_file = v;
        }
        if let Some(v) = cli.debounce_ms {
            self.debounce_ms = v;
        }
        if let Some(v) = cli.log_level {
            self.log_level = v;
        }
        self
    }

    /// work_dir を解決する。優先順位: config.work_dir → VDSL_WORK_DIR env → current_dir。
    pub fn resolved_work_dir(&self) -> anyhow::Result<PathBuf> {
        if let Some(p) = &self.work_dir {
            return Ok(p.clone());
        }
        if let Ok(env_val) = std::env::var("VDSL_WORK_DIR") {
            if !env_val.is_empty() {
                return Ok(PathBuf::from(env_val));
            }
        }
        Ok(std::env::current_dir()?)
    }
}

impl AppConfig {
    /// Config を読み込む。優先順位 (低 → 高):
    /// 1. serde default 値
    /// 2. config file (`explicit_path` > `VDSL_CONFIG` env > `~/.vdsl/config.toml`)
    /// 3. env 変数 (`VDSL_SYNCD__*`, 区切りは `__`)
    /// 4. legacy `VDSL_SYNCD_PORT` (単一 underscore) 互換
    pub fn load(explicit_path: Option<&Path>) -> anyhow::Result<Self> {
        let mut builder = config::Config::builder();

        // config file の解決
        let file_path = if let Some(p) = explicit_path {
            Some(p.to_path_buf())
        } else if let Ok(env_path) = std::env::var("VDSL_CONFIG") {
            if !env_path.is_empty() {
                Some(PathBuf::from(env_path))
            } else {
                None
            }
        } else {
            let default = expand_tilde("~/.vdsl/config.toml");
            default.exists().then_some(default)
        };

        if let Some(path) = file_path {
            builder = builder.add_source(
                config::File::from(path)
                    .required(false)
                    .format(config::FileFormat::Toml),
            );
        }

        // env 変数 (階層区切り `__`)
        builder = builder.add_source(
            config::Environment::with_prefix("VDSL")
                .separator("__")
                .try_parsing(true),
        );

        // legacy `VDSL_SYNCD_PORT` (単一 underscore) 互換
        if let Ok(port) = std::env::var("VDSL_SYNCD_PORT") {
            if !port.is_empty() {
                builder = builder.set_override("syncd.port", port)?;
            }
        }

        // build → デシリアライズ → tilde 展開正規化
        let cfg: AppConfig = builder.build()?.try_deserialize()?;
        cfg.normalized()
    }

    fn normalized(mut self) -> anyhow::Result<Self> {
        self.syncd.pid_file = expand_tilde_path(&self.syncd.pid_file);
        if let Some(wd) = self.syncd.work_dir.take() {
            self.syncd.work_dir = Some(expand_tilde_path(&wd));
        }
        Ok(self)
    }
}

/// `~` で始まるパス文字列を home dir に展開する。home が取得できない場合は `~` を `/tmp` に fallback。
fn expand_tilde(s: &str) -> PathBuf {
    if s.starts_with("~/") || s == "~" {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/tmp"));
        if s == "~" {
            home
        } else {
            home.join(&s[2..])
        }
    } else {
        PathBuf::from(s)
    }
}

/// PathBuf に対して tilde 展開を行う。先頭が `~` のときのみ置換。
fn expand_tilde_path(p: &Path) -> PathBuf {
    match p.to_str() {
        Some(s) => expand_tilde(s),
        None => p.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn load_no_file_returns_defaults() {
        // config ファイルなし + VDSL_CONFIG 未設定環境で default 値が返ること。
        // VDSL_CONFIG を確実に未設定にするため一時削除。
        // 既存 VDSL_SYNCD_PORT が設定されている可能性があるため env を明示的に unset。
        // NOTE: 他テストと並列実行されるため env を変更しない方針で、
        // explicit_path に存在しないパスを渡して file load をスキップさせる。
        let tmp = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        // 空ファイルを渡す → file source は読み込まれるが内容なし → default が適用される。
        // ただし VDSL_SYNCD_PORT / VDSL_SYNCD__* が環境に存在するとテストが壊れる可能性あり。
        // ここでは空ファイルを渡して file source の存在チェックを通す。
        let cfg = AppConfig::load(Some(tmp.path())).expect("load should succeed");
        assert_eq!(cfg.syncd.port, 7823);
        assert_eq!(cfg.syncd.debounce_ms, 500);
        assert_eq!(cfg.syncd.log_level, "info");
        assert!(cfg.syncd.work_dir.is_none());
    }

    #[test]
    fn load_from_toml_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
[syncd]
port = 9000
debounce_ms = 1000
log_level = "debug"
work_dir = "/tmp/test_work"
"#
        )
        .unwrap();
        let cfg = AppConfig::load(Some(tmp.path())).expect("load should succeed");
        assert_eq!(cfg.syncd.port, 9000);
        assert_eq!(cfg.syncd.debounce_ms, 1000);
        assert_eq!(cfg.syncd.log_level, "debug");
        assert_eq!(cfg.syncd.work_dir, Some(PathBuf::from("/tmp/test_work")));
    }

    #[test]
    fn merge_cli_overrides_port() {
        let base = SyncdConfig::default();
        let overrides = SyncdCliOverrides {
            port: Some(8000),
            ..Default::default()
        };
        let merged = base.merge_cli(overrides);
        assert_eq!(merged.port, 8000);
        // 他のフィールドは default のまま
        assert_eq!(merged.debounce_ms, DEFAULT_DEBOUNCE_MS);
    }

    #[test]
    fn merge_cli_none_fields_keep_original() {
        let mut base = SyncdConfig::default();
        base.port = 1234;
        let overrides = SyncdCliOverrides::default(); // 全 None
        let merged = base.merge_cli(overrides);
        assert_eq!(merged.port, 1234);
    }

    #[test]
    fn expand_tilde_replaces_home() {
        let result = expand_tilde("~/.vdsl/syncd.pid");
        // home が取得できるかどうか環境依存だが、少なくともパスに `~` が残らないこと。
        let s = result.to_string_lossy();
        assert!(!s.starts_with('~'), "tilde should be expanded, got: {}", s);
    }

    #[test]
    fn expand_tilde_non_tilde_path_unchanged() {
        let result = expand_tilde("/absolute/path");
        assert_eq!(result, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn resolved_work_dir_returns_explicit() {
        let mut cfg = SyncdConfig::default();
        cfg.work_dir = Some(PathBuf::from("/explicit/dir"));
        let result = cfg.resolved_work_dir().unwrap();
        assert_eq!(result, PathBuf::from("/explicit/dir"));
    }
}
