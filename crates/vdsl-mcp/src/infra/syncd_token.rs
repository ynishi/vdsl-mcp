//! syncd HTTP auth トークン管理。
//!
//! mcp と syncd の両側が読む共有シークレットを `~/.vdsl/syncd.token` に保持する。
//! syncd 起動時に存在しなければ 128bit 乱数 (uuid v4) を 32 文字 hex で生成し、
//! パーミッション 0600 で書き込む。
//!
//! `token_file` 自体の読み取り権限が loopback HTTP の認可境界になる。

use std::io::Read;
use std::path::Path;

/// トークンの長さ (hex chars). uuid v4 simple = 32 chars = 128 bit。
const TOKEN_HEX_LEN: usize = 32;

/// トークンファイルを読み込む。存在しなければ生成する (0600)。
///
/// 既存ファイルはフォーマット検証のみ行い、内容はそのまま採用する。
pub fn load_or_generate(path: &Path) -> anyhow::Result<String> {
    if let Some(existing) = read_token(path)? {
        return Ok(existing);
    }
    let token = uuid::Uuid::new_v4().as_simple().to_string();
    write_token(path, &token)?;
    Ok(token)
}

/// トークンファイルを読むだけ (mcp 側 = 書き込み禁止)。存在しなければ `None`。
pub fn read_only(path: &Path) -> anyhow::Result<Option<String>> {
    read_token(path)
}

fn read_token(path: &Path) -> anyhow::Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut contents = String::new();
    std::fs::File::open(path)?.read_to_string(&mut contents)?;
    let trimmed = contents.trim().to_string();
    if trimmed.len() < TOKEN_HEX_LEN {
        anyhow::bail!(
            "syncd token file {} is malformed (length {} < {})",
            path.display(),
            trimmed.len(),
            TOKEN_HEX_LEN
        );
    }
    Ok(Some(trimmed))
}

fn write_token(path: &Path, token: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(token.as_bytes())?;
        f.write_all(b"\n")?;
    }

    #[cfg(not(unix))]
    {
        std::fs::write(path, format!("{token}\n"))?;
    }

    Ok(())
}

/// 定時間比較。ローカル loopback でも timing attack を避ける基本的なガード。
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.len() != bb.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in ab.iter().zip(bb.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn generate_then_reload_returns_same() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        let t1 = load_or_generate(&path).unwrap();
        let t2 = load_or_generate(&path).unwrap();
        assert_eq!(t1, t2);
        assert_eq!(t1.len(), TOKEN_HEX_LEN);
    }

    #[test]
    fn read_only_returns_none_when_missing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nope");
        assert!(read_only(&path).unwrap().is_none());
    }

    #[test]
    fn read_only_returns_existing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        let generated = load_or_generate(&path).unwrap();
        let seen = read_only(&path).unwrap();
        assert_eq!(seen, Some(generated));
    }

    #[cfg(unix)]
    #[test]
    fn write_token_sets_0600_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        load_or_generate(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "token file must be 0600, got {mode:o}");
    }

    #[test]
    fn malformed_token_is_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "short\n").unwrap();
        let err = load_or_generate(&path);
        assert!(err.is_err(), "short token should be rejected");
    }

    #[test]
    fn constant_time_eq_basics() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "abcd"));
        assert!(constant_time_eq("", ""));
    }
}
