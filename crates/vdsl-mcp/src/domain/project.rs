//! Project scaffold domain logic.
//!
//! Implements idempotent directory / file creation for `vdsl_project_init`.
//! Two templates are supported:
//! - `concept_planned` — kickoff.md + journal.md
//! - `exploration`     — journal.md only (no kickoff)
//!
//! All paths are pure Rust `std::fs` — no async needed for scaffold creation.

use std::path::{Path, PathBuf};

// =============================================================================
// Public types
// =============================================================================

/// Output of a successful `scaffold_project` call.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScaffoldResult {
    pub project_path: PathBuf,
    pub files_created: Vec<PathBuf>,
    pub root_dirs_ensured: Vec<PathBuf>,
}

/// Recognized template names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Template {
    ConceptPlanned,
    Exploration,
}

impl Template {
    fn as_str(self) -> &'static str {
        match self {
            Self::ConceptPlanned => "concept_planned",
            Self::Exploration => "exploration",
        }
    }

    fn progress_template(self) -> &'static str {
        match self {
            Self::ConceptPlanned => {
                "\
1. 企画 / kickoff (axes 確定 → N 案 sketch)\n\
2. sweeps/ で 1 枚ずつ撃つ (Phase 0)\n\
3. 上位案に絞る → Phase 1\n\
4. final/ に昇格\n\
5. 仕上げ・catalog_pins 付与"
            }
            Self::Exploration => {
                "\
1. refs/ に参考素材を貼る\n\
2. sweeps/ で雑多に撃つ\n\
3. journal.md に観察を追記\n\
4. final/ に昇格"
            }
        }
    }
}

// =============================================================================
// Scaffold errors
// =============================================================================

#[derive(Debug, thiserror::Error)]
pub enum ScaffoldError {
    #[error("invalid project name: {0}")]
    InvalidName(String),

    #[error("invalid template: '{0}' (accepted: concept_planned, exploration)")]
    InvalidTemplate(String),

    #[error("project '{0}' already exists (pass overwrite=true to force)")]
    AlreadyExists(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("home directory not found")]
    NoHomeDir,
}

// =============================================================================
// Root path resolution
// =============================================================================

/// Resolve the projects root directory.
///
/// Priority:
/// 1. `req_root` parameter (explicit)
/// 2. `VDSL_WORK_DIR` env + `/projects`
/// 3. `~/projects/vdsl-work/vdsl/projects`
pub fn resolve_projects_root(req_root: Option<&str>) -> Result<PathBuf, ScaffoldError> {
    if let Some(r) = req_root {
        return Ok(PathBuf::from(r));
    }
    if let Ok(env_dir) = std::env::var("VDSL_WORK_DIR") {
        return Ok(PathBuf::from(env_dir).join("projects"));
    }
    let home = dirs::home_dir().ok_or(ScaffoldError::NoHomeDir)?;
    Ok(home.join("projects/vdsl-work/vdsl/projects"))
}

// =============================================================================
// Name validation
// =============================================================================

fn validate_name(name: &str) -> Result<(), ScaffoldError> {
    if name.is_empty() {
        return Err(ScaffoldError::InvalidName(
            "name must not be empty".to_string(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(ScaffoldError::InvalidName(format!(
            "'{name}' contains invalid characters (only [a-zA-Z0-9_-] allowed)"
        )));
    }
    Ok(())
}

// =============================================================================
// Template content helpers
// =============================================================================

fn today_str() -> String {
    let now = chrono::Local::now();
    now.format("%Y-%m-%d").to_string()
}

fn today_compact() -> String {
    let now = chrono::Local::now();
    now.format("%y%m%d").to_string()
}

fn frontmatter(project: &str, class: &str) -> String {
    let date = today_str();
    format!(
        "---\nclass: {class}\nproject: {project}\ncreated: {date}\nupdated: {date}\ntopics: []\ncatalog_pins: \"\"\nstatus: open\n---\n"
    )
}

fn readme_content(project: &str, template: Template) -> String {
    let tmpl_name = template.as_str();
    let progress = template.progress_template();
    format!(
        "# {project}\n\n\
<コンセプト 1 行 placeholder — user 後で書き換え>\n\n\
## 方針 (template={tmpl_name})\n\n\
- 参考 project は **インスピレーション・技術参照のみ**。設計や prompt はコピペしない\n\
- <ねらう絵の方向性 placeholder>\n\n\
## ディレクトリ\n\n\
| dir | 役割 |\n\
|---|---|\n\
| `refs/` | 参考画像 (path / URL) と pngmetagrep 抽出結果 |\n\
| `notes/` | journal / kickoff (frontmatter 付き) |\n\
| `sweeps/` | 技術検証 Lua |\n\
| `final/` | sweeps から昇格した本番 Lua |\n\n\
## 進め方 (template={tmpl_name})\n\
{progress}\n"
    )
}

fn refs_link_content(project: &str) -> String {
    format!(
        "# {project} refs\n\n\
外部参考 (URL / 画像 path) を貼る。pngmetagrep 抽出結果を入れたら下に追記。\n\n\
## URLs\n\n\
## Images\n\n\
## pngmetagrep notes\n"
    )
}

fn kickoff_content(project: &str) -> String {
    let fm = frontmatter(project, "journal");
    format!(
        "{fm}\n\
# {project} kickoff\n\n\
## 出発点\n\
(時期 / 既存資産取り扱い / 判断基準)\n\n\
## 軸 (3 layer)\n\
| layer | 中身 |\n\
|---|---|\n\
| **空気軸** | (湿度 / 体温 / etc) |\n\
| **季節軸** | (時期固有要素) |\n\
| **時代軸** | (color trend / 美容 trend / culture) |\n\n\
## 引き継ぐ過去資産\n\
(過去 sweeps / final / cp datasheet からの引き継ぎ)\n\n\
## 捨てる / 棚上げ\n\
(明示的な棄却項目)\n\n\
## 候補 N 案 (Phase 0 sketch 対象)\n\
| # | 組合せ | 狙い |\n\
|---|---|---|\n\
| **A** | | |\n\n\
## 次にやること\n\
- [ ] 過去 output / workspace の survey\n\
- [ ] **N 案を sweeps/ で 1 枚ずつ撃つ**\n\
- [ ] 上位案に絞る\n\
- [ ] Phase 1 へ\n\n\
## 候補モデル / lora (検証時に試す)\n\n\
## メモ\n\
(随時追記)\n"
    )
}

fn journal_content(project: &str) -> String {
    let fm = frontmatter(project, "journal");
    let compact = today_compact();
    format!(
        "{fm}\n\
# {project} journal\n\n\
## Summary\n\
(随時更新。最新の方向性 / 採用 / 棄却 / 開発中の論点)\n\n\
---\n\
## v0 {compact}\n\
(雑多に追記)\n"
    )
}

fn assets_index_content() -> String {
    let date = today_str();
    format!(
        "---\nclass: catalog_index\ntitle: Shared assets index\nlast_entry_n: 0\n---\n\n\
# Shared Assets\n\n\
projects/ 横断で使う雑多素材 (refs / lora / cp 雛形 など) の index。\n\
個別 project の `<project>/refs/` とは別。\n\n\
## Entries\n\
(## N. <asset_name> の形式で追記)\n\n\
_last updated: {date}_\n"
    )
}

// =============================================================================
// Core scaffold function
// =============================================================================

/// Idempotently write `content` to `path`.
/// If the file already exists the call is a no-op (returns `false`).
/// Returns `true` when the file was newly created.
fn write_new_file(path: &Path, content: &str) -> Result<bool, std::io::Error> {
    if path.exists() {
        return Ok(false);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content)?;
    Ok(true)
}

/// Idempotently create directory + `.gitkeep`.
/// Returns `true` when `.gitkeep` was newly written.
fn ensure_dir_with_gitkeep(dir: &Path) -> Result<bool, std::io::Error> {
    std::fs::create_dir_all(dir)?;
    write_new_file(&dir.join(".gitkeep"), "")
}

/// Scaffold a new project.
///
/// # Arguments
/// - `name`      — project slug (validated)
/// - `root`      — projects root directory (created if absent)
/// - `template`  — `"concept_planned"` | `"exploration"` | `None` (→ concept_planned)
/// - `overwrite` — allow writing into an existing `<root>/<name>` dir
pub fn scaffold_project(
    name: &str,
    root: &Path,
    template: Option<&str>,
    overwrite: bool,
) -> Result<ScaffoldResult, ScaffoldError> {
    // --- Validate name ---
    validate_name(name)?;

    // --- Resolve template ---
    let tmpl = match template {
        None | Some("concept_planned") => Template::ConceptPlanned,
        Some("exploration") => Template::Exploration,
        Some(other) => return Err(ScaffoldError::InvalidTemplate(other.to_string())),
    };

    // --- Root-level shared dirs ---
    std::fs::create_dir_all(root)?;
    let profiles_dir = root.join("profiles");
    let catalogs_dir = root.join("catalogs");
    let assets_dir = root.join("assets");

    let mut root_dirs: Vec<PathBuf> = Vec::new();
    for dir in &[&profiles_dir, &catalogs_dir, &assets_dir] {
        std::fs::create_dir_all(dir)?;
        ensure_dir_with_gitkeep(dir)?;
        root_dirs.push(dir.to_path_buf());
    }

    // assets/index.md — idempotent (first time only)
    let assets_index = assets_dir.join("index.md");
    let mut files_created: Vec<PathBuf> = Vec::new();
    if write_new_file(&assets_index, &assets_index_content())? {
        files_created.push(assets_index);
    }

    // --- Project directory ---
    let proj_dir = root.join(name);
    if proj_dir.exists() && !overwrite {
        return Err(ScaffoldError::AlreadyExists(name.to_string()));
    }
    std::fs::create_dir_all(&proj_dir)?;

    // README.md (project root, no frontmatter)
    let readme = proj_dir.join("README.md");
    if write_new_file(&readme, &readme_content(name, tmpl))? {
        files_created.push(readme);
    }

    // notes/
    let notes_dir = proj_dir.join("notes");
    std::fs::create_dir_all(&notes_dir)?;

    match tmpl {
        Template::ConceptPlanned => {
            let kickoff = notes_dir.join("kickoff.md");
            if write_new_file(&kickoff, &kickoff_content(name))? {
                files_created.push(kickoff);
            }
            let journal = notes_dir.join("journal.md");
            if write_new_file(&journal, &journal_content(name))? {
                files_created.push(journal);
            }
        }
        Template::Exploration => {
            let journal = notes_dir.join("journal.md");
            if write_new_file(&journal, &journal_content(name))? {
                files_created.push(journal);
            }
        }
    }

    // refs/
    let refs_dir = proj_dir.join("refs");
    std::fs::create_dir_all(&refs_dir)?;
    let refs_link = refs_dir.join("link.md");
    if write_new_file(&refs_link, &refs_link_content(name))? {
        files_created.push(refs_link);
    }

    // sweeps/ .gitkeep
    let sweeps_dir = proj_dir.join("sweeps");
    if ensure_dir_with_gitkeep(&sweeps_dir)? {
        files_created.push(sweeps_dir.join(".gitkeep"));
    }

    // final/ .gitkeep
    let final_dir = proj_dir.join("final");
    if ensure_dir_with_gitkeep(&final_dir)? {
        files_created.push(final_dir.join(".gitkeep"));
    }

    Ok(ScaffoldResult {
        project_path: proj_dir,
        files_created,
        root_dirs_ensured: root_dirs,
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    // 1. concept_planned: 期待ファイル一覧確認
    #[test]
    fn test_concept_planned_files() {
        let td = setup();
        let root = td.path().join("projects");
        let result =
            scaffold_project("test_proj", &root, Some("concept_planned"), false).expect("scaffold");

        let proj = result.project_path.clone();

        // required files
        assert!(proj.join("README.md").exists());
        assert!(proj.join("notes/kickoff.md").exists());
        assert!(proj.join("notes/journal.md").exists());
        assert!(proj.join("refs/link.md").exists());
        assert!(proj.join("sweeps/.gitkeep").exists());
        assert!(proj.join("final/.gitkeep").exists());

        // all created paths are in files_created
        let created_set: std::collections::HashSet<_> = result.files_created.iter().collect();
        assert!(created_set.contains(&proj.join("README.md")));
        assert!(created_set.contains(&proj.join("notes/kickoff.md")));
        assert!(created_set.contains(&proj.join("notes/journal.md")));
    }

    // 2. exploration: kickoff.md なし、journal.md だけ
    #[test]
    fn test_exploration_files() {
        let td = setup();
        let root = td.path().join("projects");
        let result = scaffold_project("bust_kawaii_run", &root, Some("exploration"), false)
            .expect("scaffold");

        let proj = result.project_path.clone();

        assert!(proj.join("notes/journal.md").exists());
        assert!(!proj.join("notes/kickoff.md").exists());
        assert!(proj.join("README.md").exists());
    }

    // 3. 既存 dir + overwrite=false → エラー
    #[test]
    fn test_overwrite_false_fails() {
        let td = setup();
        let root = td.path().join("projects");
        scaffold_project("my_proj", &root, None, false).expect("first");
        let err = scaffold_project("my_proj", &root, None, false).expect_err("should fail");
        assert!(matches!(err, ScaffoldError::AlreadyExists(_)));
    }

    // 4. 既存 dir + overwrite=true → 成功
    #[test]
    fn test_overwrite_true_succeeds() {
        let td = setup();
        let root = td.path().join("projects");
        scaffold_project("my_proj", &root, None, false).expect("first");
        scaffold_project("my_proj", &root, None, true).expect("overwrite ok");
    }

    // 5. invalid name
    #[test]
    fn test_invalid_name_empty() {
        let td = setup();
        let root = td.path().join("projects");
        let err = scaffold_project("", &root, None, false).expect_err("empty name");
        assert!(matches!(err, ScaffoldError::InvalidName(_)));
    }

    #[test]
    fn test_invalid_name_slash() {
        let td = setup();
        let root = td.path().join("projects");
        let err = scaffold_project("foo/bar", &root, None, false).expect_err("slash in name");
        assert!(matches!(err, ScaffoldError::InvalidName(_)));
    }

    #[test]
    fn test_invalid_name_dotdot() {
        let td = setup();
        let root = td.path().join("projects");
        let err = scaffold_project("..", &root, None, false).expect_err("dotdot name");
        assert!(matches!(err, ScaffoldError::InvalidName(_)));
    }

    #[test]
    fn test_invalid_name_whitespace() {
        let td = setup();
        let root = td.path().join("projects");
        let err = scaffold_project("foo bar", &root, None, false).expect_err("whitespace");
        assert!(matches!(err, ScaffoldError::InvalidName(_)));
    }

    // 6. root の profiles/catalogs/assets/ が idempotent に作られる
    #[test]
    fn test_root_dirs_idempotent() {
        let td = setup();
        let root = td.path().join("projects");

        scaffold_project("proj_a", &root, None, false).expect("first call");
        // 2回目でも panic しない
        scaffold_project("proj_b", &root, None, false).expect("second call");

        assert!(root.join("profiles").exists());
        assert!(root.join("catalogs").exists());
        assert!(root.join("assets").exists());
    }

    // 7. assets/index.md は 1 回目のみ作成、2 回目は no-op (中身保護)
    #[test]
    fn test_assets_index_idempotent() {
        let td = setup();
        let root = td.path().join("projects");

        scaffold_project("proj_a", &root, None, false).expect("first");

        // 中身を書き換え
        let index_path = root.join("assets/index.md");
        std::fs::write(&index_path, "CUSTOM CONTENT").unwrap();

        scaffold_project("proj_b", &root, None, false).expect("second");

        // 中身が保護されている
        let content = std::fs::read_to_string(&index_path).unwrap();
        assert_eq!(content, "CUSTOM CONTENT");
    }

    // 8. frontmatter render: {project} {date} 置換確認
    #[test]
    fn test_frontmatter_render() {
        let td = setup();
        let root = td.path().join("projects");
        scaffold_project("gravure_2606", &root, None, false).expect("scaffold");

        let kickoff = root.join("gravure_2606/notes/kickoff.md");
        let content = std::fs::read_to_string(kickoff).unwrap();

        assert!(content.contains("project: gravure_2606"));
        // date は YYYY-MM-DD 形式
        let today = today_str();
        assert!(content.contains(&format!("created: {today}")));
    }

    // 9. journal.md の冒頭が `---\nclass: journal\n` で始まる
    #[test]
    fn test_journal_frontmatter_starts_correctly() {
        let td = setup();
        let root = td.path().join("projects");
        scaffold_project("test_journal", &root, None, false).expect("scaffold");

        let journal = root.join("test_journal/notes/journal.md");
        let content = std::fs::read_to_string(journal).unwrap();

        assert!(
            content.starts_with("---\nclass: journal\n"),
            "journal.md must start with frontmatter class: journal, got: {:?}",
            &content[..content.len().min(50)]
        );
    }
}
