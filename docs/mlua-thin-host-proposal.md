# mlua Thin Host 統合 Proposal

## 設計方針

**Thin Host パターン**: Rust (HOST) は Infra プリミティブのみ提供し、Domain ロジックは全て Lua (CLIENT) に寄せる。

業界ベストプラクティス（ゲームエンジン、Neovim、組み込みシステム）と合致する設計。
Host = プリミティブ提供者、Guest = ドメインロジック担当。

## アーキテクチャ

```
┌─ MCP Layer ──────────────────────────────┐
│  vdsl_run / vdsl_run_script              │
│  vdsl_connect / vdsl_pod_*               │
│  vdsl_generate / vdsl_batch_generate     │
│  vdsl_image_search / vdsl_catalogs       │
│                                           │
│  削除対象:                                │
│    vdsl_repo_query                        │
│    vdsl_repo_stats                        │
│    vdsl_repo_meta_get                     │
│    vdsl_repo_meta_set                     │
│    vdsl_repo_reindex                      │
│  → DB操作は Lua スクリプト経由で実行     │
└───────────────────────────────────────────┘
        ↕
┌─ HOST (Rust) = Infra Provider ───────────┐
│                                           │
│  DB:  db.exec(sql, params)               │
│       db.query(sql, params) -> rows      │
│       db.open(path) -> connection        │
│                                           │
│  FS:  fs.mkdir(path)                     │
│       fs.cp(src, dst)                    │
│       fs.read(path) -> string|nil        │
│       fs.read_binary(path) -> string|nil │
│       fs.write(path, content)            │
│       fs.write_binary(path, content)     │
│       fs.exists(path) -> boolean         │
│       fs.ls(dir) -> string[]             │
│       fs.find(dir, pattern) -> string[]  │
│       fs.sleep(seconds)                  │
│                                           │
│  HTTP: http.get(url, headers) -> string  │
│        http.post_json(url, data, h) -> t │
│        http.upload(url, file, fields, h) │
│        http.download(url, file, h) -> b  │
│                                           │
│  PNG:  png.read(path, keys?) -> table    │
│        png.write(path, chunks)           │
│        ※ pngmetagrep-core + png crate    │
│                                           │
└───────────────────────────────────────────┘
        ↕ mlua DI 注入
┌─ CLIENT (Lua) = Domain + Application ────┐
│                                           │
│  runtime/db.lua                           │
│    Connection Provider 経由で SQL 実行    │
│    Repository 実装:                       │
│      ensure_workspace()                   │
│      save_generation()                    │
│      query_generations()                  │
│      stats()                              │
│      get_meta() / set_meta()             │
│                                           │
│  runtime/fs.lua                           │
│    set_backend(rust_fs) で DI 注入        │
│    Pure Lua fallback (io.open) 維持       │
│                                           │
│  runtime/transport.lua                    │
│    set_backend(rust_http) で DI 注入      │
│    Pure Lua fallback (curl) 維持          │
│                                           │
│  runtime/png.lua (util/ から移動)         │
│    set_backend(rust_png) で DI 注入       │
│    Pure Lua fallback (自前パース) 維持    │
│    read_comfy() 等のドメインロジックは残存│
│                                           │
│  Pipeline / DSL ロジック                   │
│  Batch CLI アプリケーション層              │
│    scripts/repo_query.lua                 │
│    scripts/repo_stats.lua                 │
│    scripts/repo_reindex.lua               │
│                                           │
└───────────────────────────────────────────┘
```

## 設計原則

### 1. Host は物理 I/O のみ

Rust 側が提供するのは `exec`/`query`/`read`/`write`/`get`/`post` レベルのプリミティブ。
PNG chunk 操作 (バイナリパース、CRC32 計算) もパフォーマンス上 Host 側のプリミティブ。
`ensure_workspace` や `save_generation` 等のドメイン操作は Lua 側の責務。

### 2. Pure Lua Fallback 維持

各 runtime モジュールは `set_backend()` で Rust バックエンドを注入可能だが、
デフォルトは Pure Lua 実装 (lsqlite3, io.open, curl) を維持。
これにより `lua` CLI での単体実行・デバッグが引き続き可能。

### 3. DB 二重化の解消

現状の問題: Rust 側 (rusqlite) と Lua 側 (lsqlite3) が同じ `generations.db` を
別々のドライバで読み書きしている。

解決: Repository 実装を Lua 側に一本化。Rust 側の `infra/sqlite.rs` と
MCP の `vdsl_repo_*` ツール群は削除。DB 操作が必要な場面は
`vdsl_run_script` で Lua スクリプトを実行する (Batch パターン)。

## Rust 側 DI 注入の実装イメージ

### DB Connection Provider

```rust
fn inject_db_backend(lua: &Lua) -> mlua::Result<()> {
    let db_backend = lua.create_table()?;

    // Connection を開く
    db_backend.set("open", lua.create_function(|lua, path: String| {
        let conn = rusqlite::Connection::open(&path)
            .map_err(|e| mlua::Error::external(e))?;
        // UserData として Lua に公開
        Ok(lua.create_userdata(DbConnection(Mutex::new(conn)))?)
    })?)?;

    lua.globals().set("_rust_db", db_backend)?;
    Ok(())
}

// DbConnection UserData
impl UserData for DbConnection {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // exec: INSERT/UPDATE/DELETE
        methods.add_method("exec", |_, this, (sql, params): (String, Vec<Value>)| {
            let conn = this.0.lock().unwrap();
            conn.execute(&sql, rusqlite::params_from_iter(params))
                .map_err(|e| mlua::Error::external(e))?;
            Ok(())
        });

        // query: SELECT → Vec<Table>
        methods.add_method("query", |lua, this, (sql, params): (String, Vec<Value>)| {
            let conn = this.0.lock().unwrap();
            // rows → Lua table 変換
            todo!()
        });
    }
}
```

### FS Backend

```rust
fn inject_fs_backend(lua: &Lua) -> mlua::Result<()> {
    let fs_backend = lua.create_table()?;

    fs_backend.set("mkdir", lua.create_function(|_, path: String| {
        std::fs::create_dir_all(&path)
            .map_err(|e| mlua::Error::external(e))?;
        Ok(())
    })?)?;

    fs_backend.set("read", lua.create_function(|_, path: String| {
        match std::fs::read_to_string(&path) {
            Ok(s) => Ok(Some(s)),
            Err(_) => Ok(None),
        }
    })?)?;

    // ... write, exists, ls, find, cp, sleep

    let fs_mod: mlua::Table = lua.load("require('vdsl.runtime.fs')").eval()?;
    fs_mod.call_method::<()>("set_backend", fs_backend)?;
    Ok(())
}
```

### HTTP Backend

```rust
fn inject_http_backend(lua: &Lua) -> mlua::Result<()> {
    let http_backend = lua.create_table()?;

    http_backend.set("get", lua.create_function(|_, (url, headers): (String, Option<Table>)| {
        // reqwest::blocking::get or tokio block_on
        let body = reqwest::blocking::get(&url)
            .map_err(|e| mlua::Error::external(e))?
            .text()
            .map_err(|e| mlua::Error::external(e))?;
        Ok(body)
    })?)?;

    // ... post_json, upload, download

    let transport_mod: mlua::Table = lua.load("require('vdsl.runtime.transport')").eval()?;
    transport_mod.call_method::<()>("set_backend", http_backend)?;
    Ok(())
}
```

### PNG Backend

```rust
fn inject_png_backend(lua: &Lua) -> mlua::Result<()> {
    let png_backend = lua.create_table()?;

    // read: tEXt chunk 読み取り (pngmetagrep-core)
    // keys 省略 or 空 → 全 chunk 返却
    png_backend.set("read", lua.create_function(|lua, (path, keys): (String, Option<Vec<String>>)| {
        let keys = keys.unwrap_or_default();
        let meta = pngmetagrep_core::extract(
            &std::path::Path::new(&path), &keys,
        ).map_err(|e| mlua::Error::external(e))?;

        let result = lua.create_table()?;
        if let Some(meta) = meta {
            for (keyword, value) in &meta.chunks {
                result.set(keyword.as_str(), value.to_string())?;
            }
        }
        Ok(result)
    })?)?;

    // write: tEXt chunk 書き込み (png crate)
    // chunks: { keyword = text, ... }
    png_backend.set("write", lua.create_function(|_, (path, chunks): (String, Table)| {
        // 既存 PNG を読み、tEXt chunk を差し替え/追加して書き戻す
        // png crate の Encoder/Decoder で実装
        todo!()
    })?)?;

    lua.globals().set("_rust_png", png_backend)?;
    Ok(())
}
```

## 段階的実装フェーズ

### Phase 1: FS Backend (最小労力・最大効果)

**Rust 側 (vdsl-mcp)**:
- mlua 依存追加
- `std::fs` ベースの FS backend 実装 (関数 10 個)
- `exec_lua` を外部プロセス起動から mlua in-process 実行に切り替え

**Lua 側 (vdsl)**:
- `init.lua` の直接 I/O 6 箇所を `fs` 経由に切り替え
- `pipeline.lua` の直接 I/O 12 箇所を `fs` 経由に切り替え

**効果**: `os.execute("mkdir")`, `io.open`, `io.popen("ls")`, `io.popen("find")` 全排除

### Phase 2: Transport Backend

**Rust 側**:
- `reqwest` ベースの HTTP backend 実装 (関数 4 個)

**Lua 側**:
- `transport/curl.lua` が丸ごと置換される (4 箇所)

**効果**: `io.popen("curl")`, `os.execute("curl")` 全排除

### Phase 3: DB Connection Provider

**Rust 側**:
- `rusqlite` Connection を UserData として公開
- `infra/sqlite.rs` の Repository 実装を削除
- MCP tools から `vdsl_repo_*` 5 ツールを削除

**Lua 側**:
- `runtime/db.lua` に Connection Provider DI を追加
- Repository ロジック (ensure_workspace, save_generation 等) が Lua 側に集約
- Batch CLI スクリプト作成: `scripts/repo_query.lua`, `scripts/repo_stats.lua` 等

**効果**: DB 二重化解消。lsqlite3 / rusqlite の二重ドライバ問題が消滅

### Phase 4: PNG Backend

PNG chunk 操作はバイナリパース + CRC32 計算であり、Pure Lua では遅い。
`util/png.lua` の 200 行超のバイナリ操作を Rust プリミティブに置き換える。

**Rust 側**:
- `pngmetagrep-core` の `extract()` を mlua バインド (Read)
- `png` crate で tEXt chunk 書き込み実装 (Write)
- 関数 2 個: `png.read(path, keys?)`, `png.write(path, chunks)`

**Lua 側**:
- `util/png.lua` → `runtime/png.lua` に移動
- `set_backend(rust_png)` DI ポイント追加
- CRC32, バイナリパース, チャンク組み立て (約 200 行) が Rust に置換
- `read_comfy()` 等の JSON decode ロジックは Lua 側に残存
- Pure Lua fallback (自前パース) は維持

**効果**:
- `pipeline.lua` の `io.popen("pngmetagrep")` CLI 呼出を排除
- PNG メタデータ操作のパフォーマンス改善
- pngmetagrep CLI への外部プロセス依存がゼロに

## async 統合の注意事項

- `reqwest` は async、mlua の callback 関数は sync コンテキスト
- 選択肢: `reqwest::blocking` を使うか、`tokio::runtime::Handle::current().block_on()` で橋渡し
- Phase 1 (FS) は `std::fs` なので sync で問題なし
- Phase 2 (Transport) で判断が必要

## vdsl_run の変更

現在の `exec_lua` (外部プロセス起動) を mlua in-process 実行に置き換える。
`vdsl_run` の Phase 2 (ComfyUI への送信・ポーリング) は引き続き Rust 側で実行。

```
vdsl_run 実行フロー (変更後):
1. mlua VM 起動 + DI 注入 (fs, http, db, png)
2. Lua スクリプト実行 (compile → workflow JSON 出力)
3. Rust 側で workflow を ComfyUI に送信
4. Rust 側でポーリング・画像ダウンロード
5. Lua 側の Repository に結果を保存 (mlua 経由)
```

## 削除対象 (vdsl-mcp)

| ファイル/コード | 内容 |
|---|---|
| `src/infra/sqlite.rs` | SqliteRepository 実装 (全体) |
| `src/domain/repository.rs` | Repository trait + ドメインモデル (※) |
| `src/interface/mcp.rs` の `vdsl_repo_query` | MCP tool |
| `src/interface/mcp.rs` の `vdsl_repo_stats` | MCP tool |
| `src/interface/mcp.rs` の `vdsl_repo_meta_get` | MCP tool |
| `src/interface/mcp.rs` の `vdsl_repo_meta_set` | MCP tool |
| `src/interface/mcp.rs` の `vdsl_repo_reindex` | MCP tool |
| `src/interface/mcp.rs` の `persist_to_repo()` | vdsl_run 後の保存処理 |

※ `repository.rs` のドメインモデル (`Generation`, `Workspace` 等) は
Lua 側に移行するため Rust 側からは削除。ただし `vdsl_run` の結果表示等で
一部構造体が必要な場合は最小限残す。

## Phase 5: Preflight Lua 移行（TODO）

現状 `vdsl_run compile_only=true` のパスで Rust 側（`domain/models.rs`）が
preflight ロジック（required models 抽出 → object_info 照合 → missing 算出 → レポート）を
実行しているが、これはドメインロジックであり Lua 側の責務。

### 方針

Rust 側は ComfyUI の model catalog（object_info 由来）を取得して Lua に返す
**Registry Backend** のみ提供する。判定・レポート生成は Lua 側で行う。

```
Host (Rust):   registry.available() -> { checkpoints: [...], loras: [...], ... }
Client (Lua):  preflight.extract_all(prompts) + preflight.check(required, available)
```

### 実装ステップ

1. Registry Backend を DI 注入（`_rust_registry` → `vdsl.runtime.registry`）
2. `scripts/preflight.lua` が Registry Backend 経由で available models を取得
3. `vdsl_run compile_only` パスから Rust 側の `extract_required_models` / `check_missing` /
   `format_preflight_report` を削除
4. `domain/models.rs` の preflight 関連コードを削除

### 削除対象

| コード | 内容 |
|---|---|
| `domain/models.rs` の `extract_required_models()` | workflow JSON パース |
| `domain/models.rs` の `check_missing()` | missing 算出 |
| `domain/models.rs` の `format_preflight_report()` | レポート生成 |
| `mcp.rs` の compile_only パス内 preflight 処理 | 3710-3728 行付近 |

## 参考: 業界 Best Practice

| 領域 | Host の責務 | Guest (Lua) の責務 |
|---|---|---|
| ゲームエンジン | レンダリング, 物理, I/O | ゲームプレイロジック, AI |
| Neovim | エディタコア, バッファ, ファイル I/O | プラグインロジック, 設定 |
| 組み込みシステム | ボード API, ランタイム環境 | ビジネスロジック |
| **VDSL** | **FS, HTTP, DB Connection, PNG chunk** | **Pipeline, Repository, DSL** |
