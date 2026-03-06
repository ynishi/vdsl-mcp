//! In-process Lua VM backend using mlua-isle.
//!
//! Replaces the external `lua` process spawning with a thread-isolated
//! mlua VM.  mlua-batteries provides `std.fs` which is bridged to
//! VDSL's `runtime/fs` via `set_backend()`.

#[cfg(feature = "mlua-backend")]
mod inner {
    use mlua::prelude::*;
    use mlua_isle::{Isle, IsleError};
    use rmcp::ErrorData as McpError;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    /// rusqlite Connection wrapped for mlua UserData.
    ///
    /// Exposes `exec(sql, params)` and `query(sql, params)` methods
    /// to Lua as the DB Connection Provider primitive.
    struct DbConnection(Mutex<rusqlite::Connection>);

    /// Extract params from a Lua table with `.n` field (table.pack result).
    /// Handles nil holes correctly by iterating 1..=n explicitly.
    fn extract_params_from_table(tbl: &LuaTable) -> Result<Vec<LuaValue>, LuaError> {
        let n: usize = tbl.get::<usize>("n").unwrap_or(0);
        let mut params = Vec::with_capacity(n);
        for i in 1..=n {
            params.push(tbl.get::<LuaValue>(i)?);
        }
        Ok(params)
    }

    impl LuaUserData for DbConnection {
        fn add_methods<M: LuaUserDataMethods<Self>>(methods: &mut M) {
            // exec: DDL/DML execution
            // No params → execute_batch (supports PRAGMA, multi-statement, DDL)
            // With params → execute (single parameterized DML)
            methods.add_method(
                "exec",
                |_lua, this, (sql, params): (String, Option<LuaTable>)| {
                    let conn = this
                        .0
                        .lock()
                        .map_err(|e| LuaError::external(e.to_string()))?;
                    let lua_vals = match &params {
                        Some(tbl) => extract_params_from_table(tbl)?,
                        None => vec![],
                    };
                    if lua_vals.is_empty() {
                        conn.execute_batch(&sql)
                            .map_err(|e| LuaError::external(e.to_string()))?;
                        Ok(0i64)
                    } else {
                        let params = lua_values_to_rusqlite(&lua_vals)?;
                        let affected = conn
                            .execute(&sql, rusqlite::params_from_iter(params.iter()))
                            .map_err(|e| LuaError::external(e.to_string()))?;
                        Ok(affected as i64)
                    }
                },
            );

            // query: SELECT — returns array of tables
            // Accepts params as table.pack() result (table with .n field)
            methods.add_method(
                "query",
                |lua, this, (sql, params): (String, Option<LuaTable>)| {
                    let conn = this
                        .0
                        .lock()
                        .map_err(|e| LuaError::external(e.to_string()))?;
                    let lua_vals = match &params {
                        Some(tbl) => extract_params_from_table(tbl)?,
                        None => vec![],
                    };
                    let params = lua_values_to_rusqlite(&lua_vals)?;
                    let mut stmt = conn
                        .prepare(&sql)
                        .map_err(|e| LuaError::external(e.to_string()))?;

                    let col_count = stmt.column_count();
                    let col_names: Vec<String> = (0..col_count)
                        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
                        .collect();

                    let rows_result: Result<Vec<LuaTable>, LuaError> = stmt
                        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                            (0..col_count)
                                .map(|i| {
                                    let val: rusqlite::types::Value = row.get(i)?;
                                    Ok((col_names[i].clone(), val))
                                })
                                .collect::<Result<Vec<_>, rusqlite::Error>>()
                        })
                        .map_err(|e| LuaError::external(e.to_string()))?
                        .map(|row_result| {
                            let cols = row_result.map_err(|e| LuaError::external(e.to_string()))?;
                            let tbl = lua.create_table()?;
                            for (name, val) in cols {
                                match val {
                                    rusqlite::types::Value::Null => tbl.set(name, LuaValue::Nil)?,
                                    rusqlite::types::Value::Integer(n) => tbl.set(name, n)?,
                                    rusqlite::types::Value::Real(f) => tbl.set(name, f)?,
                                    rusqlite::types::Value::Text(s) => tbl.set(name, s)?,
                                    rusqlite::types::Value::Blob(b) => {
                                        tbl.set(name, lua.create_string(&b)?)?
                                    }
                                }
                            }
                            Ok(tbl)
                        })
                        .collect();

                    let rows = rows_result?;
                    let result = lua.create_table()?;
                    for (i, row) in rows.into_iter().enumerate() {
                        result.set(i + 1, row)?;
                    }
                    Ok(result)
                },
            );

            // close: explicitly close the connection
            methods.add_method("close", |_lua, _this, ()| {
                // Drop happens when Lua GC collects the userdata.
                // Explicit close not needed with Mutex<Connection>,
                // but provided for Lua-side convenience.
                Ok(())
            });
        }
    }

    /// Convert Lua values to rusqlite-compatible values.
    fn lua_values_to_rusqlite(
        values: &[LuaValue],
    ) -> Result<Vec<Box<dyn rusqlite::types::ToSql>>, LuaError> {
        values
            .iter()
            .map(|v| -> Result<Box<dyn rusqlite::types::ToSql>, LuaError> {
                match v {
                    LuaValue::Nil => Ok(Box::new(rusqlite::types::Null)),
                    LuaValue::Boolean(b) => Ok(Box::new(*b)),
                    LuaValue::Integer(n) => Ok(Box::new(*n)),
                    LuaValue::Number(f) => Ok(Box::new(*f)),
                    LuaValue::String(s) => Ok(Box::new(s.to_str()?.to_string())),
                    _ => Err(LuaError::external(format!(
                        "unsupported SQL param type: {:?}",
                        v.type_name()
                    ))),
                }
            })
            .collect()
    }

    /// Convert a serde_json::Value to a Lua value.
    fn json_value_to_lua(lua: &mlua::Lua, value: &serde_json::Value) -> LuaResult<LuaValue> {
        match value {
            serde_json::Value::Null => Ok(LuaValue::Nil),
            serde_json::Value::Bool(b) => Ok(LuaValue::Boolean(*b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(LuaValue::Integer(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(LuaValue::Number(f))
                } else {
                    Ok(LuaValue::Nil)
                }
            }
            serde_json::Value::String(s) => Ok(LuaValue::String(lua.create_string(s)?)),
            serde_json::Value::Array(arr) => {
                let tbl = lua.create_table()?;
                for (i, v) in arr.iter().enumerate() {
                    tbl.set(i + 1, json_value_to_lua(lua, v)?)?;
                }
                Ok(LuaValue::Table(tbl))
            }
            serde_json::Value::Object(obj) => {
                let tbl = lua.create_table()?;
                for (k, v) in obj {
                    tbl.set(k.as_str(), json_value_to_lua(lua, v)?)?;
                }
                Ok(LuaValue::Table(tbl))
            }
        }
    }

    /// Thread-isolated Lua VM with mlua-batteries pre-loaded.
    ///
    /// Each `MluaRuntime` owns one `Isle` (one dedicated thread + one Lua VM).
    /// The VM has `std.fs`, `std.json` registered via mlua-batteries,
    /// and `package.path` configured for VDSL module resolution.
    pub struct MluaRuntime {
        isle: Isle,
        work_dir: Arc<PathBuf>,
    }

    /// Result of executing Lua code via the mlua backend.
    /// Intentionally mirrors the process-based `LuaExecResult`.
    pub struct MluaExecResult {
        pub exit_code: i32,
        pub stdout: String,
        pub stderr: String,
    }

    impl MluaRuntime {
        /// Create a new runtime rooted at `work_dir`.
        ///
        /// Initialises the Lua VM with:
        /// - mlua-batteries `std.fs` + `std.json`
        /// - `package.path` set for VDSL module resolution
        /// - DI bridge: `std.fs` → `vdsl.runtime.fs.set_backend()`
        pub fn new(work_dir: &Path) -> Result<Self, McpError> {
            let work_dir = Arc::new(work_dir.to_path_buf());
            let wd = Arc::clone(&work_dir);

            let isle = Isle::spawn(move |lua| {
                // 1. Register mlua-batteries (std.fs, std.json)
                mlua_batteries::register_all(lua, "std")?;

                // 2. Set package.path for VDSL module resolution
                let pkg_path_lua = format!(
                    "package.path = '{wd}/lua/?.lua;{wd}/lua/?/init.lua;{wd}/scripts/?.lua;' .. package.path",
                    wd = wd.display(),
                );
                lua.load(&pkg_path_lua).exec()?;

                // 3. Bridge std.fs → vdsl.runtime.fs via set_backend
                //    std.fs provides: read, write, read_binary, write_binary,
                //    copy, mkdir, remove, is_file, is_dir, walk, glob
                //    VDSL runtime/fs expects: read, write, read_binary, write_binary,
                //    cp, mkdir, exists, ls, find, sleep
                //
                //    We build an adapter table that maps VDSL's API names
                //    to mlua-batteries' std.fs functions.
                let bridge_code = r#"
                    local ok, fs_mod = pcall(require, "vdsl.runtime.fs")
                    if ok and fs_mod and fs_mod.set_backend then
                        local rust_fs = std.fs
                        local backend = {
                            read       = rust_fs.read,
                            write      = rust_fs.write,
                            read_binary  = rust_fs.read_binary,
                            write_binary = rust_fs.write_binary,
                            mkdir      = rust_fs.mkdir,
                            cp         = rust_fs.copy,
                            exists     = function(path)
                                return rust_fs.is_file(path) or rust_fs.is_dir(path)
                            end,
                            ls         = function(dir)
                                -- walk with depth=1 gives immediate children
                                local entries = rust_fs.walk(dir)
                                local result = {}
                                for _, e in ipairs(entries) do
                                    -- strip dir prefix to get just the name
                                    local name = e:match("^" .. dir:gsub("([%.%-%+%[%]%(%)%$%%])", "%%%1") .. "/?(.+)$")
                                    if name and not name:find("/") then
                                        result[#result + 1] = name
                                    end
                                end
                                return result
                            end,
                            find       = function(dir, pattern)
                                -- Use glob for pattern-based file finding
                                local glob_pattern = dir .. "/" .. (pattern or "*")
                                return rust_fs.glob(glob_pattern)
                            end,
                            sleep      = function(seconds)
                                -- No-op or use os.execute for sleep
                                -- mlua-batteries doesn't provide sleep
                            end,
                        }
                        fs_mod.set_backend(backend)
                    end
                "#;
                lua.load(bridge_code).exec()?;

                // 4. Bridge std.http → vdsl.runtime.transport via set_backend
                //    std.http provides: get(url) -> {status, body},
                //                       post(url, body, ct?) -> {status, body},
                //                       request({method, url, headers, body, timeout}) -> {status, body}
                //    VDSL transport expects: get(url, headers) -> string,
                //                            post_json(url, data, headers) -> table,
                //                            upload(url, filepath, form_fields, headers) -> table,
                //                            download(url, filepath, headers) -> boolean
                let http_bridge_code = r#"
                    local ok, transport_mod = pcall(require, "vdsl.runtime.transport")
                    if ok and transport_mod and transport_mod.set_backend then
                        local rust_http = std.http
                        local json = std.json

                        local backend = {
                            get = function(url, headers)
                                local opts = { method = "GET", url = url }
                                if headers then opts.headers = headers end
                                local resp = rust_http.request(opts)
                                if resp.status >= 400 then
                                    error("HTTP GET failed (status " .. resp.status .. ")", 2)
                                end
                                return resp.body
                            end,

                            post_json = function(url, data, headers)
                                local body = json.encode(data)
                                local h = { ["Content-Type"] = "application/json" }
                                if headers then
                                    for k, v in pairs(headers) do h[k] = v end
                                end
                                local resp = rust_http.request({
                                    method = "POST",
                                    url = url,
                                    headers = h,
                                    body = body,
                                })
                                if resp.status >= 400 then
                                    error("HTTP POST failed (status " .. resp.status .. ")", 2)
                                end
                                return json.decode(resp.body)
                            end,

                            upload = function(url, filepath, form_fields, headers)
                                -- Upload requires multipart — fall back to curl
                                -- since mlua-batteries http doesn't support multipart
                                local curl_backend = require("vdsl.runtime.transport.curl")
                                return curl_backend.upload(url, filepath, form_fields, headers)
                            end,

                            download = function(url, filepath, headers)
                                local opts = { method = "GET", url = url }
                                if headers then opts.headers = headers end
                                local resp = rust_http.request(opts)
                                if resp.status >= 400 then
                                    error("HTTP download failed (status " .. resp.status .. ")", 2)
                                end
                                -- Write binary body to file via std.fs
                                local rust_fs = std.fs
                                rust_fs.write_binary(filepath, resp.body)
                                return true
                            end,
                        }
                        transport_mod.set_backend(backend)
                    end
                "#;
                lua.load(http_bridge_code).exec()?;

                // 5. DB Connection Provider — db.open(path) -> DbConnection UserData
                //    Lua side gets: local conn = db.open(path)
                //                   conn:exec(sql, params) -> affected_rows
                //                   conn:query(sql, params) -> [{col=val, ...}, ...]
                //                   conn:close()
                let db_table = lua.create_table()?;
                db_table.set(
                    "open",
                    lua.create_function(|_lua, path: String| {
                        let conn = rusqlite::Connection::open(&path)
                            .map_err(|e| LuaError::external(e.to_string()))?;
                        Ok(DbConnection(Mutex::new(conn)))
                    })?,
                )?;
                lua.globals().set("db", db_table)?;

                // 6. PNG metadata primitives (Rust globals)
                //    png.read_chunks(path, keys?) -> table
                //    png.write_chunk(path, keyword, text) -> boolean
                //    png.read_text_raw(path) -> table  (raw strings, no JSON decode)
                let png_table = lua.create_table()?;
                png_table.set(
                    "read_chunks",
                    lua.create_function(|lua, (path, keys): (String, Option<Vec<String>>)| {
                        let keys = keys.unwrap_or_default();
                        let meta = pngmetagrep_core::extract(
                            std::path::Path::new(&path),
                            &keys,
                        )
                        .map_err(|e| LuaError::external(e.to_string()))?;

                        let result = lua.create_table()?;
                        if let Some(meta) = meta {
                            for (keyword, value) in &meta.chunks {
                                let lua_val = json_value_to_lua(lua, value)?;
                                result.set(keyword.as_str(), lua_val)?;
                            }
                        }
                        Ok(result)
                    })?,
                )?;
                png_table.set(
                    "read_text_raw",
                    lua.create_function(|lua, path: String| {
                        let chunks = pngmeta::read_text_chunks(std::path::Path::new(&path))
                            .map_err(|e| LuaError::external(e.to_string()))?;
                        let result = lua.create_table()?;
                        for (keyword, text) in &chunks {
                            result.set(keyword.as_str(), text.as_str())?;
                        }
                        Ok(result)
                    })?,
                )?;
                png_table.set(
                    "write_chunk",
                    lua.create_function(|_lua, (path, keyword, text): (String, String, String)| {
                        pngmeta::write_text_chunk(
                            std::path::Path::new(&path),
                            &keyword,
                            &text,
                        )
                        .map_err(|e| LuaError::external(e.to_string()))?;
                        Ok(true)
                    })?,
                )?;
                lua.globals().set("png", png_table)?;

                // 7. Bridge png primitives → vdsl.runtime.png via set_backend
                //    DI interface: read_text, inject_text, inject_text_to
                let png_bridge_code = r#"
                    local ok, png_mod = pcall(require, "vdsl.runtime.png")
                    if ok and png_mod and png_mod.set_backend then
                        local rust_png = png        -- Rust global
                        local rust_fs  = std.fs     -- for inject_text_to copy

                        png_mod.set_backend({
                            read_text = function(path)
                                local ok2, result = pcall(rust_png.read_text_raw, path)
                                if not ok2 then return nil, tostring(result) end
                                -- empty table → nil (no chunks found)
                                if not next(result) then return nil end
                                return result
                            end,

                            inject_text = function(path, chunks)
                                for keyword, text in pairs(chunks) do
                                    local wok, werr = pcall(rust_png.write_chunk, path, keyword, tostring(text))
                                    if not wok then return false, tostring(werr) end
                                end
                                return true
                            end,

                            inject_text_to = function(src, dst, chunks)
                                rust_fs.copy(src, dst)
                                for keyword, text in pairs(chunks) do
                                    local wok, werr = pcall(rust_png.write_chunk, dst, keyword, tostring(text))
                                    if not wok then return false, tostring(werr) end
                                end
                                return true
                            end,
                        })
                    end
                "#;
                lua.load(png_bridge_code).exec()?;

                // 8. Bridge db global → vdsl.runtime.db via set_backend
                //    Backend interface: { open(path) -> conn }
                //    conn: exec(sql, packed_params?), query(sql, packed_params?), close()
                //    packed_params = table.pack(...) result with .n field
                let db_bridge_code = r#"
                    local ok, db_mod = pcall(require, "vdsl.runtime.db")
                    if ok and db_mod and db_mod.set_backend then
                        local rust_db = db  -- Rust global

                        db_mod.set_backend({
                            open = function(path)
                                local raw = rust_db.open(path)
                                return {
                                    exec = function(self, sql, packed_params)
                                        raw:exec(sql, packed_params)
                                    end,
                                    query = function(self, sql, packed_params)
                                        return raw:query(sql, packed_params)
                                    end,
                                    close = function(self)
                                        raw:close()
                                    end,
                                }
                            end,
                        })
                    end
                "#;
                lua.load(db_bridge_code).exec()?;

                // 9. os.getenv wrapper — injected env vars override real env
                //    _injected_env is populated by exec_code_with_env preamble
                let getenv_wrapper = r#"
                    _injected_env = {}
                    local _real_getenv = os.getenv
                    os.getenv = function(key)
                        local v = _injected_env[key]
                        if v ~= nil then return v end
                        return _real_getenv(key)
                    end
                "#;
                lua.load(getenv_wrapper).exec()?;

                Ok(())
            })
            .map_err(|e| McpError::internal_error(format!("mlua init failed: {e}"), None))?;

            Ok(Self { isle, work_dir })
        }

        /// Execute Lua code and capture stdout/stderr via `print()` override.
        ///
        /// The code is run inside the existing VM with stdout/stderr captured
        /// by temporarily overriding `print` and `io.stderr:write`.
        pub fn exec_code(&self, code: &str) -> Result<MluaExecResult, McpError> {
            let code = code.to_string();

            self.isle
                .exec(move |lua| {
                    // Wrap execution with stdout/stderr capture
                    let wrapper = format!(
                        r#"
                        local _stdout_buf = {{}}
                        local _stderr_buf = {{}}
                        local _orig_print = print
                        local _orig_io_write = io.write

                        print = function(...)
                            local args = {{...}}
                            local parts = {{}}
                            for i, v in ipairs(args) do
                                parts[i] = tostring(v)
                            end
                            _stdout_buf[#_stdout_buf + 1] = table.concat(parts, "\t") .. "\n"
                        end

                        io.write = function(...)
                            local args = {{...}}
                            for _, v in ipairs(args) do
                                _stdout_buf[#_stdout_buf + 1] = tostring(v)
                            end
                        end

                        local _ok, _err = pcall(function()
                            {code}
                        end)

                        print = _orig_print
                        io.write = _orig_io_write

                        local _stdout = table.concat(_stdout_buf)
                        local _stderr = ""
                        local _exit = 0
                        if not _ok then
                            _stderr = tostring(_err)
                            _exit = 1
                        end

                        return _exit .. "\n" .. _stdout .. "\0" .. _stderr
                    "#,
                        code = code
                    );

                    let raw: String = lua
                        .load(&wrapper)
                        .eval()
                        .map_err(|e| IsleError::Lua(e.to_string()))?;

                    // Parse: "<exit_code>\n<stdout>\0<stderr>"
                    let (exit_str, rest) = raw.split_once('\n').unwrap_or(("1", &raw));
                    let exit_code: i32 = exit_str.parse().unwrap_or(1);
                    let (stdout, stderr) = rest.split_once('\0').unwrap_or((rest, ""));

                    Ok(serde_json::json!({
                        "exit_code": exit_code,
                        "stdout": stdout,
                        "stderr": stderr,
                    })
                    .to_string())
                })
                .map(|json_str| {
                    let v: serde_json::Value = serde_json::from_str(&json_str).unwrap_or_default();
                    MluaExecResult {
                        exit_code: v["exit_code"].as_i64().unwrap_or(1) as i32,
                        stdout: v["stdout"].as_str().unwrap_or("").to_string(),
                        stderr: v["stderr"].as_str().unwrap_or("").to_string(),
                    }
                })
                .map_err(|e| McpError::internal_error(format!("mlua exec failed: {e}"), None))
        }

        /// Execute a Lua script file.
        pub fn exec_file(&self, script_path: &Path) -> Result<MluaExecResult, McpError> {
            let code = std::fs::read_to_string(script_path).map_err(|e| {
                McpError::internal_error(
                    format!("failed to read script {}: {e}", script_path.display()),
                    None,
                )
            })?;
            self.exec_code(&code)
        }

        /// Execute with environment variables injected as Lua globals.
        ///
        /// Each `(key, value)` pair is set as a global string before execution.
        pub fn exec_code_with_env(
            &self,
            code: &str,
            envs: &[(&str, &str)],
        ) -> Result<MluaExecResult, McpError> {
            let mut preamble = String::new();
            for (k, v) in envs {
                let escaped = v.replace('\\', "\\\\").replace('\'', "\\'");
                preamble.push_str(&format!("_injected_env['{k}'] = '{escaped}'\n"));
            }
            preamble.push_str(code);
            self.exec_code(&preamble)
        }

        /// Shut down the Lua VM thread.
        pub fn shutdown(self) -> Result<(), McpError> {
            self.isle
                .shutdown()
                .map_err(|e| McpError::internal_error(format!("mlua shutdown failed: {e}"), None))
        }

        /// Get the working directory.
        pub fn work_dir(&self) -> &Path {
            &self.work_dir
        }
    }
}

#[cfg(feature = "mlua-backend")]
pub use inner::{MluaExecResult, MluaRuntime};
