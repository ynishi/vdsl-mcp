# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added

- **`vdsl_tunnel_open`** — Open an SSH `-N -L` tunnel to a RunPod pod service; returns active local port on success, or silently falls back to the Cloudflare proxy URL when SSH info is unavailable (`idempotent_hint=true`: same pod re-uses the existing tunnel)
- **`vdsl_tunnel_close`** — Close the SSH tunnel for a pod; idempotent (no-op if not open); kills the child `ssh` process via `kill_on_drop`
- **`vdsl_tunnel_list`** — List all active tunnels in the in-memory registry as a JSON snapshot (read-only)
- **`domain::pod::RouteKind`** — `SshTunnel | CloudflareProxy | Direct` enum with `#[serde(rename_all = "kebab-case")]` for consistent JSON serialisation across `vdsl_tunnel_list` and `vdsl_pod_list` endpoints
- **`domain::pod::PodEndpoint`** — Structured endpoint type (`service`, `url`, `route: RouteKind`, `local_port`) used in `vdsl_pod_list` output
- **`application::tunnel_registry::TunnelRegistry`** — In-memory `Arc<RwLock<HashMap<String, Arc<Mutex<TunnelHandle>>>>>` registry tracking active SSH tunnels for the MCP session lifetime
- **`application::tunnel_registry::TunnelHandle`** — Holds a `tokio::process::Child` with `kill_on_drop(true)` set at spawn time; exposes a snapshot for listing
- **`domain::error::DomainError::SshTunnel`** — New error variant for SSH spawn failures, missing key, and early-exit detection
- **`format_pod_list_with_endpoints`** — New `domain::pod` function that extends pod list output with an `endpoints[]` JSON array; existing `format_pod_list` signature is unchanged (backward-compatible)
- **`vdsl_model_search` — scope parameter** — `scope=remote|archive|pod` selects search target: CivitAI (default), B2 archive bucket, or connected pod
- **`vdsl_model_search` — type filter** — `model_type` now accepts 8 values: `checkpoint / lora / controlnet / vae / upscale / embedding / clip / unet`
- **`vdsl_model_search` — base filter** — `base` parameter accepts `sd15 / sdxl / pony / illustrious / noobai / flux / unknown`
- **`vdsl_model_search` — structured result** — returns `ModelSearchResult[]` JSON with `name`, `model_type`, `base`, `scope`, `size_bytes`, `location`, `obtain`, `metadata` fields
- **`vdsl_model_search` — obtain hint** — `obtain` field surfaces ready-to-use tool invocation for each result (`vdsl_download` for remote, `vdsl_storage_pull` for archive)
- **`domain::models::Scope`** — `Remote | Archive | Pod` enum for model search scope
- **`domain::models::BaseModel`** — 7-value enum with `from_filename()` substring inference
- **`domain::models::ModelSearchResult`** — structured search result type returned by all scope paths
- **`parse_rclone_lsf()`** — standalone parser for `rclone lsf --format tsp` output (semicolon-separated modtime/size/path)

### Changed

- **`vdsl_pod_list` output** — Now appends an `endpoints[]` array to each pod entry; each element carries `service`, `url`, `route` (`ssh-tunnel` / `cloudflare-proxy` / `direct`), and `local_port`; route is derived from the live tunnel registry and `pod_ssh_info` result (not a static default)
- **`domain::models::ModelType`** — moved from `interface::mcp` to `domain::models`, extended from 6 to 8 values (added `Clip` and `Unet`); `to_civitai_type()` now returns `Option<&'static str>` (None for Clip/Unet)
- **`ModelType::as_dir_key()`** — replaces `MODEL_DIRS` const as single source of truth for ComfyUI directory key mapping

### Performance

- **`format_pod_list_with_endpoints`** — Eliminated N+1 subprocess round-trips by batching `pod_ssh_info` lookups
- **`search_archive` sidecar lookup** — O(N²) scan replaced with O(N) `HashSet` pre-index

## [0.4.0] - 2026-04-12

### Highlights

Unified version across both crates (vdsl-mcp and vdsl-sync). Major sync engine
overhaul with mesh topology, syncd file-watching daemon, and security hardening.

### Added

- **syncd daemon** — fsevents-based file watcher with HTTP delegate and bearer-token auth (`cdffc13`, `b2d0abd`)
- **Mesh sync** — route-based N-location transfer architecture with RouteGraph DAG (`3033de3`..`39c1ef0`, `ded3e45`)
- **Archive-on-delete** — soft-delete with restore support (`b9c22fc`)
- **Runtime ComfyUI base URL** — auto-detect on connect (`70f6e4b`)
- **Schema versioning** for SyncDb (`7dd158d`)
- **Sync pipeline design document** (`1bba6aa`)

### Changed

- **Domain model redesign** — TrackedFile/Transfer separation, FileMetadata, topology-centric model (`6e3afd4`, `ae42767`)
- **Scan-Diff-Plan-Apply pipeline** with 3-layer error hierarchy (`ae42767`)
- **RouteGraph unification** and distribute split (`644cdf1`)
- **TransferEngine extraction** with dependency inversion (`a14e7f4`, `753a863`)
- **mlua-isle migrated to AsyncIsle** (`e7be3a0`)
- **Workspace split** into vdsl-mcp + vdsl-sync crates (`a97fb87`)

### Fixed

- **Security**: reject control chars in relative paths and pod_shell (`550023a`, `b2d0abd`)
- **Security**: escape newlines/NUL in env-var injection (`cfd680d`)
- **metadata**: update png module path to match vdsl refactor (`da980c4`)
- **syncd**: propagate pod_id via env, restart on mismatch (`e6320fd`)
- **syncd**: propagate VDSL_WORK_DIR to spawned syncd (`5bda8be`)
- **sync**: ensure pod auto-detected before syncd spawn (`3f59bc4`)
- **sync**: suppress phantom ContentChanged for hashless files (`db74e94`)
- **sync**: RAII lock guard + surface syncd HTTP error detail (`15dbdeb`)
- **sync**: exclude hidden files uniformly across all scanners (`b7e95f8`)
- **sync**: recursive cloud scan with rclone lsf -R (`9824228`)
- **sync**: hard-delete TF when all LFs removed after delete transfers (`c8213ce`)
- **sync**: force Update for Missing LFs in distribute_actions (`ea6c473`)
- **sync**: invalidate cached SDK when SyncDb is rebuilt (`d1abf39`)
- **sync**: revert scan-based delete propagation + SyncDb lifecycle management (`3ba0c85`)
- **rclone**: install unzip before install.sh fallback on pod (`deb924b`)

### Performance

- Batch archive-move via `rclone move --files-from` (`c709517`)

## [0.3.0] - 2025-12-15

### Added

- Initial workspace split: vdsl-mcp + vdsl-sync crates
- SyncService with DI bridge for mlua runtime
- E2E three-location sync test

## [0.2.0] - 2025-11-01

### Added

- Initial crates.io release
- MCP server with RunPod, ComfyUI, model management tools
- VDSL Lua script execution backend
