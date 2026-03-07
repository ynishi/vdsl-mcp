# vdsl-mcp

MCP server for [VDSL](https://github.com/ynishi/vdsl) — AI-native image generation platform.

RunPod GPU provisioning, ComfyUI orchestration, and model management — all accessible as [MCP](https://modelcontextprotocol.io/) tools.

## Features

- **RunPod Pod Management** — Create, start, stop, delete GPU pods. Auto-setup with ComfyUI template.
- **ComfyUI Integration** — Connect, query models, submit workflows, poll results, download images.
- **Model Download** — HuggingFace (`hf:`), CivitAI (`cv:`) with automatic token injection, or direct URLs.
- **B2 Cold Storage** — List, pull, and push models between pods and Backblaze B2 via rclone.
- **Image Batch Download** — Download all output images from ComfyUI history to a local directory.
- **Batch Generation** — Submit multiple workflows, poll all jobs, download all outputs.
- **VDSL Script Execution** — Run Lua scripts that compile into ComfyUI workflows.
- **RunPod CLI Passthrough** — Execute any `runpod-cli` command with auto API key injection.
- **ComfyUI API** — Generic REST API access with automatic authentication.

## MCP Tools

| Tool | Description |
|------|-------------|
| **Connection** | |
| `vdsl_connect` | Connect to ComfyUI (local URL or RunPod pod ID) |
| **Generation** | |
| `vdsl_generate` | Queue a workflow JSON and wait for completion |
| `vdsl_batch_generate` | Submit multiple workflows, poll all, download outputs |
| `vdsl_run` | Compile Lua script → ComfyUI workflow → generate (supports pipelines, judge gates) |
| `vdsl_run_script` | Run a Lua script (no generation — script-only execution) |
| `vdsl_interrupt` | Cancel running or pending ComfyUI jobs |
| **Models & Catalogs** | |
| `vdsl_models` | List available checkpoints, LoRAs, VAEs, etc. |
| `vdsl_model_search` | Search CivitAI for models |
| `vdsl_node_search` | Search installed ComfyUI custom nodes |
| `vdsl_catalogs` | Browse VDSL catalog entries (camera, lighting, figure, quality, etc.) |
| **RunPod Infrastructure** | |
| `vdsl_pod_list` | List all pods with GPU name and cost |
| `vdsl_pod_start` | Start a pod |
| `vdsl_pod_stop` | Stop a pod |
| `vdsl_pod_create` | Create a new pod |
| `vdsl_pod_delete` | Delete a pod |
| `vdsl_pod_setup` | Find or create a pod, wait until ready, connect |
| `vdsl_volume_list` | List network volumes |
| **Remote Operations (SSH)** | |
| `vdsl_exec` | Execute a shell command on a pod |
| `vdsl_task_run` | Start a background job on a pod |
| `vdsl_task_status` | Check background job status |
| `vdsl_task_list` | List all background jobs |
| `vdsl_task_log` | View background job log output |
| **File Transfer** | |
| `vdsl_upload` | Upload files to ComfyUI input/ directory |
| `vdsl_download` | Download models to a pod (HuggingFace, CivitAI, URL) |
| `vdsl_image_download` | Download output images from ComfyUI history or output directory |
| `vdsl_image_search` | Search local image files by filename pattern |
| **Cold Storage (B2)** | |
| `vdsl_storage_list` | List files in B2 |
| `vdsl_storage_pull` | Pull models from B2 to pod |
| `vdsl_storage_push` | Push models from pod to B2 |
| `vdsl_storage_archive` | Archive: push → verify → delete from pod |
| **Low-Level** | |
| `vdsl_queue_status` | Query ComfyUI queue and history |
| `vdsl_comfy_api` | Generic ComfyUI REST API call |
| `vdsl_runpod_cli` | RunPod CLI passthrough (any command) |

## Installation

```bash
# 1. Install the MCP server binary
cargo install vdsl-mcp

# 2. Clone VDSL runtime (Lua DSL modules — required for vdsl_run / vdsl_catalogs)
git clone https://github.com/ynishi/vdsl.git ~/vdsl
```

The binary alone is sufficient for direct ComfyUI workflow submission (`vdsl_generate`).
To use the Lua DSL (`vdsl_run`, `vdsl_catalogs`), the VDSL runtime repository is required —
it provides the `lua/` directory containing catalog definitions, compilers, and pipeline logic.

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `VDSL_RUNPOD_API_KEY` | For RunPod operations | RunPod API key ([runpod.io/console/user/settings](https://www.runpod.io/console/user/settings)) |
| `VDSL_SSH_KEY` | For RunPod SSH operations | Path to SSH private key for pod access (e.g. `~/.ssh/id_ed25519`). Falls back to `~/.ssh/id_ed25519` if unset |
| `VDSL_COMFYUI_TOKEN` | For authenticated ComfyUI | Bearer token for ComfyUI proxy authentication (RunPod proxy uses this) |
| `VDSL_CIVITAI_TOKEN` | For CivitAI downloads | CivitAI API token — auto-injected into `cv:` and CivitAI URL downloads |
| `VDSL_B2_KEY_ID` | For B2 storage | Backblaze B2 application key ID |
| `VDSL_B2_KEY` | For B2 storage | Backblaze B2 application key |
| `VDSL_B2_BUCKET` | For B2 storage (optional) | Default B2 bucket name (can also be specified per-call) |
| `VDSL_COMFYUI_BASE` | Optional | Override ComfyUI install path on pod (default: `/workspace/runpod-slim/ComfyUI`). Community templates may use `/workspace/ComfyUI` etc. |
| `VDSL_INLINE_HISTORY_DIR` | Optional | Override directory for saving inline Lua code history |

### MCP Client Configuration

#### Minimal (local ComfyUI only)

```json
{
  "mcpServers": {
    "vdsl": {
      "command": "vdsl-mcp"
    }
  }
}
```

Then connect with `vdsl_connect(url="http://localhost:8188")`.

#### Full (RunPod + B2 storage)

```json
{
  "mcpServers": {
    "vdsl": {
      "command": "vdsl-mcp",
      "env": {
        "VDSL_RUNPOD_API_KEY": "rpa_...",
        "VDSL_SSH_KEY": "~/.ssh/id_ed25519",
        "VDSL_COMFYUI_TOKEN": "your-comfyui-token",
        "VDSL_CIVITAI_TOKEN": "your-civitai-token",
        "VDSL_B2_KEY_ID": "your-b2-key-id",
        "VDSL_B2_KEY": "your-b2-key",
        "VDSL_B2_BUCKET": "your-bucket-name"
      }
    }
  }
}
```

## Usage

### Quick Start (local ComfyUI)

1. `vdsl_connect(url="http://localhost:8188")` — Connect to ComfyUI
2. `vdsl_models` — List available checkpoints and LoRAs
3. `vdsl_generate(workflow)` — Generate images from a workflow JSON

### VDSL Lua DSL

Compile human-readable Lua scripts into ComfyUI workflows:

1. `vdsl_catalogs` — Browse available catalog entries (camera angles, lighting, figures, etc.)
2. `vdsl_run(script_file="~/vdsl/examples/mlua_verify_p1.lua")` — Compile + generate
3. `vdsl_run(code="...", working_dir="~/vdsl")` — Run inline Lua code

The `working_dir` must point to the cloned VDSL repository (contains `lua/` for module resolution).
When `script_file` is used, `working_dir` is auto-detected by walking up from the script's parent directory.

### Infrastructure (RunPod)

1. `vdsl_pod_list` — List pods with GPU info
2. `vdsl_pod_setup` — Find or create a GPU pod, wait until ready
3. `vdsl_exec(command="nvidia-smi")` — Run commands on the pod via SSH
4. `vdsl_task_run(command="...")` — Start long-running background jobs
5. `vdsl_download(source="hf:...", target="loras")` — Download models to the pod

### Cold Storage (B2)

1. `vdsl_storage_list` — List files in B2
2. `vdsl_storage_pull(source="models/loras/my.safetensors", target="loras")` — Restore from B2
3. `vdsl_storage_archive(source_target="loras", filename="old.safetensors")` — Archive to B2 (push → verify → delete)

### Model Sources

```
hf:user/repo/model.safetensors        # HuggingFace
cv:1595775                             # CivitAI (token auto-injected)
https://example.com/model.safetensors  # Direct URL
user/repo/model.safetensors            # Bare path (defaults to HuggingFace)
```

## Related

- [VDSL](https://github.com/ynishi/vdsl) — VDSL core (Lua DSL for visual generation)

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
