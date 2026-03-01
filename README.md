# vdsl-mcp

MCP server for [VDSL](https://github.com/ynishi/vdsl) — AI-native image generation platform.

RunPod GPU provisioning, ComfyUI orchestration, and model management — all accessible as [MCP](https://modelcontextprotocol.io/) tools.

## Features

- **RunPod Pod Management** — Create, start, stop, delete GPU pods. Auto-setup with ComfyUI template.
- **ComfyUI Integration** — Connect, query models, submit workflows, poll results, download images.
- **Model Download** — HuggingFace (`hf:`), CivitAI (`cv:`) with automatic token injection, or direct URLs.
- **Batch Generation** — Submit multiple workflows, poll all jobs, download all outputs.
- **VDSL Script Execution** — Run Lua scripts that compile into ComfyUI workflows.
- **RunPod CLI Passthrough** — Execute any `runpod-cli` command with auto API key injection.
- **ComfyUI API** — Generic REST API access with automatic authentication.

## MCP Tools

| Tool | Description |
|------|-------------|
| `vdsl_pod_list` | List all RunPod pods |
| `vdsl_pod_start` | Start a pod |
| `vdsl_pod_stop` | Stop a pod |
| `vdsl_pod_create` | Create a new ComfyUI pod |
| `vdsl_pod_delete` | Delete a pod |
| `vdsl_pod_setup` | Find or create a pod, wait until ready |
| `vdsl_volume_list` | List network volumes |
| `vdsl_connect` | Connect to ComfyUI instance |
| `vdsl_models` | List available models (checkpoints, LoRAs, etc.) |
| `vdsl_queue_status` | Query ComfyUI queue and job history |
| `vdsl_upload` | Upload files to ComfyUI (single, batch, directory) |
| `vdsl_download` | Download models to a pod |
| `vdsl_generate` | Generate images from a workflow |
| `vdsl_batch_generate` | Batch generate from multiple workflows |
| `vdsl_run_script` | Run a VDSL Lua script |
| `vdsl_run` | Compile + generate from a VDSL script |
| `vdsl_catalogs` | List available VDSL catalog entries |
| `vdsl_comfy_api` | Generic ComfyUI REST API call |
| `vdsl_runpod_cli` | RunPod CLI passthrough |
| `vdsl_interrupt` | Cancel ComfyUI jobs |

## Installation

```bash
cargo install vdsl-mcp
```

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `RUNPOD_API_KEY` | For RunPod operations | RunPod API key |
| `COMFYUI_TOKEN` | For authenticated ComfyUI | Bearer token for ComfyUI proxy |
| `CIVITAI_TOKEN` | For CivitAI downloads | CivitAI API token (auto-injected) |
| `VDSL_INLINE_HISTORY_DIR` | Optional | Override inline script save directory |

### MCP Client Configuration

```json
{
  "mcpServers": {
    "vdsl": {
      "command": "vdsl-mcp",
      "env": {
        "RUNPOD_API_KEY": "your-api-key",
        "COMFYUI_TOKEN": "your-token",
        "CIVITAI_TOKEN": "your-civitai-token"
      }
    }
  }
}
```

## Usage

### Quick Start (ComfyUI already running)

1. `vdsl_connect(url)` — Connect to ComfyUI
2. `vdsl_models` — List available models
3. `vdsl_generate(workflow)` — Generate images

### Infrastructure (RunPod provisioning)

1. `vdsl_pod_setup` — Find or create a GPU pod
2. `vdsl_download(source, target)` — Download models
3. `vdsl_generate(workflow)` — Generate images

### Model Sources

```
hf:user/repo/model.safetensors     # HuggingFace
cv:1595775                          # CivitAI (token auto-injected)
https://example.com/model.safetensors  # Direct URL
user/repo/model.safetensors         # Bare path (defaults to HuggingFace)
```

## Related

- [VDSL](https://github.com/ynishi/vdsl) — VDSL core (Lua DSL for visual generation)

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
