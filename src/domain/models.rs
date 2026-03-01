//! Model catalog extraction from ComfyUI `/object_info` response.
//!
//! Mirrors Lua `RESOURCE_MAP` in `registry.lua` L13-19.

use serde::Serialize;

/// Resource mapping: ComfyUI node type → input field → category key.
const RESOURCE_MAP: &[(&str, &str, &str)] = &[
    ("CheckpointLoaderSimple", "ckpt_name", "checkpoints"),
    ("VAELoader", "vae_name", "vaes"),
    ("LoraLoader", "lora_name", "loras"),
    ("ControlNetLoader", "control_net_name", "controlnets"),
    ("UpscaleModelLoader", "model_name", "upscalers"),
];

/// Parsed model catalog returned to MCP clients.
#[derive(Debug, Clone, Serialize)]
pub struct ModelCatalog {
    pub checkpoints: Vec<String>,
    pub loras: Vec<String>,
    pub vaes: Vec<String>,
    pub controlnets: Vec<String>,
    pub upscalers: Vec<String>,
}

/// Extract COMBO options from a node's `input.required.<field>[0]` array.
///
/// Mirrors Lua `extract_combo()` in `registry.lua` L22-34.
fn extract_combo(
    object_info: &serde_json::Value,
    node_type: &str,
    field_name: &str,
) -> Vec<String> {
    let options = &object_info[node_type]["input"]["required"][field_name][0];
    match options.as_array() {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        None => Vec::new(),
    }
}

/// Parse `/object_info` JSON into a structured model catalog.
pub fn parse_model_catalog(object_info: &serde_json::Value) -> ModelCatalog {
    let mut checkpoints = Vec::new();
    let mut loras = Vec::new();
    let mut vaes = Vec::new();
    let mut controlnets = Vec::new();
    let mut upscalers = Vec::new();

    for &(node, field, key) in RESOURCE_MAP {
        let items = extract_combo(object_info, node, field);
        match key {
            "checkpoints" => checkpoints = items,
            "loras" => loras = items,
            "vaes" => vaes = items,
            "controlnets" => controlnets = items,
            "upscalers" => upscalers = items,
            _ => {}
        }
    }

    ModelCatalog {
        checkpoints,
        loras,
        vaes,
        controlnets,
        upscalers,
    }
}

/// Format model catalog as human-readable text for MCP output.
pub fn format_model_catalog(catalog: &ModelCatalog) -> String {
    let mut out = String::new();

    let sections: &[(&str, &[String])] = &[
        ("Checkpoints", &catalog.checkpoints),
        ("LoRAs", &catalog.loras),
        ("VAEs", &catalog.vaes),
        ("ControlNets", &catalog.controlnets),
        ("Upscalers", &catalog.upscalers),
    ];

    for (title, items) in sections {
        out.push_str(&format!("# {} ({})\n", title, items.len()));
        if items.is_empty() {
            out.push_str("  (none)\n");
        } else {
            for item in *items {
                out.push_str(&format!("  - {item}\n"));
            }
        }
        out.push('\n');
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_object_info() -> serde_json::Value {
        serde_json::json!({
            "CheckpointLoaderSimple": {
                "input": {
                    "required": {
                        "ckpt_name": [["sd_v1-5.safetensors", "sdxl_base.safetensors"]]
                    }
                }
            },
            "LoraLoader": {
                "input": {
                    "required": {
                        "lora_name": [["detail_v1.safetensors"]],
                        "strength_model": [{ "default": 1.0 }]
                    }
                }
            },
            "VAELoader": {
                "input": {
                    "required": {
                        "vae_name": [["vae-ft-mse.safetensors"]]
                    }
                }
            },
            "ControlNetLoader": {
                "input": {
                    "required": {
                        "control_net_name": [[]]
                    }
                }
            },
            "UpscaleModelLoader": {
                "input": {
                    "required": {
                        "model_name": [["RealESRGAN_x4plus.pth"]]
                    }
                }
            }
        })
    }

    #[test]
    fn parse_extracts_all_categories() {
        let info = sample_object_info();
        let catalog = parse_model_catalog(&info);

        assert_eq!(
            catalog.checkpoints,
            &["sd_v1-5.safetensors", "sdxl_base.safetensors"]
        );
        assert_eq!(catalog.loras, &["detail_v1.safetensors"]);
        assert_eq!(catalog.vaes, &["vae-ft-mse.safetensors"]);
        assert!(catalog.controlnets.is_empty());
        assert_eq!(catalog.upscalers, &["RealESRGAN_x4plus.pth"]);
    }

    #[test]
    fn parse_missing_node_returns_empty() {
        let info = serde_json::json!({});
        let catalog = parse_model_catalog(&info);

        assert!(catalog.checkpoints.is_empty());
        assert!(catalog.loras.is_empty());
        assert!(catalog.vaes.is_empty());
        assert!(catalog.controlnets.is_empty());
        assert!(catalog.upscalers.is_empty());
    }

    #[test]
    fn parse_malformed_input_returns_empty() {
        let info = serde_json::json!({
            "CheckpointLoaderSimple": {
                "input": { "required": { "ckpt_name": "not_an_array" } }
            }
        });
        let catalog = parse_model_catalog(&info);
        assert!(catalog.checkpoints.is_empty());
    }

    #[test]
    fn format_includes_counts() {
        let info = sample_object_info();
        let catalog = parse_model_catalog(&info);
        let text = format_model_catalog(&catalog);

        assert!(text.contains("# Checkpoints (2)"));
        assert!(text.contains("# LoRAs (1)"));
        assert!(text.contains("# ControlNets (0)"));
        assert!(text.contains("(none)"));
        assert!(text.contains("sd_v1-5.safetensors"));
    }
}
