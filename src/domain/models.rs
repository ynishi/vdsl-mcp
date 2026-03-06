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
    /// All available node class_types from `/object_info` top-level keys.
    pub node_types: Vec<String>,
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
///
/// Top-level keys of `/object_info` are all available node class_types.
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

    // Collect all available node types from top-level keys
    let mut node_types: Vec<String> = object_info
        .as_object()
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();
    node_types.sort();

    ModelCatalog {
        checkpoints,
        loras,
        vaes,
        controlnets,
        upscalers,
        node_types,
    }
}

/// Format model catalog as human-readable text for MCP output.
/// When `limit` is `Some(n)`, each category shows at most `n` items.
pub fn format_model_catalog(catalog: &ModelCatalog) -> String {
    format_model_catalog_with_limit(catalog, None)
}

/// Format model catalog with optional per-category limit.
pub fn format_model_catalog_with_limit(catalog: &ModelCatalog, limit: Option<usize>) -> String {
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
            let show = limit.map_or(items.len(), |l| l.min(items.len()));
            for item in &items[..show] {
                out.push_str(&format!("  - {item}\n"));
            }
            if show < items.len() {
                out.push_str(&format!(
                    "  ... and {} more (set limit to see all)\n",
                    items.len() - show
                ));
            }
        }
        out.push('\n');
    }

    out
}

/// Required models and node types extracted from compiled workflow JSONs.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RequiredModels {
    pub checkpoints: Vec<String>,
    pub loras: Vec<String>,
    pub vaes: Vec<String>,
    pub controlnets: Vec<String>,
    pub upscalers: Vec<String>,
    /// All class_types used in the compiled workflows.
    pub node_types: Vec<String>,
}

impl RequiredModels {
    pub fn is_empty(&self) -> bool {
        self.checkpoints.is_empty()
            && self.loras.is_empty()
            && self.vaes.is_empty()
            && self.controlnets.is_empty()
            && self.upscalers.is_empty()
            && self.node_types.is_empty()
    }
}

/// Extract required model names and node types from compiled ComfyUI API-format workflow JSONs.
///
/// Scans each node's `class_type` + `inputs.<field>` against `RESOURCE_MAP`.
/// Also collects all unique `class_type` values for node availability checking.
/// Deduplicates results.
pub fn extract_required_models(workflows: &[serde_json::Value]) -> RequiredModels {
    use std::collections::HashSet;

    let mut sets: std::collections::HashMap<&str, HashSet<String>> = RESOURCE_MAP
        .iter()
        .map(|&(_, _, key)| (key, HashSet::new()))
        .collect();

    let mut node_type_set: HashSet<String> = HashSet::new();

    for wf in workflows {
        let nodes = match wf.as_object() {
            Some(obj) => obj,
            None => continue,
        };
        for (_node_id, node) in nodes {
            let class_type = match node["class_type"].as_str() {
                Some(ct) => ct,
                None => continue,
            };

            node_type_set.insert(class_type.to_string());

            for &(node_type, field, key) in RESOURCE_MAP {
                if class_type == node_type {
                    if let Some(val) = node["inputs"][field].as_str() {
                        if !val.is_empty() {
                            if let Some(set) = sets.get_mut(key) {
                                set.insert(val.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    let mut to_sorted_vec = |key: &str| -> Vec<String> {
        let mut v: Vec<String> = sets.remove(key).unwrap_or_default().into_iter().collect();
        v.sort();
        v
    };

    let mut node_types: Vec<String> = node_type_set.into_iter().collect();
    node_types.sort();

    RequiredModels {
        checkpoints: to_sorted_vec("checkpoints"),
        loras: to_sorted_vec("loras"),
        vaes: to_sorted_vec("vaes"),
        controlnets: to_sorted_vec("controlnets"),
        upscalers: to_sorted_vec("upscalers"),
        node_types,
    }
}

/// Check required models and node types against available catalog. Returns missing items.
pub fn check_missing(required: &RequiredModels, available: &ModelCatalog) -> RequiredModels {
    let diff = |req: &[String], avail: &[String]| -> Vec<String> {
        req.iter()
            .filter(|r| !avail.iter().any(|a| a == *r))
            .cloned()
            .collect()
    };

    RequiredModels {
        checkpoints: diff(&required.checkpoints, &available.checkpoints),
        loras: diff(&required.loras, &available.loras),
        vaes: diff(&required.vaes, &available.vaes),
        controlnets: diff(&required.controlnets, &available.controlnets),
        upscalers: diff(&required.upscalers, &available.upscalers),
        node_types: diff(&required.node_types, &available.node_types),
    }
}

/// Format a preflight report from required and missing models/nodes.
pub fn format_preflight_report(required: &RequiredModels, missing: &RequiredModels) -> String {
    let mut out = String::new();

    let ok = missing.is_empty();
    if ok {
        out.push_str("Preflight OK: all models and nodes available.\n\n");
    } else {
        out.push_str("Preflight FAILED: missing items detected.\n\n");
    }

    // Model sections
    let sections: &[(&str, &[String], &[String])] = &[
        ("Checkpoints", &required.checkpoints, &missing.checkpoints),
        ("LoRAs", &required.loras, &missing.loras),
        ("VAEs", &required.vaes, &missing.vaes),
        ("ControlNets", &required.controlnets, &missing.controlnets),
        ("Upscalers", &required.upscalers, &missing.upscalers),
    ];

    for &(title, req, miss) in sections {
        if req.is_empty() {
            continue;
        }
        out.push_str(&format!("# {title}\n"));
        for r in req {
            let marker = if miss.contains(r) { "MISSING" } else { "ok" };
            out.push_str(&format!("  [{marker}] {r}\n"));
        }
        out.push('\n');
    }

    // Custom nodes section (only show missing to avoid noise)
    if !missing.node_types.is_empty() {
        out.push_str(&format!(
            "# Custom Nodes ({} missing)\n",
            missing.node_types.len()
        ));
        for ct in &missing.node_types {
            out.push_str(&format!("  [MISSING] {ct}\n"));
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
        // node_types = top-level keys of object_info (sorted)
        assert_eq!(
            catalog.node_types,
            &[
                "CheckpointLoaderSimple",
                "ControlNetLoader",
                "LoraLoader",
                "UpscaleModelLoader",
                "VAELoader",
            ]
        );
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
        assert!(catalog.node_types.is_empty());
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
    fn extract_required_collects_node_types() {
        let wf = serde_json::json!({
            "1": { "class_type": "CheckpointLoaderSimple", "inputs": { "ckpt_name": "m.safetensors" } },
            "2": { "class_type": "KSampler", "inputs": {} },
            "3": { "class_type": "CLIPTextEncode", "inputs": { "text": "hi" } },
            "4": { "class_type": "ColorCorrect", "inputs": {} },
        });
        let required = extract_required_models(&[wf]);
        assert_eq!(
            required.node_types,
            &[
                "CLIPTextEncode",
                "CheckpointLoaderSimple",
                "ColorCorrect",
                "KSampler"
            ]
        );
        assert_eq!(required.checkpoints, &["m.safetensors"]);
    }

    #[test]
    fn check_missing_detects_missing_nodes() {
        let required = RequiredModels {
            node_types: vec![
                "KSampler".into(),
                "ColorCorrect".into(),
                "UltralyticsDetectorProvider".into(),
            ],
            ..Default::default()
        };
        let available = ModelCatalog {
            checkpoints: vec![],
            loras: vec![],
            vaes: vec![],
            controlnets: vec![],
            upscalers: vec![],
            node_types: vec!["KSampler".into(), "VAEDecode".into()],
        };
        let missing = check_missing(&required, &available);
        assert_eq!(
            missing.node_types,
            &["ColorCorrect", "UltralyticsDetectorProvider"]
        );
    }

    #[test]
    fn format_preflight_shows_missing_nodes() {
        let required = RequiredModels {
            checkpoints: vec!["m.safetensors".into()],
            node_types: vec!["KSampler".into(), "ColorCorrect".into()],
            ..Default::default()
        };
        let missing = RequiredModels {
            node_types: vec!["ColorCorrect".into()],
            ..Default::default()
        };
        let report = format_preflight_report(&required, &missing);
        assert!(report.contains("FAILED"));
        assert!(report.contains("Custom Nodes (1 missing)"));
        assert!(report.contains("[MISSING] ColorCorrect"));
        assert!(report.contains("[ok] m.safetensors"));
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
