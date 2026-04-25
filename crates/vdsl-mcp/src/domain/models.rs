//! Model catalog extraction from ComfyUI `/object_info` response.
//!
//! Mirrors Lua `RESOURCE_MAP` in `registry.lua` L13-19.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::domain::error::DomainError;

// =============================================================================
// Size threshold constants for archive type inference
// =============================================================================

/// Minimum size (bytes) for a checkpoint model (2 GB).
pub const ARCHIVE_SIZE_CP_MIN: u64 = 2 * 1024 * 1024 * 1024;
/// Maximum size (bytes) for a checkpoint model (8 GB).
pub const ARCHIVE_SIZE_CP_MAX: u64 = 8 * 1024 * 1024 * 1024;
/// Minimum size (bytes) for a LoRA model (50 MB).
pub const ARCHIVE_SIZE_LORA_MIN: u64 = 50 * 1024 * 1024;
/// Maximum size (bytes) for a LoRA model (500 MB).
pub const ARCHIVE_SIZE_LORA_MAX: u64 = 500 * 1024 * 1024;
/// Minimum size (bytes) for a VAE model (300 MB).
pub const ARCHIVE_SIZE_VAE_MIN: u64 = 300 * 1024 * 1024;
/// Maximum size (bytes) for a VAE model (800 MB).
pub const ARCHIVE_SIZE_VAE_MAX: u64 = 800 * 1024 * 1024;

// =============================================================================
// Unified model type enum (Single Source of Truth, replaces MODEL_DIRS const)
// =============================================================================

/// Model type covering all 8 ComfyUI model categories.
///
/// This is the Single Source of Truth for model category → directory name mapping.
/// Replaces the old `MODEL_DIRS` const table in `mcp.rs`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ModelType {
    /// Stable Diffusion checkpoint (main model file).
    Checkpoint,
    /// LoRA fine-tuning adapter.
    Lora,
    /// Variational AutoEncoder.
    Vae,
    /// Textual Inversion embedding.
    Embedding,
    /// ControlNet conditioning model.
    Controlnet,
    /// Image upscaler (e.g. RealESRGAN).
    Upscale,
    /// CLIP text encoder.
    Clip,
    /// UNet diffusion backbone.
    Unet,
    /// Unknown or unrecognized model type.
    #[default]
    Unknown,
}

impl ModelType {
    /// Returns the ComfyUI model directory name for this type.
    ///
    /// Encodes the asymmetric mapping: `Upscale` key → `"upscale_models"` dir.
    /// Used by `vdsl_download`, `vdsl_storage_pull/push/archive`, and archive dir inference.
    pub const fn as_dir_key(&self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoints",
            Self::Lora => "loras",
            Self::Vae => "vae",
            Self::Embedding => "embeddings",
            Self::Controlnet => "controlnet",
            Self::Upscale => "upscale_models",
            Self::Clip => "clip",
            Self::Unet => "unet",
            Self::Unknown => "unknown",
        }
    }

    /// Returns all concrete (non-Unknown) variants for iteration.
    pub const fn all() -> &'static [ModelType; 8] {
        &[
            Self::Checkpoint,
            Self::Lora,
            Self::Vae,
            Self::Embedding,
            Self::Controlnet,
            Self::Upscale,
            Self::Clip,
            Self::Unet,
        ]
    }

    /// Returns the CivitAI `type` query parameter value for this model type.
    ///
    /// Returns `None` for `Clip`, `Unet`, and `Unknown` because CivitAI does not
    /// have a direct equivalent; the `&type=` parameter is omitted in that case.
    pub fn to_civitai_type(self) -> Option<&'static str> {
        match self {
            Self::Checkpoint => Some("Checkpoint"),
            Self::Lora => Some("LORA"),
            Self::Controlnet => Some("Controlnet"),
            Self::Vae => Some("VAE"),
            Self::Upscale => Some("Upscaler"),
            Self::Embedding => Some("TextualInversion"),
            Self::Clip | Self::Unet | Self::Unknown => None,
        }
    }

    /// Converts a CivitAI `type` field string to the corresponding `ModelType`.
    ///
    /// Performs case-insensitive matching against known CivitAI type strings.
    /// Returns `Unknown` for unrecognized or empty strings.
    pub fn from_civitai_type(s: &str) -> Self {
        let lower = s.to_lowercase();
        match lower.as_str() {
            "checkpoint" => Self::Checkpoint,
            "lora" => Self::Lora,
            "controlnet" => Self::Controlnet,
            "vae" => Self::Vae,
            "upscaler" => Self::Upscale,
            "textualinversion" => Self::Embedding,
            _ => Self::Unknown,
        }
    }

    /// Infers a model type from an archive path and optional file size.
    ///
    /// The inference chain is:
    /// 1. Directory name segment (e.g. `/checkpoints/` in the path).
    /// 2. File size range thresholds (CP/LoRA/VAE bands).
    /// 3. `Unknown` fallback.
    ///
    /// This is intentionally infallible; callers do not need to handle errors.
    pub fn from_archive_path_and_size(path: &str, size: Option<u64>) -> Self {
        // Step 1: dir name segment match
        let path_lower = path.to_lowercase();
        let dir_candidates = [
            ("checkpoints", Self::Checkpoint),
            ("loras", Self::Lora),
            ("controlnet", Self::Controlnet),
            ("vae", Self::Vae),
            ("upscale_models", Self::Upscale),
            ("upscale", Self::Upscale),
            ("embeddings", Self::Embedding),
            ("clip", Self::Clip),
            ("unet", Self::Unet),
        ];
        for (segment, variant) in &dir_candidates {
            if path_lower.contains(&format!("/{segment}/"))
                || path_lower.starts_with(&format!("{segment}/"))
                || path_lower.ends_with(&format!("/{segment}"))
                || path_lower == *segment
            {
                return *variant;
            }
        }

        // Step 2: size range fallback
        if let Some(sz) = size {
            if (ARCHIVE_SIZE_CP_MIN..=ARCHIVE_SIZE_CP_MAX).contains(&sz) {
                return Self::Checkpoint;
            }
            if (ARCHIVE_SIZE_LORA_MIN..=ARCHIVE_SIZE_LORA_MAX).contains(&sz) {
                return Self::Lora;
            }
            if (ARCHIVE_SIZE_VAE_MIN..=ARCHIVE_SIZE_VAE_MAX).contains(&sz) {
                return Self::Vae;
            }
        }

        // Step 3: unknown
        Self::Unknown
    }

    /// Returns the ComfyUI loader node name for this type, if applicable.
    ///
    /// Used to absorb `RESOURCE_MAP` entries into the enum.
    /// Returns `None` for types without a direct ComfyUI loader (Clip, Unet, Unknown).
    pub const fn comfyui_loader(&self) -> Option<&'static str> {
        match self {
            Self::Checkpoint => Some("CheckpointLoaderSimple"),
            Self::Lora => Some("LoraLoader"),
            Self::Vae => Some("VAELoader"),
            Self::Controlnet => Some("ControlNetLoader"),
            Self::Upscale => Some("UpscaleModelLoader"),
            Self::Embedding | Self::Clip | Self::Unet | Self::Unknown => None,
        }
    }

    /// Maps a ComfyUI node loader class name to the corresponding `ModelType`.
    ///
    /// Returns `None` if the loader name is not recognized.
    pub fn from_comfyui_loader(name: &str) -> Option<Self> {
        Self::all()
            .iter()
            .find(|t| t.comfyui_loader() == Some(name))
            .copied()
    }
}

impl TryFrom<&str> for ModelType {
    type Error = DomainError;

    /// Converts a dir-key string (e.g. `"checkpoints"`, `"loras"`) to `ModelType`.
    ///
    /// The key space is the left-hand side of the old `MODEL_DIRS` table.
    /// `"upscale"` (the key) maps to `Upscale`; `"upscale_models"` (the dir) is not a valid key.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::ModelTypeParse` for unrecognized keys.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "checkpoints" => Ok(Self::Checkpoint),
            "loras" => Ok(Self::Lora),
            "controlnet" => Ok(Self::Controlnet),
            "vae" => Ok(Self::Vae),
            "upscale" => Ok(Self::Upscale),
            "embeddings" => Ok(Self::Embedding),
            "clip" => Ok(Self::Clip),
            "unet" => Ok(Self::Unet),
            other => Err(DomainError::ModelTypeParse(other.to_string())),
        }
    }
}

// =============================================================================
// Scope enum
// =============================================================================

/// Search scope for `vdsl_model_search`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// Search CivitAI (or other remote marketplace).
    #[default]
    Remote,
    /// Search B2 cold-storage archive bucket.
    Archive,
    /// Search models currently available on the RunPod pod (ComfyUI).
    Pod,
}

// =============================================================================
// BaseModel enum
// =============================================================================

/// Base model / architecture of a generative model.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BaseModel {
    /// Pony Diffusion base.
    Pony,
    /// Illustrious base.
    Illustrious,
    /// NoobAI base.
    Noobai,
    /// Stable Diffusion XL.
    Sdxl,
    /// Stable Diffusion 1.5.
    Sd15,
    /// Flux base model.
    Flux,
    /// Unknown or unrecognized base model.
    #[default]
    Unknown,
}

impl BaseModel {
    /// Infers the base model from a filename using case-insensitive substring matching.
    ///
    /// Matching priority (first match wins):
    /// `illustrious` → `pony` → `noobai` → `flux` → `sdxl` → `sd15`.
    /// Falls back to `Unknown` if no substring matches.
    ///
    /// This is intentionally infallible.
    pub fn from_filename(name: &str) -> Self {
        let lower = name.to_lowercase();
        // Order matters: longer/more-specific first to avoid shadowing
        if lower.contains("illustrious") {
            Self::Illustrious
        } else if lower.contains("pony") {
            Self::Pony
        } else if lower.contains("noobai") {
            Self::Noobai
        } else if lower.contains("flux") {
            Self::Flux
        } else if lower.contains("sdxl") {
            Self::Sdxl
        } else if lower.contains("sd15") || lower.contains("sd_1.5") || lower.contains("sd-1.5") {
            Self::Sd15
        } else {
            Self::Unknown
        }
    }

    /// Returns the CivitAI `baseModels` query parameter string for this base model.
    ///
    /// Returns `None` for `Unknown` (omit the `&baseModels=` parameter).
    pub fn to_civitai_str(&self) -> Option<&'static str> {
        match self {
            Self::Pony => Some("Pony"),
            Self::Illustrious => Some("Illustrious"),
            Self::Noobai => Some("NoobAI"),
            Self::Sdxl => Some("SDXL 1.0"),
            Self::Sd15 => Some("SD 1.5"),
            Self::Flux => Some("Flux.1 D"),
            Self::Unknown => None,
        }
    }
}

// =============================================================================
// ModelSearchResult struct
// =============================================================================

/// Unified search result returned by `vdsl_model_search` across all scopes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ModelSearchResult {
    /// Human-readable model name (filename or model title).
    pub name: String,

    /// Model type (checkpoint, lora, vae, etc.).
    pub model_type: ModelType,

    /// Base model / architecture.
    pub base: BaseModel,

    /// Search scope that produced this result (remote / archive / pod).
    pub scope: Scope,

    /// File size in bytes, if known.
    pub size_bytes: Option<u64>,

    /// Location string (URL, bucket path, or pod filesystem path).
    pub location: String,

    /// Suggested obtain command (e.g. `vdsl_download source=cv:ID target=checkpoints`).
    /// `None` when the model is already available (scope=pod).
    pub obtain: Option<String>,

    /// Free-form metadata payload (CivitAI version object, rclone entry, etc.).
    pub metadata: serde_json::Value,
}

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

    // =========================================================================
    // T1 — Happy path: BaseModel::from_filename
    // =========================================================================

    #[test]
    fn base_model_from_filename_happy_path() {
        assert_eq!(
            BaseModel::from_filename("pony_v6.safetensors"),
            BaseModel::Pony
        );
        assert_eq!(
            BaseModel::from_filename("illustrious_xl.safetensors"),
            BaseModel::Illustrious
        );
        assert_eq!(
            BaseModel::from_filename("noobAI_v1.safetensors"),
            BaseModel::Noobai
        );
        assert_eq!(
            BaseModel::from_filename("flux_schnell.safetensors"),
            BaseModel::Flux
        );
        assert_eq!(
            BaseModel::from_filename("sdxl_base_1.0.safetensors"),
            BaseModel::Sdxl
        );
        assert_eq!(
            BaseModel::from_filename("sd15_realisticVision.safetensors"),
            BaseModel::Sd15
        );
    }

    // T2 — Edge: case-insensitive, exact unknown, alternative sd15 variants

    #[test]
    fn base_model_from_filename_case_insensitive() {
        assert_eq!(
            BaseModel::from_filename("PONY_V6.SAFETENSORS"),
            BaseModel::Pony
        );
        assert_eq!(
            BaseModel::from_filename("FLUX_DEV.SAFETENSORS"),
            BaseModel::Flux
        );
    }

    #[test]
    fn base_model_from_filename_unknown_for_unrecognized() {
        assert_eq!(
            BaseModel::from_filename("v1-5-pruned.ckpt"),
            BaseModel::Unknown
        );
        assert_eq!(BaseModel::from_filename(""), BaseModel::Unknown);
    }

    #[test]
    fn base_model_from_filename_sd15_variants() {
        assert_eq!(
            BaseModel::from_filename("sd_1.5_model.safetensors"),
            BaseModel::Sd15
        );
        assert_eq!(
            BaseModel::from_filename("sd-1.5-model.safetensors"),
            BaseModel::Sd15
        );
    }

    // T3 — Error path: Unknown is the fallback, never panics on empty or garbage

    #[test]
    fn base_model_from_filename_no_panic_garbage() {
        // Must not panic on unusual input (ASCII-only garbage string)
        let result = std::panic::catch_unwind(|| BaseModel::from_filename("@@## garbage **!!"));
        // Safety: catch_unwind is test-only, assert is safe after is_ok check
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), BaseModel::Unknown);
    }

    // =========================================================================
    // T1 — Happy path: ModelType::to_civitai_type
    // =========================================================================

    #[test]
    fn model_type_to_civitai_type_happy_path() {
        assert_eq!(ModelType::Checkpoint.to_civitai_type(), Some("Checkpoint"));
        assert_eq!(ModelType::Lora.to_civitai_type(), Some("LORA"));
        assert_eq!(ModelType::Vae.to_civitai_type(), Some("VAE"));
        assert_eq!(
            ModelType::Embedding.to_civitai_type(),
            Some("TextualInversion")
        );
        assert_eq!(ModelType::Controlnet.to_civitai_type(), Some("Controlnet"));
        assert_eq!(ModelType::Upscale.to_civitai_type(), Some("Upscaler"));
    }

    // T2 — Edge: Clip / Unet / Unknown return None

    #[test]
    fn model_type_to_civitai_type_none_for_unsupported() {
        assert_eq!(ModelType::Clip.to_civitai_type(), None);
        assert_eq!(ModelType::Unet.to_civitai_type(), None);
        assert_eq!(ModelType::Unknown.to_civitai_type(), None);
    }

    // =========================================================================
    // T1 — Happy path: ModelType::from_civitai_type
    // =========================================================================

    #[test]
    fn model_type_from_civitai_type_happy_path() {
        assert_eq!(
            ModelType::from_civitai_type("Checkpoint"),
            ModelType::Checkpoint
        );
        assert_eq!(ModelType::from_civitai_type("LORA"), ModelType::Lora);
        assert_eq!(
            ModelType::from_civitai_type("Controlnet"),
            ModelType::Controlnet
        );
        assert_eq!(ModelType::from_civitai_type("VAE"), ModelType::Vae);
        assert_eq!(ModelType::from_civitai_type("Upscaler"), ModelType::Upscale);
        assert_eq!(
            ModelType::from_civitai_type("TextualInversion"),
            ModelType::Embedding
        );
    }

    // T2 — Edge: case-insensitive matching
    #[test]
    fn model_type_from_civitai_type_case_insensitive() {
        assert_eq!(
            ModelType::from_civitai_type("checkpoint"),
            ModelType::Checkpoint
        );
        assert_eq!(ModelType::from_civitai_type("lora"), ModelType::Lora);
        assert_eq!(
            ModelType::from_civitai_type("CHECKPOINT"),
            ModelType::Checkpoint
        );
    }

    // T3 — Error path: unrecognized or empty returns Unknown
    #[test]
    fn model_type_from_civitai_type_unknown_fallback() {
        assert_eq!(ModelType::from_civitai_type(""), ModelType::Unknown);
        assert_eq!(ModelType::from_civitai_type("FooBar"), ModelType::Unknown);
        assert_eq!(ModelType::from_civitai_type("clip"), ModelType::Unknown);
    }

    // T3 — All() smoke: every concrete variant has a defined as_dir_key

    #[test]
    fn model_type_all_as_dir_key_exhaustive() {
        for t in ModelType::all() {
            let key = t.as_dir_key();
            assert!(
                !key.is_empty(),
                "as_dir_key should never be empty for concrete variant"
            );
        }
        assert_eq!(ModelType::all().len(), 8);
    }

    // =========================================================================
    // T1 — Happy path: TryFrom<&str> for ModelType
    // =========================================================================

    #[test]
    fn model_type_try_from_valid_keys() {
        assert_eq!(
            ModelType::try_from("checkpoints").unwrap(),
            ModelType::Checkpoint
        );
        assert_eq!(ModelType::try_from("loras").unwrap(), ModelType::Lora);
        assert_eq!(
            ModelType::try_from("controlnet").unwrap(),
            ModelType::Controlnet
        );
        assert_eq!(ModelType::try_from("vae").unwrap(), ModelType::Vae);
        assert_eq!(ModelType::try_from("upscale").unwrap(), ModelType::Upscale);
        assert_eq!(
            ModelType::try_from("embeddings").unwrap(),
            ModelType::Embedding
        );
        assert_eq!(ModelType::try_from("clip").unwrap(), ModelType::Clip);
        assert_eq!(ModelType::try_from("unet").unwrap(), ModelType::Unet);
    }

    // T2 — Edge: dir name (not key) is not a valid key

    #[test]
    fn model_type_try_from_dir_name_is_not_key() {
        // "upscale_models" is the dir name; "upscale" is the key
        assert!(ModelType::try_from("upscale_models").is_err());
        // "checkpoints" is both key and dir name — should succeed
        assert!(ModelType::try_from("checkpoints").is_ok());
    }

    // T3 — Error path: unknown key returns Err with the unknown string

    #[test]
    fn model_type_try_from_unknown_key_returns_err() {
        let err = ModelType::try_from("foobar").unwrap_err();
        // Error message should reference the unknown key
        assert!(err.to_string().contains("foobar"));
    }

    #[test]
    fn model_type_try_from_empty_key_returns_err() {
        assert!(ModelType::try_from("").is_err());
    }

    // =========================================================================
    // T1 — Happy path: ModelType::from_archive_path_and_size — dir detection
    // =========================================================================

    #[test]
    fn model_type_from_archive_path_dir_segment() {
        assert_eq!(
            ModelType::from_archive_path_and_size("/models/checkpoints/foo.safetensors", None),
            ModelType::Checkpoint
        );
        assert_eq!(
            ModelType::from_archive_path_and_size("/models/loras/bar.safetensors", None),
            ModelType::Lora
        );
        assert_eq!(
            ModelType::from_archive_path_and_size("/models/upscale_models/esrgan.pth", None),
            ModelType::Upscale
        );
    }

    // T2 — Edge: size fallback when no dir match

    #[test]
    fn model_type_from_archive_path_size_fallback() {
        // Size in CP range (4 GB)
        assert_eq!(
            ModelType::from_archive_path_and_size(
                "somefile.safetensors",
                Some(4 * 1024 * 1024 * 1024)
            ),
            ModelType::Checkpoint
        );
        // Size in LoRA range (100 MB)
        assert_eq!(
            ModelType::from_archive_path_and_size("somefile.safetensors", Some(100 * 1024 * 1024)),
            ModelType::Lora
        );
        // Size in VAE range (600 MB — above LoRA_MAX=500MB, within VAE range 300-800MB)
        assert_eq!(
            ModelType::from_archive_path_and_size("somefile.safetensors", Some(600 * 1024 * 1024)),
            ModelType::Vae
        );
    }

    // T3 — Error path: falls back to Unknown when dir and size give no match

    #[test]
    fn model_type_from_archive_path_unknown_fallback() {
        // No dir match, size 0
        assert_eq!(
            ModelType::from_archive_path_and_size("unknown_file.bin", Some(0)),
            ModelType::Unknown
        );
        // No dir match, no size
        assert_eq!(
            ModelType::from_archive_path_and_size("mystery.bin", None),
            ModelType::Unknown
        );
    }

    // =========================================================================
    // T1 — Happy path: from_comfyui_loader
    // =========================================================================

    #[test]
    fn model_type_from_comfyui_loader_happy_path() {
        assert_eq!(
            ModelType::from_comfyui_loader("CheckpointLoaderSimple"),
            Some(ModelType::Checkpoint)
        );
        assert_eq!(
            ModelType::from_comfyui_loader("LoraLoader"),
            Some(ModelType::Lora)
        );
        assert_eq!(
            ModelType::from_comfyui_loader("VAELoader"),
            Some(ModelType::Vae)
        );
        assert_eq!(
            ModelType::from_comfyui_loader("ControlNetLoader"),
            Some(ModelType::Controlnet)
        );
        assert_eq!(
            ModelType::from_comfyui_loader("UpscaleModelLoader"),
            Some(ModelType::Upscale)
        );
    }

    // T2 — Edge: unknown loader returns None

    #[test]
    fn model_type_from_comfyui_loader_unknown() {
        assert_eq!(ModelType::from_comfyui_loader("KSampler"), None);
        assert_eq!(ModelType::from_comfyui_loader(""), None);
    }

    // =========================================================================
    // ModelSearchResult serialization roundtrip
    // =========================================================================

    #[test]
    fn model_search_result_serializes() {
        let r = ModelSearchResult {
            name: "test_model.safetensors".to_string(),
            model_type: ModelType::Lora,
            base: BaseModel::Sdxl,
            scope: Scope::Remote,
            size_bytes: Some(100 * 1024 * 1024),
            location: "https://example.com/model".to_string(),
            obtain: Some("vdsl_download source=cv:123 target=loras".to_string()),
            metadata: serde_json::json!({"id": 123}),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"lora\""));
        assert!(json.contains("\"sdxl\""));
        assert!(json.contains("\"remote\""));
    }

    // =========================================================================
    // BaseModel::to_civitai_str
    // =========================================================================

    #[test]
    fn base_model_to_civitai_str_known_variants() {
        assert_eq!(BaseModel::Pony.to_civitai_str(), Some("Pony"));
        assert_eq!(BaseModel::Illustrious.to_civitai_str(), Some("Illustrious"));
        assert_eq!(BaseModel::Noobai.to_civitai_str(), Some("NoobAI"));
        assert_eq!(BaseModel::Sdxl.to_civitai_str(), Some("SDXL 1.0"));
        assert_eq!(BaseModel::Sd15.to_civitai_str(), Some("SD 1.5"));
        assert_eq!(BaseModel::Flux.to_civitai_str(), Some("Flux.1 D"));
    }

    #[test]
    fn base_model_to_civitai_str_unknown_is_none() {
        assert_eq!(BaseModel::Unknown.to_civitai_str(), None);
    }
}
