/// Format a list of pod JSON values into human-readable text.
///
/// Extracts key fields (id, name, status, GPU, cost) from raw
/// runpod-cli JSON output. Unknown fields are silently ignored.
pub fn format_pod_list(pods: &[serde_json::Value]) -> String {
    if pods.is_empty() {
        return "No pods found.".to_string();
    }

    let mut output = format!("# RunPod Pods ({})\n\n", pods.len());

    for (i, pod) in pods.iter().enumerate() {
        let id = pod["id"].as_str().unwrap_or("?");
        let name = pod["name"].as_str().unwrap_or("unnamed");
        let status = pod["desiredStatus"]
            .as_str()
            .or_else(|| pod["status"].as_str())
            .unwrap_or("unknown");
        let gpu_name = pod["machine"]["gpuDisplayName"]
            .as_str()
            .or_else(|| pod["gpuDisplayName"].as_str())
            .or_else(|| pod["machine"]["gpuTypeId"].as_str())
            .or_else(|| pod["gpuTypeId"].as_str());

        // RunPod CLI list-pods often returns empty machine{} — use gpuCount as fallback
        let gpu_label = match gpu_name {
            Some(name) => name.to_string(),
            None => match pod["gpuCount"].as_u64() {
                Some(n) => format!("GPU x{n}"),
                None => "?".to_string(),
            },
        };

        let cost_str = match pod["costPerHr"].as_f64() {
            Some(c) => format!(", ${:.2}/hr", c),
            None => String::new(),
        };

        output.push_str(&format!(
            "{}. {} — \"{}\" ({}, {}{})\n",
            i + 1,
            id,
            name,
            status,
            gpu_label,
            cost_str,
        ));
    }

    output
}

/// Format a list of network volume JSON values into human-readable text.
pub fn format_volume_list(volumes: &[serde_json::Value]) -> String {
    if volumes.is_empty() {
        return "No network volumes found.".to_string();
    }

    let mut output = format!("# Network Volumes ({})\n\n", volumes.len());

    for (i, vol) in volumes.iter().enumerate() {
        let id = vol["id"].as_str().unwrap_or("?");
        let name = vol["name"].as_str().unwrap_or("unnamed");
        let datacenter = vol["dataCenterId"].as_str().unwrap_or("?");
        let size = vol["size"]
            .as_u64()
            .map_or("?".to_string(), |s| format!("{s} GB"));

        output.push_str(&format!(
            "{}. {} — \"{}\" ({}, {})\n",
            i + 1,
            id,
            name,
            datacenter,
            size,
        ));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn format_empty_list() {
        let result = format_pod_list(&[]);
        assert_eq!(result, "No pods found.");
    }

    #[test]
    fn format_single_pod() {
        let pods = vec![json!({
            "id": "abc123",
            "name": "comfyui-vdsl",
            "desiredStatus": "RUNNING",
            "machine": { "gpuDisplayName": "NVIDIA A40" },
            "costPerHr": 0.39
        })];
        let result = format_pod_list(&pods);
        assert!(result.contains("abc123"));
        assert!(result.contains("comfyui-vdsl"));
        assert!(result.contains("RUNNING"));
        assert!(result.contains("NVIDIA A40"));
        assert!(result.contains("$0.39/hr"));
    }

    #[test]
    fn format_multiple_pods() {
        let pods = vec![
            json!({
                "id": "pod1",
                "name": "comfyui-vdsl",
                "desiredStatus": "RUNNING",
                "machine": { "gpuTypeId": "NVIDIA A40" },
                "costPerHr": 0.39
            }),
            json!({
                "id": "pod2",
                "name": "test-pod",
                "desiredStatus": "EXITED",
                "gpuTypeId": "NVIDIA L4"
            }),
        ];
        let result = format_pod_list(&pods);
        assert!(result.contains("# RunPod Pods (2)"));
        assert!(result.contains("1. pod1"));
        assert!(result.contains("2. pod2"));
        assert!(result.contains("EXITED"));
    }

    #[test]
    fn format_gpu_display_name_preferred_over_type_id() {
        let pods = vec![json!({
            "id": "gpu_test",
            "name": "gpu-pod",
            "desiredStatus": "RUNNING",
            "machine": {
                "gpuDisplayName": "RTX 4090",
                "gpuTypeId": "NVIDIA GeForce RTX 4090"
            }
        })];
        let result = format_pod_list(&pods);
        assert!(result.contains("RTX 4090"));
        assert!(!result.contains("NVIDIA GeForce RTX 4090"));
    }

    #[test]
    fn format_gpu_count_fallback_when_machine_empty() {
        let pods = vec![json!({
            "id": "pod_cli",
            "name": "comfyui-vdsl",
            "desiredStatus": "RUNNING",
            "machine": {},
            "gpuCount": 1,
            "costPerHr": 0.49
        })];
        let result = format_pod_list(&pods);
        assert!(result.contains("GPU x1"));
        assert!(!result.contains("?"));
    }

    #[test]
    fn format_minimal_fields() {
        let pods = vec![json!({"id": "x"})];
        let result = format_pod_list(&pods);
        assert!(result.contains("x"));
        assert!(result.contains("unnamed"));
        assert!(result.contains("unknown"));
    }

    #[test]
    fn format_volume_empty() {
        let result = format_volume_list(&[]);
        assert_eq!(result, "No network volumes found.");
    }

    #[test]
    fn format_volume_single() {
        let vols = vec![json!({
            "id": "vol_dummy001",
            "name": "A40_001",
            "dataCenterId": "EU-SE-1",
            "size": 300
        })];
        let result = format_volume_list(&vols);
        assert!(result.contains("# Network Volumes (1)"));
        assert!(result.contains("vol_dummy001"));
        assert!(result.contains("A40_001"));
        assert!(result.contains("EU-SE-1"));
        assert!(result.contains("300 GB"));
    }
}
