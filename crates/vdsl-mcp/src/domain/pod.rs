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

use serde::{Deserialize, Serialize};

/// Route kind for a pod endpoint — how traffic reaches the service.
///
/// Serializes to kebab-case JSON strings so downstream agents can compare
/// without risk of PascalCase / snake_case drift.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RouteKind {
    /// Traffic is forwarded through an SSH local port-forward tunnel.
    SshTunnel,
    /// Traffic is forwarded through the Cloudflare proxy URL provided by RunPod.
    CloudflareProxy,
    /// Traffic goes directly to the host (no tunnel, no proxy).
    Direct,
}

/// A single resolved endpoint for a running pod service.
///
/// Used in `vdsl_pod_list` `endpoints[]` output and `vdsl_tunnel_list` JSON.
/// The [`route`] field uses [`RouteKind`] so both outputs share the same
/// serialized representation without string comparison risk.
#[derive(Debug, Clone, Serialize)]
pub struct PodEndpoint {
    /// Service name, e.g. `"comfyui"`, `"vllm"`, `"raw"`.
    pub service: String,
    /// Resolved URL: `http://127.0.0.1:{port}` (ssh-tunnel) or
    /// `https://{pod_id}-{port}.proxy.runpod.net` (cloudflare-proxy).
    pub url: String,
    /// Active routing strategy for this endpoint.
    pub route: RouteKind,
    /// Local port used when `route == SshTunnel`, otherwise `None`.
    pub local_port: Option<u16>,
}

/// Build `Vec<PodEndpoint>` from pre-resolved SSH info per pod.
///
/// This inner function is separated from the async outer so that unit tests
/// can inject pre-resolved `ssh_infos` without requiring a real `RunPodCli`.
///
/// Routing logic (Crux 2 + Crux 3):
/// - `ssh_info = Some(_)` **and** registry has an active entry → [`RouteKind::SshTunnel`].
/// - `ssh_info = None` → [`RouteKind::CloudflareProxy`] (silent fallback, Crux 2).
///
/// `snapshots` must be cloned **before** this function is called (ensures the
/// outer function releases the registry read lock before any `.await` point,
/// Outline §4-1-1 K-4).
pub(crate) fn build_endpoints(
    pods: &[serde_json::Value],
    snapshots: &[crate::application::tunnel_registry::TunnelSnapshot],
    ssh_infos: &[Option<crate::infra::runpod_cli::PodSshInfo>],
) -> Vec<PodEndpoint> {
    let mut endpoints = Vec::with_capacity(pods.len());

    for (pod, ssh_info) in pods.iter().zip(ssh_infos.iter()) {
        let pod_id = match pod["id"].as_str() {
            Some(id) => id,
            None => continue,
        };

        let route = match ssh_info {
            Some(_) => {
                // SSH info available. Use SshTunnel if the registry has an active entry.
                if snapshots.iter().any(|s| s.pod_id == pod_id) {
                    RouteKind::SshTunnel
                } else {
                    RouteKind::CloudflareProxy
                }
            }
            None => {
                // Pod not reachable via SSH — silent Cloudflare fallback (Crux 2).
                RouteKind::CloudflareProxy
            }
        };

        let (url, local_port) = match route {
            RouteKind::SshTunnel => {
                // Find the matching snapshot for this pod.
                let snap = snapshots.iter().find(|s| s.pod_id == pod_id);
                match snap {
                    Some(s) => (
                        format!("http://127.0.0.1:{}", s.local_port),
                        Some(s.local_port),
                    ),
                    None => {
                        // Race between list() and route decision — degrade gracefully.
                        (crate::infra::comfyui_client::proxy_url(pod_id, 8188), None)
                    }
                }
            }
            RouteKind::CloudflareProxy | RouteKind::Direct => {
                (crate::infra::comfyui_client::proxy_url(pod_id, 8188), None)
            }
        };

        // Service name from registry snapshot when available; default "comfyui".
        let service = snapshots
            .iter()
            .find(|s| s.pod_id == pod_id)
            .map(|s| s.service.clone())
            .unwrap_or_else(|| "comfyui".to_string());

        endpoints.push(PodEndpoint {
            service,
            url,
            route,
            local_port,
        });
    }

    endpoints
}

/// Extract SSH connection info directly from a pod JSON value without any I/O.
///
/// Reads `desiredStatus`, `publicIp`, and `portMappings["22"]` from the
/// supplied JSON — all fields are present in the array returned by `list_pods`.
/// Returns `None` if the pod is not RUNNING, has no public IP, or has no SSH
/// port mapping.
fn extract_ssh_info_from_pod_json(
    pod: &serde_json::Value,
) -> Option<crate::infra::runpod_cli::PodSshInfo> {
    if pod["desiredStatus"].as_str().unwrap_or("") != "RUNNING" {
        return None;
    }
    let host = pod["publicIp"].as_str().unwrap_or("");
    if host.is_empty() {
        return None;
    }
    let port = pod["portMappings"]["22"]
        .as_u64()
        .or_else(|| {
            pod["portMappings"]
                .as_object()
                .and_then(|m| m.get("22"))
                .and_then(|v| v.as_u64())
        })
        .unwrap_or(0) as u16;
    if port == 0 {
        return None;
    }
    Some(crate::infra::runpod_cli::PodSshInfo {
        host: host.to_string(),
        port,
    })
}

/// Build the `## Endpoints` section appended to `vdsl_pod_list` output.
///
/// Cross-references `tunnel_registry.list()` with SSH info extracted directly
/// from the supplied `pods` array (no additional I/O) to determine the active
/// [`RouteKind`] for each pod:
///
/// - If the tunnel registry has an entry for the pod **and** the pod JSON
///   contains a valid public IP + SSH port, the route is [`RouteKind::SshTunnel`].
/// - Otherwise route is [`RouteKind::CloudflareProxy`] (Crux 2 silent fallback).
///
/// Returns a markdown code-fence string ready to be appended to the pod list
/// text. Returns an empty string on serialization failure.
///
/// # Concurrency
///
/// The registry lock is acquired briefly to clone snapshots, then released
/// before any `.await` calls (clone-then-release pattern, Outline §4-1-1 K-4).
pub async fn format_pod_list_with_endpoints(
    pods: &[serde_json::Value],
    registry: &crate::application::tunnel_registry::TunnelRegistry,
) -> String {
    if pods.is_empty() {
        return String::new();
    }

    // Clone the registry snapshot before any .await (§4-1-1 K-4).
    let snapshots = registry.list().await;

    // Extract ssh_info from the already-fetched pods array — no subprocess spawns.
    let ssh_infos: Vec<Option<crate::infra::runpod_cli::PodSshInfo>> =
        pods.iter().map(extract_ssh_info_from_pod_json).collect();

    let endpoints = build_endpoints(pods, &snapshots, &ssh_infos);

    match serde_json::to_string_pretty(&endpoints) {
        Ok(json) => format!("\n## Endpoints\n\n```json\n{json}\n```\n"),
        Err(e) => {
            tracing::warn!("format_pod_list_with_endpoints serialize failed: {}", e);
            String::new()
        }
    }
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

    #[test]
    fn route_kind_serde_kebab_case() {
        // Serialize
        assert_eq!(
            serde_json::to_string(&RouteKind::SshTunnel).unwrap(),
            "\"ssh-tunnel\""
        );
        assert_eq!(
            serde_json::to_string(&RouteKind::CloudflareProxy).unwrap(),
            "\"cloudflare-proxy\""
        );
        assert_eq!(
            serde_json::to_string(&RouteKind::Direct).unwrap(),
            "\"direct\""
        );

        // Round-trip deserialize
        let decoded: RouteKind = serde_json::from_str("\"ssh-tunnel\"").unwrap();
        assert_eq!(decoded, RouteKind::SshTunnel);
        let decoded: RouteKind = serde_json::from_str("\"cloudflare-proxy\"").unwrap();
        assert_eq!(decoded, RouteKind::CloudflareProxy);
        let decoded: RouteKind = serde_json::from_str("\"direct\"").unwrap();
        assert_eq!(decoded, RouteKind::Direct);
    }

    #[test]
    fn pod_endpoint_serialize_shape() {
        let ep = PodEndpoint {
            service: "comfyui".to_string(),
            url: "http://127.0.0.1:7100".to_string(),
            route: RouteKind::SshTunnel,
            local_port: Some(7100),
        };
        let value: serde_json::Value = serde_json::to_value(&ep).unwrap();
        assert_eq!(value["service"], "comfyui");
        assert_eq!(value["url"], "http://127.0.0.1:7100");
        assert_eq!(value["route"], "ssh-tunnel");
        assert_eq!(value["local_port"], 7100);

        // local_port absent when None
        let ep_proxy = PodEndpoint {
            service: "vllm".to_string(),
            url: "https://pod1-8000.proxy.runpod.net".to_string(),
            route: RouteKind::CloudflareProxy,
            local_port: None,
        };
        let value2: serde_json::Value = serde_json::to_value(&ep_proxy).unwrap();
        assert_eq!(value2["route"], "cloudflare-proxy");
        assert!(value2["local_port"].is_null());
    }

    // -----------------------------------------------------------------------
    // format_pod_list_with_endpoints tests (via build_endpoints inner fn)
    // -----------------------------------------------------------------------

    use crate::application::tunnel_registry::{TunnelHandle, TunnelRegistry};
    use crate::infra::runpod_cli::PodSshInfo;
    use std::process::Stdio;
    use tokio::process::Command;

    fn make_ssh_info() -> PodSshInfo {
        PodSshInfo {
            host: "1.2.3.4".to_string(),
            port: 22222,
        }
    }

    fn make_tunnel_handle(pod_id: &str, local_port: u16) -> TunnelHandle {
        let child = Command::new("sleep")
            .arg("300")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("sleep must be available in test env");

        TunnelHandle {
            pod_id: pod_id.to_string(),
            service: "comfyui".to_string(),
            local_port,
            remote_port: 8188,
            ssh_host: "ssh.runpod.io".to_string(),
            ssh_port: 22222,
            started_at_ms: 0,
            child,
        }
    }

    // -----------------------------------------------------------------------
    // extract_ssh_info_from_pod_json unit tests
    // -----------------------------------------------------------------------

    /// (a) desiredStatus != "RUNNING" → None
    #[test]
    fn extract_ssh_info_not_running() {
        let pod = json!({
            "desiredStatus": "EXITED",
            "publicIp": "1.2.3.4",
            "portMappings": { "22": 37040 }
        });
        assert!(extract_ssh_info_from_pod_json(&pod).is_none());
    }

    /// (b) publicIp empty → None
    #[test]
    fn extract_ssh_info_no_public_ip() {
        let pod = json!({
            "desiredStatus": "RUNNING",
            "publicIp": "",
            "portMappings": { "22": 37040 }
        });
        assert!(extract_ssh_info_from_pod_json(&pod).is_none());
    }

    /// (c) portMappings["22"] == 0 / missing → None
    #[test]
    fn extract_ssh_info_missing_ssh_port() {
        let pod_zero = json!({
            "desiredStatus": "RUNNING",
            "publicIp": "1.2.3.4",
            "portMappings": { "22": 0 }
        });
        assert!(extract_ssh_info_from_pod_json(&pod_zero).is_none());

        let pod_missing = json!({
            "desiredStatus": "RUNNING",
            "publicIp": "1.2.3.4",
            "portMappings": {}
        });
        assert!(extract_ssh_info_from_pod_json(&pod_missing).is_none());
    }

    /// (d) full RUNNING pod → Some(PodSshInfo { host, port })
    #[test]
    fn extract_ssh_info_running_pod() {
        let pod = json!({
            "desiredStatus": "RUNNING",
            "publicIp": "1.2.3.4",
            "portMappings": { "22": 37040 }
        });
        let info = extract_ssh_info_from_pod_json(&pod).expect("should produce Some");
        assert_eq!(info.host, "1.2.3.4");
        assert_eq!(info.port, 37040u16);
    }

    /// AC test 1: registry has open entry + ssh_info=Some → route="ssh-tunnel" (Crux 3)
    #[tokio::test]
    async fn format_pod_list_with_endpoints_active() {
        let registry = TunnelRegistry::new();
        registry.insert(make_tunnel_handle("pod_active", 7100));
        let snapshots = registry.list().await;

        let pods = vec![json!({"id": "pod_active"})];
        let ssh_infos = vec![Some(make_ssh_info())];

        let endpoints = build_endpoints(&pods, &snapshots, &ssh_infos);

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].route, RouteKind::SshTunnel);
        assert_eq!(endpoints[0].local_port, Some(7100));
        assert!(endpoints[0].url.contains("127.0.0.1:7100"));
    }

    /// AC test 2: no registry entry and ssh_info=None → route="cloudflare-proxy" (Crux 2 + Crux 3)
    #[tokio::test]
    async fn format_pod_list_with_endpoints_fallback() {
        let registry = TunnelRegistry::new();
        let snapshots = registry.list().await;

        let pods = vec![json!({"id": "pod_fallback"})];
        let ssh_infos: Vec<Option<PodSshInfo>> = vec![None];

        let endpoints = build_endpoints(&pods, &snapshots, &ssh_infos);

        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].route, RouteKind::CloudflareProxy);
        assert!(endpoints[0].local_port.is_none());
    }

    /// AC test 3: all entries carry a `route` field — value derived from actual state, not static
    /// (Crux 3 must_not_simplify: not a static default)
    #[tokio::test]
    async fn format_pod_list_with_endpoints_route_field_present() {
        let registry = TunnelRegistry::new();
        registry.insert(make_tunnel_handle("pod_a", 7200));
        let snapshots = registry.list().await;

        let pods = vec![json!({"id": "pod_a"}), json!({"id": "pod_b"})];
        // pod_a has ssh_info (→ SshTunnel), pod_b has None (→ CloudflareProxy)
        let ssh_infos = vec![Some(make_ssh_info()), None];

        let endpoints = build_endpoints(&pods, &snapshots, &ssh_infos);
        assert_eq!(endpoints.len(), 2);

        // Every entry must have a route field — serialize and check.
        let values: Vec<serde_json::Value> = endpoints
            .iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect();
        for v in &values {
            assert!(
                v.get("route").is_some(),
                "every endpoint entry must have a 'route' field: {v:?}"
            );
            let route_str = v["route"].as_str().expect("route must be a string");
            assert!(
                route_str == "ssh-tunnel"
                    || route_str == "cloudflare-proxy"
                    || route_str == "direct",
                "route must be one of {{ssh-tunnel, cloudflare-proxy, direct}}: got {route_str}"
            );
        }

        // pod_a: SshTunnel (registry has entry + ssh_info=Some)
        assert_eq!(values[0]["route"], "ssh-tunnel");
        // pod_b: CloudflareProxy (ssh_info=None)
        assert_eq!(values[1]["route"], "cloudflare-proxy");
    }

    /// AC test 5: route string in endpoints[] matches RouteKind kebab-case serialize output.
    /// This ensures vdsl_tunnel_list JSON and vdsl_pod_list endpoints[].route are string-compatible.
    #[test]
    fn format_pod_list_with_endpoints_route_kind_consistency() {
        // RouteKind serializes to the same strings used in endpoints[].route.
        let ssh_tunnel_str = serde_json::to_string(&RouteKind::SshTunnel).unwrap();
        let cf_str = serde_json::to_string(&RouteKind::CloudflareProxy).unwrap();
        let direct_str = serde_json::to_string(&RouteKind::Direct).unwrap();

        // Remove surrounding quotes from JSON string literals.
        let ssh_tunnel_str = ssh_tunnel_str.trim_matches('"');
        let cf_str = cf_str.trim_matches('"');
        let direct_str = direct_str.trim_matches('"');

        assert_eq!(ssh_tunnel_str, "ssh-tunnel");
        assert_eq!(cf_str, "cloudflare-proxy");
        assert_eq!(direct_str, "direct");

        // Now verify that build_endpoints output uses exactly these strings.
        let snapshots: Vec<crate::application::tunnel_registry::TunnelSnapshot> = vec![];
        let pods_ssh = vec![json!({"id": "p1"})];
        // ssh_info=None → CloudflareProxy
        let endpoints = build_endpoints(&pods_ssh, &snapshots, &[None]);
        assert_eq!(
            serde_json::to_value(&endpoints[0]).unwrap()["route"].as_str(),
            Some(cf_str),
            "cloudflare-proxy path must match RouteKind serialize"
        );
    }
}
