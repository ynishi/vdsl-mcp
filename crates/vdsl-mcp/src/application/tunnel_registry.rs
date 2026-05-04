//! SSH tunnel registry for `vdsl_tunnel_open` / `_close` / `_list`.
//!
//! # Problem
//!
//! Cloudflare proxy URLs issued by RunPod can experience transient instability,
//! causing ComfyUI / vLLM polling to fail mid-generation. An SSH local
//! port-forward tunnel (`ssh -N -L`) provides a stable alternative route that
//! bypasses the proxy layer entirely.
//!
//! # Shape
//!
//! `TunnelRegistry` tracks one active tunnel per pod. Each entry stores the
//! metadata needed to reconnect or inspect the tunnel:
//!
//! - `pod_id` — RunPod pod identifier (registry key)
//! - `service` — logical service name (`"comfyui"`, `"vllm"`, `"raw"`)
//! - `local_port` — OS-assigned loopback port used by the SSH forward
//! - `remote_port` — port on the pod side (e.g. 8188 for ComfyUI)
//! - `ssh_host` / `ssh_port` — RunPod SSH gateway host and port
//! - `started_at_ms` — creation timestamp (unix ms)
//!
//! The `child` field (the live `tokio::process::Child`) is added in Subtask 2.
//! This module establishes the registry skeleton and associated types so that
//! Subtask 2 (ssh spawn) and Subtask 3 (MCP tools) can build on a tested
//! foundation.
//!
//! # Lifecycle
//!
//! - `tunnel_open` → `TunnelRegistry::insert` (Subtask 2 spawns ssh first)
//! - `tunnel_close` → `TunnelRegistry::remove` (Subtask 2 kills child first)
//! - `tunnel_list` → `TunnelRegistry::list` (snapshot read, no async needed)
//!
//! # Concurrency
//!
//! [`TunnelRegistry`] is `Send + Sync` via `Arc<RwLock<HashMap<...>>>`.
//! Cloning the registry clones only the `Arc` (cheap). The inner
//! `Arc<Mutex<TunnelHandle>>` per entry lets Subtask 2 update mutable state
//! without blocking concurrent readers. Lock scopes are kept short using the
//! clone-then-release pattern to avoid deadlocks with async callers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::domain::pod::RouteKind;

// ---------------------------------------------------------------------------
// TunnelHandle — data-only skeleton (Subtask 1)
// ---------------------------------------------------------------------------

/// Metadata for one active SSH tunnel.
///
/// **Subtask 1 note**: this struct holds pure data; the `child:
/// tokio::process::Child` field is added in Subtask 2 once the ssh spawn
/// logic is in place.
///
/// # Concurrency
///
/// `TunnelHandle` derives `Clone` so the registry can snapshot entries without
/// holding the inner lock. All fields are cheap to clone (primitives /
/// `String`). The outer `Arc<Mutex<TunnelHandle>>` in the registry allows
/// Subtask 2 to update mutable fields (e.g. child PID) after spawn without
/// blocking concurrent snapshot calls.
///
/// # Panics
///
/// None. No internal synchronisation primitives are used directly by this
/// struct.
#[derive(Debug, Clone, Serialize)]
pub struct TunnelHandle {
    /// RunPod pod identifier — also the registry key.
    pub pod_id: String,
    /// Logical service name: `"comfyui"`, `"vllm"`, or `"raw"`.
    pub service: String,
    /// Loopback port assigned by the OS for the SSH local forward.
    pub local_port: u16,
    /// Port on the pod side (e.g. 8188 for ComfyUI).
    pub remote_port: u16,
    /// RunPod SSH gateway hostname.
    pub ssh_host: String,
    /// RunPod SSH gateway port (typically 22 or a non-standard port).
    pub ssh_port: u16,
    /// Unix-millisecond timestamp of tunnel creation.
    pub started_at_ms: u64,
}

// ---------------------------------------------------------------------------
// TunnelSnapshot — serialisable read-only view
// ---------------------------------------------------------------------------

/// Read-only snapshot of a [`TunnelHandle`] with a resolved [`RouteKind`].
///
/// Used as the element type for [`TunnelRegistry::list`] and as the basis for
/// `vdsl_tunnel_list` JSON output. The [`route`] field shares the same
/// [`RouteKind`] enum used by `PodEndpoint.route`, ensuring that
/// `endpoints[].route` in `vdsl_pod_list` and `vdsl_tunnel_list` output carry
/// identical JSON representations.
///
/// # Concurrency
///
/// `Clone + Serialize` — safe to hand to the MCP tool handler after releasing
/// the registry lock.
///
/// # Panics
///
/// None.
#[derive(Debug, Clone, Serialize)]
pub struct TunnelSnapshot {
    /// RunPod pod identifier.
    pub pod_id: String,
    /// Logical service name.
    pub service: String,
    /// Loopback port used by the SSH forward.
    pub local_port: u16,
    /// Pod-side service port.
    pub remote_port: u16,
    /// SSH gateway host.
    pub ssh_host: String,
    /// SSH gateway port.
    pub ssh_port: u16,
    /// Unix-millisecond creation timestamp.
    pub started_at_ms: u64,
    /// Active routing strategy — always [`RouteKind::SshTunnel`] for live
    /// tunnel entries; exposed here so `vdsl_tunnel_list` and
    /// `endpoints[].route` share the same type.
    pub route: RouteKind,
}

impl TunnelSnapshot {
    fn from_handle(handle: &TunnelHandle) -> Self {
        Self {
            pod_id: handle.pod_id.clone(),
            service: handle.service.clone(),
            local_port: handle.local_port,
            remote_port: handle.remote_port,
            ssh_host: handle.ssh_host.clone(),
            ssh_port: handle.ssh_port,
            started_at_ms: handle.started_at_ms,
            route: RouteKind::SshTunnel,
        }
    }
}

// ---------------------------------------------------------------------------
// TunnelRegistry
// ---------------------------------------------------------------------------

/// In-memory registry that tracks active SSH tunnels keyed by `pod_id`.
///
/// The registry enforces a **1 tunnel per pod** invariant: inserting a new
/// handle for a `pod_id` that already has an entry overwrites the previous
/// entry (same as [`apply_registry`][crate::application::apply_registry]
/// behaviour). Supporting multiple services per pod is deferred to a
/// separate issue.
///
/// # Concurrency
///
/// `TunnelRegistry` is `Send + Sync`. Cloning it is cheap (`Arc` clone only).
/// The inner `RwLock` allows concurrent reads; writes are infrequent (one per
/// `tunnel_open` / `tunnel_close` call). Lock scopes are kept as short as
/// possible using the clone-then-release pattern so that callers holding an
/// `Arc<Mutex<TunnelHandle>>` do not contend on the outer registry lock.
#[derive(Clone, Default)]
pub struct TunnelRegistry {
    inner: Arc<RwLock<HashMap<String, Arc<Mutex<TunnelHandle>>>>>,
}

impl TunnelRegistry {
    /// Create a new, empty registry.
    ///
    /// # Concurrency
    ///
    /// The returned value is `Send + Sync`. It is cheap to clone (wraps an
    /// `Arc`). All operations on the registry are safe to call from multiple
    /// threads or async tasks concurrently.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a tunnel handle into the registry keyed by `handle.pod_id`.
    ///
    /// If a handle for the same `pod_id` already exists it is overwritten.
    /// Returns an `Arc<Mutex<TunnelHandle>>` so the caller (Subtask 2) can
    /// later mutate the handle (e.g. to store the child PID) without holding
    /// the registry lock.
    ///
    /// # Concurrency
    ///
    /// Acquires the write lock for the duration of the `HashMap::insert` call
    /// only (short-lived). If the write lock is poisoned (extremely unlikely
    /// in practice — only happens when a previous writer panicked) the insert
    /// is silently skipped and the handle is still returned to the caller;
    /// behaviour mirrors [`apply_registry`][crate::application::apply_registry].
    ///
    /// # Panics
    ///
    /// None.
    pub fn insert(&self, handle: TunnelHandle) -> Arc<Mutex<TunnelHandle>> {
        let entry = Arc::new(Mutex::new(handle.clone()));
        if let Ok(mut map) = self.inner.write() {
            map.insert(handle.pod_id.clone(), entry.clone());
        }
        entry
    }

    /// Return a snapshot of the handle for `pod_id`, or `None` if absent.
    ///
    /// # Concurrency
    ///
    /// Acquires the read lock briefly to retrieve the `Arc`, then releases it
    /// before locking the inner `Mutex`. This clone-then-release pattern
    /// prevents the read lock from being held while waiting for the inner
    /// mutex, eliminating a potential deadlock with concurrent writers.
    ///
    /// This function is synchronous (no `.await`) because `TunnelHandle` is a
    /// plain data struct and the inner mutex is `std::sync::Mutex` (not
    /// `tokio::sync::Mutex`).
    ///
    /// # Cancel Safety
    ///
    /// Cancel-safe (no `.await`).
    ///
    /// # Panics
    ///
    /// None. Returns `None` on lock poison.
    pub fn snapshot(&self, pod_id: &str) -> Option<TunnelSnapshot> {
        let entry = {
            let map = self.inner.read().ok()?;
            map.get(pod_id).cloned()?
        };
        let guard = entry.lock().ok()?;
        Some(TunnelSnapshot::from_handle(&guard))
    }

    /// Return snapshots of all registered tunnels.
    ///
    /// The order of the returned entries is unspecified (reflects `HashMap`
    /// iteration order).
    ///
    /// # Concurrency
    ///
    /// Acquires the read lock briefly to clone the `Arc` entries, then
    /// releases it before acquiring any inner mutex. Multiple callers can
    /// invoke `list` concurrently without blocking each other.
    ///
    /// # Cancel Safety
    ///
    /// Cancel-safe (no `.await`).
    ///
    /// # Panics
    ///
    /// None. Returns an empty `Vec` on lock poison.
    pub fn list(&self) -> Vec<TunnelSnapshot> {
        let entries: Vec<Arc<Mutex<TunnelHandle>>> = {
            match self.inner.read() {
                Ok(map) => map.values().cloned().collect(),
                Err(_) => return Vec::new(),
            }
        };
        entries
            .iter()
            .filter_map(|e| e.lock().ok().map(|g| TunnelSnapshot::from_handle(&g)))
            .collect()
    }

    /// Remove the tunnel entry for `pod_id`.
    ///
    /// Idempotent: removing a `pod_id` that is not in the registry is a no-op.
    ///
    /// # Concurrency
    ///
    /// Acquires the write lock for the duration of the `HashMap::remove` call
    /// only. Lock poison is silently ignored (same as [`insert`][Self::insert]).
    ///
    /// # Cancel Safety
    ///
    /// Cancel-safe (no `.await`).
    ///
    /// # Panics
    ///
    /// None.
    pub fn remove(&self, pod_id: &str) {
        if let Ok(mut map) = self.inner.write() {
            map.remove(pod_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

#[allow(dead_code)] // used in Subtask 2 when TunnelHandle is created via open()
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handle(pod_id: &str) -> TunnelHandle {
        TunnelHandle {
            pod_id: pod_id.to_string(),
            service: "comfyui".to_string(),
            local_port: 7100,
            remote_port: 8188,
            ssh_host: "ssh.runpod.io".to_string(),
            ssh_port: 22222,
            started_at_ms: now_ms(),
        }
    }

    #[test]
    fn insert_snapshot_round_trip() {
        let reg = TunnelRegistry::new();
        let h = make_handle("pod_abc");
        reg.insert(h);
        let snap = reg.snapshot("pod_abc").expect("entry present");
        assert_eq!(snap.pod_id, "pod_abc");
        assert_eq!(snap.service, "comfyui");
        assert_eq!(snap.local_port, 7100);
        assert_eq!(snap.remote_port, 8188);
        assert_eq!(snap.ssh_host, "ssh.runpod.io");
        assert_eq!(snap.ssh_port, 22222);
        assert_eq!(snap.route, RouteKind::SshTunnel);
    }

    #[test]
    fn list_returns_all_entries() {
        let reg = TunnelRegistry::new();
        reg.insert(make_handle("pod_1"));
        reg.insert(make_handle("pod_2"));
        reg.insert(make_handle("pod_3"));
        let list = reg.list();
        assert_eq!(list.len(), 3);
        let mut ids: Vec<&str> = list.iter().map(|s| s.pod_id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["pod_1", "pod_2", "pod_3"]);
    }

    #[test]
    fn remove_drops_entry() {
        let reg = TunnelRegistry::new();
        reg.insert(make_handle("pod_xyz"));
        assert!(reg.snapshot("pod_xyz").is_some());
        reg.remove("pod_xyz");
        assert!(reg.snapshot("pod_xyz").is_none());
        // list is also empty
        assert!(reg.list().is_empty());
    }

    #[test]
    fn snapshot_unknown_returns_none() {
        let reg = TunnelRegistry::new();
        assert!(reg.snapshot("nonexistent").is_none());
    }

    #[test]
    fn insert_overwrites_existing_pod_id() {
        let reg = TunnelRegistry::new();
        let h1 = make_handle("pod_dup");
        let mut h2 = make_handle("pod_dup");
        h2.service = "vllm".to_string();
        h2.local_port = 9000;
        reg.insert(h1);
        reg.insert(h2);
        // Only the most recent insert survives
        let snap = reg.snapshot("pod_dup").expect("present");
        assert_eq!(snap.service, "vllm");
        assert_eq!(snap.local_port, 9000);
        assert_eq!(reg.list().len(), 1);
    }

    #[test]
    fn remove_idempotent() {
        let reg = TunnelRegistry::new();
        // remove on an empty registry must not panic
        reg.remove("ghost");
        reg.insert(make_handle("pod_a"));
        reg.remove("pod_a");
        reg.remove("pod_a"); // second remove — no-op
        assert!(reg.snapshot("pod_a").is_none());
    }

    #[test]
    fn snapshot_route_is_ssh_tunnel() {
        let reg = TunnelRegistry::new();
        reg.insert(make_handle("pod_route"));
        let snap = reg.snapshot("pod_route").unwrap();
        assert_eq!(snap.route, RouteKind::SshTunnel);
    }
}
