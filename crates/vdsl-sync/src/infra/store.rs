//! Remote endpoint configuration for sync persistence.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::location::LocationId;

/// Remote endpoint configuration stored in the sync database.
///
/// Path resolution (remote root) is handled by [`TransferRoute`](crate::application::route::TransferRoute),
/// not by this struct. `RemoteConfig` is purely for persistence metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Location identifier.
    pub location_id: LocationId,
    /// Backend type name: "rclone", "comfyui", "ssh_exec", "s3", ...
    pub backend: String,
    /// Backend-specific configuration (JSON).
    pub config: serde_json::Value,
    /// Registration timestamp.
    pub created_at: DateTime<Utc>,
}
