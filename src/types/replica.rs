use std::collections::HashMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Affinity, Condition, HeaderValue, ResourceRequirements, Toleration};

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
	group = "bes.au",
	version = "v1alpha1",
	kind = "PostgresPhysicalReplica",
	namespaced,
	status = "PostgresPhysicalReplicaStatus",
	shortname = "ppr",
	category = "all",
	printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
	printcolumn = r#"{"name":"Service","type":"string","jsonPath":".status.serviceName"}"#,
	printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPhysicalReplicaSpec {
	/// Reference to a Secret containing kopia repository credentials
	pub kopia_secret_ref: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub snapshot_filter: Option<SnapshotFilter>,

	/// Cron expression for scheduled restores
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schedule: Option<String>,

	/// Random jitter added to scheduled restores
	#[serde(default = "default_schedule_jitter")]
	pub schedule_jitter: String,

	/// Don't restore a new snapshot within this duration of the last restore completing
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub minimum_ttl: Option<String>,

	/// Wait before deleting old restore after switchover
	#[serde(default = "default_switchover_grace_period")]
	pub switchover_grace_period: String,

	/// Username for analytics connections
	#[serde(default = "default_analytics_username")]
	pub analytics_username: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_class: Option<String>,

	/// Override dynamic sizing with a fixed PVC size
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_size_override: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resources: Option<ResourceRequirements>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub service_annotations: Option<HashMap<String, String>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pod_annotations: Option<HashMap<String, String>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub affinity: Option<Affinity>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub tolerations: Vec<Toleration>,

	/// Set database to read-only mode
	#[serde(default = "default_read_only")]
	pub read_only: bool,

	/// Extra lines appended to postgresql.conf (e.g. shared_preload_libraries)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub postgres_extra_config: Option<String>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub notifications: Vec<NotificationConfig>,

	/// Optional overlay database configuration (FDW-based persistent database)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overlay_database: Option<OverlayDatabaseConfig>,
}

fn default_read_only() -> bool {
	true
}
fn default_switchover_grace_period() -> String {
	"5m".to_string()
}

fn default_schedule_jitter() -> String {
	"10m".to_string()
}
fn default_analytics_username() -> String {
	"analytics".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OverlayDatabaseConfig {
	/// PostgreSQL major version for the CNPG cluster (e.g. "17").
	/// If absent, resolved from the CNPG image catalog (see image_catalog).
	/// Falls back to a hardcoded default ("17") if no catalog is available.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub postgres_version: Option<String>,

	/// CNPG image catalog to use for PG version discovery and image resolution.
	/// If absent, defaults to ClusterImageCatalog kind.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub image_catalog: Option<ImageCatalogRef>,

	/// Override for the overlay database PVC size.
	/// If absent, auto-sized: 5Gi + ceil(snapshot_size / 10) rounded up to whole Gi.
	/// Auto-sizing only ever increases (ratchets up), never shrinks.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_size_override: Option<String>,

	/// Storage class for the overlay database PVC
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_class: Option<String>,

	/// Resource requirements for the overlay database pods
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resources: Option<ResourceRequirements>,

	/// Pod affinity/anti-affinity rules for the overlay database
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub affinity: Option<Affinity>,

	/// Tolerations for the overlay database pods
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub tolerations: Vec<Toleration>,

	/// Schema mapping: if provided, only these schemas are imported.
	/// Key = remote schema name, Value = local schema name in overlay DB.
	/// If absent, all user schemas are imported at their original names.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schema_mapping: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ImageCatalogRef {
	/// Name of the image catalog resource
	pub name: String,

	/// Kind: "ClusterImageCatalog" (default) or "ImageCatalog"
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotFilter {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tags: Option<HashMap<String, String>>,

	/// Glob pattern for filtering snapshot hosts
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub host_pattern: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationConfig {
	pub name: String,

	#[serde(default)]
	pub events: Vec<NotificationEvent>,

	/// Include password directly in notification payload
	#[serde(default)]
	pub include_password: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub webhook: Option<WebhookConfig>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub graphql: Option<GraphQLConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum NotificationEvent {
	RestoreComplete,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebhookConfig {
	pub url: String,

	#[serde(default = "default_method")]
	pub method: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub headers: Option<HashMap<String, HeaderValue>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GraphQLConfig {
	pub url: String,
	pub mutation: String,
	pub variables_template: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub headers: Option<HashMap<String, HeaderValue>>,
}

fn default_method() -> String {
	"POST".to_string()
}

// Status types

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPhysicalReplicaStatus {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub phase: Option<ReplicaPhase>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub current_restore: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub previous_restore: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub service_name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub last_restore_completed_at: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub next_scheduled_restore: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub latest_available_snapshot: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connection_info: Option<ConnectionInfo>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub queue_position: Option<u32>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub notifications: Vec<NotificationStatus>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub conditions: Vec<Condition>,

	/// Name of the CNPG Cluster CR for the overlay database
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overlay_cluster_name: Option<String>,

	/// Name of the restore whose schemas are currently imported via FDW
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overlay_fdw_restore: Option<String>,

	/// Current (possibly ratcheted) storage size of the overlay PVC
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overlay_storage_size: Option<String>,

	/// Resolved PG major version used for the overlay cluster
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overlay_postgres_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum ReplicaPhase {
	Pending,
	Restoring,
	Ready,
	Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
	pub host: String,
	pub port: u16,
	pub database: String,
	pub username: String,
	pub password_secret: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationStatus {
	pub name: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub last_sent_at: Option<String>,
	#[serde(default)]
	pub success: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub last_error: Option<String>,
}
