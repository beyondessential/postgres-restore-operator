use std::collections::HashMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, HeaderValue, ResourceRequirements, Toleration};

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
	pub node_selector: Option<HashMap<String, String>>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub tolerations: Vec<Toleration>,

	/// Set database to read-only mode
	#[serde(default = "default_read_only")]
	pub read_only: bool,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub notifications: Vec<NotificationConfig>,
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
