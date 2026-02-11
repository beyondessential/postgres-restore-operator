use std::{
	borrow::Cow,
	collections::{BTreeMap, HashMap},
};

use jiff::Span;
use k8s_openapi::{
	api::core::v1::{Affinity, ResourceRequirements, SecretReference, Toleration},
	apimachinery::pkg::{
		api::resource::Quantity,
		apis::meta::v1::{Condition, OwnerReference, Time},
	},
};
use kube::{CustomResource, ResourceExt as _};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};

use crate::util::TimeSpan;

use super::HeaderValue;

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
	group = "pgro.bes.au",
	version = "v1alpha1",
	kind = "PostgresPhysicalReplica",
	namespaced,
	status = "PostgresPhysicalReplicaStatus",
	shortname = "ppr",
	category = "all",
	printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
	printcolumn = r#"{"name":"Service","type":"string","jsonPath":".status.serviceName"}"#,
	printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#,
	printcolumn = r#"{"name":"Next restore","type":"date","jsonPath":".status.nextScheduledRestore"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPhysicalReplicaSpec {
	/// Reference to a Secret containing kopia repository credentials
	pub kopia_secret_ref: SecretReference,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub snapshot_filter: Option<SnapshotFilter>,

	/// Cron expression for scheduled restores
	pub schedule: String,

	/// Random jitter added to scheduled restores
	#[serde(default = "default_schedule_jitter")]
	pub schedule_jitter: TimeSpan,

	/// Don't restore a new snapshot within this duration of the last restore completing
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub minimum_ttl: Option<TimeSpan>,

	/// Wait before deleting old restore after switchover
	#[serde(default = "default_switchover_grace_period")]
	pub switchover_grace_period: TimeSpan,

	/// Username for analytics connections
	#[serde(default = "default_analytics_username")]
	pub analytics_username: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_class: Option<String>,

	/// Override dynamic sizing with a fixed PVC size
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_size_override: Option<Quantity>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resources: Option<ResourceRequirements>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub service_annotations: Option<BTreeMap<String, String>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pod_annotations: Option<BTreeMap<String, String>>,

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
fn default_switchover_grace_period() -> TimeSpan {
	TimeSpan(Span::new().minutes(5))
}
fn default_schedule_jitter() -> TimeSpan {
	TimeSpan(Span::new().minutes(10))
}
fn default_analytics_username() -> String {
	"analytics".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OverlayDatabaseConfig {
	/// PostgreSQL major version for the CNPG cluster.
	/// If absent, resolved from the CNPG image catalog (see image_catalog).
	/// Falls back to a hardcoded default (17) if no catalog is available.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub postgres_version: Option<u32>,

	/// CNPG image catalog to use for PG version discovery and image resolution.
	/// If absent, defaults to ClusterImageCatalog kind.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub image_catalog: Option<ImageCatalogRef>,

	/// Override for the overlay database PVC size.
	/// If absent, auto-sized: 5Gi + ceil(snapshot_size / 10) rounded up to whole Gi.
	/// Auto-sizing only ever increases (ratchets up), never shrinks.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub storage_size_override: Option<Quantity>,

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

	/// Annotations to apply to the overlay database's -rw Service
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub service_annotations: Option<BTreeMap<String, String>>,

	/// Schema mapping: if provided, only these schemas are imported.
	/// Key = remote schema name, Value = local schema name in overlay DB.
	/// If absent, all user schemas are imported at their original names.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schema_mapping: Option<HashMap<String, String>>,

	/// Include GENERATED expressions when importing foreign schemas.
	/// Requires that all functions used in generated columns exist on the
	/// overlay database. Defaults to false.
	#[serde(default)]
	pub import_generated: bool,
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

	/// Glob pattern for filtering snapshot descriptions
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub description_pattern: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "target")]
pub enum NotificationConfig {
	Webhook(WebhookConfig),
	GraphQL(GraphQLConfig),
}

impl JsonSchema for NotificationConfig {
	fn inline_schema() -> bool {
		true
	}

	fn schema_name() -> Cow<'static, str> {
		"NotificationConfig".into()
	}

	fn json_schema(generator: &mut SchemaGenerator) -> Schema {
		let header_schema = generator.subschema_for::<Option<HashMap<String, HeaderValue>>>();
		json_schema!({
			"type": "object",
			"required": ["target", "url"],
			"properties": {
				"target": {
					"type": "string",
					"enum": ["webhook", "graphQL"]
				},
				"url": { "type": "string" },
				"method": { "type": "string" },
				"mutation": { "type": "string" },
				"variablesTemplate": { "type": "string" },
				"headers": header_schema,
				"includePassword": { "type": "boolean" }
			}
		})
	}
}

impl NotificationConfig {
	pub fn name(&self) -> String {
		match self {
			NotificationConfig::Webhook(WebhookConfig { url, method, .. }) => {
				format!("{method} {url}")
			}
			NotificationConfig::GraphQL(GraphQLConfig { url, .. }) => format!("GraphQL {url}"),
		}
	}

	pub fn include_password(&self) -> bool {
		match self {
			NotificationConfig::Webhook(WebhookConfig {
				include_password, ..
			}) => *include_password,
			NotificationConfig::GraphQL(GraphQLConfig {
				include_password, ..
			}) => *include_password,
		}
	}
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebhookConfig {
	pub url: String,

	#[serde(default = "default_method")]
	pub method: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub headers: Option<HashMap<String, HeaderValue>>,

	/// Include password directly in notification payload
	#[serde(default)]
	pub include_password: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GraphQLConfig {
	pub url: String,
	pub mutation: String,
	pub variables_template: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub headers: Option<HashMap<String, HeaderValue>>,

	/// Include password directly in notification payload
	#[serde(default)]
	pub include_password: bool,
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
	pub last_restore_completed_at: Option<Time>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub next_scheduled_restore: Option<Time>,

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

	/// Hash of the schedule inputs used to compute `nextScheduledRestore`,
	/// so we only recompute when the inputs actually change.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schedule_input_hash: Option<String>,

	/// Current (possibly ratcheted) storage size of the overlay PVC
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overlay_storage_size: Option<Quantity>,

	/// Resolved PG major version used for the overlay cluster
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub overlay_postgres_version: Option<u32>,
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
	pub last_sent_at: Option<Time>,
	#[serde(default)]
	pub success: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub last_error: Option<String>,
}

impl PostgresPhysicalReplica {
	pub fn ns(&self) -> String {
		self.namespace()
			.expect("PostgresPhysicalReplica is a namespaced resource")
	}

	pub fn owner_reference(&self) -> OwnerReference {
		OwnerReference {
			api_version: "pgro.bes.au/v1alpha1".to_string(),
			kind: "PostgresPhysicalReplica".to_string(),
			name: self.name_any(),
			uid: self.uid().unwrap_or_default(),
			controller: Some(true),
			block_owner_deletion: Some(true),
		}
	}

	pub fn creds_secret_name(&self) -> String {
		format!("{name}-creds", name = self.name_any())
	}
}
