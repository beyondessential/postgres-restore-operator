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

	/// List of schema names to migrate from the previous restore to the new restore on each switchover.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub persistent_schemas: Option<Vec<String>>,

	/// Maximum allowed size for the restore PVC. The restore will fail if the
	/// computed size exceeds this limit. Defaults to 2Ti.
	#[serde(default = "default_storage_size_maximum")]
	pub storage_size_maximum: Quantity,

	/// If set, apply a redaction manifest to the restored data before the
	/// replica becomes eligible for switchover. Requires Postgres 18+ and
	/// the postgresql_anonymizer extension (loaded via image-volume mount
	/// on the restore Pod).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub redaction: Option<RedactionSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RedactionSpec {
	/// HTTP(S) URL of the dbt-style masking manifest. May contain a
	/// `{version}` placeholder, in which case `version` or `versionQuery`
	/// must be set.
	pub manifest_url: String,

	/// Pinned version to substitute into `{version}`. Mutually exclusive
	/// with `versionQuery`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,

	/// SQL query that returns a single text column with the version string.
	/// Run against the restore's main database as the operator's superuser.
	/// Mutually exclusive with `version`.
	///
	/// Example (Tamanu):
	/// `SELECT value FROM local_system_facts WHERE key = 'currentVersion'`
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version_query: Option<String>,

	/// If the manifest URL with the discovered/pinned version 404s, retry
	/// with the major.minor.0 base version.
	#[serde(default)]
	pub version_fallback_to_base: bool,

	/// Override the OCI image used as the source of the
	/// postgresql_anonymizer extension files (mounted as an image volume
	/// on the restore Pod). Defaults to
	/// `registry.gitlab.com/dalibo/postgresql_anon:latest`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub extension_image: Option<String>,
}

pub const DEFAULT_ANON_IMAGE: &str = "registry.gitlab.com/dalibo/postgresql_anon:latest";

fn default_storage_size_maximum() -> Quantity {
	Quantity("2Ti".to_string())
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
pub struct SnapshotFilter {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tags: Option<HashMap<String, String>>,

	/// Glob pattern for filtering snapshot hosts
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub host_pattern: Option<String>,

	/// Glob pattern for filtering snapshot descriptions
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub description_pattern: Option<String>,

	/// Glob pattern for filtering snapshot source paths.
	/// Windows paths are normalised to Unix style (e.g. `D:\Full` → `/D/Full`).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path_pattern: Option<String>,
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

	/// Hash of the schedule inputs used to compute `nextScheduledRestore`,
	/// so we only recompute when the inputs actually change.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schedule_input_hash: Option<String>,

	/// Name of the Job performing schema migration (persistent_schemas only)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schema_migration_job: Option<String>,

	/// Phase of schema migration: pending, active, complete, failed
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schema_migration_phase: Option<String>,

	/// Measured size of persistent schema data from the last successful migration (bytes).
	/// Used to size the next restore PVC.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub persistent_schema_data_size: Option<Quantity>,

	/// Number of consecutive restore failures for this replica.
	/// Reset to 0 on a successful restore. After 3 consecutive failures
	/// the operator stops scheduling new restores until the condition is
	/// cleared (e.g. by a spec change or manual intervention).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub consecutive_restore_failures: Option<u32>,

	/// Phase of redaction for the current restore:
	/// pending, active, complete, partial, failed.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub redaction_phase: Option<String>,

	/// Resolved manifest version used by the last redaction run.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub redaction_version: Option<String>,

	/// Number of columns redacted in the last run.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub redaction_columns_applied: Option<u32>,
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
