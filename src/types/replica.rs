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
	/// Reference to a Secret containing kopia repository credentials.
	/// Mutually exclusive with `canopySource`. Exactly one of the two
	/// must be set; the reconciler surfaces `KopiaSecretValid=False`
	/// with reason `SecretRefAndCanopySource` if both (or neither) are
	/// present.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kopia_secret_ref: Option<SecretReference>,

	/// Route kopia through the canopy-mediated proxy sidecar instead of
	/// a static credentials Secret. Mutually exclusive with
	/// `kopiaSecretRef`.
	///
	/// When set:
	/// - the restore + snapshot-list Jobs get the pgro-canopy-proxy
	///   sidecar and dummy AWS keys (kopia talks to `[::1]:<port>`);
	/// - the reconciler skips the snapshot-list step and instead reads
	///   the snapshot to restore from `status.canopyDesiredSnapshotId`
	///   (populated by the canopy worklist syncer);
	/// - the replica is treated as pgro-internal state — the canopy
	///   syncer materialises + tears down these CRs based on canopy's
	///   worklist. Manual edits to a canopy-managed CR are re-asserted
	///   on the next tick.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub canopy_source: Option<CanopySource>,

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

	/// Pin the postgres pod's resources. When set these are used verbatim;
	/// when unset, memory is derived from the snapshot size and floored by
	/// [`resources_floor`].
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resources: Option<ResourceRequirements>,

	/// Lower bound on the snapshot-derived postgres resources, and the source
	/// of CPU (which doesn't scale with data volume). Ignored when
	/// [`resources`] pins the values outright.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resources_floor: Option<ResourceRequirements>,

	/// Cap on the snapshot-derived postgres memory, so a pathological
	/// snapshot can't request a node's worth of memory and sit unschedulable.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub resources_maximum: Option<Quantity>,

	/// How long to wait for the restore's postgres Deployment to become Ready
	/// before failing the restore. Unset derives a budget from the snapshot
	/// size (a larger data dir needs longer to open and replay WAL), floored
	/// at the operator-wide `DEPLOYMENT_READY_TIMEOUT_SECS`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub deployment_ready_timeout: Option<TimeSpan>,

	/// Floor on the postgres pod's `/dev/shm` sizing. When set, the
	/// Deployment builder uses `max(computed, shmSizeFloor)` — computed
	/// comes from [`compute_shm_and_shared_buffers`] driven by
	/// [`resources`]. Useful when the resource-derived value would be
	/// smaller than what a workload's `shared_buffers` needs (analytics
	/// / dbt) without wanting to bump the container's memory request
	/// upward just to raise shm.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub shm_size_floor: Option<Quantity>,

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

	/// Ephemeral replica: once a restore reaches `Active` (postgres came up
	/// healthy and, for canopy replicas, the verification was reported),
	/// tear the restore down instead of keeping it running. The replica CR
	/// stays; it only restores again when a new snapshot is offered (canopy
	/// path) or the schedule next fires (legacy path). Used by the `verify`
	/// intent, whose whole job is "prove the snapshot restores" — keeping
	/// the database idling afterward just wastes cluster resources.
	#[serde(default)]
	pub ephemeral: bool,

	/// Extra lines appended to postgresql.conf (e.g. shared_preload_libraries)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub postgres_extra_config: Option<String>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub notifications: Vec<NotificationConfig>,

	/// List of schema names to migrate from the previous restore to the new restore on each switchover.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub persistent_schemas: Option<Vec<String>>,

	/// Tamanu version whose schema migrations each restore should apply once
	/// healthy, from canopy's worklist entry. Present only while canopy names a
	/// target; each restore snapshots it into its own spec at creation so a
	/// target that moves mid-restore doesn't change what that restore tested.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub migrate_to: Option<crate::types::MigrationTarget>,

	/// Maximum allowed size for the restore PVC. The restore will fail if the
	/// computed size exceeds this limit. Defaults to 2Ti.
	#[serde(default = "default_storage_size_maximum")]
	pub storage_size_maximum: Quantity,
}

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

/// Points a replica at a canopy-declared restore-replica instead of a
/// static kopia Secret. Set via the canopy worklist syncer; humans
/// shouldn't hand-author these — they're managed for you.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CanopySource {
	/// Canopy server-group id (UUID) whose backups this replica restores.
	pub group: String,
	/// Canopy backup type (e.g. `tamanu-postgres`). The
	/// `(consumer, group, type)` external-restore grant on canopy's side
	/// gates access.
	pub r#type: String,
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

	/// Snapshot id the canopy worklist syncer wants restored. Populated
	/// each syncer tick for canopy-sourced replicas (`spec.canopySource`
	/// is set); unused otherwise. The reconciler triggers a new restore
	/// when this differs from the current one.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub canopy_desired_snapshot_id: Option<String>,

	/// Last snapshot id an ephemeral replica (`spec.ephemeral`) verified
	/// and then tore down. After teardown there is no `currentRestore` to
	/// compare against, so this marker is what stops the reconciler from
	/// immediately re-restoring the same snapshot; a restore is only
	/// re-triggered when the desired snapshot differs from this.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub verified_snapshot_id: Option<String>,

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

	/// Phase of schema migration. See [`SchemaMigrationPhase`].
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schema_migration_phase: Option<SchemaMigrationPhase>,

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
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum ReplicaPhase {
	Pending,
	Restoring,
	Ready,
	Failed,
}

/// Lifecycle phase of the operator's schema-migration step that runs
/// during a switchover when `persistent_schemas` is configured on the
/// replica. The serialized form is a flat string matching the historical
/// wire format (`active` / `complete` / `partial` / `timeout-skipped` /
/// `failed: <reason>`) so existing replica status objects round-trip
/// unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaMigrationPhase {
	/// Migration Job is running. The sweep must not delete the source
	/// restore while we're in this state.
	Active,
	/// Migration Job finished cleanly; persistent schemas were carried
	/// across to the new restore.
	Complete,
	/// Migration Job finished but psql logged statement errors (typical
	/// when dbt views reference renamed/dropped upstream columns). Some
	/// persistent_schemas objects may need regenerating upstream.
	Partial,
	/// Migration exceeded the per-cycle wall-clock budget (20% of the
	/// cron interval). The operator dropped the persistent_schemas on
	/// the new restore and proceeded to switchover anyway — a usable
	/// replica beats carrying the schema through indefinitely. The next
	/// cycle re-attempts migration if the schemas have regenerated.
	TimeoutSkipped,
	/// Migration Job failed. The old restore stays Active; the new
	/// restore stays in Switching. The wrapped string is the reason
	/// surfaced from the Job's callback body (or "no callback received").
	Failed(String),
}

impl SchemaMigrationPhase {
	/// True for every phase except [`Self::Active`]. Used by the sweep
	/// gate: as long as the migration isn't currently running, deleting
	/// the previous Active restore is safe (nothing depends on it being
	/// around). Coded as "not Active" rather than enumerating terminal
	/// variants so adding a new variant doesn't risk silently
	/// reintroducing the deadlock that originally motivated this enum.
	pub fn is_settled(&self) -> bool {
		!matches!(self, Self::Active)
	}
}

impl std::fmt::Display for SchemaMigrationPhase {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Active => f.write_str("active"),
			Self::Complete => f.write_str("complete"),
			Self::Partial => f.write_str("partial"),
			Self::TimeoutSkipped => f.write_str("timeout-skipped"),
			Self::Failed(reason) => write!(f, "failed: {reason}"),
		}
	}
}

impl std::str::FromStr for SchemaMigrationPhase {
	type Err = String;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s {
			"active" => Ok(Self::Active),
			"complete" => Ok(Self::Complete),
			"partial" => Ok(Self::Partial),
			"timeout-skipped" => Ok(Self::TimeoutSkipped),
			other => {
				if let Some(reason) = other.strip_prefix("failed:") {
					Ok(Self::Failed(reason.trim().to_string()))
				} else {
					Err(format!("unknown schema migration phase: {other:?}"))
				}
			}
		}
	}
}

impl Serialize for SchemaMigrationPhase {
	fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
		s.serialize_str(&self.to_string())
	}
}

impl<'de> Deserialize<'de> for SchemaMigrationPhase {
	fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
		let s = String::deserialize(d)?;
		s.parse().map_err(serde::de::Error::custom)
	}
}

impl JsonSchema for SchemaMigrationPhase {
	fn schema_name() -> Cow<'static, str> {
		"SchemaMigrationPhase".into()
	}

	fn json_schema(_: &mut SchemaGenerator) -> Schema {
		json_schema!({
			"type": "string",
			"description": "Schema migration phase: 'active' (Job running), 'complete' (succeeded), 'partial' (succeeded with statement errors), 'timeout-skipped' (budget exceeded; persistent schemas dropped and switchover proceeded), or 'failed: <reason>' (Job failed).",
		})
	}
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

	/// Name of the operator-materialised Secret that holds the canopy path's
	/// dummy AWS keys + canopy-provided bucket/region/prefix/repo-password.
	/// Only meaningful when `spec.canopy_source` is set — the canopy syncer
	/// creates this Secret before the reconciler spawns a restore Job.
	pub fn canopy_creds_secret_name(&self) -> String {
		format!("{name}-canopy-creds", name = self.name_any())
	}

	/// Derive the credential source for kopia Jobs — the reconciler has
	/// already validated exactly one of `kopia_secret_ref` / `canopy_source`
	/// is set before we reach any callsite that needs this, so the
	/// `.expect` never fires in practice.
	pub fn kopia_source(&self) -> crate::kopia::KopiaSource {
		if let Some(canopy) = &self.spec.canopy_source {
			crate::kopia::KopiaSource::CanopyProxy {
				secret_name: self.canopy_creds_secret_name(),
				group: canopy.group.clone(),
				backup_type: canopy.r#type.clone(),
			}
		} else {
			let secret_name = self
				.spec
				.kopia_secret_ref
				.as_ref()
				.and_then(|r| r.name.clone())
				.expect("kopia_source called with neither kopia_secret_ref nor canopy_source set");
			crate::kopia::KopiaSource::Secret { secret_name }
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn schema_migration_phase_roundtrips_terminal_variants() {
		for phase in [
			SchemaMigrationPhase::Active,
			SchemaMigrationPhase::Complete,
			SchemaMigrationPhase::Partial,
			SchemaMigrationPhase::TimeoutSkipped,
		] {
			let s = serde_json::to_string(&phase).expect("serialize");
			let back: SchemaMigrationPhase = serde_json::from_str(&s).expect("deserialize");
			assert_eq!(phase, back, "round-trip mismatch for {phase:?}");
		}
	}

	#[test]
	fn schema_migration_phase_failed_preserves_reason() {
		let phase = SchemaMigrationPhase::Failed("connection refused".into());
		let s = serde_json::to_string(&phase).expect("serialize");
		assert_eq!(s, "\"failed: connection refused\"");
		let back: SchemaMigrationPhase = serde_json::from_str(&s).expect("deserialize");
		assert_eq!(phase, back);
	}

	#[test]
	fn schema_migration_phase_wire_strings_match_history() {
		// The wire format is documented in the README and consumed by
		// external tooling (dashboards, alerts). These strings are part
		// of pgro's public contract; renaming them is a breaking change.
		assert_eq!(SchemaMigrationPhase::Active.to_string(), "active");
		assert_eq!(SchemaMigrationPhase::Complete.to_string(), "complete");
		assert_eq!(SchemaMigrationPhase::Partial.to_string(), "partial");
		assert_eq!(
			SchemaMigrationPhase::TimeoutSkipped.to_string(),
			"timeout-skipped"
		);
		assert_eq!(
			SchemaMigrationPhase::Failed("boom".into()).to_string(),
			"failed: boom"
		);
	}

	#[test]
	fn schema_migration_phase_rejects_unknown_string() {
		let r: Result<SchemaMigrationPhase, _> = "what".parse();
		assert!(r.is_err());
	}

	#[test]
	fn schema_migration_phase_is_settled() {
		assert!(!SchemaMigrationPhase::Active.is_settled());
		assert!(SchemaMigrationPhase::Complete.is_settled());
		assert!(SchemaMigrationPhase::Partial.is_settled());
		assert!(SchemaMigrationPhase::TimeoutSkipped.is_settled());
		assert!(SchemaMigrationPhase::Failed("x".into()).is_settled());
	}
}
