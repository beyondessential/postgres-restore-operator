use k8s_openapi::{
	api::core::v1::LocalObjectReference,
	apimachinery::pkg::{
		api::resource::Quantity,
		apis::meta::v1::{Condition, Time},
	},
};
use kube::{CustomResource, ResourceExt as _};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
	group = "pgro.bes.au",
	version = "v1alpha1",
	kind = "PostgresPhysicalRestore",
	namespaced,
	status = "PostgresPhysicalRestoreStatus",
	shortname = "pprestore",
	category = "all",
	printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
	printcolumn = r#"{"name":"Replica","type":"string","jsonPath":".spec.replica.name"}"#,
	printcolumn = r#"{"name":"Snapshot size","type":"string","jsonPath":".spec.snapshotSize"}"#,
	printcolumn = r#"{"name":"Storage size","type":"string","jsonPath":".spec.storageSize"}"#,
	printcolumn = r#"{"name":"Postgres version","type":"string","jsonPath":".status.postgresVersion"}"#,
	printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPhysicalRestoreSpec {
	/// Reference to parent PostgresPhysicalReplica
	pub replica: LocalObjectReference,

	/// Kopia snapshot ID to restore
	pub snapshot: String,

	/// Size of the snapshot from kopia metadata
	pub snapshot_size: Quantity,

	/// Kopia snapshot start time (ISO 8601)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub snapshot_time: Option<String>,

	/// Calculated PVC size (snapshot_size * 1.1)
	pub storage_size: Quantity,

	/// Tamanu version whose schema migrations to apply once the replica is
	/// healthy, from canopy's worklist entry. Set only for `migrate` intents;
	/// without it the restore is verified and discarded as before.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub migrate_to: Option<MigrationTarget>,

	/// Image that builds a reporting schema against this restore once it is
	/// migrated. Set only for a `reporting-schema` intent; without it the
	/// restore switches over without building anything.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub builder_image: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPhysicalRestoreStatus {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub phase: Option<RestorePhase>,

	/// Canopy run-uuid for this restore run, minted when the restore Job is
	/// created and reused for the run's credential requests and its
	/// verification report so canopy can correlate them. Stored as a string
	/// (parsed to `Uuid` when sent) to avoid pulling a schemars uuid feature
	/// into the CRD schema.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub run_id: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub postgres_version: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub created_at: Option<Time>,

	/// When restore job completed
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub restored_at: Option<Time>,

	/// When service switched to this restore
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub activated_at: Option<Time>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub restore_job: Option<JobStatus>,

	/// The migration job, once the replica is healthy and a `migrateTo` is set.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub migration_job: Option<JobStatus>,

	/// The newest `logs.migrations.logged_at` already in the snapshot when the
	/// migration Job was created: a restored production database carries batches
	/// from its own past upgrades, and only a batch newer than this is the
	/// Job's. Kept as the text postgres rendered and handed back for the
	/// comparison unparsed, since `logged_at` carries the source deployment's
	/// timesync offset and only the replica's own clock can order it. Absent
	/// when the snapshot had no batches, where any batch found is the Job's.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub migration_baseline: Option<String>,

	/// What the migrations did, read back off the replica once the job ends.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub migration_result: Option<MigrationResult>,

	/// What the reporting-schema build did, once its job ends. Absent on a
	/// restore that builds nothing.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schema_build_result: Option<SchemaBuildResult>,

	/// Name of the reporting-schema build Job, for an operator following it.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schema_build_job: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub pvc: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub deployment: Option<String>,

	/// Shared credentials secret (owned by parent replica)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub credentials_secret: Option<String>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum RestorePhase {
	Pending,
	Restoring,
	/// Applying the `migrateTo` version's schema migrations. Only reached when a
	/// migration target is set; otherwise Restoring goes straight to Ready.
	Migrating,
	Ready,
	Switching,
	Active,
	Failed,
}

/// The version a migration test aims at, carried from canopy's worklist entry.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigrationTarget {
	/// Semver of the version, e.g. `2.63.2`. Selects the tamanu image tag.
	pub version: String,

	/// Canopy's id for it, echoed back on the verification report so canopy can
	/// join the result to the version it asked about.
	pub version_id: String,
}

/// Outcome of applying a target version's migrations to a restored replica.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigrationResult {
	/// Whole seconds the migration job took, wall-clock.
	pub total_elapsed_seconds: i64,

	/// The migration that failed, when one did. Its absence is what makes the
	/// result a pass, so canopy reads it as the verdict rather than the job's
	/// exit code.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub failed_migration: Option<String>,

	/// Database size before the migrations ran, and after. The growth between
	/// them is what shows a migration that backfills heavily.
	pub data_bytes_before: i64,
	pub data_bytes_after: i64,

	/// One entry per migration that ran, in the order they ran.
	#[serde(default)]
	pub timings: Vec<MigrationTiming>,
}

/// How long one migration took.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MigrationTiming {
	pub name: String,
	pub elapsed_seconds: i64,
}

/// Outcome of building a reporting schema against a migrated restore.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaBuildResult {
	/// Whether a schema came out of the build. Its absence is what makes the
	/// result a failure, which canopy grades separately from the restore's own
	/// health: a replica can come up soundly and still build nothing.
	pub built: bool,

	/// What went wrong, where it did.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<String>,

	/// Whole seconds the build took, wall-clock.
	pub total_elapsed_seconds: i64,

	/// Size of the schema the build emitted, where it emitted one.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub schema_bytes: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
	pub name: String,
	pub phase: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub completed_at: Option<Time>,
}

impl PostgresPhysicalRestore {
	pub fn ns(&self) -> String {
		self.namespace()
			.expect("PostgresPhysicalRestore is a namespaced resource")
	}
}
