use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Condition;

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
	printcolumn = r#"{"name":"Replica","type":"string","jsonPath":".spec.replica"}"#,
	printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPhysicalRestoreSpec {
	/// Reference to parent PostgresPhysicalReplica
	pub replica: String,

	/// Kopia snapshot ID to restore
	pub snapshot: String,

	/// Size of the snapshot from kopia metadata
	pub snapshot_size: String,

	/// Calculated PVC size (snapshot_size * 1.1)
	pub storage_size: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPhysicalRestoreStatus {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub phase: Option<RestorePhase>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub postgres_version: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub created_at: Option<String>,

	/// When restore job completed
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub restored_at: Option<String>,

	/// When service switched to this restore
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub activated_at: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub restore_job: Option<JobStatus>,

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
	Ready,
	Switching,
	Active,
	Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
	pub name: String,
	pub phase: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub completed_at: Option<String>,
}
