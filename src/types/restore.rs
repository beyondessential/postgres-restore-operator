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

	/// Calculated PVC size (snapshot_size * 1.1)
	pub storage_size: Quantity,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresPhysicalRestoreStatus {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub phase: Option<RestorePhase>,

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
	pub completed_at: Option<Time>,
}

impl PostgresPhysicalRestore {
	pub fn ns(&self) -> String {
		self.namespace()
			.expect("PostgresPhysicalRestore is a namespaced resource")
	}
}
