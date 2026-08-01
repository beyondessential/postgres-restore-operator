//! Server-side-apply the operator's own CRDs at startup.
//!
//! Same source of truth as the `gen-crds` binary — both call
//! `CustomResourceExt::crd()` on the derived types. The operator applies
//! them on boot so a fresh cluster only needs `kubectl apply -f
//! operator.yaml` to be functional; `crds.yaml` stays around for CI /
//! debugging / operators who prefer to manage CRD lifecycle out-of-band.

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{
	Api, Client, CustomResourceExt, ResourceExt,
	api::{Patch, PatchParams},
};
use tracing::{info, warn};

use crate::{
	error::Result,
	types::{PostgresPhysicalReplica, PostgresPhysicalRestore},
};

/// Field manager used for the SSA patch. Distinct from
/// `postgres-restore-operator` so a manual `kubectl edit` doesn't fight
/// the operator on CRD ownership — humans overriding CRD fields at
/// runtime is legitimate (e.g. temporary preservation policy changes).
const FIELD_MANAGER: &str = "postgres-restore-operator/crd";

/// SSA-apply the two pgro CRDs. Idempotent — safe to call every startup
/// and safe to run concurrently with itself across replicas.
///
/// Fails hard on RBAC / apiserver errors: without the CRDs the operator
/// can't watch anything, so surfacing the error at boot beats a
/// mysteriously-silent operator later.
pub async fn ensure_crds(client: &Client) -> Result<()> {
	let api: Api<CustomResourceDefinition> = Api::all(client.clone());
	for crd in [
		PostgresPhysicalReplica::crd(),
		PostgresPhysicalRestore::crd(),
	] {
		let name = crd.name_any();
		let value = serde_json::to_value(&crd)?;
		match api
			.patch(
				&name,
				&PatchParams::apply(FIELD_MANAGER).force(),
				&Patch::Apply(&value),
			)
			.await
		{
			Ok(_) => info!(crd = %name, "applied CRD"),
			Err(err) => {
				warn!(crd = %name, error = %err, "failed to apply CRD");
				return Err(err.into());
			}
		}
	}
	Ok(())
}
