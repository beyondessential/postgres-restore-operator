use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::{Client, ResourceExt};
use tracing::{debug, warn};

use crate::{error::Result, types::PostgresPhysicalReplica};

mod cnpg;
mod connect;
pub mod fdw;
mod storage;

pub fn overlay_cluster_name(replica_name: &str) -> String {
	format!("{replica_name}-overlay")
}

pub fn overlay_fdw_secret_name(replica_name: &str) -> String {
	format!("{replica_name}-overlay-fdw-creds")
}

fn overlay_fdw_server_name(restore_name: &str) -> String {
	let sanitized = restore_name.replace('-', "_");
	format!("fdw_{sanitized}")
}

/// Full overlay reconciliation: version resolution, cluster creation, FDW credentials.
///
/// `snapshot_size` is a Kubernetes quantity string (e.g. "10Gi") from the
/// active restore's spec.
///
/// Returns `(cluster_ready, storage_size, pg_version)`.
pub async fn reconcile_overlay(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	snapshot_size: &Quantity,
) -> Result<(bool, Quantity, String)> {
	let replica_name = replica.name_any();
	let overlay_config = match &replica.spec.overlay_database {
		Some(c) => c,
		None => {
			debug!(replica = %replica_name, "no overlay database configured, skipping");
			return Ok((false, Quantity(String::new()), String::new()));
		}
	};
	debug!(replica = %replica_name, "reconciling overlay database");

	let pg_version = cnpg::resolve_postgres_version(client, replica).await?;

	let computed_size = match &overlay_config.storage_size_override {
		Some(override_size) => override_size.clone(),
		None => storage::compute_overlay_storage_size(snapshot_size),
	};

	let current_size = replica
		.status
		.as_ref()
		.and_then(|s| s.overlay_storage_size.as_ref());
	let storage_size = storage::ratchet_storage_size(&computed_size, current_size);
	debug!(
		replica = %replica_name,
		pg_version = %pg_version,
		computed_size = ?computed_size,
		storage_size = ?storage_size,
		"resolved overlay parameters"
	);

	fdw::ensure_fdw_credentials(client, namespace, &replica_name, replica).await?;

	let cluster_ready =
		cnpg::ensure_cnpg_cluster(client, namespace, replica, storage_size, &pg_version).await?;

	if cluster_ready
		&& let Err(e) = cnpg::ensure_overlay_service_annotations(client, namespace, replica).await
	{
		warn!(
			replica = replica_name,
			error = %e,
			"failed to apply annotations to overlay -rw service"
		);
	}

	Ok((cluster_ready, storage_size.clone(), pg_version))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn fdw_server_name_format() {
		assert_eq!(
			overlay_fdw_server_name("my-replica-20250101-120000"),
			"fdw_my_replica_20250101_120000"
		);
	}
}
