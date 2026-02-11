use std::collections::BTreeMap;

use k8s_openapi::{
	api::core::v1::Service,
	apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::OwnerReference},
};
use kube::{
	Api, Client, ResourceExt,
	api::{DynamicObject, Patch, PatchParams},
};
use tracing::{debug, info};

use crate::{
	error::{Error, Result},
	types::{
		PostgresPhysicalReplica,
		cnpg::{
			self, CnpgClusterSpec, CnpgClusterStatus, CnpgImageCatalogRef, CnpgImageCatalogSpec,
			CnpgManagedRole, CnpgManagedSpec, CnpgPasswordSecretRef, CnpgPostgresqlSpec,
			CnpgStorageSpec,
		},
	},
};

const DEFAULT_PG_VERSION: i32 = 17;
const MIN_OVERLAY_PG_VERSION: i32 = 14;

/// Resolution order:
/// 1. Explicit `postgres_version` in config
/// 2. Highest major from CNPG image catalog
/// 3. Hardcoded default "17"
///
/// Returns an error if the resolved version is below 14 (required for
/// `pg_read_all_data` / `pg_write_all_data`).
pub async fn resolve_postgres_version(
	client: &Client,
	replica: &PostgresPhysicalReplica,
) -> Result<i32> {
	let overlay_config = match &replica.spec.overlay_database {
		Some(c) => c,
		None => return Ok(DEFAULT_PG_VERSION),
	};

	let version = if let Some(v) = overlay_config.postgres_version {
		v as i32
	} else {
		let catalog_ref = overlay_config.image_catalog.as_ref();
		let catalog_name = catalog_ref.map(|c| c.name.as_str());
		let catalog_kind = catalog_ref
			.and_then(|c| c.kind.as_deref())
			.unwrap_or("ClusterImageCatalog");

		let from_catalog = if let Some(name) = catalog_name {
			match catalog_kind {
				"ImageCatalog" => {
					let namespace = replica.namespace().unwrap_or_default();
					lookup_image_catalog_version(client, &namespace, name).await
				}
				_ => lookup_cluster_image_catalog_version(client, name).await,
			}
		} else {
			None
		};

		from_catalog.unwrap_or(DEFAULT_PG_VERSION)
	};

	validate_overlay_pg_version(version)?;

	Ok(version)
}

/// Resolve the PostgreSQL major version for the overlay cluster.
///
/// Validate that a resolved PG major version is high enough for the overlay.
///
/// The overlay relies on `pg_read_all_data` and `pg_write_all_data` which
/// require PostgreSQL >= 14.
pub fn validate_overlay_pg_version(version: i32) -> Result<()> {
	if version < MIN_OVERLAY_PG_VERSION {
		return Err(Error::InvalidOverlayConfig(format!(
			"overlay database requires PostgreSQL >= {MIN_OVERLAY_PG_VERSION} \
			 (pg_read_all_data / pg_write_all_data), got {version}"
		)));
	}
	Ok(())
}

async fn lookup_cluster_image_catalog_version(client: &Client, name: &str) -> Option<i32> {
	let api_resource = cnpg::api::cluster_image_catalog_resource();
	let api: Api<DynamicObject> = Api::all_with(client.clone(), &api_resource);

	let obj = api.get(name).await.ok()?;
	let spec: CnpgImageCatalogSpec = serde_json::from_value(obj.data["spec"].clone()).ok()?;

	spec.images.iter().map(|img| img.major).max()
}

async fn lookup_image_catalog_version(client: &Client, namespace: &str, name: &str) -> Option<i32> {
	let api_resource = cnpg::api::image_catalog_resource();
	let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &api_resource);

	let obj = api.get(name).await.ok()?;
	let spec: CnpgImageCatalogSpec = serde_json::from_value(obj.data["spec"].clone()).ok()?;

	spec.images.iter().map(|img| img.major).max()
}

/// Ensure the CNPG Cluster CR exists for the overlay database.
///
/// Returns `true` if the cluster is ready for FDW setup.
pub async fn ensure_cnpg_cluster(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	storage_size: &Quantity,
	pg_version: i32,
) -> Result<bool> {
	let replica_name = replica.name_any();
	let cluster_name = super::overlay_cluster_name(&replica_name);
	let overlay_config = replica
		.spec
		.overlay_database
		.as_ref()
		.ok_or_else(|| Error::MissingField("overlayDatabase".into()))?;

	let api_resource = cnpg::api::cluster_resource();
	let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &api_resource);

	let image_catalog_ref = overlay_config
		.image_catalog
		.as_ref()
		.map(|cat| CnpgImageCatalogRef {
			name: cat.name.clone(),
			kind: cat.kind.clone().unwrap_or("ClusterImageCatalog".into()),
			major: pg_version,
		});

	let spec = CnpgClusterSpec {
		instances: 1,
		enable_superuser_access: Some(true),
		image_catalog_ref,
		image_name: if overlay_config.image_catalog.is_none() {
			Some(format!("ghcr.io/cloudnative-pg/postgresql:{pg_version}"))
		} else {
			None
		},
		storage: CnpgStorageSpec {
			size: storage_size.clone(),
			storage_class: overlay_config.storage_class.clone(),
		},
		postgresql: Some(CnpgPostgresqlSpec {
			shared_preload_libraries: vec![],
			parameters: None,
		}),
		resources: overlay_config.resources.clone(),
		affinity: overlay_config.affinity.clone(),
		tolerations: overlay_config.tolerations.clone(),
		managed: Some(CnpgManagedSpec {
			roles: vec![CnpgManagedRole {
				name: replica.spec.analytics_username.clone(),
				ensure: "present".into(),
				login: true,
				superuser: false,
				createdb: true,
				password_secret: Some(CnpgPasswordSecretRef {
					name: format!("{replica_name}-creds"),
				}),
				in_roles: vec!["pg_read_all_data".into(), "pg_write_all_data".into()],
				connection_limit: None,
				comment: None,
			}],
		}),
	};

	let owner = OwnerReference {
		api_version: "pgro.bes.au/v1alpha1".to_string(),
		kind: "PostgresPhysicalReplica".to_string(),
		name: replica_name.clone(),
		uid: replica.uid().unwrap_or_default(),
		controller: Some(true),
		block_owner_deletion: Some(true),
	};

	let cluster_json = serde_json::json!({
		"apiVersion": "postgresql.cnpg.io/v1",
		"kind": "Cluster",
		"metadata": {
			"name": cluster_name,
			"namespace": namespace,
			"labels": {
				"pgro.bes.au/replica": replica_name,
				"pgro.bes.au/component": "overlay",
			},
			"ownerReferences": [owner],
		},
		"spec": spec,
	});

	let obj = api
		.patch(
			&cluster_name,
			&PatchParams::apply("postgres-restore-operator").force(),
			&Patch::Apply(&cluster_json),
		)
		.await?;

	let status: CnpgClusterStatus = obj
		.data
		.get("status")
		.and_then(|s| serde_json::from_value(s.clone()).ok())
		.unwrap_or_default();

	if status.is_ready() {
		debug!(
			replica = replica_name,
			cluster = cluster_name,
			"overlay CNPG cluster is ready"
		);
		Ok(true)
	} else {
		info!(
			replica = replica_name,
			cluster = cluster_name,
			phase = ?status.phase,
			"overlay CNPG cluster not yet ready"
		);
		Ok(false)
	}
}

/// Apply user-specified annotations to the CNPG-generated `-rw` Service.
///
/// CNPG creates a `<cluster-name>-rw` Service automatically. This function
/// patches that Service with any annotations from `overlayDatabase.serviceAnnotations`.
pub async fn ensure_overlay_service_annotations(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<()> {
	let overlay_config = match &replica.spec.overlay_database {
		Some(c) => c,
		None => return Ok(()),
	};

	let annotations = match &overlay_config.service_annotations {
		Some(a) if !a.is_empty() => a,
		_ => return Ok(()),
	};

	let replica_name = replica.name_any();
	let cluster_name = super::overlay_cluster_name(&replica_name);
	let svc_name = format!("{cluster_name}-rw");

	let services: Api<Service> = Api::namespaced(client.clone(), namespace);

	if services.get_opt(&svc_name).await?.is_none() {
		debug!(
			replica = replica_name,
			service = svc_name,
			"overlay -rw service not yet created by CNPG, skipping annotation patch"
		);
		return Ok(());
	}

	let ann_map: BTreeMap<String, String> = annotations
		.iter()
		.map(|(k, v)| (k.clone(), v.clone()))
		.collect();

	let patch = serde_json::json!({
		"metadata": {
			"annotations": ann_map,
		}
	});
	services
		.patch(
			&svc_name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;

	debug!(
		replica = replica_name,
		service = svc_name,
		count = annotations.len(),
		"applied service annotations to overlay -rw service"
	);

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn validate_pg_version_17_ok() {
		assert!(validate_overlay_pg_version(17).is_ok());
	}

	#[test]
	fn validate_pg_version_13_rejected() {
		let err = validate_overlay_pg_version(13).unwrap_err();
		let msg = err.to_string();
		assert!(msg.contains(">= 14"), "error should mention >= 14: {msg}");
		assert!(
			msg.contains("13"),
			"error should mention the bad version: {msg}"
		);
	}
}
