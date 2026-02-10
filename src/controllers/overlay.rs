use std::{collections::BTreeMap, iter::FromIterator};

use k8s_openapi::{
	ByteString,
	api::{
		batch::v1::{Job, JobSpec},
		core::v1::{Container, EnvVar, PodSpec, PodTemplateSpec, ResourceRequirements, Secret},
	},
	apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::OwnerReference},
};
use kube::{
	Api, Client, ResourceExt,
	api::{DynamicObject, ObjectMeta, Patch, PatchParams, PostParams},
};
use tracing::{info, warn};

use super::env_from_secret;
use crate::{
	error::{Error, Result},
	types::{
		PostgresPhysicalReplica,
		cnpg::{
			self, CnpgClusterSpec, CnpgClusterStatus, CnpgImageCatalogRef, CnpgImageCatalogSpec,
			CnpgManagedSpec, CnpgPostgresqlSpec, CnpgResourceRequirements, CnpgStorageSpec,
		},
	},
};

const DEFAULT_PG_VERSION: &str = "17";
const OVERLAY_STORAGE_BASE_BYTES: u64 = 5 * GI;
const GI: u64 = 1024 * 1024 * 1024;

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

/// Compute overlay storage size from snapshot size bytes.
///
/// Formula: `5Gi + ceil(snapshot_size_bytes / 10)`, rounded up to whole Gi.
pub fn compute_overlay_storage_size(snapshot_bytes: u64) -> String {
	let extra = snapshot_bytes.div_ceil(10);
	let total = OVERLAY_STORAGE_BASE_BYTES + extra;
	let gi_count = total.div_ceil(GI);
	format!("{gi_count}Gi")
}

/// Parse a Kubernetes quantity string like "10Gi" or "500Mi" into bytes.
fn parse_quantity_bytes(s: &str) -> Option<u64> {
	let s = s.trim();
	if let Some(rest) = s.strip_suffix("Gi") {
		rest.parse::<u64>().ok().map(|n| n * GI)
	} else if let Some(rest) = s.strip_suffix("Mi") {
		rest.parse::<u64>().ok().map(|n| n * 1024 * 1024)
	} else if let Some(rest) = s.strip_suffix("Ti") {
		rest.parse::<u64>().ok().map(|n| n * 1024 * GI)
	} else {
		s.parse::<u64>().ok()
	}
}

/// Apply ratchet logic: only increase, never shrink.
/// Returns the larger of `new_size` and `current_size`.
pub fn ratchet_storage_size(new_size: &str, current_size: Option<&str>) -> String {
	let Some(current) = current_size else {
		return new_size.to_string();
	};

	let new_bytes = parse_quantity_bytes(new_size).unwrap_or(0);
	let current_bytes = parse_quantity_bytes(current).unwrap_or(0);

	if new_bytes > current_bytes {
		new_size.to_string()
	} else {
		current.to_string()
	}
}

/// Resolve the PostgreSQL major version for the overlay cluster.
///
/// Resolution order:
/// 1. Explicit `postgres_version` in config
/// 2. Highest major from CNPG image catalog
/// 3. Hardcoded default "17"
pub async fn resolve_postgres_version(
	client: &Client,
	replica: &PostgresPhysicalReplica,
) -> String {
	let overlay_config = match &replica.spec.overlay_database {
		Some(c) => c,
		None => return DEFAULT_PG_VERSION.to_string(),
	};

	if let Some(ref v) = overlay_config.postgres_version {
		return v.clone();
	}

	let catalog_ref = overlay_config.image_catalog.as_ref();
	let catalog_name = catalog_ref.map(|c| c.name.as_str());
	let catalog_kind = catalog_ref
		.and_then(|c| c.kind.as_deref())
		.unwrap_or("ClusterImageCatalog");

	if let Some(name) = catalog_name {
		let version = match catalog_kind {
			"ImageCatalog" => {
				let namespace = replica.namespace().unwrap_or_default();
				lookup_image_catalog_version(client, &namespace, name).await
			}
			_ => lookup_cluster_image_catalog_version(client, name).await,
		};
		if let Some(v) = version {
			return v.to_string();
		}
	}

	DEFAULT_PG_VERSION.to_string()
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

/// Ensure the FDW credentials Secret exists.
pub async fn ensure_fdw_credentials(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<()> {
	let secret_name = overlay_fdw_secret_name(replica_name);
	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

	if secrets.get_opt(&secret_name).await?.is_some() {
		return Ok(());
	}

	info!(
		replica = replica_name,
		secret = secret_name,
		"creating overlay FDW credentials secret"
	);

	let password = super::replica::generate_password();
	let secret = Secret {
		metadata: ObjectMeta {
			name: Some(secret_name.clone()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([(
				"bes.au/replica".to_string(),
				replica_name.to_string(),
			)])),
			owner_references: Some(vec![owner_reference(replica)]),
			..Default::default()
		},
		data: Some(BTreeMap::from([
			(
				"username".to_string(),
				ByteString("fdw_reader".as_bytes().to_vec()),
			),
			(
				"password".to_string(),
				ByteString(password.as_bytes().to_vec()),
			),
		])),
		..Default::default()
	};

	secrets.create(&PostParams::default(), &secret).await?;
	Ok(())
}

/// Ensure the CNPG Cluster CR exists for the overlay database.
///
/// Returns `true` if the cluster is ready for FDW setup.
pub async fn ensure_cnpg_cluster(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	storage_size: &str,
	pg_version: &str,
) -> Result<bool> {
	let replica_name = replica.name_any();
	let cluster_name = overlay_cluster_name(&replica_name);
	let overlay_config = replica
		.spec
		.overlay_database
		.as_ref()
		.ok_or_else(|| Error::MissingField("overlayDatabase".into()))?;

	let api_resource = cnpg::api::cluster_resource();
	let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &api_resource);

	let pg_major: i32 = pg_version.parse().unwrap_or(17);

	let image_catalog_ref = overlay_config
		.image_catalog
		.as_ref()
		.map(|cat| CnpgImageCatalogRef {
			name: cat.name.clone(),
			kind: cat.kind.clone().unwrap_or("ClusterImageCatalog".into()),
			major: pg_major,
		});

	let cnpg_resources = overlay_config
		.resources
		.as_ref()
		.map(|r| CnpgResourceRequirements {
			requests: r
				.requests
				.as_ref()
				.map(|m| BTreeMap::from_iter(m.iter().map(|(k, v)| (k.clone(), v.clone())))),
			limits: r
				.limits
				.as_ref()
				.map(|m| BTreeMap::from_iter(m.iter().map(|(k, v)| (k.clone(), v.clone())))),
		});

	let cnpg_affinity = overlay_config.affinity.as_ref().map(|a| a.0.clone());

	let cnpg_tolerations: Vec<serde_json::Value> = overlay_config
		.tolerations
		.iter()
		.filter_map(|t| serde_json::to_value(t).ok())
		.collect();

	let spec = CnpgClusterSpec {
		instances: 1,
		image_catalog_ref,
		image_name: if overlay_config.image_catalog.is_none() {
			Some(format!("ghcr.io/cloudnative-pg/postgresql:{pg_version}"))
		} else {
			None
		},
		storage: CnpgStorageSpec {
			size: storage_size.to_string(),
			storage_class: overlay_config.storage_class.clone(),
		},
		postgresql: Some(CnpgPostgresqlSpec {
			shared_preload_libraries: vec![],
			parameters: None,
		}),
		resources: cnpg_resources,
		affinity: cnpg_affinity,
		tolerations: cnpg_tolerations,
		managed: Some(CnpgManagedSpec { roles: vec![] }),
	};

	let cluster_json = serde_json::json!({
		"apiVersion": "postgresql.cnpg.io/v1",
		"kind": "Cluster",
		"metadata": {
			"name": cluster_name,
			"namespace": namespace,
			"labels": {
				"bes.au/replica": replica_name,
				"bes.au/component": "overlay",
			},
			"ownerReferences": [{
				"apiVersion": "bes.au/v1alpha1",
				"kind": "PostgresPhysicalReplica",
				"name": replica_name,
				"uid": replica.uid().unwrap_or_default(),
				"controller": true,
				"blockOwnerDeletion": true,
			}],
		},
		"spec": serde_json::to_value(&spec)?,
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
		info!(
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

/// Build a Job that sets up FDW in the overlay database on switchover.
pub fn build_fdw_setup_job(
	replica: &PostgresPhysicalReplica,
	restore_name: &str,
	namespace: &str,
	pg_version: &str,
	old_restore: Option<&str>,
) -> Result<Job> {
	let replica_name = replica.name_any();
	let cluster_name = overlay_cluster_name(&replica_name);
	let fdw_secret_name = overlay_fdw_secret_name(&replica_name);
	let superuser_secret = format!("{cluster_name}-superuser");
	let server_name = overlay_fdw_server_name(restore_name);

	let old_server_drop = if let Some(old) = old_restore {
		let old_server = overlay_fdw_server_name(old);
		format!(
			r#"
echo "Dropping old FDW server '{old_server}' and its dependent objects..."
psql "$OVERLAY_CONNSTR" -c "DROP SERVER IF EXISTS {old_server} CASCADE;"
"#
		)
	} else {
		String::new()
	};

	let schema_mapping_env = replica
		.spec
		.overlay_database
		.as_ref()
		.and_then(|c| c.schema_mapping.as_ref())
		.map(|m| serde_json::to_string(m).unwrap_or_default());

	let schema_discovery_script = if schema_mapping_env.is_some() {
		r#"
echo "Using explicit schema mapping from SCHEMA_MAPPING env..."
SCHEMAS=$(echo "$SCHEMA_MAPPING" | python3 -c "
import sys, json
m = json.load(sys.stdin)
for remote, local in m.items():
    print(f'{remote}:{local}')
" 2>/dev/null || echo "$SCHEMA_MAPPING" | jq -r 'to_entries[] | "\(.key):\(.value)"')
"#
		.to_string()
	} else {
		format!(
			r#"
echo "Discovering schemas from restore database..."
SCHEMAS=$(psql "host={restore_name}.{namespace}.svc port=5432 dbname=postgres user=$FDW_USER password=$FDW_PASSWORD" \
  -t -A -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'" \
  | while read -r s; do echo "$s:$s"; done)
"#
		)
	};

	let script = format!(
		r#"set -e

OVERLAY_CONNSTR="host={cluster_name}-rw.{namespace}.svc port=5432 dbname=postgres user=$OVERLAY_USER password=$OVERLAY_PASSWORD"

{old_server_drop}

echo "Setting up FDW extension..."
psql "$OVERLAY_CONNSTR" -c "CREATE EXTENSION IF NOT EXISTS postgres_fdw;"

echo "Creating FDW server '{server_name}' -> {restore_name}..."
psql "$OVERLAY_CONNSTR" -c "CREATE SERVER IF NOT EXISTS {server_name} FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '{restore_name}.{namespace}.svc', port '5432', dbname 'postgres');"

echo "Creating user mapping..."
psql "$OVERLAY_CONNSTR" -c "DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER {server_name};"
psql "$OVERLAY_CONNSTR" -c "CREATE USER MAPPING FOR CURRENT_USER SERVER {server_name} OPTIONS (user '$FDW_USER', password '$FDW_PASSWORD');"

{schema_discovery_script}

echo "Importing foreign schemas..."
echo "$SCHEMAS" | while IFS=: read -r REMOTE LOCAL; do
  [ -z "$REMOTE" ] && continue
  echo "  Importing $REMOTE -> $LOCAL"
  psql "$OVERLAY_CONNSTR" -c "DROP SCHEMA IF EXISTS \"$LOCAL\" CASCADE;"
  psql "$OVERLAY_CONNSTR" -c "CREATE SCHEMA IF NOT EXISTS \"$LOCAL\";"
  psql "$OVERLAY_CONNSTR" -c "IMPORT FOREIGN SCHEMA \"$REMOTE\" FROM SERVER {server_name} INTO \"$LOCAL\";"
done

echo "FDW setup complete"
"#
	);

	let job_name = format!("{replica_name}-fdw-setup");
	let pg_image = format!("postgres:{pg_version}-alpine");

	let mut env_vars = vec![
		env_from_secret("OVERLAY_USER", &superuser_secret, "username"),
		env_from_secret("OVERLAY_PASSWORD", &superuser_secret, "password"),
		env_from_secret("FDW_USER", &fdw_secret_name, "username"),
		env_from_secret("FDW_PASSWORD", &fdw_secret_name, "password"),
	];

	if let Some(ref mapping) = schema_mapping_env {
		env_vars.push(EnvVar {
			name: "SCHEMA_MAPPING".to_string(),
			value: Some(mapping.clone()),
			..Default::default()
		});
	}

	Ok(Job {
		metadata: ObjectMeta {
			name: Some(job_name.clone()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("bes.au/replica".to_string(), replica_name.clone()),
				("bes.au/job-type".to_string(), "fdw-setup".to_string()),
			])),
			owner_references: Some(vec![owner_reference(replica)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(3),
			active_deadline_seconds: Some(300),
			ttl_seconds_after_finished: Some(120),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(BTreeMap::from([
						("bes.au/replica".to_string(), replica_name),
						("bes.au/job-type".to_string(), "fdw-setup".to_string()),
					])),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					containers: vec![Container {
						name: "fdw-setup".to_string(),
						image: Some(pg_image),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![script]),
						env: Some(env_vars),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("50m".to_string())),
								("memory".to_string(), Quantity("64Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("200m".to_string())),
								("memory".to_string(), Quantity("128Mi".to_string())),
							])),
							..Default::default()
						}),
						..Default::default()
					}],
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}

fn owner_reference(replica: &PostgresPhysicalReplica) -> OwnerReference {
	OwnerReference {
		api_version: "bes.au/v1alpha1".to_string(),
		kind: "PostgresPhysicalReplica".to_string(),
		name: replica.name_any(),
		uid: replica.uid().unwrap_or_default(),
		controller: Some(true),
		block_owner_deletion: Some(true),
	}
}

/// Full overlay reconciliation: version resolution, cluster creation, FDW credentials.
///
/// Returns `(cluster_ready, storage_size, pg_version)`.
pub async fn reconcile_overlay(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	snapshot_size_bytes: u64,
) -> Result<(bool, String, String)> {
	let replica_name = replica.name_any();
	let overlay_config = match &replica.spec.overlay_database {
		Some(c) => c,
		None => return Ok((false, String::new(), String::new())),
	};

	let pg_version = resolve_postgres_version(client, replica).await;

	let computed_size = match &overlay_config.storage_size_override {
		Some(override_size) => override_size.clone(),
		None => compute_overlay_storage_size(snapshot_size_bytes),
	};

	let current_size = replica
		.status
		.as_ref()
		.and_then(|s| s.overlay_storage_size.as_deref());
	let storage_size = ratchet_storage_size(&computed_size, current_size);

	ensure_fdw_credentials(client, namespace, &replica_name, replica).await?;

	let cluster_ready =
		ensure_cnpg_cluster(client, namespace, replica, &storage_size, &pg_version).await?;

	Ok((cluster_ready, storage_size, pg_version))
}

/// Run the FDW setup job for a switchover.
///
/// Deletes any existing fdw-setup job first, then creates a new one.
pub async fn run_fdw_setup(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	restore_name: &str,
	pg_version: &str,
	old_restore: Option<&str>,
) -> Result<()> {
	let replica_name = replica.name_any();
	let job_name = format!("{replica_name}-fdw-setup");
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

	if let Ok(Some(_)) = jobs.get_opt(&job_name).await {
		info!(
			replica = replica_name,
			job = job_name,
			"deleting existing FDW setup job"
		);
		let dp = kube::api::DeleteParams {
			propagation_policy: Some(kube::api::PropagationPolicy::Background),
			..Default::default()
		};
		if let Err(e) = jobs.delete(&job_name, &dp).await {
			warn!(job = job_name, error = %e, "failed to delete old FDW setup job");
		}
		tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	}

	let job = build_fdw_setup_job(replica, restore_name, namespace, pg_version, old_restore)?;
	jobs.create(&PostParams::default(), &job).await?;

	info!(
		replica = replica_name,
		restore = restore_name,
		"created FDW setup job"
	);

	Ok(())
}

/// Check if an FDW setup job has completed.
///
/// Returns `Some(true)` if succeeded, `Some(false)` if failed, `None` if still running.
pub async fn check_fdw_setup_job(
	client: &Client,
	namespace: &str,
	replica_name: &str,
) -> Option<bool> {
	let job_name = format!("{replica_name}-fdw-setup");
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

	let job = jobs.get(&job_name).await.ok()?;
	let status = job.status.as_ref()?;

	let succeeded = status.succeeded.unwrap_or(0);
	if succeeded > 0 {
		return Some(true);
	}

	let failed = status.failed.unwrap_or(0);
	let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(3);
	if failed > backoff_limit {
		return Some(false);
	}

	None
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::types::*;

	#[test]
	fn compute_overlay_storage_100gi_snapshot() {
		// 100Gi = 107374182400 bytes
		// extra = ceil(107374182400 / 10) = 10737418240
		// total = 5Gi + ~10Gi = ~15Gi
		let result = compute_overlay_storage_size(100 * GI);
		assert_eq!(result, "15Gi");
	}

	#[test]
	fn compute_overlay_storage_1gi_snapshot() {
		// 1Gi = 1073741824 bytes
		// extra = ceil(1073741824 / 10) = 107374183 (~0.1Gi)
		// total = 5Gi + 0.1Gi = ~5.1Gi -> ceil to 6Gi
		let result = compute_overlay_storage_size(GI);
		assert_eq!(result, "6Gi");
	}

	#[test]
	fn compute_overlay_storage_500mi_snapshot() {
		// 500Mi = 524288000 bytes
		// extra = ceil(524288000 / 10) = 52428800 (~50Mi)
		// total = 5Gi + 50Mi -> ceil to 6Gi
		let result = compute_overlay_storage_size(500 * 1024 * 1024);
		assert_eq!(result, "6Gi");
	}

	#[test]
	fn compute_overlay_storage_zero() {
		// 0 bytes -> 5Gi base
		let result = compute_overlay_storage_size(0);
		assert_eq!(result, "5Gi");
	}

	#[test]
	fn compute_overlay_storage_50gi_snapshot() {
		// 50Gi -> extra = 5Gi, total = 10Gi
		let result = compute_overlay_storage_size(50 * GI);
		assert_eq!(result, "10Gi");
	}

	#[test]
	fn ratchet_no_current() {
		assert_eq!(ratchet_storage_size("10Gi", None), "10Gi");
	}

	#[test]
	fn ratchet_new_larger() {
		assert_eq!(ratchet_storage_size("15Gi", Some("10Gi")), "15Gi");
	}

	#[test]
	fn ratchet_new_smaller() {
		assert_eq!(ratchet_storage_size("8Gi", Some("10Gi")), "10Gi");
	}

	#[test]
	fn ratchet_equal() {
		assert_eq!(ratchet_storage_size("10Gi", Some("10Gi")), "10Gi");
	}

	#[test]
	fn parse_quantity_gi() {
		assert_eq!(parse_quantity_bytes("10Gi"), Some(10 * GI));
	}

	#[test]
	fn parse_quantity_mi() {
		assert_eq!(parse_quantity_bytes("512Mi"), Some(512 * 1024 * 1024));
	}

	#[test]
	fn parse_quantity_ti() {
		assert_eq!(parse_quantity_bytes("1Ti"), Some(1024 * GI));
	}

	#[test]
	fn fdw_server_name_format() {
		assert_eq!(
			overlay_fdw_server_name("my-replica-20250101-120000"),
			"fdw_my_replica_20250101_120000"
		);
	}

	#[test]
	fn build_fdw_setup_job_without_old_restore() {
		let replica = make_test_replica();
		let job = build_fdw_setup_job(&replica, "test-restore-1", "default", "17", None).unwrap();
		let pod_spec = job.spec.unwrap().template.spec.unwrap();
		let script = &pod_spec.containers[0].args.as_ref().unwrap()[0];
		assert!(script.contains("CREATE EXTENSION IF NOT EXISTS postgres_fdw"));
		assert!(script.contains("CREATE SERVER IF NOT EXISTS fdw_test_restore_1"));
		assert!(!script.contains("DROP SERVER"));
	}

	#[test]
	fn build_fdw_setup_job_with_old_restore() {
		let replica = make_test_replica();
		let job = build_fdw_setup_job(
			&replica,
			"test-restore-2",
			"default",
			"17",
			Some("test-restore-1"),
		)
		.unwrap();
		let pod_spec = job.spec.unwrap().template.spec.unwrap();
		let script = &pod_spec.containers[0].args.as_ref().unwrap()[0];
		assert!(script.contains("DROP SERVER IF EXISTS fdw_test_restore_1 CASCADE"));
		assert!(script.contains("CREATE SERVER IF NOT EXISTS fdw_test_restore_2"));
	}

	#[test]
	fn build_fdw_setup_job_uses_correct_image() {
		let replica = make_test_replica();
		let job = build_fdw_setup_job(&replica, "test-restore-1", "default", "16", None).unwrap();
		let image = &job.spec.unwrap().template.spec.unwrap().containers[0].image;
		assert_eq!(image.as_deref(), Some("postgres:16-alpine"));
	}

	fn make_test_replica() -> PostgresPhysicalReplica {
		PostgresPhysicalReplica::new(
			"test-replica",
			PostgresPhysicalReplicaSpec {
				kopia_secret_ref: "kopia-secret".to_string(),
				snapshot_filter: None,
				schedule: None,
				schedule_jitter: "10m".to_string(),
				minimum_ttl: None,
				switchover_grace_period: "5m".to_string(),
				analytics_username: "analytics".to_string(),
				storage_class: None,
				storage_size_override: None,
				resources: None,
				service_annotations: None,
				pod_annotations: None,
				affinity: None,
				tolerations: vec![],
				read_only: true,
				postgres_extra_config: None,
				notifications: vec![],
				overlay_database: Some(OverlayDatabaseConfig {
					postgres_version: None,
					image_catalog: None,
					storage_size_override: None,
					storage_class: None,
					resources: None,
					affinity: None,
					tolerations: vec![],
					schema_mapping: None,
				}),
			},
		)
	}
}
