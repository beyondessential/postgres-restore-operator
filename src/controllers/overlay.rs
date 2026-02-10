use std::{
	collections::{BTreeMap, HashSet},
	iter::FromIterator,
};

use k8s_openapi::{
	ByteString,
	api::core::v1::{Secret, Service},
	apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::{
	Api, Client, ResourceExt,
	api::{DynamicObject, ObjectMeta, Patch, PatchParams, PostParams},
};
use kube_quantity::ParsedQuantity;
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

use crate::{
	error::{Error, Result},
	types::{
		PostgresPhysicalReplica,
		cnpg::{
			self, CnpgClusterSpec, CnpgClusterStatus, CnpgImageCatalogRef, CnpgImageCatalogSpec,
			CnpgManagedRole, CnpgManagedSpec, CnpgPasswordSecretRef, CnpgPostgresqlSpec,
			CnpgResourceRequirements, CnpgStorageSpec,
		},
	},
};

const DEFAULT_PG_VERSION: &str = "17";
const MIN_OVERLAY_PG_VERSION: i32 = 14;
const GI: u64 = 1024 * 1024 * 1024;
const OVERLAY_STORAGE_BASE_GI: u64 = 5;

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

/// Parse a Kubernetes quantity string (e.g. "10Gi", "500Mi") into bytes.
fn quantity_to_bytes(s: &str) -> Option<u64> {
	let pq: ParsedQuantity = s.try_into().ok()?;
	pq.to_bytes_u64()
}

/// Compute overlay storage size from a snapshot size quantity string.
///
/// Formula: `5Gi + ceil(snapshot_size_bytes / 10)`, rounded up to whole Gi.
pub fn compute_overlay_storage_size(snapshot_size: &str) -> String {
	let snapshot_bytes = quantity_to_bytes(snapshot_size).unwrap_or(0);
	let extra_gi = snapshot_bytes.div_ceil(10 * GI);
	let total_gi = OVERLAY_STORAGE_BASE_GI + extra_gi;
	format!("{total_gi}Gi")
}

/// Apply ratchet logic: only increase, never shrink.
/// Returns the larger of `new_size` and `current_size`.
pub fn ratchet_storage_size(new_size: &str, current_size: Option<&str>) -> String {
	let Some(current) = current_size else {
		return new_size.to_string();
	};

	let new_pq: std::result::Result<ParsedQuantity, _> = new_size.try_into();
	let cur_pq: std::result::Result<ParsedQuantity, _> = current.try_into();

	match (new_pq, cur_pq) {
		(Ok(n), Ok(c)) if n > c => new_size.to_string(),
		(_, Ok(_)) => current.to_string(),
		_ => new_size.to_string(),
	}
}

/// Resolve the PostgreSQL major version for the overlay cluster.
///
/// Validate that a resolved PG major version is high enough for the overlay.
///
/// The overlay relies on `pg_read_all_data` and `pg_write_all_data` which
/// require PostgreSQL >= 14.
pub fn validate_overlay_pg_version(version: &str) -> Result<()> {
	let major: i32 = version.parse().unwrap_or(0);
	if major < MIN_OVERLAY_PG_VERSION {
		return Err(Error::InvalidOverlayConfig(format!(
			"overlay database requires PostgreSQL >= {MIN_OVERLAY_PG_VERSION} \
			 (pg_read_all_data / pg_write_all_data), got \"{version}\""
		)));
	}
	Ok(())
}

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
) -> Result<String> {
	let overlay_config = match &replica.spec.overlay_database {
		Some(c) => c,
		None => return Ok(DEFAULT_PG_VERSION.to_string()),
	};

	let version = if let Some(ref v) = overlay_config.postgres_version {
		v.clone()
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

		from_catalog
			.map(|v| v.to_string())
			.unwrap_or_else(|| DEFAULT_PG_VERSION.to_string())
	};

	validate_overlay_pg_version(&version)?;

	Ok(version)
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
		debug!(
			replica = replica_name,
			secret = secret_name,
			"FDW credentials secret already exists, skipping creation"
		);
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
				"pgro.bes.au/replica".to_string(),
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
		enable_superuser_access: Some(true),
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
			"ownerReferences": [{
				"apiVersion": "pgro.bes.au/v1alpha1",
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
	let cluster_name = overlay_cluster_name(&replica_name);
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

/// Escape a SQL identifier by double-quoting it.
fn quote_ident(s: &str) -> String {
	format!("\"{}\"", s.replace('"', "\"\""))
}

/// Escape a SQL string literal by single-quoting it.
fn quote_literal(s: &str) -> String {
	format!("'{}'", s.replace('\'', "''"))
}

/// Read a UTF-8 string field from a Kubernetes Secret.
fn read_secret_field(secret: &Secret, key: &str) -> Result<String> {
	let data = secret
		.data
		.as_ref()
		.ok_or_else(|| Error::MissingField("secret has no data".to_string()))?;
	let bytes = data
		.get(key)
		.ok_or_else(|| Error::MissingField(format!("secret missing key: {key}")))?;
	String::from_utf8(bytes.0.clone())
		.map_err(|_| Error::MissingField(format!("secret key {key} is not valid UTF-8")))
}

/// Connect to the overlay database and return a tokio-postgres client.
async fn connect_overlay(
	cluster_name: &str,
	namespace: &str,
	su_secret: &Secret,
) -> Result<tokio_postgres::Client> {
	let overlay_user = read_secret_field(su_secret, "username")?;
	let overlay_password = read_secret_field(su_secret, "password")?;
	let overlay_host = format!("{cluster_name}-rw.{namespace}.svc");
	let overlay_connstr = format!(
		"host={overlay_host} port=5432 dbname=app user={} password={} connect_timeout=10",
		overlay_user, overlay_password,
	);

	debug!(host = %overlay_host, "connecting to overlay database");
	let (pg, conn) = tokio_postgres::connect(&overlay_connstr, NoTls).await?;
	tokio::spawn(async move {
		if let Err(e) = conn.await {
			warn!(error = %e, "overlay database connection error");
		}
	});
	info!(host = %overlay_host, "connected to overlay database");
	Ok(pg)
}

/// Captured FDW state from the overlay database.
struct FdwState {
	has_extension: bool,
	server_host: Option<String>,
	server_dbname: Option<String>,
	has_user_mapping: bool,
	schemas_with_fts: HashSet<String>,
}

/// Query the overlay database to determine the current FDW state.
async fn check_fdw_state(pg: &tokio_postgres::Client, server_name: &str) -> Result<FdwState> {
	let has_extension = pg
		.query_opt(
			"SELECT 1 FROM pg_extension WHERE extname = 'postgres_fdw'",
			&[],
		)
		.await?
		.is_some();
	debug!(has_extension, "checked postgres_fdw extension");

	let server_opts: Vec<(String, String)> = pg
		.query(
			"SELECT option_name, option_value FROM pg_options_to_table( \
			   (SELECT srvoptions FROM pg_foreign_server WHERE srvname = $1) \
			 )",
			&[&server_name],
		)
		.await?
		.iter()
		.map(|row| (row.get(0), row.get(1)))
		.collect();

	let server_host = server_opts
		.iter()
		.find(|(k, _)| k == "host")
		.map(|(_, v)| v.clone());
	let server_dbname = server_opts
		.iter()
		.find(|(k, _)| k == "dbname")
		.map(|(_, v)| v.clone());
	debug!(
		server = server_name,
		host = ?server_host,
		dbname = ?server_dbname,
		"checked FDW server options"
	);

	let has_user_mapping = pg
		.query_opt(
			"SELECT 1 FROM pg_user_mappings WHERE srvname = $1 AND usename = current_user",
			&[&server_name],
		)
		.await?
		.is_some();
	debug!(
		server = server_name,
		has_user_mapping, "checked user mapping"
	);

	let schema_rows = pg
		.query(
			"SELECT DISTINCT ft.foreign_table_schema \
			 FROM information_schema.foreign_tables ft \
			 WHERE ft.foreign_server_name = $1",
			&[&server_name],
		)
		.await?;
	let schemas_with_fts: HashSet<String> = schema_rows.iter().map(|row| row.get(0)).collect();
	debug!(
		server = server_name,
		schemas = ?schemas_with_fts,
		"checked schemas with foreign tables"
	);

	Ok(FdwState {
		has_extension,
		server_host,
		server_dbname,
		has_user_mapping,
		schemas_with_fts,
	})
}

/// Find all FDW servers owned by this operator (matching the `fdw_` prefix)
/// that are not the expected current server, and drop them.
async fn drop_stale_fdw_servers(
	pg: &tokio_postgres::Client,
	current_server: &str,
	replica_name: &str,
) -> Result<()> {
	let rows = pg
		.query(
			"SELECT srvname FROM pg_foreign_server WHERE srvname LIKE 'fdw_%'",
			&[],
		)
		.await?;

	for row in &rows {
		let name: String = row.get(0);
		if name != current_server {
			info!(
				replica = replica_name,
				stale_server = %name,
				"dropping stale FDW server"
			);
			pg.batch_execute(&format!(
				"DROP SERVER IF EXISTS {} CASCADE",
				quote_ident(&name)
			))
			.await?;
		}
	}
	Ok(())
}

/// Connect to the restore's `postgres` database and find the largest
/// non-system database by size. This is the database whose schemas we
/// import via FDW into the overlay's `app` database.
async fn discover_restore_database(
	restore_host: &str,
	fdw_user: &str,
	fdw_password: &str,
) -> Result<String> {
	let connstr = format!(
		"host={restore_host} port=5432 dbname=postgres user={fdw_user} password={fdw_password} connect_timeout=10",
	);
	debug!(
		host = restore_host,
		"connecting to restore postgres database for database discovery"
	);
	let (pg, conn) = tokio_postgres::connect(&connstr, NoTls).await?;
	tokio::spawn(async move {
		if let Err(e) = conn.await {
			warn!(error = %e, "restore database connection error during discovery");
		}
	});

	let row = pg
		.query_opt(
			"SELECT datname FROM pg_database \
			 WHERE datname NOT IN ('postgres', 'template0', 'template1') \
			 ORDER BY pg_database_size(datname) DESC \
			 LIMIT 1",
			&[],
		)
		.await?;

	match row {
		Some(r) => {
			let name: String = r.get(0);
			info!(
				host = restore_host,
				database = %name,
				"discovered main database in restore by size"
			);
			Ok(name)
		}
		None => Err(Error::MissingField(
			"no non-system databases found in restore".into(),
		)),
	}
}

/// Resolve which schemas to import via FDW.
///
/// Uses the explicit `schema_mapping` from the spec if present, otherwise
/// discovers user schemas from the restore database.
async fn resolve_fdw_schemas(
	replica: &PostgresPhysicalReplica,
	restore_host: &str,
	restore_dbname: &str,
	fdw_user: &str,
	fdw_password: &str,
	replica_name: &str,
) -> Result<Vec<(String, String)>> {
	if let Some(mapping) = replica
		.spec
		.overlay_database
		.as_ref()
		.and_then(|c| c.schema_mapping.as_ref())
	{
		let result: Vec<_> = mapping
			.iter()
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();
		info!(
			replica = replica_name,
			schema_count = result.len(),
			"using explicit schema mapping from spec"
		);
		debug!(schemas = ?result, "explicit schema mapping entries");
		return Ok(result);
	}

	info!(
		replica = replica_name,
		restore_host = restore_host,
		restore_dbname = restore_dbname,
		"no explicit schema mapping, discovering schemas from restore database"
	);
	let restore_connstr = format!(
		"host={restore_host} port=5432 dbname={} user={fdw_user} password={fdw_password} connect_timeout=10",
		restore_dbname,
	);
	debug!(
		host = restore_host,
		dbname = restore_dbname,
		"connecting to restore database for schema discovery"
	);
	let (restore_pg, restore_conn) = tokio_postgres::connect(&restore_connstr, NoTls).await?;
	tokio::spawn(async move {
		if let Err(e) = restore_conn.await {
			warn!(error = %e, "restore database connection error");
		}
	});
	debug!("connected to restore database");

	let rows = restore_pg
		.query(
			"SELECT schema_name FROM information_schema.schemata \
			 WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'",
			&[],
		)
		.await?;
	let result: Vec<_> = rows
		.iter()
		.map(|row| {
			let name: String = row.get(0);
			(name.clone(), name)
		})
		.collect();
	info!(
		replica = replica_name,
		schema_count = result.len(),
		"discovered schemas from restore database"
	);
	debug!(schemas = ?result, "discovered schema entries");
	Ok(result)
}

/// Reconcile FDW state in the overlay database.
///
/// Connects to the overlay, inspects the current FDW state (extension, server,
/// user mapping, imported schemas), and fixes only what is missing or incorrect.
/// Stale FDW servers from previous restores are dropped automatically.
pub async fn reconcile_fdw(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	restore_name: &str,
) -> Result<()> {
	let replica_name = replica.name_any();
	let cluster_name = overlay_cluster_name(&replica_name);
	let fdw_secret_name = overlay_fdw_secret_name(&replica_name);
	let superuser_secret_name = format!("{cluster_name}-superuser");
	let server_name = overlay_fdw_server_name(restore_name);
	let restore_host = format!("{restore_name}.{namespace}.svc");

	info!(
		replica = %replica_name,
		restore = %restore_name,
		server = %server_name,
		"reconciling FDW state"
	);

	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
	debug!(
		superuser_secret = %superuser_secret_name,
		fdw_secret = %fdw_secret_name,
		"fetching secrets for FDW reconciliation"
	);
	let su_secret = secrets.get(&superuser_secret_name).await?;
	let fdw_secret = secrets.get(&fdw_secret_name).await?;

	let fdw_user = read_secret_field(&fdw_secret, "username")?;
	let fdw_password = read_secret_field(&fdw_secret, "password")?;

	let overlay_pg = connect_overlay(&cluster_name, namespace, &su_secret).await?;

	// Discover the main database in the restore (largest by size)
	let restore_dbname = discover_restore_database(&restore_host, &fdw_user, &fdw_password).await?;

	// Check current state
	let state = check_fdw_state(&overlay_pg, &server_name).await?;

	let server_host_correct = state.server_host.as_deref() == Some(restore_host.as_str());
	let server_dbname_correct = state.server_dbname.as_deref() == Some(restore_dbname.as_str());

	info!(
		replica = %replica_name,
		has_extension = state.has_extension,
		server_exists = state.server_host.is_some(),
		server_host_correct,
		server_dbname = ?state.server_dbname,
		expected_dbname = %restore_dbname,
		server_dbname_correct,
		has_user_mapping = state.has_user_mapping,
		foreign_table_schemas = state.schemas_with_fts.len(),
		"current FDW state in overlay database"
	);

	// Drop stale servers from previous restores
	drop_stale_fdw_servers(&overlay_pg, &server_name, &replica_name).await?;

	// Ensure _pgro schema and extension
	if !state.has_extension {
		info!(replica = %replica_name, "creating _pgro schema and postgres_fdw extension");
		overlay_pg
			.batch_execute("CREATE SCHEMA IF NOT EXISTS _pgro")
			.await?;
		overlay_pg
			.batch_execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw SCHEMA _pgro")
			.await?;
	} else {
		debug!(replica = %replica_name, "postgres_fdw extension already present");
	}

	// Ensure FDW server with correct host and dbname
	if state.server_host.is_none() {
		info!(
			replica = %replica_name,
			server = %server_name,
			host = %restore_host,
			dbname = %restore_dbname,
			"creating FDW server"
		);
		overlay_pg
			.batch_execute(&format!(
				"CREATE SERVER {server_name} FOREIGN DATA WRAPPER postgres_fdw \
				 OPTIONS (host {}, port '5432', dbname {})",
				quote_literal(&restore_host),
				quote_literal(&restore_dbname),
			))
			.await?;
	} else {
		// Server exists — fix any options that are wrong
		let mut alter_parts = Vec::new();
		if !server_host_correct {
			info!(
				replica = %replica_name,
				server = %server_name,
				old_host = ?state.server_host,
				new_host = %restore_host,
				"FDW server host needs update"
			);
			alter_parts.push(format!("SET host {}", quote_literal(&restore_host)));
		}
		if !server_dbname_correct {
			info!(
				replica = %replica_name,
				server = %server_name,
				old_dbname = ?state.server_dbname,
				new_dbname = %restore_dbname,
				"FDW server dbname needs update"
			);
			let verb = if state.server_dbname.is_some() {
				"SET"
			} else {
				"ADD"
			};
			alter_parts.push(format!("{verb} dbname {}", quote_literal(&restore_dbname)));
		}
		if alter_parts.is_empty() {
			debug!(
				replica = %replica_name,
				server = %server_name,
				"FDW server already correct"
			);
		} else {
			let opts = alter_parts.join(", ");
			debug!(server = %server_name, alter = %opts, "altering FDW server options");
			overlay_pg
				.batch_execute(&format!("ALTER SERVER {server_name} OPTIONS ({opts})"))
				.await?;
		}
	}

	// Ensure user mapping (always recreate to pick up credential changes)
	if state.has_user_mapping {
		debug!(server = %server_name, "dropping existing user mapping before recreation");
		overlay_pg
			.batch_execute(&format!(
				"DROP USER MAPPING FOR CURRENT_USER SERVER {server_name}"
			))
			.await?;
	}
	info!(server = %server_name, fdw_user = %fdw_user, "creating user mapping");
	overlay_pg
		.batch_execute(&format!(
			"CREATE USER MAPPING FOR CURRENT_USER SERVER {server_name} \
			 OPTIONS (user {}, password {})",
			quote_literal(&fdw_user),
			quote_literal(&fdw_password),
		))
		.await?;

	// Resolve expected schemas and import any that are missing
	let schemas = resolve_fdw_schemas(
		replica,
		&restore_host,
		&restore_dbname,
		&fdw_user,
		&fdw_password,
		&replica_name,
	)
	.await?;

	let mut imported_count = 0u32;
	for (remote, local) in &schemas {
		if state.schemas_with_fts.contains(local) {
			debug!(
				local = %local,
				remote = %remote,
				"schema already has foreign tables, skipping import"
			);
			continue;
		}

		info!(
			remote = %remote,
			local = %local,
			"importing foreign schema"
		);
		imported_count += 1;
		let local_quoted = quote_ident(local);
		let remote_quoted = quote_ident(remote);
		debug!(local = %local, "dropping existing local schema if present");
		overlay_pg
			.batch_execute(&format!("DROP SCHEMA IF EXISTS {local_quoted} CASCADE"))
			.await?;
		debug!(local = %local, "creating local schema");
		overlay_pg
			.batch_execute(&format!("CREATE SCHEMA {local_quoted}"))
			.await?;
		debug!(
			remote = %remote,
			local = %local,
			server = %server_name,
			"executing IMPORT FOREIGN SCHEMA"
		);
		overlay_pg
			.batch_execute(&format!(
				"IMPORT FOREIGN SCHEMA {remote_quoted} FROM SERVER {server_name} INTO {local_quoted}"
			))
			.await?;
		debug!(remote = %remote, local = %local, "foreign schema imported successfully");
	}

	info!(
		replica = %replica_name,
		restore = %restore_name,
		total_schemas = schemas.len(),
		schemas_imported = imported_count,
		schemas_skipped = schemas.len() as u32 - imported_count,
		"FDW reconciliation complete"
	);

	Ok(())
}

fn owner_reference(replica: &PostgresPhysicalReplica) -> OwnerReference {
	OwnerReference {
		api_version: "pgro.bes.au/v1alpha1".to_string(),
		kind: "PostgresPhysicalReplica".to_string(),
		name: replica.name_any(),
		uid: replica.uid().unwrap_or_default(),
		controller: Some(true),
		block_owner_deletion: Some(true),
	}
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
	snapshot_size: &str,
) -> Result<(bool, String, String)> {
	let replica_name = replica.name_any();
	let overlay_config = match &replica.spec.overlay_database {
		Some(c) => c,
		None => {
			debug!(replica = %replica_name, "no overlay database configured, skipping");
			return Ok((false, String::new(), String::new()));
		}
	};
	debug!(replica = %replica_name, "reconciling overlay database");

	let pg_version = resolve_postgres_version(client, replica).await?;

	let computed_size = match &overlay_config.storage_size_override {
		Some(override_size) => override_size.clone(),
		None => compute_overlay_storage_size(snapshot_size),
	};

	let current_size = replica
		.status
		.as_ref()
		.and_then(|s| s.overlay_storage_size.as_deref());
	let storage_size = ratchet_storage_size(&computed_size, current_size);
	debug!(
		replica = %replica_name,
		pg_version = %pg_version,
		computed_size = %computed_size,
		storage_size = %storage_size,
		"resolved overlay parameters"
	);

	ensure_fdw_credentials(client, namespace, &replica_name, replica).await?;

	let cluster_ready =
		ensure_cnpg_cluster(client, namespace, replica, &storage_size, &pg_version).await?;

	if cluster_ready
		&& let Err(e) = ensure_overlay_service_annotations(client, namespace, replica).await
	{
		warn!(
			replica = replica_name,
			error = %e,
			"failed to apply annotations to overlay -rw service"
		);
	}

	Ok((cluster_ready, storage_size, pg_version))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn quote_ident_plain() {
		assert_eq!(quote_ident("public"), "\"public\"");
	}

	#[test]
	fn quote_ident_with_quotes() {
		assert_eq!(quote_ident("my\"schema"), "\"my\"\"schema\"");
	}

	#[test]
	fn quote_literal_plain() {
		assert_eq!(quote_literal("hello"), "'hello'");
	}

	#[test]
	fn quote_literal_with_quotes() {
		assert_eq!(quote_literal("it's"), "'it''s'");
	}

	#[test]
	fn compute_overlay_storage_100gi_snapshot() {
		// 100Gi snapshot -> extra = ceil(100Gi / 10Gi) = 10Gi -> 5 + 10 = 15Gi
		let result = compute_overlay_storage_size("100Gi");
		assert_eq!(result, "15Gi");
	}

	#[test]
	fn compute_overlay_storage_1gi_snapshot() {
		// 1Gi snapshot -> extra = ceil(1Gi / 10Gi) = 1 -> 5 + 1 = 6Gi
		let result = compute_overlay_storage_size("1Gi");
		assert_eq!(result, "6Gi");
	}

	#[test]
	fn compute_overlay_storage_500mi_snapshot() {
		// 500Mi -> 500*1024*1024 = 524288000 bytes
		// extra_gi = ceil(524288000 / (10 * 1073741824)) = ceil(0.0488...) = 1
		// total = 5 + 1 = 6Gi
		let result = compute_overlay_storage_size("500Mi");
		assert_eq!(result, "6Gi");
	}

	#[test]
	fn compute_overlay_storage_zero() {
		let result = compute_overlay_storage_size("0");
		assert_eq!(result, "5Gi");
	}

	#[test]
	fn compute_overlay_storage_50gi_snapshot() {
		// 50Gi -> extra = ceil(50/10) = 5 -> 5 + 5 = 10Gi
		let result = compute_overlay_storage_size("50Gi");
		assert_eq!(result, "10Gi");
	}

	#[test]
	fn compute_overlay_storage_bad_input() {
		let result = compute_overlay_storage_size("not-a-quantity");
		assert_eq!(result, "5Gi");
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
	fn ratchet_mixed_units() {
		// 1Gi = 1024Mi, so 1Gi > 512Mi
		assert_eq!(ratchet_storage_size("1Gi", Some("512Mi")), "1Gi");
		// 512Mi < 1Gi
		assert_eq!(ratchet_storage_size("512Mi", Some("1Gi")), "1Gi");
	}

	#[test]
	fn quantity_to_bytes_gi() {
		assert_eq!(quantity_to_bytes("10Gi"), Some(10 * GI));
	}

	#[test]
	fn quantity_to_bytes_mi() {
		assert_eq!(quantity_to_bytes("512Mi"), Some(512 * 1024 * 1024));
	}

	#[test]
	fn quantity_to_bytes_bare() {
		assert_eq!(quantity_to_bytes("1024"), Some(1024));
	}

	#[test]
	fn validate_pg_version_14_ok() {
		assert!(validate_overlay_pg_version("14").is_ok());
	}

	#[test]
	fn validate_pg_version_17_ok() {
		assert!(validate_overlay_pg_version("17").is_ok());
	}

	#[test]
	fn validate_pg_version_13_rejected() {
		let err = validate_overlay_pg_version("13").unwrap_err();
		let msg = err.to_string();
		assert!(msg.contains(">= 14"), "error should mention >= 14: {msg}");
		assert!(
			msg.contains("13"),
			"error should mention the bad version: {msg}"
		);
	}

	#[test]
	fn validate_pg_version_11_rejected() {
		assert!(validate_overlay_pg_version("11").is_err());
	}

	#[test]
	fn validate_pg_version_garbage_rejected() {
		assert!(validate_overlay_pg_version("banana").is_err());
	}

	#[test]
	fn validate_pg_version_empty_rejected() {
		assert!(validate_overlay_pg_version("").is_err());
	}

	#[test]
	fn fdw_server_name_format() {
		assert_eq!(
			overlay_fdw_server_name("my-replica-20250101-120000"),
			"fdw_my_replica_20250101_120000"
		);
	}
}
