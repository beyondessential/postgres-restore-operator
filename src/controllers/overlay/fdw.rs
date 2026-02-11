use std::collections::{BTreeMap, BTreeSet, HashSet};

use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{
	Api, Client, ResourceExt,
	api::{ObjectMeta, PostParams},
};
use tracing::{debug, info, warn};

/// A custom domain type discovered from the remote database.
struct RemoteDomain {
	schema: String,
	name: String,
	base_type: String,
}

/// A custom enum type discovered from the remote database.
struct RemoteEnum {
	schema: String,
	name: String,
	labels: Vec<String>,
}

use crate::{
	controllers::replica::generate_password,
	error::{Error, Result},
	types::PostgresPhysicalReplica,
};

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
	client: &Client,
	namespace: &str,
	restore_name: &str,
	fdw_user: &str,
	fdw_password: &str,
	use_port_forward: bool,
) -> Result<String> {
	let conn = super::connect::connect_to_restore(
		client,
		namespace,
		restore_name,
		"postgres",
		fdw_user,
		fdw_password,
		use_port_forward,
	)
	.await?;
	let pg = &conn.client;

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
				restore = restore_name,
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
#[expect(
	clippy::too_many_arguments,
	reason = "internal helper with tightly-coupled params"
)]
async fn resolve_fdw_schemas(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	restore_name: &str,
	restore_dbname: &str,
	fdw_user: &str,
	fdw_password: &str,
	use_port_forward: bool,
) -> Result<Vec<(String, String)>> {
	let replica_name = replica.name_any();
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
			replica = %replica_name,
			schema_count = result.len(),
			"using explicit schema mapping from spec"
		);
		debug!(schemas = ?result, "explicit schema mapping entries");
		return Ok(result);
	}

	info!(
		replica = %replica_name,
		restore = restore_name,
		restore_dbname = restore_dbname,
		"no explicit schema mapping, discovering schemas from restore database"
	);
	let conn = super::connect::connect_to_restore(
		client,
		namespace,
		restore_name,
		restore_dbname,
		fdw_user,
		fdw_password,
		use_port_forward,
	)
	.await?;

	let rows = conn
		.client
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
		replica = %replica_name,
		schema_count = result.len(),
		"discovered schemas from restore database"
	);
	debug!(schemas = ?result, "discovered schema entries");
	Ok(result)
}

/// Discover all custom domains from the remote database.
///
/// Returns domains from all user schemas. The list is ordered so that
/// domains depending on other domains come after their dependencies.
async fn discover_remote_domains(pg: &tokio_postgres::Client) -> Result<Vec<RemoteDomain>> {
	let rows = pg
		.query(
			"WITH RECURSIVE domain_tree AS ( \
			   SELECT t.oid, n.nspname, t.typname, \
			          format_type(t.typbasetype, t.typtypmod) AS base_type, \
			          0 AS depth \
			   FROM pg_type t \
			   JOIN pg_namespace n ON t.typnamespace = n.oid \
			   WHERE t.typtype = 'd' \
			     AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
			     AND n.nspname NOT LIKE 'pg_toast%' \
			     AND NOT EXISTS ( \
			       SELECT 1 FROM pg_type bt \
			       JOIN pg_namespace bn ON bt.typnamespace = bn.oid \
			       WHERE bt.oid = t.typbasetype AND bt.typtype = 'd' \
			         AND bn.nspname NOT IN ('pg_catalog', 'information_schema') \
			     ) \
			   UNION ALL \
			   SELECT t.oid, n.nspname, t.typname, \
			          format_type(t.typbasetype, t.typtypmod) AS base_type, \
			          dt.depth + 1 \
			   FROM pg_type t \
			   JOIN pg_namespace n ON t.typnamespace = n.oid \
			   JOIN domain_tree dt ON t.typbasetype = dt.oid \
			   WHERE t.typtype = 'd' \
			 ) \
			 SELECT nspname, typname, base_type, depth \
			 FROM domain_tree \
			 ORDER BY depth, nspname, typname",
			&[],
		)
		.await?;

	let result: Vec<RemoteDomain> = rows
		.iter()
		.map(|row| RemoteDomain {
			schema: row.get(0),
			name: row.get(1),
			base_type: row.get(2),
		})
		.collect();

	debug!(count = result.len(), "discovered remote domains");
	Ok(result)
}

/// Discover all custom enum types from the remote database.
async fn discover_remote_enums(pg: &tokio_postgres::Client) -> Result<Vec<RemoteEnum>> {
	let rows = pg
		.query(
			"SELECT n.nspname, t.typname, \
			        array_agg(e.enumlabel ORDER BY e.enumsortorder)::text[] AS labels \
			 FROM pg_type t \
			 JOIN pg_namespace n ON t.typnamespace = n.oid \
			 JOIN pg_enum e ON e.enumtypid = t.oid \
			 WHERE t.typtype = 'e' \
			   AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
			   AND n.nspname NOT LIKE 'pg_toast%' \
			 GROUP BY n.nspname, t.typname \
			 ORDER BY n.nspname, t.typname",
			&[],
		)
		.await?;

	let result: Vec<RemoteEnum> = rows
		.iter()
		.map(|row| RemoteEnum {
			schema: row.get(0),
			name: row.get(1),
			labels: row.get(2),
		})
		.collect();

	debug!(count = result.len(), "discovered remote enums");
	Ok(result)
}

/// Create stub custom types (domains and enums) on the overlay database so
/// that `IMPORT FOREIGN SCHEMA` can resolve column types.
async fn ensure_custom_types_on_overlay(
	overlay_pg: &tokio_postgres::Client,
	domains: &[RemoteDomain],
	enums: &[RemoteEnum],
) -> Result<()> {
	if domains.is_empty() && enums.is_empty() {
		return Ok(());
	}

	// Collect all schemas we need to ensure exist
	let schemas: BTreeSet<&str> = domains
		.iter()
		.map(|d| d.schema.as_str())
		.chain(enums.iter().map(|e| e.schema.as_str()))
		.collect();

	for schema in &schemas {
		overlay_pg
			.batch_execute(&format!(
				"CREATE SCHEMA IF NOT EXISTS {}",
				quote_ident(schema)
			))
			.await?;
	}

	// Create enums first (domains might reference them, though unlikely)
	for e in enums {
		let qualified = format!("{}.{}", quote_ident(&e.schema), quote_ident(&e.name));
		let labels_sql: Vec<String> = e.labels.iter().map(|l| quote_literal(l)).collect();
		let sql = format!(
			"DO $$ BEGIN \
			   CREATE TYPE {qualified} AS ENUM ({}); \
			 EXCEPTION WHEN duplicate_object THEN NULL; \
			 END $$;",
			labels_sql.join(", ")
		);
		if let Err(err) = overlay_pg.batch_execute(&sql).await {
			warn!(
				schema = %e.schema,
				name = %e.name,
				error = %err,
				"failed to create stub enum type, IMPORT FOREIGN SCHEMA may fail for columns using it"
			);
		} else {
			debug!(schema = %e.schema, name = %e.name, "created stub enum type");
		}
	}

	// Create domains in dependency order (the query returns them sorted)
	for d in domains {
		let qualified = format!("{}.{}", quote_ident(&d.schema), quote_ident(&d.name));
		let sql = format!(
			"DO $$ BEGIN \
			   CREATE DOMAIN {qualified} AS {}; \
			 EXCEPTION WHEN duplicate_object THEN NULL; \
			 END $$;",
			d.base_type
		);
		if let Err(err) = overlay_pg.batch_execute(&sql).await {
			warn!(
				schema = %d.schema,
				name = %d.name,
				base_type = %d.base_type,
				error = %err,
				"failed to create stub domain, IMPORT FOREIGN SCHEMA may fail for columns using it"
			);
		} else {
			debug!(schema = %d.schema, name = %d.name, base_type = %d.base_type, "created stub domain");
		}
	}

	info!(
		domains = domains.len(),
		enums = enums.len(),
		"ensured custom stub types on overlay database"
	);
	Ok(())
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
	use_port_forward: bool,
) -> Result<()> {
	let replica_name = replica.name_any();
	let cluster_name = super::overlay_cluster_name(&replica_name);
	let fdw_secret_name = super::overlay_fdw_secret_name(&replica_name);
	let superuser_secret_name = format!("{cluster_name}-superuser");
	let server_name = super::overlay_fdw_server_name(restore_name);
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

	let fdw_user = super::connect::read_secret_field(&fdw_secret, "username")?;
	let fdw_password = super::connect::read_secret_field(&fdw_secret, "password")?;

	let overlay_conn = super::connect::connect_overlay(
		client,
		&cluster_name,
		namespace,
		&su_secret,
		use_port_forward,
	)
	.await?;
	let overlay_pg = &overlay_conn.client;

	// Discover the main database in the restore (largest by size)
	let restore_dbname = discover_restore_database(
		client,
		namespace,
		restore_name,
		&fdw_user,
		&fdw_password,
		use_port_forward,
	)
	.await?;

	// Check current state
	let state = check_fdw_state(overlay_pg, &server_name).await?;

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
	drop_stale_fdw_servers(overlay_pg, &server_name, &replica_name).await?;

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

	// Discover and replicate custom types from the restore database
	let restore_conn = super::connect::connect_to_restore(
		client,
		namespace,
		restore_name,
		&restore_dbname,
		&fdw_user,
		&fdw_password,
		use_port_forward,
	)
	.await?;
	let domains = discover_remote_domains(&restore_conn.client).await?;
	let enums = discover_remote_enums(&restore_conn.client).await?;
	drop(restore_conn);

	if !domains.is_empty() || !enums.is_empty() {
		info!(
			replica = %replica_name,
			domains = domains.len(),
			enums = enums.len(),
			"replicating custom types from restore to overlay"
		);
		ensure_custom_types_on_overlay(overlay_pg, &domains, &enums).await?;
	}

	// Resolve expected schemas and import any that are missing
	let schemas = resolve_fdw_schemas(
		client,
		namespace,
		replica,
		restore_name,
		&restore_dbname,
		&fdw_user,
		&fdw_password,
		use_port_forward,
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
		let import_generated = replica
			.spec
			.overlay_database
			.as_ref()
			.is_some_and(|c| c.import_generated);
		overlay_pg
			.batch_execute(&format!(
				"IMPORT FOREIGN SCHEMA {remote_quoted} FROM SERVER {server_name} INTO {local_quoted} OPTIONS (import_collate 'false', import_generated '{import_generated}')"
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

/// Ensure the FDW credentials Secret exists.
pub async fn ensure_fdw_credentials(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<()> {
	let secret_name = super::overlay_fdw_secret_name(replica_name);
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

	let password = generate_password();
	let secret = Secret {
		metadata: ObjectMeta {
			name: Some(secret_name.clone()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([(
				"pgro.bes.au/replica".to_string(),
				replica_name.to_string(),
			)])),
			owner_references: Some(vec![replica.owner_reference()]),
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

/// Escape a SQL identifier by double-quoting it.
fn quote_ident(s: &str) -> String {
	format!("\"{}\"", s.replace('"', "\"\""))
}

/// Escape a SQL string literal by single-quoting it.
fn quote_literal(s: &str) -> String {
	format!("'{}'", s.replace('\'', "''"))
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
}
