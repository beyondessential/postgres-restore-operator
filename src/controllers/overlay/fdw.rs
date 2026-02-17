use std::collections::BTreeSet;

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client, ResourceExt};
use tracing::{debug, info, warn};

use crate::{error::Result, types::PostgresPhysicalReplica};

use super::common::{
	compute_config_hash, discover_restore_database, ensure_state_table, migrate_from_fdw_state,
	quote_ident, quote_literal, read_state, resolve_schemas, write_state,
};

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

/// Captured FDW state from the overlay database.
struct FdwState {
	has_extension: bool,
	server_host: Option<String>,
	server_dbname: Option<String>,
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

	Ok(FdwState {
		has_extension,
		server_host,
		server_dbname,
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
			debug!(
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

/// Ensure the `_pgro.stub_types` tracking table exists.
async fn ensure_stub_types_table(pg: &tokio_postgres::Client) -> Result<()> {
	pg.batch_execute(
		"CREATE TABLE IF NOT EXISTS _pgro.stub_types ( \
		   schema_name text NOT NULL, \
		   type_name text NOT NULL, \
		   kind text NOT NULL, \
		   PRIMARY KEY (schema_name, type_name) \
		 )",
	)
	.await?;
	Ok(())
}

/// Drop all previously tracked stub types from the overlay database.
///
/// Domains are dropped before enums because a domain could reference an enum.
/// Within domains, we drop in reverse insertion order to respect dependencies
/// (domains inserted later may depend on earlier ones).
async fn drop_tracked_stub_types(pg: &tokio_postgres::Client) -> Result<()> {
	// Drop domains first (in reverse order to handle dependencies)
	let domain_rows = pg
		.query(
			"SELECT schema_name, type_name FROM _pgro.stub_types \
			 WHERE kind = 'domain' \
			 ORDER BY ctid DESC",
			&[],
		)
		.await?;
	for row in &domain_rows {
		let schema: String = row.get(0);
		let name: String = row.get(1);
		let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&name));
		pg.batch_execute(&format!("DROP DOMAIN IF EXISTS {qualified} CASCADE"))
			.await?;
	}

	// Then drop enums
	let enum_rows = pg
		.query(
			"SELECT schema_name, type_name FROM _pgro.stub_types \
			 WHERE kind = 'enum'",
			&[],
		)
		.await?;
	for row in &enum_rows {
		let schema: String = row.get(0);
		let name: String = row.get(1);
		let qualified = format!("{}.{}", quote_ident(&schema), quote_ident(&name));
		pg.batch_execute(&format!("DROP TYPE IF EXISTS {qualified} CASCADE"))
			.await?;
	}

	let total = domain_rows.len() + enum_rows.len();
	if total > 0 {
		debug!(
			domains = domain_rows.len(),
			enums = enum_rows.len(),
			"dropped stale stub types from previous restore"
		);
	}

	pg.batch_execute("DELETE FROM _pgro.stub_types").await?;
	Ok(())
}

/// Record a successfully created stub type in the tracking table.
async fn track_stub_type(
	pg: &tokio_postgres::Client,
	schema: &str,
	name: &str,
	kind: &str,
) -> Result<()> {
	pg.execute(
		"INSERT INTO _pgro.stub_types (schema_name, type_name, kind) \
		 VALUES ($1, $2, $3) \
		 ON CONFLICT (schema_name, type_name) DO NOTHING",
		&[&schema, &name, &kind],
	)
	.await?;
	Ok(())
}

/// Drop all previously tracked stub types and clear the tracking table.
///
/// Called once before the schema import loop so that stale types from a
/// previous restore are removed.
async fn cleanup_stale_stub_types(overlay_pg: &tokio_postgres::Client) -> Result<()> {
	ensure_stub_types_table(overlay_pg).await?;
	drop_tracked_stub_types(overlay_pg).await?;
	Ok(())
}

/// Create stub custom types (domains and enums) on the overlay database so
/// that `IMPORT FOREIGN SCHEMA` can resolve column types.
///
/// This is idempotent: types that already exist are silently skipped via
/// `EXCEPTION WHEN duplicate_object`. It must be called inside the import
/// loop (after `DROP SCHEMA ... CASCADE` + `CREATE SCHEMA`) because
/// dropping a schema also drops any stub types that lived in it.
async fn create_stub_types(
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
			track_stub_type(overlay_pg, &e.schema, &e.name, "enum").await?;
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
			track_stub_type(overlay_pg, &d.schema, &d.name, "domain").await?;
		}
	}

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
	let reader_secret_name = super::overlay_reader_secret_name(&replica_name);
	let superuser_secret_name = format!("{cluster_name}-superuser");
	let server_name = super::overlay_fdw_server_name(restore_name);
	let restore_host = format!("{restore_name}.{namespace}.svc");

	debug!(
		replica = %replica_name,
		restore = %restore_name,
		server = %server_name,
		"reconciling FDW state"
	);

	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
	debug!(
		superuser_secret = %superuser_secret_name,
		reader_secret = %reader_secret_name,
		"fetching secrets for FDW reconciliation"
	);
	let su_secret = secrets.get(&superuser_secret_name).await?;
	let reader_secret = secrets.get(&reader_secret_name).await?;

	let reader_user = super::connect::read_secret_field(&reader_secret, "username")?;
	let reader_password = super::connect::read_secret_field(&reader_secret, "password")?;

	let overlay_conn = super::connect::connect_overlay(
		client,
		&cluster_name,
		namespace,
		&su_secret,
		use_port_forward,
	)
	.await?;
	let overlay_pg = &overlay_conn.client;

	// Ensure _pgro schema exists (needed for state tracking even before
	// the postgres_fdw extension is installed).
	overlay_pg
		.batch_execute("CREATE SCHEMA IF NOT EXISTS _pgro")
		.await?;

	// Check if FDW reconciliation is already complete for this config
	let config_hash = compute_config_hash(restore_name, replica);
	ensure_state_table(overlay_pg).await?;
	migrate_from_fdw_state(overlay_pg).await?;
	let tracked = read_state(overlay_pg).await?;
	if tracked
		.as_ref()
		.is_some_and(|t| t.config_hash == config_hash && t.phase == "complete")
	{
		debug!(
			replica = %replica_name,
			restore = %restore_name,
			"FDW reconciliation already complete for this config, skipping"
		);
		return Ok(());
	}
	write_state(overlay_pg, &config_hash, "pending", 0).await?;

	// Discover the main database in the restore (largest by size)
	let restore_dbname = discover_restore_database(
		client,
		namespace,
		restore_name,
		&reader_user,
		&reader_password,
		use_port_forward,
	)
	.await?;

	// Check current state
	let state = check_fdw_state(overlay_pg, &server_name).await?;

	let server_host_correct = state.server_host.as_deref() == Some(restore_host.as_str());
	let server_dbname_correct = state.server_dbname.as_deref() == Some(restore_dbname.as_str());

	debug!(
		replica = %replica_name,
		has_extension = state.has_extension,
		server_exists = state.server_host.is_some(),
		server_host_correct,
		server_dbname = ?state.server_dbname,
		expected_dbname = %restore_dbname,
		server_dbname_correct,
		"current FDW state in overlay database"
	);

	// Drop stale servers from previous restores
	drop_stale_fdw_servers(overlay_pg, &server_name, &replica_name).await?;

	// Ensure extension
	if !state.has_extension {
		debug!(replica = %replica_name, "creating postgres_fdw extension");
		overlay_pg
			.batch_execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw SCHEMA _pgro")
			.await?;
	} else {
		debug!(replica = %replica_name, "postgres_fdw extension already present");
	}

	// Ensure analytics user can create schemas in the overlay database
	let analytics_user = &replica.spec.analytics_username;
	overlay_pg
		.batch_execute(&format!(
			"GRANT CREATE ON DATABASE app TO {}",
			quote_ident(analytics_user)
		))
		.await?;

	// Ensure FDW server with correct host and dbname
	if state.server_host.is_none() {
		debug!(
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
			debug!(
				replica = %replica_name,
				server = %server_name,
				old_host = ?state.server_host,
				new_host = %restore_host,
				"FDW server host needs update"
			);
			alter_parts.push(format!("SET host {}", quote_literal(&restore_host)));
		}
		if !server_dbname_correct {
			debug!(
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

	// Ensure PUBLIC user mapping (always recreate to pick up credential changes).
	// A PUBLIC mapping applies to all overlay users that don't have a specific one.
	let has_public_mapping = overlay_pg
		.query_opt(
			"SELECT 1 FROM pg_user_mappings WHERE srvname = $1 AND usename = 'public'",
			&[&server_name.as_str()],
		)
		.await?
		.is_some();
	if has_public_mapping {
		debug!(server = %server_name, "dropping existing PUBLIC user mapping before recreation");
		overlay_pg
			.batch_execute(&format!(
				"DROP USER MAPPING FOR PUBLIC SERVER {server_name}"
			))
			.await?;
	}
	debug!(server = %server_name, reader_user = %reader_user, "creating PUBLIC user mapping");
	overlay_pg
		.batch_execute(&format!(
			"CREATE USER MAPPING FOR PUBLIC SERVER {server_name} \
			 OPTIONS (user {}, password {})",
			quote_literal(&reader_user),
			quote_literal(&reader_password),
		))
		.await?;

	write_state(overlay_pg, &config_hash, "server_configured", 0).await?;

	// Discover and replicate custom types from the restore database
	let restore_conn = super::connect::connect_to_restore(
		client,
		namespace,
		restore_name,
		&restore_dbname,
		&reader_user,
		&reader_password,
		use_port_forward,
	)
	.await?;
	let domains = discover_remote_domains(&restore_conn.client).await?;
	let enums = discover_remote_enums(&restore_conn.client).await?;
	drop(restore_conn);

	if !domains.is_empty() || !enums.is_empty() {
		debug!(
			replica = %replica_name,
			domains = domains.len(),
			enums = enums.len(),
			"replicating custom types from restore to overlay"
		);
	}

	// Drop stale stub types from a previous restore once before the loop
	cleanup_stale_stub_types(overlay_pg).await?;

	write_state(overlay_pg, &config_hash, "importing", 0).await?;

	// Resolve expected schemas and import any that are missing
	let schemas = resolve_schemas(
		client,
		namespace,
		replica,
		restore_name,
		&restore_dbname,
		&reader_user,
		&reader_password,
		use_port_forward,
	)
	.await?;

	for (remote, local) in &schemas {
		debug!(
			remote = %remote,
			local = %local,
			"importing foreign schema"
		);
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
		// Re-create stub types after schema drop since DROP SCHEMA CASCADE
		// destroys any stub types that lived in the dropped schema.
		create_stub_types(overlay_pg, &domains, &enums).await?;
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

	write_state(overlay_pg, &config_hash, "complete", 0).await?;

	info!(
		replica = %replica_name,
		restore = %restore_name,
		total_schemas = schemas.len(),
		"FDW reconciliation complete"
	);

	Ok(())
}
