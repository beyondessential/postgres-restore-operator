use std::{
	collections::BTreeMap,
	hash::{DefaultHasher, Hash, Hasher},
};

use jiff::Timestamp;
use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{
	Api, Client, ResourceExt,
	api::{ObjectMeta, PostParams},
};
use tracing::{debug, info};

use crate::{
	controllers::replica::generate_password,
	error::{Error, Result},
	types::PostgresPhysicalReplica,
};

pub const DEFAULT_PG_VERSION: i32 = 18;

/// Tracked overlay reconciliation state persisted in `_pgro.overlay_state`.
pub struct TrackedState {
	pub config_hash: String,
	pub phase: String,
	pub retries: i32,
	pub updated_at: Timestamp,
	pub last_error: Option<String>,
}

/// Ensure the `_pgro.overlay_state` tracking table exists.
pub async fn ensure_state_table(pg: &tokio_postgres::Client) -> Result<()> {
	pg.batch_execute(
		"CREATE TABLE IF NOT EXISTS _pgro.overlay_state ( \
		   id integer PRIMARY KEY DEFAULT 1, \
		   config_hash text NOT NULL, \
		   phase text NOT NULL DEFAULT 'pending', \
		   retries integer NOT NULL DEFAULT 0, \
		   updated_at timestamptz NOT NULL DEFAULT now(), \
		   last_error text \
		 )",
	)
	.await?;

	// Migrate: add columns if missing (existing deployments with old fdw_state table)
	pg.batch_execute(
		"DO $$ BEGIN \
		   ALTER TABLE _pgro.overlay_state ADD COLUMN IF NOT EXISTS retries integer NOT NULL DEFAULT 0; \
		   ALTER TABLE _pgro.overlay_state ADD COLUMN IF NOT EXISTS last_error text; \
		 EXCEPTION WHEN undefined_table THEN NULL; \
		 END $$",
	)
	.await?;

	Ok(())
}

/// Migrate from the old `_pgro.fdw_state` table to `_pgro.overlay_state`.
///
/// Copies any existing row so that completed FDW reconciliations are not
/// re-run after an operator upgrade.
pub async fn migrate_from_fdw_state(pg: &tokio_postgres::Client) -> Result<()> {
	pg.batch_execute(
		"DO $$ BEGIN \
		   IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '_pgro' AND table_name = 'fdw_state') \
		      AND NOT EXISTS (SELECT 1 FROM _pgro.overlay_state WHERE id = 1) THEN \
		     INSERT INTO _pgro.overlay_state (id, config_hash, phase, retries, updated_at) \
		       SELECT 1, config_hash, phase, 0, updated_at FROM _pgro.fdw_state WHERE id = 1; \
		   END IF; \
		 EXCEPTION WHEN undefined_table THEN NULL; \
		 END $$",
	)
	.await?;
	Ok(())
}

/// Read the current overlay reconciliation state from the tracking table.
pub async fn read_state(pg: &tokio_postgres::Client) -> Result<Option<TrackedState>> {
	let row = pg
		.query_opt(
			"SELECT config_hash, phase, retries, \
			   EXTRACT(EPOCH FROM updated_at)::bigint AS updated_epoch, \
			   last_error \
			 FROM _pgro.overlay_state WHERE id = 1",
			&[],
		)
		.await?;
	Ok(row.map(|r| {
		let epoch: i64 = r.get(3);
		TrackedState {
			config_hash: r.get(0),
			phase: r.get(1),
			retries: r.get(2),
			updated_at: Timestamp::from_second(epoch).unwrap_or(Timestamp::UNIX_EPOCH),
			last_error: r.get(4),
		}
	}))
}

/// Update the overlay reconciliation phase in the tracking table.
pub async fn write_state(
	pg: &tokio_postgres::Client,
	config_hash: &str,
	phase: &str,
	retries: i32,
	last_error: Option<&str>,
) -> Result<()> {
	pg.execute(
		"INSERT INTO _pgro.overlay_state (id, config_hash, phase, retries, updated_at, last_error) \
		 VALUES (1, $1, $2, $3, now(), $4) \
		 ON CONFLICT (id) DO UPDATE \
		   SET config_hash = $1, phase = $2, retries = $3, updated_at = now(), last_error = $4",
		&[&config_hash, &phase, &retries, &last_error],
	)
	.await?;
	Ok(())
}

/// Compute a stable hash of the inputs that affect overlay reconciliation.
///
/// If any of these change the setup needs to be redone from scratch.
pub fn compute_config_hash(restore_name: &str, replica: &PostgresPhysicalReplica) -> String {
	let mut hasher = DefaultHasher::new();
	env!("CARGO_PKG_VERSION").hash(&mut hasher);
	restore_name.hash(&mut hasher);
	replica.spec.analytics_username.hash(&mut hasher);
	if let Some(overlay) = &replica.spec.overlay_database {
		overlay.strategy.hash(&mut hasher);
		overlay.import_generated.hash(&mut hasher);
	}
	format!("{:016x}", hasher.finish())
}

/// Connect to the restore's `postgres` database and find the largest
/// non-system database by size. This is the database whose schemas we
/// import into the overlay's `app` database.
pub async fn discover_restore_database(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	reader_user: &str,
	reader_password: &str,
	use_port_forward: bool,
) -> Result<String> {
	let conn = super::connect::connect_to_restore(
		client,
		namespace,
		restore_name,
		"postgres",
		reader_user,
		reader_password,
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
			debug!(
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

/// Discover user schemas from the restore database.
#[expect(
	clippy::too_many_arguments,
	reason = "internal helper with tightly-coupled params"
)]
pub async fn resolve_schemas(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	restore_name: &str,
	restore_dbname: &str,
	reader_user: &str,
	reader_password: &str,
	use_port_forward: bool,
) -> Result<Vec<(String, String)>> {
	let replica_name = replica.name_any();

	debug!(
		replica = %replica_name,
		restore = restore_name,
		restore_dbname = restore_dbname,
		"discovering schemas from restore database"
	);
	let conn = super::connect::connect_to_restore(
		client,
		namespace,
		restore_name,
		restore_dbname,
		reader_user,
		reader_password,
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
	debug!(
		replica = %replica_name,
		schema_count = result.len(),
		"discovered schemas from restore database"
	);
	debug!(schemas = ?result, "discovered schema entries");
	Ok(result)
}

/// Ensure the reader credentials Secret exists.
pub async fn ensure_reader_credentials(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<()> {
	let secret_name = super::overlay_reader_secret_name(replica_name);
	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

	if secrets.get_opt(&secret_name).await?.is_some() {
		debug!(
			replica = replica_name,
			secret = secret_name,
			"reader credentials secret already exists, skipping creation"
		);
		return Ok(());
	}

	info!(
		replica = replica_name,
		secret = secret_name,
		"creating overlay reader credentials secret"
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
				ByteString("overlay_reader".as_bytes().to_vec()),
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

/// Query the on-disk size of the given database (bytes) via `pg_database_size()`.
pub async fn measure_database_size(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
	use_port_forward: bool,
) -> Result<u64> {
	let conn = super::connect::connect_to_restore(
		client,
		namespace,
		restore_name,
		dbname,
		user,
		password,
		use_port_forward,
	)
	.await?;

	let row = conn
		.client
		.query_one("SELECT pg_database_size(current_database())", &[])
		.await?;

	let size: i64 = row.get(0);
	Ok(size as u64)
}

/// Escape a SQL identifier by double-quoting it.
pub fn quote_ident(s: &str) -> String {
	format!("\"{}\"", s.replace('"', "\"\""))
}

/// Escape a SQL string literal by single-quoting it.
pub fn quote_literal(s: &str) -> String {
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

	#[test]
	fn config_hash_changes_with_strategy() {
		use crate::types::OverlayStrategy;
		use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

		let make_replica = |strategy: OverlayStrategy| -> PostgresPhysicalReplica {
			PostgresPhysicalReplica {
				metadata: ObjectMeta {
					name: Some("test".into()),
					namespace: Some("default".into()),
					..Default::default()
				},
				spec: crate::types::PostgresPhysicalReplicaSpec {
					kopia_secret_ref: Default::default(),
					snapshot_filter: None,
					schedule: "0 * * * *".into(),
					schedule_jitter: crate::util::TimeSpan(jiff::Span::new()),
					minimum_ttl: None,
					switchover_grace_period: crate::util::TimeSpan(jiff::Span::new()),
					analytics_username: "analytics".into(),
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
					overlay_database: Some(crate::types::OverlayDatabaseConfig {
						strategy,
						postgres_version: None,
						image_catalog: None,
						storage_size_override: None,
						storage_class: None,
						resources: None,
						affinity: None,
						tolerations: vec![],
						service_annotations: None,
						import_generated: false,
						retain_restore: true,
					}),
					persistent_schemas: None,
				},
				status: None,
			}
		};

		let fdw_hash = compute_config_hash("restore-1", &make_replica(OverlayStrategy::Fdw));
		let copy_hash = compute_config_hash("restore-1", &make_replica(OverlayStrategy::Copy));
		assert_ne!(fdw_hash, copy_hash);
	}
}
