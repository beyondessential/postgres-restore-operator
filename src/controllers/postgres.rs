use std::{collections::HashSet, time::Duration};

use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::{
	Api, Client, ResourceExt,
	api::{ListParams, Portforwarder},
};
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

use crate::error::{Error, Result};

pub const DEFAULT_PG_VERSION: i32 = 18;

/// TCP keepalive parameters for operator-side libpq sockets. Matches the
/// settings the migration Job uses in its libpq URI (see [[fix(migration)
/// TCP keepalives]]). Detects NAT-evicted / silently-dead sockets within
/// ~90s so reconciles can fail and retry instead of hanging forever.
/// Keepalives fire on the kernel's own timer regardless of in-flight
/// queries, so a legitimately long-running `DROP SCHEMA ... CASCADE` on
/// a populated dbt schema is fine — only dead sockets are killed.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_RETRIES: u32 = 3;

fn set_tcp_keepalive(stream: &TcpStream) -> Result<()> {
	let ka = TcpKeepalive::new()
		.with_time(KEEPALIVE_IDLE)
		.with_interval(KEEPALIVE_INTERVAL)
		.with_retries(KEEPALIVE_RETRIES);
	SockRef::from(stream)
		.set_tcp_keepalive(&ka)
		.map_err(|e| Error::MissingField(format!("failed to set TCP keepalive: {e}")))?;
	Ok(())
}

/// Holds a Postgres client and any resources that must stay alive for the
/// duration of the connection (e.g. a port-forwarder).
pub struct PgConnection {
	pub client: tokio_postgres::Client,
	_port_forwarder: Option<Portforwarder>,
}

/// Connect to a Postgres instance inside a Kubernetes pod via the kube
/// API port-forward mechanism. This works both in-cluster and
/// out-of-cluster (e.g. CI with a kind cluster).
async fn connect_via_port_forward(
	client: &Client,
	namespace: &str,
	pod_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
) -> Result<PgConnection> {
	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let mut pf = pods.portforward(pod_name, &[5432]).await?;
	let stream = pf
		.take_stream(5432)
		.ok_or_else(|| Error::MissingField("port-forward stream not available on 5432".into()))?;

	let mut config = tokio_postgres::Config::new();
	config.user(user);
	config.password(password);
	config.dbname(dbname);

	let (pg, conn) = config.connect_raw(stream, NoTls).await?;
	tokio::spawn(async move {
		if let Err(e) = conn.await {
			warn!(error = %e, "port-forwarded database connection error");
		}
	});

	Ok(PgConnection {
		client: pg,
		_port_forwarder: Some(pf),
	})
}

/// Connect to a Postgres instance via direct TCP (in-cluster networking).
async fn connect_via_tcp(
	host: &str,
	port: u16,
	dbname: &str,
	user: &str,
	password: &str,
) -> Result<PgConnection> {
	let addr = format!("{host}:{port}");
	let stream = TcpStream::connect(&addr)
		.await
		.map_err(|e| Error::MissingField(format!("failed to connect to {addr}: {e}")))?;
	set_tcp_keepalive(&stream)?;

	let mut config = tokio_postgres::Config::new();
	config.user(user);
	config.password(password);
	config.dbname(dbname);

	let (pg, conn) = config.connect_raw(stream, NoTls).await?;
	tokio::spawn(async move {
		if let Err(e) = conn.await {
			warn!(error = %e, "TCP database connection error");
		}
	});

	Ok(PgConnection {
		client: pg,
		_port_forwarder: None,
	})
}

/// Find the name of a running pod that matches the given label selector.
pub async fn find_pod_by_label(
	client: &Client,
	namespace: &str,
	label_selector: &str,
) -> Result<String> {
	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let lp = ListParams::default().labels(label_selector);
	let list = pods.list(&lp).await?;
	let pod = list
		.items
		.into_iter()
		.find(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"))
		.ok_or_else(|| {
			Error::MissingField(format!(
				"no running pod found with label {label_selector} in {namespace}"
			))
		})?;
	Ok(pod.name_any())
}

/// Find the IP of a running pod that matches the given label selector.
pub async fn find_pod_ip_by_label(
	client: &Client,
	namespace: &str,
	label_selector: &str,
) -> Result<String> {
	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let lp = ListParams::default().labels(label_selector);
	let list = pods.list(&lp).await?;
	let pod = list
		.items
		.into_iter()
		.find(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"))
		.ok_or_else(|| {
			Error::MissingField(format!(
				"no running pod found with label {label_selector} in {namespace}"
			))
		})?;
	let ip = pod
		.status
		.as_ref()
		.and_then(|s| s.pod_ip.as_ref())
		.ok_or_else(|| Error::MissingField(format!("pod {} has no podIP", pod.name_any())))?;
	Ok(ip.clone())
}

/// Connect to a restore's Postgres instance.
///
/// When `use_port_forward` is true, finds the restore pod by label and
/// connects via port-forward. Otherwise connects directly via the pod IP.
pub async fn connect_to_restore(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
	use_port_forward: bool,
) -> Result<PgConnection> {
	let label_selector = format!("pgro.bes.au/restore={restore_name}");

	if use_port_forward {
		let pod_name = find_pod_by_label(client, namespace, &label_selector).await?;
		info!(pod = %pod_name, "connecting to restore database via port-forward");
		connect_via_port_forward(client, namespace, &pod_name, dbname, user, password).await
	} else {
		let pod_ip = find_pod_ip_by_label(client, namespace, &label_selector).await?;
		info!(ip = %pod_ip, restore = %restore_name, "connecting to restore database via TCP");
		connect_via_tcp(&pod_ip, 5432, dbname, user, password).await
	}
}

/// Read a UTF-8 string field from a Kubernetes Secret.
pub fn read_secret_field(secret: &Secret, key: &str) -> Result<String> {
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

/// Connect to the restore's `postgres` database and find the largest
/// non-system database by size. This is the database whose schemas we
/// use for schema migration.
pub async fn discover_restore_database(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	reader_user: &str,
	reader_password: &str,
	use_port_forward: bool,
) -> Result<String> {
	let conn = connect_to_restore(
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

/// Query the on-disk size of the current database on an already-open
/// connection. The connection-opening variant is `measure_database_size`,
/// kept for callers that only need this single query; prefer this when
/// you're going to make multiple queries on the same database so the
/// connection can be reused.
pub async fn database_size_on(pg: &tokio_postgres::Client) -> Result<u64> {
	let row = pg
		.query_one("SELECT pg_database_size(current_database())", &[])
		.await?;
	let size: i64 = row.get(0);
	Ok(size as u64)
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
	let conn = connect_to_restore(
		client,
		namespace,
		restore_name,
		dbname,
		user,
		password,
		use_port_forward,
	)
	.await?;
	database_size_on(&conn.client).await
}

/// Escape a SQL identifier by double-quoting it.
pub fn quote_ident(s: &str) -> String {
	format!("\"{}\"", s.replace('"', "\"\""))
}

/// Return the subset of `candidates` that exist as schemas in the
/// database the connection is bound to. Reusable on an open connection.
pub async fn existing_schemas_on(
	pg: &tokio_postgres::Client,
	candidates: &[String],
) -> Result<Vec<String>> {
	if candidates.is_empty() {
		return Ok(Vec::new());
	}
	let rows = pg
		.query(
			"SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = ANY($1)",
			&[&candidates],
		)
		.await?;
	let found: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
	Ok(candidates
		.iter()
		.filter(|s| found.contains(s.as_str()))
		.cloned()
		.collect())
}

/// Drop the given schemas (and all contents) on an already-open connection.
/// Idempotent: missing schemas are skipped via `DROP SCHEMA IF EXISTS`.
pub async fn drop_schemas_on(pg: &tokio_postgres::Client, schemas: &[String]) -> Result<()> {
	for schema in schemas {
		let stmt = format!("DROP SCHEMA IF EXISTS {} CASCADE", quote_ident(schema));
		debug!(schema = schema, "dropping schema");
		pg.execute(stmt.as_str(), &[]).await?;
	}
	Ok(())
}

/// Drop the given schemas (and all their contents) from the named database
/// in the restore. Idempotent: schemas that do not exist are silently
/// skipped via `DROP SCHEMA IF EXISTS`. Used to wipe persistent_schemas in
/// a new restore before the migration writes the persistent copies from
/// the previous restore.
///
/// Assumes the connecting user owns each schema (the restore's init script
/// reassigns ownership to the analytics user at startup, see
/// `controllers::restore::builders`).
#[expect(
	clippy::too_many_arguments,
	reason = "mirrors the surrounding connect_to_restore signature; refactoring all of them is out of scope"
)]
pub async fn drop_schemas_in_restore(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
	schemas: &[String],
	use_port_forward: bool,
) -> Result<()> {
	if schemas.is_empty() {
		return Ok(());
	}
	let conn = connect_to_restore(
		client,
		namespace,
		restore_name,
		dbname,
		user,
		password,
		use_port_forward,
	)
	.await?;
	drop_schemas_on(&conn.client, schemas).await
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
}
