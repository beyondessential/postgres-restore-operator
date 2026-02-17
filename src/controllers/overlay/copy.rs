use jiff::{SignedDuration, Timestamp};
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client, ResourceExt, api::AttachParams};
use tokio::io::AsyncReadExt;
use tracing::{debug, info, warn};

use crate::{
	error::{Error, Result},
	types::PostgresPhysicalReplica,
};

use super::common::{
	compute_config_hash, discover_restore_database, ensure_state_table, migrate_from_fdw_state,
	quote_ident, read_state, resolve_schemas, write_state,
};

const MAX_COPY_RETRIES: i32 = 3;
const RETRY_COOLDOWN_SECS: i64 = 300; // 5 minutes

/// Execute a command inside a pod via the Kubernetes exec API and return
/// the combined stdout+stderr output. Returns an error if the command
/// exits with a non-zero status.
async fn exec_in_pod(
	client: &Client,
	namespace: &str,
	pod_name: &str,
	command: &[&str],
) -> Result<String> {
	let pods: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(client.clone(), namespace);
	let attach = AttachParams {
		stdout: true,
		stderr: true,
		stdin: false,
		tty: false,
		..Default::default()
	};

	let owned_command: Vec<String> = command.iter().map(|s| (*s).to_owned()).collect();
	let mut process = pods.exec(pod_name, owned_command, &attach).await?;

	let status_future = process.take_status().ok_or_else(|| {
		Error::MissingField("exec process did not provide a status channel".into())
	})?;

	let mut stdout_buf = Vec::new();
	if let Some(mut stdout) = process.stdout() {
		stdout
			.read_to_end(&mut stdout_buf)
			.await
			.map_err(|e| Error::MissingField(format!("failed to read exec stdout: {e}")))?;
	}

	let mut stderr_buf = Vec::new();
	if let Some(mut stderr) = process.stderr() {
		stderr
			.read_to_end(&mut stderr_buf)
			.await
			.map_err(|e| Error::MissingField(format!("failed to read exec stderr: {e}")))?;
	}

	let status = status_future
		.await
		.ok_or_else(|| Error::MissingField("exec process did not return a status".into()))?;

	let stdout_str = String::from_utf8_lossy(&stdout_buf);
	let stderr_str = String::from_utf8_lossy(&stderr_buf);

	if status.status.as_deref() != Some("Success") {
		let reason = status
			.message
			.as_deref()
			.or(status.reason.as_deref())
			.unwrap_or("unknown");
		return Err(Error::MissingField(format!(
			"exec in pod {pod_name} failed ({reason})\nstdout: {stdout_str}\nstderr: {stderr_str}"
		)));
	}

	Ok(stdout_str.into_owned())
}

/// Build the shell command that copies a single schema from the restore
/// database into the overlay via `pg_dump | psql`.
fn build_copy_command(
	restore_host: &str,
	restore_dbname: &str,
	reader_user: &str,
	reader_password: &str,
	remote_schema: &str,
	local_schema: &str,
) -> String {
	let remote_quoted = quote_ident(remote_schema);
	let local_quoted = quote_ident(local_schema);

	// Shell-escape the password for use in PGPASSWORD env var.
	// We use single quotes and escape any embedded single quotes.
	let escaped_password = reader_password.replace('\'', "'\\''");

	let mut script = format!(
		"set -eo pipefail\n\
		 echo 'Dropping existing local schema {local_quoted}...'\n\
		 psql -U postgres -d app -c 'DROP SCHEMA IF EXISTS {local_quoted} CASCADE;'\n\
		 echo 'Copying schema {remote_quoted} from restore...'\n\
		 PGPASSWORD='{escaped_password}' pg_dump \
		   -h {restore_host} \
		   -p 5432 \
		   -U {reader_user} \
		   -d {restore_dbname} \
		   -n {remote_quoted} \
		   --no-owner \
		   --no-privileges \
		   --no-comments \
		   --no-publications \
		   --no-subscriptions \
		   --no-security-labels \
		   --no-tablespaces \
		 | psql -U postgres -d app -v ON_ERROR_STOP=1 --quiet\n"
	);

	if remote_schema != local_schema {
		script.push_str(&format!(
			"echo 'Renaming schema {remote_quoted} to {local_quoted}...'\n\
			 psql -U postgres -d app -c 'ALTER SCHEMA {remote_quoted} RENAME TO {local_quoted};'\n"
		));
	}

	script.push_str("echo 'Schema copy complete.'\n");
	script
}

/// Reconcile overlay state using the copy strategy.
///
/// Connects to the overlay and restore databases, then exec's
/// `pg_dump | psql` inside the overlay CNPG pod to copy each schema.
pub async fn reconcile_copy(
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

	debug!(
		replica = %replica_name,
		restore = %restore_name,
		"reconciling overlay state via copy strategy"
	);

	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
	let su_secret = secrets.get(&superuser_secret_name).await?;
	let reader_secret = secrets.get(&reader_secret_name).await?;

	let reader_user = super::connect::read_secret_field(&reader_secret, "username")?;
	let reader_password = super::connect::read_secret_field(&reader_secret, "password")?;

	// Connect to overlay to manage state tracking
	let overlay_conn = super::connect::connect_overlay(
		client,
		&cluster_name,
		namespace,
		&su_secret,
		use_port_forward,
	)
	.await?;
	let overlay_pg = &overlay_conn.client;

	overlay_pg
		.batch_execute("CREATE SCHEMA IF NOT EXISTS _pgro")
		.await?;

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
			"copy reconciliation already complete for this config, skipping"
		);
		return Ok(());
	}

	// Check retry count
	let mut current_retries = tracked
		.as_ref()
		.filter(|t| t.config_hash == config_hash)
		.map(|t| t.retries)
		.unwrap_or(0);

	if current_retries >= MAX_COPY_RETRIES {
		let last_attempt = tracked
			.as_ref()
			.map(|t| t.updated_at)
			.unwrap_or(Timestamp::UNIX_EPOCH);
		let last_error = tracked
			.as_ref()
			.and_then(|t| t.last_error.as_deref())
			.unwrap_or("unknown");
		let elapsed = Timestamp::now().duration_since(last_attempt);

		if elapsed < SignedDuration::from_secs(RETRY_COOLDOWN_SECS) {
			let remaining = RETRY_COOLDOWN_SECS - elapsed.as_secs();
			warn!(
				replica = %replica_name,
				restore = %restore_name,
				cooldown_remaining_secs = remaining,
				last_error = last_error,
				"copy strategy exhausted {MAX_COPY_RETRIES} retries, waiting for cooldown"
			);
			return Err(Error::InvalidOverlayConfig(format!(
				"copy strategy exhausted {MAX_COPY_RETRIES} retries for restore {restore_name}, \
				 will reset in {remaining}s (last error: {last_error})"
			)));
		}

		info!(
			replica = %replica_name,
			restore = %restore_name,
			"retry cooldown elapsed, resetting copy retry counter"
		);
		write_state(overlay_pg, &config_hash, "pending", 0, None).await?;
		current_retries = 0;
	}

	// Discover the main database in the restore.
	// Preparatory steps are not counted as retries — only actual copy
	// exec failures consume retry budget.
	let restore_dbname = discover_restore_database(
		client,
		namespace,
		restore_name,
		&reader_user,
		&reader_password,
		use_port_forward,
	)
	.await?;

	// Ensure analytics user can create schemas in the overlay database
	let analytics_user = &replica.spec.analytics_username;
	overlay_pg
		.batch_execute(&format!(
			"GRANT CREATE ON DATABASE app TO {}",
			quote_ident(analytics_user)
		))
		.await?;

	// Resolve expected schemas
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

	// Determine the restore service host
	let restore_host = if use_port_forward {
		let label_selector = format!("pgro.bes.au/restore={restore_name}");
		super::connect::find_pod_ip_by_label(client, namespace, &label_selector).await?
	} else {
		format!("{restore_name}.{namespace}.svc")
	};

	// Determine the overlay pod to exec into
	let overlay_pod = format!("{cluster_name}-1");

	// Now that all preparatory steps succeeded, increment the retry
	// counter. Only actual copy exec failures should consume retries.
	let retries = current_retries + 1;
	write_state(overlay_pg, &config_hash, "importing", retries, None).await?;

	info!(
		replica = %replica_name,
		restore = %restore_name,
		overlay_pod = %overlay_pod,
		schema_count = schemas.len(),
		attempt = retries,
		max_retries = MAX_COPY_RETRIES,
		"copying schemas via pg_dump | psql"
	);

	for (remote, local) in &schemas {
		debug!(
			remote = %remote,
			local = %local,
			"copying schema from restore to overlay"
		);

		let script = build_copy_command(
			&restore_host,
			&restore_dbname,
			&reader_user,
			&reader_password,
			remote,
			local,
		);

		match exec_in_pod(client, namespace, &overlay_pod, &["bash", "-c", &script]).await {
			Ok(output) => {
				if !output.is_empty() {
					debug!(
						remote = %remote,
						local = %local,
						output = %output.trim(),
						"pg_dump | psql output"
					);
				}
				debug!(remote = %remote, local = %local, "schema copied successfully");
			}
			Err(e) => {
				let err_msg = e.to_string();
				warn!(
					replica = %replica_name,
					remote = %remote,
					local = %local,
					error = %err_msg,
					attempt = retries,
					max_retries = MAX_COPY_RETRIES,
					"schema copy failed"
				);
				let _ = write_state(
					overlay_pg,
					&config_hash,
					"importing",
					retries,
					Some(&err_msg),
				)
				.await;
				return Err(e);
			}
		}
	}

	write_state(overlay_pg, &config_hash, "complete", retries, None).await?;

	info!(
		replica = %replica_name,
		restore = %restore_name,
		total_schemas = schemas.len(),
		"copy reconciliation complete"
	);

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn build_copy_command_same_schema() {
		let cmd = build_copy_command(
			"restore-host.ns.svc",
			"myapp",
			"overlay_reader",
			"secret123",
			"public",
			"public",
		);
		assert!(cmd.contains("pg_dump"));
		assert!(cmd.contains("-n \"public\""));
		assert!(cmd.contains("-d myapp"));
		assert!(cmd.contains("-h restore-host.ns.svc"));
		assert!(cmd.contains("PGPASSWORD='secret123'"));
		assert!(cmd.contains("ON_ERROR_STOP=1"));
		assert!(!cmd.contains("RENAME"));
	}

	#[test]
	fn build_copy_command_renamed_schema() {
		let cmd = build_copy_command(
			"restore.ns.svc",
			"myapp",
			"overlay_reader",
			"pass",
			"source",
			"target",
		);
		assert!(cmd.contains("-n \"source\""));
		assert!(cmd.contains("RENAME TO \"target\""));
	}

	#[test]
	fn build_copy_command_escapes_password() {
		let cmd = build_copy_command("host", "db", "user", "it's a test", "public", "public");
		assert!(cmd.contains("PGPASSWORD='it'\\''s a test'"));
	}

	#[test]
	fn build_copy_command_special_schema_name() {
		let cmd = build_copy_command("host", "db", "user", "pass", "my\"schema", "my\"schema");
		assert!(cmd.contains("-n \"my\"\"schema\""));
	}
}
