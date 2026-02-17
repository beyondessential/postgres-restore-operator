use std::collections::BTreeMap;

use k8s_openapi::api::{
	batch::v1::{Job, JobSpec},
	core::v1::{
		Container, PodSecurityContext, PodSpec, PodTemplateSpec, ResourceRequirements, Secret,
	},
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::{
	Api, Client, ResourceExt,
	api::{ObjectMeta, PostParams},
};
use tracing::{debug, info, warn};

use crate::{
	context::Context,
	controllers::jobs::{JobStatus, classify_job, env_from_secret_name, env_literal},
	error::{Error, Result},
	types::PostgresPhysicalReplica,
};

use super::common::{
	compute_config_hash, discover_restore_database, ensure_state_table, migrate_from_fdw_state,
	quote_ident, read_state, resolve_schemas, write_state,
};

const MAX_COPY_RETRIES: i32 = 3;

const COPY_JOB_CONTAINER: &str = "copy";

fn copy_job_name(replica_name: &str) -> String {
	format!("{replica_name}-overlay-copy")
}

/// Build the shell script that copies schemas from the restore database
/// into the overlay via `pg_dump | psql`.
///
/// Credentials and hosts are injected as environment variables:
///   READER_USER, READER_PASSWORD, RESTORE_HOST, RESTORE_DBNAME,
///   OVERLAY_USER, OVERLAY_PASSWORD, OVERLAY_HOST, COPY_CALLBACK_URL
fn build_copy_script(schemas: &[(String, String)]) -> String {
	let mut script = String::from(
		r#"#!/bin/bash
set -o pipefail

report_result() {
  local body="$1"
  if [ -n "$COPY_CALLBACK_URL" ]; then
    curl -sf -X POST --max-time 10 \
      -H 'Content-Type: text/plain' \
      --data-binary "$body" \
      "$COPY_CALLBACK_URL" 2>/dev/null || true
  fi
}

sync_extensions() {
  echo 'Syncing extensions from restore to overlay...'
  EXTENSIONS=$(PGPASSWORD="$READER_PASSWORD" psql \
    -h "$RESTORE_HOST" -p 5432 -U "$READER_USER" -d "$RESTORE_DBNAME" \
    -t -A -c "SELECT extname FROM pg_extension WHERE extname NOT IN ('plpgsql')")
  for ext in $EXTENSIONS; do
    echo "  Creating extension $ext..."
    PGPASSWORD="$OVERLAY_PASSWORD" psql \
      -h "$OVERLAY_HOST" -p 5432 -U "$OVERLAY_USER" -d app \
      -c "CREATE EXTENSION IF NOT EXISTS \"$ext\";"
  done
  echo 'Extension sync complete.'
}

copy_schemas() {
  set -e
  sync_extensions
"#,
	);

	for (remote, local) in schemas {
		let remote_quoted = quote_ident(remote);
		let local_quoted = quote_ident(local);

		script.push_str(&format!(
			"\n  echo 'Dropping existing local schema {local_quoted}...'\n\
			   PGPASSWORD=\"$OVERLAY_PASSWORD\" psql \
			     -h \"$OVERLAY_HOST\" -p 5432 -U \"$OVERLAY_USER\" -d app \
			     -c 'DROP SCHEMA IF EXISTS {local_quoted} CASCADE;'\n\
			   echo 'Copying schema {remote_quoted} from restore...'\n\
			   PGPASSWORD=\"$READER_PASSWORD\" pg_dump \
			     -h \"$RESTORE_HOST\" \
			     -p 5432 \
			     -U \"$READER_USER\" \
			     -d \"$RESTORE_DBNAME\" \
			     -n {remote_quoted} \
			     --no-owner \
			     --no-privileges \
			     --no-comments \
			     --no-publications \
			     --no-subscriptions \
			     --no-security-labels \
			     --no-tablespaces \
			   | PGPASSWORD=\"$OVERLAY_PASSWORD\" psql \
			     -h \"$OVERLAY_HOST\" -p 5432 -U \"$OVERLAY_USER\" -d app \
			     -v ON_ERROR_STOP=1 --quiet\n"
		));

		if remote != local {
			script.push_str(&format!(
				"  echo 'Renaming schema {remote_quoted} to {local_quoted}...'\n\
				   PGPASSWORD=\"$OVERLAY_PASSWORD\" psql \
				     -h \"$OVERLAY_HOST\" -p 5432 -U \"$OVERLAY_USER\" -d app \
				     -c 'ALTER SCHEMA {remote_quoted} RENAME TO {local_quoted};'\n"
			));
		}
	}

	script.push_str(
		r#"
  echo 'All schema copies complete.'
}

OUTPUT=$(copy_schemas 2>&1)
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  report_result 'success'
else
  echo "$OUTPUT" >&2
  report_result "$(printf '%s' "$OUTPUT" | tail -c 8192)"
fi

exit $EXIT_CODE
"#,
	);
	script
}

/// Build the copy Job spec.
#[expect(
	clippy::too_many_arguments,
	reason = "internal builder with tightly-coupled params"
)]
fn build_copy_job(
	replica: &PostgresPhysicalReplica,
	namespace: &str,
	schemas: &[(String, String)],
	reader_secret_name: &str,
	superuser_secret_name: &str,
	restore_host: &str,
	restore_dbname: &str,
	overlay_host: &str,
	callback_url: &str,
	pg_version: i32,
) -> Job {
	let replica_name = replica.name_any();
	let job_name = copy_job_name(&replica_name);
	let image = format!("ghcr.io/cloudnative-pg/postgresql:{pg_version}");
	let script = build_copy_script(schemas);

	Job {
		metadata: ObjectMeta {
			name: Some(job_name),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("pgro.bes.au/replica".to_string(), replica_name.clone()),
				(
					"pgro.bes.au/component".to_string(),
					"overlay-copy".to_string(),
				),
			])),
			owner_references: Some(vec![replica.owner_reference()]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(0),
			active_deadline_seconds: Some(7200), // 2 hours
			ttl_seconds_after_finished: Some(300),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(BTreeMap::from([
						("pgro.bes.au/replica".to_string(), replica_name),
						(
							"pgro.bes.au/component".to_string(),
							"overlay-copy".to_string(),
						),
					])),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					security_context: Some(PodSecurityContext {
						run_as_non_root: Some(true),
						run_as_user: Some(26), // postgres UID in CNPG image
						run_as_group: Some(26),
						..Default::default()
					}),
					containers: vec![Container {
						name: COPY_JOB_CONTAINER.to_string(),
						image: Some(image),
						command: Some(vec!["bash".to_string(), "-c".to_string()]),
						args: Some(vec![script]),
						env: Some(vec![
							env_from_secret_name("READER_USER", reader_secret_name, "username"),
							env_from_secret_name("READER_PASSWORD", reader_secret_name, "password"),
							env_from_secret_name("OVERLAY_USER", superuser_secret_name, "username"),
							env_from_secret_name(
								"OVERLAY_PASSWORD",
								superuser_secret_name,
								"password",
							),
							env_literal("RESTORE_HOST", restore_host),
							env_literal("RESTORE_DBNAME", restore_dbname),
							env_literal("OVERLAY_HOST", overlay_host),
							env_literal("COPY_CALLBACK_URL", callback_url),
						]),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("100m".to_string())),
								("memory".to_string(), Quantity("128Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("1".to_string())),
								("memory".to_string(), Quantity("512Mi".to_string())),
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
	}
}

/// Reconcile overlay state using the copy strategy.
///
/// Creates a Kubernetes Job that runs `pg_dump | psql` to copy schemas
/// from the restore database into the overlay. Returns `true` when the
/// copy is complete, `false` when it is still in progress.
pub async fn reconcile_copy(
	client: &Client,
	ctx: &Context,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	restore_name: &str,
	use_port_forward: bool,
) -> Result<bool> {
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
		return Ok(true);
	}

	let mut current_retries = tracked
		.as_ref()
		.filter(|t| t.config_hash == config_hash)
		.map(|t| t.retries)
		.unwrap_or(0);

	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let job_name = copy_job_name(&replica_name);

	// Check for an existing copy Job
	if let Some(job) = jobs.get_opt(&job_name).await? {
		// If the config changed, delete the stale Job
		let job_config_matches = tracked
			.as_ref()
			.is_some_and(|t| t.config_hash == config_hash);
		if !job_config_matches {
			info!(
				replica = %replica_name,
				job = %job_name,
				"config changed, deleting stale copy Job"
			);
			if let Err(e) = jobs.delete(&job_name, &Default::default()).await {
				warn!(job = %job_name, error = %e, "failed to delete stale copy Job");
			}
			write_state(overlay_pg, &config_hash, "pending", 0, None).await?;
			current_retries = 0;
			// Fall through to create a new Job
		} else {
			match classify_job(&job) {
				JobStatus::Active => {
					debug!(
						replica = %replica_name,
						job = %job_name,
						"copy Job is still running"
					);
					return Ok(false);
				}
				JobStatus::Succeeded => {
					info!(
						replica = %replica_name,
						restore = %restore_name,
						"copy Job succeeded"
					);
					write_state(overlay_pg, &config_hash, "complete", current_retries, None)
						.await?;
					if let Err(e) = jobs.delete(&job_name, &Default::default()).await {
						warn!(job = %job_name, error = %e, "failed to delete completed copy Job");
					}

					// Ensure analytics user can create schemas
					let analytics_user = &replica.spec.analytics_username;
					overlay_pg
						.batch_execute(&format!(
							"GRANT CREATE ON DATABASE app TO {}",
							quote_ident(analytics_user)
						))
						.await?;

					return Ok(true);
				}
				JobStatus::Failed => {
					let last_error = ctx
						.copy_results
						.take(namespace, &replica_name)
						.unwrap_or_else(|| "no callback received".to_string());

					current_retries += 1;
					warn!(
						replica = %replica_name,
						restore = %restore_name,
						attempt = current_retries,
						max_retries = MAX_COPY_RETRIES,
						last_error = %last_error,
						"copy Job failed"
					);
					if let Err(e) = jobs.delete(&job_name, &Default::default()).await {
						warn!(job = %job_name, error = %e, "failed to delete failed copy Job");
					}

					if current_retries >= MAX_COPY_RETRIES {
						write_state(
							overlay_pg,
							&config_hash,
							"failed",
							current_retries,
							Some(&last_error),
						)
						.await?;
						return Err(Error::InvalidOverlayConfig(format!(
							"copy strategy exhausted {MAX_COPY_RETRIES} retries for restore \
							 {restore_name} (last error: {last_error})"
						)));
					}

					write_state(
						overlay_pg,
						&config_hash,
						"importing",
						current_retries,
						Some(&last_error),
					)
					.await?;
					// Otherwise fall through to create a new Job
				}
			}
		}
	}

	// Check retry budget — once exhausted, stay failed until config changes
	if current_retries >= MAX_COPY_RETRIES {
		let last_error = tracked
			.as_ref()
			.and_then(|t| t.last_error.as_deref())
			.unwrap_or("unknown");
		return Err(Error::InvalidOverlayConfig(format!(
			"copy strategy exhausted {MAX_COPY_RETRIES} retries for restore {restore_name} \
			 (last error: {last_error})"
		)));
	}

	// Discover the main database in the restore
	let restore_dbname = discover_restore_database(
		client,
		namespace,
		restore_name,
		&reader_user,
		&reader_password,
		use_port_forward,
	)
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

	// Determine hosts: the Job runs in-cluster and always uses service DNS
	let restore_host = format!("{restore_name}.{namespace}.svc");
	let overlay_host = format!("{cluster_name}-rw.{namespace}.svc");

	// Resolve the PG version for the Job image
	let pg_version = replica
		.status
		.as_ref()
		.and_then(|s| s.overlay_postgres_version)
		.unwrap_or(17) as i32;

	let callback_url = ctx.copy_callback_url(namespace, &replica_name);

	let job = build_copy_job(
		replica,
		namespace,
		&schemas,
		&reader_secret_name,
		&superuser_secret_name,
		&restore_host,
		&restore_dbname,
		&overlay_host,
		&callback_url,
		pg_version,
	);

	info!(
		replica = %replica_name,
		restore = %restore_name,
		job = %copy_job_name(&replica_name),
		schema_count = schemas.len(),
		attempt = current_retries + 1,
		max_retries = MAX_COPY_RETRIES,
		"creating copy Job"
	);

	jobs.create(&PostParams::default(), &job).await?;
	write_state(overlay_pg, &config_hash, "importing", current_retries, None).await?;

	Ok(false)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn build_copy_script_same_schema() {
		let script = build_copy_script(&[("public".into(), "public".into())]);
		assert!(script.contains("pg_dump"));
		assert!(script.contains("-n \"public\""));
		assert!(script.contains("PGPASSWORD=\"$READER_PASSWORD\""));
		assert!(script.contains("PGPASSWORD=\"$OVERLAY_PASSWORD\""));
		assert!(script.contains("ON_ERROR_STOP=1"));
		assert!(script.contains("COPY_CALLBACK_URL"));
		assert!(script.contains("report_result"));
		assert!(!script.contains("RENAME"));
		assert!(script.contains("sync_extensions"));
		assert!(script.contains("CREATE EXTENSION IF NOT EXISTS"));
	}

	#[test]
	fn build_copy_script_renamed_schema() {
		let script = build_copy_script(&[("source".into(), "target".into())]);
		assert!(script.contains("-n \"source\""));
		assert!(script.contains("RENAME TO \"target\""));
	}

	#[test]
	fn build_copy_script_multiple_schemas() {
		let script = build_copy_script(&[
			("public".into(), "public".into()),
			("data".into(), "imported".into()),
		]);
		assert!(script.contains("-n \"public\""));
		assert!(script.contains("-n \"data\""));
		assert!(script.contains("RENAME TO \"imported\""));
		assert!(!script.contains("RENAME TO \"public\""));
	}

	#[test]
	fn build_copy_script_special_schema_name() {
		let script = build_copy_script(&[("my\"schema".into(), "my\"schema".into())]);
		assert!(script.contains("-n \"my\"\"schema\""));
	}

	#[test]
	fn copy_job_name_format() {
		assert_eq!(copy_job_name("my-replica"), "my-replica-overlay-copy");
	}

	#[test]
	fn build_copy_script_reports_success() {
		let script = build_copy_script(&[("public".into(), "public".into())]);
		assert!(script.contains("report_result 'success'"));
	}

	#[test]
	fn build_copy_script_reports_error() {
		let script = build_copy_script(&[("public".into(), "public".into())]);
		assert!(script.contains("report_result \"$(printf '%s' \"$OUTPUT\" | tail -c 8192)\""));
	}

	#[test]
	fn build_copy_job_structure() {
		use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta as K8sObjectMeta;

		let replica = PostgresPhysicalReplica {
			metadata: K8sObjectMeta {
				name: Some("test-replica".into()),
				namespace: Some("default".into()),
				uid: Some("test-uid".into()),
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
				overlay_database: None,
			},
			status: None,
		};

		let job = build_copy_job(
			&replica,
			"test-ns",
			&[("public".into(), "public".into())],
			"reader-secret",
			"superuser-secret",
			"restore.test-ns.svc",
			"mydb",
			"overlay-rw.test-ns.svc",
			"http://operator.svc:8080/api/v1/copy-results/test-ns/test-replica",
			17,
		);

		let meta = &job.metadata;
		assert_eq!(meta.name.as_deref(), Some("test-replica-overlay-copy"));
		assert_eq!(meta.namespace.as_deref(), Some("test-ns"));

		let labels = meta.labels.as_ref().unwrap();
		assert_eq!(labels.get("pgro.bes.au/replica").unwrap(), "test-replica");
		assert_eq!(labels.get("pgro.bes.au/component").unwrap(), "overlay-copy");

		let spec = job.spec.as_ref().unwrap();
		assert_eq!(spec.backoff_limit, Some(0));

		let pod_spec = spec.template.spec.as_ref().unwrap();
		let container = &pod_spec.containers[0];
		assert_eq!(container.name, "copy");
		assert_eq!(
			container.image.as_deref(),
			Some("ghcr.io/cloudnative-pg/postgresql:17")
		);

		let env = container.env.as_ref().unwrap();
		let env_names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
		assert!(env_names.contains(&"READER_USER"));
		assert!(env_names.contains(&"READER_PASSWORD"));
		assert!(env_names.contains(&"OVERLAY_USER"));
		assert!(env_names.contains(&"OVERLAY_PASSWORD"));
		assert!(env_names.contains(&"RESTORE_HOST"));
		assert!(env_names.contains(&"RESTORE_DBNAME"));
		assert!(env_names.contains(&"OVERLAY_HOST"));
		assert!(env_names.contains(&"COPY_CALLBACK_URL"));

		let restore_host_env = env.iter().find(|e| e.name == "RESTORE_HOST").unwrap();
		assert_eq!(
			restore_host_env.value.as_deref(),
			Some("restore.test-ns.svc")
		);

		let dbname_env = env.iter().find(|e| e.name == "RESTORE_DBNAME").unwrap();
		assert_eq!(dbname_env.value.as_deref(), Some("mydb"));

		let reader_user_env = env.iter().find(|e| e.name == "READER_USER").unwrap();
		let secret_ref = reader_user_env
			.value_from
			.as_ref()
			.unwrap()
			.secret_key_ref
			.as_ref()
			.unwrap();
		assert_eq!(secret_ref.name, "reader-secret");
		assert_eq!(secret_ref.key, "username");

		let owner_refs = meta.owner_references.as_ref().unwrap();
		assert_eq!(owner_refs.len(), 1);
		assert_eq!(owner_refs[0].kind, "PostgresPhysicalReplica");
		assert_eq!(owner_refs[0].name, "test-replica");
	}
}
