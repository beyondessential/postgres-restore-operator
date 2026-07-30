//! Applying a target version's schema migrations to a restored replica.
//!
//! Canopy names the version on the worklist entry; this runs it. A restore
//! whose deployment comes up healthy with `spec.migrateTo` set enters
//! `Migrating` before `Switching`: a Job runs the tamanu image at that version
//! against the restored database, then the outcome is read back out of the
//! `logs.migrations` audit table tamanu writes itself, so nothing here parses
//! logs.

use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::api::{
	batch::v1::{Job, JobSpec},
	core::v1::{Container, PodSpec, PodTemplateSpec, Secret},
};
use kube::{
	Api, ResourceExt,
	api::{ObjectMeta, PostParams},
	runtime::controller::Action,
};
use tracing::{info, warn};

use crate::{
	context::Context,
	controllers::jobs::{env_from_secret_name, env_literal},
	error::Result,
	types::{
		MigrationResult, MigrationTarget, MigrationTiming, PostgresPhysicalReplica,
		PostgresPhysicalRestore,
	},
};

/// Where tamanu's server images are published. The tag is `v` + the semver
/// canopy named, which is the same image an upgrading deployment runs.
const TAMANU_IMAGE: &str = "ghcr.io/beyondessential/tamanu-central-server";

const MIGRATION_CONTAINER: &str = "migrate";

pub fn migration_job_name(restore_name: &str) -> String {
	format!("{restore_name}-migrate")
}

/// The image that owns the target version's migrations.
fn migration_image(target: &MigrationTarget) -> String {
	format!("{TAMANU_IMAGE}:v{version}", version = target.version)
}

/// A Job that migrates the restored database to the target version.
///
/// Points tamanu at the per-restore Service rather than the replica's, so a
/// switchover mid-migration cannot repoint it at a different database. Runs as
/// the app user from the replica's credentials Secret: the restore is read-only
/// by config, which the job lifts for its own session only (see
/// `MIGRATION_SCRIPT`).
pub fn build_migration_job(
	restore: &PostgresPhysicalRestore,
	replica: &PostgresPhysicalReplica,
	target: &MigrationTarget,
	namespace: &str,
) -> Job {
	let restore_name = restore.name_any();
	let job_name = migration_job_name(&restore_name);
	let creds = replica.creds_secret_name();

	Job {
		metadata: ObjectMeta {
			name: Some(job_name),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("pgro.bes.au/replica".to_string(), replica.name_any()),
				("pgro.bes.au/restore".to_string(), restore_name.clone()),
			])),
			owner_references: Some(vec![super::restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			// A failing migration is a finding, not a flake: retrying would
			// spend the same hours to reach the same answer, and canopy treats
			// a failed verdict as settled.
			backoff_limit: Some(0),
			template: PodTemplateSpec {
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					containers: vec![Container {
						name: MIGRATION_CONTAINER.to_string(),
						image: Some(migration_image(target)),
						command: Some(vec!["node".into()]),
						args: Some(vec!["dist/index.js".into(), "migrate".into()]),
						env: Some(vec![
							env_literal("DB_HOST", &restore_name),
							env_from_secret_name("DB_USERNAME", &creds, "username"),
							env_from_secret_name("DB_PASSWORD", &creds, "password"),
							env_from_secret_name("DB_NAME", &creds, "username"),
							env_literal("NODE_ENV", "production"),
						]),
						..Default::default()
					}],
					..Default::default()
				}),
				..Default::default()
			},
			..Default::default()
		}),
		..Default::default()
	}
}

/// Drive the migration Job: create it, wait, then record what it did.
// spec: RST#what-a-migration-test-reports
pub async fn reconcile_migrating(
	restore: &PostgresPhysicalRestore,
	ctx: &Context,
	name: &str,
	namespace: &str,
) -> Result<Action> {
	let Some(target) = restore.spec.migrate_to.clone() else {
		// Target withdrawn mid-flight: nothing to prove, carry on as an
		// ordinary verify.
		warn!(
			restore = name,
			"in Migrating with no migration target; returning to Ready"
		);
		super::update_restore_status(
			&ctx.client,
			namespace,
			name,
			serde_json::json!({ "phase": "Ready" }),
		)
		.await?;
		return Ok(Action::requeue(Duration::from_secs(1)));
	};

	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(ctx.client.clone(), namespace);
	let replica = replicas.get(&restore.spec.replica.name).await?;

	let jobs: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
	let job_name = migration_job_name(name);

	let job = match jobs.get_opt(&job_name).await? {
		Some(job) => job,
		None => {
			info!(
				restore = name,
				target = %target.version,
				"starting migration test"
			);
			let job = build_migration_job(restore, &replica, &target, namespace);
			jobs.create(&PostParams::default(), &job).await?
		}
	};

	let status = job.status.as_ref();
	let succeeded = status.and_then(|s| s.succeeded).unwrap_or(0);
	let failed = status.and_then(|s| s.failed).unwrap_or(0);

	if succeeded == 0 && failed == 0 {
		return Ok(Action::requeue(Duration::from_secs(15)));
	}

	// Whatever the job's exit code, the answer lives in the database: tamanu
	// records every migration it applied, with per-migration durations, in
	// `logs.migrations`. A failed job that got partway still has rows there,
	// and its last applied migration names where it stopped.
	let result = read_result(ctx, &replica, name, namespace, failed > 0).await?;

	info!(
		restore = name,
		target = %target.version,
		failed_migration = ?result.failed_migration,
		elapsed = result.total_elapsed_seconds,
		"migration test finished"
	);

	super::update_restore_status(
		&ctx.client,
		namespace,
		name,
		serde_json::json!({
			"phase": "Ready",
			"migrationJob": {
				"name": job_name,
				"phase": if failed > 0 { "Failed" } else { "Succeeded" },
			},
			"migrationResult": result,
		}),
	)
	.await?;

	// Back to Ready, which now sees `migrationResult` and proceeds to
	// Switching, where the verification report carries the outcome.
	Ok(Action::requeue(Duration::from_secs(1)))
}

/// Read what the migrations did off the replica itself.
///
/// `logs.migrations` is tamanu's own audit table: one row per batch, with a
/// `stats` payload holding `durationMsPerMigration` and the batch total. Reading
/// it beats parsing the job's logs, and it is the same record an operator would
/// inspect after a real upgrade.
async fn read_result(
	ctx: &Context,
	replica: &PostgresPhysicalReplica,
	restore_name: &str,
	namespace: &str,
	job_failed: bool,
) -> Result<MigrationResult> {
	let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
	let creds = secrets.get(&replica.creds_secret_name()).await?;
	let user = crate::controllers::postgres::read_secret_field(&creds, "username")?;
	let password = crate::controllers::postgres::read_secret_field(&creds, "password")?;
	let dbname = crate::controllers::postgres::discover_restore_database(
		&ctx.client,
		namespace,
		restore_name,
		&user,
		&password,
		ctx.use_port_forward(),
	)
	.await?;

	let conn = crate::controllers::postgres::connect_to_restore(
		&ctx.client,
		namespace,
		restore_name,
		&dbname,
		&user,
		&password,
		ctx.use_port_forward(),
	)
	.await?;

	let data_bytes_after =
		crate::controllers::postgres::database_size_on(&conn.client).await? as i64;

	// One row per batch: `migrations` is the ordered file list, `stats` holds
	// `durationMsPerMigration` keyed by file and a `preSnapshot.sizeBytes` taken
	// before the batch ran. Newest batch is this run's.
	let row = conn
		.client
		.query_opt(
			"SELECT migrations, batch_duration_ms, stats
			 FROM logs.migrations
			 WHERE direction = 'up'
			 ORDER BY created_at DESC
			 LIMIT 1",
			&[],
		)
		.await?;

	let Some(row) = row else {
		// No batch recorded: the job died before applying anything (bad image,
		// unreachable database). Nothing to attribute, so report the shape
		// without inventing timings.
		warn!(
			restore = restore_name,
			"migration job ended with no batch in logs.migrations"
		);
		return Ok(MigrationResult {
			total_elapsed_seconds: 0,
			failed_migration: job_failed.then(|| "unknown".to_string()),
			data_bytes_before: data_bytes_after,
			data_bytes_after,
			timings: Vec::new(),
		});
	};

	let applied: Vec<String> = row.get(0);
	let batch_duration_ms: Option<i64> = row.get(1);
	let stats: Option<serde_json::Value> = row.get(2);

	let durations = stats
		.as_ref()
		.and_then(|s| s.get("durationMsPerMigration"))
		.and_then(serde_json::Value::as_object)
		.cloned()
		.unwrap_or_default();

	// Ordered by the batch's own list, not the map: a JSON object has no order
	// and the sequence is what tells an operator which migration blocked which.
	let timings: Vec<MigrationTiming> = applied
		.iter()
		.map(|name| MigrationTiming {
			name: name.clone(),
			elapsed_seconds: durations
				.get(name)
				.and_then(serde_json::Value::as_i64)
				.unwrap_or(0)
				/ 1000,
		})
		.collect();

	// `-1` is tamanu's explicit "could not read"; treat it as unknown rather
	// than reporting a negative size or a nonsense growth.
	let data_bytes_before = stats
		.as_ref()
		.and_then(|s| s.pointer("/preSnapshot/sizeBytes"))
		.and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
		.filter(|bytes| *bytes >= 0)
		.unwrap_or(data_bytes_after);

	Ok(MigrationResult {
		// The batch's own wall clock where tamanu recorded it, which includes the
		// pre/post steps around the migrations themselves.
		total_elapsed_seconds: batch_duration_ms
			.map(|ms| ms / 1000)
			.unwrap_or_else(|| timings.iter().map(|t| t.elapsed_seconds).sum()),
		// The last migration in the batch is where a failed run stopped.
		failed_migration: job_failed
			.then(|| timings.last().map(|t| t.name.clone()))
			.flatten(),
		data_bytes_before,
		data_bytes_after,
		timings,
	})
}
