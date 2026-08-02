//! Applying a target version's schema migrations to a restored replica.
//!
//! Canopy names the version on the worklist entry; this runs it. A restore
//! whose deployment comes up healthy with `spec.migrateTo` set enters
//! `Migrating` before `Switching`: a Job runs the tamanu image at that version
//! against the restored database, then the outcome is read back out of the
//! `logs.migrations` audit table tamanu writes itself, so nothing here parses
//! logs.

use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::{
	api::{
		batch::v1::{Job, JobSpec},
		core::v1::{Container, PodSpec, PodTemplateSpec, ResourceRequirements, Secret},
	},
	apimachinery::pkg::api::resource::Quantity,
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
const TAMANU_IMAGE: &str = "ghcr.io/beyondessential/tamanu-central";

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
/// the credentials Secret's user, which holds superuser because a restore with a
/// migration target is built read-write (see `apply_restore_deployment`).
pub fn build_migration_job(
	restore: &PostgresPhysicalRestore,
	replica: &PostgresPhysicalReplica,
	target: &MigrationTarget,
	dbname: &str,
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
						// The image's entrypoint takes the subcommand as its
						// argument, the same way a deploy's migrator Job invokes
						// it; overriding `command` would bypass it.
						args: Some(vec!["migrate".into()]),
						// tamanu maps its `db` config from `CONFIG_SYNC_DB_*`,
						// which take precedence over anything under
						// `NODE_CONFIG_DIR`, so the job needs no mounted config.
						// Versions that understand `DATABASE_URL` prefer it over
						// those, and the job runs whichever version canopy named,
						// so both are set.
						env: Some(vec![
							env_literal("CONFIG_SYNC_DB_HOST", &restore_name),
							env_from_secret_name("CONFIG_SYNC_DB_USERNAME", &creds, "username"),
							env_from_secret_name("CONFIG_SYNC_DB_PASSWORD", &creds, "password"),
							// The restored database, not the credentials user:
							// pgro's replicas do not follow the CNPG convention
							// of naming the database after its owner.
							env_literal("CONFIG_SYNC_DB_NAME", dbname),
							// Must follow the vars it interpolates: kubelet expands
							// `$(VAR)` only from entries defined above it. The
							// generated password is ASCII alphanumeric, so it needs
							// no percent-encoding.
							env_literal(
								"DATABASE_URL",
								&format!(
									"postgresql://$(CONFIG_SYNC_DB_USERNAME):$(CONFIG_SYNC_DB_PASSWORD)@{restore_name}:5432/{dbname}"
								),
							),
							env_literal("NODE_ENV", "production"),
						]),
						// Generous, because an OOMKill here is indistinguishable
						// from a migration that failed on its own and would file a
						// known issue against a version that is actually fine. The
						// job is short-lived and one per restore.
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("100m".to_string())),
								("memory".to_string(), Quantity("256Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("2".to_string())),
								("memory".to_string(), Quantity("4Gi".to_string())),
							])),
							..Default::default()
						}),
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
	let creds = credentials(ctx, &replica, namespace).await?;
	let dbname = crate::controllers::postgres::discover_restore_database(
		&ctx.client,
		namespace,
		name,
		&creds.0,
		&creds.1,
		ctx.use_port_forward(),
	)
	.await?;

	let jobs: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
	let job_name = migration_job_name(name);

	let job = match jobs.get_opt(&job_name).await? {
		Some(job) => job,
		None => {
			// Recorded before the Job exists so the result reader only
			// attributes batches the Job wrote, and written to status before
			// the Job is created so a crash between the two re-captures on the
			// next pass, while the database is still untouched.
			if let Some(at) = latest_batch_at(ctx, name, namespace, &creds, &dbname).await? {
				super::update_restore_status(
					&ctx.client,
					namespace,
					name,
					serde_json::json!({ "migrationBaseline": at }),
				)
				.await?;
			}
			info!(
				restore = name,
				target = %target.version,
				database = %dbname,
				"starting migration test"
			);
			let job = build_migration_job(restore, &replica, &target, &dbname, namespace);
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
	let baseline = restore
		.status
		.as_ref()
		.and_then(|status| status.migration_baseline.as_deref());
	let result = read_result(ctx, name, namespace, &creds, &dbname, baseline, failed > 0).await?;

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

/// The replica's app credentials, as (user, password).
async fn credentials(
	ctx: &Context,
	replica: &PostgresPhysicalReplica,
	namespace: &str,
) -> Result<(String, String)> {
	let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
	let secret = secrets.get(&replica.creds_secret_name()).await?;
	Ok((
		crate::controllers::postgres::read_secret_field(&secret, "username")?,
		crate::controllers::postgres::read_secret_field(&secret, "password")?,
	))
}

/// The newest batch `logs.migrations` already holds, as the text postgres
/// renders it.
///
/// `logged_at` defaults to tamanu's `adjusted_timestamp()`, which carries the
/// source deployment's timesync offset, so the value is never parsed here: it
/// goes back to postgres unchanged and both sides of the comparison come from
/// the replica's own clock. `None` when the table is absent (not a tamanu
/// database, or a version predating it) or empty.
async fn latest_batch_at(
	ctx: &Context,
	restore_name: &str,
	namespace: &str,
	creds: &(String, String),
	dbname: &str,
) -> Result<Option<String>> {
	let conn = crate::controllers::postgres::connect_to_restore(
		&ctx.client,
		namespace,
		restore_name,
		dbname,
		&creds.0,
		&creds.1,
		ctx.use_port_forward(),
	)
	.await?;

	let table_exists: bool = conn
		.client
		.query_one("SELECT to_regclass('logs.migrations') IS NOT NULL", &[])
		.await?
		.get(0);
	if !table_exists {
		return Ok(None);
	}

	Ok(conn
		.client
		.query_one("SELECT max(logged_at)::text FROM logs.migrations", &[])
		.await?
		.get(0))
}

/// Read what the migrations did off the replica itself.
///
/// `logs.migrations` is tamanu's own audit table: one row per batch, with a
/// `stats` payload holding `durationMsPerMigration` and the batch total. Reading
/// it beats parsing the job's logs, and it is the same record an operator would
/// inspect after a real upgrade.
async fn read_result(
	ctx: &Context,
	restore_name: &str,
	namespace: &str,
	creds: &(String, String),
	dbname: &str,
	baseline: Option<&str>,
	job_failed: bool,
) -> Result<MigrationResult> {
	let conn = crate::controllers::postgres::connect_to_restore(
		&ctx.client,
		namespace,
		restore_name,
		dbname,
		&creds.0,
		&creds.1,
		ctx.use_port_forward(),
	)
	.await?;

	let data_bytes_after =
		crate::controllers::postgres::database_size_on(&conn.client).await? as i64;

	// One row per batch. `logged_at` is the only timestamp on the table; there is
	// no `created_at`. Batches at or before the baseline are the source
	// deployment's own upgrade history, restored along with the data.
	let row = conn
		.client
		.query_opt(
			"SELECT migrations, batch_duration_ms, stats
			 FROM logs.migrations
			 WHERE direction = 'up'
			   AND ($1::text IS NULL OR logged_at > $1::text::timestamptz)
			 ORDER BY logged_at DESC
			 LIMIT 1",
			&[&baseline],
		)
		.await?;

	let Some(row) = row else {
		// No batch the job wrote: it died before applying anything (bad image,
		// unreachable database). Nothing to attribute, so report the shape
		// without inventing timings.
		warn!(
			restore = restore_name,
			"migration job ended with no new batch in logs.migrations"
		);
		return Ok(MigrationResult {
			total_elapsed_seconds: 0,
			failed_migration: job_failed.then(|| "unknown".to_string()),
			data_bytes_before: data_bytes_after,
			data_bytes_after,
			timings: Vec::new(),
		});
	};

	Ok(result_from_batch(
		row.get(0),
		row.get(1),
		row.get(2),
		data_bytes_after,
		job_failed,
	))
}

/// Shape a `logs.migrations` batch row into the result canopy is sent.
///
/// `applied` is the batch's ordered file list, and `stats` is tamanu's payload:
/// `durationMsPerMigration` keyed by those same file names, a `preSnapshot`
/// taken before the batch ran, and `failedMigration` when the batch stopped
/// partway.
pub(super) fn result_from_batch(
	applied: Vec<String>,
	batch_duration_ms: Option<i64>,
	stats: Option<serde_json::Value>,
	data_bytes_after: i64,
	job_failed: bool,
) -> MigrationResult {
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
		.and_then(|s| s.pointer("/preSnapshot/databaseSizeBytes"))
		.and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
		.filter(|bytes| *bytes >= 0)
		.unwrap_or(data_bytes_after);

	MigrationResult {
		// The batch's own wall clock where tamanu recorded it, which includes the
		// pre/post steps around the migrations themselves.
		total_elapsed_seconds: batch_duration_ms
			.map(|ms| ms / 1000)
			.unwrap_or_else(|| timings.iter().map(|t| t.elapsed_seconds).sum()),
		// Tamanu names the migration it stopped at. Versions that record a batch
		// only once all of it applied leave nothing to name, so a failed job with
		// no such entry reports that it failed without attributing it.
		failed_migration: job_failed.then(|| {
			stats
				.as_ref()
				.and_then(|s| s.get("failedMigration"))
				.and_then(serde_json::Value::as_str)
				.map(str::to_owned)
				.unwrap_or_else(|| "unknown".to_string())
		}),
		data_bytes_before,
		data_bytes_after,
		timings,
	}
}
