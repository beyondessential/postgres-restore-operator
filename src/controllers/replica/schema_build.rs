//! Building a Tamanu reporting schema against a migrated restore.
//!
//! A reporting schema follows from a Tamanu version's own schema and from the
//! group's configuration together, so it can only be built against a database of
//! that group at that version. A `reporting-schema` restore is where such a
//! database exists, briefly, between migrating and switchover.
//!
//! pgro does not know how a schema is made. It hands the image named by the
//! replica's `builder_image` a database, a version and a group, and takes back
//! whatever SQL the build POSTs to the callback. What comes back is registered
//! with canopy as a group-scoped artifact of that version.

use std::{collections::BTreeMap, sync::Arc};

use bestool_canopy::bytes::Bytes;
use k8s_openapi::{
	api::{
		batch::v1::{Job, JobSpec, JobStatus},
		core::v1::{
			Capabilities, Container, PodSpec, PodTemplateSpec, ResourceRequirements, Secret,
			SecurityContext,
		},
	},
	apimachinery::pkg::api::resource::Quantity,
};
use kube::{
	Client, ResourceExt,
	api::{Api, DeleteParams, ObjectMeta, Patch, PatchParams, PostParams},
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
	context::Context,
	controllers::{
		canopy::labels as canopy_labels,
		jobs::{self, env_from_secret_name, env_literal},
		postgres,
	},
	error::{Error, Result},
	placement::PodPlacement,
	types::{PostgresPhysicalReplica, PostgresPhysicalRestore, SchemaBuildResult},
};

/// Ceiling on a build, after which the Job is killed and the pair records a
/// failure. Without it a dbt run that cannot finish holds the switchover, and
/// with it the whole restore, for as long as its pod lives.
const BUILD_DEADLINE_SECONDS: i64 = 30 * 60;

/// How long a finished build Job is left for an operator to read.
const BUILD_TTL_SECONDS: i32 = 300;

/// Name of the build Job for a replica. One per replica rather than per
/// restore: a replica has at most one restore building at a time, and reusing
/// the name is what makes the create idempotent across reconciles.
fn build_job_name(replica_name: &str) -> String {
	format!("{replica_name}-schema-build")
}

/// Where a build's callback token is recorded, so the operator can tell the
/// running build's POST from anybody else's.
pub const BUILD_TOKEN_ANNOTATION: &str = "pgro.bes.au/schema-build-token";

/// Where a build's delivery of its schema is recorded, carrying the size it
/// delivered. The schema itself is held in memory until a reconcile takes it,
/// so this is what separates a schema an operator restart lost from one that
/// was never sent.
pub const BUILD_RECEIPT_ANNOTATION: &str = "pgro.bes.au/schema-build-posted";

/// Record that the running build delivered its schema.
pub async fn record_receipt(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	bytes: usize,
) -> Result<()> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let annotations = BTreeMap::from([(BUILD_RECEIPT_ANNOTATION.to_string(), bytes.to_string())]);
	jobs.patch(
		&build_job_name(replica_name),
		&PatchParams::default(),
		&Patch::Merge(serde_json::json!({
			"metadata": { "annotations": annotations },
		})),
	)
	.await
	.map_err(Error::Kube)?;
	Ok(())
}

fn receipted(job: &Job) -> bool {
	job.annotations().contains_key(BUILD_RECEIPT_ANNOTATION)
}

/// The token the currently-running build for this replica must present, if
/// there is a build.
///
/// A lookup that failed is an error rather than an absent token: the two answer
/// the caller differently, and reading one as the other refuses a build that
/// holds the right token.
pub async fn build_token(
	client: &Client,
	namespace: &str,
	replica_name: &str,
) -> Result<Option<String>> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	Ok(jobs
		.get_opt(&build_job_name(replica_name))
		.await
		.map_err(Error::Kube)?
		.and_then(|job| job.annotations().get(BUILD_TOKEN_ANNOTATION).cloned()))
}

/// Everything a build needs: the restore it runs against, the version and group
/// it builds for, and where to post the SQL it produces.
struct SchemaBuildArgs<'a> {
	replica: &'a PostgresPhysicalReplica,
	namespace: &'a str,
	restore_name: &'a str,
	dbname: &'a str,
	creds_secret_name: &'a str,
	image: &'a str,
	version: &'a str,
	group: &'a str,
	callback_url: &'a str,
	callback_token: &'a str,
	placement: &'a PodPlacement,
}

/// The Job that runs a reporting-schema build against the migrated restore.
///
/// Everything the build needs arrives as environment: the dbt profiles in each
/// deployment repo already read their connection from `TAMANU_DL_DB_*`, so
/// naming those is what lets a build run against a database it is handed rather
/// than one it went looking for.
fn build_schema_build_job(
	SchemaBuildArgs {
		replica,
		namespace,
		restore_name,
		dbname,
		creds_secret_name,
		image,
		version,
		group,
		callback_url,
		callback_token,
		placement,
	}: SchemaBuildArgs<'_>,
) -> Job {
	let replica_name = replica.name_any();
	let job_name = build_job_name(&replica_name);
	let host = format!("{restore_name}.{namespace}.svc");

	info!(
		replica = %replica_name,
		restore = %restore_name,
		%version,
		"building reporting-schema build Job"
	);

	let mut job = Job {
		metadata: ObjectMeta {
			name: Some(job_name),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("pgro.bes.au/replica".to_string(), replica_name),
				(
					"pgro.bes.au/component".to_string(),
					"schema-build".to_string(),
				),
			])),
			annotations: Some(BTreeMap::from([(
				BUILD_TOKEN_ANNOTATION.to_string(),
				callback_token.to_string(),
			)])),
			owner_references: Some(vec![replica.owner_reference()]),
			..Default::default()
		},
		spec: Some(JobSpec {
			// A build against a fixed version and configuration fails the same
			// way every time, so a retry buys nothing and only delays the
			// report.
			backoff_limit: Some(0),
			active_deadline_seconds: Some(BUILD_DEADLINE_SECONDS),
			ttl_seconds_after_finished: Some(BUILD_TTL_SECONDS),
			template: PodTemplateSpec {
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					// The build talks to the restore's postgres and the
					// callback, and is the one image pgro does not choose, so
					// it is given no way to reach the API it runs beside.
					automount_service_account_token: Some(false),
					containers: vec![Container {
						name: "build".to_string(),
						image: Some(image.to_string()),
						security_context: Some(SecurityContext {
							allow_privilege_escalation: Some(false),
							capabilities: Some(Capabilities {
								drop: Some(vec!["ALL".to_string()]),
								..Default::default()
							}),
							..Default::default()
						}),
						env: Some(vec![
							env_literal("TAMANU_DL_DB_URL", &host),
							// By reference, as every other Job in this repo
							// takes its credentials: a literal value puts the
							// plaintext in the Job and Pod objects, in etcd and
							// in the audit log.
							env_from_secret_name(
								"TAMANU_DL_DB_USER",
								creds_secret_name,
								"username",
							),
							env_from_secret_name(
								"TAMANU_DL_DB_PASSWORD",
								creds_secret_name,
								"password",
							),
							env_literal("TAMANU_DL_DB_DATABASE", dbname),
							env_literal("TAMANU_VERSION", version),
							env_literal("TAMANU_DEPLOYMENT", group),
							env_literal("SCHEMA_CALLBACK_URL", callback_url),
						]),
						// The build shares a node with the Postgres it is
						// querying, so an unbounded pod starves the database
						// it reads and is the first thing evicted under node
						// pressure.
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("100m".to_string())),
								("memory".to_string(), Quantity("256Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("2".to_string())),
								("memory".to_string(), Quantity("2Gi".to_string())),
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
	};

	placement.apply_to_job(&mut job);
	job
}

/// Create the build Job. The caller has already established there is none, so
/// a Job that appeared in between is another reconcile's and left alone.
async fn create_build_job(client: &Client, namespace: &str, job: Job) -> Result<()> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

	match jobs.create(&PostParams::default(), &job).await {
		Ok(_) => Ok(()),
		Err(kube::Error::Api(err)) if err.code == 409 => Ok(()),
		Err(err) => Err(Error::Kube(err)),
	}
}

/// Remove the build Job once its outcome is recorded.
///
/// The name is per-replica, so a Job left behind makes the next restore's
/// [`create_build_job`] a no-op and settles that pair as having produced no
/// schema without ever running.
async fn delete_build_job(client: &Client, namespace: &str, job_name: &str) {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	if let Err(err) = jobs.delete(job_name, &DeleteParams::background()).await {
		warn!(job = %job_name, "deleting the finished build Job failed: {err}");
	}
}

/// Whether the build Job has finished, and how.
enum BuildOutcome {
	/// No Job yet; the caller creates one.
	NotStarted,
	/// Still going; the caller requeues.
	Running,
	/// The Job succeeded. Whether a schema actually came back is the callback's
	/// business, not the exit code's.
	Succeeded,
	/// The Job failed.
	Failed,
}

/// A build Job's state, how long it has taken, and whether it recorded
/// delivering a schema.
struct BuildStatus {
	outcome: BuildOutcome,
	elapsed_seconds: i64,
	receipted: bool,
}

async fn build_outcome(client: &Client, namespace: &str, job_name: &str) -> Result<BuildStatus> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let Some(job) = jobs.get_opt(job_name).await.map_err(Error::Kube)? else {
		return Ok(BuildStatus {
			outcome: BuildOutcome::NotStarted,
			elapsed_seconds: 0,
			receipted: false,
		});
	};

	// A Job on its way out is neither this build nor the next: creating one
	// under the same name while it deletes conflicts, and leaves none.
	let outcome = if job.metadata.deletion_timestamp.is_some() {
		BuildOutcome::Running
	} else {
		match jobs::classify_job(&job) {
			jobs::JobStatus::Succeeded => BuildOutcome::Succeeded,
			jobs::JobStatus::Failed => BuildOutcome::Failed,
			jobs::JobStatus::Active => BuildOutcome::Running,
		}
	};

	Ok(BuildStatus {
		outcome,
		receipted: receipted(&job),
		elapsed_seconds: job_elapsed_seconds(&job.status.unwrap_or_default()),
	})
}

/// Whole seconds the build itself took, read from the Job's own timestamps.
///
/// A reconcile creates the Job and a later one observes it finished, so nothing
/// the operator holds in memory spans the build.
fn job_elapsed_seconds(status: &JobStatus) -> i64 {
	let Some(start) = status.start_time.as_ref() else {
		return 0;
	};
	let end = status
		.completion_time
		.as_ref()
		.map_or_else(jiff::Timestamp::now, |t| t.0);

	end.duration_since(start.0).as_secs().max(0)
}

/// Build the reporting schema against the migrated restore, returning whether
/// the switchover may proceed.
///
/// A build that fails does not hold the switchover: the replica was sound, and
/// what failed is the schema, which canopy grades on its own. The result is
/// recorded either way so the report carries it.
pub(super) async fn reconcile_schema_build(
	client: &Client,
	ctx: &Arc<Context>,
	replica: &PostgresPhysicalReplica,
	namespace: &str,
	restore: &PostgresPhysicalRestore,
) -> Result<bool> {
	let replica_name = replica.name_any();

	let restore_name = restore.name_any();
	let job_name = build_job_name(&replica_name);

	let (image, target, group) = match build_to_do(replica, restore) {
		BuildToDo::Build {
			image,
			target,
			group,
		} => (image, target, group),
		BuildToDo::NoTarget => {
			warn!(replica = %replica_name, "reporting-schema replica has no target version; skipping build");
			return Ok(true);
		}
		BuildToDo::NoGroup => {
			return settle_failed(client, namespace, &restore_name, NO_GROUP).await;
		}
		BuildToDo::Unmigrated => {
			return settle_failed(client, namespace, &restore_name, UNMIGRATED).await;
		}
		BuildToDo::Settled => {
			// A schema posted after its build was recorded has no later taker,
			// and it is tens of megabytes.
			ctx.schema_build_results.take(namespace, &replica_name);
			return Ok(true);
		}
		BuildToDo::NoImage => return Ok(true),
	};

	let build = build_outcome(client, namespace, &job_name).await?;
	match build.outcome {
		BuildOutcome::NotStarted => {
			// The callback publishes SQL to canopy under this group, so it
			// carries a token only this build is given: without one, anything
			// that can reach the operator's port publishes for any group.
			let token = Uuid::new_v4().to_string();

			// A payload held with no Job behind it belongs to no build this
			// reconcile can record, and it is tens of megabytes.
			ctx.schema_build_results.take(namespace, &replica_name);

			let reader_secret_name = replica.creds_secret_name();
			let dbname = match discover_build_database(
				client,
				ctx,
				&reader_secret_name,
				namespace,
				&restore_name,
			)
			.await
			{
				Ok(dbname) => dbname,
				Err(err) => {
					return retry_or_settle(client, namespace, restore, err).await;
				}
			};

			let job = build_schema_build_job(SchemaBuildArgs {
				replica,
				namespace,
				restore_name: &restore_name,
				dbname: &dbname,
				creds_secret_name: &reader_secret_name,
				image,
				version: target,
				group: &group.to_string(),
				callback_url: &ctx.schema_build_callback_url(namespace, &replica_name, &token),
				callback_token: &token,
				placement: &ctx.pod_placement(),
			});
			create_build_job(client, namespace, job).await?;
			Ok(false)
		}
		BuildOutcome::Running => Ok(false),
		BuildOutcome::Succeeded => {
			// Taken, not cloned: the schema runs to tens of megabytes, and a
			// copy per read is paid inside the reconcile loop. A patch that
			// fails puts it back, since dropping it would settle a build that
			// ran as having produced nothing.
			let Some(sql) = ctx
				.schema_build_results
				.take(namespace, &replica_name)
				.map(Bytes::from)
			else {
				let attempts = attempts_so_far(restore) + 1;
				return match empty_build(build.receipted, attempts) {
					EmptyBuild::Rebuild => {
						rebuild(client, namespace, &restore_name, &job_name, attempts).await
					}
					EmptyBuild::Settle => {
						settle_empty(
							client,
							namespace,
							&restore_name,
							&job_name,
							build.elapsed_seconds,
						)
						.await
					}
				};
			};

			// A recorded result is what settles the build for good, so a
			// registration that failed on the way to canopy is answered while
			// the schema is still held rather than recorded against a build
			// that produced one.
			let registration = match ctx.canopy.as_ref() {
				None => Some("no canopy client to register the schema with".to_owned()),
				Some(canopy) => match canopy
					.register_reporting_schema(
						target,
						group,
						crate::controllers::canopy::verification::run_id_from_status(restore),
						sql.clone(),
					)
					.await
				{
					Ok(()) => None,
					Err(err) => {
						let attempts = attempts_so_far(restore) + 1;
						warn!(%target, %group, attempts, "registering the reporting schema failed: {err}");

						if attempts < BUILD_ATTEMPTS {
							keep_schema(ctx, namespace, &replica_name, &sql);
							record_build_attempt(client, namespace, &restore_name, attempts).await?;
							return Ok(false);
						}

						Some(format!("canopy did not take the schema in: {err}"))
					}
				},
			};

			let result =
				completed_build_result(Some(&sql), registration.as_deref(), build.elapsed_seconds);
			if let Err(err) =
				record_schema_build(client, namespace, &restore_name, Some(&job_name), result).await
			{
				keep_schema(ctx, namespace, &replica_name, &sql);
				return Err(err);
			}

			delete_build_job(client, namespace, &job_name).await;
			Ok(true)
		}
		BuildOutcome::Failed => {
			record_schema_build(
				client,
				namespace,
				&restore_name,
				Some(&job_name),
				SchemaBuildResult {
					built: false,
					error: Some("the build job failed".to_string()),
					total_elapsed_seconds: build.elapsed_seconds,
					schema_bytes: None,
				},
			)
			.await?;
			// A build that posted a schema and then exited non-zero has left
			// megabytes in a store nothing else empties.
			ctx.schema_build_results.take(namespace, &replica_name);
			delete_build_job(client, namespace, &job_name).await;
			Ok(true)
		}
	}
}

/// Put a taken schema back for the next reconcile.
fn keep_schema(ctx: &Arc<Context>, namespace: &str, replica_name: &str, sql: &Bytes) {
	ctx.schema_build_results.store(
		namespace,
		replica_name,
		String::from_utf8_lossy(sql).into_owned(),
	);
}

/// How many times one restore may go round before its build records a failure
/// rather than being tried again. Shared by the ways a build is worth trying
/// again, since what is recorded here is final.
const BUILD_ATTEMPTS: i64 = 5;

fn attempts_so_far(restore: &PostgresPhysicalRestore) -> i64 {
	restore
		.status
		.as_ref()
		.and_then(|status| status.schema_build_attempts)
		.unwrap_or(0)
}

async fn record_build_attempt(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	attempts: i64,
) -> Result<()> {
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), namespace);
	restores
		.patch_status(
			restore_name,
			&PatchParams::default(),
			&Patch::Merge(serde_json::json!({
				"status": { "schemaBuildAttempts": attempts },
			})),
		)
		.await?;
	Ok(())
}

/// Answer a setup failure: try again while there are attempts left, and record
/// a failed build once there are not.
///
/// The setup reads a Secret and opens a connection to a restore that has just
/// come out of its migration Job, so a pod still settling answers this way
/// without the build being at fault, and a recorded failure is final.
async fn retry_or_settle(
	client: &Client,
	namespace: &str,
	restore: &PostgresPhysicalRestore,
	err: Error,
) -> Result<bool> {
	let restore_name = restore.name_any();
	let attempts = attempts_so_far(restore) + 1;

	warn!(
		restore = %restore_name,
		attempts,
		"could not set up the reporting-schema build: {err}"
	);

	if attempts >= BUILD_ATTEMPTS {
		return settle_failed(
			client,
			namespace,
			&restore_name,
			&format!("the build could not be set up: {err}"),
		)
		.await;
	}

	record_build_attempt(client, namespace, &restore_name, attempts).await?;
	Ok(false)
}

/// What to do with a Job that ended well and has no schema held against it.
///
/// The schema is held in memory between the callback and the reconcile that
/// takes it, so an operator that restarted in that window holds nothing. The
/// build's receipt is what tells that apart from a build that delivered
/// nothing: only the first is worth building again, and the record written for
/// the second is one no later reconcile revisits.
#[derive(Debug, PartialEq, Eq)]
enum EmptyBuild {
	Rebuild,
	Settle,
}

fn empty_build(receipted: bool, attempts: i64) -> EmptyBuild {
	if receipted && attempts < BUILD_ATTEMPTS {
		EmptyBuild::Rebuild
	} else {
		EmptyBuild::Settle
	}
}

/// Drop the finished Job so the next reconcile builds the schema again.
async fn rebuild(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	job_name: &str,
	attempts: i64,
) -> Result<bool> {
	warn!(
		restore = %restore_name,
		attempts,
		"the reporting-schema build delivered a schema this operator no longer holds; building again"
	);
	record_build_attempt(client, namespace, restore_name, attempts).await?;
	delete_build_job(client, namespace, job_name).await;
	Ok(false)
}

/// Record a build that ended with no schema, and let the switchover proceed.
async fn settle_empty(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	job_name: &str,
	elapsed_seconds: i64,
) -> Result<bool> {
	record_schema_build(
		client,
		namespace,
		restore_name,
		Some(job_name),
		completed_build_result(None, None, elapsed_seconds),
	)
	.await?;
	delete_build_job(client, namespace, job_name).await;
	Ok(true)
}

/// What a build recorded when the replica names no group it could be built or
/// registered for.
const NO_GROUP: &str = "the replica names no group to register the schema for";

/// What a build records when the restore never reached the version the schema
/// would have been an artifact of.
const UNMIGRATED: &str =
	"the restore did not migrate to the target version, so there is no database to build against";

/// The canopy group this replica's data belongs to. Required both to build
/// against the right configuration and to register the result.
///
/// The spec is what the syncer always writes; the label is a second copy of the
/// same fact, and a CR that has lost it would otherwise build nothing.
fn build_group(replica: &PostgresPhysicalReplica) -> Option<Uuid> {
	replica
		.spec
		.canopy_source
		.as_ref()
		.and_then(|source| Uuid::parse_str(&source.group).ok())
		.or_else(|| {
			replica
				.labels()
				.get(canopy_labels::GROUP)
				.and_then(|s| Uuid::parse_str(s).ok())
		})
}

/// Settle the pair as a build that failed, and let the switchover proceed.
///
/// Nothing ran, so the record names no Job: an operator reading one there would
/// go looking for a Job that was never created.
async fn settle_failed(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	error: &str,
) -> Result<bool> {
	record_schema_build(
		client,
		namespace,
		restore_name,
		None,
		SchemaBuildResult {
			built: false,
			error: Some(error.to_owned()),
			total_elapsed_seconds: 0,
			schema_bytes: None,
		},
	)
	.await?;
	Ok(true)
}

/// The database inside the restore a build runs against.
async fn discover_build_database(
	client: &Client,
	ctx: &Arc<Context>,
	creds_secret_name: &str,
	namespace: &str,
	restore_name: &str,
) -> Result<String> {
	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
	let secret = secrets.get(creds_secret_name).await?;
	let user = postgres::read_secret_field(&secret, "username")?;
	let password = postgres::read_secret_field(&secret, "password")?;

	postgres::discover_restore_database(
		client,
		namespace,
		restore_name,
		&user,
		&password,
		ctx.use_port_forward(),
	)
	.await
}

/// Whether this reconcile has a build to do.
///
/// A settled restore keeps the result it recorded, whichever way it went. A
/// restore with no target version has nothing to build against, and a replica
/// with no builder image has nothing to build with.
#[derive(Debug, PartialEq, Eq)]
enum BuildToDo<'a> {
	Settled,
	NoTarget,
	NoImage,
	NoGroup,
	Unmigrated,
	Build {
		image: &'a str,
		target: &'a str,
		group: Uuid,
	},
}

fn build_to_do<'a>(
	replica: &PostgresPhysicalReplica,
	restore: &'a PostgresPhysicalRestore,
) -> BuildToDo<'a> {
	if restore
		.status
		.as_ref()
		.and_then(|s| s.schema_build_result.as_ref())
		.is_some()
	{
		return BuildToDo::Settled;
	}

	let Some(target) = restore.spec.migrate_to.as_ref() else {
		return BuildToDo::NoTarget;
	};

	let Some(image) = restore.spec.builder_image.as_deref() else {
		return BuildToDo::NoImage;
	};

	if !migrated_to_target(restore) {
		return BuildToDo::Unmigrated;
	}

	let Some(group) = build_group(replica) else {
		return BuildToDo::NoGroup;
	};

	BuildToDo::Build {
		image,
		target: &target.version,
		group,
	}
}

/// Whether the restore's database actually reached the version it targets.
///
/// A failed migration leaves the restore healthy and moves it on to switchover
/// all the same, so the target version alone says only what the restore aimed
/// at. A schema built here is published as an artifact of that version, and one
/// built against a database that never got there describes the wrong schema.
fn migrated_to_target(restore: &PostgresPhysicalRestore) -> bool {
	restore
		.status
		.as_ref()
		.and_then(|status| status.migration_result.as_ref())
		.is_some_and(|result| result.failed_migration.is_none())
}

/// What a build Job that exited zero records.
///
/// Whether a schema came out of it turns on the callback, not on the exit code:
/// the exit code says the container ran, and a Job that ran to completion
/// without posting a schema is a failed build rather than a successful empty
/// one. A schema canopy did not take in is not a built pair either: canopy has
/// no artifact to offer for it, so the size is still worth recording but the
/// pair is not settled as built.
fn completed_build_result(
	sql: Option<&[u8]>,
	registration: Option<&str>,
	elapsed_seconds: i64,
) -> SchemaBuildResult {
	SchemaBuildResult {
		built: sql.is_some() && registration.is_none(),
		error: registration.map(str::to_owned).or_else(|| {
			sql.is_none()
				.then(|| "the build produced no schema".to_string())
		}),
		total_elapsed_seconds: elapsed_seconds,
		schema_bytes: sql.map(|s| s.len() as i64),
	}
}

/// Record a build's outcome on the restore, so the report carries it.
async fn record_schema_build(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	job_name: Option<&str>,
	result: SchemaBuildResult,
) -> Result<()> {
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), namespace);
	restores
		.patch_status(
			restore_name,
			&PatchParams::default(),
			&Patch::Merge(serde_json::json!({
				"status": {
					"schemaBuildJob": job_name,
					"schemaBuildResult": result,
				}
			})),
		)
		.await?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::types::{MigrationTarget, PostgresPhysicalRestoreStatus};
	use serde_json::json;

	/// A replica named `kamaka`, enough of one to build a Job from.
	fn replica() -> PostgresPhysicalReplica {
		serde_json::from_value(json!({
			"apiVersion": "pgro.bes.au/v1alpha1",
			"kind": "PostgresPhysicalReplica",
			"metadata": { "name": "kamaka", "namespace": "pgro", "uid": "0000-1111" },
			"spec": { "schedule": "0 3 * * *" },
		}))
		.expect("a replica")
	}

	/// A restore of that replica, enough of one to decide a build from.
	fn restore() -> PostgresPhysicalRestore {
		serde_json::from_value(json!({
			"apiVersion": "pgro.bes.au/v1alpha1",
			"kind": "PostgresPhysicalRestore",
			"metadata": { "name": "kamaka-restore", "namespace": "pgro" },
			"spec": {
				"replica": { "name": "kamaka" },
				"snapshot": "snapA",
				"snapshotSize": "1Gi",
				"storageSize": "2Gi",
			},
		}))
		.expect("a restore")
	}

	fn job() -> Job {
		build_schema_build_job(SchemaBuildArgs {
			replica: &replica(),
			namespace: "pgro",
			restore_name: "kamaka-restore",
			dbname: "tamanu",
			creds_secret_name: "kamaka-creds",
			image: "ghcr.io/beyondessential/tamanu-dbt:2.60.0",
			version: "2.60.0",
			group: "kamaka",
			callback_url: "https://canopy.example/public/schema-callback/tok",
			callback_token: "tok",
			placement: &PodPlacement::default(),
		})
	}

	fn env(job: &Job) -> BTreeMap<String, String> {
		job.spec
			.as_ref()
			.unwrap()
			.template
			.spec
			.as_ref()
			.unwrap()
			.containers[0]
			.env
			.as_ref()
			.unwrap()
			.iter()
			.map(|e| (e.name.clone(), e.value.clone().unwrap_or_default()))
			.collect()
	}

	/// The build container's environment is the contract with the builder
	/// image, which lives outside this repo. A renamed or dropped variable is
	/// a build that fails inside someone else's container, so the names are
	/// pinned here rather than left to the image to discover.
	#[test]
	fn the_build_container_carries_the_agreed_environment() {
		let env = env(&job());

		assert_eq!(
			env.keys().cloned().collect::<Vec<_>>(),
			vec![
				"SCHEMA_CALLBACK_URL",
				"TAMANU_DEPLOYMENT",
				"TAMANU_DL_DB_DATABASE",
				"TAMANU_DL_DB_PASSWORD",
				"TAMANU_DL_DB_URL",
				"TAMANU_DL_DB_USER",
				"TAMANU_VERSION",
			],
			"the builder image reads exactly these"
		);

		assert_eq!(env["TAMANU_VERSION"], "2.60.0");
		assert_eq!(env["TAMANU_DEPLOYMENT"], "kamaka");
		assert_eq!(env["TAMANU_DL_DB_DATABASE"], "tamanu");
		assert_eq!(
			env["SCHEMA_CALLBACK_URL"],
			"https://canopy.example/public/schema-callback/tok"
		);
	}

	/// The credentials go in by reference. A literal value puts the plaintext
	/// in the Job and Pod objects, in etcd and in the audit log, where every
	/// other Job in this repo keeps it out.
	#[test]
	fn the_build_takes_its_credentials_by_reference() {
		let job = job();
		let env = job
			.spec
			.as_ref()
			.unwrap()
			.template
			.spec
			.as_ref()
			.unwrap()
			.containers[0]
			.env
			.clone()
			.unwrap();

		for name in ["TAMANU_DL_DB_USER", "TAMANU_DL_DB_PASSWORD"] {
			let var = env.iter().find(|e| e.name == name).expect(name);
			assert!(var.value.is_none(), "{name} carries no literal");
			assert_eq!(
				var.value_from
					.as_ref()
					.and_then(|f| f.secret_key_ref.as_ref())
					.map(|r| r.name.as_str()),
				Some("kamaka-creds")
			);
		}
	}

	/// The callback publishes SQL to canopy under the replica's group, so the
	/// Job carries the token its POST has to present.
	#[test]
	fn the_build_records_the_token_its_callback_must_present() {
		assert_eq!(
			job()
				.annotations()
				.get(BUILD_TOKEN_ANNOTATION)
				.map(String::as_str),
			Some("tok")
		);
	}

	/// The database it is handed is the restore's own service, in the
	/// namespace the restore runs in. A build reaching anywhere else would be
	/// building against the wrong group's data.
	#[test]
	fn the_database_is_the_restores_own_service() {
		assert_eq!(env(&job())["TAMANU_DL_DB_URL"], "kamaka-restore.pgro.svc");
	}

	/// A build against a fixed version and configuration fails the same way
	/// every time, so a retry buys nothing and only delays the report.
	#[test]
	fn a_failed_build_is_not_retried() {
		let job = job();
		let spec = job.spec.as_ref().unwrap();

		assert_eq!(spec.backoff_limit, Some(0));
		assert_eq!(
			spec.template
				.spec
				.as_ref()
				.unwrap()
				.restart_policy
				.as_deref(),
			Some("Never")
		);
	}

	/// A build that cannot finish holds the switchover, and with it the whole
	/// restore, for as long as its pod lives, so the Job carries its own
	/// ceiling. Without a TTL the finished Job also survives under a name the
	/// next restore's build reuses, which makes that build a no-op.
	#[test]
	fn a_build_cannot_run_forever_or_outlive_its_restore() {
		let job = job();
		let spec = job.spec.as_ref().unwrap();

		assert_eq!(spec.active_deadline_seconds, Some(BUILD_DEADLINE_SECONDS));
		assert_eq!(spec.ttl_seconds_after_finished, Some(BUILD_TTL_SECONDS));
	}

	/// The build shares a node with the Postgres it queries, so an unbounded
	/// pod starves the database it reads and is the first thing the kubelet
	/// evicts under node pressure.
	#[test]
	fn the_build_container_is_bounded() {
		let job = job();
		let resources = job
			.spec
			.as_ref()
			.unwrap()
			.template
			.spec
			.as_ref()
			.unwrap()
			.containers[0]
			.resources
			.as_ref()
			.expect("resources");

		assert!(
			resources
				.requests
				.as_ref()
				.is_some_and(|r| r.contains_key("memory"))
		);
		assert!(
			resources
				.limits
				.as_ref()
				.is_some_and(|l| l.contains_key("memory"))
		);
	}

	/// The builder image is named by a canopy worklist parameter rather than by
	/// pgro, and it runs in the operator's own namespace holding the restore's
	/// database credentials. It needs no Kubernetes API access at all, so it is
	/// given none, and no way to raise what it does hold.
	#[test]
	fn the_build_is_given_no_reach_beyond_its_database() {
		let job = job();
		let spec = job.spec.as_ref().unwrap().template.spec.as_ref().unwrap();

		assert_eq!(spec.automount_service_account_token, Some(false));

		let security = spec.containers[0]
			.security_context
			.as_ref()
			.expect("a security context");
		assert_eq!(security.allow_privilege_escalation, Some(false));
		assert_eq!(
			security
				.capabilities
				.as_ref()
				.and_then(|c| c.drop.as_deref()),
			Some(["ALL".to_string()].as_slice())
		);
	}

	/// The elapsed time comes from the Job's own timestamps. One reconcile
	/// creates the Job and a later one sees it finished, so anything the
	/// operator times itself measures the reconcile and reports near zero for
	/// every build.
	#[test]
	fn the_build_is_timed_by_the_job() {
		fn at(s: &str) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::Time {
			k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(s.parse().expect("a timestamp"))
		}

		let finished = JobStatus {
			start_time: Some(at("2026-09-08T01:00:00Z")),
			completion_time: Some(at("2026-09-08T01:07:30Z")),
			..Default::default()
		};
		assert_eq!(job_elapsed_seconds(&finished), 450);

		let running = JobStatus {
			start_time: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
				jiff::Timestamp::now() - jiff::SignedDuration::from_secs(90),
			)),
			..Default::default()
		};
		assert!(
			job_elapsed_seconds(&running) >= 90,
			"a Job still going is timed against now"
		);

		assert_eq!(
			job_elapsed_seconds(&JobStatus::default()),
			0,
			"a Job that has not started has taken no time"
		);
	}

	/// The Job hangs off the replica, so tearing the replica down takes the
	/// build with it rather than leaving a Job against a restore that is gone.
	#[test]
	fn the_job_belongs_to_its_replica() {
		let job = job();
		let meta = &job.metadata;

		assert_eq!(meta.namespace.as_deref(), Some("pgro"));
		assert_eq!(
			meta.owner_references.as_ref().map(Vec::len),
			Some(1),
			"one owner, the replica"
		);

		let labels = meta.labels.as_ref().expect("labels");
		assert_eq!(labels["pgro.bes.au/replica"], "kamaka");
		assert_eq!(labels["pgro.bes.au/component"], "schema-build");
	}

	/// A build whose callback delivered a schema is a build.
	#[test]
	fn a_schema_that_came_back_is_a_built_pair() {
		let result =
			completed_build_result(Some(b"CREATE VIEW reporting.x AS SELECT 1;"), None, 90);

		assert!(result.built);
		assert_eq!(result.error, None);
		assert_eq!(result.schema_bytes, Some(36));
		assert_eq!(result.total_elapsed_seconds, 90);
	}

	/// A build that recorded delivering its schema, with nothing held for it,
	/// was lost between the callback and this reconcile, which only an operator
	/// restart does. The schema took up to half an hour to make, so it is built
	/// again rather than recorded as having produced nothing.
	#[test]
	fn a_delivered_schema_this_operator_lost_is_built_again() {
		assert_eq!(empty_build(true, 1), EmptyBuild::Rebuild);
	}

	/// A Job that ended without ever delivering a schema produced none, and no
	/// rebuild recovers what was never made. Waiting on it would hold the
	/// switchover for nothing.
	#[test]
	fn a_build_that_delivered_nothing_settles_at_once() {
		assert_eq!(empty_build(false, 1), EmptyBuild::Settle);
	}

	/// An operator restarting inside the window on every pass would otherwise
	/// rebuild for as long as it kept doing it.
	#[test]
	fn a_schema_lost_often_enough_stops_being_rebuilt() {
		assert_eq!(empty_build(true, BUILD_ATTEMPTS), EmptyBuild::Settle);
	}

	/// The receipt is read off the same annotation the callback writes, and a
	/// Job without one is a build that delivered nothing.
	#[test]
	fn a_job_carries_its_builds_receipt() {
		assert!(!receipted(&job()));

		let mut delivered = job();
		delivered
			.metadata
			.annotations
			.get_or_insert_with(BTreeMap::new)
			.insert(BUILD_RECEIPT_ANNOTATION.to_string(), "4096".to_string());
		assert!(receipted(&delivered));
	}

	/// The record a Job that exits zero without delivering a schema ends with.
	/// The exit code says the container ran; only the callback says a schema
	/// came out of it, so reading the exit code as the verdict settles the pair
	/// as built and offers servers nothing.
	#[test]
	fn a_job_that_posted_no_schema_is_a_failed_build() {
		let result = completed_build_result(None, None, 12);

		assert!(!result.built);
		assert_eq!(
			result.error.as_deref(),
			Some("the build produced no schema")
		);
		assert_eq!(result.schema_bytes, None);
	}

	/// An empty schema is still a schema the builder chose to post, so it is not
	/// silently reclassified as a failure.
	#[test]
	fn an_empty_schema_is_reported_as_it_was_posted() {
		let result = completed_build_result(Some(b""), None, 1);

		assert!(result.built);
		assert_eq!(result.schema_bytes, Some(0));
	}

	/// A schema with no canopy to register it with is published nowhere, so it is
	/// not a built pair either. Reading that as success settles the pair against
	/// an artifact no server can fetch.
	#[test]
	fn a_schema_with_no_canopy_to_take_it_is_not_built() {
		let result = completed_build_result(
			Some(b"CREATE VIEW reporting.x AS SELECT 1;"),
			Some("no canopy client to register the schema with"),
			5,
		);

		assert!(!result.built);
		assert_eq!(result.schema_bytes, Some(36));
	}

	/// A build needs a version to build against and an image to build with, and it
	/// runs once: the three things that decide whether a reconcile does anything at
	/// all. Getting any of them wrong either builds nothing forever or rebuilds a
	/// settled pair on every pass.
	#[test]
	fn what_a_reconcile_has_to_build() {
		let group = "9c3a1b2e-0000-0000-0000-000000000001";
		let mut replica = replica();
		let mut restore = restore();

		assert_eq!(
			build_to_do(&replica, &restore),
			BuildToDo::NoTarget,
			"no version to build against"
		);

		restore.spec.migrate_to = Some(MigrationTarget {
			version: "2.60.0".into(),
			version_id: "00000000-0000-0000-0000-000000000000".into(),
		});
		assert_eq!(
			build_to_do(&replica, &restore),
			BuildToDo::NoImage,
			"no image to build with"
		);

		// The restore's own snapshot of the image, not the replica's live field:
		// an edit to the replica must not change what this restore builds with.
		restore.spec.builder_image = Some("builder:1".into());
		assert_eq!(
			build_to_do(&replica, &restore),
			BuildToDo::Unmigrated,
			"nothing has been migrated to the version yet"
		);

		restore.status = Some(PostgresPhysicalRestoreStatus {
			migration_result: Some(crate::types::MigrationResult {
				total_elapsed_seconds: 60,
				failed_migration: Some("1710000000-addThing.js".into()),
				data_bytes_before: 1,
				data_bytes_after: 1,
				timings: Vec::new(),
			}),
			..Default::default()
		});
		assert_eq!(
			build_to_do(&replica, &restore),
			BuildToDo::Unmigrated,
			"a migration that failed leaves no database at the target version"
		);

		restore.status = Some(PostgresPhysicalRestoreStatus {
			migration_result: Some(crate::types::MigrationResult {
				total_elapsed_seconds: 60,
				failed_migration: None,
				data_bytes_before: 1,
				data_bytes_after: 2,
				timings: Vec::new(),
			}),
			..Default::default()
		});
		assert_eq!(
			build_to_do(&replica, &restore),
			BuildToDo::NoGroup,
			"no group to build or register for"
		);

		// The spec is what the syncer writes; a build reads it rather than the
		// label, which a CR can have lost.
		replica.spec.canopy_source = Some(crate::types::CanopySource {
			group: group.into(),
			r#type: "tamanu-postgres".into(),
		});
		assert_eq!(
			build_to_do(&replica, &restore),
			BuildToDo::Build {
				image: "builder:1",
				target: "2.60.0",
				group: group.parse().unwrap(),
			}
		);

		let migrated = restore.status.take();
		restore.status = Some(PostgresPhysicalRestoreStatus {
			schema_build_result: Some(completed_build_result(None, None, 1)),
			..migrated.expect("a migrated restore")
		});
		assert_eq!(
			build_to_do(&replica, &restore),
			BuildToDo::Settled,
			"a failed build settles the pair as surely as a successful one"
		);
	}

	/// A schema canopy did not take in is not a built pair: canopy has no artifact
	/// to offer for it, so recording `built` would settle the pair against a schema
	/// nothing can fetch. The size still goes on the record, since the build did
	/// produce one and its absence would read as a build that emitted nothing.
	#[test]
	fn a_schema_canopy_did_not_take_is_not_built() {
		let result = completed_build_result(
			Some(b"CREATE VIEW reporting.x AS SELECT 1;"),
			Some("canopy did not take the schema in"),
			90,
		);

		assert!(!result.built);
		assert_eq!(
			result.error.as_deref(),
			Some("canopy did not take the schema in")
		);
		assert_eq!(result.schema_bytes, Some(36));
	}
}
