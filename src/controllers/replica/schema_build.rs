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

use std::collections::BTreeMap;

use bestool_canopy::bytes::Bytes;
use k8s_openapi::api::{
	batch::v1::{Job, JobSpec},
	core::v1::{Container, PodSpec, PodTemplateSpec},
};
use kube::{
	Client, ResourceExt,
	api::{Api, ObjectMeta, PostParams},
};
use tracing::{info, warn};

use crate::{
	controllers::jobs::env_literal,
	error::{Error, Result},
	placement::PodPlacement,
	types::PostgresPhysicalReplica,
};

/// Name of the build Job for a replica. One per replica rather than per
/// restore: a replica has at most one restore building at a time, and reusing
/// the name is what makes the create idempotent across reconciles.
pub fn build_job_name(replica_name: &str) -> String {
	format!("{replica_name}-schema-build")
}

/// The Job that runs a reporting-schema build against the migrated restore.
///
/// Everything the build needs arrives as environment: the dbt profiles in each
/// deployment repo already read their connection from `TAMANU_DL_DB_*`, so
/// naming those is what lets a build run against a database it is handed rather
/// than one it went looking for.
#[expect(
	clippy::too_many_arguments,
	reason = "internal builder with tightly-coupled params"
)]
pub fn build_schema_build_job(
	replica: &PostgresPhysicalReplica,
	namespace: &str,
	restore_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
	image: &str,
	version: &str,
	group: &str,
	callback_url: &str,
	placement: &PodPlacement,
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
			owner_references: Some(vec![replica.owner_reference()]),
			..Default::default()
		},
		spec: Some(JobSpec {
			// A build against a fixed version and configuration fails the same
			// way every time, so a retry buys nothing and only delays the
			// report.
			backoff_limit: Some(0),
			template: PodTemplateSpec {
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					containers: vec![Container {
						name: "build".to_string(),
						image: Some(image.to_string()),
						env: Some(vec![
							env_literal("TAMANU_DL_DB_URL", &host),
							env_literal("TAMANU_DL_DB_USER", user),
							env_literal("TAMANU_DL_DB_PASSWORD", password),
							env_literal("TAMANU_DL_DB_DATABASE", dbname),
							env_literal("TAMANU_VERSION", version),
							env_literal("TAMANU_DEPLOYMENT", group),
							env_literal("SCHEMA_CALLBACK_URL", callback_url),
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
	};

	placement.apply_to_job(&mut job);
	job
}

/// Create the build Job if it is not already there.
pub async fn ensure_build_job(client: &Client, namespace: &str, job: Job) -> Result<()> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let name = job.name_any();

	if jobs.get_opt(&name).await.map_err(Error::Kube)?.is_some() {
		return Ok(());
	}

	jobs.create(&PostParams::default(), &job)
		.await
		.map_err(Error::Kube)?;
	Ok(())
}

/// Whether the build Job has finished, and how.
pub enum BuildOutcome {
	/// Still going; the caller requeues.
	Running,
	/// The Job succeeded. Whether a schema actually came back is the callback's
	/// business, not the exit code's.
	Succeeded,
	/// The Job failed.
	Failed,
}

pub async fn build_outcome(
	client: &Client,
	namespace: &str,
	job_name: &str,
) -> Result<BuildOutcome> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let Some(job) = jobs.get_opt(job_name).await.map_err(Error::Kube)? else {
		return Ok(BuildOutcome::Running);
	};

	let status = job.status.unwrap_or_default();
	if status.succeeded.unwrap_or(0) > 0 {
		Ok(BuildOutcome::Succeeded)
	} else if status.failed.unwrap_or(0) > 0 {
		Ok(BuildOutcome::Failed)
	} else {
		Ok(BuildOutcome::Running)
	}
}

/// Register a built schema with canopy, as an artifact of the version it was
/// built for, scoped to the group whose data it was built from.
///
/// A registration that fails is logged and swallowed: the replica is sound and
/// the build ran, and canopy notices the pair is still unbuilt on its next pass.
/// Failing the restore over it would discard a good replica for a transport
/// problem.
pub async fn register(
	canopy: &crate::canopy::Client,
	version: &str,
	group: uuid::Uuid,
	run_id: Option<uuid::Uuid>,
	sql: Bytes,
) -> bool {
	match canopy
		.register_reporting_schema(version, group, run_id, sql)
		.await
	{
		Ok(()) => true,
		Err(err) => {
			warn!(%version, %group, "registering reporting schema failed: {err}");
			false
		}
	}
}
