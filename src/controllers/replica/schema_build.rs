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
	batch::v1::{Job, JobSpec, JobStatus},
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

/// Everything a build needs: the restore it runs against, the version and group
/// it builds for, and where to post the SQL it produces.
pub struct SchemaBuildArgs<'a> {
	pub replica: &'a PostgresPhysicalReplica,
	pub namespace: &'a str,
	pub restore_name: &'a str,
	pub dbname: &'a str,
	pub user: &'a str,
	pub password: &'a str,
	pub image: &'a str,
	pub version: &'a str,
	pub group: &'a str,
	pub callback_url: &'a str,
	pub placement: &'a PodPlacement,
}

/// The Job that runs a reporting-schema build against the migrated restore.
///
/// Everything the build needs arrives as environment: the dbt profiles in each
/// deployment repo already read their connection from `TAMANU_DL_DB_*`, so
/// naming those is what lets a build run against a database it is handed rather
/// than one it went looking for.
pub fn build_schema_build_job(args: SchemaBuildArgs<'_>) -> Job {
	let SchemaBuildArgs {
		replica,
		namespace,
		restore_name,
		dbname,
		user,
		password,
		image,
		version,
		group,
		callback_url,
		placement,
	} = args;

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

/// A build Job's state and how long it has taken.
pub struct BuildStatus {
	pub outcome: BuildOutcome,
	pub elapsed_seconds: i64,
}

pub async fn build_outcome(
	client: &Client,
	namespace: &str,
	job_name: &str,
) -> Result<BuildStatus> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let Some(job) = jobs.get_opt(job_name).await.map_err(Error::Kube)? else {
		return Ok(BuildStatus {
			outcome: BuildOutcome::Running,
			elapsed_seconds: 0,
		});
	};

	let status = job.status.unwrap_or_default();
	let outcome = if status.succeeded.unwrap_or(0) > 0 {
		BuildOutcome::Succeeded
	} else if status.failed.unwrap_or(0) > 0 {
		BuildOutcome::Failed
	} else {
		BuildOutcome::Running
	};

	Ok(BuildStatus {
		outcome,
		elapsed_seconds: job_elapsed_seconds(&status),
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

/// Register a built schema with canopy, as an artifact of the version it was
/// built for, scoped to the group whose data it was built from.
///
/// A registration that fails is logged rather than failing the restore: the
/// replica is sound and the build ran, and discarding a good replica over a
/// transport problem helps nobody. It is still not a built pair, since canopy
/// has no artifact to offer, so the caller records it as one that failed.
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

#[cfg(test)]
mod tests {
	use super::*;
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

	fn job() -> Job {
		build_schema_build_job(SchemaBuildArgs {
			replica: &replica(),
			namespace: "pgro",
			restore_name: "kamaka-restore",
			dbname: "tamanu",
			user: "reporter",
			password: "hunter2",
			image: "ghcr.io/beyondessential/tamanu-dbt:2.60.0",
			version: "2.60.0",
			group: "kamaka",
			callback_url: "https://canopy.example/public/schema-callback",
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
		assert_eq!(env["TAMANU_DL_DB_USER"], "reporter");
		assert_eq!(env["TAMANU_DL_DB_PASSWORD"], "hunter2");
		assert_eq!(
			env["SCHEMA_CALLBACK_URL"],
			"https://canopy.example/public/schema-callback"
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
}
