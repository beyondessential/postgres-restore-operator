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
use k8s_openapi::{
	api::{
		batch::v1::{Job, JobSpec, JobStatus},
		core::v1::{Container, PodSpec, PodTemplateSpec, ResourceRequirements},
	},
	apimachinery::pkg::api::resource::Quantity,
};
use kube::{
	Client, ResourceExt,
	api::{Api, DeleteParams, ObjectMeta, PostParams},
};
use tracing::{info, warn};

use crate::{
	controllers::jobs::{self, env_from_secret_name, env_literal},
	error::{Error, Result},
	placement::PodPlacement,
	types::PostgresPhysicalReplica,
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
pub fn build_job_name(replica_name: &str) -> String {
	format!("{replica_name}-schema-build")
}

/// Where a build's callback token is recorded, so the operator can tell the
/// running build's POST from anybody else's.
pub const BUILD_TOKEN_ANNOTATION: &str = "pgro.bes.au/schema-build-token";

/// The token the currently-running build for this replica must present, if
/// there is a build.
pub async fn build_token(client: &Client, namespace: &str, replica_name: &str) -> Option<String> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	jobs.get_opt(&build_job_name(replica_name))
		.await
		.ok()
		.flatten()?
		.annotations()
		.get(BUILD_TOKEN_ANNOTATION)
		.cloned()
}

/// Everything a build needs: the restore it runs against, the version and group
/// it builds for, and where to post the SQL it produces.
pub struct SchemaBuildArgs<'a> {
	pub replica: &'a PostgresPhysicalReplica,
	pub namespace: &'a str,
	pub restore_name: &'a str,
	pub dbname: &'a str,
	pub creds_secret_name: &'a str,
	pub image: &'a str,
	pub version: &'a str,
	pub group: &'a str,
	pub callback_url: &'a str,
	pub callback_token: &'a str,
	pub placement: &'a PodPlacement,
}

/// The Job that runs a reporting-schema build against the migrated restore.
///
/// Everything the build needs arrives as environment: the dbt profiles in each
/// deployment repo already read their connection from `TAMANU_DL_DB_*`, so
/// naming those is what lets a build run against a database it is handed rather
/// than one it went looking for.
pub fn build_schema_build_job(
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
					containers: vec![Container {
						name: "build".to_string(),
						image: Some(image.to_string()),
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
pub async fn create_build_job(client: &Client, namespace: &str, job: Job) -> Result<()> {
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
/// [`ensure_build_job`] a no-op and settles that pair as having produced no
/// schema without ever running.
pub async fn delete_build_job(client: &Client, namespace: &str, job_name: &str) {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	if let Err(err) = jobs.delete(job_name, &DeleteParams::background()).await {
		warn!(job = %job_name, "deleting the finished build Job failed: {err}");
	}
}

/// Whether the build Job has finished, and how.
pub enum BuildOutcome {
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
			outcome: BuildOutcome::NotStarted,
			elapsed_seconds: 0,
		});
	};

	let outcome = match jobs::classify_job(&job) {
		jobs::JobStatus::Succeeded => BuildOutcome::Succeeded,
		jobs::JobStatus::Failed => BuildOutcome::Failed,
		jobs::JobStatus::Active => BuildOutcome::Running,
	};

	Ok(BuildStatus {
		outcome,
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
) -> std::result::Result<(), String> {
	canopy
		.register_reporting_schema(version, group, run_id, sql)
		.await
		.map_err(|err| {
			warn!(%version, %group, "registering reporting schema failed: {err}");
			err.to_string()
		})
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
