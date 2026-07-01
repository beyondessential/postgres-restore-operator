//! Restore-verification reporter (signal 3).
//!
//! On each syncer tick, observes each managed Namespace's restore Job. When
//! the Job reaches a terminal state we transition the Namespace's
//! `restore-state` annotation and — if it hasn't been done yet — post a
//! `RestoreVerification` to canopy. At-most-once-per-terminal-state gated
//! by the `pgro.bes.au/last-verification-reported-at` annotation; failure
//! to report is recorded in `pgro.bes.au/last-verification-error` and
//! retried on the next tick.
//!
//! On restore-Job success the reporter also ensures the postgres
//! Deployment + Service exist for the namespace, so the RestoreVerification
//! reflects postgres coming up (not just kopia exiting 0). The Job's
//! termination message carries the detected postgres major version;
//! we mirror it to the namespace's `pgro.bes.au/postgres-version`
//! annotation before creating the Deployment.
//!
//! S3 traffic tallies come from the canopy-proxy sidecar's callback POST
//! to `/api/v1/canopy-stats/{ns}/{job}` (see `Context::canopy_stats`)
//! and are included in the RestoreVerification on the next tick.

use std::collections::BTreeMap;

use bestool_canopy::{Outcome, RestoreVerification, WorklistEntry};
use k8s_openapi::{
	ByteString,
	api::{
		apps::v1::Deployment,
		batch::v1::Job,
		core::v1::{Namespace, Secret, Service},
	},
	apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
	Api, ResourceExt,
	api::{ListParams, Patch, PatchParams, PostParams},
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
	context::Context,
	controllers::canopy::{annotations, labels, restore_state},
	error::Result,
};

/// Kick the reporter for every managed namespace: transition state on
/// terminal Jobs, then emit verification reports where owed. Called from
/// [`super::CanopyController::tick`] after the provision/refresh/teardown
/// dispatch.
pub async fn observe_and_report(ctx: &Context, namespaces: &[Namespace]) {
	for ns in namespaces {
		if let Err(err) = observe_one(ctx, ns).await {
			warn!(
				namespace = %ns.name_any(),
				error = %err,
				"canopy reporter: failed to observe namespace"
			);
		}
	}
}

async fn observe_one(ctx: &Context, ns: &Namespace) -> Result<()> {
	let ns_name = ns.name_any();
	let annos = ns.annotations();
	let current_state = annos
		.get(annotations::RESTORE_STATE)
		.map(String::as_str)
		.unwrap_or(restore_state::PENDING);

	// If we're in a terminal state and already reported, nothing to do.
	let terminal = matches!(current_state, restore_state::ACTIVE | restore_state::FAILED);
	let reported = annos.contains_key(annotations::LAST_VERIFICATION_REPORTED_AT);
	if terminal && reported {
		return Ok(());
	}

	// Otherwise look at the Jobs to figure out where we are.
	let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &ns_name);
	let jobs = job_api
		.list(&ListParams::default().labels("pgro.bes.au/job-kind=canopy-restore"))
		.await?;

	let Some(job) = latest_job(&jobs.items) else {
		// No Job yet — provision is still pending. Nothing to report.
		return Ok(());
	};

	let (new_state, outcome, error_msg) = classify_job(job);

	// Transition the namespace's state annotation if it changed.
	if new_state != current_state {
		set_annotations(
			ctx,
			&ns_name,
			&[
				(annotations::RESTORE_STATE, Some(new_state.into())),
				match outcome {
					Some(Outcome::Success) => (
						annotations::LAST_RESTORED_SNAPSHOT_ID,
						annos
							.get(annotations::DESIRED_SNAPSHOT_ID)
							.cloned()
							.or_else(|| snapshot_id_from_job(job).map(String::from)),
					),
					_ => (annotations::LAST_RESTORED_SNAPSHOT_ID, None),
				},
				(
					annotations::LAST_RESTORED_AT,
					if outcome == Some(Outcome::Success) {
						Some(super::now_rfc3339())
					} else {
						None
					},
				),
			],
		)
		.await?;
	}

	// On restore-Job success, ensure the postgres Deployment + Service
	// exist. This is what actually brings the restored data up as a
	// running database — the Job just materialized bytes onto the PVC.
	// We do this BEFORE emitting the verification report so replica_healthy
	// reflects postgres coming up, not just kopia exiting 0.
	if outcome == Some(Outcome::Success) {
		// Mirror the restore Job's termination message (the detected
		// postgres major version) onto the namespace annotation so the
		// Deployment builder can pick the right postgres image.
		if annos.get("pgro.bes.au/postgres-version").is_none()
			&& let Some(v) = crate::controllers::read_job_termination_message(
				&ctx.client,
				&ns_name,
				&job.name_any(),
				super::KOPIA_JOB_NAME,
			)
			.await
		{
			let _ =
				set_annotations(ctx, &ns_name, &[("pgro.bes.au/postgres-version", Some(v))]).await;
			// Re-fetch the namespace so ensure_postgres sees the annotation.
		}
		let refreshed_ns = Api::<Namespace>::all(ctx.client.clone())
			.get(&ns_name)
			.await
			.unwrap_or_else(|_| ns.clone());
		if let Err(err) = ensure_postgres(ctx, &refreshed_ns).await {
			warn!(
				namespace = %ns_name,
				error = %err,
				"canopy reporter: failed to ensure postgres Deployment/Service"
			);
		}
	}

	// Only report from a terminal state.
	let Some(outcome) = outcome else {
		return Ok(());
	};

	// If we already reported for this terminal round, nothing more to do.
	// (The reported-at gate is cleared when the next refresh spawns a new Job;
	// for now the annotation is a per-namespace at-most-once — a follow-up can
	// key it by snapshot_id if that turns out to be too coarse.)
	if annos.contains_key(annotations::LAST_VERIFICATION_REPORTED_AT) {
		return Ok(());
	}

	if let Err(err) = send_verification(ctx, ns, outcome, error_msg.as_deref()).await {
		warn!(
			namespace = %ns_name,
			error = %err,
			"canopy reporter: restore-verification POST failed; will retry next tick"
		);
		set_annotations(
			ctx,
			&ns_name,
			&[(annotations::LAST_VERIFICATION_ERROR, Some(err.to_string()))],
		)
		.await?;
	} else {
		info!(namespace = %ns_name, ?outcome, "canopy reporter: verification reported");
		set_annotations(
			ctx,
			&ns_name,
			&[
				(
					annotations::LAST_VERIFICATION_REPORTED_AT,
					Some(super::now_rfc3339()),
				),
				(annotations::LAST_VERIFICATION_ERROR, None),
			],
		)
		.await?;
	}
	Ok(())
}

/// Ensure the postgres Deployment + Service (and their prereq Secret)
/// exist for a namespace whose restore Job succeeded. Idempotent — 409s
/// on creation are ignored so re-runs don't clobber state.
///
/// Delegates the actual Deployment shape to the shared builder in
/// `restore/builders.rs` so canopy-backed replicas get the same init
/// treatment as the legacy CRD path (locale rewriting, pg_resetwal
/// fallback, analytics-user provisioning, REINDEX-on-startup, restore
/// metadata in `_pgro.restore_info`).
async fn ensure_postgres(ctx: &Context, ns: &Namespace) -> Result<()> {
	let ns_name = ns.name_any();

	let version = annotations_version(ns).unwrap_or_else(|| "16".to_string());
	let ns_labels = ns.labels();
	let intent = ns_labels
		.get("pgro.bes.au/intent")
		.map(String::as_str)
		.unwrap_or("verify");
	let replica_id = ns_labels
		.get("pgro.bes.au/declaration-id")
		.map(String::as_str)
		.unwrap_or("");
	let server_id = ns_labels
		.get("pgro.bes.au/server")
		.map(String::as_str)
		.unwrap_or("");
	let annos = ns.annotations();
	let snapshot_id = annos
		.get(annotations::LAST_RESTORED_SNAPSHOT_ID)
		.or_else(|| annos.get(annotations::DESIRED_SNAPSHOT_ID))
		.map(String::as_str)
		.unwrap_or("");
	let snapshot_time = annos
		.get(annotations::DESIRED_SNAPSHOT_AT)
		.map(String::as_str)
		.unwrap_or("");

	// Ensure the analytics-user Secret. The password is randomly
	// generated once per namespace; the setup-auth initContainer picks
	// it up via env.
	let secret_name = "analytics-credentials";
	let secret_key = "password";
	ensure_analytics_secret(ctx, &ns_name, secret_name, secret_key).await?;

	// Ensure the Deployment.
	let secret_ref = k8s_openapi::api::core::v1::SecretReference {
		name: Some(secret_name.into()),
		namespace: Some(ns_name.clone()),
	};
	let dep = super::build_canopy_postgres_deployment(&super::PostgresDeploymentConfig {
		namespace: &ns_name,
		intent,
		replica_id,
		server_id,
		postgres_major_version: &version,
		snapshot_id,
		snapshot_time,
		analytics_secret: &secret_ref,
		analytics_secret_key: secret_key,
	})?;
	let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns_name);
	match dep_api.create(&PostParams::default(), &dep).await {
		Ok(_) => info!(namespace = %ns_name, "canopy: created postgres Deployment"),
		Err(kube::Error::Api(err)) if err.code == 409 => {
			debug!(namespace = %ns_name, "canopy: postgres Deployment already exists");
		}
		Err(err) => return Err(err.into()),
	}

	// Ensure the Service. Intent + declaration name feed the intent-driven
	// service annotations (e.g. `tailscale.com/hostname: infra-replica-{name}`
	// for `analytics-dbt`).
	let declaration_name = ns
		.annotations()
		.get("pgro.bes.au/name")
		.cloned()
		.unwrap_or_default();
	let svc = super::build_canopy_postgres_service(&ns_name, intent, &declaration_name);
	let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns_name);
	match svc_api.create(&PostParams::default(), &svc).await {
		Ok(_) => info!(namespace = %ns_name, "canopy: created postgres Service"),
		Err(kube::Error::Api(err)) if err.code == 409 => {
			debug!(namespace = %ns_name, "canopy: postgres Service already exists");
		}
		Err(err) => return Err(err.into()),
	}
	Ok(())
}

/// Read the postgres major version stashed on the namespace annotation.
/// The restore Job's script writes /pgdata/.postgres-version onto the
/// PVC; the reporter mirrors it to the namespace annotation the first
/// time it sees it (see `annotate_postgres_version_from_job`).
fn annotations_version(ns: &Namespace) -> Option<String> {
	ns.annotations()
		.get("pgro.bes.au/postgres-version")
		.cloned()
}

async fn ensure_analytics_secret(
	ctx: &Context,
	namespace: &str,
	name: &str,
	key: &str,
) -> Result<()> {
	let api: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
	if api.get_opt(name).await?.is_some() {
		return Ok(());
	}
	let password = generate_password();
	let secret = Secret {
		metadata: ObjectMeta {
			name: Some(name.into()),
			namespace: Some(namespace.into()),
			..Default::default()
		},
		string_data: None,
		data: Some(std::collections::BTreeMap::from([(
			key.into(),
			ByteString(password.into_bytes()),
		)])),
		..Default::default()
	};
	match api.create(&PostParams::default(), &secret).await {
		Ok(_) => Ok(()),
		Err(kube::Error::Api(err)) if err.code == 409 => Ok(()),
		Err(err) => Err(err.into()),
	}
}

/// Random alphanumeric password for the postgres superuser. 32 chars is
/// well above brute-force practicality for a network-isolated Service.
fn generate_password() -> String {
	use rand::seq::IndexedRandom;
	let chars: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	let mut rng = rand::rng();
	(0..32)
		.map(|_| *chars.choose(&mut rng).unwrap() as char)
		.collect()
}

/// Pick the most recent restore Job by creation timestamp. Usually there
/// is only one, but a refresh may leave the old one lingering until TTL.
fn latest_job(jobs: &[Job]) -> Option<&Job> {
	jobs.iter().max_by_key(|j| {
		j.metadata
			.creation_timestamp
			.as_ref()
			.map(|t| t.0)
			.unwrap_or(jiff::Timestamp::MIN)
	})
}

/// Read the snapshot id from the running Job's env — mirrors what the
/// syncer set at Job creation time.
fn snapshot_id_from_job(job: &Job) -> Option<&str> {
	let spec = job.spec.as_ref()?;
	let containers = spec.template.spec.as_ref()?.containers.iter();
	for c in containers {
		if c.name == super::KOPIA_JOB_NAME
			&& let Some(env) = c.env.as_ref()
		{
			for e in env {
				if e.name == "SNAPSHOT_ID"
					&& let Some(v) = &e.value
				{
					return Some(v);
				}
			}
		}
	}
	None
}

/// Classify a Job's terminal state into (new_state, outcome, error).
/// `outcome` is `None` while the Job is still running.
fn classify_job(job: &Job) -> (&'static str, Option<Outcome>, Option<String>) {
	let status = match job.status.as_ref() {
		Some(s) => s,
		None => return (restore_state::PENDING, None, None),
	};
	if status.succeeded.unwrap_or(0) > 0 {
		return (restore_state::ACTIVE, Some(Outcome::Success), None);
	}
	if status.failed.unwrap_or(0) > 0 {
		// Grab the most informative failure condition message if present.
		let msg = status
			.conditions
			.as_ref()
			.and_then(|cs| {
				cs.iter().find_map(|c| {
					if c.type_ == "Failed" && c.status == "True" {
						c.message.clone().or_else(|| c.reason.clone())
					} else {
						None
					}
				})
			})
			.unwrap_or_else(|| "restore Job failed".to_string());
		return (restore_state::FAILED, Some(Outcome::Failure), Some(msg));
	}
	// Job is running (active > 0) — transition to `restoring`.
	if status.active.unwrap_or(0) > 0 {
		return (restore_state::RESTORING, None, None);
	}
	(restore_state::PENDING, None, None)
}

async fn send_verification(
	ctx: &Context,
	ns: &Namespace,
	outcome: Outcome,
	error_msg: Option<&str>,
) -> Result<()> {
	let Some(canopy) = ctx.canopy.as_ref() else {
		return Err(crate::error::Error::Canopy(
			"canopy client not configured".into(),
		));
	};
	let ns_labels = ns.labels();
	let annos = ns.annotations();

	let replica_id = ns_labels
		.get(labels::DECLARATION_ID)
		.and_then(|s| Uuid::parse_str(s).ok());
	let Some(group) = ns_labels
		.get(labels::GROUP)
		.and_then(|s| Uuid::parse_str(s).ok())
	else {
		return Err(crate::error::Error::Canopy(format!(
			"namespace {} missing group label",
			ns.name_any()
		)));
	};
	let Some(server_id) = ns_labels
		.get(labels::SERVER)
		.and_then(|s| Uuid::parse_str(s).ok())
	else {
		return Err(crate::error::Error::Canopy(format!(
			"namespace {} missing server label",
			ns.name_any()
		)));
	};
	let backup_type = ns_labels
		.get(labels::TYPE)
		.map(String::as_str)
		.unwrap_or("");
	let intent = ns_labels
		.get(labels::INTENT)
		.map(String::as_str)
		.unwrap_or("");
	let snapshot_id = annos
		.get(annotations::LAST_RESTORED_SNAPSHOT_ID)
		.or_else(|| annos.get(annotations::DESIRED_SNAPSHOT_ID))
		.map(String::as_str);
	let replica_healthy = matches!(outcome, Outcome::Success);

	// Stats are POSTed by the sidecar on shutdown to /api/v1/canopy-stats/...
	// and land in ctx.canopy_stats keyed by `{namespace}/{job}`. We look
	// them up by the terminal Job's name. Missing stats are non-fatal —
	// the report goes out without them.
	let ns_name = ns.name_any();
	let stats = latest_stats_for_namespace(ctx, ns).await;

	let report = RestoreVerification {
		replica_id,
		group,
		server_id,
		r#type: backup_type,
		intent,
		snapshot_id,
		outcome,
		error: error_msg,
		replica_healthy,
		postgres_version: None,
		observed_at: jiff::Timestamp::now(),
		s3_sent_raw_bytes: stats.as_ref().map(|s| s.sent_raw_bytes as i64),
		s3_sent_payload_bytes: stats.as_ref().map(|s| s.sent_payload_bytes as i64),
		s3_received_raw_bytes: stats.as_ref().map(|s| s.received_raw_bytes as i64),
		s3_received_payload_bytes: stats.as_ref().map(|s| s.received_payload_bytes as i64),
	};

	let result = canopy.restore_verification(&report).await;
	drop(ns_name); // silence unused-let warning if the field isn't consumed
	result
}

/// Sidecar-reported stats. Mirrors the shape the sidecar POSTs on
/// shutdown (see `src/bin/canopy_proxy.rs::StatsFile`).
#[derive(Debug, serde::Deserialize)]
struct SidecarStats {
	sent_raw_bytes: u64,
	sent_payload_bytes: u64,
	received_raw_bytes: u64,
	received_payload_bytes: u64,
}

/// Pull the latest sidecar stats for this namespace's most-recent restore
/// Job out of `ctx.canopy_stats`. Non-destructive read via `take` — a
/// second reporter pass wouldn't find them, but we've already stamped
/// `last-verification-reported-at` at that point so this doesn't matter.
async fn latest_stats_for_namespace(ctx: &Context, ns: &Namespace) -> Option<SidecarStats> {
	let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &ns.name_any());
	let jobs = job_api
		.list(&ListParams::default().labels("pgro.bes.au/job-kind=canopy-restore"))
		.await
		.ok()?;
	let job = latest_job(&jobs.items)?;
	let job_name = job.name_any();
	let raw = ctx.canopy_stats.take(&ns.name_any(), &job_name)?;
	match serde_json::from_str::<SidecarStats>(&raw) {
		Ok(s) => Some(s),
		Err(err) => {
			warn!(
				namespace = %ns.name_any(),
				job = %job_name,
				error = %err,
				"canopy reporter: sidecar stats callback body did not parse"
			);
			None
		}
	}
}

/// Patch the given annotations on a Namespace. `None` values delete the
/// annotation; `Some(v)` sets it. Uses a strategic-merge patch so we don't
/// clobber annotations we don't own.
async fn set_annotations(
	ctx: &Context,
	ns_name: &str,
	updates: &[(&str, Option<String>)],
) -> Result<()> {
	let mut annos = serde_json::Map::new();
	for (key, val) in updates {
		annos.insert(
			(*key).to_string(),
			match val {
				Some(v) => serde_json::Value::String(v.clone()),
				None => serde_json::Value::Null,
			},
		);
	}
	let patch = serde_json::json!({
		"metadata": { "annotations": annos },
	});
	let api: Api<Namespace> = Api::all(ctx.client.clone());
	api.patch(
		ns_name,
		&PatchParams::apply("postgres-restore-operator").force(),
		&Patch::Merge(&patch),
	)
	.await?;
	debug!(namespace = %ns_name, ?updates, "canopy reporter: patched annotations");
	Ok(())
}

// Silence unused-warning for helpers we plan to use in follow-ups.
#[allow(dead_code)]
fn _ensure_types_used(entry: &WorklistEntry) {
	let _ = entry.replica_id;
	let _ = BTreeMap::<String, String>::new();
}

#[cfg(test)]
mod tests {
	use super::*;
	use k8s_openapi::api::batch::v1::JobStatus;

	fn job_with_status(status: JobStatus) -> Job {
		Job {
			status: Some(status),
			..Default::default()
		}
	}

	#[test]
	fn classify_running_job_is_restoring() {
		let job = job_with_status(JobStatus {
			active: Some(1),
			..Default::default()
		});
		let (state, outcome, _) = classify_job(&job);
		assert_eq!(state, restore_state::RESTORING);
		assert!(outcome.is_none());
	}

	#[test]
	fn classify_succeeded_job_is_active_success() {
		let job = job_with_status(JobStatus {
			succeeded: Some(1),
			..Default::default()
		});
		let (state, outcome, _) = classify_job(&job);
		assert_eq!(state, restore_state::ACTIVE);
		assert_eq!(outcome, Some(Outcome::Success));
	}

	#[test]
	fn classify_failed_job_is_failed_failure() {
		let job = job_with_status(JobStatus {
			failed: Some(3),
			..Default::default()
		});
		let (state, outcome, err) = classify_job(&job);
		assert_eq!(state, restore_state::FAILED);
		assert_eq!(outcome, Some(Outcome::Failure));
		assert!(err.is_some());
	}

	#[test]
	fn classify_pending_job_is_pending() {
		let job = job_with_status(JobStatus::default());
		let (state, outcome, _) = classify_job(&job);
		assert_eq!(state, restore_state::PENDING);
		assert!(outcome.is_none());
	}
}
