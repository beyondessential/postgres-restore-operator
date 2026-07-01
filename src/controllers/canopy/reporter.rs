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
//! What the reporter does NOT do (yet):
//! - Postgres version detection. There is no Deployment on the canopy path
//!   yet; the field is left null.
//! - S3 traffic tallies. The sidecar writes them to an emptyDir volume that
//!   is gone by the time we observe the terminated Pod. Requires either
//!   annotation-write RBAC in the sidecar or a broker POST — deferred.

use std::collections::BTreeMap;

use bestool_canopy::{Outcome, RestoreVerification, WorklistEntry};
use k8s_openapi::api::{batch::v1::Job, core::v1::Namespace};
use kube::{
	Api, ResourceExt,
	api::{ListParams, Patch, PatchParams},
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
		s3_sent_raw_bytes: None,
		s3_sent_payload_bytes: None,
		s3_received_raw_bytes: None,
		s3_received_payload_bytes: None,
	};

	canopy.restore_verification(&report).await
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
