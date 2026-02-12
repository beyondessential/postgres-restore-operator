use std::{collections::BTreeMap, sync::Arc, time::Duration};

use jiff::{SignedDuration, Timestamp};
use k8s_openapi::{
	api::{
		apps::v1::Deployment,
		batch::v1::Job,
		core::v1::{ObjectReference, PersistentVolumeClaim, Service},
	},
	apimachinery::pkg::apis::meta::v1::{OwnerReference, Time},
};
use kube::{
	Api, Client, ResourceExt,
	api::{ObjectMeta, Patch, PatchParams, PostParams},
	runtime::{
		controller::Action,
		events::{Event, EventType},
	},
};
use tracing::{debug, info, warn};

use super::read_job_termination_message;
use crate::{
	context::Context,
	error::{Error, Result},
	types::*,
};

mod builders;

#[cfg(test)]
mod tests;

use builders::{build_deployment, build_pvc, build_restore_job, build_version_detect_job};

async fn fail_restore(
	ctx: &Context,
	namespace: &str,
	name: &str,
	replica_name: &str,
	status_patch: serde_json::Value,
) -> Result<Action> {
	update_restore_status(&ctx.client, namespace, name, status_patch).await?;

	if let Some(promoted_name) = ctx.release_restore_slot(replica_name).await {
		info!(promoted = %promoted_name, "promoted queued restore after failure");
	}

	ctx.metrics.restores_failed_total.inc();

	let replica_ref = ObjectReference {
		api_version: Some("pgro.bes.au/v1alpha1".into()),
		kind: Some("PostgresPhysicalReplica".into()),
		name: Some(replica_name.into()),
		namespace: Some(namespace.into()),
		..Default::default()
	};
	if let Err(e) = ctx
		.recorder
		.publish(
			&Event {
				type_: EventType::Warning,
				reason: "RestoreFailed".into(),
				note: Some(format!("Restore {name} failed")),
				action: "Restore".into(),
				secondary: None,
			},
			&replica_ref,
		)
		.await
	{
		warn!(replica = replica_name, error = %e, "failed to publish RestoreFailed event");
	}

	Ok(Action::requeue(Duration::from_secs(300)))
}

pub async fn reconcile(restore: Arc<PostgresPhysicalRestore>, ctx: Arc<Context>) -> Result<Action> {
	let name = restore.name_any();
	let namespace = restore
		.namespace()
		.ok_or_else(|| Error::MissingNamespace(name.clone()))?;

	ctx.metrics
		.reconciliations_total
		.with_label_values(&["restore"])
		.inc();

	let phase = restore.status.as_ref().and_then(|s| s.phase.clone());

	match phase {
		None | Some(RestorePhase::Pending) => {
			reconcile_pending(&restore, &ctx, &name, &namespace).await
		}
		Some(RestorePhase::Restoring) => {
			reconcile_restoring(&restore, &ctx, &name, &namespace).await
		}
		Some(RestorePhase::Ready) => reconcile_ready(&restore, &ctx, &name, &namespace).await,
		Some(RestorePhase::Switching) => {
			// Parent (replica) controller handles service update.
			// Just wait.
			Ok(Action::requeue(Duration::from_secs(10)))
		}
		Some(RestorePhase::Active) => {
			// Serving traffic, nothing to do
			Ok(Action::requeue(Duration::from_secs(300)))
		}
		Some(RestorePhase::Failed) => {
			// Nothing to do, waiting for cleanup or manual intervention
			Ok(Action::requeue(Duration::from_secs(300)))
		}
	}
}

pub fn error_policy(
	_restore: Arc<PostgresPhysicalRestore>,
	error: &Error,
	ctx: Arc<Context>,
) -> Action {
	warn!(error = %error, "restore reconciliation error");
	ctx.metrics
		.reconciliation_errors_total
		.with_label_values(&["restore"])
		.inc();
	Action::requeue(Duration::from_secs(30))
}

async fn reconcile_pending(
	restore: &PostgresPhysicalRestore,
	ctx: &Context,
	name: &str,
	namespace: &str,
) -> Result<Action> {
	let client = &ctx.client;
	let replica_name = &restore.spec.replica.name;

	// Set created_at if not set
	if restore
		.status
		.as_ref()
		.and_then(|s| s.created_at.as_ref())
		.is_none()
	{
		let now = Timestamp::now();
		update_restore_status(
			client,
			namespace,
			name,
			serde_json::json!({
				"createdAt": now,
				"phase": "Pending",
			}),
		)
		.await?;
	}

	// Delete previous restore's Job for the same replica (log cleanup)
	cleanup_previous_jobs(client, namespace, replica_name, name).await?;

	// Create PVC if it doesn't exist
	let pvc_name = format!("{name}-data");
	let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
	if pvcs.get_opt(&pvc_name).await?.is_none() {
		info!(restore = name, pvc = pvc_name, "creating PVC");
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
		let replica = replicas.get(replica_name).await?;
		let pvc = build_pvc(restore, &pvc_name, namespace, &replica)?;
		pvcs.create(&PostParams::default(), &pvc).await?;
	}

	// Transition to Restoring immediately — don't wait for PVC to bind.
	// With WaitForFirstConsumer storage classes the PVC stays Pending until
	// a pod referencing it is scheduled, so gating on Bound would deadlock.
	update_restore_status(
		client,
		namespace,
		name,
		serde_json::json!({
			"phase": "Restoring",
			"pvc": pvc_name,
		}),
	)
	.await?;

	// Mark as active in the queue (keyed by replica name to match enqueue in replica controller)
	let mut queue = ctx.restore_queue.write().await;
	queue.mark_active(replica_name);
	ctx.metrics.active_restores.set(queue.active.len() as i64);
	ctx.metrics.queue_depth.set(queue.pending.len() as i64);
	drop(queue);

	ctx.metrics.restores_started_total.inc();

	Ok(Action::requeue(Duration::from_secs(5)))
}

async fn reconcile_restoring(
	restore: &PostgresPhysicalRestore,
	ctx: &Context,
	name: &str,
	namespace: &str,
) -> Result<Action> {
	let client = &ctx.client;
	let replica_name = &restore.spec.replica.name;

	// Create or check restore Job
	let job_name = format!("{name}-restore");
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

	let job = match jobs.get_opt(&job_name).await? {
		Some(job) => job,
		None => {
			info!(restore = name, job = job_name, "creating restore job");

			// Look up the parent replica to get the kopia secret ref
			let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
			let replica = replicas.get(replica_name).await?;

			let job =
				build_restore_job(restore, &job_name, namespace, &replica, &ctx.kopia_image())?;
			jobs.create(&PostParams::default(), &job).await?
		}
	};

	// Check job status
	let job_status = &job.status;
	let succeeded = job_status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);
	let failed = job_status.as_ref().and_then(|s| s.failed).unwrap_or(0);

	if succeeded > 0 {
		debug!(restore = name, "restore job succeeded");

		let pg_version =
			read_job_termination_message(client, namespace, &job_name, "restore").await;
		if let Some(ref v) = pg_version {
			info!(
				restore = name,
				postgres_version = v,
				"detected postgres version from restore job"
			);
		} else {
			warn!(
				restore = name,
				"could not read postgres version from job pod termination message"
			);
		}

		let now = Time(Timestamp::now());
		let completed_at = job_status
			.as_ref()
			.and_then(|s| s.completion_time.clone())
			.unwrap_or_else(|| now.clone());

		let mut status_patch = serde_json::json!({
			"phase": "Ready",
			"restoredAt": now,
			"restoreJob": {
				"name": job_name,
				"phase": "Succeeded",
				"completedAt": completed_at,
			},
		});
		if let Some(v) = pg_version {
			status_patch["postgresVersion"] = serde_json::Value::String(v);
		}

		update_restore_status(client, namespace, name, status_patch).await?;

		// Delete the completed Job (and its pods) to free resources and
		// release the PVC reference. The ttlSecondsAfterFinished on the
		// Job spec acts as a safety net in case this deletion fails.
		if let Err(e) = jobs.delete(&job_name, &Default::default()).await {
			warn!(job = %job_name, error = %e, "failed to delete completed restore job");
		}

		ctx.metrics.restores_completed_total.inc();

		return Ok(Action::requeue(Duration::from_secs(5)));
	}

	// Check for backoff limit exceeded
	let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(3);
	if failed > backoff_limit {
		warn!(restore = name, failed = failed, "restore job failed");

		// Extend the TTL to 24 hours so the failed Job's pods (and their
		// logs) stick around long enough for someone to investigate.  We
		// deliberately do *not* proactively delete failed Jobs the way we
		// do for successful ones.
		const FAILED_JOB_TTL_SECS: i32 = 86_400; // 24 hours
		let ttl_patch = serde_json::json!({
			"spec": { "ttlSecondsAfterFinished": FAILED_JOB_TTL_SECS }
		});
		if let Err(e) = jobs
			.patch(
				&job_name,
				&PatchParams::apply("postgres-restore-operator").force(),
				&Patch::Merge(&ttl_patch),
			)
			.await
		{
			warn!(job = %job_name, error = %e, "failed to extend TTL on failed restore job");
		}

		return fail_restore(
			ctx,
			namespace,
			name,
			replica_name,
			serde_json::json!({
				"phase": "Failed",
				"restoreJob": {
					"name": job_name,
					"phase": "Failed",
				},
			}),
		)
		.await;
	}

	// Still running
	update_restore_status(
		client,
		namespace,
		name,
		serde_json::json!({
			"restoreJob": {
				"name": job_name,
				"phase": "Running",
			},
		}),
	)
	.await?;

	Ok(Action::requeue(Duration::from_secs(15)))
}

/// Ensure a per-restore Service exists for stable FDW endpoints.
async fn ensure_restore_service(
	client: &Client,
	restore: &PostgresPhysicalRestore,
	name: &str,
	namespace: &str,
) -> Result<()> {
	let services: Api<Service> = Api::namespaced(client.clone(), namespace);

	if services.get_opt(name).await?.is_some() {
		return Ok(());
	}

	info!(restore = name, "creating per-restore service");

	let service = Service {
		metadata: ObjectMeta {
			name: Some(name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				(
					"pgro.bes.au/replica".to_string(),
					restore.spec.replica.name.clone(),
				),
				("pgro.bes.au/restore".to_string(), name.to_string()),
			])),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(k8s_openapi::api::core::v1::ServiceSpec {
			type_: Some("ClusterIP".to_string()),
			ports: Some(vec![k8s_openapi::api::core::v1::ServicePort {
				name: Some("postgres".to_string()),
				port: 5432,
				target_port: Some(
					k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(5432),
				),
				protocol: Some("TCP".to_string()),
				..Default::default()
			}]),
			selector: Some(BTreeMap::from([(
				"pgro.bes.au/restore".to_string(),
				name.to_string(),
			)])),
			..Default::default()
		}),
		..Default::default()
	};

	services.create(&PostParams::default(), &service).await?;
	Ok(())
}

async fn reconcile_ready(
	restore: &PostgresPhysicalRestore,
	ctx: &Context,
	name: &str,
	namespace: &str,
) -> Result<Action> {
	let client = &ctx.client;
	let replica_name = &restore.spec.replica.name;

	// If postgresVersion is missing (e.g. restore job pod was evicted before
	// we could read the termination message), recover by launching a small job
	// that reads PG_VERSION from the PVC.
	if restore
		.status
		.as_ref()
		.and_then(|s| s.postgres_version.as_ref())
		.is_none()
	{
		let detect_job_name = format!("{name}-version-detect");
		let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

		match jobs.get_opt(&detect_job_name).await? {
			None => {
				info!(
					restore = name,
					job = detect_job_name,
					"postgresVersion missing from status, creating version detection job"
				);
				let pvc_name = format!("{name}-data");
				let job = build_version_detect_job(restore, &detect_job_name, namespace, &pvc_name);
				jobs.create(&PostParams::default(), &job).await?;
				return Ok(Action::requeue(Duration::from_secs(5)));
			}
			Some(job) => {
				let succeeded = job.status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);
				let failed = job.status.as_ref().and_then(|s| s.failed).unwrap_or(0);

				if succeeded > 0 {
					let version = read_job_termination_message(
						client,
						namespace,
						&detect_job_name,
						"version-detect",
					)
					.await;

					if let Err(e) = jobs.delete(&detect_job_name, &Default::default()).await {
						warn!(job = detect_job_name, error = %e, "failed to delete version detect job");
					}

					if let Some(v) = version
						&& !v.is_empty()
					{
						info!(
							restore = name,
							postgres_version = v,
							"recovered postgres version from PVC"
						);
						update_restore_status(
							client,
							namespace,
							name,
							serde_json::json!({ "postgresVersion": v }),
						)
						.await?;
						return Ok(Action::requeue(Duration::from_secs(1)));
					}

					warn!(
						restore = name,
						"version detection job succeeded but returned no version, marking as Failed"
					);
					return fail_restore(
						ctx,
						namespace,
						name,
						replica_name,
						serde_json::json!({ "phase": "Failed" }),
					)
					.await;
				}

				let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(2);
				if failed > backoff_limit {
					warn!(
						restore = name,
						"version detection job failed, marking restore as Failed"
					);
					if let Err(e) = jobs.delete(&detect_job_name, &Default::default()).await {
						warn!(job = detect_job_name, error = %e, "failed to delete version detect job");
					}
					return fail_restore(
						ctx,
						namespace,
						name,
						replica_name,
						serde_json::json!({ "phase": "Failed" }),
					)
					.await;
				}

				return Ok(Action::requeue(Duration::from_secs(5)));
			}
		}
	}

	// Look up the parent replica for config
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let replica = replicas.get(&restore.spec.replica.name).await?;

	// Ensure per-restore Service exists (stable endpoint for FDW and direct access)
	ensure_restore_service(client, restore, name, namespace).await?;

	// Apply desired deployment (creates or updates to converge on operator upgrades)
	let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
	let desired = build_deployment(restore, name, namespace, &replica)?;
	let mut patch_value = serde_json::to_value(&desired)?;
	patch_value["apiVersion"] = serde_json::json!("apps/v1");
	patch_value["kind"] = serde_json::json!("Deployment");

	let deploy = deployments
		.patch(
			name,
			&PatchParams::apply("postgres-restore-operator").force(),
			&Patch::Apply(&patch_value),
		)
		.await?;

	// Check if ready
	let ready_replicas = deploy
		.status
		.as_ref()
		.and_then(|s| s.ready_replicas)
		.unwrap_or(0);

	if ready_replicas > 0 {
		info!(
			restore = name,
			"deployment ready, transitioning to Switching"
		);
		update_restore_status(
			client,
			namespace,
			name,
			serde_json::json!({
				"phase": "Switching",
				"deployment": name,
			}),
		)
		.await?;

		if let Some(promoted_name) = ctx.release_restore_slot(replica_name).await {
			info!(promoted = %promoted_name, "promoted queued restore after switchover");
		}

		return Ok(Action::requeue(Duration::from_secs(5)));
	}

	// Check for timeout (10 minutes)
	if let Some(created_at) = restore.status.as_ref().and_then(|s| s.restored_at.as_ref()) {
		let elapsed = Timestamp::now().duration_since(created_at.0);
		if elapsed > SignedDuration::from_secs(10 * 60) {
			warn!(
				restore = name,
				"deployment not ready after 10 minutes, marking as Failed"
			);
			return fail_restore(
				ctx,
				namespace,
				name,
				replica_name,
				serde_json::json!({ "phase": "Failed" }),
			)
			.await;
		}
	}

	Ok(Action::requeue(Duration::from_secs(10)))
}

fn restore_owner_reference(restore: &PostgresPhysicalRestore) -> OwnerReference {
	OwnerReference {
		api_version: "pgro.bes.au/v1alpha1".to_string(),
		kind: "PostgresPhysicalRestore".to_string(),
		name: restore.name_any(),
		uid: restore.uid().unwrap_or_default(),
		controller: Some(true),
		block_owner_deletion: Some(true),
	}
}

async fn update_restore_status(
	client: &Client,
	namespace: &str,
	name: &str,
	fields: serde_json::Value,
) -> Result<()> {
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({ "status": fields });
	restores
		.patch_status(
			name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

/// Delete completed Jobs from previous restores for the same replica,
/// excluding the current restore's Job.
async fn cleanup_previous_jobs(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	current_restore_name: &str,
) -> Result<()> {
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let job_list = jobs
		.list(
			&kube::api::ListParams::default()
				.labels(&format!("pgro.bes.au/replica={replica_name}")),
		)
		.await?;

	for job in &job_list.items {
		let job_name = job.metadata.name.as_deref().unwrap_or("");
		let restore_label = job
			.metadata
			.labels
			.as_ref()
			.and_then(|l| l.get("pgro.bes.au/restore"))
			.map(|s| s.as_str())
			.unwrap_or("");

		if restore_label != current_restore_name {
			info!(
				job = job_name,
				replica = replica_name,
				"deleting previous restore job"
			);
			if let Err(e) = jobs.delete(job_name, &Default::default()).await {
				warn!(job = job_name, error = %e, "failed to delete previous job");
			}
		}
	}

	Ok(())
}
