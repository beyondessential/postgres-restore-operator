use std::{collections::BTreeMap, sync::Arc, time::Duration};

use jiff::Timestamp;
use k8s_openapi::{
	api::{
		apps::v1::Deployment,
		batch::v1::Job,
		core::v1::{ObjectReference, PersistentVolumeClaim, Service},
	},
	apimachinery::pkg::{
		api::resource::Quantity,
		apis::meta::v1::{OwnerReference, Time},
	},
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
	context::{Context, ReplicaKey},
	error::{Error, Result},
	types::*,
};

pub mod builders;

pub(crate) use builders::{build_credential_reset_job, credential_reset_job_name};

#[cfg(test)]
mod tests;

use builders::{build_deployment, build_pvc, build_restore_job, build_version_detect_job};

/// SSA-apply the desired Deployment for a restore so it converges on any
/// spec changes (e.g. an init-script update introduced by an operator
/// upgrade). Returns the resulting `Deployment` so callers can inspect
/// `status.ready_replicas` for phase-transition decisions.
///
/// Used by every phase that owns a running restore pod — `Restoring`,
/// `Ready`, `Switching`, `Active` — so that a deployment whose phase
/// happened to be in any of those states during an operator upgrade
/// doesn't keep stale init scripts forever.
async fn apply_restore_deployment(
	client: &Client,
	restore: &PostgresPhysicalRestore,
	replica: &PostgresPhysicalReplica,
	name: &str,
	namespace: &str,
) -> Result<Deployment> {
	let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
	let desired = build_deployment(restore, name, namespace, replica)?;
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
	Ok(deploy)
}

/// Ensure the per-replica kopia cache PVC exists and is at least sized for
/// the current snapshot. Creates the PVC on first call, patches the
/// requested storage upward on subsequent calls when the desired size
/// (computed from this snapshot) is larger than the current request.
/// Never shrinks. Resize requires the storage class to allow volume
/// expansion; failure is logged and the restore proceeds with whatever
/// size the PVC currently has.
async fn ensure_kopia_cache_pvc(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	snapshot_size: &Quantity,
) -> Result<()> {
	let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
	let cache_pvc_name = builders::kopia_cache_pvc_name(replica_name);
	let desired_size = builders::kopia_cache_pvc_size(snapshot_size);

	match pvcs.get_opt(&cache_pvc_name).await? {
		None => {
			info!(pvc = cache_pvc_name, "creating shared kopia cache PVC");
			let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
			let replica = replicas.get(replica_name).await?;
			let pvc = builders::build_kopia_cache_pvc(&replica, snapshot_size, namespace);
			pvcs.create(&PostParams::default(), &pvc).await?;
		}
		Some(existing) => {
			let current = existing
				.spec
				.as_ref()
				.and_then(|s| s.resources.as_ref())
				.and_then(|r| r.requests.as_ref())
				.and_then(|reqs| reqs.get("storage"));
			if let Some(current) = current
				&& builders::cache_size_needs_grow(current, &desired_size)
			{
				info!(
					pvc = cache_pvc_name,
					current = current.0,
					desired = desired_size.0,
					"growing shared kopia cache PVC"
				);
				let patch = serde_json::json!({
					"spec": {
						"resources": {
							"requests": {
								"storage": desired_size,
							}
						}
					}
				});
				if let Err(e) = pvcs
					.patch(
						&cache_pvc_name,
						&PatchParams::apply("postgres-restore-operator"),
						&Patch::Merge(&patch),
					)
					.await
				{
					warn!(
						pvc = cache_pvc_name,
						error = %e,
						"failed to grow cache PVC (storage class may not support volume expansion); continuing with current size"
					);
				}
			}
		}
	}
	Ok(())
}

/// Bump the kopia cache PVC's requested storage by
/// [`builders::KOPIA_CACHE_PRESSURE_GROWTH_FACTOR`] in response to a
/// `PGRO_CACHE_PRESSURE` HTTP callback from a restore Job. Best-effort:
/// any failure to look up the restore or patch the PVC is logged but
/// doesn't escalate, since the callback itself isn't the source of truth
/// (the next pressure event will re-fire).
pub async fn grow_cache_pvc_after_pressure(client: &Client, namespace: &str, restore_name: &str) {
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), namespace);
	let Ok(restore) = restores.get(restore_name).await else {
		warn!(
			restore = restore_name,
			"cannot grow cache PVC after pressure: restore not found"
		);
		return;
	};
	let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
	let cache_pvc_name = builders::kopia_cache_pvc_name(&restore.spec.replica.name);
	let Ok(Some(existing)) = pvcs.get_opt(&cache_pvc_name).await else {
		warn!(
			pvc = cache_pvc_name,
			"cannot grow cache PVC after pressure: PVC not found"
		);
		return;
	};
	let Some(current) = existing
		.spec
		.as_ref()
		.and_then(|s| s.resources.as_ref())
		.and_then(|r| r.requests.as_ref())
		.and_then(|reqs| reqs.get("storage"))
		.cloned()
	else {
		warn!(
			pvc = cache_pvc_name,
			"cannot grow cache PVC after pressure: no storage request set"
		);
		return;
	};
	let next = builders::next_cache_pvc_size_after_pressure(&current, &restore.spec.snapshot_size);
	if !builders::cache_size_needs_grow(&current, &next) {
		info!(
			pvc = cache_pvc_name,
			current = current.0,
			"cache PVC already at growth cap; not growing further despite pressure"
		);
		return;
	}
	info!(
		pvc = cache_pvc_name,
		current = current.0,
		next = next.0,
		"PGRO_CACHE_PRESSURE observed; growing cache PVC"
	);
	let patch = serde_json::json!({
		"spec": {
			"resources": {
				"requests": {
					"storage": next,
				}
			}
		}
	});
	if let Err(e) = pvcs
		.patch(
			&cache_pvc_name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await
	{
		warn!(
			pvc = cache_pvc_name,
			error = %e,
			"failed to grow cache PVC after pressure (storage class may not support volume expansion)"
		);
	}
}

async fn fail_restore(
	ctx: &Context,
	namespace: &str,
	name: &str,
	replica_name: &str,
	status_patch: serde_json::Value,
	error: &str,
) -> Result<Action> {
	update_restore_status(&ctx.client, namespace, name, status_patch).await?;

	if let Some(promoted) = ctx
		.release_restore_slot(&ReplicaKey::new(namespace, replica_name))
		.await
	{
		info!(promoted = %promoted, "promoted queued restore after failure");
	}

	// Increment consecutiveRestoreFailures on the parent replica and advance
	// nextScheduledRestore by the failure backoff, so the next reconcile
	// retries on a bounded, sub-cron cadence instead of waiting until the
	// next cron tick.
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(ctx.client.clone(), namespace);
	if let Ok(replica) = replicas.get(replica_name).await {
		let current = replica
			.status
			.as_ref()
			.and_then(|s| s.consecutive_restore_failures)
			.unwrap_or(0);
		let new_count = current + 1;
		let backoff = super::replica::scheduling::failure_backoff_delay(new_count);
		let next_scheduled = backoff.map(|d| Time(Timestamp::now() + d));
		let patch = serde_json::json!({
			"status": {
				"consecutiveRestoreFailures": new_count,
				"nextScheduledRestore": next_scheduled,
			}
		});
		if let Err(e) = replicas
			.patch_status(
				replica_name,
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await
		{
			warn!(replica = replica_name, error = %e, "failed to increment consecutiveRestoreFailures");
		} else {
			info!(
				replica = replica_name,
				consecutive_failures = new_count,
				next_scheduled = ?next_scheduled.map(|t| t.0),
				"incremented consecutive restore failure count, scheduled retry via backoff"
			);
		}
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

	// Canopy verification (signal 3, failure) — no-op unless the replica
	// has spec.canopy_source. The caller passes a short reason so
	// operators see it on canopy's UI without having to trawl k8s events.
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(ctx.client.clone(), namespace);
	if let (Ok(restore), Ok(replica)) = (restores.get(name).await, replicas.get(replica_name).await)
	{
		crate::controllers::canopy::verification::report(
			ctx,
			&replica,
			&restore,
			bestool_canopy::schema::RunOutcome::Failure,
			Some(error),
		)
		.await;
	}

	Ok(Action::requeue(Duration::from_secs(300)))
}

pub async fn reconcile(restore: Arc<PostgresPhysicalRestore>, ctx: Arc<Context>) -> Result<Action> {
	let name = restore.name_any();
	let namespace = restore
		.namespace()
		.ok_or_else(|| Error::MissingNamespace(name.clone()))?;

	ctx.last_reconcile.store(
		jiff::Timestamp::now().as_second(),
		std::sync::atomic::Ordering::Relaxed,
	);

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
			reconcile_switching(&restore, &ctx, &name, &namespace).await
		}
		Some(RestorePhase::Active) => reconcile_active(&restore, &ctx, &name, &namespace).await,
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

	// Ensure data PVC exists (one per restore, no resize needed)
	let pvc_name = format!("{name}-data");
	let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
	if pvcs.get_opt(&pvc_name).await?.is_none() {
		info!(restore = name, pvc = pvc_name, "creating PVC");
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
		let replica = replicas.get(replica_name).await?;
		let pvc = build_pvc(restore, &pvc_name, namespace, &replica)?;
		pvcs.create(&PostParams::default(), &pvc).await?;
	}

	// Ensure cache PVC exists and is sized for the current snapshot. The
	// cache PVC is shared across all restores for the replica and ratchets
	// up as snapshots grow — it never shrinks.
	ensure_kopia_cache_pvc(client, namespace, replica_name, &restore.spec.snapshot_size).await?;

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

	let mut queue = ctx.restore_queue.write().await;
	queue.mark_active(&ReplicaKey::new(namespace, replica_name));
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

			// Mint (or reuse) the canopy run-uuid for this restore run before
			// creating the Job, so the sidecar's credential requests and the
			// eventual verification report carry the same id and canopy can
			// correlate them. Only canopy-backed restores are a "run"; the
			// legacy kopia path has no canopy report. Persist it to status
			// first so it survives a crash between here and Job creation.
			let run_id = if replica.spec.canopy_source.is_some() {
				let id = match restore.status.as_ref().and_then(|s| s.run_id.clone()) {
					Some(existing) => existing,
					None => {
						let minted = uuid::Uuid::new_v4().to_string();
						update_restore_status(
							client,
							namespace,
							name,
							serde_json::json!({ "runId": minted }),
						)
						.await?;
						minted
					}
				};
				Some(id)
			} else {
				None
			};

			let cache_pressure_url = ctx.cache_pressure_callback_url(namespace, name);
			let stats_callback_url = ctx.canopy_stats_callback_url(namespace, &job_name);
			let progress_callback_url = ctx.canopy_progress_callback_url(namespace, &job_name);
			let canopy_proxy = if replica.spec.canopy_source.is_some() {
				Some(builders::CanopyProxyArgs {
					image: &ctx.canopy_proxy_image,
					broker_base_url: &ctx.canopy_broker_base_url,
					stats_callback_url: &stats_callback_url,
					progress_callback_url: Some(&progress_callback_url),
					run_id: run_id.as_deref(),
				})
			} else {
				None
			};
			let job = build_restore_job(
				restore,
				&job_name,
				namespace,
				&replica,
				&ctx.kopia_image(),
				&cache_pressure_url,
				canopy_proxy.as_ref(),
			)?;
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

		// Set a TTL on the failed Job so its pods (and their logs) stick
		// around long enough for investigation. The first failure for a
		// replica gets 24 hours; subsequent consecutive failures get only
		// 10 minutes to avoid accumulating PVCs held by pod references.
		let consecutive_failures = {
			let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
			replicas
				.get(replica_name)
				.await
				.ok()
				.and_then(|r| r.status.as_ref()?.consecutive_restore_failures)
				.unwrap_or(0)
		};
		let failed_job_ttl_secs: i32 = if consecutive_failures > 0 {
			600 // 10 minutes for retries
		} else {
			86_400 // 24 hours for the first failure
		};
		let ttl_patch = serde_json::json!({
			"spec": { "ttlSecondsAfterFinished": failed_job_ttl_secs }
		});
		if let Err(e) = jobs
			.patch(
				&job_name,
				&PatchParams::apply("postgres-restore-operator"),
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
			"kopia restore Job failed after backoff exhausted",
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

/// Keep the restore deployment in sync while it is actively serving.
///
/// This is intentionally lightweight: it SSA-patches the deployment so that
/// credential renames, image changes, or config tweaks introduced by an
/// operator upgrade are applied without requiring a manual restart.
/// Switching phase: the replica controller is in the middle of swapping the
/// service selector to this restore. We don't need to drive the switchover
/// here, but we *do* need to keep the deployment converged with the latest
/// spec — otherwise an init-script update introduced by an operator upgrade
/// while a restore was already Switching would never roll the pod, leaving
/// the running postgres on a stale config indefinitely. (Observed in
/// production: a restore stuck in Switching for 33h+ across two operator
/// upgrades because nothing here was re-applying the deployment.)
async fn reconcile_switching(
	restore: &PostgresPhysicalRestore,
	ctx: &Context,
	name: &str,
	namespace: &str,
) -> Result<Action> {
	let client = &ctx.client;
	let replica_name = &restore.spec.replica.name;

	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let replica = match replicas.get_opt(replica_name).await? {
		Some(r) => r,
		None => return Ok(Action::requeue(Duration::from_secs(30))),
	};

	if restore
		.status
		.as_ref()
		.and_then(|s| s.postgres_version.as_ref())
		.is_some()
	{
		apply_restore_deployment(client, restore, &replica, name, namespace).await?;
	}

	Ok(Action::requeue(Duration::from_secs(10)))
}

async fn reconcile_active(
	restore: &PostgresPhysicalRestore,
	ctx: &Context,
	name: &str,
	namespace: &str,
) -> Result<Action> {
	let client = &ctx.client;
	let replica_name = &restore.spec.replica.name;

	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let replica = match replicas.get_opt(replica_name).await? {
		Some(r) => r,
		None => return Ok(Action::requeue(Duration::from_secs(300))),
	};

	// SSA-patch the deployment to converge on any spec changes.
	if restore
		.status
		.as_ref()
		.and_then(|s| s.postgres_version.as_ref())
		.is_some()
	{
		apply_restore_deployment(client, restore, &replica, name, namespace).await?;
	}

	// Defensively ensure the ready-for-traffic label is on the pod. If the
	// pod restarted (OOM, eviction, node loss), the label is gone from the
	// new pod and the Service stops routing to it; re-applying every pass
	// closes that gap. Also handles the upgrade path where existing Active
	// pods predate the label.
	restore.mark_pod_ready_for_traffic(client).await?;

	Ok(Action::requeue(Duration::from_secs(300)))
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
						"version detection Job succeeded but did not report a postgres version",
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
						"version detection Job failed after backoff exhausted",
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
	let deploy = apply_restore_deployment(client, restore, &replica, name, namespace).await?;

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

		if let Some(promoted) = ctx
			.release_restore_slot(&ReplicaKey::new(namespace, replica_name))
			.await
		{
			info!(promoted = %promoted, "promoted queued restore after switchover");
		}

		return Ok(Action::requeue(Duration::from_secs(5)));
	}

	// Deployment readiness timeout. Replicas with larger data dirs need time
	// for postgres to open the data dir and replay WAL after a fresh kopia
	// restore, so the budget scales with the snapshot unless the replica pins
	// `deploymentReadyTimeout`. The `DEPLOYMENT_READY_TIMEOUT_SECS` env var is
	// the operator-wide floor.
	if let Some(created_at) = restore.status.as_ref().and_then(|s| s.restored_at.as_ref()) {
		let elapsed = Timestamp::now().duration_since(created_at.0);
		let timeout = crate::controllers::replica::scheduling::deployment_ready_timeout(
			replica.spec.deployment_ready_timeout.as_ref(),
			&restore.spec.snapshot_size,
			ctx.deployment_ready_timeout_secs,
		);
		let timeout_secs = timeout.as_secs();
		if elapsed > timeout {
			warn!(
				restore = name,
				timeout_secs, "deployment not ready within configured timeout, marking as Failed"
			);
			return fail_restore(
				ctx,
				namespace,
				name,
				replica_name,
				serde_json::json!({ "phase": "Failed" }),
				&format!(
					"postgres Deployment did not become Ready within {timeout_secs}s of \
					 restore completion"
				),
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
