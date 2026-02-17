use std::{sync::Arc, time::Duration};

use jiff::{SignedDuration, Timestamp};
use k8s_openapi::{
	api::{batch::v1::Job, core::v1::Secret},
	apimachinery::pkg::apis::meta::v1::Time,
};
use kube::{
	Api, Resource, ResourceExt,
	api::{Patch, PatchParams, PostParams},
	runtime::{
		controller::Action,
		events::{Event, EventType},
	},
};
use rand::RngExt;
use tracing::{debug, info, warn};

use super::{overlay, read_job_pod_logs};
use crate::{
	context::Context,
	error::{Error, Result},
	kopia,
	types::*,
};
use scheduling::ScheduleDecision;

mod resources;
mod scheduling;
mod status;

#[cfg(test)]
mod tests;

use resources::*;

/// Generate a random password for analytics credentials.
pub(crate) fn generate_password() -> String {
	let mut rng = rand::rng();
	let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		.chars()
		.collect();
	(0..32)
		.map(|_| chars[rng.random_range(0..chars.len())])
		.collect()
}

pub async fn reconcile(replica: Arc<PostgresPhysicalReplica>, ctx: Arc<Context>) -> Result<Action> {
	let name = replica.name_any();
	let namespace = replica
		.namespace()
		.ok_or_else(|| Error::MissingNamespace(name.clone()))?;
	let now = Timestamp::now();

	ctx.metrics
		.reconciliations_total
		.with_label_values(&["replica"])
		.inc();

	let client = &ctx.client;

	// Validate kopia Secret
	let secret_name = replica
		.spec
		.kopia_secret_ref
		.name
		.as_deref()
		.unwrap_or_default();
	let secrets: Api<Secret> = Api::namespaced(client.clone(), &namespace);
	let secret = match secrets.get(secret_name).await {
		Ok(s) => s,
		Err(e) => {
			warn!(
				replica = name,
				secret = ?replica.spec.kopia_secret_ref,
				error = %e,
				"kopia secret not found"
			);
			replica
				.update_condition(
					client,
					"KopiaSecretValid",
					"False",
					"SecretNotFound",
					&format!("Secret {secret_name} not found: {e}"),
				)
				.await?;
			return Ok(Action::requeue(Duration::from_secs(60)));
		}
	};

	let _creds = match kopia::validate_kopia_secret(&secret) {
		Ok(c) => {
			replica
				.update_condition(
					client,
					"KopiaSecretValid",
					"True",
					"SecretValid",
					"All required keys present",
				)
				.await?;
			c
		}
		Err(e) => {
			warn!(replica = name, error = %e, "kopia secret invalid");
			replica
				.update_condition(
					client,
					"KopiaSecretValid",
					"False",
					"SecretInvalid",
					&e.to_string(),
				)
				.await?;
			return Ok(Action::requeue(Duration::from_secs(60)));
		}
	};

	replica.ensure_credentials_secret(client).await?;

	replica.ensure_service(client).await?;

	replica
		.update_status_field(client, "serviceName", &name)
		.await?;

	replica.update_connection_info(client).await?;

	// Reconcile overlay database if configured, but only after the first
	// restore exists so we know the real snapshot size.
	if replica.spec.overlay_database.is_some() {
		let restores_api: Api<PostgresPhysicalRestore> =
			Api::namespaced(client.clone(), &namespace);
		let snapshot_size = match replica
			.status
			.as_ref()
			.and_then(|s| s.current_restore.as_ref())
		{
			Some(current_restore_name) => match restores_api.get(current_restore_name).await {
				Ok(current_restore) => Some(current_restore.spec.snapshot_size.clone()),
				Err(_) => None,
			},
			None => None,
		};

		if let Some(snapshot_size) = snapshot_size {
			match overlay::reconcile_overlay(client, &namespace, &replica, &snapshot_size).await {
				Ok((cluster_ready, storage_size, pg_version)) => {
					let replicas_api: Api<PostgresPhysicalReplica> =
						Api::namespaced(client.clone(), &namespace);
					let cluster_name = overlay::overlay_cluster_name(&name);
					let patch = serde_json::json!({
						"status": {
							"overlayClusterName": cluster_name,
							"overlayStorageSize": storage_size,
							"overlayPostgresVersion": pg_version,
						}
					});
					replicas_api
						.patch_status(
							&name,
							&PatchParams::apply("postgres-restore-operator"),
							&Patch::Merge(&patch),
						)
						.await?;

					if !cluster_ready {
						info!(replica = name, "overlay cluster not yet ready, will retry");
					}
				}
				Err(e) => {
					warn!(replica = name, error = %e, "failed to reconcile overlay database");
				}
			}
		} else {
			debug!(
				replica = name,
				"no active restore yet, deferring overlay creation until first snapshot"
			);
		}
	}

	// Check child PostgresPhysicalRestore resources
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), &namespace);
	let restore_list = restores
		.list(&kube::api::ListParams::default().labels(&format!("pgro.bes.au/replica={name}")))
		.await?;

	// Find current active restore and any in-progress restores
	let active_restore = restore_list
		.items
		.iter()
		.find(|r| r.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&RestorePhase::Active));
	let switching_restore = restore_list.items.iter().find(|r| {
		r.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&RestorePhase::Switching)
	});
	let in_progress_restore = restore_list.items.iter().find(|r| {
		matches!(
			r.status.as_ref().and_then(|s| s.phase.as_ref()),
			Some(RestorePhase::Pending) | Some(RestorePhase::Restoring) | Some(RestorePhase::Ready)
		)
	});

	// Handle switchover: if a restore is in Switching phase, update Service selector
	if let Some(switching) = switching_restore {
		let switching_name = switching.name_any();
		info!(
			replica = name,
			restore = switching_name,
			"performing blue-green switchover"
		);

		// Update Service selector to point to the new restore
		switching.update_service_selector(client, &name).await?;

		// Transition the switching restore to Active
		switching.update_phase(client, RestorePhase::Active).await?;
		let now = Timestamp::now();
		switching.update_activated_at(client, Time(now)).await?;

		// Update replica status
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), &namespace);
		let previous_restore = replica
			.status
			.as_ref()
			.and_then(|s| s.current_restore.clone());
		let patch = serde_json::json!({
			"status": {
				"phase": "Ready",
				"currentRestore": switching_name,
				"previousRestore": previous_restore,
				"lastRestoreCompletedAt": Time(now),
			}
		});
		replicas
			.patch_status(
				&name,
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;

		ctx.metrics.switchovers_total.inc();

		// Record event on replica CR
		if let Err(e) = ctx
			.recorder
			.publish(
				&Event {
					type_: EventType::Normal,
					reason: "RestoreCompleted".into(),
					note: Some(format!("Switchover to restore {switching_name} completed")),
					action: "Restore".into(),
					secondary: Some(switching.object_ref(&())),
				},
				&replica.object_ref(&()),
			)
			.await
		{
			warn!(replica = name, error = %e, "failed to publish RestoreCompleted event");
		}

		// Send notifications
		replica
			.send_notifications(client, &ctx.http_client, switching, &ctx.metrics)
			.await;

		return Ok(Action::requeue(Duration::from_secs(10)));
	}

	// Reconcile overlay database state (FDW or copy strategy).
	// Always verify and fix the actual state inside the overlay database
	// rather than relying solely on the status field, which can become stale
	// if the overlay cluster is reset.
	if let Some(overlay_config) = &replica.spec.overlay_database {
		use crate::types::OverlayStrategy;

		let current_restore = replica
			.status
			.as_ref()
			.and_then(|s| s.current_restore.as_ref());
		let overlay_restore = replica
			.status
			.as_ref()
			.and_then(|s| s.overlay_restore.as_ref());

		if let Some(current) = current_restore.filter(|_| active_restore.is_some()) {
			debug!(
				replica = name,
				current_restore = %current,
				overlay_restore = ?overlay_restore,
				strategy = ?overlay_config.strategy,
				"reconciling overlay state"
			);

			let result = match overlay_config.strategy {
				OverlayStrategy::Fdw => {
					overlay::fdw::reconcile_fdw(
						client,
						&namespace,
						&replica,
						current,
						ctx.use_port_forward(),
					)
					.await
				}
				OverlayStrategy::Copy => {
					overlay::copy::reconcile_copy(
						client,
						&namespace,
						&replica,
						current,
						ctx.use_port_forward(),
					)
					.await
				}
			};

			match result {
				Ok(()) => {
					if overlay_restore.as_ref() != Some(&current) {
						info!(
							replica = name,
							restore = current,
							strategy = ?overlay_config.strategy,
							"overlay reconciled, updating overlayRestore status"
						);
						let replicas: Api<PostgresPhysicalReplica> =
							Api::namespaced(client.clone(), &namespace);
						let patch = serde_json::json!({
							"status": {
								"overlayRestore": current,
							}
						});
						replicas
							.patch_status(
								&name,
								&PatchParams::apply("postgres-restore-operator"),
								&Patch::Merge(&patch),
							)
							.await?;
						debug!(replica = name, "overlayRestore status patched");
					}

					// Copy strategy: clean up restore resources if retainRestore is false
					if overlay_config.strategy == OverlayStrategy::Copy
						&& !overlay_config.retain_restore
					{
						debug!(
							replica = name,
							restore = current,
							"retainRestore=false, cleaning up restore deployment and PVC"
						);
						let deployments: Api<k8s_openapi::api::apps::v1::Deployment> =
							Api::namespaced(client.clone(), &namespace);
						let pvcs: Api<k8s_openapi::api::core::v1::PersistentVolumeClaim> =
							Api::namespaced(client.clone(), &namespace);
						let pvc_name = format!("{current}-data");

						if let Err(e) = deployments.delete(current, &Default::default()).await {
							warn!(
								replica = name,
								restore = current,
								error = %e,
								"failed to delete restore deployment after copy"
							);
						}
						if let Err(e) = pvcs.delete(&pvc_name, &Default::default()).await {
							warn!(
								replica = name,
								pvc = pvc_name,
								error = %e,
								"failed to delete restore PVC after copy"
							);
						}
					}
				}
				Err(e) => {
					warn!(
						replica = name,
						restore = current,
						strategy = ?overlay_config.strategy,
						error = ?e,
						"overlay reconciliation failed, will retry"
					);
					return Ok(Action::requeue(Duration::from_secs(30)));
				}
			}
		} else {
			debug!(
				replica = name,
				"no current restore set, skipping overlay reconciliation"
			);
		}
	}

	// Clean up old restores after grace period
	if let Some(prev_name) = replica
		.status
		.as_ref()
		.and_then(|s| s.previous_restore.clone())
	{
		let grace_period =
			SignedDuration::try_from(replica.spec.switchover_grace_period.0).unwrap_or_default();
		let last_completed = replica
			.status
			.as_ref()
			.and_then(|s| s.last_restore_completed_at.as_ref());

		if let Some(completed_at) = last_completed {
			let elapsed = now.duration_since(completed_at.0);
			if elapsed > grace_period {
				info!(
					replica = name,
					restore = prev_name,
					"cleaning up previous restore after grace period"
				);
				if let Err(e) = restores.delete(&prev_name, &Default::default()).await {
					warn!(restore = prev_name, error = %e, "failed to delete previous restore");
				}
				// Clear previousRestore from status
				let replicas: Api<PostgresPhysicalReplica> =
					Api::namespaced(client.clone(), &namespace);
				let patch = serde_json::json!({
					"status": {
						"previousRestore": null,
					}
				});
				replicas
					.patch_status(
						&name,
						&PatchParams::apply("postgres-restore-operator"),
						&Patch::Merge(&patch),
					)
					.await?;
			}
		}
	}

	// Clean up failed restores (ownerReferences will cascade-delete their PVCs)
	let failed_restores: Vec<_> = restore_list
		.items
		.iter()
		.filter(|r| r.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&RestorePhase::Failed))
		.collect();
	for failed in &failed_restores {
		let failed_name = failed.name_any();
		if let Some(created_at) = failed.status.as_ref().and_then(|s| s.created_at.as_ref()) {
			let age = now.duration_since(created_at.0);
			if age > SignedDuration::from_secs(300) {
				info!(
					replica = name,
					restore = failed_name,
					"cleaning up failed restore"
				);
				if let Err(e) = restores.delete(&failed_name, &Default::default()).await {
					warn!(restore = failed_name, error = %e, "failed to delete failed restore");
				}
			}
		}
	}

	// Process any existing snapshot-list job regardless of scheduling state.
	// This runs before the in-progress / should_restore gates so that
	// completed jobs are always cleaned up promptly.
	let snapshot_job_name = format!("{name}-snapshot-list");
	let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);
	let snapshot_job = jobs.get_opt(&snapshot_job_name).await?;

	if let Some(ref job) = snapshot_job {
		let succeeded = job.status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);
		let failed = job.status.as_ref().and_then(|s| s.failed).unwrap_or(0);
		let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(2);

		if succeeded > 0 {
			let raw = ctx
				.take_snapshot_result(&namespace, &name)
				.or_else(|| {
					debug!(
						replica = name,
						job = snapshot_job_name,
						"no callback result in store, falling back to pod logs"
					);
					None
				})
				.or(
					read_job_pod_logs(client, &namespace, &snapshot_job_name, "snapshot-list")
						.await,
				);

			let Some(ref raw) = raw else {
				let completion_time = job.status.as_ref().and_then(|s| s.completion_time.as_ref());
				let stale = completion_time
					.is_some_and(|t| now.duration_since(t.0) > SignedDuration::from_secs(60));
				if stale {
					warn!(
						replica = name,
						job = snapshot_job_name,
						"snapshot list job results unreadable after 60s, deleting stale job"
					);
					if let Err(e) = jobs.delete(&snapshot_job_name, &Default::default()).await {
						warn!(job = snapshot_job_name, error = %e, "failed to delete stale snapshot list job");
					}
					return Ok(Action::requeue(Duration::from_secs(10)));
				}
				info!(
					replica = name,
					job = snapshot_job_name,
					"snapshot list job succeeded but results not yet available, retrying"
				);
				return Ok(Action::requeue(Duration::from_secs(5)));
			};

			// We have the data — safe to delete the job now.
			if let Err(e) = jobs.delete(&snapshot_job_name, &Default::default()).await {
				warn!(job = snapshot_job_name, error = %e, "failed to delete snapshot list job");
			}

			match kopia::parse_snapshot_list_output(raw) {
				Ok(all_snapshots) => {
					let filtered = kopia::filter_snapshots(
						&all_snapshots,
						replica.spec.snapshot_filter.as_ref(),
					);
					let latest = kopia::latest_snapshot(&filtered);

					if let Some(snap) = latest {
						let size = snap.total_size_bytes();
						replica
							.update_status_field(client, "latestAvailableSnapshot", &snap.id)
							.await?;

						let current_snapshot_id = active_restore.map(|r| r.spec.snapshot.as_str());

						if current_snapshot_id == Some(&snap.id) {
							debug!(
								replica = name,
								snapshot = snap.id,
								"latest snapshot already active, skipping"
							);
						} else {
							info!(
								replica = name,
								snapshot = snap.id,
								size,
								"new snapshot available, creating restore"
							);
							let info = SnapshotInfo {
								id: snap.id.clone(),
								size,
							};
							replica.create_restore_for_snapshot(client, &info).await?;
							ctx.metrics.restores_started_total.inc();

							if let Err(e) = ctx
								.recorder
								.publish(
									&Event {
										type_: EventType::Normal,
										reason: "RestoreStarted".into(),
										note: Some(format!(
											"Started restore from snapshot {}",
											snap.id
										)),
										action: "Restore".into(),
										secondary: None,
									},
									&replica.object_ref(&()),
								)
								.await
							{
								warn!(replica = name, error = %e, "failed to publish RestoreStarted event");
							}
						}
					} else {
						warn!(
							replica = name,
							"snapshot list job returned no matching snapshots"
						);
					}
				}
				Err(e) => {
					warn!(
						replica = name,
						error = %e,
						"failed to parse snapshot list job output"
					);
				}
			}
		} else if failed > backoff_limit {
			warn!(replica = name, "snapshot list job failed");
			// Extend the TTL to 24 hours so the failed Job's pods (and
			// their logs) stick around long enough for someone to
			// investigate. We deliberately do *not* proactively delete
			// failed Jobs the way we do for successful ones.
			const FAILED_JOB_TTL_SECS: i32 = 86_400; // 24 hours
			let ttl_patch = serde_json::json!({
				"spec": { "ttlSecondsAfterFinished": FAILED_JOB_TTL_SECS }
			});
			if let Err(e) = jobs
				.patch(
					&snapshot_job_name,
					&PatchParams::apply("postgres-restore-operator").force(),
					&Patch::Merge(&ttl_patch),
				)
				.await
			{
				warn!(job = snapshot_job_name, error = %e, "failed to extend TTL on failed snapshot list job");
			}
		} else {
			return Ok(Action::requeue(Duration::from_secs(10)));
		}
	}

	// Decide whether to trigger a new restore
	if let Some(in_progress) = in_progress_restore {
		let phase = in_progress.status.as_ref().and_then(|s| s.phase.as_ref());
		debug!(
			replica = name,
			restore = in_progress.name_any(),
			phase = ?phase,
			"restore already in progress, waiting"
		);
		replica
			.update_phase(client, ReplicaPhase::Restoring)
			.await?;
		return Ok(Action::requeue(Duration::from_secs(30)));
	}

	let never_restored = active_restore.is_none()
		&& replica
			.status
			.as_ref()
			.and_then(|s| s.last_restore_completed_at.as_ref())
			.is_none();

	// Detect when the active Restore CR referenced in status has been deleted
	let active_restore_deleted = active_restore.is_none()
		&& in_progress_restore.is_none()
		&& replica
			.status
			.as_ref()
			.and_then(|s| s.current_restore.as_ref())
			.is_some();

	// Detect if the schedule or jitter has changed since we last computed nextScheduledRestore
	let current_hash = replica.schedule_input_hash();
	let schedule_changed = replica
		.status
		.as_ref()
		.and_then(|s| s.schedule_input_hash.as_ref())
		.is_none_or(|h| h != &current_hash);

	// Recompute nextScheduledRestore when missing or when schedule config changed
	if (schedule_changed
		|| replica
			.status
			.as_ref()
			.and_then(|s| s.next_scheduled_restore.as_ref())
			.is_none())
		&& let Some(next) = replica.compute_next_scheduled_restore(now)
	{
		if schedule_changed {
			debug!(
				replica = %name,
				schedule = %replica.spec.schedule,
				jitter = %replica.spec.schedule_jitter,
				next = %next,
				"schedule config changed, recomputing nextScheduledRestore"
			);
		}
		replica
			.update_schedule_status(client, next, &current_hash)
			.await?;
	}

	if never_restored {
		info!(
			replica = name,
			"no successful restore yet, triggering immediately"
		);
	}

	if active_restore_deleted {
		info!(
			replica = name,
			"active restore CR was deleted, triggering immediate replacement"
		);
	}

	let schedule_decision = replica.check_schedule();

	let should_restore = never_restored
		|| active_restore_deleted
		|| matches!(schedule_decision, ScheduleDecision::Trigger);

	if should_restore && snapshot_job.is_none() {
		// Check concurrent restore limit
		let mut queue = ctx.restore_queue.write().await;
		if !queue.can_start(ctx.max_concurrent_restores()) {
			queue.enqueue(name.clone());
			let position = queue.position(&name);
			let pending_len = queue.pending.len();
			drop(queue);

			let replicas: Api<PostgresPhysicalReplica> =
				Api::namespaced(client.clone(), &namespace);
			let patch = serde_json::json!({
				"status": {
					"queuePosition": position,
				}
			});
			replicas
				.patch_status(
					&name,
					&PatchParams::apply("postgres-restore-operator"),
					&Patch::Merge(&patch),
				)
				.await?;

			ctx.metrics.queue_depth.set(pending_len as i64);
			return Ok(Action::requeue(Duration::from_secs(30)));
		}
		drop(queue);

		info!(replica = name, "creating snapshot list job");
		let callback_url = ctx.snapshot_callback_url(&namespace, &name);
		let job = build_snapshot_list_job(
			&replica,
			&snapshot_job_name,
			&namespace,
			&ctx.kopia_image(),
			&callback_url,
		)?;
		jobs.create(&PostParams::default(), &job).await?;
		return Ok(Action::requeue(Duration::from_secs(10)));
	}

	// Advance the schedule only after the restore decision has been fully
	// processed (or skipped by TTL).  Bumping before the should_restore
	// block caused watch-triggered reconciliations to see the future
	// nextScheduledRestore and skip snapshot-job processing entirely.
	if matches!(
		schedule_decision,
		ScheduleDecision::Trigger | ScheduleDecision::SkippedByTtl
	) && let Some(next) = replica.compute_next_scheduled_restore(now)
	{
		replica
			.update_schedule_status(client, next, &current_hash)
			.await?;
	}

	// Update phase based on current state
	if active_restore.is_some() {
		replica.update_phase(client, ReplicaPhase::Ready).await?;
	} else {
		replica.update_phase(client, ReplicaPhase::Pending).await?;
	}

	// Clear queue position if not queued
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), &namespace);
	let patch = serde_json::json!({
		"status": {
			"queuePosition": 0,
		}
	});
	replicas
		.patch_status(
			&name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;

	Ok(Action::requeue(Duration::from_secs(300)))
}

pub fn error_policy(
	_replica: Arc<PostgresPhysicalReplica>,
	error: &Error,
	ctx: Arc<Context>,
) -> Action {
	warn!(error = %error, "replica reconciliation error");
	ctx.metrics
		.reconciliation_errors_total
		.with_label_values(&["replica"])
		.inc();
	Action::requeue(Duration::from_secs(60))
}
