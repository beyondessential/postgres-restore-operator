use std::{sync::Arc, time::Duration};

use chrono::Utc;
use k8s_openapi::api::{batch::v1::Job, core::v1::Secret};
use kube::{
	Api, ResourceExt,
	api::{Patch, PatchParams, PostParams},
	runtime::controller::Action,
};
use rand::RngExt;
use tracing::{debug, info, warn};

use super::{overlay, read_job_termination_message};
use crate::{
	context::Context,
	error::{Error, Result},
	kopia,
	types::*,
	util::parse_duration,
};

mod resources;
mod scheduling;
mod status;

#[cfg(test)]
mod tests;

use resources::*;
use scheduling::*;
use status::*;

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

	ctx.metrics
		.reconciliations_total
		.with_label_values(&["replica"])
		.inc();

	let client = &ctx.client;

	// 1. Validate kopia Secret
	let secrets: Api<Secret> = Api::namespaced(client.clone(), &namespace);
	let secret = match secrets.get(&replica.spec.kopia_secret_ref).await {
		Ok(s) => s,
		Err(e) => {
			warn!(
				replica = name,
				secret = replica.spec.kopia_secret_ref,
				error = %e,
				"kopia secret not found"
			);
			update_replica_condition(
				client,
				&namespace,
				&name,
				"KopiaSecretValid",
				"False",
				"SecretNotFound",
				&format!("Secret {} not found: {e}", replica.spec.kopia_secret_ref),
			)
			.await?;
			return Ok(Action::requeue(Duration::from_secs(60)));
		}
	};

	let _creds = match kopia::validate_kopia_secret(&secret) {
		Ok(c) => {
			update_replica_condition(
				client,
				&namespace,
				&name,
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
			update_replica_condition(
				client,
				&namespace,
				&name,
				"KopiaSecretValid",
				"False",
				"SecretInvalid",
				&e.to_string(),
			)
			.await?;
			return Ok(Action::requeue(Duration::from_secs(60)));
		}
	};

	// 2. Ensure credentials Secret exists
	let creds_secret_name = format!("{name}-creds");
	ensure_credentials_secret(client, &namespace, &name, &creds_secret_name, &replica).await?;

	// 3. Ensure stable Service exists
	ensure_service(client, &namespace, &name, &replica).await?;

	// 4. Update service name in status
	update_replica_status_field(client, &namespace, &name, "serviceName", &name).await?;

	// 5. Update connection info in status
	let conn_info = ConnectionInfo {
		host: format!("{name}.{namespace}.svc.cluster.local"),
		port: 5432,
		database: "postgres".to_string(),
		username: replica.spec.analytics_username.clone(),
		password_secret: creds_secret_name.clone(),
	};
	update_replica_connection_info(client, &namespace, &name, &conn_info).await?;

	// 5b. Reconcile overlay database if configured
	if replica.spec.overlay_database.is_some() {
		// Try to get snapshot size from active restore
		let restores_api: Api<PostgresPhysicalRestore> =
			Api::namespaced(client.clone(), &namespace);
		let snapshot_size = if let Some(current_restore_name) = replica
			.status
			.as_ref()
			.and_then(|s| s.current_restore.as_ref())
		{
			if let Ok(current_restore) = restores_api.get(current_restore_name).await {
				current_restore.spec.snapshot_size.clone()
			} else {
				"0".to_string()
			}
		} else {
			"0".to_string()
		};

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
	}

	// 6. Check child PostgresPhysicalRestore resources
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

	// 7. Handle switchover: if a restore is in Switching phase, update Service selector
	if let Some(switching) = switching_restore {
		let switching_name = switching.name_any();
		info!(
			replica = name,
			restore = switching_name,
			"performing blue-green switchover"
		);

		// Update Service selector to point to the new restore
		update_service_selector(client, &namespace, &name, &switching_name).await?;

		// Transition the switching restore to Active
		update_restore_phase(client, &namespace, &switching_name, RestorePhase::Active).await?;
		let now = Utc::now().to_rfc3339();
		update_restore_activated_at(client, &namespace, &switching_name, &now).await?;

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
				"lastRestoreCompletedAt": now,
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

		// Trigger FDW setup in overlay database if configured
		if replica.spec.overlay_database.is_some() {
			let pg_version = replica
				.status
				.as_ref()
				.and_then(|s| s.overlay_postgres_version.clone())
				.unwrap_or_else(|| "17".to_string());
			let old_restore = replica
				.status
				.as_ref()
				.and_then(|s| s.overlay_fdw_restore.as_deref());

			match overlay::run_fdw_setup(
				client,
				&namespace,
				&replica,
				&switching_name,
				&pg_version,
				old_restore,
			)
			.await
			{
				Ok(()) => {
					info!(
						replica = name,
						restore = switching_name,
						"FDW setup job created for overlay switchover"
					);
					let patch = serde_json::json!({
						"status": {
							"overlayFdwRestore": switching_name,
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
				Err(e) => {
					warn!(
						replica = name,
						restore = switching_name,
						error = %e,
						"failed to create FDW setup job for overlay"
					);
				}
			}
		}

		// Send notifications
		send_restore_notifications(
			client,
			&ctx.http_client,
			&namespace,
			&replica,
			switching,
			&conn_info,
			&creds_secret_name,
			&ctx.metrics,
		)
		.await;

		// Recompute next scheduled restore in case schedule changed while restore was in flight
		if let Some(schedule) = &replica.spec.schedule
			&& let Some(next) = compute_next_scheduled_restore(schedule)
		{
			update_replica_status_field(
				client,
				&namespace,
				&name,
				"nextScheduledRestore",
				&next.to_rfc3339(),
			)
			.await?;
		}

		return Ok(Action::requeue(Duration::from_secs(10)));
	}

	// 8. Clean up old restores after grace period
	// Safety check: if the previous restore's schemas are still imported in the overlay,
	// they should have been swapped out by the switchover step above.
	if replica.spec.overlay_database.is_some()
		&& let (Some(prev_name), Some(fdw_restore)) = (
			replica
				.status
				.as_ref()
				.and_then(|s| s.previous_restore.as_ref()),
			replica
				.status
				.as_ref()
				.and_then(|s| s.overlay_fdw_restore.as_ref()),
		) && prev_name == fdw_restore
	{
		warn!(
			replica = name,
			previous_restore = prev_name,
			fdw_restore = fdw_restore,
			"previous restore still has FDW schemas imported — switchover may not have completed FDW swap"
		);
	}

	if let Some(prev_name) = replica
		.status
		.as_ref()
		.and_then(|s| s.previous_restore.clone())
	{
		let grace_period = parse_duration(&replica.spec.switchover_grace_period)
			.unwrap_or(Duration::from_secs(300));
		let last_completed = replica
			.status
			.as_ref()
			.and_then(|s| s.last_restore_completed_at.as_ref())
			.and_then(|t| t.parse::<chrono::DateTime<Utc>>().ok());

		if let Some(completed_at) = last_completed {
			let elapsed = Utc::now().signed_duration_since(completed_at);
			if elapsed.to_std().unwrap_or_default() > grace_period {
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

	// 8b. Clean up failed restores (ownerReferences will cascade-delete their PVCs)
	let failed_restores: Vec<_> = restore_list
		.items
		.iter()
		.filter(|r| r.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&RestorePhase::Failed))
		.collect();
	for failed in &failed_restores {
		let failed_name = failed.name_any();
		let created_at = failed
			.status
			.as_ref()
			.and_then(|s| s.created_at.as_ref())
			.and_then(|t| t.parse::<chrono::DateTime<Utc>>().ok());
		let age = created_at
			.map(|t| Utc::now().signed_duration_since(t))
			.and_then(|d| d.to_std().ok())
			.unwrap_or_default();
		if age > Duration::from_secs(300) {
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

	// 9. Decide whether to trigger a new restore
	if let Some(in_progress) = in_progress_restore {
		let phase = in_progress.status.as_ref().and_then(|s| s.phase.as_ref());
		debug!(
			replica = name,
			restore = in_progress.name_any(),
			phase = ?phase,
			"restore already in progress, waiting"
		);
		update_replica_phase(client, &namespace, &name, ReplicaPhase::Restoring).await?;
		return Ok(Action::requeue(Duration::from_secs(30)));
	}

	let never_restored = active_restore.is_none()
		&& replica
			.status
			.as_ref()
			.and_then(|s| s.last_restore_completed_at.as_ref())
			.is_none();

	if never_restored {
		info!(
			replica = name,
			"no successful restore yet, triggering immediately"
		);
	}

	let should_restore = never_restored || should_trigger_scheduled_restore(&replica);

	if should_restore {
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

		// 10. Snapshot discovery via Job
		let snapshot_job_name = format!("{name}-snapshot-list");
		let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);

		match jobs.get_opt(&snapshot_job_name).await? {
			None => {
				info!(replica = name, "creating snapshot list job");
				let job = build_snapshot_list_job(&replica, &snapshot_job_name, &namespace)?;
				jobs.create(&PostParams::default(), &job).await?;
				return Ok(Action::requeue(Duration::from_secs(10)));
			}
			Some(job) => {
				let succeeded = job.status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);
				let failed = job.status.as_ref().and_then(|s| s.failed).unwrap_or(0);
				let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(2);

				if succeeded > 0 {
					let msg = read_job_termination_message(
						client,
						&namespace,
						&snapshot_job_name,
						"snapshot-list",
					)
					.await;

					let Some(ref raw) = msg else {
						// Termination message not yet available (pod status
						// may not have propagated). Retry shortly instead of
						// deleting the job and losing the result.
						info!(
							replica = name,
							job = snapshot_job_name,
							"snapshot list job succeeded but termination message not yet available, retrying"
						);
						return Ok(Action::requeue(Duration::from_secs(5)));
					};

					// We have the message — safe to delete the job now.
					if let Err(e) = jobs.delete(&snapshot_job_name, &Default::default()).await {
						warn!(job = snapshot_job_name, error = %e, "failed to delete snapshot list job");
					}

					// "{}" means no matching snapshots were found
					if raw == "{}" {
						warn!(
							replica = name,
							"snapshot list job returned no matching snapshots"
						);
					} else if let Ok(snap) = serde_json::from_str::<SnapshotInfo>(raw) {
						update_replica_status_field(
							client,
							&namespace,
							&name,
							"latestAvailableSnapshot",
							&snap.id,
						)
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
								size = snap.size,
								"new snapshot available, creating restore"
							);
							create_restore_for_snapshot(client, &namespace, &replica, &snap)
								.await?;
							ctx.metrics.restores_started_total.inc();
						}
					} else {
						warn!(
							replica = name,
							raw = raw,
							"failed to parse snapshot list job output"
						);
					}
				} else if failed > backoff_limit {
					warn!(replica = name, "snapshot list job failed");
					if let Err(e) = jobs.delete(&snapshot_job_name, &Default::default()).await {
						warn!(job = snapshot_job_name, error = %e, "failed to delete snapshot list job");
					}
				} else {
					return Ok(Action::requeue(Duration::from_secs(10)));
				}
			}
		}

		// Whether a restore was created or skipped, advance nextScheduledRestore
		// so we don't re-trigger on the next reconciliation.
		if let Some(schedule) = &replica.spec.schedule
			&& let Some(next) = compute_next_scheduled_restore(schedule)
		{
			update_replica_status_field(
				client,
				&namespace,
				&name,
				"nextScheduledRestore",
				&next.to_rfc3339(),
			)
			.await?;
		}
	}

	// Update phase based on current state
	if active_restore.is_some() {
		update_replica_phase(client, &namespace, &name, ReplicaPhase::Ready).await?;
	} else {
		update_replica_phase(client, &namespace, &name, ReplicaPhase::Pending).await?;
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
