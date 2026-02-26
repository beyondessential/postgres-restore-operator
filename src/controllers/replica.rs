use std::{collections::HashSet, sync::Arc, time::Duration};

use jiff::{SignedDuration, Timestamp};
use k8s_openapi::{
	api::{
		batch::v1::Job,
		core::v1::{Pod, Secret},
	},
	apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::Time},
};
use kube::{
	Api, Client, Resource, ResourceExt,
	api::{Patch, PatchParams, PostParams},
	runtime::{
		controller::Action,
		events::{Event, EventType},
	},
};
use kube_quantity::ParsedQuantity;
use rand::RngExt;
use rust_decimal::Decimal;
use tracing::{debug, error, info, warn};

use super::{
	jobs::{JobStatus, classify_job},
	overlay,
	overlay::common::DEFAULT_PG_VERSION,
};
use crate::{
	context::Context,
	error::{Error, Result},
	kopia,
	types::*,
};
use scheduling::ScheduleDecision;

mod resources;
mod scheduling;
mod schema_migration;
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
	ctx.last_reconcile
		.store(now.as_second(), std::sync::atomic::Ordering::Relaxed);

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

	// Validate mutual exclusivity: overlay_database and persistent_schemas cannot both be set
	if replica.spec.overlay_database.is_some() && replica.spec.persistent_schemas.is_some() {
		warn!(
			replica = name,
			"invalid configuration: both overlay_database and persistent_schemas are set"
		);

		replica
			.update_condition(
				client,
				"ConfigValid",
				"False",
				"ConfigConflict",
				"Cannot configure both overlay_database and persistent_schemas - they are mutually exclusive. \
				 Choose one: overlay_database for FDW/Copy strategies, or persistent_schemas for schema migration.",
			)
			.await?;

		// Skip reconciliation - return with requeue to allow user to fix
		return Ok(Action::requeue(Duration::from_secs(300)));
	}

	// Clear any previous ConfigValid=False condition if config is now valid
	if replica
		.status
		.as_ref()
		.and_then(|s| s.conditions.iter().find(|c| c.type_ == "ConfigValid"))
		.is_some_and(|c| c.status == "False")
	{
		replica
			.update_condition(
				client,
				"ConfigValid",
				"True",
				"ConfigValid",
				"Configuration is valid",
			)
			.await?;
	}

	replica.ensure_credentials_secret(client).await?;

	replica.ensure_service(client).await?;

	replica
		.update_status_field(client, "serviceName", &name)
		.await?;

	replica.update_connection_info(client).await?;

	// Reconcile overlay database if configured, but only after the first
	// restore exists so we know the real snapshot size.
	let mut overlay_cluster_ready = false;
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

					overlay_cluster_ready = cluster_ready;
					if !cluster_ready {
						info!(replica = name, "overlay cluster not yet ready, will retry");
						replica
							.update_condition(
								client,
								"OverlayReady",
								"False",
								"ClusterNotReady",
								"Overlay CNPG cluster is not yet ready",
							)
							.await?;
					}
				}
				Err(e) => {
					warn!(replica = name, error = %e, "failed to reconcile overlay database");
					replica
						.update_condition(
							client,
							"OverlayReady",
							"False",
							"ProvisioningFailed",
							&format!("Failed to reconcile overlay cluster: {e}"),
						)
						.await?;
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

	// Handle schema migration for persistent_schemas configuration
	if replica.spec.persistent_schemas.is_some()
		&& let Some(switching) = switching_restore
	{
		let migration_complete = reconcile_schema_migration(
			client,
			&ctx,
			&replica,
			&namespace,
			switching,
			active_restore,
		)
		.await?;

		if !migration_complete {
			return Ok(Action::requeue(Duration::from_secs(30)));
		}
	}

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
				"consecutiveRestoreFailures": 0,
			}
		});
		replicas
			.patch_status(
				&name,
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;

		// Clear the scheduling-suspended condition if it was set
		if replica
			.status
			.as_ref()
			.and_then(|s| {
				s.conditions
					.iter()
					.find(|c| c.type_ == "RestoreSchedulingSuspended")
			})
			.is_some_and(|c| c.status == "True")
		{
			replica
				.update_condition(
					client,
					"RestoreSchedulingSuspended",
					"False",
					"RestoreSucceeded",
					"Consecutive failure counter reset after successful restore",
				)
				.await?;
		}

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
	if let Some(overlay_config) = replica
		.spec
		.overlay_database
		.as_ref()
		.filter(|_| overlay_cluster_ready)
	{
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
						&ctx,
						&namespace,
						&replica,
						current,
						ctx.use_port_forward(),
					)
					.await
				}
			};

			match result {
				Ok(true) => {
					replica
						.update_condition(
							client,
							"OverlayReady",
							"True",
							"Reconciled",
							"Overlay database is reconciled and ready",
						)
						.await?;
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
				Ok(false) => {
					debug!(
						replica = name,
						restore = current,
						strategy = ?overlay_config.strategy,
						"overlay reconciliation in progress"
					);
					replica
						.update_condition(
							client,
							"OverlayReady",
							"False",
							"ReconciliationInProgress",
							"Overlay database reconciliation is in progress",
						)
						.await?;
					return Ok(Action::requeue(Duration::from_secs(30)));
				}
				Err(Error::InvalidOverlayConfig(msg)) => {
					warn!(
						replica = name,
						restore = current,
						strategy = ?overlay_config.strategy,
						error = msg,
						"overlay reconciliation permanently failed, continuing with scheduling"
					);
					replica
						.update_condition(
							client,
							"OverlayReady",
							"False",
							"ConfigInvalid",
							&format!("Overlay configuration is invalid: {msg}"),
						)
						.await?;
				}
				Err(e) => {
					warn!(
						replica = name,
						restore = current,
						strategy = ?overlay_config.strategy,
						error = ?e,
						"overlay reconciliation failed, will retry"
					);
					replica
						.update_condition(
							client,
							"OverlayReady",
							"False",
							"ReconciliationFailed",
							&format!("Overlay reconciliation failed: {e}"),
						)
						.await?;
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
		// For persistent_schemas: Don't start grace period countdown until migration completes
		let migration_complete = if replica.spec.persistent_schemas.is_some() {
			replica
				.status
				.as_ref()
				.and_then(|s| s.schema_migration_phase.as_ref())
				.is_some_and(|p| p == "complete")
		} else {
			true // For overlay strategies or no persistence config, always allow grace period
		};

		if migration_complete {
			let grace_period = SignedDuration::try_from(replica.spec.switchover_grace_period.0)
				.unwrap_or_default();
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
							"schemaMigrationJob": null,
							"schemaMigrationPhase": null,
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
	}

	// Clean up failed restores.
	// We explicitly delete associated pods before deleting the restore CR
	// because Kubernetes cascade deletion can leave pods orphaned, and the
	// pvc-protection finalizer blocks PVC deletion while any pod still
	// references the PVC.
	let failed_restores: Vec<_> = restore_list
		.items
		.iter()
		.filter(|r| r.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&RestorePhase::Failed))
		.collect();
	let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
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

				// Delete pods by label first so PVC protection finalizers can resolve
				if let Ok(pod_list) = pods
					.list(
						&kube::api::ListParams::default()
							.labels(&format!("pgro.bes.au/restore={failed_name}")),
					)
					.await
				{
					for pod in &pod_list.items {
						let pod_name = pod.metadata.name.as_deref().unwrap_or("");
						if let Err(e) = pods.delete(pod_name, &Default::default()).await {
							warn!(pod = pod_name, error = %e, "failed to delete pod for failed restore");
						}
					}
				}

				if let Err(e) = restores.delete(&failed_name, &Default::default()).await {
					warn!(restore = failed_name, error = %e, "failed to delete failed restore");
				}
			}
		}
	}

	// Sweep for orphaned pods: pods with a pgro.bes.au/replica label but no
	// ownerReferences. These can be left behind when cascade deletion from a
	// restore CR (or the replica CR for snapshot-list jobs) fails to
	// propagate to the Job's pods.
	let known_restores: HashSet<String> = restore_list.items.iter().map(|r| r.name_any()).collect();
	if let Ok(all_replica_pods) = pods
		.list(&kube::api::ListParams::default().labels(&format!("pgro.bes.au/replica={name}")))
		.await
	{
		for pod in &all_replica_pods.items {
			let has_owner = pod
				.metadata
				.owner_references
				.as_ref()
				.is_some_and(|refs| !refs.is_empty());
			if has_owner {
				continue;
			}
			let labels = pod.metadata.labels.as_ref();
			let restore_label = labels.and_then(|l| l.get("pgro.bes.au/restore"));
			let job_type_label = labels.and_then(|l| l.get("pgro.bes.au/job-type"));

			// Skip pods whose restore CR still exists
			if let Some(restore_name) = restore_label
				&& known_restores.contains(restore_name)
			{
				continue;
			}

			let pod_name = pod.metadata.name.as_deref().unwrap_or("");
			let reason = if let Some(restore_name) = restore_label {
				format!("restore {restore_name} no longer exists")
			} else if let Some(job_type) = job_type_label {
				format!("orphaned {job_type} pod")
			} else {
				"orphaned pod with no restore or job-type label".to_string()
			};
			info!(pod = pod_name, reason, "deleting orphaned pod");
			if let Err(e) = pods.delete(pod_name, &Default::default()).await {
				warn!(pod = pod_name, error = %e, "failed to delete orphaned pod");
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
		match classify_job(job) {
			JobStatus::Succeeded => {
				let raw = ctx.snapshot_results.take(&namespace, &name);

				let Some(ref raw) = raw else {
					let completion_time =
						job.status.as_ref().and_then(|s| s.completion_time.as_ref());
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
							replica
								.update_condition(
									client,
									"SnapshotAvailable",
									"True",
									"SnapshotFound",
									&format!("Snapshot {} available ({size} bytes)", snap.id),
								)
								.await?;

							let current_snapshot_id =
								active_restore.map(|r| r.spec.snapshot.as_str());

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
									start_time: snap.start_time.clone(),
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
							replica
								.update_condition(
									client,
									"SnapshotAvailable",
									"False",
									"NoMatchingSnapshots",
									"Snapshot list job returned no snapshots matching the configured filter",
								)
								.await?;
						}
					}
					Err(e) => {
						warn!(
							replica = name,
							error = %e,
							"failed to parse snapshot list job output"
						);
						replica
							.update_condition(
								client,
								"SnapshotAvailable",
								"False",
								"ParseError",
								&format!("Failed to parse snapshot list output: {e}"),
							)
							.await?;
					}
				}
			}
			JobStatus::Failed => {
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
						&PatchParams::apply("postgres-restore-operator"),
						&Patch::Merge(&ttl_patch),
					)
					.await
				{
					warn!(job = snapshot_job_name, error = %e, "failed to extend TTL on failed snapshot list job");
				}
				replica
					.update_condition(
						client,
						"SnapshotAvailable",
						"False",
						"JobFailed",
						"Snapshot list job failed, check job logs for details",
					)
					.await?;
			}
			JobStatus::Active => {
				return Ok(Action::requeue(Duration::from_secs(10)));
			}
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

	let consecutive_failures = replica
		.status
		.as_ref()
		.and_then(|s| s.consecutive_restore_failures)
		.unwrap_or(0);

	const MAX_CONSECUTIVE_FAILURES: u32 = 3;
	if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
		// Update phase even while suspended so it doesn't stay stuck at
		// "Restoring" after all in-progress restores have failed.
		if active_restore.is_some() {
			replica.update_phase(client, ReplicaPhase::Ready).await?;
		} else {
			replica.update_phase(client, ReplicaPhase::Pending).await?;
		}

		let already_suspended = replica
			.status
			.as_ref()
			.and_then(|s| {
				s.conditions
					.iter()
					.find(|c| c.type_ == "RestoreSchedulingSuspended")
			})
			.is_some_and(|c| c.status == "True");

		if !already_suspended {
			warn!(
				replica = name,
				consecutive_failures,
				"restore scheduling suspended after {MAX_CONSECUTIVE_FAILURES} consecutive failures"
			);
			replica
				.update_condition(
					client,
					"RestoreSchedulingSuspended",
					"True",
					"ConsecutiveFailures",
					&format!(
						"Scheduling suspended after {consecutive_failures} consecutive restore failures. \
						 Fix the underlying issue and reset with: kubectl patch postgresphysicalreplica {name} \
						 -n {namespace} --subresource=status --type=merge \
						 -p '{{\"status\":{{\"consecutiveRestoreFailures\":0}}}}'"
					),
				)
				.await?;
		}
		return Ok(Action::requeue(Duration::from_secs(300)));
	}

	// Clear RestoreSchedulingSuspended if it was previously set but the
	// counter has since been reset below the threshold (e.g. manual patch).
	if replica
		.status
		.as_ref()
		.and_then(|s| {
			s.conditions
				.iter()
				.find(|c| c.type_ == "RestoreSchedulingSuspended")
		})
		.is_some_and(|c| c.status == "True")
	{
		info!(
			replica = name,
			consecutive_failures,
			"clearing RestoreSchedulingSuspended condition (counter below threshold)"
		);
		replica
			.update_condition(
				client,
				"RestoreSchedulingSuspended",
				"False",
				"CounterReset",
				"Consecutive failure counter reset below threshold",
			)
			.await?;
	}

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

/// Reconcile schema migration for persistent_schemas configuration.
///
/// This function manages the migration of specified schemas from the previous
/// restore to the new restore during switchover. It creates and monitors a
/// Kubernetes Job that runs pg_dump|psql to transfer the schemas.
///
/// Returns `Ok(true)` when migration is complete (or skipped), `Ok(false)` when
/// in progress, or `Err` on permanent failure.
async fn reconcile_schema_migration(
	client: &Client,
	ctx: &Arc<Context>,
	replica: &PostgresPhysicalReplica,
	namespace: &str,
	new_restore: &PostgresPhysicalRestore,
	old_restore_opt: Option<&PostgresPhysicalRestore>,
) -> Result<bool> {
	let replica_name = replica.name_any();
	let new_restore_name = new_restore.name_any();

	let schemas =
		replica.spec.persistent_schemas.as_ref().ok_or_else(|| {
			Error::InvalidOverlayConfig("missing persistent_schemas config".into())
		})?;

	// Edge case: First restore, no previous restore to migrate from
	let old_restore = match old_restore_opt {
		Some(r) => r,
		None => {
			info!(replica = %replica_name, "first restore, skipping schema migration");
			return Ok(true); // Allow switchover to proceed
		}
	};

	let old_restore_name = old_restore.name_any();

	// Edge case: No persistent schemas configured
	if schemas.is_empty() {
		info!(replica = %replica_name, "no persistent schemas configured, skipping migration");
		return Ok(true);
	}

	// Check if migration already complete in status
	if replica
		.status
		.as_ref()
		.and_then(|s| s.schema_migration_phase.as_ref())
		.is_some_and(|p| p == "complete")
	{
		debug!(replica = %replica_name, "migration already complete in status");
		return Ok(true);
	}

	// Check if migration Job exists
	let job_name = schema_migration::migration_job_name(&replica_name);
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

	if let Some(job) = jobs.get_opt(&job_name).await? {
		match classify_job(&job) {
			JobStatus::Active => {
				debug!(replica = %replica_name, job = %job_name, "migration Job still running");
				return Ok(false);
			}
			JobStatus::Succeeded => {
				info!(replica = %replica_name, "migration Job succeeded");

				// Update status
				let replicas: Api<PostgresPhysicalReplica> =
					Api::namespaced(client.clone(), namespace);
				let patch = serde_json::json!({
					"status": {
						"schemaMigrationJob": null,
						"schemaMigrationPhase": "complete",
					}
				});
				replicas
					.patch_status(
						&replica_name,
						&PatchParams::apply("postgres-restore-operator"),
						&Patch::Merge(&patch),
					)
					.await?;

				// Clean up Job
				if let Err(e) = jobs.delete(&job_name, &Default::default()).await {
					warn!(job = %job_name, error = %e, "failed to delete completed migration Job");
				}

				return Ok(true);
			}
			JobStatus::Failed => {
				let last_error = ctx
					.schema_migration_results
					.take(namespace, &replica_name)
					.unwrap_or_else(|| "no callback received".to_string());

				warn!(
					replica = %replica_name,
					source = %old_restore_name,
					target = %new_restore_name,
					error = %last_error,
					"migration Job failed"
				);

				// Delete failed Job
				if let Err(e) = jobs.delete(&job_name, &Default::default()).await {
					warn!(job = %job_name, error = %e, "failed to delete failed Job");
				}

				// Update status with error
				let replicas: Api<PostgresPhysicalReplica> =
					Api::namespaced(client.clone(), namespace);
				let patch = serde_json::json!({
					"status": {
						"schemaMigrationJob": null,
						"schemaMigrationPhase": format!("failed: {}", last_error),
					}
				});
				replicas
					.patch_status(
						&replica_name,
						&PatchParams::apply("postgres-restore-operator"),
						&Patch::Merge(&patch),
					)
					.await?;

				// Strategy: Keep old restore active, fail the new restore
				return Err(Error::InvalidOverlayConfig(format!(
					"schema migration failed: {}",
					last_error
				)));
			}
		}
	}

	// No Job exists, need to create it
	info!(
		replica = %replica_name,
		source = %old_restore_name,
		target = %new_restore_name,
		schemas = ?schemas,
		"creating schema migration Job"
	);

	// Discover database names
	let reader_secret_name = replica.creds_secret_name();
	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
	let reader_secret = secrets.get(&reader_secret_name).await?;
	let reader_user = overlay::connect::read_secret_field(&reader_secret, "username")?;
	let reader_password = overlay::connect::read_secret_field(&reader_secret, "password")?;

	let source_dbname = overlay::common::discover_restore_database(
		client,
		namespace,
		&old_restore_name,
		&reader_user,
		&reader_password,
		ctx.use_port_forward(),
	)
	.await?;

	// Measure the actual on-disk database size of the source restore and
	// compute how much the persistent schemas have grown beyond the original
	// snapshot.  This delta is stored in the replica status so the next
	// restore PVC can be sized accordingly.
	let db_size_bytes = overlay::common::measure_database_size(
		client,
		namespace,
		&old_restore_name,
		&source_dbname,
		&reader_user,
		&reader_password,
		ctx.use_port_forward(),
	)
	.await?;

	let snapshot_size_bytes: ParsedQuantity = old_restore
		.spec
		.snapshot_size
		.clone()
		.try_into()
		.unwrap_or_else(|_| ParsedQuantity::from(Decimal::ZERO));
	let snapshot_bytes_f64 = snapshot_size_bytes.to_bytes_f64().unwrap_or(0.0);
	let schema_data_bytes = (db_size_bytes as f64 - snapshot_bytes_f64).max(0.0) as u64;

	info!(
		replica = %replica_name,
		db_size_bytes,
		snapshot_bytes = %snapshot_bytes_f64,
		schema_data_bytes,
		"measured persistent schema data delta"
	);

	// Store the measured size in the replica status
	let schema_data_quantity = Quantity(format!("{schema_data_bytes}"));
	let replicas_for_size: Api<PostgresPhysicalReplica> =
		Api::namespaced(client.clone(), namespace);
	let size_patch = serde_json::json!({
		"status": {
			"persistentSchemaDataSize": schema_data_quantity,
		}
	});
	replicas_for_size
		.patch_status(
			&replica_name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&size_patch),
		)
		.await?;

	let target_dbname = overlay::common::discover_restore_database(
		client,
		namespace,
		&new_restore_name,
		&reader_user,
		&reader_password,
		ctx.use_port_forward(),
	)
	.await?;

	// Check that none of the persistent schemas already exist in the snapshot.
	// If they do, the pg_dump|psql migration would conflict, so we must fail
	// the restore instead of attempting migration.
	let conflicting = check_snapshot_schema_conflicts(
		client,
		namespace,
		&new_restore_name,
		&target_dbname,
		&reader_user,
		&reader_password,
		schemas,
		ctx.use_port_forward(),
	)
	.await?;

	if !conflicting.is_empty() {
		let msg = format!(
			"persistent schemas already present in snapshot: {}",
			conflicting.join(", ")
		);
		error!(
			replica = %replica_name,
			restore = %new_restore_name,
			conflicting_schemas = ?conflicting,
			"failing restore: persistent schemas found in snapshot"
		);

		// Mark the new restore as Failed
		new_restore
			.update_phase(client, RestorePhase::Failed)
			.await?;

		// Increment consecutiveRestoreFailures on the replica
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
		let current_failures = replica
			.status
			.as_ref()
			.and_then(|s| s.consecutive_restore_failures)
			.unwrap_or(0);
		let patch = serde_json::json!({
			"status": {
				"consecutiveRestoreFailures": current_failures + 1,
				"schemaMigrationPhase": null,
				"schemaMigrationJob": null,
			}
		});
		replicas
			.patch_status(
				&replica_name,
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;

		ctx.metrics.restores_failed_total.inc();

		if let Err(e) = ctx
			.recorder
			.publish(
				&Event {
					type_: EventType::Warning,
					reason: "RestoreFailed".into(),
					note: Some(format!("Restore {new_restore_name} failed: {msg}")),
					action: "Restore".into(),
					secondary: Some(new_restore.object_ref(&())),
				},
				&replica.object_ref(&()),
			)
			.await
		{
			warn!(replica = %replica_name, error = %e, "failed to publish RestoreFailed event");
		}

		return Err(Error::InvalidOverlayConfig(msg));
	}

	// The analytics user already has pg_write_all_data + CREATE ON DATABASE
	// when persistent_schemas is configured (read_only is effectively false),
	// so we reuse the replica creds secret for write access to the target.
	let target_superuser_secret_name = reader_secret_name.clone();

	let pg_version = new_restore
		.status
		.as_ref()
		.and_then(|s| s.postgres_version.as_ref())
		.and_then(|v| v.split('.').next())
		.and_then(|v| v.parse::<i32>().ok())
		.unwrap_or(DEFAULT_PG_VERSION);

	let callback_url = ctx.schema_migration_callback_url(namespace, &replica_name);

	let job = schema_migration::build_schema_migration_job(
		replica,
		namespace,
		&old_restore_name,
		&new_restore_name,
		&source_dbname,
		&target_dbname,
		schemas,
		&reader_secret_name,
		&target_superuser_secret_name,
		&callback_url,
		pg_version,
	);

	jobs.create(&PostParams::default(), &job).await?;

	// Update status
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"status": {
			"schemaMigrationJob": job_name,
			"schemaMigrationPhase": "active",
		}
	});
	replicas
		.patch_status(
			&replica_name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;

	Ok(false) // Job created, not complete yet
}

/// Check whether any of the persistent schemas already exist in the snapshot
/// (i.e. in the new restore database before migration). Returns the list of
/// schema names that were found.
#[expect(
	clippy::too_many_arguments,
	reason = "internal helper with tightly-coupled params"
)]
async fn check_snapshot_schema_conflicts(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
	schemas: &[String],
	use_port_forward: bool,
) -> Result<Vec<String>> {
	let conn = overlay::connect::connect_to_restore(
		client,
		namespace,
		restore_name,
		dbname,
		user,
		password,
		use_port_forward,
	)
	.await?;

	let rows = conn
		.client
		.query(
			"SELECT schema_name FROM information_schema.schemata \
			 WHERE schema_name = ANY($1)",
			&[&schemas],
		)
		.await?;

	let found: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
	Ok(found)
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
