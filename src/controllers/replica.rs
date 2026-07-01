use std::{collections::HashSet, sync::Arc, time::Duration};

use jiff::{SignedDuration, Timestamp};
use k8s_openapi::{
	api::{
		apps::v1::Deployment,
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
use tracing::{debug, info, warn};

use super::{
	jobs::{JobStatus, classify_job},
	postgres,
	postgres::DEFAULT_PG_VERSION,
	restore::{build_credential_reset_job, credential_reset_job_name},
};
use crate::{
	context::Context,
	error::{Error, Result},
	kopia,
	types::*,
};
use scheduling::ScheduleDecision;

mod resources;
pub(super) mod scheduling;
mod schema_migration;
mod status;

#[cfg(test)]
mod tests;

use resources::*;

/// True when no schema migration is currently in flight for this replica
/// — i.e. the sweep can safely delete the previous Active restore without
/// pulling the rug out from under a running `pg_dump | psql`.
///
/// Returns `true` when `persistent_schemas` is unset (no migration ever
/// runs) or when `schemaMigrationPhase` is anything other than `active`.
/// Coded as "not in flight" rather than enumerating terminal phases so
/// future additions (the operator's set of phase labels has grown over
/// time: `complete`, `partial`, `failed: <reason>`, `timeout-skipped`)
/// don't silently block the sweep.
pub fn persistent_schemas_migration_settled(replica: &PostgresPhysicalReplica) -> bool {
	if replica.spec.persistent_schemas.is_none() {
		return true;
	}
	replica
		.status
		.as_ref()
		.and_then(|s| s.schema_migration_phase.as_ref())
		.is_none_or(SchemaMigrationPhase::is_settled)
}

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

	// Validate the credential source: exactly one of kopiaSecretRef or
	// canopySource must be set. Legacy replicas take the Secret path;
	// canopy-managed ones take the proxy-sidecar path and get their
	// creds via the operator's in-cluster broker at Job time.
	match (&replica.spec.kopia_secret_ref, &replica.spec.canopy_source) {
		(Some(_), Some(_)) => {
			warn!(
				replica = name,
				"kopiaSecretRef and canopySource are mutually exclusive"
			);
			replica
				.update_condition(
					client,
					"KopiaSecretValid",
					"False",
					"SecretRefAndCanopySource",
					"kopiaSecretRef and canopySource are mutually exclusive; set exactly one",
				)
				.await?;
			return Ok(Action::requeue(Duration::from_secs(60)));
		}
		(None, None) => {
			warn!(
				replica = name,
				"neither kopiaSecretRef nor canopySource set"
			);
			replica
				.update_condition(
					client,
					"KopiaSecretValid",
					"False",
					"NoCredentialSource",
					"one of kopiaSecretRef or canopySource must be set",
				)
				.await?;
			return Ok(Action::requeue(Duration::from_secs(60)));
		}
		(Some(secret_ref), None) => {
			let secret_name = secret_ref.name.as_deref().unwrap_or_default();
			let secrets: Api<Secret> = Api::namespaced(client.clone(), &namespace);
			let secret = match secrets.get(secret_name).await {
				Ok(s) => s,
				Err(e) => {
					warn!(
						replica = name,
						secret = ?secret_ref,
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
			match kopia::validate_kopia_secret(&secret) {
				Ok(_) => {
					replica
						.update_condition(
							client,
							"KopiaSecretValid",
							"True",
							"SecretValid",
							"All required keys present",
						)
						.await?;
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
			}
		}
		(None, Some(_canopy_source)) => {
			// Nothing to pre-validate at this stage — the proxy sidecar
			// fetches short-lived creds at Job time via the operator's
			// broker; any auth failure surfaces there.
			replica
				.update_condition(
					client,
					"KopiaSecretValid",
					"True",
					"CanopySourced",
					"canopy-managed replica; credentials issued by canopy at Job time",
				)
				.await?;
		}
	}

	replica.ensure_credentials_secret(client).await?;

	replica.ensure_service(client).await?;

	replica
		.update_status_field(client, "serviceName", &name)
		.await?;

	replica.update_connection_info(client).await?;

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

		// Mark the new restore's pod ready for traffic BEFORE pointing the
		// Service at it. The Service selector requires the
		// `ready-for-traffic=true` label (see [[READY_FOR_TRAFFIC_LABEL]]),
		// so until the operator sets the label, no external client can
		// reach the restore via the Service — operator-side prep work
		// (DROP SCHEMA, migration Job) runs without external interference.
		switching.mark_pod_ready_for_traffic(client).await?;

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

		// Canopy verification (signal 3) — no-op unless the replica has
		// spec.canopy_source.
		crate::controllers::canopy::verification::report(
			&ctx,
			&replica,
			switching,
			bestool_canopy::Outcome::Success,
			None,
		)
		.await;

		return Ok(Action::requeue(Duration::from_secs(10)));
	}

	// Sweep stale Active restores after grace period.
	//
	// Any Active restore for this replica that isn't the current one is a
	// leftover from a prior cycle and should be deleted once the grace
	// period elapses. Sweep-based rather than tracking a single
	// `previousRestore` pointer means cleanup converges even if multiple
	// stale restores accumulate (e.g. from a bug or operator restart
	// during a switchover).
	let current_restore_name = replica
		.status
		.as_ref()
		.and_then(|s| s.current_restore.as_deref());
	if let Some(current) = current_restore_name {
		// Gate the sweep on "migration isn't actively in flight": the
		// gate's only purpose is to keep us from deleting the migration's
		// source restore while pg_dump | psql still has it open. Any
		// terminal phase (`complete`, `partial`, `timeout-skipped`,
		// `failed: …`) — and `None` (no migration has run since this
		// replica was first reconciled, or a previous sweep cleared the
		// field) — all mean "no migration in flight, sweep is safe".
		//
		// Coded as "not in flight" rather than enumerating every terminal
		// phase so future additions don't have to remember to update this
		// gate. Previously this was an allow-list of `complete`/`partial`,
		// which silently blocked the sweep when `timeout-skipped` was
		// added — the replica then accumulated stale Active restores,
		// hit the too-many-restores guardrail, and couldn't recover
		// automatically.
		let migration_complete = persistent_schemas_migration_settled(&replica);

		let grace_period =
			SignedDuration::try_from(replica.spec.switchover_grace_period.0).unwrap_or_default();
		let last_completed = replica
			.status
			.as_ref()
			.and_then(|s| s.last_restore_completed_at.as_ref());

		// Refuse to sweep if status.currentRestore doesn't match any live
		// Active restore — likely an inconsistent state where another path
		// (manual intervention, controller startup) should resolve it.
		// Filter on Active phase too so the sweep can't fire while the
		// "current" is mid-switchover, even if a future flow change lets
		// this block run with a non-Active current.
		let has_matching_current = restore_list.items.iter().any(|r| {
			r.name_any() == current
				&& r.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&RestorePhase::Active)
		});

		if migration_complete
			&& has_matching_current
			&& let Some(completed_at) = last_completed
			&& now.duration_since(completed_at.0) > grace_period
		{
			let stale: Vec<String> = restore_list
				.items
				.iter()
				.filter(|r| {
					r.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&RestorePhase::Active)
						&& r.name_any() != current
				})
				.map(|r| r.name_any())
				.collect();

			if !stale.is_empty() {
				info!(
					replica = name,
					count = stale.len(),
					current = current,
					"sweeping stale Active restores after grace period"
				);
				let mut all_deleted = true;
				for stale_name in &stale {
					if let Err(e) = restores.delete(stale_name, &Default::default()).await {
						warn!(restore = stale_name, error = %e, "failed to delete stale restore");
						all_deleted = false;
					}
				}

				// Only clear schemaMigrationPhase if every delete succeeded.
				// Clearing it while a stale restore survives would set
				// migration_complete=false on the next reconcile (when
				// persistent_schemas is configured), blocking the sweep
				// from retrying until the next switchover re-marks
				// complete. Leave the next reconcile to retry instead.
				if all_deleted {
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
				} else {
					warn!(
						replica = name,
						"some stale restores failed to delete; leaving schemaMigrationPhase set so next reconcile retries"
					);
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

	// Snapshot-list runs on both paths. Canopy-sourced replicas fetch the
	// same snapshot metadata but the result handler filters by
	// `status.canopyDesiredSnapshotId` instead of the CR's snapshot_filter.
	let is_canopy = replica.spec.canopy_source.is_some();
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
						// Canopy path: canopy already picked a specific
						// snapshot; select the metadata for that ID
						// instead of applying snapshot_filter + "latest".
						// Legacy path: filter by snapshot_filter, pick
						// latest.
						let picked = if is_canopy {
							let desired = replica
								.status
								.as_ref()
								.and_then(|s| s.canopy_desired_snapshot_id.as_deref());
							desired
								.and_then(|id| all_snapshots.iter().find(|s| s.id == id))
								.cloned()
						} else {
							let filtered = kopia::filter_snapshots(
								&all_snapshots,
								replica.spec.snapshot_filter.as_ref(),
							);
							kopia::latest_snapshot(&filtered).cloned()
						};

						if let Some(ref snap) = picked {
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
								let created =
									replica.create_restore_for_snapshot(client, &info).await?;
								if created {
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
								} else if let Err(e) = ctx
									.recorder
									.publish(
										&Event {
											type_: EventType::Warning,
											reason: "RestoreCreationBlocked".into(),
											note: Some(format!(
												"Refused to create restore for snapshot {} — \
												 too many restores already exist",
												snap.id
											)),
											action: "Restore".into(),
											secondary: None,
										},
										&replica.object_ref(&()),
									)
									.await
								{
									warn!(replica = name, error = %e, "failed to publish RestoreCreationBlocked event");
								}
							}
						} else if is_canopy {
							let desired = replica
								.status
								.as_ref()
								.and_then(|s| s.canopy_desired_snapshot_id.as_deref())
								.unwrap_or("<none>");
							warn!(
								replica = name,
								desired = desired,
								"canopy: canopyDesiredSnapshotId not present in kopia snapshot list"
							);
							replica
								.update_condition(
									client,
									"SnapshotAvailable",
									"False",
									"CanopyDesiredSnapshotMissing",
									&format!(
										"canopyDesiredSnapshotId {desired} was not found in the \
										 kopia snapshot list — canopy may be pointing at a \
										 snapshot that hasn't been indexed yet"
									),
								)
								.await?;
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

	// Clear the legacy RestoreSchedulingSuspended condition on existing
	// replicas if still True (left over from the suspension behaviour we
	// removed — see commit dropping MAX_CONSECUTIVE_FAILURES). The
	// operator no longer suspends; bounded backoff via
	// scheduling::failure_backoff_delay is the new rate limit, applied in
	// fail_restore when each failure increments the counter.
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
			"clearing legacy RestoreSchedulingSuspended condition (suspension removed)"
		);
		replica
			.update_condition(
				client,
				"RestoreSchedulingSuspended",
				"False",
				"SuspensionRemoved",
				"Operator no longer suspends on consecutive failures; backoff is used instead",
			)
			.await?;
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

	// On the canopy path, the worklist syncer updates
	// `status.canopyDesiredSnapshotId` when canopy offers a newer snapshot.
	// Trigger a restore whenever the desired snapshot differs from what the
	// active restore already carries, in addition to the usual schedule /
	// never-restored / active-deleted triggers. minimum_ttl still gates:
	// the intent may declare an explicit lower bound on restore frequency
	// even in the face of a newer canopy snapshot.
	let canopy_desired_changed = is_canopy
		&& match (
			replica
				.status
				.as_ref()
				.and_then(|s| s.canopy_desired_snapshot_id.as_ref()),
			active_restore.map(|r| r.spec.snapshot.as_str()),
		) {
			(Some(desired), Some(current)) => desired != current,
			(Some(_), None) => true,
			_ => false,
		} && !replica.within_minimum_ttl(now);

	let should_restore = never_restored
		|| active_restore_deleted
		|| canopy_desired_changed
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
		let stats_callback_url = ctx.canopy_stats_callback_url(&namespace, &snapshot_job_name);
		let canopy_proxy = if is_canopy {
			Some(crate::controllers::restore::builders::CanopyProxyArgs {
				image: &ctx.canopy_proxy_image,
				broker_base_url: &ctx.canopy_broker_base_url,
				stats_callback_url: &stats_callback_url,
			})
		} else {
			None
		};
		let job = build_snapshot_list_job(
			&replica,
			&snapshot_job_name,
			&namespace,
			&ctx.kopia_image(),
			&callback_url,
			canopy_proxy.as_ref(),
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
/// Returns true if a tokio_postgres error is an authentication failure.
/// We detect this by inspecting the SQLSTATE code (28P01 = invalid_password,
/// 28000 = invalid_authorization_specification) and the error message text as
/// a fallback for cases where the code is not surfaced.
fn is_auth_error(e: &Error) -> bool {
	let Error::Postgres(pg_err) = e else {
		return false;
	};
	if let Some(db_err) = pg_err.as_db_error() {
		let code = db_err.code();
		return code == &tokio_postgres::error::SqlState::INVALID_PASSWORD
			|| code == &tokio_postgres::error::SqlState::INVALID_AUTHORIZATION_SPECIFICATION;
	}
	// Fallback: match the error message text that Postgres emits for auth failures.
	let msg = pg_err.to_string().to_lowercase();
	msg.contains("password authentication failed") || msg.contains("no password assigned")
}

/// Ensure the credential-reset Job exists and has run for the given restore.
///
/// The sequence is:
///   1. Scale the restore deployment to 0 (so the PVC is free).
///   2. Create a Job that uses `postgres --single` to ALTER ROLE.
///   3. While the job is running, return `Ok(false)` (caller retries).
///   4. When the job succeeds, scale the deployment back to 1 and return
///      `Ok(true)` so the caller re-attempts the connection on the next loop.
///   5. If the job fails, log a warning and return `Ok(false)` — it will be
///      retried on the next reconcile once the job has been deleted.
///
/// Idempotent: safe to call repeatedly while the job is in flight.
async fn ensure_credential_reset(
	client: &Client,
	namespace: &str,
	restore: &PostgresPhysicalRestore,
	replica: &PostgresPhysicalReplica,
) -> Result<bool> {
	let restore_name = restore.name_any();
	let job_name = credential_reset_job_name(&restore_name);
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);

	// Check for an existing reset job first.
	if let Some(job) = jobs.get_opt(&job_name).await? {
		match classify_job(&job) {
			JobStatus::Active => {
				// Still running — nothing to do yet.
				return Ok(false);
			}
			JobStatus::Succeeded => {
				info!(
					restore = %restore_name,
					job = %job_name,
					"credential reset job succeeded, scaling deployment back up"
				);

				// Scale the deployment back to 1.
				let scale_patch = serde_json::json!({
					"spec": { "replicas": 1 }
				});
				if let Err(e) = deployments
					.patch(
						&restore_name,
						&PatchParams::apply("postgres-restore-operator"),
						&Patch::Merge(&scale_patch),
					)
					.await
				{
					warn!(restore = %restore_name, error = %e, "failed to scale deployment back up after credential reset");
				}

				// Delete the completed job.
				if let Err(e) = jobs.delete(&job_name, &Default::default()).await {
					warn!(job = %job_name, error = %e, "failed to delete completed credential reset job");
				}

				// Return true: credentials are fixed, caller should retry connection.
				return Ok(true);
			}
			JobStatus::Failed => {
				warn!(
					restore = %restore_name,
					job = %job_name,
					"credential reset job failed; will retry on next reconcile"
				);

				// Delete the failed job so we create a fresh one next time.
				if let Err(e) = jobs.delete(&job_name, &Default::default()).await {
					warn!(job = %job_name, error = %e, "failed to delete failed credential reset job");
				}

				// Scale back up so the restore remains accessible in the meantime.
				let scale_patch = serde_json::json!({
					"spec": { "replicas": 1 }
				});
				if let Err(e) = deployments
					.patch(
						&restore_name,
						&PatchParams::apply("postgres-restore-operator"),
						&Patch::Merge(&scale_patch),
					)
					.await
				{
					warn!(restore = %restore_name, error = %e, "failed to scale deployment back up after failed credential reset");
				}

				return Ok(false);
			}
		}
	}

	// No job exists yet. Scale down the deployment first so the PVC is free.
	info!(
		restore = %restore_name,
		"auth failure detected; scaling deployment to 0 for credential reset"
	);
	let scale_patch = serde_json::json!({
		"spec": { "replicas": 0 }
	});
	deployments
		.patch(
			&restore_name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&scale_patch),
		)
		.await?;

	// Wait for the pod to terminate so the PVC is released before we mount it
	// in the job. We check once; if it's not gone yet we'll be called again on
	// the next reconcile and will skip straight to the job-exists branch above.
	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let label = format!("pgro.bes.au/restore={restore_name}");
	let running_pods = pods
		.list(&kube::api::ListParams::default().labels(&label).limit(2))
		.await?;
	let any_running = running_pods.items.iter().any(|p| {
		p.status
			.as_ref()
			.and_then(|s| s.phase.as_deref())
			.is_some_and(|ph| ph == "Running" || ph == "Pending")
	});
	if any_running {
		info!(
			restore = %restore_name,
			"waiting for pod to terminate before creating credential reset job"
		);
		return Ok(false);
	}

	// Pod is gone — create the reset job.
	info!(
		restore = %restore_name,
		job = %job_name,
		"creating credential reset job"
	);
	let job = build_credential_reset_job(restore, replica, &job_name, namespace)?;
	jobs.create(&PostParams::default(), &job).await?;

	Ok(false)
}

/// Mark schema migration as complete in the replica status without running a
/// Job. Used by the early-return branches of `reconcile_schema_migration`
/// (first restore, empty config, all schemas missing on source) so that the
/// cleanup gate in `reconcile_replica` can fire — it requires
/// `schemaMigrationPhase == "complete"` whenever `persistent_schemas` is set
/// in spec.
async fn mark_schema_migration_complete(
	client: &Client,
	replica_name: &str,
	namespace: &str,
) -> Result<()> {
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"status": {
			"schemaMigrationJob": null,
			"schemaMigrationPhase": SchemaMigrationPhase::Complete,
		}
	});
	replicas
		.patch_status(
			replica_name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

/// True if the running schema-migration Job has been alive longer than the
/// replica's per-cycle migration budget (see
/// [`PostgresPhysicalReplica::schema_migration_timeout`]). Uses the Job's
/// `creationTimestamp` as the start; falls back to "not exceeded" if the
/// Job has no creation timestamp (which shouldn't happen in practice).
fn migration_exceeded_budget(replica: &PostgresPhysicalReplica, job: &Job) -> bool {
	let Some(created) = job.metadata.creation_timestamp.as_ref().map(|t| t.0) else {
		return false;
	};
	let elapsed = Timestamp::now().duration_since(created);
	elapsed > replica.schema_migration_timeout()
}

/// Abandon a stuck schema migration: drop the Job, DROP SCHEMA … CASCADE
/// the configured `persistent_schemas` on the new restore, record a
/// Warning event, and mark the migration phase as `timeout-skipped`.
/// Lets switchover proceed so the replica gets a usable (if
/// schema-less) database instead of being blocked indefinitely. The
/// next restore cycle re-attempts migration if the schemas reappear on
/// the source.
/// Connect to the new restore and `DROP SCHEMA … CASCADE` each given
/// schema. Separate function so the caller can wrap it in
/// `tokio::time::timeout`; the operation has no internal deadline and
/// can hang indefinitely if a backend on the target has a lock on the
/// schema's namespace.
async fn drop_persistent_schemas_on_target(
	client: &Client,
	ctx: &Arc<Context>,
	replica: &PostgresPhysicalReplica,
	namespace: &str,
	new_restore_name: &str,
	schemas: &[String],
) -> Result<()> {
	let reader_secret_name = replica.creds_secret_name();
	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
	let reader_secret = secrets.get(&reader_secret_name).await?;
	let reader_user = postgres::read_secret_field(&reader_secret, "username")?;
	let reader_password = postgres::read_secret_field(&reader_secret, "password")?;
	let target_dbname = postgres::discover_restore_database(
		client,
		namespace,
		new_restore_name,
		&reader_user,
		&reader_password,
		ctx.use_port_forward(),
	)
	.await?;
	let conn = postgres::connect_to_restore(
		client,
		namespace,
		new_restore_name,
		&target_dbname,
		&reader_user,
		&reader_password,
		ctx.use_port_forward(),
	)
	.await?;
	postgres::drop_schemas_on(&conn.client, schemas).await
}

async fn timeout_schema_migration(
	client: &Client,
	ctx: &Arc<Context>,
	replica: &PostgresPhysicalReplica,
	namespace: &str,
	new_restore: &PostgresPhysicalRestore,
	job_name: &str,
) -> Result<()> {
	let replica_name = replica.name_any();
	let new_restore_name = new_restore.name_any();
	let schemas: Vec<String> = replica.spec.persistent_schemas.clone().unwrap_or_default();

	warn!(
		replica = %replica_name,
		restore = %new_restore_name,
		schemas = ?schemas,
		timeout = ?replica.schema_migration_timeout(),
		"schema migration exceeded budget; dropping persistent schemas on new restore and proceeding to switchover"
	);

	// Cancel the Job (background propagation so its pods are GC'd too).
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
	let dp = kube::api::DeleteParams::background();
	if let Err(e) = jobs.delete(job_name, &dp).await {
		warn!(job = %job_name, error = %e, "failed to delete timed-out migration Job");
	}

	// Opportunistically DROP SCHEMA … CASCADE the persistent_schemas on
	// the new restore so the operator's "owned" schemas don't carry stale
	// leftovers from the restored data. Bounded at 60 seconds: if a
	// backend in the target restore is itself stuck (the exact failure
	// mode that tripped the migration budget in the first place — e.g.
	// CREATE TABLE spinning at 100% CPU and ignoring SIGTERM), our DROP
	// queues on its lock and never completes. Better to leave leftover
	// schemas than to wedge the switchover indefinitely on the cleanup.
	const CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);
	if !schemas.is_empty() {
		let cleanup = drop_persistent_schemas_on_target(
			client,
			ctx,
			replica,
			namespace,
			&new_restore_name,
			&schemas,
		);
		match tokio::time::timeout(CLEANUP_TIMEOUT, cleanup).await {
			Ok(Ok(())) => {}
			Ok(Err(e)) => {
				warn!(
					replica = %replica_name,
					error = %e,
					"DROP SCHEMA cleanup errored in timeout-skip path; proceeding to switchover with leftover schemas"
				);
			}
			Err(_) => {
				warn!(
					replica = %replica_name,
					timeout = ?CLEANUP_TIMEOUT,
					"DROP SCHEMA cleanup itself timed out (target postgres backend likely stuck); proceeding to switchover with leftover schemas"
				);
			}
		}
	}

	// Surface as a Warning event so this is visible on the replica CR.
	let note = format!(
		"Schema migration exceeded its time budget (20% of cron interval). \
		 Persistent schemas [{}] were dropped on the new restore so it can come up. \
		 The next restore cycle will reattempt migration if the schemas have been regenerated upstream.",
		schemas.join(", ")
	);
	if let Err(e) = ctx
		.recorder
		.publish(
			&Event {
				type_: EventType::Warning,
				reason: "SchemaMigrationTimedOut".into(),
				note: Some(note),
				action: "Restore".into(),
				secondary: Some(new_restore.object_ref(&())),
			},
			&replica.object_ref(&()),
		)
		.await
	{
		warn!(replica = %replica_name, error = %e, "failed to publish SchemaMigrationTimedOut event");
	}

	// Status: phase = timeout-skipped so it's distinguishable from
	// complete/partial/failed.
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"status": {
			"schemaMigrationJob": null,
			"schemaMigrationPhase": SchemaMigrationPhase::TimeoutSkipped,
		}
	});
	replicas
		.patch_status(
			&replica_name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;

	Ok(())
}

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

	let schemas = replica
		.spec
		.persistent_schemas
		.as_ref()
		.ok_or_else(|| Error::SchemaMigration("missing persistent_schemas config".into()))?;

	// Edge case: First restore, no previous restore to migrate from. Don't
	// mark schemaMigrationPhase=complete here: nothing is stale yet (no
	// previous restore exists), so the cleanup gate is moot, and setting
	// the phase would short-circuit the *next* migration via the guard at
	// L1123 even when real schemas need to be carried over.
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
		mark_schema_migration_complete(client, &replica_name, namespace).await?;
		return Ok(true);
	}

	// Check if migration already complete in status
	if replica
		.status
		.as_ref()
		.and_then(|s| s.schema_migration_phase.as_ref())
		.is_some_and(|p| matches!(p, SchemaMigrationPhase::Complete))
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
				if migration_exceeded_budget(replica, &job) {
					timeout_schema_migration(
						client,
						ctx,
						replica,
						namespace,
						new_restore,
						&job_name,
					)
					.await?;
					return Ok(true);
				}
				debug!(replica = %replica_name, job = %job_name, "migration Job still running");
				return Ok(false);
			}
			JobStatus::Succeeded => {
				// The migration script reports a partial-success callback body
				// when psql exited cleanly but some individual statements
				// failed (typical when dbt views reference renamed/dropped
				// upstream columns). We treat it as completion either way —
				// the replica must come up — but surface partials as a
				// Warning event so operators can find them.
				let callback = ctx.schema_migration_results.take(namespace, &replica_name);
				let is_partial = callback
					.as_deref()
					.is_some_and(|b| b.starts_with("partial"));

				if is_partial {
					warn!(
						replica = %replica_name,
						result = ?callback,
						"migration Job succeeded with statement errors; some persistent_schemas objects may need regenerating"
					);
					if let Err(e) = ctx
						.recorder
						.publish(
							&Event {
								type_: EventType::Warning,
								reason: "SchemaMigrationPartial".into(),
								note: callback.clone(),
								action: "Restore".into(),
								secondary: Some(new_restore.object_ref(&())),
							},
							&replica.object_ref(&()),
						)
						.await
					{
						warn!(replica = %replica_name, error = %e, "failed to publish SchemaMigrationPartial event");
					}
				} else {
					info!(replica = %replica_name, "migration Job succeeded");
				}

				let phase = if is_partial {
					SchemaMigrationPhase::Partial
				} else {
					SchemaMigrationPhase::Complete
				};
				let replicas: Api<PostgresPhysicalReplica> =
					Api::namespaced(client.clone(), namespace);
				let patch = serde_json::json!({
					"status": {
						"schemaMigrationJob": null,
						"schemaMigrationPhase": phase,
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
						"schemaMigrationPhase": SchemaMigrationPhase::Failed(last_error.clone()),
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
				return Err(Error::SchemaMigration(format!(
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
	let reader_user = postgres::read_secret_field(&reader_secret, "username")?;
	let reader_password = postgres::read_secret_field(&reader_secret, "password")?;

	let source_dbname = match postgres::discover_restore_database(
		client,
		namespace,
		&old_restore_name,
		&reader_user,
		&reader_password,
		ctx.use_port_forward(),
	)
	.await
	{
		Ok(db) => db,
		Err(e) if is_auth_error(&e) => {
			warn!(
				replica = %replica_name,
				restore = %old_restore_name,
				error = %e,
				"auth failure connecting to active restore; triggering credential reset"
			);
			if ensure_credential_reset(client, namespace, old_restore, replica).await? {
				info!(
					replica = %replica_name,
					restore = %old_restore_name,
					"credential reset complete, retrying on next reconcile"
				);
			}
			return Ok(false);
		}
		Err(e) => return Err(e),
	};

	// Source-side queries: schema existence + database size. Both run on
	// (source restore, source_dbname), so we open one connection and
	// reuse it across both queries to halve the connection count for
	// this leg of the reconcile.
	let all_schemas: &[String] = schemas;
	let (schemas, db_size_bytes) = {
		let conn = postgres::connect_to_restore(
			client,
			namespace,
			&old_restore_name,
			&source_dbname,
			&reader_user,
			&reader_password,
			ctx.use_port_forward(),
		)
		.await?;
		let existing = postgres::existing_schemas_on(&conn.client, all_schemas).await?;
		let size = postgres::database_size_on(&conn.client).await?;
		(existing, size)
	};
	let existing_set: HashSet<&str> = schemas.iter().map(String::as_str).collect();
	let skipped: Vec<&String> = all_schemas
		.iter()
		.filter(|s| !existing_set.contains(s.as_str()))
		.collect();
	if !skipped.is_empty() {
		warn!(
			restore = %old_restore_name,
			skipped = ?skipped,
			"persistent schemas not found on source, skipping"
		);
	}

	if schemas.is_empty() {
		info!(
			replica = %replica_name,
			"no persistent schemas exist on source, skipping migration"
		);
		mark_schema_migration_complete(client, &replica_name, namespace).await?;
		return Ok(true);
	}

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

	let target_dbname = match postgres::discover_restore_database(
		client,
		namespace,
		&new_restore_name,
		&reader_user,
		&reader_password,
		ctx.use_port_forward(),
	)
	.await
	{
		Ok(db) => db,
		Err(e) if is_auth_error(&e) => {
			warn!(
				replica = %replica_name,
				restore = %new_restore_name,
				error = %e,
				"auth failure connecting to switching restore; triggering credential reset"
			);
			if ensure_credential_reset(client, namespace, new_restore, replica).await? {
				info!(
					replica = %replica_name,
					restore = %new_restore_name,
					"credential reset complete, retrying on next reconcile"
				);
			}
			return Ok(false);
		}
		Err(e) => return Err(e),
	};

	// Target-side: detect any persistent_schemas that are already present
	// in the new restore and drop them. Both queries run on (new restore,
	// target_dbname), so open one connection and reuse it for both. The
	// schemas to drop are operator-owned — whatever was in the restored
	// data is junk (typically because the schema has leaked back into
	// the source DB) — and the migration Job is about to write the
	// canonical copy from the previous restore via pg_dump | psql,
	// which would otherwise conflict on existing objects. Idempotent
	// and no-op when the schemas are absent.
	{
		let conn = postgres::connect_to_restore(
			client,
			namespace,
			&new_restore_name,
			&target_dbname,
			&reader_user,
			&reader_password,
			ctx.use_port_forward(),
		)
		.await?;
		let preexisting = postgres::existing_schemas_on(&conn.client, &schemas).await?;
		if !preexisting.is_empty() {
			info!(
				replica = %replica_name,
				restore = %new_restore_name,
				schemas = ?preexisting,
				"dropping pre-existing persistent schemas in target before migration"
			);
			postgres::drop_schemas_on(&conn.client, &preexisting).await?;
		}
	}

	// The analytics user is granted SUPERUSER on every read-write restore
	// (persistent_schemas implies read_only is effectively false), so we
	// reuse the replica creds secret for write access to the target.
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
		&schemas,
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
			"schemaMigrationPhase": SchemaMigrationPhase::Active,
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
