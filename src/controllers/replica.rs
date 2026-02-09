use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::Utc;
use k8s_openapi::{
	ByteString,
	api::{
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, EnvVar, PodSpec, PodTemplateSpec, ResourceRequirements, Secret, Service,
			ServicePort, ServiceSpec,
		},
	},
	apimachinery::pkg::{
		api::resource::Quantity, apis::meta::v1::OwnerReference, util::intstr::IntOrString,
	},
};
use kube::{
	Api, Client, ResourceExt,
	api::{ObjectMeta, Patch, PatchParams, PostParams},
	runtime::controller::Action,
};
use rand::RngExt;
use serde::Deserialize;
use tracing::{info, warn};

use super::{env_from_secret, read_job_termination_message};
use crate::{
	context::Context,
	error::{Error, Result},
	kopia,
	notifications::{self, ConnectionInfoPayload, NotificationPayload, ReplicaRef, RestoreRef},
	types::*,
	util::parse_duration,
};

#[derive(Debug, Deserialize)]
struct SnapshotInfo {
	id: String,
	size: u64,
}

/// Calculate stable jitter for a replica name.
fn calculate_jitter(replica_name: &str, max_jitter: Duration) -> Duration {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};

	let mut hasher = DefaultHasher::new();
	replica_name.hash(&mut hasher);
	let hash = hasher.finish();

	let max_secs = max_jitter.as_secs();
	if max_secs == 0 {
		return Duration::ZERO;
	}
	Duration::from_secs(hash % max_secs)
}

/// Generate a random password for analytics credentials.
fn generate_password() -> String {
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

	// 6. Check child PostgresPhysicalRestore resources
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), &namespace);
	let restore_list = restores
		.list(&kube::api::ListParams::default().labels(&format!("bes.au/replica={name}")))
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

		// Send notifications
		send_restore_notifications(
			client,
			&ctx.http_client,
			&namespace,
			&replica,
			&switching,
			&conn_info,
			&creds_secret_name,
			&ctx.metrics,
		)
		.await;

		// Recompute next scheduled restore in case schedule changed while restore was in flight
		if let Some(schedule) = &replica.spec.schedule {
			if let Some(next) = compute_next_scheduled_restore(schedule) {
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

		return Ok(Action::requeue(Duration::from_secs(10)));
	}

	// 8. Clean up old restores after grace period
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

	// 9. Decide whether to trigger a new restore
	if in_progress_restore.is_some() {
		update_replica_phase(client, &namespace, &name, ReplicaPhase::Restoring).await?;
		return Ok(Action::requeue(Duration::from_secs(30)));
	}

	let should_restore = should_trigger_scheduled_restore(&replica);

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

					if let Err(e) = jobs.delete(&snapshot_job_name, &Default::default()).await {
						warn!(job = snapshot_job_name, error = %e, "failed to delete snapshot list job");
					}

					if let Some(ref raw) = msg {
						if let Ok(snap) = serde_json::from_str::<SnapshotInfo>(raw) {
							update_replica_status_field(
								client,
								&namespace,
								&name,
								"latestAvailableSnapshot",
								&snap.id,
							)
							.await?;

							let current_snapshot_id =
								active_restore.map(|r| r.spec.snapshot.as_str());

							if current_snapshot_id == Some(&snap.id) {
								info!(
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
					} else {
						info!(
							replica = name,
							"snapshot list job returned no matching snapshots"
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
		if let Some(schedule) = &replica.spec.schedule {
			if let Some(next) = compute_next_scheduled_restore(schedule) {
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

// ─── Helpers ────────────────────────────────────────────────────────────────

fn build_snapshot_list_job(
	replica: &PostgresPhysicalReplica,
	job_name: &str,
	namespace: &str,
) -> Result<Job> {
	let kopia_secret = &replica.spec.kopia_secret_ref;
	let replica_name = replica.name_any();

	let mut env_vars = vec![
		env_from_secret("KOPIA_BUCKET", kopia_secret, "bucket"),
		env_from_secret("KOPIA_REGION", kopia_secret, "region"),
		env_from_secret("AWS_ACCESS_KEY_ID", kopia_secret, "accessKeyId"),
		env_from_secret("AWS_SECRET_ACCESS_KEY", kopia_secret, "secretAccessKey"),
		env_from_secret("KOPIA_PASSWORD", kopia_secret, "repositoryPassword"),
	];

	if let Some(ref filter) = replica.spec.snapshot_filter {
		if let Some(ref pattern) = filter.host_pattern {
			env_vars.push(EnvVar {
				name: "FILTER_HOST_PATTERN".to_string(),
				value: Some(pattern.clone()),
				..Default::default()
			});
		}
		if let Some(ref tags) = filter.tags {
			let tag_str = tags
				.iter()
				.map(|(k, v)| format!("{k}={v}"))
				.collect::<Vec<_>>()
				.join(",");
			env_vars.push(EnvVar {
				name: "FILTER_TAGS".to_string(),
				value: Some(tag_str),
				..Default::default()
			});
		}
	}

	let script = r#"set -e

kopia repository connect s3 \
  --bucket="$KOPIA_BUCKET" \
  --region="$KOPIA_REGION" \
  --access-key="$AWS_ACCESS_KEY_ID" \
  --secret-access-key="$AWS_SECRET_ACCESS_KEY" \
  --password="$KOPIA_PASSWORD"

SNAPSHOTS=$(kopia snapshot list --json --all 2>/dev/null || echo "[]")

if [ -n "$FILTER_HOST_PATTERN" ]; then
  REGEX=$(printf '%s' "$FILTER_HOST_PATTERN" | sed 's/\./\\./g; s/\*/\.\*/g; s/\?/\./g')
  SNAPSHOTS=$(echo "$SNAPSHOTS" | jq -c --arg pat "^${REGEX}$" '[.[] | select(.hostname | test($pat))]')
fi

if [ -n "$FILTER_TAGS" ]; then
  IFS=',' read -r TAG_LIST <<EOF
$FILTER_TAGS
EOF
  for tag in $TAG_LIST; do
    KEY="${tag%%=*}"
    VALUE="${tag#*=}"
    SNAPSHOTS=$(echo "$SNAPSHOTS" | jq -c --arg k "$KEY" --arg v "$VALUE" '[.[] | select(.tags[$k] == $v)]')
  done
fi

LATEST=$(echo "$SNAPSHOTS" | jq -c 'sort_by(.startTime) | last // empty')

if [ -z "$LATEST" ] || [ "$LATEST" = "null" ]; then
  echo "No matching snapshots found"
  exit 0
fi

ID=$(echo "$LATEST" | jq -r '.id')
SIZE=$(echo "$LATEST" | jq -r '.summary.size // 0')
echo "Latest snapshot: id=$ID size=$SIZE"
printf '{"id":"%s","size":%s}' "$ID" "$SIZE" > /dev/termination-log
"#;

	Ok(Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("bes.au/replica".to_string(), replica_name.clone()),
				("bes.au/job-type".to_string(), "snapshot-list".to_string()),
			])),
			owner_references: Some(vec![owner_reference(replica)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(2),
			active_deadline_seconds: Some(300),
			ttl_seconds_after_finished: Some(120),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(BTreeMap::from([
						("bes.au/replica".to_string(), replica_name),
						("bes.au/job-type".to_string(), "snapshot-list".to_string()),
					])),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					containers: vec![Container {
						name: "snapshot-list".to_string(),
						image: Some("kopia/kopia:latest".to_string()),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![script.to_string()]),
						env: Some(env_vars),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("50m".to_string())),
								("memory".to_string(), Quantity("64Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("200m".to_string())),
								("memory".to_string(), Quantity("128Mi".to_string())),
							])),
							..Default::default()
						}),
						..Default::default()
					}],
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}

async fn create_restore_for_snapshot(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	snapshot: &SnapshotInfo,
) -> Result<()> {
	let replica_name = replica.name_any();
	let timestamp = Utc::now().format("%Y%m%d-%H%M%S");
	let restore_name = format!("{replica_name}-{timestamp}");

	let snapshot_size = format_bytes(snapshot.size);
	let storage_size = match &replica.spec.storage_size_override {
		Some(override_size) => override_size.clone(),
		None => format_bytes((snapshot.size as f64 * 1.1) as u64),
	};

	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), namespace);
	let restore = PostgresPhysicalRestore::new(
		&restore_name,
		PostgresPhysicalRestoreSpec {
			replica: replica_name.clone(),
			snapshot: snapshot.id.clone(),
			snapshot_size,
			storage_size,
		},
	);

	let mut restore_obj = serde_json::to_value(&restore)?;
	if let Some(meta) = restore_obj
		.as_object_mut()
		.and_then(|o| o.get_mut("metadata"))
		.and_then(|m| m.as_object_mut())
	{
		meta.insert(
			"namespace".to_string(),
			serde_json::Value::String(namespace.to_string()),
		);
		meta.insert(
			"labels".to_string(),
			serde_json::json!({ "bes.au/replica": replica_name }),
		);
		meta.insert(
			"ownerReferences".to_string(),
			serde_json::json!([{
				"apiVersion": "bes.au/v1alpha1",
				"kind": "PostgresPhysicalReplica",
				"name": replica.name_any(),
				"uid": replica.uid().unwrap_or_default(),
				"controller": true,
				"blockOwnerDeletion": true,
			}]),
		);
	}

	let restore_resource: PostgresPhysicalRestore = serde_json::from_value(restore_obj)?;
	restores
		.create(&PostParams::default(), &restore_resource)
		.await?;

	info!(
		replica = replica_name,
		restore = restore_name,
		snapshot = snapshot.id,
		"created restore resource"
	);

	Ok(())
}

fn format_bytes(bytes: u64) -> String {
	const GI: u64 = 1024 * 1024 * 1024;
	const MI: u64 = 1024 * 1024;
	if bytes >= GI {
		let gi = (bytes as f64) / (GI as f64);
		format!("{:.1}Gi", gi)
	} else {
		let mi = (bytes as f64) / (MI as f64);
		format!("{:.0}Mi", mi.max(1.0))
	}
}

/// Returns the next cron occurrence after `now`.
fn compute_next_scheduled_restore(schedule: &str) -> Option<chrono::DateTime<Utc>> {
	let cron_schedule = schedule.parse::<cron::Schedule>().ok()?;
	cron_schedule.upcoming(Utc).next()
}

fn should_trigger_scheduled_restore(replica: &PostgresPhysicalReplica) -> bool {
	let Some(schedule) = &replica.spec.schedule else {
		return false;
	};

	let status = replica.status.as_ref();

	// Check minimumTTL
	if let Some(last_completed) = status.and_then(|s| s.last_restore_completed_at.as_ref()) {
		if let Ok(last_completed) = last_completed.parse::<chrono::DateTime<Utc>>() {
			let minimum_ttl =
				parse_duration(&replica.spec.minimum_ttl).unwrap_or(Duration::from_secs(6 * 3600));
			let elapsed = Utc::now().signed_duration_since(last_completed);
			if elapsed.to_std().unwrap_or_default() < minimum_ttl {
				return false;
			}
		}
	}

	// Check cron schedule
	let Ok(cron_schedule) = schedule.parse::<cron::Schedule>() else {
		warn!(schedule = schedule, "invalid cron expression");
		return false;
	};

	let jitter = calculate_jitter(
		&replica.name_any(),
		parse_duration(&replica.spec.schedule_jitter).unwrap_or(Duration::from_secs(600)),
	);

	let now = Utc::now();

	if let Some(next_scheduled) = status.and_then(|s| s.next_scheduled_restore.as_ref()) {
		if let Ok(next) = next_scheduled.parse::<chrono::DateTime<Utc>>() {
			// Add jitter to the scheduled time
			let trigger_at = next + chrono::Duration::from_std(jitter).unwrap_or_default();
			return now >= trigger_at;
		}
	}

	// Initial seed: nextScheduledRestore not yet set (first reconciliation or field was cleared).
	// Fall back to checking whether a cron occurrence falls within a 24h lookback window.
	let jittered_now = now - chrono::Duration::from_std(jitter).unwrap_or_default();
	for prev in cron_schedule.after(&(jittered_now - chrono::Duration::hours(24))) {
		if prev <= now {
			return true;
		}
		break;
	}

	false
}

async fn ensure_credentials_secret(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	secret_name: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<()> {
	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

	// Check if it already exists
	if secrets.get_opt(secret_name).await?.is_some() {
		return Ok(());
	}

	info!(
		replica = replica_name,
		secret = secret_name,
		"creating credentials secret"
	);

	let password = generate_password();
	let secret = Secret {
		metadata: ObjectMeta {
			name: Some(secret_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([(
				"bes.au/replica".to_string(),
				replica_name.to_string(),
			)])),
			owner_references: Some(vec![owner_reference(replica)]),
			..Default::default()
		},
		data: Some(BTreeMap::from([
			(
				"username".to_string(),
				ByteString(replica.spec.analytics_username.as_bytes().to_vec()),
			),
			(
				"password".to_string(),
				ByteString(password.as_bytes().to_vec()),
			),
		])),
		..Default::default()
	};

	secrets.create(&PostParams::default(), &secret).await?;
	Ok(())
}

async fn ensure_service(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<()> {
	let services: Api<Service> = Api::namespaced(client.clone(), namespace);

	if services.get_opt(replica_name).await?.is_some() {
		// Service exists; update annotations if needed
		let mut annotations = BTreeMap::new();
		if let Some(sa) = &replica.spec.service_annotations {
			for (k, v) in sa {
				annotations.insert(k.clone(), v.clone());
			}
		}
		let patch = serde_json::json!({
			"metadata": {
				"annotations": annotations,
			}
		});
		services
			.patch(
				replica_name,
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		return Ok(());
	}

	info!(replica = replica_name, "creating stable service");

	let mut annotations = BTreeMap::new();
	if let Some(sa) = &replica.spec.service_annotations {
		for (k, v) in sa {
			annotations.insert(k.clone(), v.clone());
		}
	}

	let service = Service {
		metadata: ObjectMeta {
			name: Some(replica_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([(
				"bes.au/replica".to_string(),
				replica_name.to_string(),
			)])),
			annotations: Some(annotations),
			owner_references: Some(vec![owner_reference(replica)]),
			..Default::default()
		},
		spec: Some(ServiceSpec {
			type_: Some("ClusterIP".to_string()),
			ports: Some(vec![ServicePort {
				name: Some("postgres".to_string()),
				port: 5432,
				target_port: Some(IntOrString::Int(5432)),
				protocol: Some("TCP".to_string()),
				..Default::default()
			}]),
			// No selector initially — set during switchover
			..Default::default()
		}),
		..Default::default()
	};

	services.create(&PostParams::default(), &service).await?;
	Ok(())
}

async fn update_service_selector(
	client: &Client,
	namespace: &str,
	service_name: &str,
	restore_name: &str,
) -> Result<()> {
	let services: Api<Service> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"spec": {
			"selector": {
				"bes.au/restore": restore_name,
			}
		}
	});
	services
		.patch(
			service_name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

fn owner_reference(replica: &PostgresPhysicalReplica) -> OwnerReference {
	OwnerReference {
		api_version: "bes.au/v1alpha1".to_string(),
		kind: "PostgresPhysicalReplica".to_string(),
		name: replica.name_any(),
		uid: replica.uid().unwrap_or_default(),
		controller: Some(true),
		block_owner_deletion: Some(true),
	}
}

async fn update_replica_condition(
	client: &Client,
	namespace: &str,
	name: &str,
	type_: &str,
	status: &str,
	reason: &str,
	message: &str,
) -> Result<()> {
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let now = Utc::now().to_rfc3339();
	let patch = serde_json::json!({
		"status": {
			"conditions": [{
				"type": type_,
				"status": status,
				"reason": reason,
				"message": message,
				"lastTransitionTime": now,
			}]
		}
	});
	replicas
		.patch_status(
			name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

async fn update_replica_phase(
	client: &Client,
	namespace: &str,
	name: &str,
	phase: ReplicaPhase,
) -> Result<()> {
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"status": {
			"phase": phase,
		}
	});
	replicas
		.patch_status(
			name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

async fn update_replica_status_field(
	client: &Client,
	namespace: &str,
	name: &str,
	field: &str,
	value: &str,
) -> Result<()> {
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"status": {
			(field): value,
		}
	});
	replicas
		.patch_status(
			name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

async fn update_replica_connection_info(
	client: &Client,
	namespace: &str,
	name: &str,
	info: &ConnectionInfo,
) -> Result<()> {
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"status": {
			"connectionInfo": info,
		}
	});
	replicas
		.patch_status(
			name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

async fn update_restore_phase(
	client: &Client,
	namespace: &str,
	name: &str,
	phase: RestorePhase,
) -> Result<()> {
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"status": {
			"phase": phase,
		}
	});
	restores
		.patch_status(
			name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

async fn update_restore_activated_at(
	client: &Client,
	namespace: &str,
	name: &str,
	timestamp: &str,
) -> Result<()> {
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"status": {
			"activatedAt": timestamp,
		}
	});
	restores
		.patch_status(
			name,
			&PatchParams::apply("postgres-restore-operator"),
			&Patch::Merge(&patch),
		)
		.await?;
	Ok(())
}

async fn send_restore_notifications(
	client: &Client,
	http_client: &reqwest::Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	restore: &PostgresPhysicalRestore,
	conn_info: &ConnectionInfo,
	creds_secret_name: &str,
	metrics: &crate::metrics::Metrics,
) {
	let replica_name = replica.name_any();

	let mut statuses: Vec<NotificationStatus> = Vec::new();

	for notif_config in &replica.spec.notifications {
		if !notif_config
			.events
			.contains(&NotificationEvent::RestoreComplete)
		{
			continue;
		}

		// Resolve password if includePassword is true
		let password = if notif_config.include_password {
			let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
			secrets
				.get(creds_secret_name)
				.await
				.ok()
				.and_then(|s| s.data)
				.and_then(|d| d.get("password").cloned())
				.map(|b| String::from_utf8(b.0).unwrap_or_default())
		} else {
			None
		};

		let payload = NotificationPayload {
			event: "RestoreComplete".to_string(),
			timestamp: Utc::now().to_rfc3339(),
			replica: ReplicaRef {
				name: replica_name.clone(),
				namespace: namespace.to_string(),
			},
			restore: RestoreRef {
				name: restore.name_any(),
				snapshot: restore.spec.snapshot.clone(),
				postgres_version: restore
					.status
					.as_ref()
					.and_then(|s| s.postgres_version.clone())
					.unwrap_or_default(),
			},
			connection_info: ConnectionInfoPayload::from_connection_info(conn_info, password),
		};

		let status = notifications::send_notification(
			client,
			http_client,
			namespace,
			notif_config,
			&payload,
		)
		.await;

		if status.success {
			metrics
				.notifications_sent_total
				.with_label_values(&[&notif_config.name, &payload.event])
				.inc();
		} else {
			metrics
				.notifications_failed_total
				.with_label_values(&[&notif_config.name, &payload.event])
				.inc();
		}

		statuses.push(status);
	}

	if !statuses.is_empty() {
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
		let patch = serde_json::json!({
			"status": {
				"notifications": statuses,
			}
		});
		if let Err(e) = replicas
			.patch_status(
				&replica_name,
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await
		{
			warn!(replica = replica_name, error = %e, "failed to update notification status");
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn jitter_is_deterministic() {
		let j1 = calculate_jitter("my-replica", Duration::from_secs(300));
		let j2 = calculate_jitter("my-replica", Duration::from_secs(300));
		assert_eq!(j1, j2);
	}

	#[test]
	fn jitter_differs_for_different_names() {
		let j1 = calculate_jitter("replica-a", Duration::from_secs(300));
		let j2 = calculate_jitter("replica-b", Duration::from_secs(300));
		// Extremely unlikely to collide with different names
		assert_ne!(j1, j2);
	}

	#[test]
	fn jitter_zero_max_returns_zero() {
		let j = calculate_jitter("anything", Duration::ZERO);
		assert_eq!(j, Duration::ZERO);
	}

	#[test]
	fn jitter_within_bounds() {
		let max = Duration::from_secs(600);
		for name in ["a", "b", "c", "replica-prod-01", "zzz"] {
			let j = calculate_jitter(name, max);
			assert!(j < max, "jitter {j:?} should be < {max:?} for {name}");
		}
	}

	#[test]
	fn generate_password_length_and_charset() {
		let pw = generate_password();
		assert_eq!(pw.len(), 32);
		assert!(pw.chars().all(|c| c.is_ascii_alphanumeric()));
	}

	#[test]
	fn generate_password_is_random() {
		let pw1 = generate_password();
		let pw2 = generate_password();
		assert_ne!(pw1, pw2);
	}

	#[test]
	fn format_bytes_gigabytes() {
		assert_eq!(format_bytes(10 * 1024 * 1024 * 1024), "10.0Gi");
		assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0Gi");
		assert_eq!(format_bytes(2_500_000_000), "2.3Gi");
	}

	#[test]
	fn format_bytes_megabytes() {
		assert_eq!(format_bytes(500 * 1024 * 1024), "500Mi");
		assert_eq!(format_bytes(1024 * 1024), "1Mi");
	}

	#[test]
	fn format_bytes_small_clamps_to_1mi() {
		assert_eq!(format_bytes(0), "1Mi");
		assert_eq!(format_bytes(1024), "1Mi");
	}

	#[test]
	fn snapshot_info_parse() {
		let raw = r#"{"id":"abc123def","size":5368709120}"#;
		let snap: SnapshotInfo = serde_json::from_str(raw).unwrap();
		assert_eq!(snap.id, "abc123def");
		assert_eq!(snap.size, 5368709120);
	}

	#[test]
	fn snapshot_info_parse_zero_size() {
		let raw = r#"{"id":"snap0","size":0}"#;
		let snap: SnapshotInfo = serde_json::from_str(raw).unwrap();
		assert_eq!(snap.id, "snap0");
		assert_eq!(snap.size, 0);
	}
}
