use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::Utc;
use k8s_openapi::{
	api::{
		apps::v1::{Deployment, DeploymentSpec},
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, ContainerPort, EnvVar, ExecAction, PersistentVolumeClaim,
			PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe, ResourceRequirements,
			Volume, VolumeMount, VolumeResourceRequirements,
		},
	},
	apimachinery::pkg::{
		api::resource::Quantity,
		apis::meta::v1::{LabelSelector, OwnerReference},
	},
};
use kube::{
	Api, Client, ResourceExt,
	api::{ObjectMeta, Patch, PatchParams, PostParams},
	runtime::controller::Action,
};
use tracing::{info, warn};

use super::{env_from_secret, kopia_writable_env, read_job_termination_message};
use crate::{
	context::Context,
	error::{Error, Result},
	types::*,
};

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

// ─── Phase handlers ─────────────────────────────────────────────────────────

async fn reconcile_pending(
	restore: &PostgresPhysicalRestore,
	ctx: &Context,
	name: &str,
	namespace: &str,
) -> Result<Action> {
	let client = &ctx.client;
	let replica_name = &restore.spec.replica;

	// Set created_at if not set
	if restore
		.status
		.as_ref()
		.and_then(|s| s.created_at.as_ref())
		.is_none()
	{
		let now = Utc::now().to_rfc3339();
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
	cleanup_previous_jobs(client, namespace, &restore.spec.replica, name).await?;

	// Create PVC if it doesn't exist
	let pvc_name = format!("{name}-data");
	let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
	if pvcs.get_opt(&pvc_name).await?.is_none() {
		info!(restore = name, pvc = pvc_name, "creating PVC");
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
		let replica = replicas.get(&restore.spec.replica).await?;
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
	let replica_name = &restore.spec.replica;

	// Create or check restore Job
	let job_name = format!("{name}-restore");
	let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

	let job = match jobs.get_opt(&job_name).await? {
		Some(job) => job,
		None => {
			info!(restore = name, job = job_name, "creating restore job");

			// Look up the parent replica to get the kopia secret ref
			let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
			let replica = replicas.get(&restore.spec.replica).await?;

			let job = build_restore_job(restore, &job_name, namespace, &replica)?;
			jobs.create(&PostParams::default(), &job).await?
		}
	};

	// Check job status
	let job_status = &job.status;
	let succeeded = job_status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);
	let failed = job_status.as_ref().and_then(|s| s.failed).unwrap_or(0);

	if succeeded > 0 {
		info!(restore = name, "restore job succeeded");

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

		let now = Utc::now().to_rfc3339();
		let completed_at = job_status
			.as_ref()
			.and_then(|s| s.completion_time.as_ref())
			.map(|t| t.0.to_string())
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

		ctx.metrics.restores_completed_total.inc();

		return Ok(Action::requeue(Duration::from_secs(5)));
	}

	// Check for backoff limit exceeded
	let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(3);
	if failed > backoff_limit {
		warn!(restore = name, failed = failed, "restore job failed");

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

async fn reconcile_ready(
	restore: &PostgresPhysicalRestore,
	ctx: &Context,
	name: &str,
	namespace: &str,
) -> Result<Action> {
	let client = &ctx.client;
	let replica_name = &restore.spec.replica;

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

	// Create Deployment if it doesn't exist
	let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);

	if let Some(deploy) = deployments.get_opt(name).await? {
		// Deployment exists, check if ready
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
		if let Some(created_at) = restore.status.as_ref().and_then(|s| s.restored_at.as_ref())
			&& let Ok(created) = created_at.parse::<chrono::DateTime<Utc>>()
		{
			let elapsed = Utc::now().signed_duration_since(created);
			if elapsed > chrono::Duration::minutes(10) {
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

		return Ok(Action::requeue(Duration::from_secs(10)));
	}

	// Look up the parent replica for config
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
	let replica = replicas.get(&restore.spec.replica).await?;

	info!(restore = name, "creating deployment");
	let deploy = build_deployment(restore, name, namespace, &replica)?;
	deployments.create(&PostParams::default(), &deploy).await?;

	Ok(Action::requeue(Duration::from_secs(10)))
}

fn build_version_detect_job(
	restore: &PostgresPhysicalRestore,
	job_name: &str,
	namespace: &str,
	pvc_name: &str,
) -> Job {
	let script = r#"set -e

echo "PVC contents:"
ls -la /pgdata/ 2>&1 || true
echo "---"
ls -la /pgdata/pgdata/ 2>&1 || true
echo "---"

# If the pgdata symlink already exists, just read the version
if [ -L /pgdata/pgdata ] && [ -f /pgdata/pgdata/PG_VERSION ]; then
  VERSION=$(cat /pgdata/pgdata/PG_VERSION)
  echo "Detected postgres version: $VERSION"
  echo -n "$VERSION" > /dev/termination-log
  exit 0
fi

# Otherwise locate PGDATA and recreate the symlink
echo "pgdata symlink missing, locating PGDATA directory..."
PGDATA_DIR=""

# Prefer 'current' symlink (org convention)
if [ -L /pgdata/postgres/current ]; then
  LINK_TARGET=$(readlink /pgdata/postgres/current)
  RELATIVE=$(echo "$LINK_TARGET" | sed 's|.*/\([0-9]\{1,\}/\)|/pgdata/postgres/\1|')
  if [ -f "$RELATIVE/PG_VERSION" ]; then
    PGDATA_DIR="$RELATIVE"
    echo "Found PGDATA via 'current' symlink: $PGDATA_DIR"
  fi
fi

# Fallback: pick the highest version directory containing PG_VERSION
if [ -z "$PGDATA_DIR" ]; then
  PGDATA_DIR=$(find /pgdata/postgres -name "PG_VERSION" -exec dirname {} \; 2>/dev/null | sort -t/ -k4 -rn | head -1)
fi

# Last resort: search anywhere under /pgdata
if [ -z "$PGDATA_DIR" ]; then
  echo "Searching for PG_VERSION recursively..."
  find /pgdata -name "PG_VERSION" 2>/dev/null || true
  PGDATA_DIR=$(find /pgdata -name "PG_VERSION" -exec dirname {} \; 2>/dev/null | sort -t/ -k4 -rn | head -1)
fi

if [ -z "$PGDATA_DIR" ]; then
  echo "ERROR: Could not detect postgres version from PVC"
  exit 1
fi

echo "Found PGDATA at: $PGDATA_DIR"
ln -sfn "$PGDATA_DIR" /pgdata/pgdata

VERSION=$(cat /pgdata/pgdata/PG_VERSION)
echo "Detected postgres version: $VERSION"
echo "$VERSION" > /pgdata/.postgres-version
echo -n "$VERSION" > /dev/termination-log
"#;

	Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("bes.au/replica".to_string(), restore.spec.replica.clone()),
				("bes.au/restore".to_string(), restore.name_any()),
				("bes.au/job-type".to_string(), "version-detect".to_string()),
			])),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(2),
			active_deadline_seconds: Some(120),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(BTreeMap::from([
						("bes.au/replica".to_string(), restore.spec.replica.clone()),
						("bes.au/restore".to_string(), restore.name_any()),
					])),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
						run_as_user: Some(999),
						run_as_group: Some(999),
						fs_group: Some(999),
						..Default::default()
					}),
					containers: vec![Container {
						name: "version-detect".to_string(),
						image: Some("alpine:latest".to_string()),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![script.to_string()]),
						volume_mounts: Some(vec![VolumeMount {
							name: "pgdata".to_string(),
							mount_path: "/pgdata".to_string(),
							..Default::default()
						}]),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("10m".to_string())),
								("memory".to_string(), Quantity("16Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("50m".to_string())),
								("memory".to_string(), Quantity("32Mi".to_string())),
							])),
							..Default::default()
						}),
						..Default::default()
					}],
					volumes: Some(vec![Volume {
						name: "pgdata".to_string(),
						persistent_volume_claim: Some(
							k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
								claim_name: pvc_name.to_string(),
								read_only: Some(false),
							},
						),
						..Default::default()
					}]),
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	}
}

// ─── Resource builders ──────────────────────────────────────────────────────

fn build_pvc(
	restore: &PostgresPhysicalRestore,
	pvc_name: &str,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<PersistentVolumeClaim> {
	Ok(PersistentVolumeClaim {
		metadata: ObjectMeta {
			name: Some(pvc_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("bes.au/replica".to_string(), restore.spec.replica.clone()),
				("bes.au/restore".to_string(), restore.name_any()),
			])),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(PersistentVolumeClaimSpec {
			access_modes: Some(vec!["ReadWriteOnce".to_string()]),
			storage_class_name: replica.spec.storage_class.clone(),
			resources: Some(VolumeResourceRequirements {
				requests: Some(BTreeMap::from([(
					"storage".to_string(),
					Quantity(restore.spec.storage_size.clone()),
				)])),
				..Default::default()
			}),
			..Default::default()
		}),
		..Default::default()
	})
}

fn build_restore_job(
	restore: &PostgresPhysicalRestore,
	job_name: &str,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<Job> {
	let kopia_secret = &replica.spec.kopia_secret_ref;
	let pvc_name = format!("{}-data", restore.name_any());

	let restore_script = r#"set -e

mkdir -p /tmp/kopia/config /tmp/kopia/logs /tmp/kopia/cache

echo "Connecting to kopia repository..."
kopia repository connect s3 \
  --bucket="$KOPIA_BUCKET" \
  --region="$KOPIA_REGION" \
  --access-key="$AWS_ACCESS_KEY_ID" \
  --secret-access-key="$AWS_SECRET_ACCESS_KEY" \
  --password="$KOPIA_PASSWORD"

echo "Starting restore..."
kopia snapshot restore "$SNAPSHOT_ID" /pgdata/postgres

echo "Restore complete"
ls -la /pgdata/

echo "Locating PGDATA directory..."

# Prefer the 'current' symlink if it exists (org convention)
if [ -L /pgdata/postgres/current ]; then
  # The symlink target is an absolute path from the original host, resolve it
  # relative to /pgdata/postgres by extracting the version/cluster part.
  LINK_TARGET=$(readlink /pgdata/postgres/current)
  # e.g. /var/lib/postgresql/16/main -> try /pgdata/postgres/16/main
  RELATIVE=$(echo "$LINK_TARGET" | sed 's|.*/\([0-9]\{1,\}/\)|/pgdata/postgres/\1|')
  if [ -f "$RELATIVE/PG_VERSION" ]; then
    PGDATA_DIR="$RELATIVE"
    echo "Found PGDATA via 'current' symlink: $PGDATA_DIR"
  fi
fi

# Fallback: pick the highest version directory containing PG_VERSION
if [ -z "$PGDATA_DIR" ]; then
  PGDATA_DIR=$(find /pgdata/postgres -name "PG_VERSION" -exec dirname {} \; 2>/dev/null | sort -t/ -k4 -rn | head -1)
fi

if [ -z "$PGDATA_DIR" ]; then
  echo "ERROR: Could not find PG_VERSION in restored data"
  exit 1
fi
echo "Found PGDATA at: $PGDATA_DIR"
ln -sfn "$PGDATA_DIR" /pgdata/pgdata
rm -f "$PGDATA_DIR/postmaster.pid"

VERSION=$(cat /pgdata/pgdata/PG_VERSION)
echo "Detected postgres version: $VERSION"
echo "$VERSION" > /pgdata/.postgres-version
echo -n "$VERSION" > /dev/termination-log
"#;

	Ok(Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("bes.au/replica".to_string(), restore.spec.replica.clone()),
				("bes.au/restore".to_string(), restore.name_any()),
			])),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(3),
			active_deadline_seconds: Some(7200), // 2 hours
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(BTreeMap::from([
						("bes.au/replica".to_string(), restore.spec.replica.clone()),
						("bes.au/restore".to_string(), restore.name_any()),
					])),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
						run_as_user: Some(999),
						run_as_group: Some(999),
						fs_group: Some(999),
						..Default::default()
					}),

					containers: vec![Container {
						name: "restore".to_string(),
						image: Some("kopia/kopia:latest".to_string()),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![restore_script.to_string()]),
						env: Some(
							[
								vec![EnvVar {
									name: "SNAPSHOT_ID".to_string(),
									value: Some(restore.spec.snapshot.clone()),
									..Default::default()
								}],
								kopia_writable_env(),
								vec![
									env_from_secret("KOPIA_BUCKET", kopia_secret, "bucket"),
									env_from_secret("KOPIA_REGION", kopia_secret, "region"),
									env_from_secret(
										"AWS_ACCESS_KEY_ID",
										kopia_secret,
										"accessKeyId",
									),
									env_from_secret(
										"AWS_SECRET_ACCESS_KEY",
										kopia_secret,
										"secretAccessKey",
									),
									env_from_secret(
										"KOPIA_PASSWORD",
										kopia_secret,
										"repositoryPassword",
									),
								],
							]
							.concat(),
						),
						volume_mounts: Some(vec![VolumeMount {
							name: "pgdata".to_string(),
							mount_path: "/pgdata".to_string(),
							..Default::default()
						}]),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("500m".to_string())),
								("memory".to_string(), Quantity("1Gi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("2".to_string())),
								("memory".to_string(), Quantity("4Gi".to_string())),
							])),
							..Default::default()
						}),
						..Default::default()
					}],
					volumes: Some(vec![Volume {
						name: "pgdata".to_string(),
						persistent_volume_claim: Some(
							k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
								claim_name: pvc_name,
								read_only: Some(false),
							},
						),
						..Default::default()
					}]),
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}

fn build_deployment(
	restore: &PostgresPhysicalRestore,
	name: &str,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<Deployment> {
	let pvc_name = format!("{name}-data");
	let creds_secret = format!("{}-creds", restore.spec.replica);

	let pg_version = restore
		.status
		.as_ref()
		.and_then(|s| s.postgres_version.as_ref())
		.cloned()
		.ok_or_else(|| Error::MissingField("status.postgresVersion".to_string()))?;

	let pg_image = format!("postgres:{pg_version}");
	let pg_alpine_image = format!("postgres:{pg_version}-alpine");

	let read_only = if replica.spec.read_only {
		"true"
	} else {
		"false"
	};

	let init_script = format!(
		r#"set -e
PGDATA=/pgdata/pgdata

echo "Configuring pg_hba.conf..."
cat > "$PGDATA/pg_hba.conf" << 'HBAEOF'
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             postgres                                peer
host    all             all             0.0.0.0/0               scram-sha-256
host    all             all             ::/0                    scram-sha-256
HBAEOF

echo "Starting temporary postgres to configure analytics user..."
pg_ctl -D "$PGDATA" -o "-c listen_addresses='' -c log_min_messages=WARNING" -w start

psql -U postgres -d postgres << SQLEOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${{ANALYTICS_USERNAME}}') THEN
    CREATE ROLE ${{ANALYTICS_USERNAME}} WITH LOGIN PASSWORD '${{ANALYTICS_PASSWORD}}';
  ELSE
    ALTER ROLE ${{ANALYTICS_USERNAME}} WITH PASSWORD '${{ANALYTICS_PASSWORD}}';
  END IF;
END
\$\$;
GRANT CONNECT ON DATABASE postgres TO ${{ANALYTICS_USERNAME}};
GRANT USAGE ON SCHEMA public TO ${{ANALYTICS_USERNAME}};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${{ANALYTICS_USERNAME}};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${{ANALYTICS_USERNAME}};
SQLEOF

echo "Stopping temporary postgres..."
pg_ctl -D "$PGDATA" -w stop

if [ "{read_only}" = "true" ]; then
  echo "Enabling read-only mode..."
  # Remove any existing setting to avoid duplicates across restarts
  sed -i '/^default_transaction_read_only/d' "$PGDATA/postgresql.conf"
  echo "default_transaction_read_only = on" >> "$PGDATA/postgresql.conf"
fi

echo "Auth setup complete"
"#
	);

	let labels = BTreeMap::from([
		("bes.au/replica".to_string(), restore.spec.replica.clone()),
		("bes.au/restore".to_string(), name.to_string()),
	]);

	// Build resource requirements from replica spec
	let container_resources = replica
		.spec
		.resources
		.as_ref()
		.map(|r| ResourceRequirements {
			requests: r.requests.as_ref().map(|reqs| {
				reqs.iter()
					.map(|(k, v)| (k.clone(), Quantity(v.clone())))
					.collect()
			}),
			limits: r.limits.as_ref().map(|lims| {
				lims.iter()
					.map(|(k, v)| (k.clone(), Quantity(v.clone())))
					.collect()
			}),
			..Default::default()
		});

	let mut pod_annotations = BTreeMap::new();
	if let Some(pa) = &replica.spec.pod_annotations {
		for (k, v) in pa {
			pod_annotations.insert(k.clone(), v.clone());
		}
	}

	let mut node_selector = BTreeMap::new();
	if let Some(ns) = &replica.spec.node_selector {
		for (k, v) in ns {
			node_selector.insert(k.clone(), v.clone());
		}
	}

	let tolerations: Vec<k8s_openapi::api::core::v1::Toleration> = replica
		.spec
		.tolerations
		.iter()
		.map(|t| k8s_openapi::api::core::v1::Toleration {
			key: t.key.clone(),
			operator: t.operator.clone(),
			value: t.value.clone(),
			effect: t.effect.clone(),
			toleration_seconds: t.toleration_seconds,
		})
		.collect();

	Ok(Deployment {
		metadata: ObjectMeta {
			name: Some(name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(labels.clone()),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(DeploymentSpec {
			replicas: Some(1),
			selector: LabelSelector {
				match_labels: Some(BTreeMap::from([(
					"bes.au/restore".to_string(),
					name.to_string(),
				)])),
				..Default::default()
			},
			strategy: Some(k8s_openapi::api::apps::v1::DeploymentStrategy {
				type_: Some("Recreate".to_string()),
				..Default::default()
			}),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(labels),
					annotations: if pod_annotations.is_empty() {
						None
					} else {
						Some(pod_annotations)
					},
					..Default::default()
				}),
				spec: Some(PodSpec {
					security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
						run_as_user: Some(999),
						run_as_group: Some(999),
						fs_group: Some(999),
						..Default::default()
					}),
					init_containers: Some(vec![Container {
						name: "setup-auth".to_string(),
						image: Some(pg_alpine_image),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![init_script]),
						env: Some(vec![
							EnvVar {
								name: "ANALYTICS_USERNAME".to_string(),
								value: Some(replica.spec.analytics_username.clone()),
								..Default::default()
							},
							env_from_secret("ANALYTICS_PASSWORD", &creds_secret, "password"),
							EnvVar {
								name: "READ_ONLY".to_string(),
								value: Some(read_only.to_string()),
								..Default::default()
							},
						]),
						volume_mounts: Some(vec![VolumeMount {
							name: "pgdata".to_string(),
							mount_path: "/pgdata".to_string(),
							..Default::default()
						}]),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("100m".to_string())),
								("memory".to_string(), Quantity("128Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("500m".to_string())),
								("memory".to_string(), Quantity("256Mi".to_string())),
							])),
							..Default::default()
						}),
						..Default::default()
					}]),
					containers: vec![Container {
						name: "postgres".to_string(),
						image: Some(pg_image),
						args: Some(vec![
							"postgres".to_string(),
							"-D".to_string(),
							"/pgdata/pgdata".to_string(),
						]),
						env: Some(vec![
							EnvVar {
								name: "PGDATA".to_string(),
								value: Some("/pgdata/pgdata".to_string()),
								..Default::default()
							},
							EnvVar {
								name: "POSTGRES_HOST_AUTH_METHOD".to_string(),
								value: Some("scram-sha-256".to_string()),
								..Default::default()
							},
						]),
						ports: Some(vec![ContainerPort {
							name: Some("postgres".to_string()),
							container_port: 5432,
							protocol: Some("TCP".to_string()),
							..Default::default()
						}]),
						volume_mounts: Some(vec![VolumeMount {
							name: "pgdata".to_string(),
							mount_path: "/pgdata".to_string(),
							..Default::default()
						}]),
						readiness_probe: Some(Probe {
							exec: Some(ExecAction {
								command: Some(vec![
									"pg_isready".to_string(),
									"-U".to_string(),
									"postgres".to_string(),
									"-d".to_string(),
									"postgres".to_string(),
								]),
							}),
							initial_delay_seconds: Some(5),
							period_seconds: Some(5),
							timeout_seconds: Some(3),
							failure_threshold: Some(6),
							..Default::default()
						}),
						liveness_probe: Some(Probe {
							exec: Some(ExecAction {
								command: Some(vec![
									"pg_isready".to_string(),
									"-U".to_string(),
									"postgres".to_string(),
									"-d".to_string(),
									"postgres".to_string(),
								]),
							}),
							initial_delay_seconds: Some(30),
							period_seconds: Some(10),
							timeout_seconds: Some(3),
							failure_threshold: Some(3),
							..Default::default()
						}),
						resources: container_resources,
						..Default::default()
					}],
					volumes: Some(vec![Volume {
						name: "pgdata".to_string(),
						persistent_volume_claim: Some(
							k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
								claim_name: pvc_name,
								read_only: Some(false),
							},
						),
						..Default::default()
					}]),
					node_selector: if node_selector.is_empty() {
						None
					} else {
						Some(node_selector)
					},
					tolerations: if tolerations.is_empty() {
						None
					} else {
						Some(tolerations)
					},
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn restore_owner_reference(restore: &PostgresPhysicalRestore) -> OwnerReference {
	OwnerReference {
		api_version: "bes.au/v1alpha1".to_string(),
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
		.list(&kube::api::ListParams::default().labels(&format!("bes.au/replica={replica_name}")))
		.await?;

	for job in &job_list.items {
		let job_name = job.metadata.name.as_deref().unwrap_or("");
		let restore_label = job
			.metadata
			.labels
			.as_ref()
			.and_then(|l| l.get("bes.au/restore"))
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
