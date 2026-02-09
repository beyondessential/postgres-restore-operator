use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, ExecAction, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe, ResourceRequirements, SecretKeySelector,
    Volume, VolumeMount, VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use kube::api::{ObjectMeta, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::{Api, Client, ResourceExt};
use tracing::{info, warn};

use crate::context::Context;
use crate::error::{Error, Result};
use crate::types::*;

pub async fn reconcile(
    restore: Arc<PostgresPhysicalRestore>,
    ctx: Arc<Context>,
) -> Result<Action> {
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
        None | Some(RestorePhase::Pending) => reconcile_pending(&restore, &ctx, &name, &namespace).await,
        Some(RestorePhase::Restoring) => reconcile_restoring(&restore, &ctx, &name, &namespace).await,
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

    // Set created_at if not set
    if restore.status.as_ref().and_then(|s| s.created_at.as_ref()).is_none() {
        let now = Utc::now().to_rfc3339();
        update_restore_status(client, namespace, name, serde_json::json!({
            "createdAt": now,
            "phase": "Pending",
        }))
        .await?;
    }

    // Delete previous restore's Job for the same replica (log cleanup)
    cleanup_previous_jobs(client, namespace, &restore.spec.replica, name).await?;

    // Create PVC if it doesn't exist
    let pvc_name = format!("{name}-data");
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    if pvcs.get_opt(&pvc_name).await?.is_none() {
        info!(restore = name, pvc = pvc_name, "creating PVC");
        let pvc = build_pvc(restore, &pvc_name, namespace)?;
        pvcs.create(&PostParams::default(), &pvc).await?;
    }

    // Check if PVC is bound
    let pvc = pvcs.get(&pvc_name).await?;
    let pvc_phase = pvc
        .status
        .as_ref()
        .and_then(|s| s.phase.as_deref())
        .unwrap_or("Unknown");

    if pvc_phase != "Bound" {
        info!(restore = name, pvc = pvc_name, phase = pvc_phase, "waiting for PVC to bind");
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    // Update status and transition to Restoring
    update_restore_status(client, namespace, name, serde_json::json!({
        "phase": "Restoring",
        "pvc": pvc_name,
    }))
    .await?;

    // Mark as active in the queue
    let mut queue = ctx.restore_queue.write().await;
    queue.mark_active(name);
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

    // Create or check restore Job
    let job_name = format!("{name}-restore");
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);

    let job = match jobs.get_opt(&job_name).await? {
        Some(job) => job,
        None => {
            info!(restore = name, job = job_name, "creating restore job");

            // Look up the parent replica to get the kopia secret ref
            let replicas: Api<PostgresPhysicalReplica> =
                Api::namespaced(client.clone(), namespace);
            let replica = replicas.get(&restore.spec.replica).await?;

            let job = build_restore_job(restore, &job_name, namespace, &replica)?;
            jobs.create(&PostParams::default(), &job).await?
        }
    };

    // Check job status
    let job_status = &job.status;
    let succeeded = job_status
        .as_ref()
        .and_then(|s| s.succeeded)
        .unwrap_or(0);
    let failed = job_status
        .as_ref()
        .and_then(|s| s.failed)
        .unwrap_or(0);

    if succeeded > 0 {
        info!(restore = name, "restore job succeeded");

        // We can't directly read the postgres version from the PVC here.
        // The Job wrote it to /pgdata/.postgres-version.
        // The Deployment's init container will handle auth setup.
        // For now, we'll track the job completion and move to Ready.
        let now = Utc::now().to_rfc3339();
        let completed_at = job_status
            .as_ref()
            .and_then(|s| s.completion_time.as_ref())
            .map(|t| t.0.to_string())
            .unwrap_or_else(|| now.clone());

        update_restore_status(client, namespace, name, serde_json::json!({
            "phase": "Ready",
            "restoredAt": now,
            "restoreJob": {
                "name": job_name,
                "phase": "Succeeded",
                "completedAt": completed_at,
            },
        }))
        .await?;

        ctx.metrics.restores_completed_total.inc();

        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    // Check for backoff limit exceeded
    let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(3);
    if failed > backoff_limit {
        warn!(restore = name, failed = failed, "restore job failed");

        update_restore_status(client, namespace, name, serde_json::json!({
            "phase": "Failed",
            "restoreJob": {
                "name": job_name,
                "phase": "Failed",
            },
        }))
        .await?;

        // Remove from active queue
        let mut queue = ctx.restore_queue.write().await;
        queue.remove(name);
        ctx.metrics.active_restores.set(queue.active.len() as i64);
        ctx.metrics.queue_depth.set(queue.pending.len() as i64);
        drop(queue);

        ctx.metrics.restores_failed_total.inc();

        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Still running
    update_restore_status(client, namespace, name, serde_json::json!({
        "restoreJob": {
            "name": job_name,
            "phase": "Running",
        },
    }))
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

    // Create Deployment if it doesn't exist
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);

    if deployments.get_opt(name).await?.is_some() {
        // Deployment exists, check if ready
        let deploy = deployments.get(name).await?;
        let ready_replicas = deploy
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0);

        if ready_replicas > 0 {
            info!(restore = name, "deployment ready, transitioning to Switching");
            update_restore_status(client, namespace, name, serde_json::json!({
                "phase": "Switching",
                "deployment": name,
            }))
            .await?;

            // Remove from active queue — restore is done
            let mut queue = ctx.restore_queue.write().await;
            queue.remove(name);
            ctx.metrics.active_restores.set(queue.active.len() as i64);
            ctx.metrics.queue_depth.set(queue.pending.len() as i64);

            return Ok(Action::requeue(Duration::from_secs(5)));
        }

        // Check for timeout (10 minutes)
        if let Some(created_at) = restore.status.as_ref().and_then(|s| s.restored_at.as_ref()) {
            if let Ok(created) = created_at.parse::<chrono::DateTime<Utc>>() {
                let elapsed = Utc::now().signed_duration_since(created);
                if elapsed > chrono::Duration::minutes(10) {
                    warn!(restore = name, "deployment not ready after 10 minutes, marking as Failed");
                    update_restore_status(client, namespace, name, serde_json::json!({
                        "phase": "Failed",
                    }))
                    .await?;

                    let mut queue = ctx.restore_queue.write().await;
                    queue.remove(name);
                    ctx.metrics.active_restores.set(queue.active.len() as i64);
                    ctx.metrics.restores_failed_total.inc();

                    return Ok(Action::requeue(Duration::from_secs(300)));
                }
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

// ─── Resource builders ──────────────────────────────────────────────────────

fn build_pvc(
    restore: &PostgresPhysicalRestore,
    pvc_name: &str,
    namespace: &str,
) -> Result<PersistentVolumeClaim> {
    // Look up storage class from the parent replica if available.
    // For now, use the storage_size from the restore spec.
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

    let script = r#"set -e

echo "Connecting to kopia repository..."
kopia repository connect s3 \
  --bucket="$KOPIA_BUCKET" \
  --region="$KOPIA_REGION" \
  --access-key="$AWS_ACCESS_KEY_ID" \
  --secret-access-key="$AWS_SECRET_ACCESS_KEY" \
  --password="$KOPIA_PASSWORD"

echo "Detecting postgres version..."
VERSION=$(kopia snapshot show "$SNAPSHOT_ID" --json 2>/dev/null | jq -r '.metadata.pg_version // empty' || true)

if [ -z "$VERSION" ]; then
  echo "Version not in metadata, checking PG_VERSION file..."
  kopia snapshot restore "$SNAPSHOT_ID" /tmp/pgcheck \
    --include="**/PG_VERSION" \
    --no-overwrite-files \
    --ignore-errors || true
  VERSION=$(find /tmp/pgcheck -name "PG_VERSION" -exec cat {} \; | head -1)
fi

if [ -z "$VERSION" ]; then
  echo "ERROR: Could not detect postgres version"
  exit 1
fi

echo "Detected postgres version: $VERSION"
echo "$VERSION" > /pgdata/.postgres-version

echo "Starting restore..."
kopia snapshot restore "$SNAPSHOT_ID" /pgdata/postgres

echo "Restore complete"
ls -la /pgdata/
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
                        args: Some(vec![script.to_string()]),
                        env: Some(vec![
                            EnvVar {
                                name: "SNAPSHOT_ID".to_string(),
                                value: Some(restore.spec.snapshot.clone()),
                                ..Default::default()
                            },
                            env_from_secret("KOPIA_BUCKET", kopia_secret, "bucket"),
                            env_from_secret("KOPIA_REGION", kopia_secret, "region"),
                            env_from_secret("AWS_ACCESS_KEY_ID", kopia_secret, "accessKeyId"),
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
                        ]),
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

    // Use a placeholder version — the init container reads .postgres-version
    // and the actual image tag will be set. For now, default to "16".
    // TODO: Read from restore status once the Job writes it.
    let pg_version = restore
        .status
        .as_ref()
        .and_then(|s| s.postgres_version.as_ref())
        .cloned()
        .unwrap_or_else(|| "16".to_string());

    let pg_image = format!("postgres:{pg_version}");
    let pg_alpine_image = format!("postgres:{pg_version}-alpine");

    let read_only = if replica.spec.read_only { "true" } else { "false" };

    let init_script = format!(
        r#"set -e
PGDATA=/pgdata/postgres

echo "Configuring pg_hba.conf..."
cat > "$PGDATA/pg_hba.conf" << 'HBAEOF'
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             postgres                                peer
host    all             all             0.0.0.0/0               scram-sha-256
host    all             all             ::/0                    scram-sha-256
HBAEOF

echo "Creating analytics user setup script..."
mkdir -p /docker-entrypoint-initdb.d
cat > /docker-entrypoint-initdb.d/01-setup-analytics.sql << SQLEOF
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

if [ "{read_only}" = "true" ]; then
  echo "Enabling read-only mode..."
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
    let container_resources = replica.spec.resources.as_ref().map(|r| {
        ResourceRequirements {
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
        }
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
                            env_from_secret(
                                "ANALYTICS_PASSWORD",
                                &creds_secret,
                                "password",
                            ),
                            EnvVar {
                                name: "READ_ONLY".to_string(),
                                value: Some(read_only.to_string()),
                                ..Default::default()
                            },
                        ]),
                        volume_mounts: Some(vec![
                            VolumeMount {
                                name: "pgdata".to_string(),
                                mount_path: "/pgdata".to_string(),
                                ..Default::default()
                            },
                            VolumeMount {
                                name: "initdb".to_string(),
                                mount_path: "/docker-entrypoint-initdb.d".to_string(),
                                ..Default::default()
                            },
                        ]),
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
                            "/pgdata/postgres".to_string(),
                        ]),
                        env: Some(vec![
                            EnvVar {
                                name: "PGDATA".to_string(),
                                value: Some("/pgdata/postgres".to_string()),
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
                        volume_mounts: Some(vec![
                            VolumeMount {
                                name: "pgdata".to_string(),
                                mount_path: "/pgdata".to_string(),
                                ..Default::default()
                            },
                            VolumeMount {
                                name: "initdb".to_string(),
                                mount_path: "/docker-entrypoint-initdb.d".to_string(),
                                ..Default::default()
                            },
                        ]),
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
                    volumes: Some(vec![
                        Volume {
                            name: "pgdata".to_string(),
                            persistent_volume_claim: Some(
                                k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                                    claim_name: pvc_name,
                                    read_only: Some(false),
                                },
                            ),
                            ..Default::default()
                        },
                        Volume {
                            name: "initdb".to_string(),
                            empty_dir: Some(Default::default()),
                            ..Default::default()
                        },
                    ]),
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

fn env_from_secret(env_name: &str, secret_name: &str, key: &str) -> EnvVar {
    EnvVar {
        name: env_name.to_string(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: secret_name.to_string(),
                key: key.to_string(),
                optional: Some(false),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

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
        .list(
            &kube::api::ListParams::default()
                .labels(&format!("bes.au/replica={replica_name}")),
        )
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
