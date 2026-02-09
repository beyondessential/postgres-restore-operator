use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::Utc;
use k8s_openapi::{
    ByteString,
    api::core::v1::{Secret, Service, ServicePort, ServiceSpec},
    apimachinery::pkg::{apis::meta::v1::OwnerReference, util::intstr::IntOrString},
};
use kube::{
    Api, Client, ResourceExt,
    api::{ObjectMeta, Patch, PatchParams, PostParams},
    runtime::controller::Action,
};
use rand::RngExt;
use tracing::{info, warn};

use crate::{
    context::Context,
    error::{Error, Result},
    kopia,
    notifications::{self, ConnectionInfoPayload, NotificationPayload, ReplicaRef, RestoreRef},
    types::*,
    util::parse_duration,
};

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
        )
        .await;

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
        // Already have a restore in progress, don't trigger another
        update_replica_phase(client, &namespace, &name, ReplicaPhase::Restoring).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // TODO: Query kopia for latest snapshot. For now, this is a placeholder
    // that will be implemented when we have the kopia CLI integration via Jobs.
    // The operator doesn't directly connect to kopia — it delegates to Jobs.
    // Snapshot detection will happen through:
    //   a) Schedule-based triggers (cron + jitter)
    //   b) External triggers (e.g., webhook or annotation)
    //
    // For now, we check if a restore should be triggered based on schedule.
    let should_restore = should_trigger_scheduled_restore(&replica);

    if should_restore {
        // Check concurrent restore limit
        let mut queue = ctx.restore_queue.write().await;
        if !queue.can_start(ctx.max_concurrent_restores) {
            let restore_name = format!("{name}-pending");
            queue.enqueue(restore_name);
            let position = queue.position(&format!("{name}-pending"));
            let pending_len = queue.pending.len();
            drop(queue);

            // Update queue position in status
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

        // Note: actual restore creation will require a snapshot ID,
        // which comes from kopia snapshot listing (done via Job or direct API).
        // This is the integration point where the snapshot ID flows in.
        info!(
            replica = name,
            "schedule triggered, ready to create restore when snapshot is available"
        );
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

    // Find the last scheduled time before now
    let now = Utc::now();
    let jittered_now = now - chrono::Duration::from_std(jitter).unwrap_or_default();

    if let Some(next_scheduled) = status.and_then(|s| s.next_scheduled_restore.as_ref()) {
        if let Ok(next) = next_scheduled.parse::<chrono::DateTime<Utc>>() {
            // Add jitter to the scheduled time
            let trigger_at = next + chrono::Duration::from_std(jitter).unwrap_or_default();
            return now >= trigger_at;
        }
    }

    // No next_scheduled set — check if cron says we're due
    // Look at the previous occurrence from the cron schedule
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
            field: value,
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
) {
    let replica_name = replica.name_any();

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
            &NotificationEvent::RestoreComplete,
            &payload,
        )
        .await;

        // Update notification status on the replica
        let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), namespace);
        let patch = serde_json::json!({
            "status": {
                "notifications": [status],
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
}
