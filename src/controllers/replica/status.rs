use chrono::Utc;
use k8s_openapi::api::core::v1::Secret;
use kube::{
	Api, Client, ResourceExt,
	api::{Patch, PatchParams},
};
use tracing::warn;

use crate::{
	error::Result,
	notifications::{self, ConnectionInfoPayload, NotificationPayload, ReplicaRef, RestoreRef},
	types::*,
};

pub async fn update_replica_condition(
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

pub async fn update_replica_phase(
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

pub async fn update_replica_status_field(
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

pub async fn update_replica_connection_info(
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

pub async fn update_restore_phase(
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

pub async fn update_restore_activated_at(
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

#[expect(
	clippy::too_many_arguments,
	reason = "notification dispatch needs all these context params"
)]
pub async fn send_restore_notifications(
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
