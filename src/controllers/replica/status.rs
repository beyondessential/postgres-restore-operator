use jiff::Timestamp;
use k8s_openapi::{api::core::v1::Secret, apimachinery::pkg::apis::meta::v1::Time};
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

impl PostgresPhysicalReplica {
	/// Atomically update `nextScheduledRestore` and `scheduleInputHash` in a
	/// single status patch, preventing race conditions where a reconcile
	/// re-triggers between two separate field updates.
	pub async fn update_schedule_status(
		&self,
		client: &Client,
		next: Timestamp,
		input_hash: &str,
	) -> Result<()> {
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(
			client.clone(),
			self.metadata
				.namespace
				.as_deref()
				.expect("PostgresPhysicalReplica is a namespaced resource"),
		);
		let patch = serde_json::json!({
			"status": {
				"nextScheduledRestore": Time(next),
				"scheduleInputHash": input_hash,
			}
		});
		replicas
			.patch_status(
				self.metadata
					.name
					.as_deref()
					.expect("cannot be called on new resource"),
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		Ok(())
	}

	pub async fn update_condition(
		&self,
		client: &Client,
		type_: &str,
		status: &str,
		reason: &str,
		message: &str,
	) -> Result<()> {
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(
			client.clone(),
			self.metadata
				.namespace
				.as_deref()
				.expect("PostgresPhysicalReplica is a namespaced resource"),
		);
		let now = Timestamp::now();
		let patch = serde_json::json!({
			"status": {
				"conditions": [{
					"type": type_,
					"status": status,
					"reason": reason,
					"message": message,
					"lastTransitionTime": Time(now),
				}]
			}
		});
		replicas
			.patch_status(
				self.metadata
					.name
					.as_deref()
					.expect("cannot be called on new resource"),
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		Ok(())
	}

	pub async fn update_phase(&self, client: &Client, phase: ReplicaPhase) -> Result<()> {
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(
			client.clone(),
			self.metadata
				.namespace
				.as_deref()
				.expect("PostgresPhysicalReplica is a namespaced resource"),
		);
		let patch = serde_json::json!({
			"status": {
				"phase": phase,
			}
		});
		replicas
			.patch_status(
				self.metadata
					.name
					.as_deref()
					.expect("cannot be called on new resource"),
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		Ok(())
	}

	pub async fn update_status_field(
		&self,
		client: &Client,
		field: &str,
		value: impl serde::Serialize,
	) -> Result<()> {
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(
			client.clone(),
			self.metadata
				.namespace
				.as_deref()
				.expect("PostgresPhysicalReplica is a namespaced resource"),
		);
		let patch = serde_json::json!({
			"status": {
				(field): value,
			}
		});
		replicas
			.patch_status(
				self.metadata
					.name
					.as_deref()
					.expect("cannot be called on new resource"),
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		Ok(())
	}

	pub fn conn_info(&self) -> ConnectionInfo {
		ConnectionInfo {
			host: format!("{}.{}.svc.cluster.local", self.name_any(), self.ns()),
			port: 5432,
			database: "postgres".to_string(),
			username: self.spec.analytics_username.clone(),
			password_secret: self.creds_secret_name(),
		}
	}

	pub async fn update_connection_info(&self, client: &Client) -> Result<()> {
		let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), &self.ns());
		let patch = serde_json::json!({
			"status": {
				"connectionInfo": self.conn_info(),
			}
		});
		replicas
			.patch_status(
				&self.name_any(),
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		Ok(())
	}

	pub async fn send_notifications(
		&self,
		client: &Client,
		http_client: &reqwest::Client,
		restore: &PostgresPhysicalRestore,
		metrics: &crate::metrics::Metrics,
	) {
		let replica_name = self.name_any();

		let mut statuses: Vec<NotificationStatus> = Vec::new();

		for notif_config in &self.spec.notifications {
			// Resolve password if includePassword is true
			let password = if notif_config.include_password() {
				let secrets: Api<Secret> = Api::namespaced(client.clone(), &self.ns());
				secrets
					.get(&self.creds_secret_name())
					.await
					.ok()
					.and_then(|s| s.data)
					.and_then(|d| d.get("password").cloned())
					.map(|b| String::from_utf8(b.0).unwrap_or_default())
			} else {
				None
			};

			let payload = NotificationPayload {
				timestamp: Timestamp::now(),
				replica: ReplicaRef {
					name: replica_name.clone(),
					namespace: self.ns(),
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
				connection_info: ConnectionInfoPayload::from_connection_info(
					&self.conn_info(),
					password,
				),
			};

			let status = notifications::send_notification(
				client,
				http_client,
				&self.ns(),
				notif_config,
				&payload,
			)
			.await;

			if status.success {
				metrics
					.notifications_sent_total
					.with_label_values(&[notif_config.name()])
					.inc();
			} else {
				metrics
					.notifications_failed_total
					.with_label_values(&[notif_config.name()])
					.inc();
			}

			statuses.push(status);
		}

		if !statuses.is_empty() {
			let replicas: Api<PostgresPhysicalReplica> =
				Api::namespaced(client.clone(), &self.ns());
			let patch = serde_json::json!({
				"status": {
					"notifications": statuses,
				}
			});
			if let Err(e) = replicas
				.patch_status(
					self.metadata
						.name
						.as_deref()
						.expect("cannot be called on new resource"),
					&PatchParams::apply("postgres-restore-operator"),
					&Patch::Merge(&patch),
				)
				.await
			{
				warn!(replica = replica_name, error = %e, "failed to update notification status");
			}
		}
	}
}

impl PostgresPhysicalRestore {
	pub async fn update_phase(&self, client: &Client, phase: RestorePhase) -> Result<()> {
		let restores: Api<Self> = Api::namespaced(
			client.clone(),
			self.metadata
				.namespace
				.as_deref()
				.expect("PostgresPhysicalRestore is a namespaced resource"),
		);
		let patch = serde_json::json!({
			"status": {
				"phase": phase,
			}
		});
		restores
			.patch_status(
				self.metadata
					.name
					.as_deref()
					.expect("cannot be called on new resource"),
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		Ok(())
	}

	pub async fn update_activated_at(&self, client: &Client, timestamp: Time) -> Result<()> {
		let restores: Api<Self> = Api::namespaced(
			client.clone(),
			self.metadata
				.namespace
				.as_deref()
				.expect("PostgresPhysicalRestore is a namespaced resource"),
		);
		let patch = serde_json::json!({
			"status": {
				"activatedAt": timestamp,
			}
		});
		restores
			.patch_status(
				self.metadata
					.name
					.as_deref()
					.expect("cannot be called on new resource"),
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		Ok(())
	}
}
