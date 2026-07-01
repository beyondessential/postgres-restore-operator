//! POST `RestoreVerification` back to canopy on restore success / failure.
//!
//! Canopy sees the whole restore cycle as three signals: it dispatches an
//! entry (signal 1), pgro reports the outcome (signal 3), canopy closes the
//! loop. This module owns signal 3 — one function called at each terminal
//! transition (switchover success, restore failure).

use bestool_canopy::{Outcome, RestoreVerification};
use jiff::Timestamp;
use kube::ResourceExt;
use serde::Deserialize;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
	context::Context,
	controllers::canopy::labels,
	types::{PostgresPhysicalReplica, PostgresPhysicalRestore},
};

/// The sidecar posts this JSON shape to `/api/v1/canopy-stats/{ns}/{job}`
/// on exit; kept in sync with `src/bin/canopy_proxy.rs::StatsFile`.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanopyStats {
	pub sent_raw_bytes: u64,
	pub sent_payload_bytes: u64,
	pub received_raw_bytes: u64,
	pub received_payload_bytes: u64,
}

/// Report the outcome of a canopy-managed restore to canopy. No-op for
/// non-canopy replicas (missing `spec.canopy_source`) and when the canopy
/// client isn't configured.
pub async fn report(
	ctx: &Context,
	replica: &PostgresPhysicalReplica,
	restore: &PostgresPhysicalRestore,
	outcome: Outcome,
	error: Option<&str>,
) {
	if replica.spec.canopy_source.is_none() {
		return;
	}
	let Some(canopy) = ctx.canopy.as_ref() else {
		return;
	};

	let labels = replica.labels();
	let Some(group) = labels
		.get(labels::GROUP)
		.and_then(|s| Uuid::parse_str(s).ok())
	else {
		warn!(
			replica = %replica.name_any(),
			"canopy verification: replica CR missing {} label, skipping report",
			labels::GROUP,
		);
		return;
	};
	let Some(server_id) = labels
		.get(labels::SERVER)
		.and_then(|s| Uuid::parse_str(s).ok())
	else {
		warn!(
			replica = %replica.name_any(),
			"canopy verification: replica CR missing {} label, skipping report",
			labels::SERVER,
		);
		return;
	};
	let replica_id = labels
		.get(labels::DECLARATION_ID)
		.and_then(|s| Uuid::parse_str(s).ok());
	let backup_type = labels
		.get(labels::TYPE)
		.map(String::as_str)
		.unwrap_or_default()
		.to_string();
	let intent = labels
		.get(labels::INTENT)
		.map(String::as_str)
		.unwrap_or_default()
		.to_string();

	let restore_job = format!("{}-restore", restore.name_any());
	let stats = ctx
		.canopy_stats
		.take(&replica.namespace().unwrap_or_default(), &restore_job)
		.and_then(|raw| match serde_json::from_str::<CanopyStats>(&raw) {
			Ok(s) => Some(s),
			Err(err) => {
				warn!(
					restore = %restore.name_any(),
					error = %err,
					"canopy verification: failed to parse sidecar stats JSON"
				);
				None
			}
		})
		.unwrap_or_default();

	let postgres_version = restore
		.status
		.as_ref()
		.and_then(|s| s.postgres_version.as_deref());

	let replica_healthy = matches!(outcome, Outcome::Success);

	let report = RestoreVerification {
		replica_id,
		group,
		server_id,
		r#type: &backup_type,
		intent: &intent,
		snapshot_id: Some(restore.spec.snapshot.as_str()),
		outcome,
		error,
		replica_healthy,
		postgres_version,
		observed_at: Timestamp::now(),
		s3_sent_raw_bytes: Some(stats.sent_raw_bytes as i64),
		s3_sent_payload_bytes: Some(stats.sent_payload_bytes as i64),
		s3_received_raw_bytes: Some(stats.received_raw_bytes as i64),
		s3_received_payload_bytes: Some(stats.received_payload_bytes as i64),
	};

	match canopy.restore_verification(&report).await {
		Ok(()) => info!(
			replica = %replica.name_any(),
			restore = %restore.name_any(),
			?outcome,
			"canopy verification reported"
		),
		Err(err) => warn!(
			replica = %replica.name_any(),
			restore = %restore.name_any(),
			error = %err,
			"canopy verification report failed"
		),
	}
}
