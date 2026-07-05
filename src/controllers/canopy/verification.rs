//! POST `RestoreVerification` back to canopy on restore success / failure.
//!
//! Canopy sees the whole restore cycle as three signals: it dispatches an
//! entry (signal 1), pgro reports the outcome (signal 3), canopy closes the
//! loop. This module owns signal 3 — one function called at each terminal
//! transition (switchover success, restore failure).

use bestool_canopy::schema::{RunOutcome, VerificationArgs};
use jiff::Timestamp;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, ResourceExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
	context::Context,
	controllers::{canopy::labels, postgres},
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
	outcome: RunOutcome,
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
		.and_then(|s| s.postgres_version.clone());

	// The canopy run-uuid minted when this run's Job was created (persisted on
	// the restore status). Reported so canopy can correlate the report with the
	// run's credential requests.
	let run_id = run_id_from_status(restore);

	let replica_healthy = matches!(outcome, RunOutcome::Success);

	// Gather health details; send None rather than an empty object when
	// nothing was gathered (e.g. failure path, postgres unreachable).
	let mut health = gather_health_details(ctx, replica, restore).await;
	// `url` semantic: for a replica exposed on the tailnet, attach a link to
	// it so canopy can surface it to operators alongside the report.
	if let Some(url) = exposed_replica_url(ctx, replica).await
		&& let Some(obj) = health.as_object_mut()
	{
		obj.insert("url".to_string(), json!(url));
	}
	let health_details = health
		.as_object()
		.is_some_and(|m| !m.is_empty())
		.then_some(health);

	// Typed request body generated from canopy's OpenAPI (bestool#628).
	// Constructing it here means the field set is checked against canopy's
	// spec at compile time; `health_details` stays free-form by design.
	let args = VerificationArgs::builder()
		.maybe_replica_id(replica_id)
		.maybe_run_id(run_id)
		.group(group)
		.server_id(server_id)
		.type_(backup_type.clone())
		.intent(intent.clone())
		.snapshot_id(restore.spec.snapshot.clone())
		.outcome(outcome_wire(outcome).to_string())
		.maybe_error(error.map(str::to_string))
		.replica_healthy(replica_healthy)
		.maybe_postgres_version(postgres_version)
		.observed_at(Timestamp::now().to_string())
		.s3_sent_raw_bytes(stats.sent_raw_bytes as i64)
		.s3_sent_payload_bytes(stats.sent_payload_bytes as i64)
		.s3_received_raw_bytes(stats.received_raw_bytes as i64)
		.s3_received_payload_bytes(stats.received_payload_bytes as i64)
		.maybe_health_details(health_details)
		.build();

	match canopy.restore_verification_typed(&args).await {
		Ok(()) => info!(
			replica = %replica.name_any(),
			restore = %restore.name_any(),
			?outcome,
			health_details = %args.health_details.as_ref().map(|v| v.to_string()).unwrap_or_default(),
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

/// The canopy run-uuid persisted on the restore status, parsed to a `Uuid`.
/// A malformed value is treated as absent rather than failing the report —
/// canopy still accepts a report without a run_id while the field is optional.
fn run_id_from_status(restore: &PostgresPhysicalRestore) -> Option<Uuid> {
	restore
		.status
		.as_ref()
		.and_then(|s| s.run_id.as_deref())
		.and_then(|s| Uuid::parse_str(s).ok())
}

/// Wire string canopy expects for the `outcome` field (matches the
/// lowercase serialization of `bestool_canopy::Outcome`).
fn outcome_wire(outcome: RunOutcome) -> &'static str {
	match outcome {
		RunOutcome::Success => "success",
		RunOutcome::Failure => "failure",
	}
}

/// The tailnet URL of a replica exposed via the `url` semantic, or `None` if
/// it isn't exposed or the MagicDNS suffix can't be resolved. The hostname is
/// read from the Service's `tailscale.com/hostname` annotation the intent set,
/// so the reported URL matches exactly what tailscale publishes.
async fn exposed_replica_url(ctx: &Context, replica: &PostgresPhysicalReplica) -> Option<String> {
	let annotations = replica.spec.service_annotations.as_ref()?;
	if annotations.get("tailscale.com/expose").map(String::as_str) != Some("true") {
		return None;
	}
	let hostname = annotations.get("tailscale.com/hostname")?;
	let suffix = ctx.magic_dns_suffix().await?;
	Some(crate::tailscale::replica_url(hostname, &suffix))
}

/// Best-effort gather of the `health_details` map (snake_case keys):
/// `{ sizes: {<db>: bytes}, fixes: {reindex, locale}, restore_duration_sec }`.
///
/// `sizes` and `fixes` come from a single read-only connection to the
/// restore's postgres (`sizes` from `pg_database_size`, `fixes` from the
/// `_pgro.restore_info` flags the init recorded). Any connection or query
/// failure — expected on the failure path, where postgres may never have
/// come up — just omits that piece; the verification still sends.
/// `restore_duration_sec` is the wall-clock from the restore CR's
/// `createdAt` to now (≈ activation), independent of postgres.
async fn gather_health_details(
	ctx: &Context,
	replica: &PostgresPhysicalReplica,
	restore: &PostgresPhysicalRestore,
) -> Value {
	let duration_sec = restore
		.status
		.as_ref()
		.and_then(|s| s.created_at.as_ref())
		.map(|created| Timestamp::now().duration_since(created.0).as_secs().max(0) as u64);

	let pg = match gather_from_postgres(ctx, replica, restore).await {
		Ok(parts) => Some(parts),
		// Expected on the failure path (postgres may never have come up) and
		// on ephemeral replicas racing teardown; the verification still sends.
		Err(err) => {
			warn!(
				replica = %replica.name_any(),
				restore = %restore.name_any(),
				error = %err,
				"canopy verification: could not gather sizes/fixes; reporting without them"
			);
			None
		}
	};

	build_health_details(duration_sec, pg)
}

/// Postgres-derived pieces of the health details: per-database sizes and
/// the `fixes` map the restore init recorded. `fixes` is an arbitrary JSON
/// object (`{locale, reindex, reset_wal, ...}`) forwarded verbatim, so new
/// fix flags flow through without operator changes.
struct PostgresHealth {
	sizes: Vec<(String, u64)>,
	fixes: Value,
}

/// Assemble the `health_details` JSON from already-gathered parts. Pure, so
/// the snake_case wire shape is unit-testable without a database. Omits any
/// piece that wasn't gathered.
fn build_health_details(duration_sec: Option<u64>, pg: Option<PostgresHealth>) -> Value {
	let mut details = serde_json::Map::new();
	if let Some(secs) = duration_sec {
		details.insert("restore_duration_sec".into(), json!(secs));
	}
	if let Some(pg) = pg {
		let sizes_obj: serde_json::Map<String, Value> = pg
			.sizes
			.into_iter()
			.map(|(name, bytes)| (name, json!(bytes)))
			.collect();
		details.insert("sizes".into(), Value::Object(sizes_obj));
		details.insert("fixes".into(), pg.fixes);
	}
	Value::Object(details)
}

/// Connect to the restore's postgres (as the replica's reader user) and
/// read the per-database sizes + fix flags. Returns
/// `(sizes, (locale_fixed, reindex_done))`.
async fn gather_from_postgres(
	ctx: &Context,
	replica: &PostgresPhysicalReplica,
	restore: &PostgresPhysicalRestore,
) -> crate::error::Result<PostgresHealth> {
	let namespace = replica.namespace().unwrap_or_default();
	let restore_name = restore.name_any();

	let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &namespace);
	let reader_secret = secrets.get(&replica.creds_secret_name()).await?;
	let reader_user = postgres::read_secret_field(&reader_secret, "username")?;
	let reader_password = postgres::read_secret_field(&reader_secret, "password")?;

	let conn = postgres::connect_to_restore(
		&ctx.client,
		&namespace,
		&restore_name,
		"postgres",
		&reader_user,
		&reader_password,
		ctx.use_port_forward(),
	)
	.await?;

	let sizes = postgres::list_database_sizes(&conn.client).await?;
	let fixes = postgres::read_restore_fixes(&conn.client)
		.await
		.unwrap_or_else(|_| json!({}));
	Ok(PostgresHealth { sizes, fixes })
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn health_details_shape_is_snake_case() {
		let v = build_health_details(
			Some(700),
			Some(PostgresHealth {
				sizes: vec![("tamanu".into(), 1_872_782), ("postgres".into(), 12_829)],
				fixes: json!({ "locale": true, "reindex": false, "reset_wal": false }),
			}),
		);
		assert_eq!(v["restore_duration_sec"], json!(700));
		assert_eq!(v["sizes"]["tamanu"], json!(1_872_782u64));
		assert_eq!(v["sizes"]["postgres"], json!(12_829u64));
		// fixes is forwarded verbatim, so new flags pass through untouched.
		assert_eq!(v["fixes"]["locale"], json!(true));
		assert_eq!(v["fixes"]["reindex"], json!(false));
		assert_eq!(v["fixes"]["reset_wal"], json!(false));
	}

	#[test]
	fn health_details_omits_ungathered_parts() {
		// Failure path: no postgres connection, no createdAt.
		let v = build_health_details(None, None);
		assert_eq!(v, json!({}));
		// Duration known but postgres unreachable: sizes/fixes omitted.
		let v = build_health_details(Some(42), None);
		assert_eq!(v, json!({ "restore_duration_sec": 42 }));
		assert!(v.get("sizes").is_none());
		assert!(v.get("fixes").is_none());
	}

	fn restore_with_run_id(run_id: Option<&str>) -> PostgresPhysicalRestore {
		use k8s_openapi::api::core::v1::LocalObjectReference;
		use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

		use crate::types::{PostgresPhysicalRestoreSpec, PostgresPhysicalRestoreStatus};

		let mut restore = PostgresPhysicalRestore::new(
			"r",
			PostgresPhysicalRestoreSpec {
				replica: LocalObjectReference {
					name: "rep".to_string(),
				},
				snapshot: "snap".to_string(),
				snapshot_size: Quantity("1Gi".to_string()),
				snapshot_time: None,
				storage_size: Quantity("2Gi".to_string()),
			},
		);
		restore.status = Some(PostgresPhysicalRestoreStatus {
			run_id: run_id.map(str::to_string),
			..Default::default()
		});
		restore
	}

	#[test]
	fn run_id_from_status_parses_valid_uuid() {
		let id = "44444444-4444-4444-4444-444444444444";
		assert_eq!(
			run_id_from_status(&restore_with_run_id(Some(id))),
			Some(Uuid::parse_str(id).unwrap())
		);
	}

	#[test]
	fn run_id_from_status_absent_or_malformed_is_none() {
		assert_eq!(run_id_from_status(&restore_with_run_id(None)), None);
		assert_eq!(
			run_id_from_status(&restore_with_run_id(Some("not-a-uuid"))),
			None,
			"a malformed run_id is tolerated as absent, not a hard error"
		);
	}
}
