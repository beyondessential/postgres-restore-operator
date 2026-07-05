//! Canopy worklist syncer — pgro's third top-level controller.
//!
//! Ticks periodically (default 30s, jittered ±20%), fetches
//! `GET /restore-worklist`, and reconciles the diff into pgro's own
//! `PostgresPhysicalReplica` CRs. Each entry lives in its own labelled
//! Namespace and carries a canopy-creds Secret + a
//! `PostgresPhysicalReplica` CR with `spec.canopySource` set. The replica
//! and restore controllers pick up from there.
//!
//! This module owns the "canopy is desired-state, pgro reconciles" edge
//! only. Everything downstream — Job creation, Deployment, verification —
//! goes through the same CR machinery as legacy replicas.

use std::{
	collections::{BTreeMap, HashSet},
	sync::Arc,
	time::Duration,
};

use bestool_canopy::schema::WorklistEntry;
use futures::stream::{self, StreamExt};
use k8s_openapi::{
	ByteString,
	api::core::v1::{Namespace, Secret},
	apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time},
};
use kube::{
	Api, ResourceExt,
	api::{DeleteParams, ListParams, Patch, PatchParams, PostParams},
};
use rand::RngExt;
use sha2::{Digest, Sha256};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
	context::Context,
	controllers::canopy::intent::{IntentConfig, config_for},
	error::Result,
	types::{PostgresPhysicalReplica, PostgresPhysicalReplicaSpec},
};

pub mod intent;
pub mod verification;

/// How many per-entry reconciliations run concurrently within one tick.
/// Keeps the k8s apiserver from being hit by a stampede when the worklist
/// is large.
const RECONCILE_CONCURRENCY: usize = 8;

/// Jitter multiplier applied to the reconcile interval each tick (±20%).
const JITTER_RATIO: f64 = 0.2;

/// Labels applied to canopy-managed Namespaces and CRs.
pub mod labels {
	pub const MANAGED_BY: &str = "pgro.bes.au/managed-by";
	pub const MANAGED_BY_VALUE: &str = "pgro-canopy";
	pub const DECLARATION_ID: &str = "pgro.bes.au/declaration-id";
	pub const GROUP: &str = "pgro.bes.au/group";
	pub const SERVER: &str = "pgro.bes.au/server";
	pub const TYPE: &str = "pgro.bes.au/type";
	pub const INTENT: &str = "pgro.bes.au/intent";
}

/// Compute the k8s Namespace name for a worklist entry.
///
/// Format: `<slug(entry.name)>-<8-hex(SHA-256(replica_id || server_id))>`.
pub fn namespace_name_for(entry: &WorklistEntry) -> String {
	format!(
		"{}-{}",
		slug(&entry.name),
		short_hash(entry.replica_id, entry.server_id),
	)
}

/// Slugify to DNS-1123-label-safe: lowercased, non-alphanumeric runs → `-`,
/// leading/trailing `-` trimmed, truncated to 50 chars (leaves ≥9 chars for
/// the `-XXXXXXXX` disambiguator suffix in a 63-char label limit).
fn slug(s: &str) -> String {
	let mut out = String::with_capacity(s.len());
	let mut prev_dash = true;
	for c in s.chars() {
		let mapped = if c.is_ascii_alphanumeric() {
			c.to_ascii_lowercase()
		} else {
			'-'
		};
		if mapped == '-' {
			if !prev_dash {
				out.push('-');
				prev_dash = true;
			}
		} else {
			out.push(mapped);
			prev_dash = false;
		}
	}
	while out.ends_with('-') {
		out.pop();
	}
	while out.starts_with('-') {
		out.remove(0);
	}
	if out.is_empty() {
		out.push_str("replica");
	}
	if out.len() > 50 {
		out.truncate(50);
		while out.ends_with('-') {
			out.pop();
		}
	}
	out
}

/// Apply ±20% jitter to a Duration. Scopes the (non-Send) thread rng so the
/// caller can `.await` after receiving the result.
fn jittered(base: Duration) -> Duration {
	let mut rng = rand::rng();
	let jitter = rng.random_range(-JITTER_RATIO..JITTER_RATIO);
	base.mul_f64(1.0 + jitter)
}

/// 8-hex-char disambiguator from SHA-256 of `replica_id || server_id`.
fn short_hash(replica_id: Uuid, server_id: Uuid) -> String {
	let mut hasher = Sha256::new();
	hasher.update(replica_id.as_bytes());
	hasher.update(server_id.as_bytes());
	let digest = hasher.finalize();
	let mut out = String::with_capacity(8);
	for byte in &digest[..4] {
		out.push_str(&format!("{byte:02x}"));
	}
	out
}

/// Name of the canopy-owned CR inside each per-replica Namespace. Fixed —
/// each namespace holds exactly one canopy-managed CR.
pub const CR_NAME: &str = "canopy-replica";

/// The syncer controller. Holds a shared `Context`; runs a periodic tick
/// via [`Self::run_forever`], or one-shot via [`Self::tick`] for tests.
pub struct CanopyController {
	ctx: Arc<Context>,
	interval: Duration,
}

impl CanopyController {
	pub fn new(ctx: Arc<Context>, interval: Duration) -> Self {
		Self { ctx, interval }
	}

	/// Run the syncer forever with jittered ticks. Returns only if the
	/// canopy client is not configured on `ctx` (legacy-only mode).
	pub async fn run_forever(&self) {
		if self.ctx.canopy.is_none() {
			info!("canopy client not configured; skipping worklist syncer");
			return;
		}
		loop {
			tokio::time::sleep(jittered(self.interval)).await;
			if let Err(err) = self.tick().await {
				error!(error = %err, "canopy worklist syncer tick failed");
			}
		}
	}

	/// One reconciliation pass. Fetches the worklist, lists managed
	/// Namespaces, provisions CRs where missing / patches desired-snapshot
	/// where changed / tears down orphans.
	pub async fn tick(&self) -> Result<()> {
		let Some(canopy) = self.ctx.canopy.as_ref() else {
			return Ok(());
		};
		let entries = canopy.worklist().await?;
		debug!(count = entries.len(), "fetched canopy worklist");

		let ns_api: Api<Namespace> = Api::all(self.ctx.client.clone());
		let params = ListParams::default().labels(&format!(
			"{}={}",
			labels::MANAGED_BY,
			labels::MANAGED_BY_VALUE
		));
		let namespaces = ns_api.list(&params).await?.items;

		let entry_ns_names: HashSet<String> = entries.iter().map(namespace_name_for).collect();

		info!(
			worklist_entries = entries.len(),
			existing_namespaces = namespaces.len(),
			"canopy worklist syncer tick"
		);

		// Provision / refresh each entry concurrently.
		let ctx = self.ctx.clone();
		stream::iter(entries)
			.for_each_concurrent(RECONCILE_CONCURRENCY, |entry| {
				let ctx = ctx.clone();
				async move {
					let ns_name = namespace_name_for(&entry);
					if let Err(err) = reconcile_entry(&ctx, &ns_name, &entry).await {
						warn!(
							namespace = %ns_name,
							replica_id = %entry.replica_id,
							error = %err,
							"canopy reconcile_entry failed"
						);
					}
				}
			})
			.await;

		// Teardown namespaces with no matching worklist entry.
		let orphans: Vec<Namespace> = namespaces
			.into_iter()
			.filter(|ns| !entry_ns_names.contains(&ns.name_any()))
			.collect();
		let ctx = self.ctx.clone();
		stream::iter(orphans)
			.for_each_concurrent(RECONCILE_CONCURRENCY, |ns| {
				let ctx = ctx.clone();
				async move {
					let ns_name = ns.name_any();
					if let Err(err) = teardown(&ctx, &ns_name).await {
						warn!(
							namespace = %ns_name,
							error = %err,
							"canopy teardown failed"
						);
					}
				}
			})
			.await;

		Ok(())
	}
}

/// Ensure the namespace, canopy-creds Secret, and PostgresPhysicalReplica
/// CR exist and reflect the worklist entry. Patches
/// `status.canopyDesiredSnapshotId` when canopy is offering a snapshot the
/// CR hasn't seen yet.
async fn reconcile_entry(ctx: &Context, ns_name: &str, entry: &WorklistEntry) -> Result<()> {
	let Some(intent) = config_for(&entry.intent) else {
		warn!(
			namespace = %ns_name,
			intent = %entry.intent,
			"worklist entry has unsupported intent — skipping"
		);
		return Ok(());
	};

	ensure_namespace(ctx, ns_name, entry).await?;

	// Fetch the repo password (canopy's restore-credentials endpoint carries
	// it alongside chained STS creds; the sidecar refreshes the STS half
	// out-of-band). Bucket/region/prefix come from the worklist entry
	// directly.
	let Some(canopy) = ctx.canopy.as_ref() else {
		return Ok(());
	};
	// This per-tick fetch exists only to read the repo password for the creds
	// Secret; it is not a restore run, so it carries no run_id.
	let creds = canopy
		.restore_credentials(&entry.type_, entry.group_id, None)
		.await?;

	ensure_canopy_creds_secret(ctx, ns_name, entry, &creds.repo_password.0).await?;
	ensure_replica_cr(ctx, ns_name, entry, &intent).await?;
	set_desired_snapshot(ctx, ns_name, entry).await?;

	Ok(())
}

async fn ensure_namespace(ctx: &Context, ns_name: &str, entry: &WorklistEntry) -> Result<()> {
	let mut labels_map = BTreeMap::new();
	labels_map.insert(labels::MANAGED_BY.into(), labels::MANAGED_BY_VALUE.into());
	labels_map.insert(labels::DECLARATION_ID.into(), entry.replica_id.to_string());
	labels_map.insert(labels::GROUP.into(), entry.group_id.to_string());
	labels_map.insert(labels::SERVER.into(), entry.server_id.to_string());
	labels_map.insert(labels::TYPE.into(), entry.type_.clone());
	labels_map.insert(labels::INTENT.into(), entry.intent.to_string());

	let ns = Namespace {
		metadata: ObjectMeta {
			name: Some(ns_name.to_string()),
			labels: Some(labels_map),
			..Default::default()
		},
		..Default::default()
	};

	let ns_api: Api<Namespace> = Api::all(ctx.client.clone());
	match ns_api.create(&PostParams::default(), &ns).await {
		Ok(_) => info!(namespace = %ns_name, "canopy: created replica namespace"),
		Err(kube::Error::Api(err)) if err.code == 409 => {
			debug!(namespace = %ns_name, "canopy: namespace already exists");
		}
		Err(err) => return Err(err.into()),
	}
	Ok(())
}

/// Materialise the canopy-creds Secret with the entry's bucket / region /
/// prefix / repo password + dummy AWS keys. Kopia reads these via
/// `env_from_secret` in the restore Job; the proxy sidecar overrides the
/// AWS auth by re-signing upstream with real (refreshed) STS creds.
async fn ensure_canopy_creds_secret(
	ctx: &Context,
	ns_name: &str,
	entry: &WorklistEntry,
	repo_password: &str,
) -> Result<()> {
	let secret_name = IntentConfig::canopy_creds_secret_name(CR_NAME);

	let mut data: BTreeMap<String, ByteString> = BTreeMap::new();
	data.insert(
		"bucket".into(),
		ByteString(entry.bucket.clone().into_bytes()),
	);
	data.insert(
		"region".into(),
		ByteString(entry.region.clone().into_bytes()),
	);
	data.insert(
		"prefix".into(),
		ByteString(entry.prefix.clone().into_bytes()),
	);
	data.insert(
		"repositoryPassword".into(),
		ByteString(repo_password.as_bytes().to_vec()),
	);
	// Dummy AWS keys — kopia carries these to satisfy its s3 backend arg
	// validation, but every request is re-signed by the proxy sidecar
	// before it hits real S3.
	data.insert(
		"accessKeyId".into(),
		ByteString(b"PROXY_DUMMY_ACCESS_KEY".to_vec()),
	);
	data.insert(
		"secretAccessKey".into(),
		ByteString(b"PROXY_DUMMY_SECRET_KEY".to_vec()),
	);

	let secret = Secret {
		metadata: ObjectMeta {
			name: Some(secret_name.clone()),
			namespace: Some(ns_name.to_string()),
			labels: Some(BTreeMap::from([(
				labels::MANAGED_BY.into(),
				labels::MANAGED_BY_VALUE.into(),
			)])),
			..Default::default()
		},
		type_: Some("Opaque".into()),
		data: Some(data),
		..Default::default()
	};

	let api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns_name);
	let mut patch_value = serde_json::to_value(&secret)?;
	patch_value["apiVersion"] = serde_json::json!("v1");
	patch_value["kind"] = serde_json::json!("Secret");
	api.patch(
		&secret_name,
		&PatchParams::apply("postgres-restore-operator").force(),
		&Patch::Apply(&patch_value),
	)
	.await?;
	Ok(())
}

/// Create the PostgresPhysicalReplica CR if it doesn't exist, or re-apply
/// its spec to self-heal drift (canopy-managed CRs aren't user-editable).
async fn ensure_replica_cr(
	ctx: &Context,
	ns_name: &str,
	entry: &WorklistEntry,
	intent: &IntentConfig,
) -> Result<()> {
	let spec = intent.to_replica_spec(entry, Vec::new());

	let mut labels_map = BTreeMap::new();
	labels_map.insert(labels::MANAGED_BY.into(), labels::MANAGED_BY_VALUE.into());
	labels_map.insert(labels::DECLARATION_ID.into(), entry.replica_id.to_string());
	labels_map.insert(labels::GROUP.into(), entry.group_id.to_string());
	labels_map.insert(labels::SERVER.into(), entry.server_id.to_string());
	labels_map.insert(labels::TYPE.into(), entry.type_.clone());
	labels_map.insert(labels::INTENT.into(), entry.intent.to_string());

	let cr = replica_cr(CR_NAME, ns_name, labels_map, spec);
	let api: Api<PostgresPhysicalReplica> = Api::namespaced(ctx.client.clone(), ns_name);
	let mut patch_value = serde_json::to_value(&cr)?;
	patch_value["apiVersion"] = serde_json::json!("pgro.bes.au/v1alpha1");
	patch_value["kind"] = serde_json::json!("PostgresPhysicalReplica");
	api.patch(
		CR_NAME,
		&PatchParams::apply("postgres-restore-operator").force(),
		&Patch::Apply(&patch_value),
	)
	.await?;
	Ok(())
}

fn replica_cr(
	name: &str,
	namespace: &str,
	labels: BTreeMap<String, String>,
	spec: PostgresPhysicalReplicaSpec,
) -> PostgresPhysicalReplica {
	PostgresPhysicalReplica {
		metadata: ObjectMeta {
			name: Some(name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(labels),
			..Default::default()
		},
		spec,
		status: None,
	}
}

async fn set_desired_snapshot(ctx: &Context, ns_name: &str, entry: &WorklistEntry) -> Result<()> {
	let Some(desired) = entry.snapshot_id.as_deref() else {
		return Ok(());
	};

	let api: Api<PostgresPhysicalReplica> = Api::namespaced(ctx.client.clone(), ns_name);
	let cr = api.get(CR_NAME).await?;
	let current = cr
		.status
		.as_ref()
		.and_then(|s| s.canopy_desired_snapshot_id.as_deref());
	if current == Some(desired) {
		return Ok(());
	}

	let patch = serde_json::json!({
		"status": { "canopyDesiredSnapshotId": desired }
	});
	api.patch_status(
		CR_NAME,
		&PatchParams::apply("postgres-restore-operator"),
		&Patch::Merge(&patch),
	)
	.await?;
	info!(
		namespace = %ns_name,
		snapshot = desired,
		"canopy: updated canopyDesiredSnapshotId"
	);
	Ok(())
}

async fn teardown(ctx: &Context, ns_name: &str) -> Result<()> {
	info!(namespace = %ns_name, "canopy: worklist no longer covers this namespace; tearing down");
	let api: Api<Namespace> = Api::all(ctx.client.clone());
	match api.delete(ns_name, &DeleteParams::default()).await {
		Ok(_) => Ok(()),
		Err(kube::Error::Api(err)) if err.code == 404 => Ok(()),
		Err(err) => Err(err.into()),
	}
}

/// Now-timestamp helper for annotation values. Kept as a public helper for
/// integration tests that assert on RFC3339 format.
pub fn now_rfc3339() -> String {
	Time(jiff::Timestamp::now()).0.to_string()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn entry(name: &str, replica: Uuid, server: Uuid) -> WorklistEntry {
		serde_json::from_value(serde_json::json!({
			"replica_id": replica.to_string(),
			"group_id": Uuid::nil().to_string(),
			"server_id": server.to_string(),
			"type": "tamanu-postgres",
			"intent": "verify",
			"name": name,
			"params": {},
			"snapshot_id": null,
			"snapshot_at": null,
			"storage": "s3",
			"bucket": "b",
			"prefix": "",
			"region": "us-east-1",
		}))
		.unwrap()
	}

	#[test]
	fn slug_ascii_alnum_untouched() {
		assert_eq!(slug("hello123"), "hello123");
	}

	#[test]
	fn slug_lowercases_and_replaces_specials() {
		assert_eq!(slug("Nauru Prod Analytics!"), "nauru-prod-analytics");
	}

	#[test]
	fn slug_collapses_runs_of_specials() {
		assert_eq!(slug("a__b--c/d.e"), "a-b-c-d-e");
	}

	#[test]
	fn slug_trims_edges() {
		assert_eq!(slug("---weird---"), "weird");
	}

	#[test]
	fn slug_empty_becomes_replica() {
		assert_eq!(slug(""), "replica");
		assert_eq!(slug("...!!!"), "replica");
	}

	#[test]
	fn slug_truncates_at_50_without_trailing_dash() {
		let out = slug(&"a".repeat(60));
		assert_eq!(out.len(), 50);
		let out = slug(&format!("{}{}", "a".repeat(48), "--"));
		assert!(out.len() <= 50);
		assert!(!out.ends_with('-'));
	}

	#[test]
	fn short_hash_deterministic() {
		let a = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
		let b = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
		let h1 = short_hash(a, b);
		let h2 = short_hash(a, b);
		assert_eq!(h1, h2);
		assert_eq!(h1.len(), 8);
	}

	#[test]
	fn short_hash_differs_by_server() {
		let a = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
		let b1 = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
		let b2 = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
		assert_ne!(short_hash(a, b1), short_hash(a, b2));
	}

	#[test]
	fn namespace_name_length_under_dns_label_limit() {
		let e = entry(&"A".repeat(200), Uuid::nil(), Uuid::nil());
		let name = namespace_name_for(&e);
		assert!(name.len() <= 63, "got {} chars: {name}", name.len());
	}
}
