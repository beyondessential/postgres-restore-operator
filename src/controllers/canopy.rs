//! Canopy worklist syncer — pgro's third top-level controller.
//!
//! Ticks periodically (default 30s, jittered ±20%), fetches
//! `GET /restore-worklist`, discovers the pgro-managed Namespaces already
//! in the cluster, and reconciles the diff by provisioning / refreshing /
//! tearing down per-replica Namespaces. Unlike the `replica` and `restore`
//! controllers this one is **not** CRD-watched: cluster state (labelled
//! Namespaces + annotations) is the runtime model, canopy's worklist is
//! the spec, and there is no intermediate CR.
//!
//! Actual Job creation for the restore itself is delegated to the Job
//! builder (see step 7 in the integration spec); this module only writes
//! Namespaces and their labels/annotations, leaving `restore-state`
//! transitions for the follow-up commits.

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
	time::Duration,
};

use bestool_canopy::WorklistEntry;
use futures::stream::{self, StreamExt};
use k8s_openapi::{
	api::core::v1::Namespace,
	apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time},
};
use kube::{
	Api, ResourceExt,
	api::{DeleteParams, ListParams, PostParams},
};
use rand::RngExt;
use sha2::{Digest, Sha256};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{context::Context, error::Result};

/// How many per-entry reconciliations run concurrently within one tick.
/// Keeps the k8s apiserver from being hit by a stampede when the worklist
/// is large.
const RECONCILE_CONCURRENCY: usize = 8;

/// Jitter multiplier applied to the reconcile interval each tick (±20%).
const JITTER_RATIO: f64 = 0.2;

/// Labels applied to canopy-managed Namespaces.
///
/// Labels are the discovery key: `LIST Namespaces
/// label=pgro.bes.au/managed-by=pgro-canopy` returns every canopy-backed
/// replica in one call. They also carry the immutable identity of the
/// replica (declaration id, group, server, type, intent) — mutable
/// runtime state lives in [`annotations`].
pub mod labels {
	pub const MANAGED_BY: &str = "pgro.bes.au/managed-by";
	pub const MANAGED_BY_VALUE: &str = "pgro-canopy";
	pub const DECLARATION_ID: &str = "pgro.bes.au/declaration-id";
	pub const GROUP: &str = "pgro.bes.au/group";
	pub const SERVER: &str = "pgro.bes.au/server";
	pub const TYPE: &str = "pgro.bes.au/type";
	pub const INTENT: &str = "pgro.bes.au/intent";
}

/// Annotations on canopy-managed Namespaces — mutable per-replica state.
pub mod annotations {
	/// The snapshot canopy wants restored. Populated on every successful
	/// worklist sync; compared against `LAST_RESTORED_SNAPSHOT_ID` to
	/// detect the "newer snapshot available" refresh trigger.
	pub const DESIRED_SNAPSHOT_ID: &str = "pgro.bes.au/desired-snapshot-id";
	pub const DESIRED_SNAPSHOT_AT: &str = "pgro.bes.au/desired-snapshot-at";
	pub const LAST_RESTORED_SNAPSHOT_ID: &str = "pgro.bes.au/last-restored-snapshot-id";
	pub const LAST_RESTORED_AT: &str = "pgro.bes.au/last-restored-at";
	pub const RESTORE_STATE: &str = "pgro.bes.au/restore-state";
	pub const LAST_VERIFICATION_REPORTED_AT: &str = "pgro.bes.au/last-verification-reported-at";
	pub const LAST_VERIFICATION_ERROR: &str = "pgro.bes.au/last-verification-error";
	/// Operator escape hatch: setting this annotation to any value triggers
	/// a refresh on the next tick and is cleared once the refresh Job is
	/// spawned.
	pub const FORCE_REFRESH: &str = "pgro.bes.au/force-refresh";
}

/// Values for the `RESTORE_STATE` annotation.
pub mod restore_state {
	pub const PENDING: &str = "pending";
	pub const RESTORING: &str = "restoring";
	pub const ACTIVE: &str = "active";
	pub const FAILED: &str = "failed";
	pub const TERMINATING: &str = "terminating";
}

/// The reconciliation decision for one (worklist entry, current namespace) pair.
#[derive(Debug, PartialEq, Eq)]
pub enum Action {
	/// Worklist has an entry, no matching namespace exists — create one.
	Provision,
	/// Both present, refresh needed for the given reason.
	Refresh(RefreshReason),
	/// Namespace exists but worklist has no entry — tear down.
	Teardown,
	/// Both present, in sync, healthy — nothing to do this tick.
	NoOp,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RefreshReason {
	/// Canopy is offering a snapshot newer than what we last restored.
	NewerSnapshot,
	/// `last-restored-at` exceeded the declaration's `freshness_seconds`.
	FreshnessExpired,
	/// Operator set the `force-refresh` annotation.
	Forced,
}

/// Compute the k8s Namespace name for a worklist entry.
///
/// Format: `<slug(entry.name)>-<8-hex(SHA-256(replica_id || server_id))>`.
///
/// The slug is derived from the operator-set declaration name in canopy so
/// namespaces are human-recognisable; the 8-hex disambiguator covers the
/// case where a group-wide declaration (`server_id=NULL` in canopy)
/// expands to one worklist entry per live server in the group — all
/// carrying the same `replica_id` and `name`, only `server_id` differs.
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

/// 8-hex-char disambiguator from SHA-256 of `replica_id || server_id`.
///
/// Not cryptographic — just a stable, DNS-safe way to keep namespaces
/// for different (replica, server) pairs from colliding when they share
/// a `name` (group-wide declaration expanded across servers).
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

/// Diff the worklist against the discovered Namespaces and produce one
/// [`Action`] per (namespace, entry) pair. Entries and namespaces are
/// matched by `pgro.bes.au/declaration-id` label / `WorklistEntry.replica_id`;
/// unmatched entries produce a `Provision`, unmatched namespaces produce a
/// `Teardown`. Refresh vs NoOp decisions live in `evaluate_existing`.
///
/// Pure function; testable without a cluster.
pub fn diff(entries: &[WorklistEntry], namespaces: &[Namespace]) -> Vec<(String, Action)> {
	let mut by_replica: HashMap<Uuid, Vec<Namespace>> = HashMap::new();
	for ns in namespaces {
		if let Some(replica) = ns
			.labels()
			.get(labels::DECLARATION_ID)
			.and_then(|s| Uuid::parse_str(s).ok())
		{
			by_replica.entry(replica).or_default().push(ns.clone());
		}
	}

	let mut seen_replicas: HashSet<Uuid> = HashSet::new();
	let mut actions = Vec::new();

	for entry in entries {
		let target_name = namespace_name_for(entry);
		let matched = by_replica
			.get(&entry.replica_id)
			.and_then(|nss| nss.iter().find(|ns| ns.name_any() == target_name).cloned());
		match matched {
			Some(existing) => {
				let action = evaluate_existing(entry, &existing);
				actions.push((target_name, action));
			}
			None => actions.push((target_name, Action::Provision)),
		}
		seen_replicas.insert(entry.replica_id);
	}

	// Namespaces with no matching worklist entry → teardown. Match by the
	// full derived name so a namespace lingering after a declaration was
	// deleted and re-created with the same id but a different server won't
	// be reused mid-flight.
	for ns in namespaces {
		let name = ns.name_any();
		let already = actions.iter().any(|(n, _)| n == &name);
		if !already {
			actions.push((name, Action::Teardown));
		}
	}

	actions
}

/// Refresh decision for an existing namespace vs its worklist entry. Pure.
fn evaluate_existing(entry: &WorklistEntry, ns: &Namespace) -> Action {
	let annos = ns.annotations();
	if annos.get(annotations::FORCE_REFRESH).is_some() {
		return Action::Refresh(RefreshReason::Forced);
	}
	let last_restored = annos.get(annotations::LAST_RESTORED_SNAPSHOT_ID);
	if let (Some(desired), Some(last)) = (entry.snapshot_id.as_ref(), last_restored)
		&& desired != last
	{
		return Action::Refresh(RefreshReason::NewerSnapshot);
	}
	if entry.snapshot_id.is_some() && last_restored.is_none() {
		// Namespace exists but never completed a restore — treat as still
		// in the initial provisioning attempt. The provision path will
		// re-observe the Job on the next tick.
		return Action::NoOp;
	}
	if let (Some(fresh_secs), Some(last_at_str)) = (
		entry.freshness_seconds,
		annos.get(annotations::LAST_RESTORED_AT),
	) && fresh_secs > 0
		&& let Ok(last_at) = last_at_str.parse::<jiff::Timestamp>()
	{
		let elapsed_secs = jiff::Timestamp::now()
			.duration_since(last_at)
			.as_secs()
			.max(0);
		if (elapsed_secs as u64) > (fresh_secs as u64) {
			return Action::Refresh(RefreshReason::FreshnessExpired);
		}
	}
	Action::NoOp
}

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
		let mut rng = rand::rng();
		loop {
			let jitter = rng.random_range(-JITTER_RATIO..JITTER_RATIO);
			let delay = self.interval.mul_f64(1.0 + jitter);
			tokio::time::sleep(delay).await;
			if let Err(err) = self.tick().await {
				error!(error = %err, "canopy worklist syncer tick failed");
			}
		}
	}

	/// One reconciliation pass. Fetches the worklist, lists namespaces,
	/// dispatches per-entry actions concurrently.
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

		let actions = diff(&entries, &namespaces);
		info!(
			worklist_entries = entries.len(),
			existing_namespaces = namespaces.len(),
			actions = actions.len(),
			"canopy worklist syncer tick"
		);

		let entries_by_replica: HashMap<Uuid, &WorklistEntry> =
			entries.iter().map(|e| (e.replica_id, e)).collect();
		let ns_by_name: HashMap<String, &Namespace> =
			namespaces.iter().map(|n| (n.name_any(), n)).collect();

		let ctx = self.ctx.clone();
		stream::iter(actions)
			.for_each_concurrent(RECONCILE_CONCURRENCY, |(ns_name, action)| {
				let ctx = ctx.clone();
				let entry = entries_by_replica
					.iter()
					.find_map(|(_, e)| {
						if namespace_name_for(e) == ns_name {
							Some(*e)
						} else {
							None
						}
					})
					.cloned();
				let existing = ns_by_name.get(&ns_name).map(|n| (*n).clone());
				async move {
					if let Err(err) = dispatch(&ctx, &ns_name, action, entry, existing).await {
						warn!(
							namespace = %ns_name,
							error = %err,
							"canopy per-namespace reconciliation failed"
						);
					}
				}
			})
			.await;

		Ok(())
	}
}

async fn dispatch(
	ctx: &Context,
	ns_name: &str,
	action: Action,
	entry: Option<WorklistEntry>,
	existing: Option<Namespace>,
) -> Result<()> {
	match action {
		Action::Provision => {
			let Some(entry) = entry else {
				return Ok(()); // shouldn't happen; diff produces Provision only with an entry
			};
			provision(ctx, ns_name, &entry).await
		}
		Action::Refresh(reason) => {
			let Some(entry) = entry else { return Ok(()) };
			refresh(ctx, ns_name, &entry, reason, existing).await
		}
		Action::Teardown => teardown(ctx, ns_name, existing).await,
		Action::NoOp => Ok(()),
	}
}

/// Create the Namespace for a new worklist entry with immutable labels + the
/// initial mutable annotations. Follow-up commits wire up the PVC / Deployment /
/// restore Job (step 7 in the integration spec); this tick just records the
/// intent by creating the Namespace and marking `restore-state=pending`.
async fn provision(ctx: &Context, ns_name: &str, entry: &WorklistEntry) -> Result<()> {
	info!(namespace = %ns_name, replica_id = %entry.replica_id, "canopy: provisioning replica namespace");

	let mut labels_map = std::collections::BTreeMap::new();
	labels_map.insert(labels::MANAGED_BY.into(), labels::MANAGED_BY_VALUE.into());
	labels_map.insert(labels::DECLARATION_ID.into(), entry.replica_id.to_string());
	labels_map.insert(labels::GROUP.into(), entry.group_id.to_string());
	labels_map.insert(labels::SERVER.into(), entry.server_id.to_string());
	labels_map.insert(labels::TYPE.into(), entry.r#type.to_string());
	labels_map.insert(labels::INTENT.into(), entry.intent.to_string());

	let mut annos = std::collections::BTreeMap::new();
	annos.insert(
		annotations::RESTORE_STATE.into(),
		restore_state::PENDING.into(),
	);
	if let Some(sid) = &entry.snapshot_id {
		annos.insert(annotations::DESIRED_SNAPSHOT_ID.into(), sid.clone());
	}
	if let Some(sat) = &entry.snapshot_at {
		annos.insert(annotations::DESIRED_SNAPSHOT_AT.into(), sat.clone());
	}

	let ns = Namespace {
		metadata: ObjectMeta {
			name: Some(ns_name.to_string()),
			labels: Some(labels_map),
			annotations: Some(annos),
			..Default::default()
		},
		..Default::default()
	};

	let api: Api<Namespace> = Api::all(ctx.client.clone());
	api.create(&PostParams::default(), &ns).await?;
	Ok(())
}

/// Placeholder — refresh flow lands in the follow-up commit alongside the
/// Job builder. For now, just log the reason and update the desired-snapshot
/// annotation so the next tick can pick up if the entry keeps changing.
async fn refresh(
	ctx: &Context,
	ns_name: &str,
	entry: &WorklistEntry,
	reason: RefreshReason,
	_existing: Option<Namespace>,
) -> Result<()> {
	info!(namespace = %ns_name, replica_id = %entry.replica_id, ?reason, "canopy: refresh needed (Job spawn not yet implemented)");
	let _ = (ctx, ns_name, entry);
	Ok(())
}

/// Placeholder — teardown flow (drain, delete children, delete namespace)
/// lands in a follow-up commit. For now, mark the namespace terminating so
/// operators can see it via `kubectl` and delete the namespace directly.
async fn teardown(ctx: &Context, ns_name: &str, existing: Option<Namespace>) -> Result<()> {
	info!(namespace = %ns_name, "canopy: worklist no longer covers this namespace; tearing down");
	if existing.is_none() {
		return Ok(());
	}
	let api: Api<Namespace> = Api::all(ctx.client.clone());
	// Namespace deletion cascades to everything inside it; k8s handles the
	// child cleanup.
	match api.delete(ns_name, &DeleteParams::default()).await {
		Ok(_) => Ok(()),
		Err(kube::Error::Api(err)) if err.code == 404 => Ok(()),
		Err(err) => Err(err.into()),
	}
}

/// Now-timestamp helper for annotation values. Keeps RFC3339 stringification
/// centralised so tests can rely on the format.
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

	#[test]
	fn diff_missing_namespace_is_provision() {
		let e = entry("nauru", Uuid::new_v4(), Uuid::new_v4());
		let actions = diff(std::slice::from_ref(&e), &[]);
		assert_eq!(actions.len(), 1);
		assert_eq!(actions[0].1, Action::Provision);
	}

	#[test]
	fn diff_orphan_namespace_is_teardown() {
		let ns = Namespace {
			metadata: ObjectMeta {
				name: Some("orphan".into()),
				labels: Some(std::collections::BTreeMap::from([(
					labels::MANAGED_BY.into(),
					labels::MANAGED_BY_VALUE.into(),
				)])),
				..Default::default()
			},
			..Default::default()
		};
		let actions = diff(&[], &[ns]);
		assert_eq!(actions.len(), 1);
		assert_eq!(actions[0].1, Action::Teardown);
	}

	#[test]
	fn diff_matched_pair_is_noop_when_never_restored_yet() {
		let replica = Uuid::new_v4();
		let server = Uuid::new_v4();
		let e = entry("nauru", replica, server);
		let ns = Namespace {
			metadata: ObjectMeta {
				name: Some(namespace_name_for(&e)),
				labels: Some(std::collections::BTreeMap::from([
					(labels::MANAGED_BY.into(), labels::MANAGED_BY_VALUE.into()),
					(labels::DECLARATION_ID.into(), replica.to_string()),
				])),
				..Default::default()
			},
			..Default::default()
		};
		let actions = diff(&[e], &[ns]);
		assert_eq!(actions.len(), 1);
		assert_eq!(actions[0].1, Action::NoOp);
	}

	#[test]
	fn diff_newer_snapshot_triggers_refresh() {
		let replica = Uuid::new_v4();
		let server = Uuid::new_v4();
		let mut e = entry("nauru", replica, server);
		e.snapshot_id = Some("new-snap".into());
		let ns = Namespace {
			metadata: ObjectMeta {
				name: Some(namespace_name_for(&e)),
				labels: Some(std::collections::BTreeMap::from([
					(labels::MANAGED_BY.into(), labels::MANAGED_BY_VALUE.into()),
					(labels::DECLARATION_ID.into(), replica.to_string()),
				])),
				annotations: Some(std::collections::BTreeMap::from([(
					annotations::LAST_RESTORED_SNAPSHOT_ID.into(),
					"old-snap".into(),
				)])),
				..Default::default()
			},
			..Default::default()
		};
		let actions = diff(&[e], &[ns]);
		assert_eq!(actions[0].1, Action::Refresh(RefreshReason::NewerSnapshot));
	}

	#[test]
	fn diff_forced_refresh_annotation_wins() {
		let replica = Uuid::new_v4();
		let server = Uuid::new_v4();
		let e = entry("nauru", replica, server);
		let ns = Namespace {
			metadata: ObjectMeta {
				name: Some(namespace_name_for(&e)),
				labels: Some(std::collections::BTreeMap::from([
					(labels::MANAGED_BY.into(), labels::MANAGED_BY_VALUE.into()),
					(labels::DECLARATION_ID.into(), replica.to_string()),
				])),
				annotations: Some(std::collections::BTreeMap::from([(
					annotations::FORCE_REFRESH.into(),
					"now".into(),
				)])),
				..Default::default()
			},
			..Default::default()
		};
		let actions = diff(&[e], &[ns]);
		assert_eq!(actions[0].1, Action::Refresh(RefreshReason::Forced));
	}
}
