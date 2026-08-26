//! Intent-driven configuration for canopy-backed replicas.
//!
//! Each canopy `WorklistEntry` carries an `intent` string plus resolved
//! `params`. pgro advertises its intents as [`descriptors`] at startup —
//! each a name, human description, the canopy `semantics` it opts into, and
//! a typed parameter schema — so canopy only dispatches entries pgro can
//! handle and collects the right parameters when an operator declares one.
//!
//! On each tick the syncer looks up the entry's intent in [`config_for`] for
//! the fixed per-intent bits, then [`IntentConfig::to_replica_spec`] merges
//! the entry's resolved params on top to materialise a
//! `PostgresPhysicalReplicaSpec` — the CR path takes over from there.
//!
//! The single parametrised `analytics` intent covers what used to be
//! `analytics-dev` (no persistent schemas) and `analytics-dbt`
//! (`persistent_schemas=dbt`, exposed): the difference is now operator-set
//! parameters, not distinct intents.

use std::collections::{BTreeMap, HashMap};

use bestool_canopy::schema::{
	BTreeMap as ParamSchema, BTreeMapValue as ParamSpec, IntentDescriptor, ParamType, WorklistEntry,
};
use jiff::Span;
use k8s_openapi::{
	api::core::v1::{ResourceRequirements, SecretReference},
	apimachinery::pkg::api::resource::Quantity,
};
use serde_json::{Map, Value, json};

use crate::{
	types::{CanopySource, PostgresPhysicalReplicaSpec, RedactionSpec},
	util::TimeSpan,
};

/// Canopy semantics pgro's intents opt into (see the RST spec). Carried as
/// plain strings; canopy acts only on the ones it recognises.
mod semantics {
	/// The intent produces restore-health feedback (canopy expects a report
	/// and holds it to the overdue bound).
	pub const CHECK: &str = "check";
	/// A given snapshot is dispatched at most once; canopy self-suppresses
	/// the worklist entry once the snapshot has a healthy report.
	pub const ONCE: &str = "once";
	/// The health report carries a link to the running replica, which canopy
	/// surfaces to operators.
	pub const URL: &str = "url";
	/// Canopy names a target version on the worklist entry; the consumer applies
	/// that version's schema migrations to the restored replica and reports how
	/// they went alongside the replica's health.
	pub const MIGRATE: &str = "migrate";
	/// The intent can de-identify the restored data before serving it:
	/// whether a given replica does is up to its `redaction_*` params, and
	/// the replica's `redactionPhase` status says whether it took. Canopy
	/// doesn't recognise this one yet — unrecognised semantics are stored
	/// and have no effect — so declaring it now is how canopy learns the
	/// capability exists when it grows support for it.
	pub const REDACT: &str = "redact";
}

/// Names of the parameters the `analytics` intent advertises. Shared between
/// the descriptor (what canopy collects) and [`IntentConfig::to_replica_spec`]
/// (what pgro reads back) so the two can't drift.
pub mod params {
	/// `duration` — minimum time between restores of this replica.
	pub const MINIMUM_TTL: &str = "minimum_ttl";
	/// `duration` — grace period before tearing down the old restore on
	/// switchover.
	pub const SWITCHOVER_GRACE: &str = "switchover_grace";
	/// `bytes` — cap on the restore PVC size.
	pub const STORAGE_SIZE_MAXIMUM: &str = "storage_size_maximum";
	/// `text` — comma-separated schemas migrated into the restore and kept
	/// across restores (the dbt workload). Empty/unset = a plain replica.
	pub const PERSISTENT_SCHEMAS: &str = "persistent_schemas";
	/// `text` — a Tamanu version to migrate this analytics replica's restores
	/// to, turning it into a persistent, upgraded query replica. Empty/unset
	/// leaves it a plain replica with no migration step. The outcome is not
	/// reported to canopy: an analytics migration gives the operator an upgraded
	/// database to work against, not a version-readiness signal.
	pub const MIGRATE_TO: &str = "migrate_to";
	/// `boolean` — expose the replica on the tailnet and report its URL.
	pub const EXPOSE: &str = "expose";
	/// `text` — comma-separated usernames to provision as extra `LOGIN
	/// SUPERUSER` roles alongside the analytics user, each with its own
	/// operator-generated password Secret. Empty/unset = analytics user only.
	pub const EXTRA_USERS: &str = "extra_users";
	/// `bytes` — pin the postgres memory request, instead of deriving it from
	/// the snapshot size.
	pub const MEMORY_REQUEST: &str = "memory_request";
	/// `bytes` — pin the postgres memory limit, instead of deriving it.
	pub const MEMORY_LIMIT: &str = "memory_limit";
	/// `text` — pin the postgres CPU request (a k8s quantity, e.g. `500m`).
	pub const CPU_REQUEST: &str = "cpu_request";
	/// `text` — pin the postgres CPU limit (a k8s quantity, e.g. `4`).
	pub const CPU_LIMIT: &str = "cpu_limit";
	/// `bytes` — cap on the snapshot-derived postgres memory.
	pub const RESOURCES_MAXIMUM: &str = "resources_maximum";
	/// `duration` — how long to wait for the restore's postgres Deployment to
	/// become Ready before failing the restore.
	pub const DEPLOYMENT_READY_TIMEOUT: &str = "deployment_ready_timeout";
	/// `text` — URL of the dbt-shaped masking manifest to apply to the
	/// restored data before it goes live. Setting it is what turns
	/// redaction on; empty/unset leaves the data as restored. May carry a
	/// `{version}` placeholder, which needs [`REDACTION_VERSION_QUERY`].
	pub const REDACTION_MANIFEST_URL: &str = "redaction_manifest_url";
	/// `text` — SQL returning one row, one column: the version to
	/// substitute into the manifest URL's `{version}`. A replica that wants
	/// a fixed version writes it into the URL instead.
	pub const REDACTION_VERSION_QUERY: &str = "redaction_version_query";
	/// `boolean` — when the versioned manifest URL 404s, retry at the
	/// `major.minor.0` base version. For sources that publish a manifest
	/// per minor rather than per patch.
	pub const REDACTION_VERSION_FALLBACK_TO_BASE: &str = "redaction_version_fallback_to_base";
}

/// Default minimum TTL for `analytics` replicas when the operator leaves the
/// param unset (2 hours, in seconds).
const DEFAULT_ANALYTICS_MINIMUM_TTL_SECS: i64 = 7200;
/// Default switchover grace for `analytics` replicas (2 minutes, in seconds).
const DEFAULT_ANALYTICS_SWITCHOVER_GRACE_SECS: i64 = 120;

fn param(type_: ParamType, default: Option<Value>) -> ParamSpec {
	ParamSpec::builder()
		.type_(type_)
		.maybe_default(default)
		.build()
}

/// The `analytics` intent's parameter schema (name → typed spec + default).
fn analytics_param_schema() -> ParamSchema {
	ParamSchema(HashMap::from([
		(
			params::MINIMUM_TTL.to_string(),
			param(
				ParamType::Duration,
				Some(json!(DEFAULT_ANALYTICS_MINIMUM_TTL_SECS)),
			),
		),
		(
			params::SWITCHOVER_GRACE.to_string(),
			param(
				ParamType::Duration,
				Some(json!(DEFAULT_ANALYTICS_SWITCHOVER_GRACE_SECS)),
			),
		),
		(
			params::STORAGE_SIZE_MAXIMUM.to_string(),
			param(ParamType::Bytes, None),
		),
		(
			params::PERSISTENT_SCHEMAS.to_string(),
			param(ParamType::Text, None),
		),
		(params::MIGRATE_TO.to_string(), param(ParamType::Text, None)),
		(
			params::EXPOSE.to_string(),
			param(ParamType::Boolean, Some(json!(false))),
		),
		(
			params::EXTRA_USERS.to_string(),
			param(ParamType::Text, None),
		),
		(
			params::MEMORY_REQUEST.to_string(),
			param(ParamType::Bytes, None),
		),
		(
			params::MEMORY_LIMIT.to_string(),
			param(ParamType::Bytes, None),
		),
		(
			params::CPU_REQUEST.to_string(),
			param(ParamType::Text, None),
		),
		(params::CPU_LIMIT.to_string(), param(ParamType::Text, None)),
		(
			params::RESOURCES_MAXIMUM.to_string(),
			param(ParamType::Bytes, None),
		),
		(
			params::DEPLOYMENT_READY_TIMEOUT.to_string(),
			param(ParamType::Duration, None),
		),
		(
			params::REDACTION_MANIFEST_URL.to_string(),
			param(ParamType::Text, None),
		),
		(
			params::REDACTION_VERSION_QUERY.to_string(),
			param(ParamType::Text, None),
		),
		(
			params::REDACTION_VERSION_FALLBACK_TO_BASE.to_string(),
			param(ParamType::Boolean, Some(json!(false))),
		),
	]))
}

/// Build the replica's [`RedactionSpec`] from the canopy params, or `None`
/// when the operator left the manifest URL unset — the common case, and the
/// one that leaves the restore exactly as it came out of the snapshot.
fn redaction_spec(p: &Map<String, Value>) -> Option<RedactionSpec> {
	let non_empty = |name: &str| {
		param_str(p, name)
			.map(str::trim)
			.filter(|s| !s.is_empty())
			.map(str::to_string)
	};

	Some(RedactionSpec {
		manifest_url: non_empty(params::REDACTION_MANIFEST_URL)?,
		// Not a canopy param: a replica that wants a fixed version writes
		// it into the manifest URL, which needs no placeholder and so no
		// version resolution at all.
		version: None,
		version_query: non_empty(params::REDACTION_VERSION_QUERY),
		version_fallback_to_base: param_bool(p, params::REDACTION_VERSION_FALLBACK_TO_BASE)
			.unwrap_or(false),
	})
}

/// Build a pinned `resources` from the canopy params, or `None` when the
/// operator declared none of them — in which case the deployment builder
/// derives memory from the snapshot size instead.
///
/// A partially-specified pin falls back to `floor` for the fields the operator
/// left out, so setting only `memory_limit` doesn't silently drop CPU.
fn pinned_resources(
	p: &Map<String, Value>,
	floor: Option<&ResourceRequirements>,
) -> Option<ResourceRequirements> {
	let bytes = |name: &str| param_i64(p, name).map(|b| Quantity(b.to_string()));
	let text = |name: &str| param_str(p, name).map(|s| Quantity(s.to_string()));

	let memory_request = bytes(params::MEMORY_REQUEST);
	let memory_limit = bytes(params::MEMORY_LIMIT);
	let cpu_request = text(params::CPU_REQUEST);
	let cpu_limit = text(params::CPU_LIMIT);
	if memory_request.is_none()
		&& memory_limit.is_none()
		&& cpu_request.is_none()
		&& cpu_limit.is_none()
	{
		return None;
	}

	let from_floor = |which: fn(&ResourceRequirements) -> &Option<BTreeMap<String, Quantity>>,
	                  key: &str| {
		floor
			.and_then(|f| which(f).as_ref())
			.and_then(|m| m.get(key))
			.cloned()
	};
	let entries = |cpu: Option<Quantity>, memory: Option<Quantity>| {
		let map: BTreeMap<String, Quantity> = [("cpu", cpu), ("memory", memory)]
			.into_iter()
			.filter_map(|(k, v)| v.map(|v| (k.to_string(), v)))
			.collect();
		(!map.is_empty()).then_some(map)
	};

	Some(ResourceRequirements {
		requests: entries(
			cpu_request.or_else(|| from_floor(|r| &r.requests, "cpu")),
			memory_request.or_else(|| from_floor(|r| &r.requests, "memory")),
		),
		limits: entries(
			cpu_limit.or_else(|| from_floor(|r| &r.limits, "cpu")),
			memory_limit.or_else(|| from_floor(|r| &r.limits, "memory")),
		),
		..Default::default()
	})
}

/// The intent descriptors pgro advertises to canopy at startup. Canopy stores
/// them and offers the intents (with descriptions and parameter fields) to
/// operators, dispatches only these, and applies the semantics' behaviours.
pub fn descriptors() -> Vec<IntentDescriptor> {
	vec![
		IntentDescriptor::builder()
			.intent("verify".to_string())
			.description(
				"Restore the snapshot to prove it is restorable, then discard it.".to_string(),
			)
			.semantics(vec![
				semantics::CHECK.to_string(),
				semantics::ONCE.to_string(),
			])
			.build(),
		IntentDescriptor::builder()
			.intent("upgrade".to_string())
			.description(
				"Restore the snapshot, apply the next version's schema migrations to it to \
				 prove the upgrade survives this deployment's data, then discard it."
					.to_string(),
			)
			.semantics(vec![
				semantics::CHECK.to_string(),
				semantics::ONCE.to_string(),
				semantics::MIGRATE.to_string(),
			])
			.build(),
		IntentDescriptor::builder()
			.intent("analytics".to_string())
			.description(
				"Keep a long-lived read-only query replica restored from the latest snapshot."
					.to_string(),
			)
			.semantics(vec![
				semantics::CHECK.to_string(),
				semantics::URL.to_string(),
				semantics::REDACT.to_string(),
			])
			.params(analytics_param_schema())
			.build(),
	]
}

/// Fixed (non-parametrised) spec fragments for a supported intent, plus the
/// defaults used when a parametrised field is left unset by the operator.
#[derive(Debug, Clone)]
pub struct IntentConfig {
	/// Lower bound on the snapshot-derived postgres resources, and the source
	/// of CPU (which doesn't scale with data volume). Not an exact size — a
	/// fixed value here would provision a replica holding a few hundred MB
	/// identically to one holding a hundred GB.
	pub resources_floor: Option<ResourceRequirements>,
	/// Cap on the snapshot-derived postgres memory.
	pub resources_maximum: Quantity,
	/// Default when the `deployment_ready_timeout` param is unset. `None`
	/// leaves the timeout to be derived from the snapshot size.
	pub deployment_ready_timeout: Option<TimeSpan>,
	pub read_only: bool,
	/// Default when the `minimum_ttl` param is unset.
	pub minimum_ttl: Option<TimeSpan>,
	/// Default when the `switchover_grace` param is unset.
	pub switchover_grace_period: TimeSpan,
	/// Default when the `persistent_schemas` param is unset.
	pub persistent_schemas: Option<Vec<String>>,
	pub storage_size_override: Quantity,
	/// Default when the `storage_size_maximum` param is unset.
	pub storage_size_maximum: Quantity,
	/// Tear the restore down once it's verified healthy rather than keeping
	/// it running. Materialised into `PostgresPhysicalReplicaSpec.ephemeral`.
	/// True for `verify` and `upgrade` (throwaway snapshot checks), false for
	/// `analytics` (long-lived query replica).
	pub ephemeral: bool,
	/// Floor on the postgres pod's `/dev/shm` sizing. Materialised into
	/// `PostgresPhysicalReplicaSpec.shm_size_floor` so the shared Deployment
	/// builder picks `max(computed_from_resources, floor)`. `None` leaves the
	/// resource-derived value alone, which is what an intent wants once its
	/// memory request reflects the pod's real size — shm then lands at 36% of
	/// memory on its own.
	pub shm_size_floor: Option<Quantity>,
}

/// Build an intent's resource floor. `cpu_lim` is optional: a CPU limit on a
/// database buys nothing the request doesn't already guarantee, and throttles
/// bursty analytical queries at the ceiling. Memory always carries both, since
/// [`crate::quantity::scale_memory_for_snapshot`] sets request equal to limit.
fn resources(
	cpu_req: &str,
	mem_req: &str,
	cpu_lim: Option<&str>,
	mem_lim: &str,
) -> ResourceRequirements {
	let mut limits = BTreeMap::from([("memory".to_string(), Quantity(mem_lim.to_string()))]);
	if let Some(cpu_lim) = cpu_lim {
		limits.insert("cpu".to_string(), Quantity(cpu_lim.to_string()));
	}
	ResourceRequirements {
		requests: Some(BTreeMap::from([
			("cpu".to_string(), Quantity(cpu_req.to_string())),
			("memory".to_string(), Quantity(mem_req.to_string())),
		])),
		limits: Some(limits),
		..Default::default()
	}
}

/// Look up the fixed configuration for a supported intent name. Returns
/// `None` for unsupported intents — canopy shouldn't dispatch these because
/// they aren't advertised in [`descriptors`], but callers must still handle
/// the possibility (e.g. a worklist entry sneaking through during an operator
/// downgrade).
pub fn config_for(intent: &str) -> Option<IntentConfig> {
	match intent {
		"verify" | "upgrade" => Some(IntentConfig {
			resources_floor: Some(resources("250m", "512Mi", Some("2"), "2Gi")),
			read_only: true,
			minimum_ttl: None,
			switchover_grace_period: TimeSpan(Span::new().minutes(5)),
			persistent_schemas: None,
			storage_size_override: Quantity("20Gi".to_string()),
			storage_size_maximum: Quantity("2Ti".to_string()),
			ephemeral: true,
			resources_maximum: Quantity("8Gi".to_string()),
			deployment_ready_timeout: None,
			shm_size_floor: Some(Quantity("512Mi".to_string())),
		}),
		"analytics" => Some(IntentConfig {
			// No CPU limit: dbt runs are bursty and a ceiling only buys CFS
			// throttling. The request is raised to match, so the guarantee is
			// higher than the old 500m even though the ceiling is gone.
			resources_floor: Some(resources("2", "2Gi", None, "8Gi")),
			read_only: true,
			minimum_ttl: Some(TimeSpan(
				Span::new().seconds(DEFAULT_ANALYTICS_MINIMUM_TTL_SECS),
			)),
			switchover_grace_period: TimeSpan(
				Span::new().seconds(DEFAULT_ANALYTICS_SWITCHOVER_GRACE_SECS),
			),
			persistent_schemas: None,
			storage_size_override: Quantity("50Gi".to_string()),
			storage_size_maximum: Quantity("2Ti".to_string()),
			ephemeral: false,
			resources_maximum: Quantity("64Gi".to_string()),
			deployment_ready_timeout: None,
			// Unset deliberately. The old 2Gi floor existed only to lift shm
			// above what a quarter-sized memory request derived; now that the
			// request matches the limit, shm lands at 36% of memory unaided.
			shm_size_floor: None,
		}),
		_ => None,
	}
}

/// Tailscale Service annotations exposing the replica on the tailnet under
/// the deterministic hostname `infra-replica-{entry_name}`.
fn expose_annotations(entry_name: &str) -> BTreeMap<String, String> {
	BTreeMap::from([
		("tailscale.com/expose".to_string(), "true".to_string()),
		(
			"tailscale.com/hostname".to_string(),
			exposed_hostname(entry_name),
		),
	])
}

/// The tailnet hostname a replica is exposed under (without the MagicDNS
/// suffix). Shared with the verification reporter so the reported URL matches
/// what the Service annotation requests.
pub fn exposed_hostname(entry_name: &str) -> String {
	format!("infra-replica-{entry_name}")
}

fn param_i64(params: &Map<String, Value>, key: &str) -> Option<i64> {
	params.get(key).and_then(Value::as_i64)
}

fn param_bool(params: &Map<String, Value>, key: &str) -> Option<bool> {
	params.get(key).and_then(Value::as_bool)
}

fn param_str<'a>(params: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
	params.get(key).and_then(Value::as_str)
}

/// Whether the entry's `expose` param is set true.
pub fn is_exposed(entry: &WorklistEntry) -> bool {
	param_bool(&entry.params, params::EXPOSE).unwrap_or(false)
}

impl IntentConfig {
	/// Materialise a `PostgresPhysicalReplicaSpec` for a canopy-managed
	/// replica, merging the entry's resolved params over the intent defaults.
	/// The syncer patches the CR with this spec on Provision / re-asserts it
	/// on subsequent ticks so drift from manual edits is self-healing.
	pub fn to_replica_spec(
		&self,
		entry: &WorklistEntry,
		notifications: Vec<crate::types::NotificationConfig>,
	) -> PostgresPhysicalReplicaSpec {
		let p = &entry.params;

		let minimum_ttl = param_i64(p, params::MINIMUM_TTL)
			.map(|secs| TimeSpan(Span::new().seconds(secs)))
			.or(self.minimum_ttl);
		let switchover_grace_period = param_i64(p, params::SWITCHOVER_GRACE)
			.map(|secs| TimeSpan(Span::new().seconds(secs)))
			.unwrap_or(self.switchover_grace_period);
		let storage_size_maximum = param_i64(p, params::STORAGE_SIZE_MAXIMUM)
			.map(|bytes| Quantity(bytes.to_string()))
			.unwrap_or_else(|| self.storage_size_maximum.clone());
		let persistent_schemas = param_str(p, params::PERSISTENT_SCHEMAS)
			.map(parse_persistent_schemas)
			.filter(|schemas| !schemas.is_empty())
			.or_else(|| self.persistent_schemas.clone());
		let extra_users = param_str(p, params::EXTRA_USERS)
			.map(parse_comma_list)
			.unwrap_or_default();
		let service_annotations = is_exposed(entry).then(|| expose_annotations(&entry.name));
		// Two routes to a target. `upgrade`'s is named by canopy on the entry via
		// the `migrate` semantic, and canopy withholds the entry entirely when
		// the server has no candidate version, so the pair being present is the
		// whole signal. `analytics`'s is the operator's `migrate_to` param, a
		// version they choose; its outcome isn't reported to canopy, so it
		// carries no canopy version id and is built with an empty one.
		let migrate_to = entry
			.target_version
			.as_deref()
			.zip(entry.target_version_id)
			.map(|(version, version_id)| crate::types::MigrationTarget {
				version: version.to_owned(),
				version_id: version_id.to_string(),
			})
			.or_else(|| {
				param_str(p, params::MIGRATE_TO)
					.map(str::trim)
					.filter(|s| !s.is_empty())
					.map(|version| crate::types::MigrationTarget {
						version: version.to_owned(),
						version_id: String::new(),
					})
			});

		// Resources are pinned only when the operator declared at least one of
		// them in canopy; otherwise they stay unset and the deployment builder
		// derives memory from the snapshot size, floored by `resources_floor`.
		let pinned_resources = pinned_resources(p, self.resources_floor.as_ref());
		let resources_maximum = param_i64(p, params::RESOURCES_MAXIMUM)
			.map(|bytes| Quantity(bytes.to_string()))
			.unwrap_or_else(|| self.resources_maximum.clone());
		let deployment_ready_timeout = param_i64(p, params::DEPLOYMENT_READY_TIMEOUT)
			.map(|secs| TimeSpan(Span::new().seconds(secs)))
			.or(self.deployment_ready_timeout);

		PostgresPhysicalReplicaSpec {
			kopia_secret_ref: None,
			canopy_source: Some(CanopySource {
				group: entry.group_id.to_string(),
				r#type: entry.type_.to_string(),
			}),
			snapshot_filter: None,
			// Long cadence — the actual restore trigger on the canopy path is
			// a change to `status.canopyDesiredSnapshotId` written by the
			// worklist syncer. The cron is a belt-and-braces fallback (e.g.
			// missed status watch).
			schedule: "H * * * *".to_string(),
			schedule_jitter: TimeSpan(Span::new().minutes(10)),
			minimum_ttl,
			switchover_grace_period,
			analytics_username: "analytics".to_string(),
			extra_users,
			storage_class: None,
			storage_size_override: Some(self.storage_size_override.clone()),
			resources: pinned_resources,
			resources_floor: self.resources_floor.clone(),
			resources_maximum: Some(resources_maximum),
			deployment_ready_timeout,
			shm_size_floor: self.shm_size_floor.clone(),
			service_annotations,
			pod_annotations: None,
			affinity: None,
			tolerations: Vec::new(),
			read_only: self.read_only,
			ephemeral: self.ephemeral,
			postgres_extra_config: None,
			notifications,
			persistent_schemas,
			migrate_to,
			redaction: redaction_spec(p),
			storage_size_maximum,
		}
	}

	/// Name of the namespace-local Secret the canopy syncer materialises with
	/// the worklist entry's bucket / region / prefix / repo password + dummy
	/// AWS keys. `build_restore_job` mounts it via env_from_secret.
	pub fn canopy_creds_secret_name(replica_name: &str) -> String {
		format!("{replica_name}-canopy-creds")
	}

	/// Convenience: build a `SecretReference` pointing at the canopy creds
	/// Secret for the given replica.
	pub fn canopy_creds_secret_ref(replica_name: &str) -> SecretReference {
		SecretReference {
			name: Some(Self::canopy_creds_secret_name(replica_name)),
			namespace: None,
		}
	}
}

/// Split a comma-separated `persistent_schemas` param into trimmed,
/// non-empty schema names.
fn parse_persistent_schemas(raw: &str) -> Vec<String> {
	parse_comma_list(raw)
}

/// Split a comma-separated text param into trimmed, non-empty items.
fn parse_comma_list(raw: &str) -> Vec<String> {
	raw.split(',')
		.map(str::trim)
		.filter(|s| !s.is_empty())
		.map(str::to_string)
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn entry(intent: &str, name: &str, params: Value) -> WorklistEntry {
		serde_json::from_value(json!({
			"replica_id": "11111111-1111-1111-1111-111111111111",
			"group_id": "22222222-2222-2222-2222-222222222222",
			"server_id": "33333333-3333-3333-3333-333333333333",
			"type": "tamanu-postgres",
			"intent": intent,
			"name": name,
			"params": params,
			"snapshot_id": "abc123",
			"snapshot_at": "2026-07-01T00:00:00Z",
			"overdue_after_seconds": null,
			"storage": "s3",
			"bucket": "canopy-test",
			"prefix": "",
			"region": "ap-southeast-2",
		}))
		.unwrap()
	}

	fn entry_with_target(intent: &str) -> WorklistEntry {
		let mut v = serde_json::to_value(entry(intent, "site", json!({}))).unwrap();
		v["target_version"] = json!("2.63.0");
		v["target_version_id"] = json!("44444444-4444-4444-4444-444444444444");
		serde_json::from_value(v).unwrap()
	}

	#[test]
	fn only_a_named_target_migrates() {
		// The target is the whole signal: canopy names one only for a `migrate`
		// intent, so nothing here matches on the intent's name.
		let spec = config_for("upgrade")
			.unwrap()
			.to_replica_spec(&entry_with_target("upgrade"), vec![]);
		let target = spec.migrate_to.expect("a named target migrates");
		assert_eq!(target.version, "2.63.0");
		assert_eq!(target.version_id, "44444444-4444-4444-4444-444444444444");

		for intent in ["verify", "analytics"] {
			let spec = config_for(intent)
				.unwrap()
				.to_replica_spec(&entry(intent, "site", json!({})), vec![]);
			assert!(
				spec.migrate_to.is_none(),
				"{intent} names no target, so it must not migrate"
			);
		}
	}

	#[test]
	fn analytics_migrate_to_param_builds_a_target() {
		// The operator-chosen version turns an analytics replica into a
		// persistent, upgraded query replica. It carries no canopy version id —
		// analytics migrations aren't reported — so the id is built empty.
		let spec = config_for("analytics").unwrap().to_replica_spec(
			&entry("analytics", "site", json!({ "migrate_to": "2.41.0" })),
			vec![],
		);
		let target = spec
			.migrate_to
			.expect("migrate_to param names a target to migrate to");
		assert_eq!(target.version, "2.41.0");
		assert_eq!(
			target.version_id, "",
			"an analytics target has no canopy version id"
		);
	}

	#[test]
	fn blank_migrate_to_param_leaves_the_replica_unmigrated() {
		for value in [json!(""), json!("  "), Value::Null] {
			let spec = config_for("analytics").unwrap().to_replica_spec(
				&entry("analytics", "site", json!({ "migrate_to": value })),
				vec![],
			);
			assert!(
				spec.migrate_to.is_none(),
				"{value:?} must leave the replica a plain query replica"
			);
		}
	}

	#[test]
	fn descriptors_advertise_expected_intents_and_semantics() {
		let ds = descriptors();
		let names: Vec<&str> = ds.iter().map(|d| d.intent.as_str()).collect();
		assert_eq!(names, ["verify", "upgrade", "analytics"]);

		let verify = &ds[0];
		// `migrate` withholds an entry from a server with no candidate version,
		// which for a verifying intent would stop verifying the backups of every
		// server that has none: a non-Tamanu product, or one already on the newest
		// published version.
		assert_eq!(verify.semantics, ["check", "once"]);
		assert!(verify.params.is_none(), "verify takes no params");
		assert!(verify.description.is_some());

		let upgrade = &ds[1];
		assert_eq!(upgrade.semantics, ["check", "once", "migrate"]);
		assert!(upgrade.params.is_none(), "upgrade takes no params");
		assert!(upgrade.description.is_some());

		let analytics = &ds[2];
		assert_eq!(analytics.semantics, ["check", "url", "redact"]);
		let params = analytics
			.params
			.as_ref()
			.expect("analytics is parametrised");
		// Every advertised param is present with its declared type.
		assert_eq!(
			params.get(params::MINIMUM_TTL).unwrap().type_,
			ParamType::Duration
		);
		assert_eq!(
			params.get(params::SWITCHOVER_GRACE).unwrap().type_,
			ParamType::Duration
		);
		assert_eq!(
			params.get(params::STORAGE_SIZE_MAXIMUM).unwrap().type_,
			ParamType::Bytes
		);
		assert_eq!(
			params.get(params::PERSISTENT_SCHEMAS).unwrap().type_,
			ParamType::Text
		);
		assert_eq!(
			params.get(params::MIGRATE_TO).unwrap().type_,
			ParamType::Text
		);
		assert_eq!(
			params.get(params::EXPOSE).unwrap().type_,
			ParamType::Boolean
		);
		assert_eq!(
			params.get(params::EXTRA_USERS).unwrap().type_,
			ParamType::Text
		);
		assert_eq!(
			params.get(params::MEMORY_LIMIT).unwrap().type_,
			ParamType::Bytes
		);
		assert_eq!(
			params.get(params::CPU_LIMIT).unwrap().type_,
			ParamType::Text
		);
		assert_eq!(
			params.get(params::DEPLOYMENT_READY_TIMEOUT).unwrap().type_,
			ParamType::Duration
		);
		assert_eq!(
			params.get(params::REDACTION_MANIFEST_URL).unwrap().type_,
			ParamType::Text
		);
		assert_eq!(
			params.get(params::REDACTION_VERSION_QUERY).unwrap().type_,
			ParamType::Text
		);
		assert_eq!(
			params
				.get(params::REDACTION_VERSION_FALLBACK_TO_BASE)
				.unwrap()
				.type_,
			ParamType::Boolean
		);
		assert_eq!(
			params.get(params::MIGRATE_TO).unwrap().type_,
			ParamType::Text
		);
		// Only the params pgro actually acts on are advertised.
		assert_eq!(
			params.len(),
			16,
			"advertising a param pgro doesn't read, or dropping one it does"
		);
		assert!(params.get("anonymise").is_none());
		// Defaults the sketch specified.
		assert_eq!(
			params.get(params::MINIMUM_TTL).unwrap().default,
			Some(json!(7200))
		);
		assert_eq!(
			params.get(params::SWITCHOVER_GRACE).unwrap().default,
			Some(json!(120))
		);
		assert_eq!(
			params.get(params::EXPOSE).unwrap().default,
			Some(json!(false))
		);
		// A `bytes` cap with no default is sent as null when unset.
		assert_eq!(
			params.get(params::STORAGE_SIZE_MAXIMUM).unwrap().default,
			None
		);
	}

	#[test]
	fn descriptors_serialise_to_canopy_wire_shape() {
		// This is the actual `POST /restore-capabilities` body contract, so
		// assert the on-the-wire JSON, not just the Rust field values (catches
		// serde renames / a non-transparent params map).
		let ds = descriptors();
		let v = serde_json::to_value(&ds).unwrap();
		assert_eq!(v[0]["intent"], "verify");
		assert_eq!(v[0]["semantics"], json!(["check", "once"]));
		assert!(v[0]["params"].is_null(), "verify has no params");

		assert_eq!(v[1]["intent"], "upgrade");
		assert_eq!(v[1]["semantics"], json!(["check", "once", "migrate"]));
		assert!(v[1]["params"].is_null(), "upgrade has no params");

		assert_eq!(v[2]["intent"], "analytics");
		// `redact` is not a semantic canopy knows yet: it stores unrecognised
		// ones untouched, so advertising it early is how the capability gets
		// there ahead of canopy's support for it.
		assert_eq!(v[2]["semantics"], json!(["check", "url", "redact"]));
		// params is a flat object of name -> { type, default? }.
		assert_eq!(v[2]["params"]["minimum_ttl"]["type"], "duration");
		assert_eq!(v[2]["params"]["minimum_ttl"]["default"], 7200);
		assert_eq!(v[2]["params"]["storage_size_maximum"]["type"], "bytes");
		assert_eq!(v[2]["params"]["persistent_schemas"]["type"], "text");
		assert_eq!(v[2]["params"]["expose"]["type"], "boolean");
		assert_eq!(v[2]["params"]["redaction_manifest_url"]["type"], "text");
		assert_eq!(
			v[2]["params"]["redaction_version_fallback_to_base"]["default"],
			false
		);
		// a `bytes` param with no default omits the key entirely.
		assert!(
			v[2]["params"]["storage_size_maximum"]
				.get("default")
				.is_none()
		);
	}

	#[test]
	fn config_for_known_and_unknown() {
		assert!(config_for("verify").unwrap().ephemeral);
		assert!(config_for("upgrade").unwrap().ephemeral);
		assert!(!config_for("analytics").unwrap().ephemeral);
		assert!(
			config_for("analytics-dev").is_none(),
			"old split names retired"
		);
		assert!(config_for("analytics-dbt").is_none());
		assert!(config_for("").is_none());
	}

	/// A CPU limit on a database only throttles bursty analytical queries; the
	/// request is what reserves the share. Memory carries both, and
	/// `scale_memory_for_snapshot` sets them equal, so the floor's memory limit
	/// is what a small snapshot lands on for request *and* limit.
	#[test]
	fn analytics_floor_has_no_cpu_limit() {
		let floor = config_for("analytics")
			.unwrap()
			.resources_floor
			.expect("analytics declares a floor");
		let requests = floor.requests.expect("floor sets requests");
		let limits = floor.limits.expect("floor sets limits");

		assert_eq!(requests.get("cpu").expect("cpu request").0, "2");
		assert_eq!(requests.get("memory").expect("memory request").0, "2Gi");
		assert_eq!(limits.get("memory").expect("memory limit").0, "8Gi");
		assert!(
			!limits.contains_key("cpu"),
			"analytics must not cap CPU: the limit throttles dbt without reserving anything the request doesn't"
		);
	}

	/// The floor existed only to lift shm above what a quarter-sized memory
	/// request derived. With request equal to limit, shm settles at 36% of
	/// memory on its own, so a floor here would be dead configuration.
	#[test]
	fn analytics_leaves_shm_to_the_resource_derivation() {
		assert!(config_for("analytics").unwrap().shm_size_floor.is_none());
		let spec = config_for("analytics")
			.unwrap()
			.to_replica_spec(&entry("analytics", "site", json!({})), vec![]);
		assert!(spec.shm_size_floor.is_none());
	}

	#[test]
	fn verify_spec_uses_defaults_and_is_ephemeral() {
		let spec = config_for("verify")
			.unwrap()
			.to_replica_spec(&entry("verify", "site", json!({})), vec![]);
		assert!(spec.ephemeral);
		assert!(spec.minimum_ttl.is_none());
		assert!(spec.persistent_schemas.is_none());
		assert!(spec.service_annotations.is_none());
		assert_eq!(spec.storage_size_maximum.0, "2Ti");
	}

	#[test]
	fn analytics_spec_defaults_when_params_unset() {
		let spec = config_for("analytics")
			.unwrap()
			.to_replica_spec(&entry("analytics", "site", json!({})), vec![]);
		assert!(!spec.ephemeral);
		// Defaults from the intent config (2h TTL, 2m grace).
		assert!(spec.minimum_ttl.is_some());
		assert!(spec.persistent_schemas.is_none());
		assert!(
			spec.service_annotations.is_none(),
			"unset expose = not exposed"
		);
		assert_eq!(spec.storage_size_maximum.0, "2Ti");
		assert!(
			spec.redaction.is_none(),
			"no manifest URL = serve the snapshot's data as-is"
		);
	}

	#[test]
	fn redaction_params_build_the_spec() {
		let spec = config_for("analytics").unwrap().to_replica_spec(
			&entry(
				"analytics",
				"site",
				json!({
					"redaction_manifest_url": "https://docs.example.org/v{version}/manifest.json",
					"redaction_version_query": "SELECT value FROM local_system_facts WHERE key = 'currentVersion'",
					"redaction_version_fallback_to_base": true,
				}),
			),
			vec![],
		);
		let redaction = spec.redaction.expect("manifest URL turns redaction on");
		assert_eq!(
			redaction.manifest_url,
			"https://docs.example.org/v{version}/manifest.json"
		);
		assert_eq!(
			redaction.version_query.as_deref(),
			Some("SELECT value FROM local_system_facts WHERE key = 'currentVersion'")
		);
		assert!(redaction.version_fallback_to_base);
		assert!(
			redaction.version.is_none(),
			"a pinned version goes in the URL, so canopy has no param for it"
		);
	}

	/// A manifest URL with no placeholder needs no version resolution, and
	/// canopy sends unset text params as null or "".
	#[test]
	fn redaction_spec_without_a_version_query() {
		let spec = config_for("analytics").unwrap().to_replica_spec(
			&entry(
				"analytics",
				"site",
				json!({
					"redaction_manifest_url": "https://docs.example.org/v2.41.0/manifest.json",
					"redaction_version_query": "",
					"redaction_version_fallback_to_base": false,
				}),
			),
			vec![],
		);
		let redaction = spec.redaction.expect("manifest URL turns redaction on");
		assert!(redaction.version_query.is_none());
		assert!(!redaction.version_fallback_to_base);
	}

	#[test]
	fn blank_redaction_manifest_url_leaves_redaction_off() {
		for url in [json!(""), json!("   "), Value::Null] {
			let spec = config_for("analytics").unwrap().to_replica_spec(
				&entry(
					"analytics",
					"site",
					json!({ "redaction_manifest_url": url }),
				),
				vec![],
			);
			assert!(
				spec.redaction.is_none(),
				"{url:?} must not enable redaction"
			);
		}
	}

	#[test]
	fn analytics_dbt_via_params() {
		// The old analytics-dbt: persistent schema + exposed + size cap.
		let spec = config_for("analytics").unwrap().to_replica_spec(
			&entry(
				"analytics",
				"example-site",
				json!({
					"persistent_schemas": "dbt",
					"storage_size_maximum": 107374182400i64,
					"expose": true,
				}),
			),
			vec![],
		);
		assert_eq!(
			spec.persistent_schemas.as_deref(),
			Some(&["dbt".to_string()][..])
		);
		assert_eq!(spec.storage_size_maximum.0, "107374182400");
		let annos = spec
			.service_annotations
			.expect("expose=true sets annotations");
		assert_eq!(
			annos.get("tailscale.com/hostname").map(String::as_str),
			Some("infra-replica-example-site")
		);
		assert_eq!(
			annos.get("tailscale.com/expose").map(String::as_str),
			Some("true")
		);
	}

	#[test]
	fn extra_users_param_parses_into_the_list() {
		let spec = config_for("analytics").unwrap().to_replica_spec(
			&entry(
				"analytics",
				"site",
				json!({ "extra_users": "reporting, etl ,dashboards" }),
			),
			vec![],
		);
		assert_eq!(
			spec.extra_users,
			vec![
				"reporting".to_string(),
				"etl".to_string(),
				"dashboards".to_string()
			]
		);
	}

	#[test]
	fn unset_extra_users_leaves_the_list_empty() {
		let spec = config_for("analytics")
			.unwrap()
			.to_replica_spec(&entry("analytics", "site", json!({})), vec![]);
		assert!(spec.extra_users.is_empty());

		let spec = config_for("analytics").unwrap().to_replica_spec(
			&entry("analytics", "site", json!({ "extra_users": "  , " })),
			vec![],
		);
		assert!(spec.extra_users.is_empty());
	}

	#[test]
	fn duration_params_map_to_seconds() {
		let spec = config_for("analytics").unwrap().to_replica_spec(
			&entry(
				"analytics",
				"site",
				json!({ "minimum_ttl": 3600, "switchover_grace": 30 }),
			),
			vec![],
		);
		let ttl = spec.minimum_ttl.expect("minimum_ttl set from param");
		assert_eq!(ttl.0.get_seconds(), 3600);
		assert_eq!(spec.switchover_grace_period.0.get_seconds(), 30);
	}

	#[test]
	fn persistent_schemas_parsing() {
		assert_eq!(parse_persistent_schemas("dbt"), vec!["dbt"]);
		assert_eq!(
			parse_persistent_schemas("dbt, reporting , analytics"),
			vec!["dbt", "reporting", "analytics"]
		);
		assert!(parse_persistent_schemas("").is_empty());
		assert!(parse_persistent_schemas(" , ").is_empty());
	}

	#[test]
	fn empty_persistent_schemas_param_is_treated_as_unset() {
		let spec = config_for("analytics").unwrap().to_replica_spec(
			&entry("analytics", "site", json!({ "persistent_schemas": "" })),
			vec![],
		);
		assert!(spec.persistent_schemas.is_none());
	}

	#[test]
	fn is_exposed_reads_param() {
		assert!(is_exposed(&entry(
			"analytics",
			"s",
			json!({ "expose": true })
		)));
		assert!(!is_exposed(&entry(
			"analytics",
			"s",
			json!({ "expose": false })
		)));
		assert!(!is_exposed(&entry("analytics", "s", json!({}))));
	}

	#[test]
	fn to_replica_spec_sets_canopy_source_from_entry() {
		let spec = config_for("verify")
			.unwrap()
			.to_replica_spec(&entry("verify", "site", json!({})), vec![]);
		assert!(spec.kopia_secret_ref.is_none());
		let cs = spec.canopy_source.expect("canopy_source must be set");
		assert_eq!(cs.r#type, "tamanu-postgres");
	}
}
