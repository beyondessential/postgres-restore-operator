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
	types::{CanopySource, PostgresPhysicalReplicaSpec},
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
	/// `boolean` — expose the replica on the tailnet and report its URL.
	pub const EXPOSE: &str = "expose";
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
		(
			params::EXPOSE.to_string(),
			param(ParamType::Boolean, Some(json!(false))),
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
	]))
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
			.intent("analytics".to_string())
			.description(
				"Keep a long-lived read-only query replica restored from the latest snapshot."
					.to_string(),
			)
			.semantics(vec![
				semantics::CHECK.to_string(),
				semantics::URL.to_string(),
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
	/// True for `verify` (throwaway snapshot check), false for `analytics`
	/// (long-lived query replica).
	pub ephemeral: bool,
	/// Floor on the postgres pod's `/dev/shm` sizing. Materialised into
	/// `PostgresPhysicalReplicaSpec.shm_size_floor` so the shared Deployment
	/// builder picks `max(computed_from_resources, floor)`. Analytics
	/// workloads want a higher shm than a 2 GiB memory request derives,
	/// without paying the k8s scheduling cost of bumping the request.
	pub shm_size_floor: Quantity,
}

fn resources(cpu_req: &str, mem_req: &str, cpu_lim: &str, mem_lim: &str) -> ResourceRequirements {
	ResourceRequirements {
		requests: Some(BTreeMap::from([
			("cpu".to_string(), Quantity(cpu_req.to_string())),
			("memory".to_string(), Quantity(mem_req.to_string())),
		])),
		limits: Some(BTreeMap::from([
			("cpu".to_string(), Quantity(cpu_lim.to_string())),
			("memory".to_string(), Quantity(mem_lim.to_string())),
		])),
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
		"verify" => Some(IntentConfig {
			resources_floor: Some(resources("250m", "512Mi", "2", "2Gi")),
			read_only: true,
			minimum_ttl: None,
			switchover_grace_period: TimeSpan(Span::new().minutes(5)),
			persistent_schemas: None,
			storage_size_override: Quantity("20Gi".to_string()),
			storage_size_maximum: Quantity("2Ti".to_string()),
			ephemeral: true,
			resources_maximum: Quantity("8Gi".to_string()),
			deployment_ready_timeout: None,
			shm_size_floor: Quantity("512Mi".to_string()),
		}),
		"analytics" => Some(IntentConfig {
			resources_floor: Some(resources("500m", "2Gi", "4", "8Gi")),
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
			shm_size_floor: Quantity("2Gi".to_string()),
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
		let service_annotations = is_exposed(entry).then(|| expose_annotations(&entry.name));

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
			storage_class: None,
			storage_size_override: Some(self.storage_size_override.clone()),
			resources: pinned_resources,
			resources_floor: self.resources_floor.clone(),
			resources_maximum: Some(resources_maximum),
			deployment_ready_timeout,
			shm_size_floor: Some(self.shm_size_floor.clone()),
			service_annotations,
			pod_annotations: None,
			affinity: None,
			tolerations: Vec::new(),
			read_only: self.read_only,
			ephemeral: self.ephemeral,
			postgres_extra_config: None,
			notifications,
			persistent_schemas,
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

	#[test]
	fn descriptors_advertise_expected_intents_and_semantics() {
		let ds = descriptors();
		let names: Vec<&str> = ds.iter().map(|d| d.intent.as_str()).collect();
		assert_eq!(names, ["verify", "analytics"]);

		let verify = &ds[0];
		assert_eq!(verify.semantics, ["check", "once"]);
		assert!(verify.params.is_none(), "verify takes no params");
		assert!(verify.description.is_some());

		let analytics = &ds[1];
		assert_eq!(analytics.semantics, ["check", "url"]);
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
			params.get(params::EXPOSE).unwrap().type_,
			ParamType::Boolean
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
		// Only the params pgro actually acts on are advertised.
		assert_eq!(
			params.len(),
			11,
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

		assert_eq!(v[1]["intent"], "analytics");
		assert_eq!(v[1]["semantics"], json!(["check", "url"]));
		// params is a flat object of name -> { type, default? }.
		assert_eq!(v[1]["params"]["minimum_ttl"]["type"], "duration");
		assert_eq!(v[1]["params"]["minimum_ttl"]["default"], 7200);
		assert_eq!(v[1]["params"]["storage_size_maximum"]["type"], "bytes");
		assert_eq!(v[1]["params"]["persistent_schemas"]["type"], "text");
		assert_eq!(v[1]["params"]["expose"]["type"], "boolean");
		// a `bytes` param with no default omits the key entirely.
		assert!(
			v[1]["params"]["storage_size_maximum"]
				.get("default")
				.is_none()
		);
	}

	#[test]
	fn config_for_known_and_unknown() {
		assert!(config_for("verify").unwrap().ephemeral);
		assert!(!config_for("analytics").unwrap().ephemeral);
		assert!(
			config_for("analytics-dev").is_none(),
			"old split names retired"
		);
		assert!(config_for("analytics-dbt").is_none());
		assert!(config_for("").is_none());
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
