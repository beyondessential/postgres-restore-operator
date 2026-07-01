//! Per-intent defaults for canopy-managed replicas.
//!
//! Concentrates every intent-driven knob in one place. Adding a new intent
//! is: extend [`PGRO_SUPPORTED_INTENTS`], add a match arm in [`config_for`],
//! done — no scattering of `match intent { ... }` across the codebase.
//!
//! Canopy dispatches only intents that appear in
//! [`PGRO_SUPPORTED_INTENTS`]; anything else surfaces as a configuration
//! gap in the operator UI and never reaches pgro. So a "safe fallback"
//! branch is only used defensively.

use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::{
	api::core::v1::ResourceRequirements, apimachinery::pkg::api::resource::Quantity,
};

/// Intents pgro registers via `POST /restore-capabilities` on startup.
///
/// Kept in sync with [`config_for`] — every intent listed here must have a
/// concrete match arm there.
pub const PGRO_SUPPORTED_INTENTS: &[&str] = &["verify", "analytics-dev", "analytics-dbt"];

/// Per-intent configuration for the canopy-backed replica lifecycle.
#[derive(Debug, Clone)]
pub struct IntentConfig {
	/// Set `default_transaction_read_only = on` in the restored postgres.
	pub read_only: bool,
	/// Postgres container resource requests / limits.
	pub postgres_resources: ResourceRequirements,
	/// `/dev/shm` sizing for the postgres pod. Feeds
	/// `compute_shm_and_shared_buffers` in the shared Deployment builder.
	pub shm_floor: Quantity,
	/// Minimum time between refreshes: even if the desired snapshot id
	/// changes, don't refresh a replica that was restored fewer than this
	/// many seconds ago. `None` disables the gate.
	pub min_ttl: Option<Duration>,
	/// Schemas to migrate across restores via `pg_dump | psql`. Empty =
	/// no persistent-schemas migration (default; matches `verify` and
	/// `analytics-dev`).
	pub persistent_schemas: Vec<String>,
	/// After a successful switchover, wait this long before deleting the
	/// previous Deployment + PVC. Gives long-running client connections
	/// a chance to drain. `None` means immediate teardown.
	pub switchover_grace: Option<Duration>,
	/// Annotations applied to the postgres Service. Templated with
	/// [`substitute_service_annotations`] — the only currently-supported
	/// placeholder is `{name}`, which expands to the declaration's name
	/// from the worklist entry.
	pub service_annotations: BTreeMap<String, String>,
}

/// Look up the config for an intent name. Unknown intents fall through to
/// the `verify` shape, but canopy shouldn't dispatch them because pgro
/// doesn't register them.
pub fn config_for(intent: &str) -> IntentConfig {
	match intent {
		"analytics-dev" => analytics_dev(),
		"analytics-dbt" => analytics_dbt(),
		_ => verify(),
	}
}

fn verify() -> IntentConfig {
	IntentConfig {
		read_only: true,
		postgres_resources: resources("250m", "512Mi", "2", "2Gi"),
		shm_floor: Quantity("512Mi".into()),
		min_ttl: None,
		persistent_schemas: Vec::new(),
		switchover_grace: None,
		service_annotations: BTreeMap::new(),
	}
}

fn analytics_dev() -> IntentConfig {
	IntentConfig {
		read_only: true,
		postgres_resources: resources("500m", "2Gi", "4", "8Gi"),
		shm_floor: Quantity("2Gi".into()),
		min_ttl: None,
		persistent_schemas: Vec::new(),
		switchover_grace: None,
		service_annotations: BTreeMap::new(),
	}
}

fn analytics_dbt() -> IntentConfig {
	IntentConfig {
		read_only: true,
		postgres_resources: resources("500m", "2Gi", "4", "8Gi"),
		shm_floor: Quantity("2Gi".into()),
		min_ttl: Some(Duration::from_secs(2 * 60 * 60)),
		persistent_schemas: vec!["dbt".to_string()],
		switchover_grace: Some(Duration::from_secs(2 * 60)),
		service_annotations: BTreeMap::from([
			("tailscale.com/expose".to_string(), "true".to_string()),
			(
				"tailscale.com/hostname".to_string(),
				"infra-replica-{name}".to_string(),
			),
		]),
	}
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

/// Substitute `{name}` in every value of a service-annotations map with
/// `name`. Currently the only supported placeholder — the declaration's
/// name from the worklist entry.
pub fn substitute_service_annotations(
	annos: &BTreeMap<String, String>,
	name: &str,
) -> BTreeMap<String, String> {
	annos
		.iter()
		.map(|(k, v)| (k.clone(), v.replace("{name}", name)))
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn all_supported_intents_have_configs() {
		for intent in PGRO_SUPPORTED_INTENTS {
			let cfg = config_for(intent);
			assert!(cfg.postgres_resources.requests.is_some(), "{intent}");
		}
	}

	#[test]
	fn analytics_dbt_has_expected_settings() {
		let cfg = config_for("analytics-dbt");
		assert_eq!(cfg.min_ttl, Some(Duration::from_secs(2 * 60 * 60)));
		assert_eq!(cfg.persistent_schemas, vec!["dbt".to_string()]);
		assert_eq!(cfg.switchover_grace, Some(Duration::from_secs(120)));
		assert_eq!(
			cfg.service_annotations.get("tailscale.com/expose"),
			Some(&"true".to_string())
		);
		assert_eq!(
			cfg.service_annotations.get("tailscale.com/hostname"),
			Some(&"infra-replica-{name}".to_string())
		);
	}

	#[test]
	fn substitute_name_expands_placeholder() {
		let mut annos = BTreeMap::new();
		annos.insert("h".to_string(), "infra-replica-{name}".to_string());
		annos.insert("static".to_string(), "unchanged".to_string());
		let out = substitute_service_annotations(&annos, "nauru-analytics");
		assert_eq!(out.get("h").unwrap(), "infra-replica-nauru-analytics");
		assert_eq!(out.get("static").unwrap(), "unchanged");
	}

	#[test]
	fn unknown_intent_falls_through_to_verify() {
		let cfg = config_for("something-else");
		assert!(cfg.persistent_schemas.is_empty());
		assert!(cfg.min_ttl.is_none());
		assert!(cfg.service_annotations.is_empty());
	}

	#[test]
	fn analytics_dev_has_no_migration() {
		let cfg = config_for("analytics-dev");
		assert!(cfg.persistent_schemas.is_empty());
		assert!(cfg.min_ttl.is_none());
	}
}
