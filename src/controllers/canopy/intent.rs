//! Intent-driven configuration for canopy-backed replicas.
//!
//! Each canopy `WorklistEntry` carries an `intent` string. pgro registers
//! its supported intents at startup ([`SUPPORTED`]) so canopy only
//! dispatches entries it can handle. On each tick, the syncer looks up the
//! entry's intent in [`config_for`] and calls
//! [`IntentConfig::to_replica_spec`] to materialise a
//! `PostgresPhysicalReplicaSpec` — the CR path takes over from there.
//!
//! Adding an intent is a two-step edit:
//! 1. Add a new [`IntentConfig`] entry to [`config_for`].
//! 2. Extend [`SUPPORTED`] with the new intent name.

use std::collections::BTreeMap;

use bestool_canopy::WorklistEntry;
use jiff::Span;
use k8s_openapi::{
	api::core::v1::{ResourceRequirements, SecretReference},
	apimachinery::pkg::api::resource::Quantity,
};

use crate::{
	types::{CanopySource, PostgresPhysicalReplicaSpec},
	util::TimeSpan,
};

/// pgro-supported intent names, registered with canopy at operator startup.
/// Canopy only dispatches worklist entries whose intent appears here.
pub const SUPPORTED: &[&str] = &["verify", "analytics-dev", "analytics-dbt"];

/// Intent-derived spec fragments merged onto the base replica spec.
#[derive(Debug, Clone)]
pub struct IntentConfig {
	pub resources: Option<ResourceRequirements>,
	pub read_only: bool,
	pub minimum_ttl: Option<TimeSpan>,
	pub persistent_schemas: Option<Vec<String>>,
	/// Service annotations. `{name}` in a value is substituted with the
	/// worklist entry's `name` at materialisation time.
	pub service_annotations: Option<BTreeMap<String, String>>,
	pub switchover_grace_period: TimeSpan,
	pub storage_size_override: Quantity,
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
/// `None` for unsupported intents — canopy shouldn't dispatch these
/// because they aren't in [`SUPPORTED`], but callers must still handle
/// the possibility (e.g. a worklist entry sneaking through during an
/// operator downgrade).
pub fn config_for(intent: &str) -> Option<IntentConfig> {
	match intent {
		"verify" => Some(IntentConfig {
			resources: Some(resources("250m", "512Mi", "2", "2Gi")),
			read_only: true,
			minimum_ttl: None,
			persistent_schemas: None,
			service_annotations: None,
			switchover_grace_period: TimeSpan(Span::new().minutes(5)),
			storage_size_override: Quantity("20Gi".to_string()),
		}),
		"analytics-dev" => Some(IntentConfig {
			resources: Some(resources("500m", "2Gi", "4", "8Gi")),
			read_only: true,
			minimum_ttl: None,
			persistent_schemas: None,
			service_annotations: None,
			switchover_grace_period: TimeSpan(Span::new().minutes(5)),
			storage_size_override: Quantity("50Gi".to_string()),
		}),
		"analytics-dbt" => Some(IntentConfig {
			resources: Some(resources("500m", "2Gi", "4", "8Gi")),
			read_only: true,
			minimum_ttl: Some(TimeSpan(Span::new().hours(2))),
			persistent_schemas: Some(vec!["dbt".to_string()]),
			service_annotations: Some(BTreeMap::from([
				("tailscale.com/expose".to_string(), "true".to_string()),
				(
					"tailscale.com/hostname".to_string(),
					"infra-replica-{name}".to_string(),
				),
			])),
			switchover_grace_period: TimeSpan(Span::new().minutes(2)),
			storage_size_override: Quantity("50Gi".to_string()),
		}),
		_ => None,
	}
}

/// Substitute `{name}` in each value with `entry_name`. Other braces are
/// left alone; there's only one placeholder in current use.
fn substitute_annotations(
	base: BTreeMap<String, String>,
	entry_name: &str,
) -> BTreeMap<String, String> {
	base.into_iter()
		.map(|(k, v)| (k, v.replace("{name}", entry_name)))
		.collect()
}

impl IntentConfig {
	/// Materialise a `PostgresPhysicalReplicaSpec` for a canopy-managed
	/// replica. The syncer patches the CR with this spec on Provision /
	/// re-asserts it on subsequent ticks so drift from manual edits is
	/// self-healing.
	pub fn to_replica_spec(
		&self,
		entry: &WorklistEntry,
		notifications: Vec<crate::types::NotificationConfig>,
	) -> PostgresPhysicalReplicaSpec {
		PostgresPhysicalReplicaSpec {
			kopia_secret_ref: None,
			canopy_source: Some(CanopySource {
				group: entry.group_id.to_string(),
				r#type: entry.r#type.to_string(),
			}),
			snapshot_filter: None,
			// Long cadence — the actual restore trigger on the canopy
			// path is a change to `status.canopyDesiredSnapshotId`
			// written by the worklist syncer. The cron is a
			// belt-and-braces fallback (e.g. missed status watch).
			schedule: "H * * * *".to_string(),
			schedule_jitter: TimeSpan(Span::new().minutes(10)),
			minimum_ttl: self.minimum_ttl,
			switchover_grace_period: self.switchover_grace_period,
			analytics_username: "analytics".to_string(),
			storage_class: None,
			storage_size_override: Some(self.storage_size_override.clone()),
			resources: self.resources.clone(),
			service_annotations: self
				.service_annotations
				.clone()
				.map(|a| substitute_annotations(a, &entry.name)),
			pod_annotations: None,
			affinity: None,
			tolerations: Vec::new(),
			read_only: self.read_only,
			postgres_extra_config: None,
			notifications,
			persistent_schemas: self.persistent_schemas.clone(),
			storage_size_maximum: Quantity("2Ti".to_string()),
		}
	}

	/// Name of the namespace-local Secret the canopy syncer materialises
	/// with the worklist entry's bucket / region / prefix / repo password
	/// + dummy AWS keys. `build_restore_job` mounts it via env_from_secret.
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

#[cfg(test)]
mod tests {
	use super::*;
	use uuid::Uuid;

	fn entry(intent: &str, name: &str) -> WorklistEntry {
		serde_json::from_value(serde_json::json!({
			"replica_id": Uuid::new_v4().to_string(),
			"group_id": Uuid::new_v4().to_string(),
			"server_id": Uuid::new_v4().to_string(),
			"type": "tamanu-postgres",
			"intent": intent,
			"name": name,
			"snapshot_id": "abc123",
			"snapshot_at": "2026-07-01T00:00:00Z",
			"storage": "s3",
			"bucket": "canopy-test",
			"prefix": "",
			"region": "ap-southeast-2",
		}))
		.unwrap()
	}

	#[test]
	fn config_for_verify() {
		let cfg = config_for("verify").expect("verify is supported");
		assert!(cfg.read_only);
		assert!(cfg.minimum_ttl.is_none());
		assert!(cfg.persistent_schemas.is_none());
	}

	#[test]
	fn config_for_analytics_dbt_has_all_extras() {
		let cfg = config_for("analytics-dbt").expect("analytics-dbt is supported");
		assert!(cfg.minimum_ttl.is_some());
		assert_eq!(
			cfg.persistent_schemas.as_deref(),
			Some(&["dbt".to_string()][..])
		);
		assert!(cfg.service_annotations.is_some());
	}

	#[test]
	fn config_for_unknown_intent() {
		assert!(config_for("disaster-recovery").is_none());
		assert!(config_for("").is_none());
	}

	#[test]
	fn supported_names_all_resolve() {
		for name in SUPPORTED {
			assert!(
				config_for(name).is_some(),
				"SUPPORTED lists {name} but config_for returned None"
			);
		}
	}

	#[test]
	fn to_replica_spec_substitutes_name_in_service_annotations() {
		let cfg = config_for("analytics-dbt").unwrap();
		let e = entry("analytics-dbt", "example-site");
		let spec = cfg.to_replica_spec(&e, vec![]);
		let annos = spec.service_annotations.expect("dbt has annotations");
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
	fn to_replica_spec_sets_canopy_source_from_entry() {
		let cfg = config_for("verify").unwrap();
		let e = entry("verify", "test");
		let spec = cfg.to_replica_spec(&e, vec![]);
		assert!(spec.kopia_secret_ref.is_none());
		let cs = spec.canopy_source.expect("canopy_source must be set");
		assert_eq!(cs.r#type, "tamanu-postgres");
		assert_eq!(cs.group, e.group_id.to_string());
	}

	#[test]
	fn to_replica_spec_dbt_carries_migration_settings() {
		let cfg = config_for("analytics-dbt").unwrap();
		let e = entry("analytics-dbt", "test");
		let spec = cfg.to_replica_spec(&e, vec![]);
		assert!(spec.minimum_ttl.is_some());
		assert_eq!(
			spec.persistent_schemas.as_deref(),
			Some(&["dbt".to_string()][..])
		);
	}
}
