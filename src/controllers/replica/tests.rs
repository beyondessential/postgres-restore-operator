use k8s_openapi::api::core::v1::SecretReference;
use kube::api::ObjectMeta;

use crate::{kopia::Snapshot, types::*, util::TimeSpan};

use super::{
	generate_password, persistent_schemas_migration_settled, resources::build_snapshot_list_job,
	snapshot_already_covered,
};

fn make_replica(
	persistent_schemas: Option<Vec<String>>,
	schema_migration_phase: Option<SchemaMigrationPhase>,
) -> PostgresPhysicalReplica {
	PostgresPhysicalReplica {
		metadata: ObjectMeta {
			name: Some("test".into()),
			namespace: Some("default".into()),
			..Default::default()
		},
		spec: PostgresPhysicalReplicaSpec {
			kopia_secret_ref: Some(SecretReference {
				name: Some("creds".into()),
				namespace: None,
			}),
			canopy_source: None,
			snapshot_filter: None,
			schedule: "0 * * * *".into(),
			schedule_jitter: TimeSpan(jiff::Span::new()),
			minimum_ttl: None,
			switchover_grace_period: TimeSpan(jiff::Span::new()),
			analytics_username: "analytics".into(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			shm_size_floor: None,
			service_annotations: None,
			pod_annotations: None,
			affinity: None,
			tolerations: vec![],
			read_only: true,
			ephemeral: false,
			postgres_extra_config: None,
			notifications: vec![],
			persistent_schemas,
			storage_size_maximum: k8s_openapi::apimachinery::pkg::api::resource::Quantity(
				"2Ti".to_string(),
			),
		},
		status: schema_migration_phase.map(|p| PostgresPhysicalReplicaStatus {
			schema_migration_phase: Some(p),
			..Default::default()
		}),
	}
}

#[test]
fn migration_settled_when_persistent_schemas_unset() {
	let replica = make_replica(None, None);
	assert!(persistent_schemas_migration_settled(&replica));
}

#[test]
fn migration_settled_when_no_status() {
	let replica = make_replica(Some(vec!["dbt".into()]), None);
	assert!(persistent_schemas_migration_settled(&replica));
}

#[test]
fn migration_settled_in_terminal_phases() {
	let terminal = [
		SchemaMigrationPhase::Complete,
		SchemaMigrationPhase::Partial,
		SchemaMigrationPhase::TimeoutSkipped,
		SchemaMigrationPhase::Failed("stuff".into()),
	];
	for phase in terminal {
		let replica = make_replica(Some(vec!["dbt".into()]), Some(phase.clone()));
		assert!(
			persistent_schemas_migration_settled(&replica),
			"phase {phase:?} should let sweep proceed"
		);
	}
}

#[test]
fn migration_blocks_sweep_only_when_active() {
	let replica = make_replica(Some(vec!["dbt".into()]), Some(SchemaMigrationPhase::Active));
	assert!(
		!persistent_schemas_migration_settled(&replica),
		"active phase must block sweep so we don't delete the migration source"
	);
}

#[test]
fn generate_password_length_and_charset() {
	let pw = generate_password();
	assert_eq!(pw.len(), 32);
	assert!(pw.chars().all(|c| c.is_ascii_alphanumeric()));
}

#[test]
fn generate_password_is_random() {
	let pw1 = generate_password();
	let pw2 = generate_password();
	assert_ne!(pw1, pw2);
}

#[test]
fn parse_kopia_snapshot_list() {
	let raw = r#"[
		{
			"id": "abc123def",
			"description": "",
			"source": {"host": "db-prod-01", "userName": "kopia", "path": "/data"},
			"tags": {},
			"startTime": "2024-06-15T12:00:00Z",
			"stats": {"totalSize": 5368709120}
		},
		{
			"id": "xyz789ghi",
			"description": "daily backup",
			"source": {"host": "db-prod-02", "userName": "kopia", "path": "/data"},
			"tags": {"tag:env": "prod"},
			"startTime": "2024-06-16T12:00:00Z",
			"stats": {"totalSize": 1073741824}
		}
	]"#;
	let snaps: Vec<Snapshot> = serde_json::from_str(raw).unwrap();
	assert_eq!(snaps.len(), 2);
	assert_eq!(snaps[0].id, "abc123def");
	assert_eq!(snaps[0].hostname(), "db-prod-01");
	assert_eq!(snaps[0].total_size_bytes(), 5368709120);
	assert_eq!(snaps[1].id, "xyz789ghi");
	assert_eq!(snaps[1].description, "daily backup");
	assert_eq!(snaps[1].total_size_bytes(), 1073741824);
}

#[test]
fn parse_kopia_snapshot_list_empty() {
	let raw = "[]";
	let snaps: Vec<Snapshot> = serde_json::from_str(raw).unwrap();
	assert!(snaps.is_empty());
}

#[test]
fn parse_kopia_snapshot_missing_optional_fields() {
	let raw = r#"[{"id": "snap0"}]"#;
	let snaps: Vec<Snapshot> = serde_json::from_str(raw).unwrap();
	assert_eq!(snaps.len(), 1);
	assert_eq!(snaps[0].id, "snap0");
	assert_eq!(snaps[0].total_size_bytes(), 0);
	assert_eq!(snaps[0].hostname(), "");
}

#[test]
fn parse_kopia_snapshot_with_backslash_paths() {
	let raw = r#"[{
		"id": "snap1",
		"source": {"host": "win-server", "userName": "admin", "path": "C:\\Users\\backup\\data"},
		"startTime": "2024-06-15T12:00:00Z",
		"stats": {"totalSize": 1024}
	}]"#;
	let snaps: Vec<Snapshot> = serde_json::from_str(raw).unwrap();
	assert_eq!(snaps[0].source.path, r"C:\Users\backup\data");
}

#[test]
fn snapshot_list_job_rotates_kopia_logs() {
	// Snapshot-list jobs run on every scheduled reconcile (many times
	// per day per replica). Without log rotation kopia's CLI logs
	// accumulate in the pod's writable layer / cache PVC over time and
	// eventually contribute to filling it. Confirm the script applies
	// the global log-rotation flags to every kopia invocation.
	let replica = PostgresPhysicalReplica {
		metadata: ObjectMeta {
			name: Some("test".into()),
			namespace: Some("default".into()),
			uid: Some("uid".into()),
			..Default::default()
		},
		spec: PostgresPhysicalReplicaSpec {
			kopia_secret_ref: Some(SecretReference {
				name: Some("creds".into()),
				namespace: None,
			}),
			canopy_source: None,
			snapshot_filter: None,
			schedule: "0 * * * *".into(),
			schedule_jitter: TimeSpan(jiff::Span::new()),
			minimum_ttl: None,
			switchover_grace_period: TimeSpan(jiff::Span::new()),
			analytics_username: "analytics".into(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			shm_size_floor: None,
			service_annotations: None,
			pod_annotations: None,
			affinity: None,
			tolerations: vec![],
			read_only: true,
			ephemeral: false,
			postgres_extra_config: None,
			notifications: vec![],
			persistent_schemas: None,
			storage_size_maximum: k8s_openapi::apimachinery::pkg::api::resource::Quantity(
				"2Ti".to_string(),
			),
		},
		status: None,
	};

	let job = build_snapshot_list_job(
		&replica,
		"test-snap",
		"default",
		"kopia:latest",
		"http://x",
		None,
	)
	.expect("job builds");
	let script = job.spec.unwrap().template.spec.unwrap().containers[0]
		.args
		.as_ref()
		.unwrap()[0]
		.clone();
	assert!(
		script.contains("--log-dir-max-files=20"),
		"snapshot-list script must rotate kopia logs by file count"
	);
	assert!(
		script.contains("--log-dir-max-age=24h"),
		"snapshot-list script must rotate kopia logs by age"
	);
}

fn make_restore(snapshot: &str, phase: Option<RestorePhase>) -> PostgresPhysicalRestore {
	PostgresPhysicalRestore {
		metadata: ObjectMeta {
			name: Some(format!("test-{snapshot}")),
			namespace: Some("default".into()),
			..Default::default()
		},
		spec: PostgresPhysicalRestoreSpec {
			replica: k8s_openapi::api::core::v1::LocalObjectReference {
				name: "test".into(),
			},
			snapshot: snapshot.into(),
			snapshot_size: k8s_openapi::apimachinery::pkg::api::resource::Quantity("1Gi".into()),
			snapshot_time: None,
			storage_size: k8s_openapi::apimachinery::pkg::api::resource::Quantity("2Gi".into()),
		},
		status: phase.map(|p| PostgresPhysicalRestoreStatus {
			phase: Some(p),
			..Default::default()
		}),
	}
}

#[test]
fn snapshot_covered_by_verified_marker() {
	// Ephemeral `verify` replica: the restore has been torn down (no live
	// restores) but the marker records that we already verified the
	// snapshot, so we must not restore it again and double-report.
	assert!(snapshot_already_covered("snapA", Some("snapA"), &[]));
	assert!(!snapshot_already_covered("snapB", Some("snapA"), &[]));
}

#[test]
fn snapshot_covered_by_live_restore() {
	for phase in [
		RestorePhase::Pending,
		RestorePhase::Restoring,
		RestorePhase::Ready,
		RestorePhase::Switching,
		RestorePhase::Active,
	] {
		let restores = [make_restore("snapA", Some(phase.clone()))];
		assert!(
			snapshot_already_covered("snapA", None, &restores),
			"a {phase:?} restore on the snapshot must count as covered"
		);
	}
}

#[test]
fn failed_restore_does_not_cover_snapshot() {
	// A Failed restore is allowed to be retried via the failure backoff
	// path, so it must not block a fresh restore of the same snapshot.
	let restores = [make_restore("snapA", Some(RestorePhase::Failed))];
	assert!(!snapshot_already_covered("snapA", None, &restores));
}

#[test]
fn uncovered_snapshot_is_created() {
	// No marker, and the only live restore is for a different snapshot.
	let restores = [make_restore("snapOld", Some(RestorePhase::Active))];
	assert!(!snapshot_already_covered("snapNew", None, &restores));
	// A restore with no status yet (phase None) still counts as live.
	let pending = [make_restore("snapNew", None)];
	assert!(snapshot_already_covered("snapNew", None, &pending));
}
