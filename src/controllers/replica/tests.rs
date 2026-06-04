use k8s_openapi::api::core::v1::SecretReference;
use kube::api::ObjectMeta;

use crate::{kopia::Snapshot, types::*, util::TimeSpan};

use super::{generate_password, resources::build_snapshot_list_job};

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
			kopia_secret_ref: SecretReference {
				name: Some("creds".into()),
				namespace: None,
			},
			snapshot_filter: None,
			schedule: "0 * * * *".into(),
			schedule_jitter: TimeSpan(jiff::Span::new()),
			minimum_ttl: None,
			switchover_grace_period: TimeSpan(jiff::Span::new()),
			analytics_username: "analytics".into(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			service_annotations: None,
			pod_annotations: None,
			affinity: None,
			tolerations: vec![],
			read_only: true,
			postgres_extra_config: None,
			notifications: vec![],
			persistent_schemas: None,
			storage_size_maximum: k8s_openapi::apimachinery::pkg::api::resource::Quantity(
				"2Ti".to_string(),
			),
		},
		status: None,
	};

	let job = build_snapshot_list_job(&replica, "test-snap", "default", "kopia:latest", "http://x")
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
