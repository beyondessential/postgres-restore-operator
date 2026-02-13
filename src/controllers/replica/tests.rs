use crate::kopia::Snapshot;

use super::generate_password;

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
