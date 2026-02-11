use super::{generate_password, resources::SnapshotInfo};

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
fn snapshot_info_parse() {
	let raw = r#"{"id":"abc123def","size":5368709120}"#;
	let snap: SnapshotInfo = serde_json::from_str(raw).unwrap();
	assert_eq!(snap.id, "abc123def");
	assert_eq!(snap.size, 5368709120);
}

#[test]
fn snapshot_info_parse_zero_size() {
	let raw = r#"{"id":"snap0","size":0}"#;
	let snap: SnapshotInfo = serde_json::from_str(raw).unwrap();
	assert_eq!(snap.id, "snap0");
	assert_eq!(snap.size, 0);
}
