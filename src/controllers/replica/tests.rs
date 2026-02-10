use std::time::Duration;

use super::{generate_password, resources::SnapshotInfo, resources::format_bytes, scheduling::*};

#[test]
fn jitter_is_deterministic() {
	let j1 = calculate_jitter("my-replica", Duration::from_secs(300));
	let j2 = calculate_jitter("my-replica", Duration::from_secs(300));
	assert_eq!(j1, j2);
}

#[test]
fn jitter_differs_for_different_names() {
	let j1 = calculate_jitter("replica-a", Duration::from_secs(300));
	let j2 = calculate_jitter("replica-b", Duration::from_secs(300));
	// Extremely unlikely to collide with different names
	assert_ne!(j1, j2);
}

#[test]
fn jitter_zero_max_returns_zero() {
	let j = calculate_jitter("anything", Duration::ZERO);
	assert_eq!(j, Duration::ZERO);
}

#[test]
fn jitter_within_bounds() {
	let max = Duration::from_secs(600);
	for name in ["a", "b", "c", "replica-prod-01", "zzz"] {
		let j = calculate_jitter(name, max);
		assert!(j < max, "jitter {j:?} should be < {max:?} for {name}");
	}
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
fn format_bytes_gigabytes() {
	assert_eq!(format_bytes(10 * 1024 * 1024 * 1024), "10Gi");
	assert_eq!(format_bytes(1024 * 1024 * 1024), "1Gi");
	assert_eq!(format_bytes(2_500_000_000), "3Gi");
}

#[test]
fn format_bytes_gigabytes_exact() {
	assert_eq!(format_bytes(2 * 1024 * 1024 * 1024), "2Gi");
}

#[test]
fn format_bytes_gigabytes_ceil() {
	assert_eq!(format_bytes(1024 * 1024 * 1024 + 1), "2Gi");
}

#[test]
fn format_bytes_megabytes() {
	assert_eq!(format_bytes(500 * 1024 * 1024), "500Mi");
	assert_eq!(format_bytes(1024 * 1024), "1Mi");
}

#[test]
fn format_bytes_megabytes_ceil() {
	assert_eq!(format_bytes(1024 * 1024 + 1), "2Mi");
}

#[test]
fn format_bytes_small_clamps_to_1mi() {
	assert_eq!(format_bytes(0), "1Mi");
	assert_eq!(format_bytes(1024), "1Mi");
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

#[test]
fn normalize_cron_5_fields() {
	assert_eq!(normalize_cron("*/20 * * * *"), "0 */20 * * * * *");
}

#[test]
fn normalize_cron_6_fields() {
	assert_eq!(normalize_cron("*/20 * * * * *"), "0 */20 * * * * *");
}

#[test]
fn normalize_cron_7_fields_unchanged() {
	assert_eq!(normalize_cron("0 */20 * * * * *"), "0 */20 * * * * *");
}

#[test]
fn compute_next_scheduled_restore_5_field() {
	let next = compute_next_scheduled_restore("*/20 * * * *");
	assert!(next.is_some(), "standard 5-field cron should parse");
}

#[test]
fn compute_next_scheduled_restore_7_field() {
	let next = compute_next_scheduled_restore("0 */20 * * * * *");
	assert!(next.is_some(), "7-field cron should parse");
}

#[test]
fn compute_next_scheduled_restore_invalid() {
	let next = compute_next_scheduled_restore("not a cron");
	assert!(next.is_none());
}
