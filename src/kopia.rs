use k8s_openapi::api::core::v1::Secret;
use serde::{Deserialize, Serialize};

use crate::{error::Error, types::SnapshotFilter, util::glob_matches};

/// Credentials extracted from a kopia Kubernetes Secret.
#[derive(Debug, Clone)]
pub struct KopiaCredentials {
	pub bucket: String,
	pub region: String,
	pub access_key_id: String,
	pub secret_access_key: String,
	pub repository_password: String,
}

/// A kopia snapshot, as returned by `kopia snapshot list --json`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Snapshot {
	pub id: String,
	#[serde(default)]
	pub description: String,
	#[serde(default)]
	pub hostname: String,
	#[serde(default)]
	pub username: String,
	#[serde(default)]
	pub tags: std::collections::HashMap<String, String>,
	#[serde(default)]
	pub start_time: String,
	#[serde(default, rename = "summary")]
	pub summary: Option<SnapshotSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotSummary {
	#[serde(default)]
	pub size: u64,
}

impl Snapshot {
	/// Returns the total size in bytes, or 0 if unknown.
	pub fn total_size_bytes(&self) -> u64 {
		self.summary.as_ref().map(|s| s.size).unwrap_or(0)
	}
}

const REQUIRED_KEYS: &[&str] = &[
	"bucket",
	"region",
	"accessKeyId",
	"secretAccessKey",
	"repositoryPassword",
];

/// Validates a Kubernetes Secret has all required kopia keys.
pub fn validate_kopia_secret(secret: &Secret) -> Result<KopiaCredentials, Error> {
	let secret_name = secret.metadata.name.as_deref().unwrap_or("<unnamed>");

	let data = secret
		.data
		.as_ref()
		.ok_or_else(|| Error::InvalidKopiaSecret {
			secret: secret_name.to_string(),
			reason: "secret has no data".to_string(),
		})?;

	for key in REQUIRED_KEYS {
		if !data.contains_key(*key) {
			return Err(Error::InvalidKopiaSecret {
				secret: secret_name.to_string(),
				reason: format!("missing key: {key}"),
			});
		}
	}

	let get_string = |key: &str| -> Result<String, Error> {
		let bytes = &data[key].0;
		String::from_utf8(bytes.clone()).map_err(|_| Error::InvalidKopiaSecret {
			secret: secret_name.to_string(),
			reason: format!("key {key} is not valid UTF-8"),
		})
	};

	Ok(KopiaCredentials {
		bucket: get_string("bucket")?,
		region: get_string("region")?,
		access_key_id: get_string("accessKeyId")?,
		secret_access_key: get_string("secretAccessKey")?,
		repository_password: get_string("repositoryPassword")?,
	})
}

/// Filters snapshots according to the SnapshotFilter spec.
pub fn filter_snapshots(snapshots: &[Snapshot], filter: Option<&SnapshotFilter>) -> Vec<Snapshot> {
	let Some(filter) = filter else {
		return snapshots.to_vec();
	};

	snapshots
		.iter()
		.filter(|snap| {
			// Tag filtering
			if let Some(required_tags) = &filter.tags {
				for (k, v) in required_tags {
					match snap.tags.get(k) {
						Some(sv) if sv == v => {}
						_ => return false,
					}
				}
			}

			// Host pattern filtering
			if let Some(pattern) = &filter.host_pattern {
				if !glob_matches(pattern, &snap.hostname) {
					return false;
				}
			}

			true
		})
		.cloned()
		.collect()
}

/// Returns the latest snapshot from a list, sorted by start_time descending.
pub fn latest_snapshot(snapshots: &[Snapshot]) -> Option<&Snapshot> {
	snapshots
		.iter()
		.max_by(|a, b| a.start_time.cmp(&b.start_time))
}

/// Builds the kopia CLI args for connecting to a repository.
/// Used when constructing Job commands.
pub fn kopia_connect_args(creds: &KopiaCredentials) -> Vec<String> {
	vec![
		"repository".to_string(),
		"connect".to_string(),
		"s3".to_string(),
		format!("--bucket={}", creds.bucket),
		format!("--region={}", creds.region),
		format!("--access-key={}", creds.access_key_id),
		format!("--secret-access-key={}", creds.secret_access_key),
		format!("--password={}", creds.repository_password),
	]
}

#[cfg(test)]
mod tests {
	use super::*;
	use k8s_openapi::ByteString;
	use std::collections::{BTreeMap, HashMap};

	fn make_secret(data: BTreeMap<String, ByteString>) -> Secret {
		Secret {
			metadata: kube::api::ObjectMeta {
				name: Some("test-secret".into()),
				..Default::default()
			},
			data: Some(data),
			..Default::default()
		}
	}

	fn valid_secret_data() -> BTreeMap<String, ByteString> {
		BTreeMap::from([
			("bucket".into(), ByteString("my-bucket".into())),
			("region".into(), ByteString("us-east-1".into())),
			("accessKeyId".into(), ByteString("AKIA123".into())),
			("secretAccessKey".into(), ByteString("secret456".into())),
			("repositoryPassword".into(), ByteString("repopass".into())),
		])
	}

	#[test]
	fn validate_kopia_secret_valid() {
		let secret = make_secret(valid_secret_data());
		let creds = validate_kopia_secret(&secret).unwrap();
		assert_eq!(creds.bucket, "my-bucket");
		assert_eq!(creds.region, "us-east-1");
		assert_eq!(creds.access_key_id, "AKIA123");
		assert_eq!(creds.secret_access_key, "secret456");
		assert_eq!(creds.repository_password, "repopass");
	}

	#[test]
	fn validate_kopia_secret_missing_key() {
		let mut data = valid_secret_data();
		data.remove("bucket");
		let secret = make_secret(data);
		let err = validate_kopia_secret(&secret).unwrap_err();
		assert!(err.to_string().contains("missing key: bucket"));
	}

	#[test]
	fn validate_kopia_secret_no_data() {
		let secret = Secret {
			metadata: kube::api::ObjectMeta {
				name: Some("empty".into()),
				..Default::default()
			},
			..Default::default()
		};
		let err = validate_kopia_secret(&secret).unwrap_err();
		assert!(err.to_string().contains("no data"));
	}

	#[test]
	fn validate_kopia_secret_invalid_utf8() {
		let mut data = valid_secret_data();
		data.insert("bucket".into(), ByteString(vec![0xFF, 0xFE]));
		let secret = make_secret(data);
		let err = validate_kopia_secret(&secret).unwrap_err();
		assert!(err.to_string().contains("not valid UTF-8"));
	}

	fn make_snapshot(
		id: &str,
		hostname: &str,
		start_time: &str,
		tags: HashMap<String, String>,
	) -> Snapshot {
		Snapshot {
			id: id.into(),
			hostname: hostname.into(),
			start_time: start_time.into(),
			tags,
			..Default::default()
		}
	}

	#[test]
	fn filter_snapshots_no_filter_returns_all() {
		let snaps = vec![
			make_snapshot("a", "host1", "2024-01-01", HashMap::new()),
			make_snapshot("b", "host2", "2024-01-02", HashMap::new()),
		];
		let result = filter_snapshots(&snaps, None);
		assert_eq!(result.len(), 2);
	}

	#[test]
	fn filter_snapshots_by_tags() {
		let snaps = vec![
			make_snapshot(
				"a",
				"h",
				"t1",
				HashMap::from([("env".into(), "prod".into())]),
			),
			make_snapshot(
				"b",
				"h",
				"t2",
				HashMap::from([("env".into(), "dev".into())]),
			),
			make_snapshot("c", "h", "t3", HashMap::new()),
		];
		let filter = SnapshotFilter {
			tags: Some(HashMap::from([("env".into(), "prod".into())])),
			host_pattern: None,
		};
		let result = filter_snapshots(&snaps, Some(&filter));
		assert_eq!(result.len(), 1);
		assert_eq!(result[0].id, "a");
	}

	#[test]
	fn filter_snapshots_by_host_pattern() {
		let snaps = vec![
			make_snapshot("a", "fiji-prod-01", "t1", HashMap::new()),
			make_snapshot("b", "fiji-dev-01", "t2", HashMap::new()),
			make_snapshot("c", "fiji-prod-02", "t3", HashMap::new()),
		];
		let filter = SnapshotFilter {
			tags: None,
			host_pattern: Some("fiji-prod-*".into()),
		};
		let result = filter_snapshots(&snaps, Some(&filter));
		assert_eq!(result.len(), 2);
		assert!(result.iter().all(|s| s.hostname.starts_with("fiji-prod-")));
	}

	#[test]
	fn filter_snapshots_combined_tag_and_host() {
		let snaps = vec![
			make_snapshot(
				"a",
				"fiji-prod-01",
				"t1",
				HashMap::from([("env".into(), "prod".into())]),
			),
			make_snapshot(
				"b",
				"fiji-prod-02",
				"t2",
				HashMap::from([("env".into(), "dev".into())]),
			),
			make_snapshot(
				"c",
				"fiji-dev-01",
				"t3",
				HashMap::from([("env".into(), "prod".into())]),
			),
		];
		let filter = SnapshotFilter {
			tags: Some(HashMap::from([("env".into(), "prod".into())])),
			host_pattern: Some("fiji-prod-*".into()),
		};
		let result = filter_snapshots(&snaps, Some(&filter));
		assert_eq!(result.len(), 1);
		assert_eq!(result[0].id, "a");
	}

	#[test]
	fn latest_snapshot_picks_newest() {
		let snaps = vec![
			make_snapshot("old", "h", "2024-01-01T00:00:00Z", HashMap::new()),
			make_snapshot("new", "h", "2024-06-15T12:00:00Z", HashMap::new()),
			make_snapshot("mid", "h", "2024-03-10T06:00:00Z", HashMap::new()),
		];
		let latest = latest_snapshot(&snaps).unwrap();
		assert_eq!(latest.id, "new");
	}

	#[test]
	fn latest_snapshot_empty_returns_none() {
		assert!(latest_snapshot(&[]).is_none());
	}

	#[test]
	fn snapshot_total_size_bytes_with_summary() {
		let snap = Snapshot {
			id: "s1".into(),
			summary: Some(SnapshotSummary { size: 1024 }),
			..Default::default()
		};
		assert_eq!(snap.total_size_bytes(), 1024);
	}

	#[test]
	fn snapshot_total_size_bytes_without_summary() {
		let snap = Snapshot {
			id: "s1".into(),
			summary: None,
			..Default::default()
		};
		assert_eq!(snap.total_size_bytes(), 0);
	}

	#[test]
	fn kopia_connect_args_format() {
		let creds = KopiaCredentials {
			bucket: "b".into(),
			region: "r".into(),
			access_key_id: "ak".into(),
			secret_access_key: "sk".into(),
			repository_password: "rp".into(),
		};
		let args = kopia_connect_args(&creds);
		assert_eq!(args[0], "repository");
		assert_eq!(args[1], "connect");
		assert_eq!(args[2], "s3");
		assert!(args.contains(&"--bucket=b".to_string()));
		assert!(args.contains(&"--region=r".to_string()));
		assert!(args.contains(&"--access-key=ak".to_string()));
		assert!(args.contains(&"--secret-access-key=sk".to_string()));
		assert!(args.contains(&"--password=rp".to_string()));
	}
}
