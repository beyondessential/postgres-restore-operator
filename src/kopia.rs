use k8s_openapi::api::core::v1::Secret;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::types::SnapshotFilter;
use crate::util::glob_to_regex;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    let secret_name = secret
        .metadata
        .name
        .as_deref()
        .unwrap_or("<unnamed>");

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
                let regex_str = glob_to_regex(pattern);
                if let Ok(re) = regex::Regex::new(&regex_str) {
                    if !re.is_match(&snap.hostname) {
                        return false;
                    }
                }
            }

            true
        })
        .cloned()
        .collect()
}

/// Returns the latest snapshot from a list, sorted by start_time descending.
pub fn latest_snapshot(snapshots: &[Snapshot]) -> Option<&Snapshot> {
    snapshots.iter().max_by(|a, b| a.start_time.cmp(&b.start_time))
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
