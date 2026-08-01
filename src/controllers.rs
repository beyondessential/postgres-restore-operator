use k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, Pod, SecretKeySelector, SecretReference};
use kube::{Api, Client};

pub mod canopy;
pub mod jobs;
pub mod postgres;
pub mod replica;
pub mod restore;

/// Build an EnvVar that references a key in a Kubernetes Secret.
pub fn env_from_secret(env_name: &str, secret_ref: &SecretReference, key: &str) -> EnvVar {
	EnvVar {
		name: env_name.to_string(),
		value_from: Some(EnvVarSource {
			secret_key_ref: Some(SecretKeySelector {
				name: secret_ref.name.as_deref().unwrap_or_default().into(),
				key: key.to_string(),
				optional: Some(false),
			}),
			..Default::default()
		}),
		..Default::default()
	}
}

/// Build an EnvVar that references an optional key in a Kubernetes Secret.
///
/// If the key does not exist in the Secret, the env var is simply not set
/// (the pod will not fail to start).
pub fn env_from_secret_optional(env_name: &str, secret_ref: &SecretReference, key: &str) -> EnvVar {
	EnvVar {
		name: env_name.to_string(),
		value_from: Some(EnvVarSource {
			secret_key_ref: Some(SecretKeySelector {
				name: secret_ref.name.as_deref().unwrap_or_default().into(),
				key: key.to_string(),
				optional: Some(true),
			}),
			..Default::default()
		}),
		..Default::default()
	}
}

/// Env vars that redirect kopia's config, cache, and log directories to
/// `/tmp/kopia` so the container doesn't need write access to `/app`.
pub fn kopia_writable_env() -> Vec<EnvVar> {
	vec![
		EnvVar {
			name: "KOPIA_CONFIG_PATH".to_string(),
			value: Some("/tmp/kopia/config/repository.config".to_string()),
			..Default::default()
		},
		EnvVar {
			name: "KOPIA_LOG_DIR".to_string(),
			value: Some("/tmp/kopia/logs".to_string()),
			..Default::default()
		},
		EnvVar {
			name: "KOPIA_CACHE_DIRECTORY".to_string(),
			value: Some("/tmp/kopia/cache".to_string()),
			..Default::default()
		},
		EnvVar {
			name: "USER".to_string(),
			value: Some("kopia".to_string()),
			..Default::default()
		},
	]
}

/// Read the termination message from a named container in a Job's pod.
///
/// Looks up pods by the `job-name` label, then finds the specified container's
/// terminated state and returns its message.
pub async fn read_job_termination_message(
	client: &Client,
	namespace: &str,
	job_name: &str,
	container_name: &str,
) -> Option<String> {
	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let pod_list = pods
		.list(&kube::api::ListParams::default().labels(&format!("job-name={job_name}")))
		.await
		.ok()?;

	for pod in &pod_list.items {
		let Some(statuses) = pod
			.status
			.as_ref()
			.and_then(|s| s.container_statuses.as_ref())
		else {
			continue;
		};
		for cs in statuses {
			if cs.name != container_name {
				continue;
			}
			let msg = cs
				.state
				.as_ref()
				.and_then(|s| s.terminated.as_ref())
				.and_then(|t| t.message.as_ref())
				.map(|m| m.trim().to_string());
			if let Some(ref m) = msg
				&& !m.is_empty()
			{
				return msg;
			}
		}
	}

	None
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn env_from_secret_structure() {
		let env = env_from_secret(
			"PG_PASSWORD",
			&SecretReference {
				name: Some("my-secret".into()),
				namespace: None,
			},
			"password",
		);
		assert_eq!(env.name, "PG_PASSWORD");
		assert!(env.value.is_none());
		let vf = env.value_from.unwrap();
		let skr = vf.secret_key_ref.unwrap();
		assert_eq!(skr.name, "my-secret");
		assert_eq!(skr.key, "password");
		assert_eq!(skr.optional, Some(false));
	}

	#[test]
	fn env_from_secret_different_keys() {
		let env = env_from_secret(
			"DB_HOST",
			&SecretReference {
				name: Some("conn-secret".into()),
				namespace: None,
			},
			"host",
		);
		let skr = env.value_from.unwrap().secret_key_ref.unwrap();
		assert_eq!(skr.name, "conn-secret");
		assert_eq!(skr.key, "host");
	}

	#[test]
	fn env_from_secret_optional_is_optional() {
		let env = env_from_secret_optional(
			"ENDPOINT",
			&SecretReference {
				name: Some("my-secret".into()),
				namespace: None,
			},
			"endpoint",
		);
		assert_eq!(env.name, "ENDPOINT");
		let skr = env.value_from.unwrap().secret_key_ref.unwrap();
		assert_eq!(skr.name, "my-secret");
		assert_eq!(skr.key, "endpoint");
		assert_eq!(skr.optional, Some(true));
	}
}
