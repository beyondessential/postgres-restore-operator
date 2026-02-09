use k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, Pod, SecretKeySelector};
use kube::{Api, Client};

pub mod replica;
pub mod restore;

/// Build an EnvVar that references a key in a Kubernetes Secret.
pub fn env_from_secret(env_name: &str, secret_name: &str, key: &str) -> EnvVar {
	EnvVar {
		name: env_name.to_string(),
		value_from: Some(EnvVarSource {
			secret_key_ref: Some(SecretKeySelector {
				name: secret_name.to_string(),
				key: key.to_string(),
				optional: Some(false),
			}),
			..Default::default()
		}),
		..Default::default()
	}
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
		let statuses = pod.status.as_ref()?.container_statuses.as_ref()?;
		for cs in statuses {
			if cs.name == container_name {
				let terminated = cs.state.as_ref()?.terminated.as_ref()?;
				let msg = terminated.message.as_ref()?.trim().to_string();
				if !msg.is_empty() {
					return Some(msg);
				}
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
		let env = env_from_secret("PG_PASSWORD", "my-secret", "password");
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
		let env = env_from_secret("DB_HOST", "conn-secret", "host");
		let skr = env.value_from.unwrap().secret_key_ref.unwrap();
		assert_eq!(skr.name, "conn-secret");
		assert_eq!(skr.key, "host");
	}
}
