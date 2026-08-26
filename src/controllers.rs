use std::{any::Any, future::Future, panic::AssertUnwindSafe};

use futures::FutureExt;
use k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, Pod, SecretKeySelector, SecretReference};
use kube::{Api, Client};

use crate::error::{Error, Result};

pub mod canopy;
pub mod jobs;
pub mod postgres;
pub mod replica;
pub mod restore;

/// Label carried by every restore's postgres pod, and required by the
/// per-replica Service selector alongside the restore name.
///
/// Set in the Deployment's pod template so a pod the ReplicaSet replaces —
/// eviction, node loss, OOM — rejoins the Service as soon as it is Ready,
/// rather than waiting for the operator to notice and patch it.
///
/// It is *not* what keeps a switching restore unreachable: the stable Service
/// is created with no selector at all and only gets one at switchover, after
/// operator-side prep has finished, and that selector names a specific
/// restore. The restore-name component is the gate. This label is the second
/// term in the same selector, kept because live Services already carry it in
/// their selector and a merge patch cannot drop a selector key.
pub const READY_FOR_TRAFFIC_LABEL: &str = "pgro.bes.au/ready-for-traffic";

/// Run a reconciler, turning a panic into an error the controller can requeue.
///
/// kube-rs unwraps the reconciler's JoinHandle, so a panic that escapes into it
/// aborts the operator process rather than just this object's reconcile.
pub async fn catching_panics<T>(fut: impl Future<Output = Result<T>>) -> Result<T> {
	match AssertUnwindSafe(fut).catch_unwind().await {
		Ok(result) => result,
		Err(payload) => Err(Error::ReconcilePanic(panic_message(&payload))),
	}
}

fn panic_message(payload: &Box<dyn Any + Send>) -> String {
	payload
		.downcast_ref::<&str>()
		.map(|s| (*s).to_owned())
		.or_else(|| payload.downcast_ref::<String>().cloned())
		.unwrap_or_else(|| "unknown panic".to_owned())
}

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

	#[tokio::test]
	async fn a_panicking_reconciler_becomes_a_requeueable_error() {
		async fn boom() -> Result<()> {
			panic!("boom");
		}
		let err = catching_panics(boom()).await.unwrap_err();
		assert!(matches!(err, Error::ReconcilePanic(msg) if msg == "boom"));
	}

	#[tokio::test]
	async fn a_healthy_reconciler_passes_its_value_through() {
		async fn fine() -> Result<u8> {
			Ok(7)
		}
		assert_eq!(catching_panics(fine()).await.unwrap(), 7);
	}
}
