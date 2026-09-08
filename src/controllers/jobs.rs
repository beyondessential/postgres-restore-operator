use std::{collections::HashMap, sync::Mutex};

use k8s_openapi::api::{
	batch::v1::Job,
	core::v1::{EnvVar, EnvVarSource, SecretKeySelector},
};

/// Status of a Kubernetes Job.
#[derive(Debug)]
pub enum JobStatus {
	Active,
	Succeeded,
	Failed,
}

/// Classify a Job's current status from its Kubernetes status fields.
pub fn classify_job(job: &Job) -> JobStatus {
	let status = match &job.status {
		Some(s) => s,
		None => return JobStatus::Active,
	};
	if status.succeeded.unwrap_or(0) > 0 {
		return JobStatus::Succeeded;
	}
	let failed = status.failed.unwrap_or(0);
	let backoff_limit = job.spec.as_ref().and_then(|s| s.backoff_limit).unwrap_or(0);
	if failed > backoff_limit {
		return JobStatus::Failed;
	}
	if let Some(conditions) = &status.conditions {
		for cond in conditions {
			if cond.type_ == "Failed" && cond.status == "True" {
				return JobStatus::Failed;
			}
		}
	}
	JobStatus::Active
}

/// In-memory store for job callback results, keyed by `{namespace}/{name}`.
#[derive(Default)]
pub struct CallbackStore {
	inner: Mutex<HashMap<String, String>>,
}

impl CallbackStore {
	pub fn store(&self, namespace: &str, name: &str, data: String) {
		let key = format!("{namespace}/{name}");
		self.inner.lock().unwrap().insert(key, data);
	}

	pub fn take(&self, namespace: &str, name: &str) -> Option<String> {
		let key = format!("{namespace}/{name}");
		self.inner.lock().unwrap().remove(&key)
	}
}

/// Build an `EnvVar` that references a key in a named Kubernetes Secret.
pub fn env_from_secret_name(env_name: &str, secret_name: &str, key: &str) -> EnvVar {
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

/// Build an `EnvVar` with a literal string value.
pub fn env_literal(name: &str, value: &str) -> EnvVar {
	EnvVar {
		name: name.to_string(),
		value: Some(value.to_string()),
		..Default::default()
	}
}

#[cfg(test)]
mod tests {
	use k8s_openapi::api::batch::v1::{JobSpec, JobStatus as K8sJobStatus};

	use super::*;

	#[test]
	fn classify_active_when_no_status() {
		let job = Job::default();
		assert!(matches!(classify_job(&job), JobStatus::Active));
	}

	#[test]
	fn classify_succeeded() {
		let job = Job {
			status: Some(K8sJobStatus {
				succeeded: Some(1),
				..Default::default()
			}),
			..Default::default()
		};
		assert!(matches!(classify_job(&job), JobStatus::Succeeded));
	}

	#[test]
	fn classify_failed_by_count() {
		let job = Job {
			spec: Some(JobSpec {
				backoff_limit: Some(3),
				..Default::default()
			}),
			status: Some(K8sJobStatus {
				failed: Some(4),
				..Default::default()
			}),
			..Default::default()
		};
		assert!(matches!(classify_job(&job), JobStatus::Failed));
	}

	#[test]
	fn classify_failed_by_condition() {
		let job = Job {
			status: Some(K8sJobStatus {
				conditions: Some(vec![k8s_openapi::api::batch::v1::JobCondition {
					type_: "Failed".into(),
					status: "True".into(),
					..Default::default()
				}]),
				..Default::default()
			}),
			..Default::default()
		};
		assert!(matches!(classify_job(&job), JobStatus::Failed));
	}

	#[test]
	fn classify_still_active_with_some_failures() {
		let job = Job {
			spec: Some(JobSpec {
				backoff_limit: Some(3),
				..Default::default()
			}),
			status: Some(K8sJobStatus {
				failed: Some(1),
				..Default::default()
			}),
			..Default::default()
		};
		assert!(matches!(classify_job(&job), JobStatus::Active));
	}

	#[test]
	fn callback_store_store_and_take() {
		let store = CallbackStore::default();
		store.store("ns", "replica", "payload".into());
		assert_eq!(store.take("ns", "replica"), Some("payload".into()));
		assert_eq!(store.take("ns", "replica"), None);
	}

	#[test]
	fn callback_store_take_missing() {
		let store = CallbackStore::default();
		assert_eq!(store.take("ns", "nope"), None);
	}

	#[test]
	fn callback_store_overwrite() {
		let store = CallbackStore::default();
		store.store("ns", "r", "first".into());
		store.store("ns", "r", "second".into());
		assert_eq!(store.take("ns", "r"), Some("second".into()));
	}

	#[test]
	fn env_from_secret_name_structure() {
		let env = env_from_secret_name("PG_PASSWORD", "my-secret", "password");
		assert_eq!(env.name, "PG_PASSWORD");
		assert!(env.value.is_none());
		let skr = env.value_from.unwrap().secret_key_ref.unwrap();
		assert_eq!(skr.name, "my-secret");
		assert_eq!(skr.key, "password");
		assert_eq!(skr.optional, Some(false));
	}

	#[test]
	fn env_literal_structure() {
		let env = env_literal("HOST", "localhost");
		assert_eq!(env.name, "HOST");
		assert_eq!(env.value.as_deref(), Some("localhost"));
		assert!(env.value_from.is_none());
	}
}
