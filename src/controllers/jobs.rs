use std::{
	collections::HashMap,
	sync::Mutex,
	time::{Duration, Instant},
};

use k8s_openapi::api::{
	batch::v1::Job,
	core::v1::{EnvVar, EnvVarSource, Pod, SecretKeySelector},
};
use kube::{
	Api, Client, ResourceExt,
	api::{ListParams, LogParams},
};
use tracing::warn;

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
	inner: Mutex<HashMap<String, Entry>>,
}

struct Entry {
	stored_at: Instant,
	data: String,
}

impl CallbackStore {
	pub fn store(&self, namespace: &str, name: &str, data: String) {
		let key = format!("{namespace}/{name}");
		self.inner.lock().unwrap().insert(
			key,
			Entry {
				stored_at: Instant::now(),
				data,
			},
		);
	}

	pub fn take(&self, namespace: &str, name: &str) -> Option<String> {
		let key = format!("{namespace}/{name}");
		self.inner.lock().unwrap().remove(&key).map(|e| e.data)
	}

	/// Drop what nothing came back for, returning how many entries went.
	///
	/// A caller that posts a result the operator then has no reason to read
	/// again leaves it here for the process's lifetime, which for the tens of
	/// megabytes a reporting schema runs to is worth reclaiming.
	pub fn sweep(&self, max_age: Duration) -> usize {
		let mut inner = self.inner.lock().unwrap();
		let before = inner.len();
		inner.retain(|_, entry| entry.stored_at.elapsed() < max_age);
		before - inner.len()
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

/// The tail of a Job container's log, as its pod still holds it.
///
/// Best effort: a log that cannot be read leaves the caller without a cause and
/// the reconcile carries on.
pub async fn container_log(
	client: &Client,
	namespace: &str,
	job_name: &str,
	container: &str,
) -> Option<String> {
	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let list = pods
		.list(
			&ListParams::default()
				.labels(&format!("job-name={job_name}"))
				.limit(1),
		)
		.await
		.map_err(|e| warn!(job = job_name, error = %e, "listing job pods failed"))
		.ok()?;
	let pod = list.items.first()?;

	pods.logs(
		&pod.name_any(),
		&LogParams {
			container: Some(container.to_string()),
			tail_lines: Some(200),
			..Default::default()
		},
	)
	.await
	.map_err(|e| warn!(job = job_name, error = %e, "reading job log failed"))
	.ok()
}

/// The last part of a Job's log, colour codes stripped and every given secret
/// masked, raw and in the forms a URL carries it, before any of it is recorded.
pub fn redact(log: &str, secrets: &[String]) -> String {
	let mut start = log.len().saturating_sub(REDACT_WINDOW);
	while !log.is_char_boundary(start) {
		start += 1;
	}
	let mut log = String::from_utf8_lossy(&strip_ansi_escapes::strip(&log[start..])).into_owned();
	for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
		log = log.replace(secret.as_str(), REDACTED);
		for set in [percent_encoding::NON_ALPHANUMERIC, URL_COMPONENT] {
			let encoded = percent_encoding::utf8_percent_encode(secret, set).to_string();
			log = log.replace(&encoded, REDACTED);
		}
	}
	log
}

const REDACTED: &str = "[redacted]";

/// Comfortably more than [`log_tail`] keeps, so a secret straddling its cut is
/// still masked.
const REDACT_WINDOW: usize = 64 * 1024;

/// What URL encoders leave alone: RFC 3986's unreserved characters.
const URL_COMPONENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
	.remove(b'-')
	.remove(b'.')
	.remove(b'_')
	.remove(b'~');

/// The tail of a failed Job's log, bounded so a status field stays readable.
///
/// A thrown error prints its message first and the failing SQL after it, so a
/// long statement pushes the message out of the tail. The first line naming an
/// error is kept ahead of the tail when the tail doesn't already hold it.
pub fn log_tail(log: &str) -> String {
	const KEEP_LINES: usize = 40;
	const MAX_BYTES: usize = 2000;
	const MAX_HEADLINE_BYTES: usize = 500;

	let lines: Vec<&str> = log.lines().collect();
	let tail_start = lines.len().saturating_sub(KEEP_LINES);
	let tail = lines[tail_start..].join("\n");

	let headline = lines
		.iter()
		.position(|line| is_error_headline(line))
		.filter(|&at| at < tail_start || tail.len() > MAX_BYTES)
		.map(|at| truncate_to(lines[at].trim(), MAX_HEADLINE_BYTES));

	let budget = match &headline {
		Some(headline) => MAX_BYTES.saturating_sub(headline.len() + "\n…\n".len()),
		None => MAX_BYTES,
	};
	let kept = if tail.len() <= budget {
		tail.as_str()
	} else {
		let cut = (tail.len() - budget..tail.len())
			.find(|at| tail.is_char_boundary(*at))
			.unwrap_or(tail.len());
		&tail[cut..]
	};

	match headline {
		Some(headline) if !kept.contains(headline.as_str()) => format!("{headline}\n…\n{kept}"),
		_ => kept.to_string(),
	}
}

fn is_error_headline(line: &str) -> bool {
	let line = line.trim_start();
	[
		"Error:",
		"ERROR:",
		"error:",
		"Error [",
		"DatabaseError",
		"SequelizeDatabaseError",
		"Database Error in",
		"Compilation Error in",
		"Runtime Error in",
		"Failure in model",
	]
	.iter()
	.any(|marker| line.starts_with(marker) || line.contains(&format!(" {marker}")))
}

fn truncate_to(text: &str, max_bytes: usize) -> String {
	if text.len() <= max_bytes {
		return text.to_string();
	}
	let end = (0..=max_bytes)
		.rev()
		.find(|at| text.is_char_boundary(*at))
		.unwrap_or(0);
	text[..end].to_string()
}

#[cfg(test)]
mod tests {
	use k8s_openapi::api::batch::v1::{JobSpec, JobStatus as K8sJobStatus};

	use super::*;

	/// A payload nobody came back for is what the sweep is for, and one that
	/// has just arrived is one a reconcile is still on its way to take.
	#[test]
	fn a_payload_nothing_took_is_swept() {
		let store = CallbackStore::default();
		store.store("ns", "replica", "schema".to_string());

		assert_eq!(store.sweep(Duration::from_secs(3600)), 0);
		assert_eq!(store.sweep(Duration::ZERO), 1);
		assert_eq!(store.take("ns", "replica"), None);
	}

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

#[cfg(test)]
mod log_tail_tests {
	use super::log_tail;

	#[test]
	fn the_log_is_kept_whole() {
		let log = concat!(
			"running 1721000002-addBaz.ts\n",
			"error: insert or update on table \"encounters\" violates foreign key constraint \"fk_patient\"\n",
			"DETAIL:  Key (patient_id)=(abc-123) is not present in table \"patients\".\n",
			"HINT:  add the patient first\n",
		);

		let out = log_tail(log);

		assert!(out.contains("violates foreign key constraint"), "{out}");
		assert!(out.contains("Key (patient_id)=(abc-123)"), "{out}");
		assert!(out.contains("HINT:  add the patient first"), "{out}");
	}

	#[test]
	fn only_the_last_lines_are_kept() {
		let log = (0..100)
			.map(|i| format!("applying migration {i}"))
			.collect::<Vec<_>>()
			.join("\n");

		let out = log_tail(&log);

		assert!(out.starts_with("applying migration 60"), "{out}");
		assert!(out.ends_with("applying migration 99"), "{out}");
		assert!(!out.contains("applying migration 59"), "{out}");
	}

	#[test]
	fn the_tail_is_capped() {
		let log = (0..60)
			.map(|i| format!("applying migration {i} {}", "x".repeat(200)))
			.collect::<Vec<_>>()
			.join("\n");

		let out = log_tail(&log);

		assert!(out.len() <= 2000, "{} bytes", out.len());
		assert!(out.ends_with("xxx"), "the end of the log is what is kept");
	}

	#[test]
	fn the_error_survives_a_long_statement_after_it() {
		let mut log = String::from(
			"info: Applying migration: 1781222400001-makeIdDeterministic\n\
			 error: DatabaseError [SequelizeDatabaseError]: index \"idx_example\" does not exist\n\
			 \x20   at Query.run (query.js:50:25)\n",
		);
		for i in 0..60 {
			log.push_str(&format!(
				"  sql: 'INSERT INTO logs.changes line {i} {}',\n",
				"x".repeat(80)
			));
		}

		let out = log_tail(&log);

		assert!(out.len() <= 2000, "{} bytes", out.len());
		assert!(
			out.starts_with(
				"error: DatabaseError [SequelizeDatabaseError]: index \"idx_example\" does not exist"
			),
			"{out}"
		);
		assert!(
			out.contains("line 59"),
			"the end of the log is still kept: {out}"
		);
	}

	#[test]
	fn a_dbt_model_failure_survives_truncation() {
		let mut log = String::from(
			"Database Error in model ds__encounters (models/datasets/ds__encounters.sql)\n",
		);
		for i in 0..200 {
			log.push_str(&format!("dbt line {i} {}\n", "x".repeat(80)));
		}

		let out = log_tail(&log);

		assert!(
			out.starts_with("Database Error in model ds__encounters"),
			"{out}"
		);
	}

	#[test]
	fn a_headline_already_in_the_tail_is_not_repeated() {
		let log = "running 1-foo\nError: boom\nat x\n";

		assert_eq!(log_tail(log), log.trim_end());
	}

	#[test]
	fn an_empty_log_is_nothing() {
		assert_eq!(log_tail(""), "");
	}
}
