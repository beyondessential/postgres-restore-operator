#![allow(
	dead_code,
	reason = "this file acts as its own crate, and so can't figure out dead_code properly"
)]

use std::{collections::BTreeMap, time::Duration};

use jiff::Span;

use k8s_openapi::{
	ByteString,
	api::{
		batch::v1::Job,
		core::v1::{LocalObjectReference, Secret, SecretReference},
	},
	apimachinery::pkg::api::resource::Quantity,
};
use kube::{
	Api, Client, ResourceExt,
	api::{ListParams, ObjectMeta, Patch, PatchParams},
};
use postgres_restore_operator::{
	types::{
		PostgresPhysicalReplica, PostgresPhysicalReplicaSpec, PostgresPhysicalRestore,
		PostgresPhysicalRestoreSpec, RedactionSpec, ReplicaPhase, RestorePhase,
	},
	util::TimeSpan,
};
use tokio::time::{sleep, timeout};

pub const POLL_INTERVAL: Duration = Duration::from_secs(2);
pub const PHASE_TIMEOUT: Duration = Duration::from_secs(300);
pub const LONG_PHASE_TIMEOUT: Duration = Duration::from_secs(480);

pub async fn make_client() -> Client {
	Client::try_default()
		.await
		.expect("expected a valid kubeconfig (e.g. from kind)")
}

pub async fn setup_namespace(client: &Client, ns: &str) {
	let ns_api: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client.clone());
	let ns_obj = k8s_openapi::api::core::v1::Namespace {
		metadata: ObjectMeta {
			name: Some(ns.into()),
			..Default::default()
		},
		..Default::default()
	};
	let _ = ns_api
		.patch(
			ns,
			&PatchParams::apply("integration-test"),
			&Patch::Apply(ns_obj),
		)
		.await;
}

pub async fn cleanup_namespace(client: &Client, ns: &str, replica_names: &[&str]) {
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	for name in replica_names {
		let _ = replicas.delete(name, &Default::default()).await;
	}

	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);
	if let Ok(list) = restores.list(&ListParams::default()).await {
		for restore in &list.items {
			let _ = restores
				.delete(&restore.name_any(), &Default::default())
				.await;
		}
	}

	// Wait for cascading deletes from owner references
	sleep(Duration::from_secs(5)).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	if let Ok(list) = secrets.list(&ListParams::default()).await {
		for secret in &list.items {
			let _ = secrets
				.delete(&secret.name_any(), &Default::default())
				.await;
		}
	}

	let jobs: Api<Job> = Api::namespaced(client.clone(), ns);
	if let Ok(list) = jobs.list(&ListParams::default()).await {
		for job in &list.items {
			let _ = jobs.delete(&job.name_any(), &Default::default()).await;
		}
	}

	sleep(Duration::from_secs(3)).await;
}

pub fn build_kopia_secret(ns: &str, name: &str, bucket: &str) -> Secret {
	Secret {
		metadata: ObjectMeta {
			name: Some(name.into()),
			namespace: Some(ns.into()),
			..Default::default()
		},
		data: Some(BTreeMap::from([
			("bucket".into(), ByteString(bucket.as_bytes().to_vec())),
			("region".into(), ByteString("us-east-1".into())),
			("accessKeyId".into(), ByteString("minioadmin".into())),
			("secretAccessKey".into(), ByteString("minioadmin".into())),
			(
				"repositoryPassword".into(),
				ByteString("test-repo-password".into()),
			),
			("endpoint".into(), ByteString("minio.minio.svc:9000".into())),
			("disableTls".into(), ByteString("true".into())),
		])),
		..Default::default()
	}
}

pub struct ReplicaOpts {
	pub schedule: String,
	pub minimum_ttl: Option<TimeSpan>,
	pub schedule_jitter: Option<TimeSpan>,
	pub read_only: bool,
	pub ephemeral: bool,
	pub redaction: Option<RedactionSpec>,
}

impl Default for ReplicaOpts {
	fn default() -> Self {
		Self {
			schedule: "0 */6 * * *".into(),
			minimum_ttl: None,
			schedule_jitter: None,
			read_only: true,
			ephemeral: false,
			redaction: None,
		}
	}
}

pub fn build_replica(name: &str, secret_ref: &str, opts: ReplicaOpts) -> PostgresPhysicalReplica {
	PostgresPhysicalReplica::new(
		name,
		PostgresPhysicalReplicaSpec {
			kopia_secret_ref: Some(SecretReference {
				name: Some(secret_ref.into()),
				namespace: None,
			}),
			canopy_source: None,
			snapshot_filter: None,
			schedule: opts.schedule,
			schedule_jitter: opts.schedule_jitter.unwrap_or_default(),
			minimum_ttl: opts.minimum_ttl,
			switchover_grace_period: TimeSpan(Span::new().seconds(10)),
			analytics_username: "analytics".into(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			resources_floor: None,
			resources_maximum: None,
			deployment_ready_timeout: None,
			shm_size_floor: None,
			service_annotations: None,
			pod_annotations: None,
			affinity: None,
			tolerations: vec![],
			read_only: opts.read_only,
			ephemeral: opts.ephemeral,
			postgres_extra_config: None,
			notifications: vec![],
			persistent_schemas: None,
			redaction: opts.redaction,
			storage_size_maximum: Quantity("2Ti".to_string()),
		},
	)
}

pub async fn wait_for_replica_phase(
	api: &Api<PostgresPhysicalReplica>,
	name: &str,
	target: ReplicaPhase,
	timeout_dur: Duration,
) {
	let phase_name = format!("{target:?}");
	timeout(timeout_dur, async {
		loop {
			if let Ok(replica) = api.get(name).await {
				let phase = replica.status.as_ref().and_then(|s| s.phase.as_ref());
				if phase == Some(&target) {
					println!("[{name}] reached phase {phase_name}");
					return;
				}
				println!("[{name}] phase: {phase:?}, waiting for {phase_name}");
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.unwrap_or_else(|_| panic!("timed out waiting for replica {name} to reach phase {phase_name}"));
}

pub async fn wait_for_restore_phase(
	api: &Api<PostgresPhysicalRestore>,
	replica_name: &str,
	target: RestorePhase,
	timeout_dur: Duration,
) -> String {
	let phase_name = format!("{target:?}");
	timeout(timeout_dur, async {
		loop {
			let list = api
				.list(&ListParams::default().labels(&format!("pgro.bes.au/replica={replica_name}")))
				.await
				.expect("failed to list restores");

			for restore in &list.items {
				let phase = restore.status.as_ref().and_then(|s| s.phase.as_ref());
				if phase == Some(&target) {
					let name = restore.name_any();
					println!("[{replica_name}] restore {name} reached phase {phase_name}");
					return name;
				}
				println!(
					"[{replica_name}] restore {} phase: {phase:?}, waiting for {phase_name}",
					restore.name_any(),
				);
			}

			if list.items.is_empty() {
				println!("[{replica_name}] no restores found yet, waiting for {phase_name}");
			}

			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.unwrap_or_else(|_| {
		panic!("timed out waiting for a restore of {replica_name} to reach phase {phase_name}")
	})
}

pub async fn wait_for_replica_condition(
	client: &Client,
	ns: &str,
	name: &str,
	condition_type: &str,
	expected_status: &str,
	timeout_dur: Duration,
) {
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	timeout(timeout_dur, async {
		loop {
			if let Ok(replica) = replicas.get(name).await
				&& let Some(status) = &replica.status
			{
				for cond in &status.conditions {
					if cond.type_ == condition_type && cond.status == expected_status {
						println!(
							"[{name}] condition {condition_type}={expected_status} (reason: {})",
							cond.reason
						);
						return;
					}
				}
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.unwrap_or_else(|_| {
		panic!("timed out waiting for replica {name} condition {condition_type}={expected_status}")
	});
}

pub async fn count_restores_for_replica(client: &Client, ns: &str, replica_name: &str) -> usize {
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);
	let list = match restores
		.list(&ListParams::default().labels(&format!("pgro.bes.au/replica={replica_name}")))
		.await
	{
		Ok(l) => l,
		Err(_) => return 0,
	};
	list.items.len()
}

pub async fn try_kubectl_exec(ns: &str, target: &str, cmd: &[&str]) -> (bool, String, String) {
	let mut args = vec!["exec", "-n", ns, target, "--"];
	args.extend_from_slice(cmd);
	let output = tokio::process::Command::new("kubectl")
		.args(&args)
		.output()
		.await
		.expect("failed to run kubectl exec");
	let stdout = String::from_utf8_lossy(&output.stdout).to_string();
	let stderr = String::from_utf8_lossy(&output.stderr).to_string();
	(output.status.success(), stdout, stderr)
}

pub async fn kubectl_exec(ns: &str, target: &str, cmd: &[&str]) -> String {
	let (ok, stdout, stderr) = try_kubectl_exec(ns, target, cmd).await;
	if !ok {
		panic!("kubectl exec failed\nstdout: {stdout}\nstderr: {stderr}",);
	}
	stdout
}

/// Wait for a pod to be ready (all containers running).
pub async fn wait_for_pod_ready(ns: &str, pod: &str, timeout_dur: Duration) {
	timeout(timeout_dur, async {
		loop {
			let result = tokio::process::Command::new("kubectl")
				.args([
					"get",
					"pod",
					"-n",
					ns,
					pod,
					"-o",
					"jsonpath={.status.conditions[?(@.type=='Ready')].status}",
				])
				.output()
				.await
				.expect("failed to run kubectl");
			let stdout = String::from_utf8_lossy(&result.stdout);
			if stdout.trim() == "True" {
				println!("[{pod}] pod is ready");
				return;
			}
			println!("[{pod}] not ready yet, waiting...");
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.unwrap_or_else(|_| panic!("timed out waiting for pod {pod} to be ready in namespace {ns}"));
}

pub fn build_second_restore(
	name: &str,
	ns: &str,
	first_restore: &PostgresPhysicalRestore,
	replica: &PostgresPhysicalReplica,
) -> PostgresPhysicalRestore {
	let mut restore = PostgresPhysicalRestore::new(
		name,
		PostgresPhysicalRestoreSpec {
			replica: LocalObjectReference {
				name: replica.name_any(),
			},
			snapshot: first_restore.spec.snapshot.clone(),
			snapshot_size: first_restore.spec.snapshot_size.clone(),
			snapshot_time: None,
			storage_size: first_restore.spec.storage_size.clone(),
		},
	);
	restore.metadata.namespace = Some(ns.to_string());
	restore.metadata.labels = Some(BTreeMap::from([(
		"pgro.bes.au/replica".to_string(),
		replica.name_any(),
	)]));
	restore.metadata.owner_references = Some(vec![replica.owner_reference()]);
	restore
}
