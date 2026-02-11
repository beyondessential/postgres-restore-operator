use std::collections::BTreeMap;
use std::time::Duration;

use jiff::Span;
use k8s_openapi::ByteString;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
	LocalObjectReference, PersistentVolumeClaim, Secret, SecretReference, Service,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{ListParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::{Api, Client, ResourceExt};
use postgres_restore_operator::util::TimeSpan;
use tokio::time::{sleep, timeout};

use postgres_restore_operator::types::{
	OverlayDatabaseConfig, PostgresPhysicalReplica, PostgresPhysicalReplicaSpec,
	PostgresPhysicalRestore, PostgresPhysicalRestoreSpec, ReplicaPhase, RestorePhase,
};

// ─── Constants ───────────────────────────────────────────────────────────────

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const PHASE_TIMEOUT: Duration = Duration::from_secs(300);
const LONG_PHASE_TIMEOUT: Duration = Duration::from_secs(480);

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn make_client() -> Client {
	Client::try_default()
		.await
		.expect("expected a valid kubeconfig (e.g. from kind)")
}

async fn setup_namespace(client: &Client, ns: &str) {
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

async fn cleanup_namespace(client: &Client, ns: &str, replica_names: &[&str]) {
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

fn build_kopia_secret(ns: &str, name: &str, bucket: &str) -> Secret {
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

struct ReplicaOpts {
	schedule: Option<String>,
	minimum_ttl: Option<TimeSpan>,
	schedule_jitter: Option<TimeSpan>,
	overlay_database: Option<OverlayDatabaseConfig>,
}

impl Default for ReplicaOpts {
	fn default() -> Self {
		Self {
			schedule: None,
			minimum_ttl: None,
			schedule_jitter: None,
			overlay_database: None,
		}
	}
}

fn build_replica(name: &str, secret_ref: &str, opts: ReplicaOpts) -> PostgresPhysicalReplica {
	PostgresPhysicalReplica::new(
		name,
		PostgresPhysicalReplicaSpec {
			kopia_secret_ref: SecretReference {
				name: Some(secret_ref.into()),
				namespace: None,
			},
			snapshot_filter: None,
			schedule: opts.schedule,
			schedule_jitter: opts.schedule_jitter.unwrap_or_default(),
			minimum_ttl: opts.minimum_ttl,
			switchover_grace_period: TimeSpan(Span::new().seconds(10)),
			analytics_username: "analytics".into(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			service_annotations: None,
			pod_annotations: None,
			affinity: None,
			tolerations: vec![],
			read_only: true,
			postgres_extra_config: None,
			notifications: vec![],
			overlay_database: opts.overlay_database,
		},
	)
}

async fn wait_for_replica_phase(
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

async fn wait_for_restore_phase(
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

async fn wait_for_replica_condition(
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

async fn count_restores_for_replica(client: &Client, ns: &str, replica_name: &str) -> usize {
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

// ─── Test 1: Full restore lifecycle (happy path) ─────────────────────────────

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn full_restore_lifecycle() {
	let client = make_client().await;
	let ns = "test-lifecycle";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["lifecycle-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);
	let services: Api<Service> = Api::namespaced(client.clone(), ns);
	let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), ns);
	let deployments: Api<Deployment> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "lifecycle-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica");
	let mut replica = build_replica(
		"lifecycle-replica",
		"lifecycle-kopia-creds",
		Default::default(),
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for replica Restoring phase");
	wait_for_replica_phase(
		&replicas,
		"lifecycle-replica",
		ReplicaPhase::Restoring,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for restore Active phase");
	let restore_name = wait_for_restore_phase(
		&restores,
		"lifecycle-replica",
		RestorePhase::Active,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for replica Ready phase");
	wait_for_replica_phase(
		&replicas,
		"lifecycle-replica",
		ReplicaPhase::Ready,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- verifying created resources");

	let creds_secret_name = "lifecycle-replica-creds";
	let creds = secrets
		.get(creds_secret_name)
		.await
		.expect("credentials secret not found");
	let creds_data = creds.data.expect("credentials secret has no data");
	assert!(
		creds_data.contains_key("password"),
		"credentials secret missing 'password' key"
	);

	let svc = services
		.get("lifecycle-replica")
		.await
		.expect("service not found");
	let svc_spec = svc.spec.expect("service has no spec");
	let ports = svc_spec.ports.expect("service has no ports");
	assert!(
		ports.iter().any(|p| p.port == 5432),
		"service should expose port 5432"
	);

	let pvc_name = format!("{restore_name}-data");
	pvcs.get(&pvc_name)
		.await
		.unwrap_or_else(|_| panic!("PVC {pvc_name} not found"));

	deployments
		.get(&restore_name)
		.await
		.unwrap_or_else(|_| panic!("Deployment {restore_name} not found"));

	let replica = replicas
		.get("lifecycle-replica")
		.await
		.expect("failed to get replica");
	let status = replica.status.expect("replica has no status");

	assert_eq!(status.phase, Some(ReplicaPhase::Ready));
	assert_eq!(
		status.current_restore.as_deref(),
		Some(restore_name.as_str()),
	);
	assert!(status.service_name.is_some());
	assert!(status.last_restore_completed_at.is_some());
	assert!(status.connection_info.is_some());

	let conn = status.connection_info.unwrap();
	assert_eq!(conn.port, 5432);
	assert_eq!(conn.database, "postgres");
	assert_eq!(conn.username, "analytics");
	assert_eq!(conn.password_secret, creds_secret_name);

	let restore = restores
		.get(&restore_name)
		.await
		.expect("failed to get restore");
	let restore_status = restore.status.expect("restore has no status");

	assert_eq!(restore_status.phase, Some(RestorePhase::Active));
	assert!(restore_status.postgres_version.is_some());
	assert!(restore_status.restored_at.is_some());
	assert!(restore_status.activated_at.is_some());
	assert!(restore_status.pvc.is_some());
	assert!(restore_status.deployment.is_some());

	println!("--- all assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &["lifecycle-replica"]).await;
}

// ─── Test 2: Restore fails when snapshot contains non-Postgres data ──────────

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn restore_fails_for_non_postgres_data() {
	let client = make_client().await;
	let ns = "test-non-pg-data";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["non-pg-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret pointing to non-postgres bucket");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "non-pg-kopia-creds", "non-postgres-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica");
	let mut replica = build_replica("non-pg-replica", "non-pg-kopia-creds", Default::default());
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for replica to start restoring");
	wait_for_replica_phase(
		&replicas,
		"non-pg-replica",
		ReplicaPhase::Restoring,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for restore to fail (no PG_VERSION in restored data)");
	let restore_name = wait_for_restore_phase(
		&restores,
		"non-pg-replica",
		RestorePhase::Failed,
		LONG_PHASE_TIMEOUT,
	)
	.await;

	let restore = restores
		.get(&restore_name)
		.await
		.expect("failed to get restore");
	let restore_status = restore.status.expect("restore has no status");
	assert_eq!(restore_status.phase, Some(RestorePhase::Failed));
	assert!(
		restore_status.restore_job.is_some(),
		"restore should have a restoreJob status"
	);
	let job_status = restore_status.restore_job.unwrap();
	assert_eq!(job_status.phase, "Failed");

	// Postgres version should NOT be detected (since data isn't postgres)
	assert!(
		restore_status.postgres_version.is_none(),
		"postgresVersion should not be set for non-postgres data"
	);

	// Replica should still be functional (not crashed)
	let replica = replicas
		.get("non-pg-replica")
		.await
		.expect("failed to get replica after failure");
	assert!(
		replica.status.is_some(),
		"replica should still have status after restore failure"
	);

	println!("--- restore correctly failed for non-postgres data");
	cleanup_namespace(&client, ns, &["non-pg-replica"]).await;
}

// ─── Test 3: Invalid kopia secret structure ──────────────────────────────────

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn invalid_kopia_secret_structure() {
	let client = make_client().await;
	let ns = "test-invalid-secret";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["invalid-secret-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret with missing required keys");
	let invalid_secret = Secret {
		metadata: ObjectMeta {
			name: Some("invalid-kopia-creds".into()),
			namespace: Some(ns.into()),
			..Default::default()
		},
		data: Some(BTreeMap::from([
			("bucket".into(), ByteString("test-bucket".into())),
			// Missing: region, accessKeyId, secretAccessKey, repositoryPassword
		])),
		..Default::default()
	};
	secrets
		.create(&PostParams::default(), &invalid_secret)
		.await
		.expect("failed to create invalid secret");

	println!("--- creating PostgresPhysicalReplica");
	let mut replica = build_replica(
		"invalid-secret-replica",
		"invalid-kopia-creds",
		Default::default(),
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for KopiaSecretValid=False condition");
	wait_for_replica_condition(
		&client,
		ns,
		"invalid-secret-replica",
		"KopiaSecretValid",
		"False",
		Duration::from_secs(60),
	)
	.await;

	// Verify the condition has the right reason
	let replica = replicas
		.get("invalid-secret-replica")
		.await
		.expect("failed to get replica");
	let status = replica.status.expect("replica has no status");
	let cond = status
		.conditions
		.iter()
		.find(|c| c.type_ == "KopiaSecretValid")
		.expect("KopiaSecretValid condition not found");
	assert_eq!(cond.status, "False");
	assert_eq!(cond.reason, "SecretInvalid");
	assert!(
		cond.message.contains("missing key"),
		"condition message should mention missing key, got: {}",
		cond.message
	);

	// No restores should be created
	let restore_count = count_restores_for_replica(&client, ns, "invalid-secret-replica").await;
	assert_eq!(
		restore_count, 0,
		"no restores should be created with invalid secret"
	);

	// No snapshot-list jobs should be created
	let jobs: Api<Job> = Api::namespaced(client.clone(), ns);
	let job_list = jobs
		.list(&ListParams::default().labels("pgro.bes.au/replica=invalid-secret-replica"))
		.await
		.expect("failed to list jobs");
	assert!(
		job_list.items.is_empty(),
		"no jobs should be created with invalid secret"
	);

	println!("--- invalid secret correctly detected");
	cleanup_namespace(&client, ns, &["invalid-secret-replica"]).await;
}

// ─── Test 4: Missing kopia secret ────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn missing_kopia_secret_handled() {
	let client = make_client().await;
	let ns = "test-missing-secret";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["missing-secret-replica"]).await;

	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);

	println!("--- creating PostgresPhysicalReplica referencing non-existent secret");
	let mut replica = build_replica(
		"missing-secret-replica",
		"this-secret-does-not-exist",
		Default::default(),
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for KopiaSecretValid=False condition");
	wait_for_replica_condition(
		&client,
		ns,
		"missing-secret-replica",
		"KopiaSecretValid",
		"False",
		Duration::from_secs(60),
	)
	.await;

	let replica = replicas
		.get("missing-secret-replica")
		.await
		.expect("failed to get replica");
	let status = replica.status.expect("replica has no status");
	let cond = status
		.conditions
		.iter()
		.find(|c| c.type_ == "KopiaSecretValid")
		.expect("KopiaSecretValid condition not found");
	assert_eq!(cond.status, "False");
	assert_eq!(cond.reason, "SecretNotFound");

	let restore_count = count_restores_for_replica(&client, ns, "missing-secret-replica").await;
	assert_eq!(
		restore_count, 0,
		"no restores should be created when secret is missing"
	);

	println!("--- missing secret correctly detected");
	cleanup_namespace(&client, ns, &["missing-secret-replica"]).await;
}

// ─── Test 5: Snapshot job fails when bucket doesn't exist ────────────────────

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn snapshot_job_fails_for_wrong_bucket() {
	let client = make_client().await;
	let ns = "test-wrong-bucket";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["wrong-bucket-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret pointing to non-existent bucket");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "wrong-bucket-creds", "bucket-that-does-not-exist"),
		)
		.await
		.expect("failed to create secret");

	println!("--- creating PostgresPhysicalReplica");
	let mut replica = build_replica(
		"wrong-bucket-replica",
		"wrong-bucket-creds",
		Default::default(),
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	// The kopia secret is structurally valid, so the condition should be True
	println!("--- waiting for KopiaSecretValid=True condition");
	wait_for_replica_condition(
		&client,
		ns,
		"wrong-bucket-replica",
		"KopiaSecretValid",
		"True",
		Duration::from_secs(60),
	)
	.await;

	// Wait for the snapshot-list job to be created by the operator
	println!("--- waiting for snapshot-list job to be created");
	let jobs: Api<Job> = Api::namespaced(client.clone(), ns);
	timeout(Duration::from_secs(60), async {
		loop {
			if let Ok(list) = jobs
				.list(&ListParams::default().labels(
					"pgro.bes.au/replica=wrong-bucket-replica,pgro.bes.au/job-type=snapshot-list",
				))
				.await && !list.items.is_empty()
			{
				println!("[wrong-bucket-replica] snapshot-list job exists");
				return;
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for snapshot-list job to be created");

	// The operator deletes failed jobs and immediately recreates them (because
	// never_restored is true), so we can't reliably catch the transient failed
	// state. Instead, wait long enough for at least one full job cycle
	// (backoff_limit=2 → 3 attempts × ~40s each ≈ 2 min) and then verify the
	// observable effects: no restores created and replica still Pending.
	println!("--- waiting 150s for job failure cycle to complete");
	sleep(Duration::from_secs(150)).await;

	// No restores should be created since snapshot discovery keeps failing
	let restore_count = count_restores_for_replica(&client, ns, "wrong-bucket-replica").await;
	assert_eq!(
		restore_count, 0,
		"no restores should be created when snapshot list job fails"
	);

	// Replica should stay in Pending (no active restore)
	let replica = replicas
		.get("wrong-bucket-replica")
		.await
		.expect("failed to get replica");
	let phase = replica.status.as_ref().and_then(|s| s.phase.as_ref());
	assert_eq!(
		phase,
		Some(&ReplicaPhase::Pending),
		"replica should be Pending after snapshot job failure"
	);

	println!("--- snapshot job failure handled correctly");
	cleanup_namespace(&client, ns, &["wrong-bucket-replica"]).await;
}

// ─── Test 6: Minimum TTL prevents premature restores ─────────────────────────

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn minimum_ttl_prevents_premature_restore() {
	let client = make_client().await;
	let ns = "test-min-ttl";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["ttl-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "ttl-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with schedule and long minimum_ttl");
	let mut replica = build_replica(
		"ttl-replica",
		"ttl-kopia-creds",
		ReplicaOpts {
			schedule: Some("* * * * *".into()),                 // every minute
			minimum_ttl: Some(TimeSpan(Span::new().hours(24))), // 24 hours - won't expire during test
			schedule_jitter: Some(TimeSpan::default()),         // no jitter
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for first restore to complete");
	wait_for_replica_phase(
		&replicas,
		"ttl-replica",
		ReplicaPhase::Restoring,
		PHASE_TIMEOUT,
	)
	.await;
	let first_restore_name = wait_for_restore_phase(
		&restores,
		"ttl-replica",
		RestorePhase::Active,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for replica to be Ready");
	wait_for_replica_phase(&replicas, "ttl-replica", ReplicaPhase::Ready, PHASE_TIMEOUT).await;

	// Record the number of restores after first restore completes
	let initial_restore_count = count_restores_for_replica(&client, ns, "ttl-replica").await;
	println!(
		"--- first restore complete: {first_restore_name} (total restores: {initial_restore_count})"
	);

	// Wait long enough for at least 2 cron schedule triggers (>= 90s)
	// If TTL were not blocking, the operator would trigger a new restore
	println!("--- waiting 100s to verify no premature restores are triggered");
	sleep(Duration::from_secs(100)).await;

	// Count restores again - should be the same (TTL blocks new restores)
	let final_restore_count = count_restores_for_replica(&client, ns, "ttl-replica").await;
	println!(
		"--- restore count after waiting: {final_restore_count} (was: {initial_restore_count})"
	);

	// The count may stay the same or decrease (if the operator cleaned up failed ones),
	// but it should NOT increase
	assert!(
		final_restore_count <= initial_restore_count,
		"minimum_ttl should prevent new restores: expected <= {initial_restore_count}, got {final_restore_count}"
	);

	// Verify the replica is still Ready (not trying to restore again)
	let replica = replicas
		.get("ttl-replica")
		.await
		.expect("failed to get replica");
	let status = replica.status.expect("replica has no status");
	assert_eq!(
		status.phase,
		Some(ReplicaPhase::Ready),
		"replica should remain Ready while TTL is active"
	);
	assert_eq!(
		status.current_restore.as_deref(),
		Some(first_restore_name.as_str()),
		"currentRestore should still be the first restore"
	);

	println!("--- minimum TTL correctly prevented premature restore");
	cleanup_namespace(&client, ns, &["ttl-replica"]).await;
}

// ─── Test 7: Second restore triggers switchover ──────────────────────────────

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn second_restore_and_switchover() {
	let client = make_client().await;
	let ns = "test-switchover";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["switchover-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);
	let services: Api<Service> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "switchover-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica (no schedule, manual trigger)");
	let mut replica = build_replica(
		"switchover-replica",
		"switchover-kopia-creds",
		Default::default(),
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for first restore to complete");
	wait_for_replica_phase(
		&replicas,
		"switchover-replica",
		ReplicaPhase::Restoring,
		PHASE_TIMEOUT,
	)
	.await;
	let first_restore_name = wait_for_restore_phase(
		&restores,
		"switchover-replica",
		RestorePhase::Active,
		PHASE_TIMEOUT,
	)
	.await;
	wait_for_replica_phase(
		&replicas,
		"switchover-replica",
		ReplicaPhase::Ready,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- first restore active: {first_restore_name}");

	// Read the first restore's snapshot info to reuse
	let first_restore = restores
		.get(&first_restore_name)
		.await
		.expect("failed to get first restore");
	let snapshot_id = first_restore.spec.snapshot.clone();
	let snapshot_size = first_restore.spec.snapshot_size.clone();
	let storage_size = first_restore.spec.storage_size.clone();

	// Get replica UID for owner reference
	let replica = replicas
		.get("switchover-replica")
		.await
		.expect("failed to get replica");
	let replica_uid = replica.uid().expect("replica has no UID");

	// Verify the service selector points to the first restore
	let svc = services
		.get("switchover-replica")
		.await
		.expect("service not found");
	let svc_selector = svc
		.spec
		.as_ref()
		.and_then(|s| s.selector.as_ref())
		.cloned()
		.unwrap_or_default();
	assert_eq!(
		svc_selector.get("pgro.bes.au/restore").map(|s| s.as_str()),
		Some(first_restore_name.as_str()),
		"service selector should point to first restore"
	);

	// Manually create a second PostgresPhysicalRestore to trigger switchover
	let second_restore_name = "switchover-replica-manual-second";
	println!("--- creating second restore manually: {second_restore_name}");

	let second_restore = PostgresPhysicalRestore::new(
		second_restore_name,
		PostgresPhysicalRestoreSpec {
			replica: LocalObjectReference {
				name: "switchover-replica".into(),
			},
			snapshot: snapshot_id.clone(),
			snapshot_size,
			storage_size,
		},
	);

	let mut restore_value = serde_json::to_value(&second_restore).unwrap();
	if let Some(meta) = restore_value
		.as_object_mut()
		.and_then(|o| o.get_mut("metadata"))
		.and_then(|m| m.as_object_mut())
	{
		meta.insert(
			"namespace".to_string(),
			serde_json::Value::String(ns.to_string()),
		);
		meta.insert(
			"labels".to_string(),
			serde_json::json!({ "pgro.bes.au/replica": "switchover-replica" }),
		);
		meta.insert(
			"ownerReferences".to_string(),
			serde_json::json!([{
				"apiVersion": "pgro.bes.au/v1alpha1",
				"kind": "PostgresPhysicalReplica",
				"name": "switchover-replica",
				"uid": replica_uid,
				"controller": true,
				"blockOwnerDeletion": true,
			}]),
		);
	}

	let second_restore_resource: PostgresPhysicalRestore =
		serde_json::from_value(restore_value).unwrap();
	restores
		.create(&PostParams::default(), &second_restore_resource)
		.await
		.expect("failed to create second restore");

	println!("--- waiting for second restore to become Active");
	timeout(LONG_PHASE_TIMEOUT, async {
		loop {
			if let Ok(restore) = restores.get(second_restore_name).await {
				let phase = restore.status.as_ref().and_then(|s| s.phase.as_ref());
				println!("[{second_restore_name}] phase: {phase:?}");
				if phase == Some(&RestorePhase::Active) {
					return;
				}
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for second restore to become Active");

	println!("--- verifying switchover occurred");

	// Replica status should reflect the switchover
	let replica = replicas
		.get("switchover-replica")
		.await
		.expect("failed to get replica after switchover");
	let status = replica
		.status
		.expect("replica has no status after switchover");

	assert_eq!(
		status.phase,
		Some(ReplicaPhase::Ready),
		"replica should be Ready after switchover"
	);
	assert_eq!(
		status.current_restore.as_deref(),
		Some(second_restore_name),
		"currentRestore should be the second restore"
	);
	assert_eq!(
		status.previous_restore.as_deref(),
		Some(first_restore_name.as_str()),
		"previousRestore should be the first restore"
	);

	// Service selector should now point to the second restore
	let svc = services
		.get("switchover-replica")
		.await
		.expect("service not found after switchover");
	let svc_selector = svc
		.spec
		.as_ref()
		.and_then(|s| s.selector.as_ref())
		.cloned()
		.unwrap_or_default();
	assert_eq!(
		svc_selector.get("pgro.bes.au/restore").map(|s| s.as_str()),
		Some(second_restore_name),
		"service selector should point to second restore after switchover"
	);

	// Second restore should have activated_at set
	let second_restore = restores
		.get(second_restore_name)
		.await
		.expect("failed to get second restore");
	let second_status = second_restore.status.expect("second restore has no status");
	assert!(
		second_status.activated_at.is_some(),
		"activatedAt should be set on second restore"
	);
	assert!(
		second_status.restored_at.is_some(),
		"restoredAt should be set on second restore"
	);

	println!("--- switchover completed successfully");
	cleanup_namespace(&client, ns, &["switchover-replica"]).await;
}

// ─── Test 7: Overlay FDW reconciliation ──────────────────────────────────────

/// Helper: run kubectl exec and return stdout as a String.
async fn kubectl_exec(ns: &str, pod: &str, cmd: &[&str]) -> String {
	let mut args = vec!["exec", "-n", ns, pod, "--"];
	args.extend_from_slice(cmd);
	let output = tokio::process::Command::new("kubectl")
		.args(&args)
		.output()
		.await
		.expect("failed to run kubectl exec");
	let stdout = String::from_utf8_lossy(&output.stdout).to_string();
	let stderr = String::from_utf8_lossy(&output.stderr).to_string();
	if !output.status.success() {
		panic!(
			"kubectl exec failed (exit {})\nstdout: {stdout}\nstderr: {stderr}",
			output.status
		);
	}
	stdout
}

/// Wait for a pod to be ready (all containers running).
async fn wait_for_pod_ready(ns: &str, pod: &str, timeout_dur: Duration) {
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

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO, kopia, and CNPG"]
async fn overlay_fdw_reconciliation() {
	let client = make_client().await;
	let ns = "test-overlay";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["overlay-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "overlay-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with overlay database");
	let mut replica = build_replica(
		"overlay-replica",
		"overlay-kopia-creds",
		ReplicaOpts {
			overlay_database: Some(OverlayDatabaseConfig {
				postgres_version: Some(17),
				image_catalog: None,
				storage_size_override: Some(Quantity("2Gi".into())),
				storage_class: None,
				resources: None,
				affinity: None,
				tolerations: vec![],
				service_annotations: None,
				schema_mapping: None,
			}),
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for replica Restoring phase");
	wait_for_replica_phase(
		&replicas,
		"overlay-replica",
		ReplicaPhase::Restoring,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for restore Active phase");
	let restore_name = wait_for_restore_phase(
		&restores,
		"overlay-replica",
		RestorePhase::Active,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for replica Ready phase");
	wait_for_replica_phase(
		&replicas,
		"overlay-replica",
		ReplicaPhase::Ready,
		LONG_PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for overlay CNPG cluster pod to be ready");
	let overlay_pod = "overlay-replica-overlay-1";
	wait_for_pod_ready(ns, overlay_pod, LONG_PHASE_TIMEOUT).await;

	println!("--- waiting for overlayFdwRestore status to be set");
	timeout(PHASE_TIMEOUT, async {
		loop {
			if let Ok(r) = replicas.get("overlay-replica").await {
				let fdw_restore = r
					.status
					.as_ref()
					.and_then(|s| s.overlay_fdw_restore.as_ref());
				if fdw_restore.is_some() {
					println!(
						"[overlay-replica] overlayFdwRestore = {}",
						fdw_restore.unwrap()
					);
					return;
				}
				println!("[overlay-replica] overlayFdwRestore not set yet");
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for overlayFdwRestore to be set");

	println!("--- verifying replica status fields");
	let replica = replicas
		.get("overlay-replica")
		.await
		.expect("failed to get replica");
	let status = replica.status.as_ref().expect("replica has no status");

	assert_eq!(
		status.overlay_fdw_restore.as_deref(),
		Some(restore_name.as_str()),
		"overlayFdwRestore should match currentRestore"
	);
	assert!(
		status.overlay_cluster_name.is_some(),
		"overlayClusterName should be set"
	);
	assert_eq!(
		status.overlay_cluster_name.as_deref(),
		Some("overlay-replica-overlay"),
	);

	println!("--- verifying FDW server exists in overlay database");
	let fdw_servers = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT srvname FROM pg_foreign_server WHERE srvname LIKE 'fdw_%'",
		],
	)
	.await;
	let fdw_servers: Vec<&str> = fdw_servers.trim().lines().collect();
	assert!(
		!fdw_servers.is_empty(),
		"expected at least one FDW server, got none"
	);
	println!("  FDW servers: {fdw_servers:?}");

	println!("--- verifying FDW server points to the correct database (myapp)");
	let server_dbname = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			&format!(
				"SELECT option_value FROM pg_options_to_table( \
				   (SELECT srvoptions FROM pg_foreign_server WHERE srvname = '{}') \
				 ) WHERE option_name = 'dbname'",
				fdw_servers[0]
			),
		],
	)
	.await;
	assert_eq!(
		server_dbname.trim(),
		"myapp",
		"FDW server should point to 'myapp' database, got '{}'",
		server_dbname.trim()
	);

	println!("--- verifying foreign tables were imported");
	let ft_count = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM information_schema.foreign_tables",
		],
	)
	.await;
	let ft_count: i64 = ft_count
		.trim()
		.parse()
		.expect("failed to parse foreign table count");
	assert!(
		ft_count > 0,
		"expected at least one foreign table, got {ft_count}"
	);
	println!("  foreign tables: {ft_count}");

	println!("--- verifying end-to-end FDW query works (reading through foreign table)");
	let row_count = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM public.test_data",
		],
	)
	.await;
	let row_count: i64 = row_count.trim().parse().expect("failed to parse row count");
	assert_eq!(
		row_count, 1000,
		"expected 1000 rows from foreign table test_data, got {row_count}"
	);

	println!("--- all overlay FDW assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &["overlay-replica"]).await;
}
