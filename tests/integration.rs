use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::ByteString;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Secret, Service};
use kube::api::{ObjectMeta, Patch, PatchParams, PostParams};
use kube::{Api, Client, ResourceExt};
use tokio::time::{sleep, timeout};

use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalReplicaSpec, PostgresPhysicalRestore, ReplicaPhase,
	RestorePhase,
};

const TEST_NAMESPACE: &str = "integration-test";
const KOPIA_SECRET_NAME: &str = "test-kopia-creds";
const REPLICA_NAME: &str = "test-replica";

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const PHASE_TIMEOUT: Duration = Duration::from_secs(300);

fn kopia_secret() -> Secret {
	Secret {
		metadata: ObjectMeta {
			name: Some(KOPIA_SECRET_NAME.into()),
			namespace: Some(TEST_NAMESPACE.into()),
			..Default::default()
		},
		data: Some(BTreeMap::from([
			("bucket".into(), ByteString("test-bucket".into())),
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

fn replica_cr() -> PostgresPhysicalReplica {
	PostgresPhysicalReplica::new(
		REPLICA_NAME,
		PostgresPhysicalReplicaSpec {
			kopia_secret_ref: KOPIA_SECRET_NAME.into(),
			snapshot_filter: None,
			schedule: None,
			schedule_jitter: "0s".into(),
			minimum_ttl: None,
			switchover_grace_period: "10s".into(),
			analytics_username: "analytics".into(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			service_annotations: None,
			pod_annotations: None,
			node_selector: None,
			tolerations: vec![],
			read_only: true,
			postgres_extra_config: None,
			notifications: vec![],
		},
	)
}

async fn wait_for_replica_phase(
	api: &Api<PostgresPhysicalReplica>,
	name: &str,
	target: ReplicaPhase,
) {
	let phase_name = format!("{target:?}");
	timeout(PHASE_TIMEOUT, async {
		loop {
			if let Ok(replica) = api.get(name).await {
				let phase = replica.status.as_ref().and_then(|s| s.phase.as_ref());
				if phase == Some(&target) {
					println!("replica {name} reached phase {phase_name}");
					return;
				}
				println!("replica {name} phase: {phase:?}, waiting for {phase_name}",);
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
) -> String {
	let phase_name = format!("{target:?}");
	timeout(PHASE_TIMEOUT, async {
		loop {
			let list = api
				.list(
					&kube::api::ListParams::default()
						.labels(&format!("bes.au/replica={replica_name}")),
				)
				.await
				.expect("failed to list restores");

			for restore in &list.items {
				let phase = restore.status.as_ref().and_then(|s| s.phase.as_ref());
				if phase == Some(&target) {
					let name = restore.name_any();
					println!("restore {name} reached phase {phase_name}");
					return name;
				}
				println!(
					"restore {} phase: {phase:?}, waiting for {phase_name}",
					restore.name_any(),
				);
			}

			if list.items.is_empty() {
				println!(
					"no restores found yet for replica {replica_name}, waiting for {phase_name}"
				);
			}

			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.unwrap_or_else(|_| {
		panic!("timed out waiting for a restore of {replica_name} to reach phase {phase_name}")
	})
}

async fn setup_namespace(client: &Client) {
	let ns_api: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client.clone());
	let ns = k8s_openapi::api::core::v1::Namespace {
		metadata: ObjectMeta {
			name: Some(TEST_NAMESPACE.into()),
			..Default::default()
		},
		..Default::default()
	};
	let _ = ns_api
		.patch(
			TEST_NAMESPACE,
			&PatchParams::apply("integration-test"),
			&Patch::Apply(ns),
		)
		.await;
}

async fn cleanup(client: &Client) {
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), TEST_NAMESPACE);
	let _ = replicas.delete(REPLICA_NAME, &Default::default()).await;

	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), TEST_NAMESPACE);
	let list = match restores.list(&kube::api::ListParams::default()).await {
		Ok(list) => list,
		Err(_) => return,
	};
	for restore in &list.items {
		let _ = restores
			.delete(&restore.name_any(), &Default::default())
			.await;
	}

	// Wait for restores to be deleted so their owned resources are cleaned up
	sleep(Duration::from_secs(5)).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), TEST_NAMESPACE);
	let _ = secrets.delete(KOPIA_SECRET_NAME, &Default::default()).await;

	// Wait for cascading deletes
	sleep(Duration::from_secs(5)).await;
}

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn full_restore_lifecycle() {
	let client = Client::try_default()
		.await
		.expect("expected a valid kubeconfig (e.g. from kind)");

	setup_namespace(&client).await;
	cleanup(&client).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), TEST_NAMESPACE);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), TEST_NAMESPACE);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), TEST_NAMESPACE);
	let services: Api<Service> = Api::namespaced(client.clone(), TEST_NAMESPACE);
	let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), TEST_NAMESPACE);
	let deployments: Api<Deployment> = Api::namespaced(client.clone(), TEST_NAMESPACE);

	// Step 1: Create kopia secret
	println!("--- creating kopia secret");
	secrets
		.create(&PostParams::default(), &kopia_secret())
		.await
		.expect("failed to create kopia secret");

	// Step 2: Create the replica CR
	println!("--- creating PostgresPhysicalReplica");
	let mut replica = replica_cr();
	replica.metadata.namespace = Some(TEST_NAMESPACE.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	// Step 3: Wait for replica to start restoring (snapshot discovery + restore creation)
	println!("--- waiting for replica Restoring phase");
	wait_for_replica_phase(&replicas, REPLICA_NAME, ReplicaPhase::Restoring).await;

	// Step 4: Wait for a restore to reach the Active phase (full lifecycle)
	println!("--- waiting for restore Active phase");
	let restore_name = wait_for_restore_phase(&restores, REPLICA_NAME, RestorePhase::Active).await;

	// Step 5: Verify replica reaches Ready
	println!("--- waiting for replica Ready phase");
	wait_for_replica_phase(&replicas, REPLICA_NAME, ReplicaPhase::Ready).await;

	// Step 6: Verify resources were created
	println!("--- verifying created resources");

	// Credentials secret
	let creds_secret_name = format!("{REPLICA_NAME}-creds");
	let creds = secrets
		.get(&creds_secret_name)
		.await
		.expect("credentials secret not found");
	let creds_data = creds.data.expect("credentials secret has no data");
	assert!(
		creds_data.contains_key("password"),
		"credentials secret missing 'password' key"
	);

	// Service
	let svc = services.get(REPLICA_NAME).await.expect("service not found");
	let svc_spec = svc.spec.expect("service has no spec");
	let ports = svc_spec.ports.expect("service has no ports");
	assert!(
		ports.iter().any(|p| p.port == 5432),
		"service should expose port 5432"
	);

	// PVC for the restore
	let pvc_name = format!("{restore_name}-data");
	pvcs.get(&pvc_name)
		.await
		.unwrap_or_else(|_| panic!("PVC {pvc_name} not found"));

	// Deployment for the restore (deployment name = restore name)
	deployments
		.get(&restore_name)
		.await
		.unwrap_or_else(|_| panic!("Deployment {restore_name} not found"));

	// Step 7: Verify replica status fields
	let replica = replicas
		.get(REPLICA_NAME)
		.await
		.expect("failed to get replica");
	let status = replica.status.expect("replica has no status");

	assert_eq!(status.phase, Some(ReplicaPhase::Ready));
	assert_eq!(
		status.current_restore.as_deref(),
		Some(restore_name.as_str()),
		"currentRestore should match"
	);
	assert!(status.service_name.is_some(), "serviceName should be set");
	assert!(
		status.last_restore_completed_at.is_some(),
		"lastRestoreCompletedAt should be set"
	);
	assert!(
		status.connection_info.is_some(),
		"connectionInfo should be set"
	);

	let conn = status.connection_info.unwrap();
	assert_eq!(conn.port, 5432);
	assert_eq!(conn.database, "postgres");
	assert_eq!(conn.username, "analytics");
	assert_eq!(conn.password_secret, creds_secret_name);

	// Step 8: Verify restore status fields
	let restore = restores
		.get(&restore_name)
		.await
		.expect("failed to get restore");
	let restore_status = restore.status.expect("restore has no status");

	assert_eq!(restore_status.phase, Some(RestorePhase::Active));
	assert!(
		restore_status.postgres_version.is_some(),
		"postgresVersion should be detected"
	);
	assert!(
		restore_status.restored_at.is_some(),
		"restoredAt should be set"
	);
	assert!(
		restore_status.activated_at.is_some(),
		"activatedAt should be set"
	);
	assert!(restore_status.pvc.is_some(), "pvc should be set in status");
	assert!(
		restore_status.deployment.is_some(),
		"deployment should be set in status"
	);

	println!("--- all assertions passed, cleaning up");
	cleanup(&client).await;
}
