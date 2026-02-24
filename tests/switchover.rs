use k8s_openapi::api::core::v1::{LocalObjectReference, Secret, Service};
use kube::{Api, ResourceExt, api::PostParams};
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, PostgresPhysicalRestoreSpec, ReplicaPhase,
	RestorePhase,
};
use tokio::time::{sleep, timeout};

use helpers::*;

mod helpers;

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
			snapshot_time: None,
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
