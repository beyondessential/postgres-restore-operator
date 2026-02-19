use std::time::Duration;

use k8s_openapi::api::{batch::v1::Job, core::v1::Secret};
use kube::{
	Api,
	api::{ListParams, PostParams},
};
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, ReplicaPhase, RestorePhase,
};
use tokio::time::{sleep, timeout};

use helpers::*;

mod helpers;

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
