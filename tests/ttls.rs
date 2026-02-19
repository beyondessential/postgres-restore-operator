use std::time::Duration;

use jiff::Span;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, api::PostParams};
use postgres_restore_operator::{
	types::{PostgresPhysicalReplica, PostgresPhysicalRestore, ReplicaPhase, RestorePhase},
	util::TimeSpan,
};
use tokio::time::sleep;

use helpers::*;

mod helpers;

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
			schedule: "* * * * *".into(),                       // every minute
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
