use std::time::Duration;

use k8s_openapi::api::core::v1::Secret;
use kube::{
	Api,
	api::{Patch, PatchParams, PostParams},
};
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, ReplicaPhase, RestorePhase,
};
use tokio::time::sleep;

use helpers::*;

mod helpers;

/// After 3 consecutive restore failures the operator should set the
/// RestoreSchedulingSuspended condition to True and stop creating new
/// restores. Manually resetting consecutiveRestoreFailures to 0 via the
/// status subresource should clear the condition and allow scheduling to
/// resume.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn consecutive_failures_suspend_and_reset() {
	let client = make_client().await;
	let ns = "test-consecutive-failures";
	let replica_name = "consec-fail-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	// Use a bucket with non-postgres data so every restore fails during the
	// version-detect phase (no PG_VERSION file).
	println!("--- creating kopia secret pointing to non-postgres bucket");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "consec-fail-kopia", "non-postgres-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with aggressive schedule");
	let mut replica = build_replica(
		replica_name,
		"consec-fail-kopia",
		ReplicaOpts {
			// Trigger as often as possible so failures accumulate quickly
			schedule: "* * * * *".into(),
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	// Wait for the first restore to fail
	println!("--- waiting for the first restore to fail");
	wait_for_restore_phase(
		&restores,
		replica_name,
		RestorePhase::Failed,
		LONG_PHASE_TIMEOUT,
	)
	.await;

	// Wait until the operator suspends scheduling (>= 3 consecutive failures).
	// This may take several minutes as each restore cycle involves snapshot
	// listing, restoring, version detection, and failure cleanup.
	println!("--- waiting for RestoreSchedulingSuspended=True");
	wait_for_replica_condition(
		&client,
		ns,
		replica_name,
		"RestoreSchedulingSuspended",
		"True",
		Duration::from_secs(600),
	)
	.await;

	// Verify consecutiveRestoreFailures >= 3
	let replica_obj = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica");
	let status = replica_obj.status.as_ref().expect("replica has no status");
	let failures = status.consecutive_restore_failures.unwrap_or(0);
	assert!(
		failures >= 3,
		"consecutiveRestoreFailures should be >= 3, got {failures}"
	);

	// The replica phase should NOT be stuck at Restoring — it should be
	// updated even while suspended.
	let phase = status.phase.as_ref();
	assert_ne!(
		phase,
		Some(&ReplicaPhase::Restoring),
		"phase should not be stuck at Restoring while suspended"
	);

	// Record how many restores exist now
	let pre_reset_count = count_restores_for_replica(&client, ns, replica_name).await;
	println!("--- restores before reset: {pre_reset_count}");

	// Wait a bit and verify no new restores are created while suspended
	sleep(Duration::from_secs(90)).await;
	let still_suspended_count = count_restores_for_replica(&client, ns, replica_name).await;
	// Failed restores may get cleaned up, so count could decrease — the key
	// thing is that it shouldn't increase.
	assert!(
		still_suspended_count <= pre_reset_count,
		"no new restores should be created while suspended \
		 (before: {pre_reset_count}, after: {still_suspended_count})"
	);

	// Reset consecutiveRestoreFailures to 0 via the status subresource
	println!("--- resetting consecutiveRestoreFailures to 0 via status subresource");
	let reset_patch = serde_json::json!({
		"status": {
			"consecutiveRestoreFailures": 0
		}
	});
	replicas
		.patch_status(
			replica_name,
			&PatchParams::apply("integration-test"),
			&Patch::Merge(&reset_patch),
		)
		.await
		.expect("failed to reset consecutiveRestoreFailures");

	// The operator should clear RestoreSchedulingSuspended
	println!("--- waiting for RestoreSchedulingSuspended=False after reset");
	wait_for_replica_condition(
		&client,
		ns,
		replica_name,
		"RestoreSchedulingSuspended",
		"False",
		Duration::from_secs(120),
	)
	.await;

	// Verify the counter stayed at 0 (wasn't immediately reverted)
	let replica_after = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica after reset");
	let failures_after = replica_after
		.status
		.as_ref()
		.and_then(|s| s.consecutive_restore_failures)
		.unwrap_or(0);
	// The counter may have been re-incremented by a new (failing) restore, but
	// it should not have jumped straight back to the pre-reset value.
	assert!(
		failures_after < failures,
		"consecutiveRestoreFailures should not revert to the old value \
		 (was {failures}, now {failures_after})"
	);

	println!("--- consecutive failure suspension and reset working correctly");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}

/// When the replica phase is set to Restoring because of an in-progress
/// restore, and that restore subsequently fails, the phase should be
/// corrected even if consecutive failures reach the suspension threshold.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn phase_not_stuck_at_restoring_after_suspension() {
	let client = make_client().await;
	let ns = "test-phase-stuck";
	let replica_name = "phase-stuck-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret pointing to non-postgres bucket");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "phase-stuck-kopia", "non-postgres-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica");
	let mut replica = build_replica(
		replica_name,
		"phase-stuck-kopia",
		ReplicaOpts {
			schedule: "* * * * *".into(),
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	// The replica should pass through Restoring at some point
	println!("--- waiting for replica to enter Restoring phase");
	wait_for_replica_phase(
		&replicas,
		replica_name,
		ReplicaPhase::Restoring,
		PHASE_TIMEOUT,
	)
	.await;

	// Wait for suspension
	println!("--- waiting for RestoreSchedulingSuspended=True");
	wait_for_replica_condition(
		&client,
		ns,
		replica_name,
		"RestoreSchedulingSuspended",
		"True",
		Duration::from_secs(600),
	)
	.await;

	// Give the operator a couple of reconcile cycles to fix the phase
	sleep(Duration::from_secs(30)).await;

	// Phase must NOT be Restoring — it should be Pending (no active restore
	// ever succeeded) or Ready if one somehow got through.
	let replica_obj = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica");
	let phase = replica_obj
		.status
		.as_ref()
		.and_then(|s| s.phase.as_ref())
		.cloned();
	assert_ne!(
		phase,
		Some(ReplicaPhase::Restoring),
		"phase should not be stuck at Restoring after suspension (got {phase:?})"
	);

	println!("--- phase correctly updated after suspension, cleaning up");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}

/// Verify that resetting consecutiveRestoreFailures without using the status
/// subresource does NOT actually clear the field (since the CRD has the
/// status subresource enabled). This documents expected Kubernetes behavior.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn reset_requires_status_subresource() {
	let client = make_client().await;
	let ns = "test-subresource-reset";
	let replica_name = "subresource-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "subresource-kopia", "non-postgres-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating replica and waiting for failures to accumulate");
	let mut replica = build_replica(
		replica_name,
		"subresource-kopia",
		ReplicaOpts {
			schedule: "* * * * *".into(),
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	wait_for_replica_condition(
		&client,
		ns,
		replica_name,
		"RestoreSchedulingSuspended",
		"True",
		Duration::from_secs(600),
	)
	.await;

	let before = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica");
	let failures_before = before
		.status
		.as_ref()
		.and_then(|s| s.consecutive_restore_failures)
		.unwrap_or(0);
	assert!(failures_before >= 3);

	// Try to reset via the main resource endpoint (NOT status subresource).
	// This should be silently ignored by the API server.
	println!("--- attempting reset via main resource endpoint (should be ignored)");
	let bad_patch = serde_json::json!({
		"status": {
			"consecutiveRestoreFailures": 0
		}
	});
	let _ = replicas
		.patch(
			replica_name,
			&PatchParams::apply("integration-test"),
			&Patch::Merge(&bad_patch),
		)
		.await;

	// Give the operator time to reconcile
	sleep(Duration::from_secs(15)).await;

	let after = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica after bad patch");
	let failures_after = after
		.status
		.as_ref()
		.and_then(|s| s.consecutive_restore_failures)
		.unwrap_or(0);
	assert_eq!(
		failures_after, failures_before,
		"consecutiveRestoreFailures should not change when patched without --subresource=status \
		 (before: {failures_before}, after: {failures_after})"
	);

	// Condition should still be True
	let still_suspended = after
		.status
		.as_ref()
		.and_then(|s| {
			s.conditions
				.iter()
				.find(|c| c.type_ == "RestoreSchedulingSuspended")
		})
		.is_some_and(|c| c.status == "True");
	assert!(
		still_suspended,
		"RestoreSchedulingSuspended should still be True after bad patch"
	);

	// Now do it correctly via status subresource
	println!("--- resetting via status subresource (correct way)");
	replicas
		.patch_status(
			replica_name,
			&PatchParams::apply("integration-test"),
			&Patch::Merge(&bad_patch),
		)
		.await
		.expect("failed to patch status subresource");

	// The condition should now be cleared
	println!("--- waiting for RestoreSchedulingSuspended=False");
	wait_for_replica_condition(
		&client,
		ns,
		replica_name,
		"RestoreSchedulingSuspended",
		"False",
		Duration::from_secs(120),
	)
	.await;

	println!("--- status subresource requirement verified, cleaning up");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}
