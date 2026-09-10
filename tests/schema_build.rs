use k8s_openapi::api::{batch::v1::Job, core::v1::Secret};
use kube::{Api, api::PostParams};
use postgres_restore_operator::types::{
	MigrationTarget, PostgresPhysicalReplica, PostgresPhysicalRestore, RestorePhase,
};
use tokio::time::{sleep, timeout};

use helpers::*;

mod helpers;

/// The version the restore migrates to. No such tamanu release exists, so the
/// migration Job's pull never resolves and the restore sits in `Migrating`. That
/// is fine: this test is about the build Job the operator constructs from the
/// replica's spec, not about tamanu's migrations or a real dbt build.
const TARGET_VERSION: &str = "0.0.1-pgro-integration";
const TARGET_VERSION_ID: &str = "88888888-8888-8888-8888-888888888888";

/// An image that will never pull, for the same reason as the version above.
const BUILDER_IMAGE: &str = "ghcr.io/beyondessential/pgro-integration-builder:0.0.1";

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn a_builder_image_drives_a_schema_build_job() {
	let client = make_client().await;
	let ns = "test-schema-build";
	let replica_name = "build-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);
	let jobs: Api<Job> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "build-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with a builder image");
	let mut replica = build_replica(replica_name, "build-kopia-creds", ReplicaOpts::default());
	// A build needs a version to build for, which is the one the restore
	// migrated to, so a builder image without a target builds nothing.
	replica.spec.migrate_to = Some(MigrationTarget {
		version: TARGET_VERSION.to_string(),
		version_id: TARGET_VERSION_ID.to_string(),
	});
	replica.spec.builder_image = Some(BUILDER_IMAGE.to_string());
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for a restore to reach Migrating");
	let restore_name = wait_for_restore_phase(
		&restores,
		replica_name,
		RestorePhase::Migrating,
		LONG_PHASE_TIMEOUT,
	)
	.await;
	println!("--- restore {restore_name} is migrating");

	// The replica's spec is snapshotted onto each restore at creation, so a
	// builder image that moves mid-restore does not change what this restore
	// builds with.
	let restore = restores
		.get(&restore_name)
		.await
		.expect("failed to read the restore");
	assert_eq!(
		restore.spec.builder_image.as_deref(),
		Some(BUILDER_IMAGE),
		"the restore snapshots the image it builds with"
	);

	// The build gate runs before switchover, so the Job appears once the
	// restore has migrated. It never will here, because the migration image
	// does not resolve, which is what this half of the test settles: the
	// operator does not build against an unmigrated restore.
	let job_name = format!("{replica_name}-schema-build");
	println!("--- confirming no build job while the restore is still migrating");
	sleep(POLL_INTERVAL * 4).await;
	assert!(
		jobs.get_opt(&job_name)
			.await
			.expect("failed to list jobs")
			.is_none(),
		"a build must not run against a restore that has not migrated"
	);

	println!("--- confirming the migration job is the one that exists");
	let migration_job_name = format!("{restore_name}-migrate");
	let job = timeout(PHASE_TIMEOUT, async {
		loop {
			if let Ok(Some(job)) = jobs.get_opt(&migration_job_name).await {
				return job;
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.unwrap_or_else(|_| panic!("timed out waiting for migration job {migration_job_name}"));

	assert_eq!(
		job.spec.as_ref().unwrap().backoff_limit,
		Some(0),
		"a failed migration is the finding, so the job must not retry"
	);
}

/// A replica with no builder image switches over without building anything, so
/// the gate must not hold a plain replica.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn a_replica_without_a_builder_image_builds_nothing() {
	let client = make_client().await;
	let ns = "test-schema-build-absent";
	let replica_name = "no-build-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);
	let jobs: Api<Job> = Api::namespaced(client.clone(), ns);

	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "no-build-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	let mut replica = build_replica(replica_name, "no-build-kopia-creds", ReplicaOpts::default());
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for the restore to go active without a build");
	let restore_name = wait_for_restore_phase(
		&restores,
		replica_name,
		RestorePhase::Active,
		LONG_PHASE_TIMEOUT,
	)
	.await;
	println!("--- restore {restore_name} is active");

	assert!(
		jobs.get_opt(&format!("{replica_name}-schema-build"))
			.await
			.expect("failed to list jobs")
			.is_none(),
		"a replica with no builder image must not run a build job"
	);

	let restore = restores
		.get(&restore_name)
		.await
		.expect("failed to read the restore");
	assert!(
		restore
			.status
			.as_ref()
			.and_then(|s| s.schema_build_result.as_ref())
			.is_none(),
		"nothing was built, so there is no build result to record"
	);
}
