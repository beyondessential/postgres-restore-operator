use k8s_openapi::api::{batch::v1::Job, core::v1::Secret};
use kube::{Api, api::PostParams};
use postgres_restore_operator::types::{
	MigrationTarget, PostgresPhysicalReplica, PostgresPhysicalRestore, RestorePhase,
};
use tokio::time::{sleep, timeout};

use helpers::*;

mod helpers;

/// The version the Job aims at. No such tamanu release exists, so the pull never
/// resolves and the Job stays pending: this test is about the Job the operator
/// builds and the state it puts the replica in, not about tamanu's migrations.
const TARGET_VERSION: &str = "0.0.1-pgro-integration";
const TARGET_VERSION_ID: &str = "77777777-7777-7777-7777-777777777777";

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn migration_target_drives_a_migration_job() {
	let client = make_client().await;
	let ns = "test-migration";
	let replica_name = "mig-replica";

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
			&build_kopia_secret(ns, "mig-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	// read_only stays true: lifting it for the migration is the operator's job,
	// and asserting the DDL below is what proves it did.
	println!("--- creating PostgresPhysicalReplica with a migration target");
	let mut replica = build_replica(replica_name, "mig-kopia-creds", ReplicaOpts::default());
	replica.spec.migrate_to = Some(MigrationTarget {
		version: TARGET_VERSION.to_string(),
		version_id: TARGET_VERSION_ID.to_string(),
	});
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

	println!("--- checking the restore is writable despite the replica being read-only");
	kubectl_exec(
		ns,
		&format!("deployment/{restore_name}"),
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-c",
			"CREATE SCHEMA migration_probe; CREATE TABLE migration_probe.t (id int)",
		],
	)
	.await;

	// Migrating is set by the reconcile before the one that creates the Job.
	let job_name = format!("{restore_name}-migrate");
	println!("--- waiting for migration job {job_name}");
	let job = timeout(PHASE_TIMEOUT, async {
		loop {
			if let Ok(Some(job)) = jobs.get_opt(&job_name).await {
				return job;
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.unwrap_or_else(|_| panic!("timed out waiting for migration job {job_name}"));

	assert_eq!(
		job.spec.as_ref().unwrap().backoff_limit,
		Some(0),
		"a failed migration is the finding, so the job must not retry"
	);

	let container = &job.spec.unwrap().template.spec.unwrap().containers[0];
	assert_eq!(
		container.image.as_deref(),
		Some(format!("ghcr.io/beyondessential/tamanu-central:v{TARGET_VERSION}").as_str())
	);
	assert_eq!(
		container.command, None,
		"the image entrypoint takes the subcommand"
	);
	assert_eq!(
		container.args.as_deref(),
		Some(["migrate".to_string()].as_slice())
	);

	let env = |name: &str| {
		container
			.env
			.as_ref()
			.unwrap()
			.iter()
			.find(|e| e.name == name)
			.unwrap_or_else(|| panic!("expected env {name} on the migration job"))
			.clone()
	};

	// The database the snapshot actually holds, discovered from the restore, not
	// the credentials username.
	assert_eq!(
		env("CONFIG_SYNC_DB_NAME").value.as_deref(),
		Some("myapp"),
		"the job must target the restored database"
	);
	assert_eq!(
		env("CONFIG_SYNC_DB_HOST").value.as_deref(),
		Some(restore_name.as_str()),
		"the per-restore Service, so a switchover cannot repoint it mid-migration"
	);
	for name in ["CONFIG_SYNC_DB_USERNAME", "CONFIG_SYNC_DB_PASSWORD"] {
		assert!(
			env(name).value_from.is_some(),
			"{name} must come from the credentials secret"
		);
	}

	println!("--- cleaning up");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}
