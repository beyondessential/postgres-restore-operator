use k8s_openapi::api::core::v1::Secret;
use kube::{Api, api::PostParams};
use postgres_restore_operator::types::{
	ExtraUserSpec, PostgresPhysicalReplica, PostgresPhysicalRestore, ReplicaPhase, RestorePhase,
	SchemaMigrationPhase,
};
use tokio::time::{sleep, timeout};

use helpers::*;

mod helpers;

/// An extra user scoped to a `persistent_schemas` schema keeps its grants
/// across a switchover. The init script can't grant a schema that isn't in
/// the snapshot yet, and the migration Job rewrites it with
/// `--no-privileges`, so this only passes if the operator re-applies.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn extra_user_keeps_schema_grants_across_a_persistent_schema_migration() {
	let client = make_client().await;
	let ns = "test-extra-user-grants";
	let replica_name = "eug-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "eug-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating replica with a persistent schema and a user scoped to it");
	let mut replica = build_replica(
		replica_name,
		"eug-kopia-creds",
		ReplicaOpts {
			read_only: false,
			extra_users: vec![ExtraUserSpec {
				name: "reporting".into(),
				schemas: vec!["persistent_data".into()],
			}],
			..Default::default()
		},
	);
	replica.spec.persistent_schemas = Some(vec!["persistent_data".to_string()]);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for the first restore to become Active");
	let first_restore_name =
		wait_for_restore_phase(&restores, replica_name, RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, replica_name, ReplicaPhase::Ready, PHASE_TIMEOUT).await;
	let first_deploy = format!("deployment/{first_restore_name}");

	println!("--- creating the persistent schema in the first restore");
	kubectl_exec(
		ns,
		&first_deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-c",
			"CREATE SCHEMA persistent_data; \
			 CREATE TABLE persistent_data.important_records (id serial PRIMARY KEY, value text NOT NULL); \
			 INSERT INTO persistent_data.important_records (value) \
			   SELECT 'record-' || i FROM generate_series(1, 42) AS i",
		],
	)
	.await;

	let first_restore_obj = restores
		.get(&first_restore_name)
		.await
		.expect("failed to get first restore");
	let replica_obj = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica");

	let second_restore_name = format!("{replica_name}-second");
	println!("--- creating second restore manually: {second_restore_name}");
	restores
		.create(
			&PostParams::default(),
			&build_second_restore(&second_restore_name, ns, &first_restore_obj, &replica_obj),
		)
		.await
		.expect("failed to create second restore");

	println!("--- waiting for the schema migration to complete");
	timeout(LONG_PHASE_TIMEOUT, async {
		loop {
			if let Ok(replica) = replicas.get(replica_name).await {
				let phase = replica
					.status
					.as_ref()
					.and_then(|s| s.schema_migration_phase.as_ref());
				println!("[{replica_name}] schemaMigrationPhase: {phase:?}");
				if matches!(phase, Some(SchemaMigrationPhase::Complete)) {
					return;
				}
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for schema migration to complete");

	println!("--- waiting for the second restore to become Active");
	timeout(LONG_PHASE_TIMEOUT, async {
		loop {
			if let Ok(restore) = restores.get(&second_restore_name).await {
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
	.expect("timed out waiting for the second restore to become Active");
	wait_for_replica_phase(&replicas, replica_name, ReplicaPhase::Ready, PHASE_TIMEOUT).await;

	let second_deploy = format!("deployment/{second_restore_name}");

	println!("--- verifying USAGE on the migrated schema");
	let out = kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"myapp",
			"-tAc",
			"SELECT has_schema_privilege('reporting', 'persistent_data', 'USAGE')",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"t",
		"the migrated schema must be granted USAGE after the migration recreated it"
	);

	println!("--- verifying the user can actually read the migrated rows");
	let out = kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"reporting",
			"-d",
			"myapp",
			"-tAc",
			"SELECT count(*) FROM persistent_data.important_records",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"42",
		"the scoped user must be able to SELECT the migrated table"
	);

	// `pg_default_acl` rows are keyed by namespace, so without a re-apply
	// tables a dbt run writes after the migration come back unreadable even
	// though the schema itself is granted.
	println!("--- verifying default privileges cover tables written after the migration");
	kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-c",
			"CREATE TABLE persistent_data.written_later (id integer)",
		],
	)
	.await;
	let out = kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"reporting",
			"-d",
			"myapp",
			"-tAc",
			"SELECT count(*) FROM persistent_data.written_later",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"0",
		"a table the analytics role creates after the migration must be readable too"
	);

	println!("--- verifying the grant stays scoped to the declared schema");
	let out = kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"myapp",
			"-tAc",
			"SELECT has_schema_privilege('reporting', 'public', 'USAGE')",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"f",
		"only the declared schemas are granted; `public` is not one of them"
	);

	println!("--- all extra user grant assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}
