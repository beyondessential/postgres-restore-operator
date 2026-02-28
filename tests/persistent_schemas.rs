use k8s_openapi::api::core::v1::{LocalObjectReference, Secret};
use kube::{Api, ResourceExt as _, api::PostParams};
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, PostgresPhysicalRestoreSpec, ReplicaPhase,
	RestorePhase,
};
use tokio::time::{sleep, timeout};

use helpers::*;

mod helpers;

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn persistent_schemas_migration() {
	let client = make_client().await;
	let ns = "test-persistent-schemas";
	let replica_name = "ps-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "ps-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with persistent_schemas and readOnly: false");
	let mut replica = build_replica(
		replica_name,
		"ps-kopia-creds",
		ReplicaOpts {
			read_only: false,
			..Default::default()
		},
	);
	replica.spec.persistent_schemas = Some(vec!["persistent_data".to_string()]);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for first restore to become Active");
	let first_restore_name =
		wait_for_restore_phase(&restores, replica_name, RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, replica_name, ReplicaPhase::Ready, PHASE_TIMEOUT).await;

	println!("--- first restore active: {first_restore_name}");

	let first_deploy = format!("deployment/{first_restore_name}");

	println!("--- creating persistent schema with data in the first restore");
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

	println!("--- verifying 42 rows exist in persistent schema on first restore");
	let count_out = kubectl_exec(
		ns,
		&first_deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM persistent_data.important_records",
		],
	)
	.await;
	assert_eq!(
		count_out.trim(),
		"42",
		"expected 42 rows in persistent_data schema on first restore"
	);

	let first_restore_obj = restores
		.get(&first_restore_name)
		.await
		.expect("failed to get first restore");
	let replica_obj = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica");

	// Manually create a second restore from the same snapshot to trigger switchover
	let second_restore_name = format!("{replica_name}-second");
	println!("--- creating second restore manually: {second_restore_name}");

	restores
		.create(
			&PostParams::default(),
			&build_second_restore(&second_restore_name, ns, &first_restore_obj, &replica_obj),
		)
		.await
		.expect("failed to create second restore");

	println!("--- waiting for second restore to reach Switching phase (migration starts)");
	timeout(LONG_PHASE_TIMEOUT, async {
		loop {
			if let Ok(restore) = restores.get(&second_restore_name).await {
				let phase = restore.status.as_ref().and_then(|s| s.phase.as_ref());
				println!("[{second_restore_name}] phase: {phase:?}");
				if phase == Some(&RestorePhase::Switching) {
					return;
				}
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for second restore to reach Switching phase");

	println!("--- waiting for schema migration Job to complete");
	timeout(LONG_PHASE_TIMEOUT, async {
		loop {
			if let Ok(replica) = replicas.get(replica_name).await {
				let phase = replica
					.status
					.as_ref()
					.and_then(|s| s.schema_migration_phase.as_deref());
				println!("[{replica_name}] schemaMigrationPhase: {phase:?}");
				if phase == Some("complete") {
					return;
				}
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for schema migration to complete");

	println!("--- waiting for second restore to become Active");
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
	.expect("timed out waiting for second restore to become Active");

	println!("--- verifying replica status after switchover");
	let replica_after = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica after switchover");
	let status = replica_after
		.status
		.as_ref()
		.expect("replica has no status after switchover");

	assert_eq!(
		status.phase,
		Some(ReplicaPhase::Ready),
		"replica should be Ready after switchover"
	);
	assert_eq!(
		status.current_restore.as_deref(),
		Some(second_restore_name.as_str()),
		"currentRestore should be the second restore"
	);
	assert!(
		status.persistent_schema_data_size.is_some(),
		"persistentSchemaDataSize should be set after migration"
	);

	println!("--- verifying persistent schema data exists in second restore");
	let second_deploy = format!("deployment/{second_restore_name}");
	let migrated_count = kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM persistent_data.important_records",
		],
	)
	.await;
	assert_eq!(
		migrated_count.trim(),
		"42",
		"expected 42 rows migrated into persistent_data schema on second restore"
	);

	println!("--- verifying schema structure was preserved");
	let col_count = kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM information_schema.columns \
			 WHERE table_schema = 'persistent_data' AND table_name = 'important_records'",
		],
	)
	.await;
	assert_eq!(
		col_count.trim(),
		"2",
		"expected 2 columns (id, value) in migrated table"
	);

	println!("--- all persistent schema migration assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn persistent_schemas_conflict_fails_restore() {
	let client = make_client().await;
	let ns = "test-ps-conflict";
	let replica_name = "ps-conflict-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "ps-conflict-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	// Configure persistent_schemas with "public", which already exists in the
	// snapshot's database. This should cause the second restore to fail when
	// the operator detects the conflict before migration.
	println!("--- creating replica with persistent_schemas: [\"public\"]");
	let mut replica = build_replica(
		replica_name,
		"ps-conflict-kopia-creds",
		ReplicaOpts {
			read_only: false,
			..Default::default()
		},
	);
	replica.spec.persistent_schemas = Some(vec!["public".to_string()]);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for first restore to become Active");
	let first_restore_name =
		wait_for_restore_phase(&restores, replica_name, RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, replica_name, ReplicaPhase::Ready, PHASE_TIMEOUT).await;
	println!("--- first restore active: {first_restore_name}");

	let first_restore_obj = restores
		.get(&first_restore_name)
		.await
		.expect("failed to get first restore");
	let replica_obj = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica");

	// Manually create a second restore from the same snapshot to trigger switchover.
	// The "public" schema will already exist in this snapshot, so migration should
	// be blocked and the restore should fail.
	let second_restore_name = format!("{replica_name}-conflict");
	println!("--- creating second restore: {second_restore_name}");

	restores
		.create(
			&PostParams::default(),
			&build_second_restore(&second_restore_name, ns, &first_restore_obj, &replica_obj),
		)
		.await
		.expect("failed to create second restore");

	println!("--- waiting for second restore to reach Failed phase (schema conflict)");
	timeout(LONG_PHASE_TIMEOUT, async {
		loop {
			if let Ok(restore) = restores.get(&second_restore_name).await {
				let phase = restore.status.as_ref().and_then(|s| s.phase.as_ref());
				println!("[{second_restore_name}] phase: {phase:?}");
				if phase == Some(&RestorePhase::Failed) {
					return;
				}
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for second restore to fail due to schema conflict");

	println!("--- verifying first restore is still Active");
	let first_restore_after = restores
		.get(&first_restore_name)
		.await
		.expect("failed to get first restore after conflict");
	assert_eq!(
		first_restore_after
			.status
			.as_ref()
			.and_then(|s| s.phase.as_ref()),
		Some(&RestorePhase::Active),
		"first restore should remain Active after schema conflict"
	);

	println!("--- verifying replica still points to first restore");
	let replica_after = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica after conflict");
	let status = replica_after
		.status
		.as_ref()
		.expect("replica has no status");
	assert_eq!(
		status.current_restore.as_deref(),
		Some(first_restore_name.as_str()),
		"currentRestore should still be the first restore"
	);

	println!("--- verifying consecutiveRestoreFailures was incremented");
	assert!(
		status.consecutive_restore_failures.unwrap_or(0) >= 1,
		"consecutiveRestoreFailures should be at least 1 after schema conflict"
	);

	println!("--- persistent schema conflict correctly prevented switchover, cleaning up");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}

/// When `persistent_schemas` includes a schema that was never created on the
/// source restore, migration should skip it and still succeed.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn persistent_schemas_skip_missing_on_source() {
	let client = make_client().await;
	let ns = "test-ps-skip-missing";
	let replica_name = "ps-skip-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "ps-skip-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	// Configure persistent_schemas with one real schema and one that will
	// never be created. The operator should skip the missing one.
	println!(
		"--- creating replica with persistent_schemas: [\"real_schema\", \"nonexistent_schema\"]"
	);
	let mut replica = build_replica(
		replica_name,
		"ps-skip-kopia-creds",
		ReplicaOpts {
			read_only: false,
			..Default::default()
		},
	);
	replica.spec.persistent_schemas = Some(vec![
		"real_schema".to_string(),
		"nonexistent_schema".to_string(),
	]);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for first restore to become Active");
	let first_restore_name =
		wait_for_restore_phase(&restores, replica_name, RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, replica_name, ReplicaPhase::Ready, PHASE_TIMEOUT).await;
	println!("--- first restore active: {first_restore_name}");

	let first_deploy = format!("deployment/{first_restore_name}");

	// Only create real_schema; nonexistent_schema is intentionally never created.
	println!("--- creating real_schema with data in the first restore");
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
			"CREATE SCHEMA real_schema; \
			 CREATE TABLE real_schema.items (id serial PRIMARY KEY, val text NOT NULL); \
			 INSERT INTO real_schema.items (val) \
			   SELECT 'item-' || i FROM generate_series(1, 10) AS i",
		],
	)
	.await;

	println!("--- verifying 10 rows in real_schema on first restore");
	let count_out = kubectl_exec(
		ns,
		&first_deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM real_schema.items",
		],
	)
	.await;
	assert_eq!(count_out.trim(), "10");

	let first_restore_obj = restores
		.get(&first_restore_name)
		.await
		.expect("failed to get first restore");
	let snapshot_id = first_restore_obj.spec.snapshot.clone();
	let snapshot_size = first_restore_obj.spec.snapshot_size.clone();
	let storage_size = first_restore_obj.spec.storage_size.clone();

	let replica_obj = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica");
	let replica_uid = replica_obj.uid().expect("replica has no UID");

	let second_restore_name = format!("{replica_name}-second");
	println!("--- creating second restore: {second_restore_name}");

	let second_restore = PostgresPhysicalRestore::new(
		&second_restore_name,
		PostgresPhysicalRestoreSpec {
			replica: LocalObjectReference {
				name: replica_name.into(),
			},
			snapshot: snapshot_id,
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
			serde_json::json!({ "pgro.bes.au/replica": replica_name }),
		);
		meta.insert(
			"ownerReferences".to_string(),
			serde_json::json!([{
				"apiVersion": "pgro.bes.au/v1alpha1",
				"kind": "PostgresPhysicalReplica",
				"name": replica_name,
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

	println!("--- waiting for schema migration to complete");
	timeout(LONG_PHASE_TIMEOUT, async {
		loop {
			if let Ok(replica) = replicas.get(replica_name).await {
				let phase = replica
					.status
					.as_ref()
					.and_then(|s| s.schema_migration_phase.as_deref());
				println!("[{replica_name}] schemaMigrationPhase: {phase:?}");
				if phase == Some("complete") {
					return;
				}
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for schema migration to complete");

	println!("--- waiting for second restore to become Active");
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
	.expect("timed out waiting for second restore to become Active");

	println!("--- verifying real_schema data was migrated to the second restore");
	let second_deploy = format!("deployment/{second_restore_name}");
	let migrated_count = kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM real_schema.items",
		],
	)
	.await;
	assert_eq!(
		migrated_count.trim(),
		"10",
		"expected 10 rows migrated into real_schema on second restore"
	);

	println!("--- verifying nonexistent_schema does not exist on the second restore");
	let schema_exists = kubectl_exec(
		ns,
		&second_deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'nonexistent_schema'",
		],
	)
	.await;
	assert_eq!(
		schema_exists.trim(),
		"0",
		"nonexistent_schema should not exist on the second restore"
	);

	println!("--- verifying replica is Ready after switchover");
	let replica_after = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica after switchover");
	let status = replica_after
		.status
		.as_ref()
		.expect("replica has no status");
	assert_eq!(status.phase, Some(ReplicaPhase::Ready));
	assert_eq!(
		status.current_restore.as_deref(),
		Some(second_restore_name.as_str()),
	);

	println!("--- skip-missing-schema assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}
