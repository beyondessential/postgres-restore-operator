use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::{
	ByteString,
	api::{
		apps::v1::Deployment,
		batch::v1::Job,
		core::v1::{PersistentVolumeClaim, Secret, Service},
	},
};
use kube::{
	Api,
	api::{ListParams, ObjectMeta, PostParams},
};
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, ReplicaPhase, RestorePhase,
};

use helpers::*;

mod helpers;

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

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn analytics_create_schema_read_write() {
	let client = make_client().await;
	let ns = "test-rw-schema";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["rw-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "rw-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with readOnly: false");
	let mut replica = build_replica(
		"rw-replica",
		"rw-kopia-creds",
		ReplicaOpts {
			read_only: false,
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for restore Active phase");
	let restore_name =
		wait_for_restore_phase(&restores, "rw-replica", RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, "rw-replica", ReplicaPhase::Ready, PHASE_TIMEOUT).await;

	let deploy_target = format!("deployment/{restore_name}");

	println!("--- verifying analytics user can CREATE SCHEMA on restored database");
	kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"postgres",
			"-c",
			"CREATE SCHEMA test_pgro",
		],
	)
	.await;

	println!("--- verifying analytics user can CREATE SCHEMA on application database");
	kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-c",
			"CREATE SCHEMA test_pgro_app",
		],
	)
	.await;

	println!("--- verifying analytics user can write data");
	kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-c",
			"CREATE TABLE test_pgro_app.rw_test (id int); INSERT INTO test_pgro_app.rw_test VALUES (1)",
		],
	)
	.await;

	println!("--- all read-write assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &["rw-replica"]).await;
}

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn analytics_read_only_cannot_create_schema() {
	let client = make_client().await;
	let ns = "test-ro-schema";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["ro-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "ro-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with readOnly: true");
	let mut replica = build_replica(
		"ro-replica",
		"ro-kopia-creds",
		ReplicaOpts {
			read_only: true,
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for restore Active phase");
	let restore_name =
		wait_for_restore_phase(&restores, "ro-replica", RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, "ro-replica", ReplicaPhase::Ready, PHASE_TIMEOUT).await;

	let deploy_target = format!("deployment/{restore_name}");

	println!("--- verifying analytics user can read data");
	kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-c",
			"SELECT COUNT(*) FROM test_data",
		],
	)
	.await;

	println!("--- verifying analytics user cannot CREATE SCHEMA");
	let (ok, stdout, stderr) = try_kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"postgres",
			"-c",
			"CREATE SCHEMA test_pgro",
		],
	)
	.await;
	let combined = format!("{stdout}{stderr}");
	assert!(
		!ok || combined.contains("permission denied") || combined.contains("ERROR"),
		"CREATE SCHEMA should fail in read-only mode, but it succeeded.\nstdout: {stdout}\nstderr: {stderr}"
	);

	println!("--- verifying analytics user cannot write data");
	let (ok, stdout, stderr) = try_kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-c",
			"INSERT INTO test_data (data) VALUES ('should fail')",
		],
	)
	.await;
	let combined = format!("{stdout}{stderr}");
	assert!(
		!ok || combined.contains("read-only") || combined.contains("ERROR"),
		"INSERT should fail in read-only mode, but it succeeded.\nstdout: {stdout}\nstderr: {stderr}"
	);

	println!("--- all read-only assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &["ro-replica"]).await;
}
