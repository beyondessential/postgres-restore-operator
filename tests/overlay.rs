use k8s_openapi::{api::core::v1::Secret, apimachinery::pkg::api::resource::Quantity};
use kube::{Api, api::PostParams};
use postgres_restore_operator::types::{
	OverlayDatabaseConfig, OverlayStrategy, PostgresPhysicalReplica, PostgresPhysicalRestore,
	ReplicaPhase, RestorePhase,
};
use tokio::time::{sleep, timeout};

use helpers::*;

mod helpers;

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO, kopia, and CNPG"]
async fn overlay_fdw_reconciliation() {
	let client = make_client().await;
	let ns = "test-overlay";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["overlay-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "overlay-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with overlay database");
	let mut replica = build_replica(
		"overlay-replica",
		"overlay-kopia-creds",
		ReplicaOpts {
			overlay_database: Some(OverlayDatabaseConfig {
				strategy: OverlayStrategy::Fdw,
				postgres_version: Some(17),
				image_catalog: None,
				storage_size_override: Some(Quantity("2Gi".into())),
				storage_class: None,
				resources: None,
				affinity: None,
				tolerations: vec![],
				service_annotations: None,
				import_generated: false,
				retain_restore: true,
			}),
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for replica Restoring phase");
	wait_for_replica_phase(
		&replicas,
		"overlay-replica",
		ReplicaPhase::Restoring,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for restore Active phase");
	let restore_name = wait_for_restore_phase(
		&restores,
		"overlay-replica",
		RestorePhase::Active,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for replica Ready phase");
	wait_for_replica_phase(
		&replicas,
		"overlay-replica",
		ReplicaPhase::Ready,
		LONG_PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for overlay CNPG cluster pod to be ready");
	let overlay_pod = "overlay-replica-overlay-1";
	wait_for_pod_ready(ns, overlay_pod, LONG_PHASE_TIMEOUT).await;

	println!("--- waiting for overlayRestore status to be set");
	timeout(PHASE_TIMEOUT, async {
		loop {
			if let Ok(r) = replicas.get("overlay-replica").await {
				let overlay_restore = r.status.as_ref().and_then(|s| s.overlay_restore.as_ref());
				if let Some(restore) = overlay_restore {
					println!("[overlay-replica] overlayRestore = {restore}");
					return;
				}
				println!("[overlay-replica] overlayRestore not set yet");
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for overlayRestore to be set");

	println!("--- verifying replica status fields");
	let replica = replicas
		.get("overlay-replica")
		.await
		.expect("failed to get replica");
	let status = replica.status.as_ref().expect("replica has no status");

	assert_eq!(
		status.overlay_restore.as_deref(),
		Some(restore_name.as_str()),
		"overlayRestore should match currentRestore"
	);
	assert!(
		status.overlay_cluster_name.is_some(),
		"overlayClusterName should be set"
	);
	assert_eq!(
		status.overlay_cluster_name.as_deref(),
		Some("overlay-replica-overlay"),
	);

	println!("--- verifying FDW server exists in overlay database");
	let fdw_servers = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT srvname FROM pg_foreign_server WHERE srvname LIKE 'fdw_%'",
		],
	)
	.await;
	let fdw_servers: Vec<&str> = fdw_servers.trim().lines().collect();
	assert!(
		!fdw_servers.is_empty(),
		"expected at least one FDW server, got none"
	);
	println!("  FDW servers: {fdw_servers:?}");

	println!("--- verifying FDW server points to the correct database (myapp)");
	let server_dbname = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			&format!(
				"SELECT option_value FROM pg_options_to_table( \
				   (SELECT srvoptions FROM pg_foreign_server WHERE srvname = '{}') \
				 ) WHERE option_name = 'dbname'",
				fdw_servers[0]
			),
		],
	)
	.await;
	assert_eq!(
		server_dbname.trim(),
		"myapp",
		"FDW server should point to 'myapp' database, got '{}'",
		server_dbname.trim()
	);

	println!("--- verifying foreign tables were imported");
	let ft_count = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM information_schema.foreign_tables",
		],
	)
	.await;
	let ft_count: i64 = ft_count
		.trim()
		.parse()
		.expect("failed to parse foreign table count");
	assert!(
		ft_count > 0,
		"expected at least one foreign table, got {ft_count}"
	);
	println!("  foreign tables: {ft_count}");

	println!("--- verifying end-to-end FDW query works (reading through foreign table)");
	let row_count = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM public.test_data",
		],
	)
	.await;
	let row_count: i64 = row_count.trim().parse().expect("failed to parse row count");
	assert_eq!(
		row_count, 1000,
		"expected 1000 rows from foreign table test_data, got {row_count}"
	);

	println!("--- verifying analytics user can CREATE SCHEMA on overlay database");
	let analytics_password = {
		let creds: Api<Secret> = Api::namespaced(client.clone(), ns);
		let secret = creds
			.get("overlay-replica-creds")
			.await
			.expect("creds secret not found");
		let data = secret.data.expect("creds secret has no data");
		let pw = data.get("password").expect("no password key");
		String::from_utf8(pw.0.clone()).expect("password not utf8")
	};
	kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			&format!("host=localhost dbname=app user=analytics password={analytics_password}"),
			"-c",
			"CREATE SCHEMA test_pgro",
		],
	)
	.await;
	kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			&format!("host=localhost dbname=app user=analytics password={analytics_password}"),
			"-c",
			"DROP SCHEMA test_pgro",
		],
	)
	.await;

	println!("--- all overlay FDW assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &["overlay-replica"]).await;
}

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO, kopia, and CNPG"]
async fn overlay_copy_reconciliation() {
	let client = make_client().await;
	let ns = "test-overlay-copy";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["copy-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "copy-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with overlay database (copy strategy)");
	let mut replica = build_replica(
		"copy-replica",
		"copy-kopia-creds",
		ReplicaOpts {
			overlay_database: Some(OverlayDatabaseConfig {
				strategy: OverlayStrategy::Copy,
				postgres_version: Some(17),
				image_catalog: None,
				storage_size_override: Some(Quantity("2Gi".into())),
				storage_class: None,
				resources: None,
				affinity: None,
				tolerations: vec![],
				service_annotations: None,
				import_generated: false,
				retain_restore: true,
			}),
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for replica Restoring phase");
	wait_for_replica_phase(
		&replicas,
		"copy-replica",
		ReplicaPhase::Restoring,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for restore Active phase");
	let restore_name = wait_for_restore_phase(
		&restores,
		"copy-replica",
		RestorePhase::Active,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for replica Ready phase");
	wait_for_replica_phase(
		&replicas,
		"copy-replica",
		ReplicaPhase::Ready,
		LONG_PHASE_TIMEOUT,
	)
	.await;

	println!("--- waiting for overlay CNPG cluster pod to be ready");
	let overlay_pod = "copy-replica-overlay-1";
	wait_for_pod_ready(ns, overlay_pod, LONG_PHASE_TIMEOUT).await;

	println!("--- waiting for overlayRestore status to be set");
	timeout(PHASE_TIMEOUT, async {
		loop {
			if let Ok(r) = replicas.get("copy-replica").await {
				let overlay_restore = r.status.as_ref().and_then(|s| s.overlay_restore.as_ref());
				if let Some(restore) = overlay_restore {
					println!("[copy-replica] overlayRestore = {restore}");
					return;
				}
				println!("[copy-replica] overlayRestore not set yet");
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for overlayRestore to be set");

	println!("--- verifying replica status fields");
	let replica = replicas
		.get("copy-replica")
		.await
		.expect("failed to get replica");
	let status = replica.status.as_ref().expect("replica has no status");

	assert_eq!(
		status.overlay_restore.as_deref(),
		Some(restore_name.as_str()),
		"overlayRestore should match currentRestore"
	);
	assert!(
		status.overlay_cluster_name.is_some(),
		"overlayClusterName should be set"
	);
	assert_eq!(
		status.overlay_cluster_name.as_deref(),
		Some("copy-replica-overlay"),
	);

	println!("--- waiting for overlay copy to complete (state = complete)");
	timeout(LONG_PHASE_TIMEOUT, async {
		loop {
			let output = kubectl_exec(
				ns,
				overlay_pod,
				&[
					"psql",
					"-U",
					"postgres",
					"-d",
					"app",
					"-t",
					"-A",
					"-c",
					"SELECT phase FROM _pgro.overlay_state WHERE id = 1",
				],
			)
			.await;
			let phase = output.trim();
			if phase == "complete" {
				println!("[copy-replica] overlay copy phase: complete");
				return;
			}
			println!("[copy-replica] overlay copy phase: {phase}, waiting for complete");
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for overlay copy to complete");

	println!("--- verifying no foreign tables exist (copy should create real tables)");
	let ft_count = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM information_schema.foreign_tables",
		],
	)
	.await;
	let ft_count: i64 = ft_count
		.trim()
		.parse()
		.expect("failed to parse foreign table count");
	assert_eq!(
		ft_count, 0,
		"copy strategy should not create foreign tables, got {ft_count}"
	);

	println!("--- verifying no FDW servers exist");
	let fdw_servers = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM pg_foreign_server",
		],
	)
	.await;
	let fdw_count: i64 = fdw_servers
		.trim()
		.parse()
		.expect("failed to parse FDW server count");
	assert_eq!(
		fdw_count, 0,
		"copy strategy should not create FDW servers, got {fdw_count}"
	);

	println!("--- verifying real tables were copied into the overlay");
	let table_count = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
		],
	)
	.await;
	let table_count: i64 = table_count
		.trim()
		.parse()
		.expect("failed to parse table count");
	assert!(
		table_count > 0,
		"expected at least one real table in public schema, got {table_count}"
	);
	println!("  real tables in public schema: {table_count}");

	println!("--- verifying data was copied (reading from real table)");
	let row_count = kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"app",
			"-t",
			"-A",
			"-c",
			"SELECT COUNT(*) FROM public.test_data",
		],
	)
	.await;
	let row_count: i64 = row_count.trim().parse().expect("failed to parse row count");
	assert_eq!(
		row_count, 1000,
		"expected 1000 rows copied into test_data, got {row_count}"
	);

	println!("--- verifying analytics user can CREATE SCHEMA on overlay database");
	let analytics_password = {
		let creds: Api<Secret> = Api::namespaced(client.clone(), ns);
		let secret = creds
			.get("copy-replica-creds")
			.await
			.expect("creds secret not found");
		let data = secret.data.expect("creds secret has no data");
		let pw = data.get("password").expect("no password key");
		String::from_utf8(pw.0.clone()).expect("password not utf8")
	};
	kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			&format!("host=localhost dbname=app user=analytics password={analytics_password}"),
			"-c",
			"CREATE SCHEMA test_pgro",
		],
	)
	.await;
	kubectl_exec(
		ns,
		overlay_pod,
		&[
			"psql",
			&format!("host=localhost dbname=app user=analytics password={analytics_password}"),
			"-c",
			"DROP SCHEMA test_pgro",
		],
	)
	.await;

	println!("--- all overlay copy assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &["copy-replica"]).await;
}
