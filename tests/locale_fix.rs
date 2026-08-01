use kube::Api;
use kube::api::PostParams;
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, ReplicaPhase, RestorePhase,
};

use helpers::*;

mod helpers;

/// Restoring a source whose databases carry a non-C locale must rewrite them,
/// record that it did so in `_pgro.restore_info.fixes`, and reindex before the
/// replica takes traffic.
///
/// The regression this guards: the single-user pass did the rewrite and set a
/// shell variable to say so, then the post-startup fallback overwrote that
/// variable with its own row count — necessarily 0, because the single-user
/// pass had already fixed every database. `fixes.locale` was therefore false on
/// every restore ever taken, and `/pgdata/needs-reindex` was never created, so
/// the collation reindex and the readiness gate that depends on it were both
/// dead code. A unit test on the generated script can catch the clobber, but
/// only a real restore of a real non-C cluster proves the flag ends up true.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn locale_rewrite_is_recorded_and_reindexed() {
	let client = make_client().await;
	let ns = "test-locale-fix";
	let replica_name = "locale-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<k8s_openapi::api::core::v1::Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret pointing at the non-C-locale snapshot");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "locale-kopia-creds", "locale-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating replica");
	let mut replica = build_replica(
		replica_name,
		"locale-kopia-creds",
		ReplicaOpts {
			schedule: "0 0 1 1 *".into(), // once a year: only the initial restore
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for the restore to go Active");
	let restore_name =
		wait_for_restore_phase(&restores, replica_name, RestorePhase::Active, PHASE_TIMEOUT).await;
	// The readiness probe is gated on /pgdata/needs-reindex, so reaching Ready
	// means the collation reindex has already finished. Allow the longer
	// timeout: this path does strictly more work than an unfixed restore.
	wait_for_replica_phase(
		&replicas,
		replica_name,
		ReplicaPhase::Ready,
		LONG_PHASE_TIMEOUT,
	)
	.await;
	let deploy = format!("deployment/{restore_name}");

	println!("--- checking recorded fixes");
	let fixes = kubectl_exec(
		ns,
		&deploy,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"postgres",
			"-At",
			"-c",
			"SELECT fixes::text FROM _pgro.restore_info WHERE id = 1",
		],
	)
	.await;
	let fixes = fixes.trim();
	println!("--- fixes = {fixes}");
	let fixes: serde_json::Value = serde_json::from_str(fixes).expect("fixes must be valid JSON");

	assert_eq!(
		fixes["locale"],
		serde_json::json!(true),
		"restoring an en_US.UTF-8 source must record locale: true"
	);
	assert_eq!(
		fixes["reindex"],
		serde_json::json!(true),
		"a locale rewrite invalidates collation-ordered indexes, so it must record reindex: true"
	);
	assert_eq!(
		fixes["reset_wal"],
		serde_json::json!(false),
		"a cleanly stopped source needs no pg_resetwal"
	);

	println!("--- checking the databases were actually rewritten");
	let collates = kubectl_exec(
		ns,
		&deploy,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"postgres",
			"-At",
			"-c",
			"SELECT DISTINCT datcollate FROM pg_database",
		],
	)
	.await;
	for collate in collates.split_whitespace() {
		assert_eq!(
			collate, "C.UTF-8",
			"every database must be rewritten to C.UTF-8; found {collate}"
		);
	}

	println!("--- checking the reindex ran to completion before Ready");
	let stage = kubectl_exec(
		ns,
		&deploy,
		&[
			"psql",
			"-U",
			"postgres",
			"-d",
			"postgres",
			"-At",
			"-c",
			"SELECT stage FROM _pgro.restore_info WHERE id = 1",
		],
	)
	.await;
	assert_eq!(
		stage.trim(),
		"ready",
		"the restore must reach the ready stage once the reindex finishes"
	);

	// The readiness probe is `pg_isready && [ ! -f /pgdata/needs-reindex ]`, so
	// the flag being gone is what let the replica go Ready. Asserting it
	// directly catches a probe that stopped consulting the flag.
	let (flag_present, _, _) =
		try_kubectl_exec(ns, &deploy, &["test", "-f", "/pgdata/needs-reindex"]).await;
	assert!(
		!flag_present,
		"/pgdata/needs-reindex must be cleared by the reindex before the replica serves traffic"
	);

	println!("--- checking the data survived the rewrite");
	let count = kubectl_exec(
		ns,
		&deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-At",
			"-c",
			"SELECT count(*) FROM test_data",
		],
	)
	.await;
	assert_eq!(
		count.trim(),
		"1000",
		"the locale rewrite and reindex must not lose rows"
	);

	println!("--- all assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &[replica_name]).await;
}
