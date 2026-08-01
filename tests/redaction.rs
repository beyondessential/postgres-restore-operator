//! End-to-end redaction integration test.
//!
//! Requires a PG 18 kopia snapshot (see `setup-kopia-repo-pg18.yaml`)
//! and the in-namespace static manifest server (see
//! `manifest-server.yaml`). Both are deployed by the workflow before this
//! test runs.

use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{
	Api,
	api::{ObjectMeta, PostParams},
};
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, RedactionSpec, ReplicaPhase, RestorePhase,
};

use helpers::*;

mod helpers;

const NS: &str = "test-redaction";
const REPLICA_NAME: &str = "redaction-replica";

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO, PG-18 kopia snapshot and the manifest server"]
async fn redaction_applies_masks_to_restored_data() {
	let client = make_client().await;

	setup_namespace(&client, NS).await;
	cleanup_namespace(&client, NS, &[REPLICA_NAME]).await;

	println!("--- deploying in-namespace static manifest server");
	deploy_manifest_server(NS).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), NS);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), NS);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), NS);

	println!("--- creating kopia secret (PG-18 bucket)");
	secrets
		.create(
			&PostParams::default(),
			&build_pg18_kopia_secret(NS, "redaction-kopia-creds"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating PostgresPhysicalReplica with redaction config");
	let replica = build_redaction_replica(REPLICA_NAME, "redaction-kopia-creds");
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for first restore to become Active");
	let restore_name = wait_for_restore_phase(
		&restores,
		REPLICA_NAME,
		RestorePhase::Active,
		LONG_PHASE_TIMEOUT,
	)
	.await;
	wait_for_replica_phase(
		&replicas,
		REPLICA_NAME,
		ReplicaPhase::Ready,
		LONG_PHASE_TIMEOUT,
	)
	.await;
	println!("--- restore {restore_name} active");

	// At this point the operator has already applied redaction (because
	// the restore wouldn't transition Switching -> Active otherwise).
	let final_replica = replicas.get(REPLICA_NAME).await.unwrap();
	let status = final_replica.status.as_ref().expect("status set");
	let phase = status.redaction_phase.as_deref();
	assert!(
		matches!(phase, Some("complete") | Some("partial")),
		"redactionPhase should be complete or partial, got {phase:?}"
	);
	let version = status.redaction_version.as_deref();
	assert_eq!(
		version,
		Some("1.0.0"),
		"manifest version should be read from local_system_facts"
	);
	let cols = status.redaction_columns_applied.unwrap_or(0);
	assert!(
		cols >= 6,
		"expected at least 6 columns redacted, got {cols}"
	);

	let deploy = format!("deployment/{restore_name}");

	println!("--- verifying truncate mask emptied sync_lookup");
	let count = query_one_value(NS, &deploy, "SELECT count(*) FROM sync_lookup").await;
	assert_eq!(count.trim(), "0", "sync_lookup should be truncated");

	println!("--- verifying unmasked column kept original values");
	let unmasked = query_one_value(
		NS,
		&deploy,
		"SELECT string_agg(unmasked, ',' ORDER BY id) FROM users",
	)
	.await;
	assert_eq!(unmasked.trim(), "keep-1,keep-2,keep-3,keep-4,keep-5");

	println!("--- verifying email column was changed");
	let emails = query_one_value(
		NS,
		&deploy,
		"SELECT string_agg(email, ',' ORDER BY id) FROM users",
	)
	.await;
	assert!(
		!emails.contains("a@example.com"),
		"original email should be masked, got: {emails}"
	);
	assert!(
		emails.contains('@'),
		"masked email should still look like an email, got: {emails}"
	);

	println!("--- verifying name masks: full names (with space) and single names");
	// full_name has spaces in the original; mask preserves the space-pattern via
	// CASE-WHEN: full names get fake_name(), single names get fake_first_name().
	let full_names = query_one_value(
		NS,
		&deploy,
		"SELECT string_agg(full_name, '|' ORDER BY id) FROM users",
	)
	.await;
	assert!(
		!full_names.contains("Alice Apple"),
		"full_name should be masked, got: {full_names}"
	);
	let single_names = query_one_value(
		NS,
		&deploy,
		"SELECT string_agg(single_name, '|' ORDER BY id) FROM users",
	)
	.await;
	assert!(
		!single_names.contains("Alice"),
		"single_name should be masked, got: {single_names}"
	);

	println!("--- verifying date mask changed dob");
	let dobs = query_one_value(
		NS,
		&deploy,
		"SELECT string_agg(dob::text, ',' ORDER BY id) FROM users",
	)
	.await;
	assert!(
		!dobs.contains("1980-01-15"),
		"dob should be masked, got: {dobs}"
	);

	println!("--- verifying phone mask preserves prefix/suffix");
	let phones = query_one_value(
		NS,
		&deploy,
		"SELECT string_agg(phone, ',' ORDER BY id) FROM users",
	)
	.await;
	// anon.partial(phone, 2, '****', 2) keeps first 2 and last 2 chars
	assert!(
		phones.contains("+6") && phones.contains("****"),
		"phone should be partial-masked with ****, got: {phones}"
	);

	println!("--- verifying integer-range mask kept values in [50, 100]");
	let out_of_range = query_one_value(
		NS,
		&deploy,
		"SELECT count(*) FROM users WHERE heart_rate < 50 OR heart_rate > 100",
	)
	.await;
	assert_eq!(
		out_of_range.trim(),
		"0",
		"heart_rate should stay in 50..100"
	);

	println!("--- verifying read-only was re-enabled");
	let setting = query_one_value(NS, &deploy, "SHOW default_transaction_read_only").await;
	assert_eq!(
		setting.trim(),
		"on",
		"default_transaction_read_only should be on"
	);

	println!("--- verifying analytics role was demoted from SUPERUSER");
	let rolsuper = query_one_value(
		NS,
		&deploy,
		"SELECT rolsuper::text FROM pg_roles WHERE rolname = 'analytics'",
	)
	.await;
	assert_eq!(
		rolsuper.trim(),
		"false",
		"analytics user should no longer be SUPERUSER"
	);

	println!("--- all redaction assertions passed");
}

async fn deploy_manifest_server(ns: &str) {
	let status = tokio::process::Command::new("kubectl")
		.args([
			"apply",
			"-n",
			ns,
			"-f",
			"tests/fixtures/manifest-server.yaml",
		])
		.status()
		.await
		.expect("failed to run kubectl apply");
	assert!(status.success(), "kubectl apply for manifest server failed");

	// Wait briefly for the Service endpoint to come up. Best-effort —
	// the in-cluster DNS resolves the Service name even before the
	// backing pod is Ready, and the operator's reqwest call retries
	// on transient connection failures via the redaction failed:* path.
	tokio::time::sleep(Duration::from_secs(5)).await;
}

fn build_pg18_kopia_secret(ns: &str, name: &str) -> Secret {
	Secret {
		metadata: ObjectMeta {
			name: Some(name.into()),
			namespace: Some(ns.into()),
			..Default::default()
		},
		data: Some(BTreeMap::from([
			("bucket".into(), ByteString("test-bucket-pg18".into())),
			("region".into(), ByteString("us-east-1".into())),
			("accessKeyId".into(), ByteString("minioadmin".into())),
			("secretAccessKey".into(), ByteString("minioadmin".into())),
			(
				"repositoryPassword".into(),
				ByteString("test-repo-password".into()),
			),
			("endpoint".into(), ByteString("minio.minio.svc:9000".into())),
			("disableTls".into(), ByteString("true".into())),
		])),
		..Default::default()
	}
}

fn build_redaction_replica(name: &str, secret_ref: &str) -> PostgresPhysicalReplica {
	let mut replica = build_replica(
		name,
		secret_ref,
		ReplicaOpts {
			redaction: Some(RedactionSpec {
				manifest_url: format!("http://manifest-server.{NS}.svc/manifest.json"),
				version: None,
				version_query: Some(
					"SELECT value FROM local_system_facts WHERE key = 'currentVersion'".into(),
				),
				version_fallback_to_base: false,
			}),
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(NS.into());
	replica
}

async fn query_one_value(ns: &str, deploy: &str, sql: &str) -> String {
	kubectl_exec(
		ns,
		deploy,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"myapp",
			"-t",
			"-A",
			"-c",
			sql,
		],
	)
	.await
}
