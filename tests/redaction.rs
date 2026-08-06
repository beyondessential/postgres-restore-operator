//! End-to-end redaction integration test.
//!
//! Requires a PG 18 kopia snapshot (see `setup-kopia-repo-pg18.yaml`) and
//! an HTTP server on the test host serving `redaction-manifest.json` at
//! [`MANIFEST_URL`]. The workflow sets both up before this test runs.
//!
//! The manifest server is host-side rather than in-cluster because the
//! operator is what fetches the manifest, and the integration harness runs
//! it out-of-cluster — a `.svc` name would not resolve for it.

use std::collections::BTreeMap;

use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{
	Api,
	api::{ObjectMeta, PostParams},
};
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, RedactionPhase, RedactionSpec, ReplicaPhase,
	RestorePhase,
};

use helpers::*;

mod helpers;

const NS: &str = "test-redaction";
const REPLICA_NAME: &str = "redaction-replica";
/// Where the harness serves `tests/fixtures/redaction-manifest.json`. The
/// literal `{version}` is the placeholder the operator fills from
/// `versionQuery`; the file is served under `v1.0.0/` to match the
/// `currentVersion` the PG-18 snapshot fixture seeds.
const MANIFEST_URL: &str = "http://127.0.0.1:8099/v{version}/manifest.json";

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO, PG-18 kopia snapshot and the manifest server"]
async fn redaction_applies_masks_to_restored_data() {
	let client = make_client().await;

	setup_namespace(&client, NS).await;
	cleanup_namespace(&client, NS, &[REPLICA_NAME]).await;

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
	let phase = status.redaction_phase.as_ref();
	assert!(
		phase.is_some_and(RedactionPhase::is_done),
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
	// Compare whole values, not substrings. The fixture's local parts are a
	// single letter, so a generated fake like `alexandragarcia@example.com`
	// contains `a@example.com` and read as unmasked — the assertion failed
	// whenever the faker happened to end a name in the right letter.
	let masked_emails: Vec<&str> = emails.trim().split(',').collect();
	assert_eq!(
		masked_emails.len(),
		5,
		"expected one email per row, got: {emails}"
	);
	for original in [
		"a@example.com",
		"b@example.com",
		"c@example.com",
		"d@example.com",
		"e@example.com",
	] {
		assert!(
			!masked_emails.contains(&original),
			"original email {original} survived masking, got: {emails}"
		);
	}
	assert!(
		masked_emails.iter().all(|email| email.contains('@')),
		"every masked email should still look like an email, got: {emails}"
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
	// Same hazard as the emails: "Alice" is short enough to turn up inside a
	// longer generated first name.
	let masked_single: Vec<&str> = single_names.trim().split('|').collect();
	for original in ["Alice", "Bob", "Carol", "Dave", "Eve"] {
		assert!(
			!masked_single.contains(&original),
			"original single_name {original} survived masking, got: {single_names}"
		);
	}

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
				manifest_url: MANIFEST_URL.to_string(),
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
