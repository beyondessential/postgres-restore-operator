use std::time::Duration;

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, api::PostParams};
use postgres_restore_operator::types::{
	PersistentUser, PostgresPhysicalReplica, PostgresPhysicalRestore, ReplicaPhase, RestorePhase,
	SchemaMigrationPhase,
};
use tokio::time::{sleep, timeout};

use helpers::*;

mod helpers;

/// Run psql inside a restore's pod as `tupaia_read`, using the password the
/// operator generated. Returns stdout on success and the pair on failure so
/// negative cases can assert on the error text.
async fn psql_as_reader(
	ns: &str,
	deploy: &str,
	password: &str,
	sql: &[&str],
) -> (bool, String, String) {
	let pgpassword = format!("PGPASSWORD={password}");
	let mut cmd = vec![
		"env",
		pgpassword.as_str(),
		"psql",
		"-h",
		"127.0.0.1",
		"-U",
		"tupaia_read",
		"-d",
		"myapp",
		"-t",
		"-A",
	];
	cmd.extend_from_slice(sql);
	try_kubectl_exec(ns, deploy, &cmd).await
}

async fn read_secret_password(secrets: &Api<Secret>, name: &str) -> String {
	let secret = secrets
		.get(name)
		.await
		.unwrap_or_else(|e| panic!("failed to get secret {name}: {e}"));
	let data = secret
		.data
		.as_ref()
		.unwrap_or_else(|| panic!("secret {name} has no data"));
	let raw = data
		.get("password")
		.unwrap_or_else(|| panic!("secret {name} has no password key"));
	String::from_utf8(raw.0.clone()).expect("password is valid utf-8")
}

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn persistent_user_survives_switchover() {
	let client = make_client().await;
	let ns = "test-persistent-users";
	let replica_name = "pu-replica";
	let secret_name = "pu-replica-user-tupaia-read";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "pu-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating replica with a persistent schema and a persistent user");
	let mut replica = build_replica(
		replica_name,
		"pu-kopia-creds",
		ReplicaOpts {
			read_only: false,
			..Default::default()
		},
	);
	replica.spec.persistent_schemas = Some(vec!["public_tupaia".to_string()]);
	replica.spec.persistent_users = vec![PersistentUser {
		name: "tupaia_read".to_string(),
		read_schemas: vec!["public_tupaia".to_string()],
		search_path: vec!["public_tupaia".to_string()],
		secret_name: None,
	}];
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for first restore to become Active");
	let first_restore_name =
		wait_for_restore_phase(&restores, replica_name, RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, replica_name, ReplicaPhase::Ready, PHASE_TIMEOUT).await;
	let first_deploy = format!("deployment/{first_restore_name}");

	println!("--- verifying the per-user secret was created");
	let password = read_secret_password(&secrets, secret_name).await;
	assert!(!password.is_empty(), "generated password must not be empty");

	println!("--- creating the persistent schema owned by analytics on the first restore");
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
			"CREATE SCHEMA public_tupaia AUTHORIZATION analytics; \
			 CREATE TABLE public_tupaia.reports (id serial PRIMARY KEY, value text NOT NULL); \
			 INSERT INTO public_tupaia.reports (value) \
			   SELECT 'row-' || i FROM generate_series(1, 7) AS i",
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

	println!("--- waiting for schema migration to complete");
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

	let second_deploy = format!("deployment/{second_restore_name}");

	println!("--- verifying the password did not rotate across the switchover");
	let password_after = read_secret_password(&secrets, secret_name).await;
	assert_eq!(
		password, password_after,
		"persistent user password must stay stable across restores"
	);

	println!("--- verifying tupaia_read can log in to the new restore and read migrated data");
	let (ok, stdout, stderr) = psql_as_reader(
		ns,
		&second_deploy,
		&password,
		&["-c", "SELECT COUNT(*) FROM public_tupaia.reports"],
	)
	.await;
	assert!(
		ok,
		"tupaia_read login failed\nstdout: {stdout}\nstderr: {stderr}"
	);
	assert_eq!(
		stdout.trim(),
		"7",
		"expected the 7 migrated rows to be readable by tupaia_read"
	);

	println!("--- verifying searchPath lets it query unqualified names");
	let (ok, stdout, stderr) = psql_as_reader(
		ns,
		&second_deploy,
		&password,
		&["-c", "SELECT COUNT(*) FROM reports"],
	)
	.await;
	assert!(
		ok,
		"unqualified query failed, searchPath not applied\nstdout: {stdout}\nstderr: {stderr}"
	);
	assert_eq!(stdout.trim(), "7", "unqualified query returned wrong count");

	println!("--- verifying the role is read-only");
	let (ok, _, stderr) = psql_as_reader(
		ns,
		&second_deploy,
		&password,
		&[
			"-v",
			"ON_ERROR_STOP=1",
			"-c",
			"INSERT INTO public_tupaia.reports (value) VALUES ('nope')",
		],
	)
	.await;
	assert!(
		!ok && stderr.contains("permission denied"),
		"tupaia_read must not be able to write: stderr: {stderr}"
	);

	// The whole point of ALTER DEFAULT PRIVILEGES FOR ROLE <owner>: a table the
	// owner creates *after* provisioning must still be readable. Issuing the
	// statement as anyone but the owner silently covers nothing, and that
	// failure only shows up here.
	println!("--- verifying default privileges cover tables created after provisioning");
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
			"CREATE TABLE public_tupaia.later (id int); INSERT INTO public_tupaia.later VALUES (1)",
		],
	)
	.await;
	let (ok, stdout, stderr) = psql_as_reader(
		ns,
		&second_deploy,
		&password,
		&["-c", "SELECT COUNT(*) FROM public_tupaia.later"],
	)
	.await;
	assert!(
		ok,
		"default privileges did not cover a later-created table\nstdout: {stdout}\nstderr: {stderr}"
	);
	assert_eq!(
		stdout.trim(),
		"1",
		"expected to read the later-created table"
	);

	println!("--- removing the user from the spec and verifying its secret is deleted");
	let mut replica_obj = replicas
		.get(replica_name)
		.await
		.expect("failed to get replica");
	replica_obj.spec.persistent_users = vec![];
	replica_obj.metadata.managed_fields = None;
	replicas
		.replace(replica_name, &PostParams::default(), &replica_obj)
		.await
		.expect("failed to update replica");

	timeout(Duration::from_secs(120), async {
		loop {
			if secrets.get_opt(secret_name).await.ok().flatten().is_none() {
				return;
			}
			sleep(POLL_INTERVAL).await;
		}
	})
	.await
	.expect("timed out waiting for the removed user's secret to be deleted");

	cleanup_namespace(&client, ns, &[replica_name]).await;
}

#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn persistent_user_with_missing_schema_still_switches_over() {
	let client = make_client().await;
	let ns = "test-pu-missing-schema";
	let replica_name = "pu-missing-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "pu-missing-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating replica whose persistent user reads a schema that does not exist");
	let mut replica = build_replica(
		replica_name,
		"pu-missing-kopia-creds",
		ReplicaOpts {
			read_only: false,
			..Default::default()
		},
	);
	// Also covers the secretName override: the credential lands in a bare
	// `tupaia-read` rather than the replica-scoped default.
	replica.spec.persistent_users = vec![PersistentUser {
		name: "tupaia_read".to_string(),
		read_schemas: vec!["never_created".to_string()],
		search_path: vec![],
		secret_name: Some("tupaia-read".to_string()),
	}];
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for the restore to become Active despite the absent schema");
	let restore_name =
		wait_for_restore_phase(&restores, replica_name, RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, replica_name, ReplicaPhase::Ready, PHASE_TIMEOUT).await;

	println!("--- verifying the role was still created and can connect");
	let password = read_secret_password(&secrets, "tupaia-read").await;
	let (ok, stdout, stderr) = psql_as_reader(
		ns,
		&format!("deployment/{restore_name}"),
		&password,
		&["-c", "SELECT 1"],
	)
	.await;
	assert!(
		ok,
		"role must still be usable when a read schema is absent\nstdout: {stdout}\nstderr: {stderr}"
	);

	cleanup_namespace(&client, ns, &[replica_name]).await;
}

/// A `readOnly` replica — the default — has `default_transaction_read_only = on`
/// in its postgresql.conf, which fails every `CREATE ROLE` and `GRANT` unless
/// the provisioning session turns it off first. Without that, provisioning
/// errors and the switchover never completes, so this asserts the replica
/// reaches Ready at all as much as it asserts the role works.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn persistent_user_on_read_only_replica() {
	let client = make_client().await;
	let ns = "test-pu-read-only";
	let replica_name = "pu-ro-replica";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &[replica_name]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "pu-ro-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating a readOnly replica with a persistent user");
	let mut replica = build_replica(replica_name, "pu-ro-kopia-creds", ReplicaOpts::default());
	assert!(
		replica.spec.read_only,
		"this test is meaningless unless the replica is read-only"
	);
	replica.spec.persistent_users = vec![PersistentUser {
		name: "tupaia_read".to_string(),
		read_schemas: vec!["public".to_string()],
		search_path: vec!["public".to_string()],
		secret_name: None,
	}];
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for the restore to become Active (provisioning must not block it)");
	let restore_name =
		wait_for_restore_phase(&restores, replica_name, RestorePhase::Active, PHASE_TIMEOUT).await;
	wait_for_replica_phase(&replicas, replica_name, ReplicaPhase::Ready, PHASE_TIMEOUT).await;

	println!("--- verifying the role was created despite the read-only default");
	let password = read_secret_password(&secrets, "pu-ro-replica-user-tupaia-read").await;
	let (ok, stdout, stderr) = psql_as_reader(
		ns,
		&format!("deployment/{restore_name}"),
		&password,
		&["-c", "SELECT 1"],
	)
	.await;
	assert!(
		ok,
		"tupaia_read must exist on a read-only replica\nstdout: {stdout}\nstderr: {stderr}"
	);
	assert_eq!(stdout.trim(), "1");

	println!("--- verifying the replica is still read-only for that role");
	let (ok, _, stderr) = psql_as_reader(
		ns,
		&format!("deployment/{restore_name}"),
		&password,
		&[
			"-v",
			"ON_ERROR_STOP=1",
			"-c",
			"CREATE TABLE public.should_not_exist (id int)",
		],
	)
	.await;
	assert!(
		!ok,
		"the operator's session GUC must not leave the replica writable: {stderr}"
	);

	cleanup_namespace(&client, ns, &[replica_name]).await;
}
