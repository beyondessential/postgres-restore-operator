use k8s_openapi::api::core::v1::Secret;
use kube::{Api, api::PostParams};
use postgres_restore_operator::types::{
	PostgresPhysicalReplica, PostgresPhysicalRestore, ReplicaPhase, RestorePhase,
};

use helpers::*;

mod helpers;

/// An extra user is provisioned as an unprivileged LOGIN role with its own
/// password Secret: it can connect, its sessions are read-only, and it holds no
/// privileges beyond what `PUBLIC` carries.
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster with MinIO and kopia"]
async fn extra_user_provisioned_without_privileges() {
	let client = make_client().await;
	let ns = "test-extra-users";

	setup_namespace(&client, ns).await;
	cleanup_namespace(&client, ns, &["extra-users-replica"]).await;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
	let replicas: Api<PostgresPhysicalReplica> = Api::namespaced(client.clone(), ns);
	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), ns);

	println!("--- creating kopia secret");
	secrets
		.create(
			&PostParams::default(),
			&build_kopia_secret(ns, "extra-users-kopia-creds", "test-bucket"),
		)
		.await
		.expect("failed to create kopia secret");

	println!("--- creating replica with extra users (readOnly: true)");
	let mut replica = build_replica(
		"extra-users-replica",
		"extra-users-kopia-creds",
		ReplicaOpts {
			read_only: true,
			extra_users: vec!["writer".into(), "report_writer".into()],
			..Default::default()
		},
	);
	replica.metadata.namespace = Some(ns.into());
	replicas
		.create(&PostParams::default(), &replica)
		.await
		.expect("failed to create replica");

	println!("--- waiting for restore Active and replica Ready");
	let restore_name = wait_for_restore_phase(
		&restores,
		"extra-users-replica",
		RestorePhase::Active,
		PHASE_TIMEOUT,
	)
	.await;
	wait_for_replica_phase(
		&replicas,
		"extra-users-replica",
		ReplicaPhase::Ready,
		PHASE_TIMEOUT,
	)
	.await;

	println!("--- verifying the per-user credentials secret");
	let user_secret = secrets
		.get("extra-users-replica-user-writer-creds")
		.await
		.expect("extra user secret not found");
	let data = user_secret.data.expect("extra user secret has no data");
	assert!(
		data.contains_key("password"),
		"extra user secret missing 'password' key"
	);
	assert_eq!(
		data.get("username")
			.map(|b| String::from_utf8_lossy(&b.0).to_string())
			.as_deref(),
		Some("writer"),
		"extra user secret should carry the username"
	);

	println!("--- verifying the underscored user's credentials secret");
	let underscored_secret = secrets
		.get("extra-users-replica-user-report-writer-creds")
		.await
		.expect("underscored extra user secret not found");
	assert_eq!(
		underscored_secret
			.data
			.expect("underscored extra user secret has no data")
			.get("username")
			.map(|b| String::from_utf8_lossy(&b.0).to_string())
			.as_deref(),
		Some("report_writer"),
		"the Secret carries the role name, not the slugged form"
	);

	let deploy_target = format!("deployment/{restore_name}");

	println!("--- verifying the extra user exists without elevated attributes");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"writer",
			"-d",
			"postgres",
			"-tAc",
			"SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls \
			 FROM pg_roles WHERE rolname = 'writer'",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"f|f|f|f|f",
		"the extra user should exist with no elevated role attributes"
	);

	println!("--- verifying the extra user holds no role memberships");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"writer",
			"-d",
			"postgres",
			"-tAc",
			"SELECT count(*) FROM pg_auth_members m \
			 JOIN pg_roles r ON r.oid = m.member WHERE r.rolname = 'writer'",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"0",
		"a freshly created extra user should be granted nothing, not even a read role"
	);

	println!("--- verifying the underscored user exists under its real name");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"report_writer",
			"-d",
			"postgres",
			"-tAc",
			"SELECT rolname FROM pg_roles WHERE rolname = 'report_writer'",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"report_writer",
		"the underscored role is created with its declared name"
	);

	println!("--- verifying the extra user's sessions are read-only");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"writer",
			"-d",
			"postgres",
			"-tAc",
			"SHOW default_transaction_read_only",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"on",
		"the extra user's sessions should start read-only"
	);

	println!("--- verifying the extra user cannot write");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"writer",
			"-d",
			"postgres",
			"-c",
			"CREATE SCHEMA test_writer",
		],
	)
	.await;
	assert!(
		out.contains("read-only transaction"),
		"the extra user must not be able to write, got: {out}"
	);

	println!("--- verifying the extra user cannot reach the public schema");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"writer",
			"-d",
			"postgres",
			"-tAc",
			"SELECT has_schema_privilege('writer', 'public', 'USAGE')",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"f",
		"the extra user must not inherit USAGE on public via PUBLIC"
	);

	println!("--- verifying the analytics user still reaches schemas");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"postgres",
			"-tAc",
			"SELECT has_schema_privilege('analytics', 'public', 'USAGE')",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"t",
		"closing public to PUBLIC must not cost the analytics user its access"
	);

	println!("--- verifying the extra user cannot read application data");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"writer",
			"-d",
			"postgres",
			"-c",
			"SELECT count(*) FROM _pgro.restore_info",
		],
	)
	.await;
	assert!(
		out.contains("permission denied"),
		"the extra user must hold no read grants of its own, got: {out}"
	);

	println!("--- verifying the analytics user is still read-only");
	let out = kubectl_exec(
		ns,
		&deploy_target,
		&[
			"psql",
			"-U",
			"analytics",
			"-d",
			"postgres",
			"-tAc",
			"SHOW default_transaction_read_only",
		],
	)
	.await;
	assert_eq!(
		out.trim(),
		"on",
		"provisioning extra users must leave the analytics user read-only"
	);

	println!("--- all assertions passed, cleaning up");
	cleanup_namespace(&client, ns, &["extra-users-replica"]).await;
}
