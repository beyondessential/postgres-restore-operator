use jiff::Span;
use k8s_openapi::{
	api::{
		apps::v1::Deployment,
		core::v1::{
			Affinity, LocalObjectReference, NodeAffinity, NodeSelector, NodeSelectorRequirement,
			NodeSelectorTerm, SecretReference,
		},
	},
	apimachinery::pkg::api::resource::Quantity,
};

use super::builders::build_deployment;
use crate::{types::*, util::TimeSpan};

fn make_replica(extra_config: Option<String>) -> PostgresPhysicalReplica {
	make_replica_with_opts(extra_config, true)
}

fn make_replica_with_opts(
	extra_config: Option<String>,
	read_only: bool,
) -> PostgresPhysicalReplica {
	PostgresPhysicalReplica::new(
		"test-replica",
		PostgresPhysicalReplicaSpec {
			kopia_secret_ref: SecretReference {
				name: Some("kopia-secret".to_string()),
				namespace: None,
			},
			snapshot_filter: None,
			schedule: "0 */6 * * *".into(),
			schedule_jitter: TimeSpan(Span::new().minutes(10)),
			minimum_ttl: None,
			switchover_grace_period: TimeSpan(Span::new().minutes(5)),
			analytics_username: "analytics".to_string(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			service_annotations: None,
			pod_annotations: None,
			affinity: None,
			tolerations: vec![],
			read_only,
			postgres_extra_config: extra_config,
			notifications: vec![],
			overlay_database: None,
		},
	)
}

fn make_restore() -> PostgresPhysicalRestore {
	let mut restore = PostgresPhysicalRestore::new(
		"test-restore",
		PostgresPhysicalRestoreSpec {
			replica: LocalObjectReference {
				name: "test-replica".to_string(),
			},
			snapshot: "snap123".to_string(),
			snapshot_size: Quantity("10Gi".to_string()),
			storage_size: Quantity("11Gi".to_string()),
		},
	);
	restore.metadata.uid = Some("uid-123".to_string());
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	restore
}

fn get_init_script(deploy: &Deployment) -> String {
	deploy
		.spec
		.as_ref()
		.unwrap()
		.template
		.spec
		.as_ref()
		.unwrap()
		.init_containers
		.as_ref()
		.unwrap()[0]
		.args
		.as_ref()
		.unwrap()[0]
		.clone()
}

#[test]
fn minimal_config_includes_max_prepared_transactions() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains("max_prepared_transactions = 16"),
		"minimal config must include max_prepared_transactions"
	);
}

#[test]
fn minimal_config_created_only_when_missing() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains(r#"if [ ! -f "$PGDATA/postgresql.conf" ]"#),
		"minimal config should only be created when file is missing"
	);
}

#[test]
fn extra_config_appended_to_postgresql_conf() {
	let replica = make_replica(Some(
		"shared_preload_libraries = 'timescaledb'\nwork_mem = '64MB'".into(),
	));
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains("Appending extra postgresql.conf"),
		"script must contain the extra config echo"
	);
	assert!(
		script.contains("shared_preload_libraries = 'timescaledb'"),
		"script must contain the user-provided shared_preload_libraries"
	);
	assert!(
		script.contains("work_mem = '64MB'"),
		"script must contain the user-provided work_mem"
	);
}

#[test]
fn no_extra_config_block_when_none() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		!script.contains("EXTRACONFEOF"),
		"script must not contain extra config heredoc when postgres_extra_config is None"
	);
}

#[test]
fn extra_config_appended_unconditionally() {
	let replica = make_replica(Some(
		"shared_preload_libraries = 'pg_stat_statements'".into(),
	));
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);

	// The extra config block must be outside the `if [ ! -f ... ]` guard
	// so it's appended even when postgresql.conf already exists in the snapshot
	let conf_fi = script.find("CONFEOF\nfi").expect("must have CONFEOF/fi");
	let extra_pos = script.find("EXTRACONFEOF").expect("must have EXTRACONFEOF");
	assert!(
		extra_pos > conf_fi,
		"extra config block must appear after the minimal-config if/fi block"
	);
}

#[test]
fn pg_ident_conf_created_when_missing() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains(r#"if [ ! -f "$PGDATA/pg_ident.conf" ]"#),
		"script must create pg_ident.conf when missing"
	);
	assert!(
		script.contains(r#"touch "$PGDATA/pg_ident.conf""#),
		"script must touch pg_ident.conf"
	);
}

#[test]
fn local_auth_uses_trust() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains("local   all             all                                     trust"),
		"local connections must use trust auth (peer fails with UID 999)"
	);
	assert!(
		!script.contains("peer\n"),
		"pg_hba.conf must not use peer as an auth method"
	);
}

#[test]
fn strips_source_host_config_overrides() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains("Stripping source-host config overrides"),
		"script must strip source-host overrides"
	);
	for directive in ["hba_file", "ident_file", "data_directory"] {
		assert!(
			script.contains(directive),
			"script must strip {directive} from postgresql.conf"
		);
	}
}

#[test]
fn read_only_mode_uses_pg_read_all_data_branch() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains("pg_read_all_data"),
		"read-only mode must grant pg_read_all_data"
	);
	assert!(
		script.contains(r#"[ "$PG_MAJOR" -ge 14 ]"#),
		"script must check version gate for pg_read_all_data"
	);
	assert!(
		script.contains(r#"if [ "true" = "true" ]"#),
		"read-only mode must check read_only flag"
	);
	assert!(
		script.contains("Read-only mode with PG >= 14, granted pg_read_all_data"),
		"read-only mode must echo read-only confirmation"
	);
}

#[test]
fn not_read_only_uses_superuser_branch() {
	let replica = make_replica_with_opts(None, false);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains(r#"if [ "false" = "true" ]"#),
		"non-read-only mode must have false condition so it falls through to write grants"
	);
	assert!(
		script.contains("SUPERUSER"),
		"script must contain superuser grant in PG < 14 fallback branch"
	);
}

#[test]
fn not_read_only_grants_write_and_create_schema() {
	let replica = make_replica_with_opts(None, false);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains("pg_write_all_data"),
		"non-read-only mode must grant pg_write_all_data"
	);
	assert!(
		script.contains("GRANT CREATE ON DATABASE"),
		"non-read-only mode must grant CREATE ON DATABASE"
	);
}

#[test]
fn superuser_branch_detects_pg_version() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains(r#"PG_MAJOR=$(cat "$PGDATA/PG_VERSION")"#),
		"script must read PG major version"
	);
	assert!(
		script.contains(r#"[ "$PG_MAJOR" -ge 14 ]"#),
		"script must check for PG >= 14"
	);
}

#[test]
fn read_only_appended_after_extra_config() {
	let replica = make_replica(Some("shared_preload_libraries = 'timescaledb'".into()));
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);

	let extra_pos = script.find("EXTRACONFEOF").expect("must have EXTRACONFEOF");
	let read_only_pos = script
		.find("default_transaction_read_only")
		.expect("must have read_only setting");
	assert!(
		read_only_pos > extra_pos,
		"read_only setting must come after extra config so it can't be overridden"
	);
}

#[test]
fn fdw_user_created_when_overlay_configured() {
	let mut replica = make_replica(None);
	replica.spec.overlay_database = Some(OverlayDatabaseConfig {
		postgres_version: None,
		image_catalog: None,
		storage_size_override: None,
		storage_class: None,
		resources: None,
		affinity: None,
		tolerations: vec![],
		service_annotations: None,
		schema_mapping: None,
		import_generated: false,
	});
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		script.contains("Creating FDW read-only user"),
		"init script must create FDW user when overlay is configured"
	);
	assert!(
		script.contains("FDW_USERNAME"),
		"init script must reference FDW_USERNAME env"
	);
}

#[test]
fn fdw_user_not_created_without_overlay() {
	let replica = make_replica(None);
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let script = get_init_script(&deploy);
	assert!(
		!script.contains("Creating FDW read-only user"),
		"init script must not create FDW user when overlay is not configured"
	);
}

#[test]
fn deployment_uses_affinity_not_node_selector() {
	let mut replica = make_replica(None);
	replica.spec.affinity = Some(Affinity {
		node_affinity: Some(NodeAffinity {
			required_during_scheduling_ignored_during_execution: Some(NodeSelector {
				node_selector_terms: vec![NodeSelectorTerm {
					match_expressions: Some(vec![NodeSelectorRequirement {
						key: "kubernetes.io/os".to_string(),
						operator: "In".to_string(),
						values: Some(vec!["linux".to_string()]),
					}]),
					..Default::default()
				}],
			}),
			..Default::default()
		}),
		pod_affinity: None,
		pod_anti_affinity: None,
	});
	let restore = make_restore();
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let pod_spec = deploy
		.spec
		.as_ref()
		.unwrap()
		.template
		.spec
		.as_ref()
		.unwrap();
	assert!(
		pod_spec.affinity.is_some(),
		"pod spec must have affinity set"
	);
	assert!(
		pod_spec.node_selector.is_none(),
		"pod spec must not have node_selector"
	);
}
