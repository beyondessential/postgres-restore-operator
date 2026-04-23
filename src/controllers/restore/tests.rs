use std::collections::BTreeMap;

use jiff::Span;
use k8s_openapi::api::core::v1::{
	Affinity, LocalObjectReference, NodeAffinity, NodeSelector, NodeSelectorRequirement,
	NodeSelectorTerm, ResourceRequirements, SecretReference,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use super::builders::{build_deployment, build_restore_job, build_version_detect_job};
use crate::{types::*, util::TimeSpan};

#[test]
fn deployment_uses_affinity_not_node_selector() {
	let mut replica = PostgresPhysicalReplica::new(
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
			read_only: true,
			postgres_extra_config: None,
			notifications: vec![],

			persistent_schemas: None,
			storage_size_maximum: Quantity("2Ti".to_string()),
		},
	);
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

	let mut restore = PostgresPhysicalRestore::new(
		"test-restore",
		PostgresPhysicalRestoreSpec {
			replica: LocalObjectReference {
				name: "test-replica".to_string(),
			},
			snapshot: "snap123".to_string(),
			snapshot_size: Quantity("10Gi".to_string()),
			snapshot_time: None,
			storage_size: Quantity("11Gi".to_string()),
		},
	);
	restore.metadata.uid = Some("uid-123".to_string());
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});

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

fn test_restore_and_replica() -> (PostgresPhysicalRestore, PostgresPhysicalReplica) {
	let replica = PostgresPhysicalReplica::new(
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
			read_only: true,
			postgres_extra_config: None,
			notifications: vec![],

			persistent_schemas: None,
			storage_size_maximum: Quantity("2Ti".to_string()),
		},
	);

	let mut restore = PostgresPhysicalRestore::new(
		"test-restore",
		PostgresPhysicalRestoreSpec {
			replica: LocalObjectReference {
				name: "test-replica".to_string(),
			},
			snapshot: "snap123".to_string(),
			snapshot_size: Quantity("10Gi".to_string()),
			snapshot_time: None,
			storage_size: Quantity("11Gi".to_string()),
		},
	);
	restore.metadata.uid = Some("uid-123".to_string());

	(restore, replica)
}

#[test]
fn restore_job_has_ttl_seconds_after_finished() {
	let (restore, replica) = test_restore_and_replica();
	let job = build_restore_job(
		&restore,
		"test-restore-restore",
		"default",
		&replica,
		"kopia:latest",
	)
	.unwrap();
	let ttl = job
		.spec
		.as_ref()
		.unwrap()
		.ttl_seconds_after_finished
		.expect("restore job must set ttlSecondsAfterFinished");
	assert!(ttl > 0, "ttlSecondsAfterFinished must be positive");
}

#[test]
fn version_detect_job_has_ttl_seconds_after_finished() {
	let (restore, _replica) = test_restore_and_replica();
	let job = build_version_detect_job(&restore, "test-version-detect", "default", "test-pvc");
	let ttl = job
		.spec
		.as_ref()
		.unwrap()
		.ttl_seconds_after_finished
		.expect("version-detect job must set ttlSecondsAfterFinished");
	assert!(ttl > 0, "ttlSecondsAfterFinished must be positive");
}

#[test]
fn deployment_has_dshm_volume_with_default_resources() {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});

	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let pod_spec = deploy.spec.unwrap().template.spec.unwrap();

	let dshm_vol = pod_spec
		.volumes
		.as_ref()
		.unwrap()
		.iter()
		.find(|v| v.name == "dshm")
		.expect("dshm volume must exist");

	let empty_dir = dshm_vol.empty_dir.as_ref().expect("dshm must be emptyDir");
	assert_eq!(empty_dir.medium.as_deref(), Some("Memory"));
	// Default 1Gi request: min(512Mi, ceil(36% of 1Gi)) = min(512Mi, 369Mi) = 369Mi
	assert_eq!(empty_dir.size_limit.as_ref().unwrap().0, "369Mi");
}

#[test]
fn deployment_has_dshm_volume_with_custom_resources() {
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	replica.spec.resources = Some(ResourceRequirements {
		requests: Some(BTreeMap::from([(
			"memory".to_string(),
			Quantity("4Gi".to_string()),
		)])),
		limits: Some(BTreeMap::from([(
			"memory".to_string(),
			Quantity("8Gi".to_string()),
		)])),
		..Default::default()
	});

	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let pod_spec = deploy.spec.unwrap().template.spec.unwrap();

	let dshm_vol = pod_spec
		.volumes
		.as_ref()
		.unwrap()
		.iter()
		.find(|v| v.name == "dshm")
		.expect("dshm volume must exist");

	let empty_dir = dshm_vol.empty_dir.as_ref().expect("dshm must be emptyDir");
	assert_eq!(empty_dir.medium.as_deref(), Some("Memory"));
	// 4Gi request, 8Gi limit: min(4Gi/2, ceil(36% of 8Gi)) = min(2048Mi, 2950Mi) = 2048Mi
	assert_eq!(empty_dir.size_limit.as_ref().unwrap().0, "2048Mi");
}

#[test]
fn deployment_mounts_dshm_on_postgres_and_setup_auth() {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});

	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let pod_spec = deploy.spec.unwrap().template.spec.unwrap();

	let postgres = &pod_spec.containers[0];
	assert_eq!(postgres.name, "postgres");
	let pg_mounts = postgres.volume_mounts.as_ref().unwrap();
	let pg_shm = pg_mounts
		.iter()
		.find(|m| m.mount_path == "/dev/shm")
		.expect("postgres container must mount /dev/shm");
	assert_eq!(pg_shm.name, "dshm");

	let setup_auth = pod_spec
		.init_containers
		.as_ref()
		.unwrap()
		.iter()
		.find(|c| c.name == "setup-auth")
		.expect("setup-auth init container must exist");
	let sa_mounts = setup_auth.volume_mounts.as_ref().unwrap();
	assert!(
		!sa_mounts.iter().any(|m| m.mount_path == "/dev/shm"),
		"setup-auth must NOT mount /dev/shm (it never uses shared memory)"
	);
}

#[test]
fn deployment_init_script_sets_shared_buffers() {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});

	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let pod_spec = deploy.spec.unwrap().template.spec.unwrap();

	let setup_auth = pod_spec
		.init_containers
		.as_ref()
		.unwrap()
		.iter()
		.find(|c| c.name == "setup-auth")
		.expect("setup-auth init container must exist");
	let script = &setup_auth.args.as_ref().unwrap()[0];

	// Default 1Gi: SHM=369Mi, shared_buffers = floor(70% of 369) = 258MB
	assert!(
		script.contains("shared_buffers = 258MB"),
		"init script must set shared_buffers to computed value, got script containing: {}",
		script
			.lines()
			.find(|l| l.contains("shared_buffers"))
			.unwrap_or("<not found>")
	);

	// The sed block must strip shared_buffers from source config
	assert!(
		script.contains("shared_buffers[[:space:]]*="),
		"sed must strip shared_buffers from source config"
	);
}

#[test]
fn init_script_sets_initial_stage_based_on_reindex_flag() {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});

	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let pod_spec = deploy.spec.unwrap().template.spec.unwrap();

	let setup_auth = pod_spec
		.init_containers
		.as_ref()
		.unwrap()
		.iter()
		.find(|c| c.name == "setup-auth")
		.expect("setup-auth init container must exist");
	let script = &setup_auth.args.as_ref().unwrap()[0];

	assert!(
		script.contains("stage text NOT NULL DEFAULT 'restored'"),
		"table must include stage column"
	);
	assert!(
		script.contains("last_transition_time timestamptz NOT NULL DEFAULT now()"),
		"table must include last_transition_time column"
	);
	assert!(
		script.contains("ADD COLUMN IF NOT EXISTS stage"),
		"must add stage column for existing tables carried in snapshot"
	);
	assert!(
		script.contains("ADD COLUMN IF NOT EXISTS last_transition_time"),
		"must add last_transition_time column for existing tables carried in snapshot"
	);
	assert!(
		script.contains("PGRO_STAGE=restored") && script.contains("PGRO_STAGE=ready"),
		"init must pick stage based on needs-reindex flag"
	);
	assert!(
		script.contains("'${PGRO_STAGE}'"),
		"insert must use the chosen stage"
	);
}

#[test]
fn postgres_container_updates_stage_around_reindex() {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});

	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let pod_spec = deploy.spec.unwrap().template.spec.unwrap();

	let postgres = pod_spec
		.containers
		.iter()
		.find(|c| c.name == "postgres")
		.expect("postgres container must exist");
	let script = &postgres.args.as_ref().unwrap()[0];

	let reindexing_pos = script
		.find("stage = 'reindexing'")
		.expect("must update stage to reindexing before the REINDEX loop");
	let ready_pos = script
		.find("stage = 'ready'")
		.expect("must update stage to ready after the REINDEX loop");
	assert!(
		reindexing_pos < ready_pos,
		"reindexing update must come before ready update"
	);
	assert!(
		reindexing_pos < script.find("REINDEX INDEX").unwrap(),
		"reindexing update must come before any REINDEX call"
	);
	assert!(
		script[..ready_pos].contains("rm -f /pgdata/needs-reindex"),
		"ready update must happen after the needs-reindex flag is cleared"
	);
}

#[test]
fn deployment_shared_buffers_with_custom_resources() {
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	replica.spec.resources = Some(ResourceRequirements {
		requests: Some(BTreeMap::from([(
			"memory".to_string(),
			Quantity("2Gi".to_string()),
		)])),
		..Default::default()
	});

	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let pod_spec = deploy.spec.unwrap().template.spec.unwrap();

	let setup_auth = pod_spec
		.init_containers
		.as_ref()
		.unwrap()
		.iter()
		.find(|c| c.name == "setup-auth")
		.expect("setup-auth init container must exist");
	let script = &setup_auth.args.as_ref().unwrap()[0];

	// 2Gi request: SHM=738Mi, shared_buffers = floor(70% of 738) = 516MB
	assert!(
		script.contains("shared_buffers = 516MB"),
		"init script must set shared_buffers for 2Gi request"
	);
}
