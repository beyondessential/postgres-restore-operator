use std::collections::BTreeMap;

use jiff::Span;
use k8s_openapi::api::core::v1::{
	Affinity, LocalObjectReference, NodeAffinity, NodeSelector, NodeSelectorRequirement,
	NodeSelectorTerm, ResourceRequirements, SecretReference,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use kube::ResourceExt;

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
fn restore_job_mounts_persistent_kopia_cache() {
	// Kopia's cache and logs used to land on the pod's writable layer at
	// /tmp/kopia, causing the restore pod to be evicted under
	// ephemeral-storage pressure (observed in production at ~10 GiB
	// usage). Mount a per-replica PVC at /tmp/kopia so the cache persists
	// across restores and doesn't count toward ephemeral-storage.
	let (restore, replica) = test_restore_and_replica();
	let job = build_restore_job(
		&restore,
		"test-restore-restore",
		"default",
		&replica,
		"kopia:latest",
	)
	.unwrap();
	let pod_spec = job.spec.unwrap().template.spec.unwrap();

	let volumes = pod_spec.volumes.as_ref().expect("pod must declare volumes");
	let cache_volume = volumes
		.iter()
		.find(|v| v.name == "kopia-cache")
		.expect("restore Job pod must include a kopia-cache volume");
	let claim_name = cache_volume
		.persistent_volume_claim
		.as_ref()
		.expect("kopia-cache volume must reference a PVC")
		.claim_name
		.as_str();
	assert_eq!(
		claim_name,
		super::builders::kopia_cache_pvc_name(&restore.spec.replica.name),
		"kopia-cache volume must reference the per-replica cache PVC"
	);

	let mounts = pod_spec.containers[0]
		.volume_mounts
		.as_ref()
		.expect("restore container must declare volume mounts");
	assert!(
		mounts
			.iter()
			.any(|m| m.name == "kopia-cache" && m.mount_path == "/tmp/kopia"),
		"restore container must mount kopia-cache at /tmp/kopia"
	);
}

#[test]
fn kopia_cache_pvc_owned_by_replica() {
	let (restore, replica) = test_restore_and_replica();
	let pvc =
		super::builders::build_kopia_cache_pvc(&replica, &restore.spec.snapshot_size, "default");

	let owner_refs = pvc
		.metadata
		.owner_references
		.as_ref()
		.expect("cache PVC must have owner references");
	assert_eq!(owner_refs.len(), 1);
	assert_eq!(owner_refs[0].kind, "PostgresPhysicalReplica");
	assert_eq!(owner_refs[0].name, replica.name_any());

	let access_modes = pvc
		.spec
		.as_ref()
		.unwrap()
		.access_modes
		.as_ref()
		.expect("cache PVC must declare access modes");
	assert_eq!(access_modes, &vec!["ReadWriteOnce".to_string()]);
}

#[test]
fn kopia_content_cache_mb_floor_for_small_pvc() {
	// 10Gi PVC (the floor for small snapshots) minus the 2Gi reserve =
	// 8 Gi content cache, expressed in MiB.
	let small = Quantity("1Gi".to_string());
	let mb = super::builders::kopia_content_cache_mb(&small);
	let expected = 10 * 1024 - super::builders::KOPIA_CACHE_RESERVE_MB;
	assert_eq!(mb, expected);
	assert!(
		mb >= super::builders::KOPIA_CONTENT_CACHE_FLOOR_MB,
		"content cache must always be at least the floor"
	);
}

#[test]
fn kopia_content_cache_mb_scales_with_snapshot() {
	// 100Gi snapshot → 20Gi PVC → 20Gi - 2Gi reserve = 18Gi cache.
	let big = Quantity("100Gi".to_string());
	let mb = super::builders::kopia_content_cache_mb(&big);
	let expected = 20 * 1024 - super::builders::KOPIA_CACHE_RESERVE_MB;
	assert_eq!(mb, expected);
}

#[test]
fn restore_job_passes_cache_caps_and_log_rotation() {
	// The restore Job's pod spec must set KOPIA_CONTENT_CACHE_MB and
	// KOPIA_METADATA_CACHE_MB so the embedded script can cap kopia's
	// caches, and the script must rotate CLI logs. Without these the
	// cache PVC fills up and every subsequent restore Job pod exits
	// in 1–2 minutes ("no space left on device").
	let (restore, replica) = test_restore_and_replica();
	let job = build_restore_job(
		&restore,
		"test-restore-restore",
		"default",
		&replica,
		"kopia:latest",
	)
	.unwrap();
	let pod_spec = job.spec.unwrap().template.spec.unwrap();
	let container = &pod_spec.containers[0];
	let env = container.env.as_ref().expect("container must declare env");
	let names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
	assert!(
		names.contains(&"KOPIA_CONTENT_CACHE_MB"),
		"restore Job env must include KOPIA_CONTENT_CACHE_MB"
	);
	assert!(
		names.contains(&"KOPIA_METADATA_CACHE_MB"),
		"restore Job env must include KOPIA_METADATA_CACHE_MB"
	);

	let script = &container.args.as_ref().unwrap()[0];
	assert!(
		script.contains("--content-cache-size-mb=\"$KOPIA_CONTENT_CACHE_MB\""),
		"connect command must cap content cache via env var"
	);
	assert!(
		script.contains("--metadata-cache-size-mb=\"$KOPIA_METADATA_CACHE_MB\""),
		"connect command must cap metadata cache"
	);
	assert!(
		script.contains("--log-dir-max-files=20"),
		"kopia invocations must rotate CLI logs by file count"
	);
	assert!(
		script.contains("--log-dir-max-age=24h"),
		"kopia invocations must rotate CLI logs by age"
	);
}

#[test]
fn cache_size_needs_grow_ratchet() {
	let small = Quantity("10Gi".to_string());
	let bigger = Quantity("20Gi".to_string());
	assert!(
		super::builders::cache_size_needs_grow(&small, &bigger),
		"must grow when desired > current"
	);
	assert!(
		!super::builders::cache_size_needs_grow(&bigger, &small),
		"must NOT shrink when desired < current"
	);
	assert!(
		!super::builders::cache_size_needs_grow(&small, &small),
		"equal sizes must not trigger a grow"
	);
}

#[test]
fn kopia_cache_pvc_size_floors_at_10gi() {
	// For a small snapshot, 20% would be sub-Gi; the floor catches that.
	let small = Quantity("1Gi".to_string());
	let size = super::builders::kopia_cache_pvc_size(&small);
	let parsed = kube_quantity::ParsedQuantity::try_from(size).unwrap();
	let floor = kube_quantity::ParsedQuantity::try_from("10Gi").unwrap();
	assert!(
		parsed >= floor,
		"sub-floor snapshot must clamp to at least 10Gi"
	);
}

#[test]
fn kopia_cache_pvc_size_scales_with_snapshot() {
	// For a 100Gi snapshot, 20% = 20Gi which is above the 10Gi floor.
	let big = Quantity("100Gi".to_string());
	let size = super::builders::kopia_cache_pvc_size(&big);
	let parsed = kube_quantity::ParsedQuantity::try_from(size).unwrap();
	let expected = kube_quantity::ParsedQuantity::try_from("20Gi").unwrap();
	let floor = kube_quantity::ParsedQuantity::try_from("10Gi").unwrap();
	assert!(parsed > floor, "100Gi snapshot must not hit the floor");
	assert_eq!(
		parsed, expected,
		"100Gi snapshot must produce exactly 20Gi cache"
	);
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
fn deployment_init_script_grants_superuser_for_read_write() {
	// Read-write restores grant SUPERUSER to the analytics user. The
	// granular pg_*_all_data + pg_maintain + CREATE ON DATABASE set was
	// tried and didn't cover DDL on existing schemas the user does not own
	// (CREATE TABLE in public on PG >= 15, dropping persistent_schemas
	// owned by other users on migration). Falling back to superuser keeps
	// the use cases working on every PG version.
	let (mut restore, mut replica) = test_restore_and_replica();
	replica.spec.read_only = false;
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("18".to_string()),
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
		script.contains("ALTER ROLE ${ANALYTICS_USERNAME} WITH SUPERUSER"),
		"read-write init script must grant superuser to analytics"
	);
	assert!(
		!script.contains("GRANT pg_write_all_data"),
		"read-write init script must not use the predefined pg_write_all_data role"
	);
}

#[test]
fn deployment_init_script_grants_read_only_on_pg14_plus() {
	// PG >= 14 read-only uses pg_read_all_data instead of superuser to keep
	// the surface area minimal. Below PG 14 the read-only path falls through
	// to the superuser branch (no predefined read role).
	let (mut restore, mut replica) = test_restore_and_replica();
	replica.spec.read_only = true;
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("18".to_string()),
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
		script.contains("GRANT pg_read_all_data TO ${ANALYTICS_USERNAME}"),
		"read-only init script must grant pg_read_all_data"
	);
}

#[test]
fn deployment_init_script_two_stage_pg_resetwal_fallback() {
	// When a snapshot is taken mid-online-backup the trailing WAL isn't
	// included, and postgres recovery fails with "WAL ends before end of
	// online backup" (or a similar signature). For an analytics replica
	// we prefer "comes up at the snapshotted state" over "permanently
	// stuck", so the init script runs `pg_resetwal -f` as a fallback.
	//
	// Two stages:
	//   - Detected WAL signature → short-circuit straight to pg_resetwal
	//     (retrying the same command won't help when recovery itself is
	//     blocking startup).
	//   - Undetected failure → retry once with the same settings (could
	//     be a transient I/O / catalog blip), then pg_resetwal as a
	//     last resort.
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	let deploy = build_deployment(&restore, "test-restore", "default", &replica).unwrap();
	let setup_auth = deploy
		.spec
		.unwrap()
		.template
		.spec
		.unwrap()
		.init_containers
		.unwrap()
		.into_iter()
		.find(|c| c.name == "setup-auth")
		.expect("setup-auth init container must exist");
	let script = setup_auth.args.unwrap().remove(0);

	// pg_resetwal appears multiple times — once in each fallback branch.
	let resetwal_calls = script.matches("pg_resetwal -f").count();
	assert!(
		resetwal_calls >= 2,
		"init script must reference pg_resetwal -f in both the detected and last-resort branches (saw {resetwal_calls})"
	);
	// Detection signatures.
	for sig in [
		"WAL ends before end of online backup",
		"invalid record length at",
		"database system was interrupted while in recovery",
	] {
		assert!(
			script.contains(sig),
			"fallback must detect WAL signature: {sig}"
		);
	}
	// Stage-2 retry message — verifies the script attempts the same
	// command once more before reset when no signature matched.
	assert!(
		script.contains("retrying once before falling back to pg_resetwal"),
		"init script must do a same-settings retry before the last-resort reset"
	);
	assert!(
		script.contains("last resort"),
		"init script must label the second pg_resetwal as a last resort"
	);
}

#[test]
fn deployment_init_script_overrides_listen_addresses() {
	// Some source backups carry `listen_addresses = 'localhost'` in
	// postgresql.conf, which restricts the restored postgres to localhost
	// only and breaks operator → restore connections (e.g. schema
	// migration discovery). Strip the source value and append our own '*'
	// so the restored pod is reachable on the pod IP.
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
		script.contains("listen_addresses[[:space:]]*="),
		"sed must strip listen_addresses from source config"
	);
	assert!(
		script.contains(r#"listen_addresses = '*'"#),
		"init script must append listen_addresses = '*' override"
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
