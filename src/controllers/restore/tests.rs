use std::collections::BTreeMap;

use jiff::Span;
use k8s_openapi::api::core::v1::{
	Affinity, LocalObjectReference, NodeAffinity, NodeSelector, NodeSelectorRequirement,
	NodeSelectorTerm, ResourceRequirements, SecretReference,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use kube::ResourceExt;

use super::builders::{
	build_deployment, build_restore_job, build_version_detect_job, resolve_postgres_resources,
};
use crate::{placement::PodPlacement, types::*, util::TimeSpan};

#[test]
fn deployment_uses_affinity_not_node_selector() {
	let mut replica = PostgresPhysicalReplica::new(
		"test-replica",
		PostgresPhysicalReplicaSpec {
			migrate_to: None,
			kopia_secret_ref: Some(SecretReference {
				name: Some("kopia-secret".to_string()),
				namespace: None,
			}),
			canopy_source: None,
			snapshot_filter: None,
			schedule: "0 */6 * * *".into(),
			schedule_jitter: TimeSpan(Span::new().minutes(10)),
			minimum_ttl: None,
			switchover_grace_period: TimeSpan(Span::new().minutes(5)),
			analytics_username: "analytics".to_string(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			resources_floor: None,
			resources_maximum: None,
			deployment_ready_timeout: None,
			shm_size_floor: None,
			service_annotations: None,
			pod_annotations: None,
			affinity: None,
			tolerations: vec![],
			read_only: true,
			ephemeral: false,
			postgres_extra_config: None,
			notifications: vec![],

			persistent_schemas: None,
			redaction: None,
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
			migrate_to: None,
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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
			migrate_to: None,
			kopia_secret_ref: Some(SecretReference {
				name: Some("kopia-secret".to_string()),
				namespace: None,
			}),
			canopy_source: None,
			snapshot_filter: None,
			schedule: "0 */6 * * *".into(),
			schedule_jitter: TimeSpan(Span::new().minutes(10)),
			minimum_ttl: None,
			switchover_grace_period: TimeSpan(Span::new().minutes(5)),
			analytics_username: "analytics".to_string(),
			storage_class: None,
			storage_size_override: None,
			resources: None,
			resources_floor: None,
			resources_maximum: None,
			deployment_ready_timeout: None,
			shm_size_floor: None,
			service_annotations: None,
			pod_annotations: None,
			affinity: None,
			tolerations: vec![],
			read_only: true,
			ephemeral: false,
			postgres_extra_config: None,
			notifications: vec![],

			persistent_schemas: None,
			redaction: None,
			storage_size_maximum: Quantity("2Ti".to_string()),
		},
	);

	let mut restore = PostgresPhysicalRestore::new(
		"test-restore",
		PostgresPhysicalRestoreSpec {
			migrate_to: None,
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

/// The `setup-auth` init container's inline script, which carries the locale,
/// WAL and reindex fix steps.
fn setup_auth_script() -> String {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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
	setup_auth.args.unwrap().remove(0)
}

/// The `postgres` container's inline script, which carries the background
/// reindex hook and its stage bookkeeping.
fn postgres_container_script() -> String {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
	let postgres = deploy
		.spec
		.unwrap()
		.template
		.spec
		.unwrap()
		.containers
		.into_iter()
		.find(|c| c.name == "postgres")
		.expect("postgres container must exist");
	postgres.args.unwrap().remove(0)
}

/// Memory in bytes. Compared numerically because the derived value is ceiled
/// to whole Mi, so `2Gi` legitimately comes back as `2048Mi`.
fn mem(r: &ResourceRequirements, which: &str) -> u64 {
	let map = match which {
		"limits" => r.limits.as_ref(),
		_ => r.requests.as_ref(),
	};
	let q = map.unwrap().get("memory").unwrap().clone();
	parsed_bytes(&q)
}

fn gib(n: u64) -> u64 {
	n * (1 << 30)
}

fn cpu(r: &ResourceRequirements, which: &str) -> Option<String> {
	let map = match which {
		"limits" => r.limits.as_ref(),
		_ => r.requests.as_ref(),
	};
	map.and_then(|m| m.get("cpu")).map(|q| q.0.clone())
}

/// A pinned `resources` is the operator's explicit intent (or a canopy
/// parameter) and must beat anything derived from the snapshot.
#[test]
fn pinned_resources_win_over_the_derived_value() {
	let (restore, mut replica) = test_restore_and_replica();
	replica.spec.resources = Some(ResourceRequirements {
		limits: Some(BTreeMap::from([(
			"memory".to_string(),
			Quantity("3Gi".to_string()),
		)])),
		..Default::default()
	});
	replica.spec.resources_floor = Some(ResourceRequirements {
		limits: Some(BTreeMap::from([(
			"memory".to_string(),
			Quantity("8Gi".to_string()),
		)])),
		..Default::default()
	});

	let resolved =
		resolve_postgres_resources(&replica, &restore.spec.snapshot_size).expect("pinned");
	assert_eq!(mem(&resolved, "limits"), gib(3));
}

/// With nothing pinned the memory comes from the snapshot, while CPU is
/// carried over from the floor — CPU tracks query concurrency, not data size.
#[test]
fn derived_memory_scales_but_cpu_comes_from_the_floor() {
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.spec.snapshot_size = Quantity("200Gi".to_string());
	replica.spec.resources = None;
	replica.spec.resources_floor = Some(ResourceRequirements {
		requests: Some(BTreeMap::from([
			("cpu".to_string(), Quantity("500m".to_string())),
			("memory".to_string(), Quantity("2Gi".to_string())),
		])),
		limits: Some(BTreeMap::from([
			("cpu".to_string(), Quantity("4".to_string())),
			("memory".to_string(), Quantity("8Gi".to_string())),
		])),
		..Default::default()
	});

	let resolved =
		resolve_postgres_resources(&replica, &restore.spec.snapshot_size).expect("derived");

	assert_eq!(mem(&resolved, "limits"), gib(20), "10% of a 200Gi snapshot");
	assert_eq!(cpu(&resolved, "limits").as_deref(), Some("4"));
	assert_eq!(cpu(&resolved, "requests").as_deref(), Some("500m"));
}

/// A small replica must not inherit a large replica's reservation.
#[test]
fn derived_memory_falls_back_to_the_floor_for_a_small_snapshot() {
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.spec.snapshot_size = Quantity("300Mi".to_string());
	replica.spec.resources = None;
	replica.spec.resources_floor = Some(ResourceRequirements {
		limits: Some(BTreeMap::from([(
			"memory".to_string(),
			Quantity("2Gi".to_string()),
		)])),
		..Default::default()
	});

	let resolved =
		resolve_postgres_resources(&replica, &restore.spec.snapshot_size).expect("derived");
	assert_eq!(mem(&resolved, "limits"), gib(2));
}

/// The derived memory request must equal the limit. Requests are the only
/// figure the cluster acts on — instance selection, bin-packing, consolidation,
/// eviction order — so understating one puts the pod on a node that cannot
/// satisfy its own limit and marks the node as spare capacity.
#[test]
fn derived_memory_request_equals_limit() {
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.spec.snapshot_size = Quantity("200Gi".to_string());
	replica.spec.resources = None;
	replica.spec.resources_floor = Some(ResourceRequirements {
		limits: Some(BTreeMap::from([(
			"memory".to_string(),
			Quantity("8Gi".to_string()),
		)])),
		..Default::default()
	});

	let resolved =
		resolve_postgres_resources(&replica, &restore.spec.snapshot_size).expect("derived");
	assert_eq!(mem(&resolved, "limits"), gib(20));
	assert_eq!(mem(&resolved, "requests"), gib(20));
}

/// A floor that omits the CPU limit must produce resources with no CPU limit.
/// `resolve_postgres_resources` copies CPU from the floor's requests and limits
/// independently, so this holds without special-casing — but it's load-bearing
/// for the analytics intent and implicit in the merge, so pin it.
#[test]
fn absent_cpu_limit_in_the_floor_stays_absent() {
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.spec.snapshot_size = Quantity("200Gi".to_string());
	replica.spec.resources = None;
	replica.spec.resources_floor = Some(ResourceRequirements {
		requests: Some(BTreeMap::from([
			("cpu".to_string(), Quantity("2".to_string())),
			("memory".to_string(), Quantity("2Gi".to_string())),
		])),
		limits: Some(BTreeMap::from([(
			"memory".to_string(),
			Quantity("8Gi".to_string()),
		)])),
		..Default::default()
	});

	let resolved =
		resolve_postgres_resources(&replica, &restore.spec.snapshot_size).expect("derived");

	assert_eq!(cpu(&resolved, "requests").as_deref(), Some("2"));
	assert_eq!(
		cpu(&resolved, "limits"),
		None,
		"a floor without a CPU limit must not grow one"
	);
	assert_eq!(mem(&resolved, "limits"), gib(20));
}

#[test]
fn canopy_restore_job_proxy_is_native_sidecar() {
	// Regression: the canopy-proxy must be an init container with
	// restartPolicy=Always (a native sidecar), NOT a plain container. As a
	// plain container it keeps the Pod Running after the main `restore`
	// container exits, so the Job never succeeds and eventually hits
	// activeDeadlineSeconds → DeadlineExceeded.
	let (restore, mut replica) = test_restore_and_replica();
	replica.spec.kopia_secret_ref = None;
	replica.spec.canopy_source = Some(CanopySource {
		group: "11111111-1111-1111-1111-111111111111".to_string(),
		r#type: "tamanu-postgres".to_string(),
	});
	let proxy = super::builders::CanopyProxyArgs {
		image: "ghcr.io/beyondessential/postgres-restore-operator:latest",
		broker_base_url: "http://operator.pgro-system.svc:9091",
		stats_callback_url: "http://operator.pgro-system.svc:8080/api/v1/canopy-stats/ns/job",
		progress_callback_url: None,
		run_id: Some("44444444-4444-4444-4444-444444444444"),
	};
	let job = build_restore_job(
		&restore,
		"test-restore-restore",
		"default",
		&replica,
		"kopia:latest",
		"http://operator/api/v1/cache-pressure/default/test-restore",
		Some(&proxy),
		&PodPlacement::default(),
	)
	.unwrap();
	let pod_spec = job.spec.unwrap().template.spec.unwrap();

	// The proxy is NOT a regular container.
	assert!(
		!pod_spec.containers.iter().any(|c| c.name == "canopy-proxy"),
		"canopy-proxy must not be a plain container (Pod would never complete)"
	);
	// It IS an init container with restartPolicy=Always.
	let init = pod_spec
		.init_containers
		.as_ref()
		.expect("canopy path must declare init containers");
	let sidecar = init
		.iter()
		.find(|c| c.name == "canopy-proxy")
		.expect("canopy-proxy must be a native sidecar (init container)");
	assert_eq!(
		sidecar.restart_policy.as_deref(),
		Some("Always"),
		"native sidecar must set restartPolicy=Always"
	);
	assert!(
		pod_spec.termination_grace_period_seconds.unwrap_or(0) > 0,
		"canopy Pod must allow grace time for the sidecar to flush stats on SIGTERM"
	);
}

#[test]
fn canopy_restore_job_sidecar_carries_run_id() {
	// The run_id passed in CanopyProxyArgs must reach the sidecar as
	// PGRO_RUN_ID so its credential requests are attributed to the run; when
	// no run_id is given (non-run credential consumers) the env is absent.
	let (restore, mut replica) = test_restore_and_replica();
	replica.spec.kopia_secret_ref = None;
	replica.spec.canopy_source = Some(CanopySource {
		group: "11111111-1111-1111-1111-111111111111".to_string(),
		r#type: "tamanu-postgres".to_string(),
	});

	let run_id_env = |run_id: Option<&str>| {
		let proxy = super::builders::CanopyProxyArgs {
			image: "ghcr.io/beyondessential/postgres-restore-operator:latest",
			broker_base_url: "http://operator.pgro-system.svc:9091",
			stats_callback_url: "http://operator.pgro-system.svc:8080/api/v1/canopy-stats/ns/job",
			progress_callback_url: None,
			run_id,
		};
		let job = build_restore_job(
			&restore,
			"test-restore-restore",
			"default",
			&replica,
			"kopia:latest",
			"http://operator/api/v1/cache-pressure/default/test-restore",
			Some(&proxy),
			&PodPlacement::default(),
		)
		.unwrap();
		let sidecar = job
			.spec
			.unwrap()
			.template
			.spec
			.unwrap()
			.init_containers
			.unwrap()
			.into_iter()
			.find(|c| c.name == "canopy-proxy")
			.expect("canopy-proxy sidecar");
		sidecar
			.env
			.unwrap_or_default()
			.into_iter()
			.find(|e| e.name == "PGRO_RUN_ID")
			.and_then(|e| e.value)
	};

	assert_eq!(
		run_id_env(Some("44444444-4444-4444-4444-444444444444")).as_deref(),
		Some("44444444-4444-4444-4444-444444444444"),
	);
	assert_eq!(
		run_id_env(None),
		None,
		"no run_id → no PGRO_RUN_ID env on the sidecar"
	);
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
		"http://operator/api/v1/cache-pressure/default/test-restore",
		None,
		&PodPlacement::default(),
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
		"http://operator/api/v1/cache-pressure/default/test-restore",
		None,
		&PodPlacement::default(),
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
	// 10Gi PVC (the floor for small snapshots): reserve = max(2Gi,
	// 30% × 10Gi) = 3Gi → content cache = 7Gi.
	let small = Quantity("1Gi".to_string());
	let mb = super::builders::kopia_content_cache_mb(&small);
	let pvc_mb = 10 * 1024;
	let proportional = (pvc_mb as f64 * super::builders::KOPIA_CACHE_RESERVE_FRACTION) as u64;
	let reserve = proportional.max(super::builders::KOPIA_CACHE_RESERVE_MIN_MB);
	let expected = pvc_mb - reserve;
	assert_eq!(mb, expected);
	assert!(
		mb >= super::builders::KOPIA_CONTENT_CACHE_FLOOR_MB,
		"content cache must always be at least the floor"
	);
}

#[test]
fn kopia_content_cache_mb_scales_with_snapshot() {
	// 100Gi snapshot → 20Gi PVC. Reserve = max(2Gi, 30% × 20Gi) = 6Gi
	// → content cache = 14Gi.
	let big = Quantity("100Gi".to_string());
	let mb = super::builders::kopia_content_cache_mb(&big);
	let pvc_mb = 20 * 1024;
	let proportional = (pvc_mb as f64 * super::builders::KOPIA_CACHE_RESERVE_FRACTION) as u64;
	let reserve = proportional.max(super::builders::KOPIA_CACHE_RESERVE_MIN_MB);
	let expected = pvc_mb - reserve;
	assert_eq!(mb, expected);
}

#[test]
fn next_cache_pvc_size_after_pressure_bumps_and_caps() {
	// 10Gi PVC → first bump → 11.5Gi. Eventually caps at 2×10Gi = 20Gi.
	let snapshot = Quantity("1Gi".to_string());
	let mut size = Quantity("10Gi".to_string());
	let original_bytes = parsed_bytes(&size);
	let max_bytes = parsed_bytes(&super::builders::kopia_cache_pvc_max(&snapshot));
	assert_eq!(max_bytes, original_bytes * 2);

	let first = super::builders::next_cache_pvc_size_after_pressure(&size, &snapshot);
	let first_bytes = parsed_bytes(&first);
	assert!(first_bytes > original_bytes, "first bump must grow");
	assert!(first_bytes <= max_bytes, "first bump must not exceed cap");

	// Loop until we hit the cap. Should take a bounded number of steps.
	for _ in 0..50 {
		size = super::builders::next_cache_pvc_size_after_pressure(&size, &snapshot);
		if parsed_bytes(&size) >= max_bytes {
			break;
		}
	}
	assert!(
		parsed_bytes(&size) >= max_bytes,
		"repeated bumps must eventually reach the cap; got {}",
		size.0
	);

	// Once at the cap, further bumps don't push past it.
	let after_cap = super::builders::next_cache_pvc_size_after_pressure(&size, &snapshot);
	assert!(
		parsed_bytes(&after_cap) <= max_bytes,
		"at-cap bump must not exceed cap"
	);
}

fn parsed_bytes(q: &Quantity) -> u64 {
	kube_quantity::ParsedQuantity::try_from(q.clone())
		.ok()
		.and_then(|p| p.to_bytes_f64())
		.map(|b| b as u64)
		.unwrap_or(0)
}

/// The sidecar can only sample progress if the Job tells it where to post.
#[test]
fn canopy_restore_job_sidecar_carries_progress_callback_url() {
	let (restore, mut replica) = test_restore_and_replica();
	replica.spec.canopy_source = Some(CanopySource {
		group: "11111111-1111-1111-1111-111111111111".to_string(),
		r#type: "tamanu-postgres".to_string(),
	});
	let url = "http://operator.pgro-system.svc:8080/api/v1/canopy-progress/ns/job";
	let proxy = super::builders::CanopyProxyArgs {
		image: "ghcr.io/beyondessential/postgres-restore-operator:latest",
		broker_base_url: "http://operator.pgro-system.svc:9091",
		stats_callback_url: "http://operator.pgro-system.svc:8080/api/v1/canopy-stats/ns/job",
		progress_callback_url: Some(url),
		run_id: Some("22222222-2222-2222-2222-222222222222"),
	};
	let job = build_restore_job(
		&restore,
		"test-restore-restore",
		"default",
		&replica,
		"kopia:latest",
		"http://operator/api/v1/cache-pressure/default/test-restore",
		Some(&proxy),
		&PodPlacement::default(),
	)
	.unwrap();
	let sidecar = job
		.spec
		.unwrap()
		.template
		.spec
		.unwrap()
		.init_containers
		.unwrap()
		.into_iter()
		.find(|c| c.name == "canopy-proxy")
		.expect("canopy-proxy sidecar");

	let got = sidecar
		.env
		.unwrap()
		.into_iter()
		.find(|e| e.name == "PGRO_PROGRESS_CALLBACK_URL")
		.and_then(|e| e.value);
	assert_eq!(got.as_deref(), Some(url));
}

fn restore_job_for(snapshot_size: &str) -> k8s_openapi::api::batch::v1::Job {
	let (mut restore, replica) = test_restore_and_replica();
	restore.spec.snapshot_size = Quantity(snapshot_size.to_string());
	build_restore_job(
		&restore,
		"test-restore-restore",
		"default",
		&replica,
		"kopia:latest",
		"http://operator/api/v1/cache-pressure/default/test-restore",
		None,
		&PodPlacement::default(),
	)
	.unwrap()
}

/// kopia sizes its worker pool from the CPUs it can see on the node, not from
/// the container's cgroup limit, so it spawns far more workers than it has CPU
/// to run them on. Pin the parallelism to what the container is actually
/// allowed to use.
#[test]
fn restore_job_pins_kopia_parallelism_to_its_cpu_limit() {
	let job = restore_job_for("10Gi");
	let pod_spec = job.spec.unwrap().template.spec.unwrap();
	let container = &pod_spec.containers[0];
	let cpu_limit = container
		.resources
		.as_ref()
		.and_then(|r| r.limits.as_ref())
		.and_then(|m| m.get("cpu"))
		.expect("restore container must cap CPU")
		.0
		.clone();
	let script = container.args.as_ref().unwrap().join(" ");
	assert!(
		script.contains("--parallel=\"$KOPIA_PARALLEL\""),
		"restore must pass --parallel to kopia"
	);

	let parallel = container
		.env
		.as_ref()
		.unwrap()
		.iter()
		.find(|e| e.name == "KOPIA_PARALLEL")
		.and_then(|e| e.value.clone())
		.expect("KOPIA_PARALLEL must be set");
	assert_eq!(
		parallel, cpu_limit,
		"parallelism must match the container's CPU limit"
	);
}

/// A restore holding far more data gets more memory to work in, rather than
/// every restore sharing whatever suited the first one.
#[test]
fn restore_job_memory_scales_with_snapshot_size() {
	let mem_of = |job: k8s_openapi::api::batch::v1::Job| -> u64 {
		let pod_spec = job.spec.unwrap().template.spec.unwrap();
		let q = pod_spec.containers[0]
			.resources
			.as_ref()
			.unwrap()
			.limits
			.as_ref()
			.unwrap()
			.get("memory")
			.unwrap()
			.clone();
		parsed_bytes(&q)
	};

	let small = mem_of(restore_job_for("1Gi"));
	let large = mem_of(restore_job_for("500Gi"));
	assert!(
		large > small,
		"a much larger snapshot must get more memory, got {large} vs {small}"
	);
	assert_eq!(small, gib(4), "small restores keep today's 4Gi floor");
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
		"http://operator/api/v1/cache-pressure/default/test-restore",
		None,
		&PodPlacement::default(),
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
	assert!(
		names.contains(&"CACHE_PRESSURE_CALLBACK_URL"),
		"restore Job env must include CACHE_PRESSURE_CALLBACK_URL"
	);
	assert!(
		script.contains("PGRO_CACHE_PRESSURE"),
		"restore script must emit pre-flight pressure marker"
	);
	assert!(
		script.contains("$CACHE_PRESSURE_CALLBACK_URL"),
		"restore script must POST to the cache-pressure callback URL on pressure"
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
	let job = build_version_detect_job(
		&restore,
		"test-version-detect",
		"default",
		"test-pvc",
		&PodPlacement::default(),
	);
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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
	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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
fn deployment_init_script_flags_full_reindex_after_resetwal() {
	// pg_resetwal bypasses WAL replay, so any in-flight index write at
	// snapshot time can leave torn pages (postgres later surfaces these
	// as "unexpected zero page at block N" when queries hit the index).
	// Every pg_resetwal call must therefore touch /pgdata/needs-reindex-all
	// so the main container's startup hook runs REINDEX DATABASE on every
	// user database before the readiness probe lets traffic in.
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

	// The flag must be set every time pg_resetwal -f runs (currently
	// twice — detected-signature branch and last-resort branch). The
	// quick check: at least one `touch /pgdata/needs-reindex-all`
	// follows each pg_resetwal invocation, and the count of the touch
	// matches the count of resets.
	// Count actual invocations (the literal command line) — using just
	// "pg_resetwal -f" picks up comments and echo messages too.
	let resetwal_calls = script.matches("pg_resetwal -f \"$PGDATA\"").count();
	let touch_calls = script.matches("touch /pgdata/needs-reindex-all").count();
	assert_eq!(
		touch_calls, resetwal_calls,
		"every pg_resetwal -f invocation must be paired with `touch /pgdata/needs-reindex-all` (got {touch_calls} touches for {resetwal_calls} resets)"
	);
	assert!(
		resetwal_calls >= 2,
		"expected at least two pg_resetwal invocation sites; got {resetwal_calls}"
	);
}

#[test]
fn deployment_init_script_detects_locale_mismatch_before_rewriting() {
	// The single-user pass is where the locale rewrite actually happens, so
	// it is the only place that can observe whether a rewrite was needed:
	// once it runs, every database conforms and a later query can no longer
	// tell. `postgres --single` reports no row count, so the rewrite must be
	// preceded by a labelled count probe in the same session.
	let script = setup_auth_script();

	assert!(
		script.contains("pgro_locale_mismatch"),
		"single-user locale pass must probe for non-conforming databases under a distinctive label before rewriting them"
	);
	let probe_call = script
		.find(r#"postgres_single_or_resetwal "SELECT count(*) AS pgro_locale_mismatch"#)
		.expect("the probe must be submitted through the single-user helper");
	assert!(
		script[probe_call..].starts_with(
			r#"postgres_single_or_resetwal "SELECT count(*) AS pgro_locale_mismatch FROM pg_database WHERE $LOCALE_MISMATCH_WHERE;
$LOCALE_REWRITE""#
		),
		"the rewrite must be submitted in the same single-user session, on the line after the probe: single-user mode ends a statement at the newline, and a separate session would probe a database the rewrite had already fixed"
	);
}

#[test]
fn deployment_init_script_records_locale_fix_from_sticky_flag() {
	// `fixes.locale` must be driven by a flag file the rewrite touches, the
	// same way reset_wal and recreated_pg_wal are. The previous shell
	// variable was set to 1 by the single-user pass and then unconditionally
	// overwritten by the post-startup fallback's row count — always 0,
	// because the single-user pass had already fixed every database — so the
	// flag could never report true.
	let script = setup_auth_script();

	assert!(
		script.contains("touch /pgdata/fix-locale"),
		"a locale rewrite must record itself in a flag file"
	);
	assert!(
		script.contains("if [ -f /pgdata/fix-locale ]; then PGRO_LOCALE=true"),
		"PGRO_LOCALE must be read back from the flag file, not from a shell variable a later step can clobber"
	);
	assert!(
		!script.contains("LOCALE_CHANGED=1\n"),
		"the unconditional LOCALE_CHANGED=1 must be gone — it was overwritten by the post-startup fallback before it was ever read"
	);
}

#[test]
fn deployment_init_script_pairs_locale_rewrite_with_reindex_flag() {
	// Rewriting datcollate changes collation semantics, so every text btree
	// built under the old collation is potentially misordered. Each site
	// that records a locale rewrite must also raise needs-reindex, which
	// gates the readiness probe until the rebuild finishes.
	let script = setup_auth_script();

	let locale_flags = script.matches("touch /pgdata/fix-locale").count();
	// Trailing newline: `needs-reindex-all` shares this prefix.
	let reindex_flags = script.matches("touch /pgdata/needs-reindex\n").count();
	assert!(
		locale_flags >= 1,
		"expected at least one locale-rewrite site; got {locale_flags}"
	);
	assert_eq!(
		reindex_flags, locale_flags,
		"every locale rewrite must be paired with `touch /pgdata/needs-reindex` (got {reindex_flags} reindex flags for {locale_flags} locale rewrites)"
	);
}

#[test]
fn deployment_runtime_reindex_handles_full_database_flag() {
	// The main container's startup hook handles the broad flag
	// (needs-reindex-all) via blind REINDEX DATABASE on every user DB.
	// The earlier amcheck-driven smart pass turned out to hit the same
	// postgres-internal pathology that wedges other vanilla DDL on
	// the prod data — bt_index_check burned 100% CPU forever on
	// individual indexes. REINDEX uses a different code path (reads
	// the heap, rebuilds from scratch) and doesn't trip that wedge.
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
	let postgres = deploy
		.spec
		.unwrap()
		.template
		.spec
		.unwrap()
		.containers
		.into_iter()
		.find(|c| c.name == "postgres")
		.expect("postgres container must exist");
	let script = postgres.args.unwrap().remove(0);

	assert!(
		script.contains("/pgdata/needs-reindex-all"),
		"runtime startup hook must check the needs-reindex-all flag"
	);
	assert!(
		script.contains("REINDEX DATABASE CONCURRENTLY"),
		"needs-reindex-all branch must run REINDEX DATABASE CONCURRENTLY on PG ≥ 12 so clients can keep querying the old indexes during the rebuild"
	);
	// The PG < 12 fallback uses plain REINDEX DATABASE — verify the
	// version gate exists in the script. (We can't string-match on the
	// literal SQL because the shell-quoted `\"` form differs from a
	// Rust string literal.)
	assert!(
		script.contains(r#""$PG_MAJOR" -ge 12"#),
		"needs-reindex-all branch must gate CONCURRENTLY behind a PG ≥ 12 check"
	);
	assert!(
		!script.contains("bt_index_check("),
		"runtime hook must not call bt_index_check — amcheck wedges on the prod data"
	);
	assert!(
		script.contains("rm -f /pgdata/needs-reindex-all"),
		"runtime hook must clear the needs-reindex-all flag after the reindex"
	);
}

#[test]
fn deployment_readiness_probe_only_gates_on_locale_reindex() {
	// The readiness probe waits for the locale-only `needs-reindex`
	// flag to clear (small, fast, finishes in seconds) but NOT for
	// `needs-reindex-all` (the post-pg_resetwal blind REINDEX DATABASE
	// — takes hours on prod-sized indexes; gating here would trip the
	// operator's deployment_ready_timeout and block restores
	// indefinitely). The -all reindex runs in the background; clients
	// hitting a not-yet-reindexed corrupt index see the explicit
	// "unexpected zero page" error and can retry.
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
	let postgres = deploy
		.spec
		.unwrap()
		.template
		.spec
		.unwrap()
		.containers
		.into_iter()
		.find(|c| c.name == "postgres")
		.expect("postgres container must exist");
	let probe_cmd = postgres
		.readiness_probe
		.expect("readiness probe must exist")
		.exec
		.expect("readiness probe must be an exec probe")
		.command
		.expect("exec probe must have a command");
	let probe_script = probe_cmd.join(" ");
	assert!(
		probe_script.contains("[ ! -f /pgdata/needs-reindex ]"),
		"readiness probe must still wait for the locale-only needs-reindex flag; got: {probe_script}"
	);
	assert!(
		!probe_script.contains("needs-reindex-all"),
		"readiness probe must NOT gate on needs-reindex-all (the long-running post-resetwal reindex); got: {probe_script}"
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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
fn postgres_container_stage_updates_override_read_only() {
	// A read-only replica runs with default_transaction_read_only = on, which
	// rejects the stage bookkeeping the reindex hook does about itself:
	//   ERROR: cannot execute UPDATE in a read-only transaction
	// The stage then sticks at whatever the init container wrote and never
	// reaches 'ready', so `_pgro.restore_info` misreports a finished reindex as
	// still pending. Each stage update must ask for a writable session.
	let script = postgres_container_script();

	let stage_updates = script
		.matches("UPDATE _pgro.restore_info SET stage")
		.count();
	let overrides = script
		.matches("PGOPTIONS='-c default_transaction_read_only=off'")
		.count();
	assert!(
		stage_updates >= 2,
		"expected the reindexing and ready stage updates; got {stage_updates}"
	);
	assert_eq!(
		overrides, stage_updates,
		"every stage update must run with default_transaction_read_only=off (got {overrides} overrides for {stage_updates} updates)"
	);
}

#[test]
fn locale_reindex_targets_only_default_collation_indexes() {
	// A datcollate rewrite only invalidates indexes ordered by the database
	// default collation (OID 100). Catalog indexes over `name` columns carry
	// the C collation (950) and are unaffected — and REINDEX INDEX
	// CONCURRENTLY cannot touch a system catalog at all, so selecting them
	// yields dozens of swallowed "cannot reindex system catalogs
	// concurrently" errors that look like work being done.
	let script = postgres_container_script();

	assert!(
		script.contains("a.attcollation = 100"),
		"the locale reindex must select only indexes ordered by the default collation"
	);
	assert!(
		!script.contains("a.attcollation <> 0"),
		"selecting every collation-bearing attribute pulls in system catalogs that cannot be reindexed concurrently"
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

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
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

fn migration_target() -> MigrationTarget {
	MigrationTarget {
		version: "2.62.0".to_string(),
		version_id: "66666666-6666-6666-6666-666666666666".to_string(),
	}
}

fn env_value(container: &k8s_openapi::api::core::v1::Container, name: &str) -> Option<String> {
	container
		.env
		.as_ref()?
		.iter()
		.find(|e| e.name == name)?
		.value
		.clone()
}

fn env_secret_key(
	container: &k8s_openapi::api::core::v1::Container,
	name: &str,
) -> Option<(String, String)> {
	let e = container.env.as_ref()?.iter().find(|e| e.name == name)?;
	let sel = e.value_from.as_ref()?.secret_key_ref.as_ref()?;
	Some((sel.name.clone(), sel.key.clone()))
}

#[test]
fn migration_job_lets_the_image_entrypoint_take_the_subcommand() {
	let (restore, replica) = test_restore_and_replica();
	let job = super::migration::build_migration_job(
		&restore,
		&replica,
		&migration_target(),
		"tamanu-fiji",
		"default",
		&PodPlacement::default(),
	);
	let container = &job.spec.unwrap().template.spec.unwrap().containers[0];

	// The image's ENTRYPOINT dispatches the subcommand; overriding command would
	// bypass it and run the app the wrong way.
	assert_eq!(
		container.command, None,
		"command must be left to the image entrypoint"
	);
	assert_eq!(
		container.args.as_deref(),
		Some(["migrate".to_string()].as_slice())
	);
	assert_eq!(
		container.image.as_deref(),
		Some("ghcr.io/beyondessential/tamanu-central:v2.62.0"),
		"the image must be the target version's own, since it owns the migrations"
	);
}

#[test]
fn migration_job_points_tamanu_at_the_restored_database() {
	let (restore, replica) = test_restore_and_replica();
	let job = super::migration::build_migration_job(
		&restore,
		&replica,
		&migration_target(),
		"tamanu-fiji",
		"default",
		&PodPlacement::default(),
	);
	let container = &job.spec.unwrap().template.spec.unwrap().containers[0];

	// tamanu reads its db config from CONFIG_SYNC_DB_*; plain DB_* is ignored, so
	// the job would silently target the packaged default instead.
	assert_eq!(
		env_value(container, "CONFIG_SYNC_DB_HOST").as_deref(),
		Some("test-restore"),
		"the per-restore Service, so a switchover cannot repoint it mid-migration"
	);
	// The restored database's own name: pgro replicas don't name the database
	// after its owner, so the credentials username is the wrong answer.
	assert_eq!(
		env_value(container, "CONFIG_SYNC_DB_NAME").as_deref(),
		Some("tamanu-fiji")
	);
	assert_eq!(
		env_secret_key(container, "CONFIG_SYNC_DB_USERNAME"),
		Some(("test-replica-creds".to_string(), "username".to_string()))
	);
	assert_eq!(
		env_secret_key(container, "CONFIG_SYNC_DB_PASSWORD"),
		Some(("test-replica-creds".to_string(), "password".to_string()))
	);
	assert!(
		container
			.env
			.as_ref()
			.unwrap()
			.iter()
			.all(|e| e.name != "DB_HOST"),
		"no plain DB_* vars, which tamanu does not read"
	);
}

#[test]
fn migration_job_supplies_a_connection_url_too() {
	let (restore, replica) = test_restore_and_replica();
	let job = super::migration::build_migration_job(
		&restore,
		&replica,
		&migration_target(),
		"tamanu-fiji",
		"default",
		&PodPlacement::default(),
	);
	let container = &job.spec.unwrap().template.spec.unwrap().containers[0];

	assert_eq!(
		env_value(container, "DATABASE_URL").as_deref(),
		Some(
			"postgresql://$(CONFIG_SYNC_DB_USERNAME):$(CONFIG_SYNC_DB_PASSWORD)@test-restore:5432/tamanu-fiji"
		),
		"a version that prefers DATABASE_URL must reach the same database as CONFIG_SYNC_DB_*"
	);

	let env = container.env.as_ref().unwrap();
	let position = |name: &str| env.iter().position(|e| e.name == name).unwrap();
	assert!(
		position("DATABASE_URL") > position("CONFIG_SYNC_DB_USERNAME")
			&& position("DATABASE_URL") > position("CONFIG_SYNC_DB_PASSWORD"),
		"kubelet expands $(VAR) only from entries above it, so the URL would ship the literal placeholders"
	);
}

#[test]
fn migration_job_does_not_retry_and_is_owned_by_the_restore() {
	let (restore, replica) = test_restore_and_replica();
	let job = super::migration::build_migration_job(
		&restore,
		&replica,
		&migration_target(),
		"tamanu-fiji",
		"default",
		&PodPlacement::default(),
	);

	// A failed migration is the finding; retrying spends the same hours to reach
	// the same answer.
	assert_eq!(job.spec.as_ref().unwrap().backoff_limit, Some(0));

	let container = &job
		.spec
		.as_ref()
		.unwrap()
		.template
		.spec
		.as_ref()
		.unwrap()
		.containers[0];
	let limits = container
		.resources
		.as_ref()
		.expect("the job must be capped")
		.limits
		.as_ref()
		.expect("limits set");
	// Too tight and an OOMKill reads as a failed migration, filing a known issue
	// against a version that is fine.
	assert_eq!(limits.get("memory").expect("memory limit").0, "4Gi");
	assert!(container.resources.as_ref().unwrap().requests.is_some());
	assert_eq!(
		job.spec
			.as_ref()
			.unwrap()
			.template
			.spec
			.as_ref()
			.unwrap()
			.restart_policy
			.as_deref(),
		Some("Never")
	);

	// Owned by the restore, so tearing the restore down takes the job with it.
	let owners = job.metadata.owner_references.as_ref().unwrap();
	assert_eq!(owners.len(), 1);
	assert_eq!(owners[0].uid, "uid-123");
	assert_eq!(owners[0].kind, "PostgresPhysicalRestore");
}

#[test]
fn deployment_lifts_read_only_for_a_migration_target() {
	// Migrations are DDL, so a read-only replica cannot host one: a restore
	// carrying a target is built read-write for the same reason persistent_schemas
	// restores are. On PG >= 14 read-only means `pg_read_all_data`, which holds no
	// DDL whatever the transaction default says.
	let (mut restore, mut replica) = test_restore_and_replica();
	replica.spec.read_only = true;
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("18".to_string()),
		..Default::default()
	});

	let script_for = |restore: &PostgresPhysicalRestore| {
		let deploy = build_deployment(
			restore,
			"test-restore",
			"default",
			&replica,
			&PodPlacement::default(),
		)
		.unwrap();
		let pod_spec = deploy.spec.unwrap().template.spec.unwrap();
		pod_spec
			.init_containers
			.as_ref()
			.unwrap()
			.iter()
			.find(|c| c.name == "setup-auth")
			.expect("setup-auth init container must exist")
			.args
			.as_ref()
			.unwrap()[0]
			.clone()
	};

	// Both grant branches are in the script; the flag the operator interpolates is
	// what picks one.
	assert!(
		script_for(&restore).contains(r#"[ "true" = "true" ]"#),
		"an ordinary restore of a read-only replica stays read-only"
	);

	restore.spec.migrate_to = Some(migration_target());
	assert!(
		script_for(&restore).contains(r#"[ "false" = "true" ]"#),
		"a migrating restore must take the superuser branch"
	);
}

/// Tamanu's own `logs.migrations.stats` payload, per
/// `MigrationLogStats` + `PreMigrationDbSnapshot`.
fn tamanu_stats(size_bytes: i64) -> serde_json::Value {
	serde_json::json!({
		"durationMsPerMigration": {
			"1721000000-addFoo.ts": 400_000,
			"1721000001-addBar.ts": 12_500,
		},
		"totalMigrationsDurationMs": 412_500,
		"preSnapshot": {
			"databaseSizeBytes": size_bytes,
			"tableRowEstimates": [{ "table": "public.patients", "rows": 12_000 }],
		},
	})
}

fn applied() -> Vec<String> {
	vec![
		"1721000000-addFoo.ts".to_string(),
		"1721000001-addBar.ts".to_string(),
	]
}

#[test]
fn batch_result_reads_tamanu_stats() {
	let r = super::migration::result_from_batch(
		applied(),
		Some(415_000),
		Some(tamanu_stats(1_000_000)),
		1_200_000,
		false,
	);

	assert_eq!(r.total_elapsed_seconds, 415);
	assert_eq!(r.data_bytes_before, 1_000_000);
	assert_eq!(r.data_bytes_after, 1_200_000);
	assert_eq!(r.failed_migration, None);
	// Order comes from the batch's file list, since a JSON object has none.
	let names: Vec<_> = r.timings.iter().map(|t| t.name.as_str()).collect();
	assert_eq!(names, ["1721000000-addFoo.ts", "1721000001-addBar.ts"]);
	assert_eq!(r.timings[0].elapsed_seconds, 400);
	assert_eq!(r.timings[1].elapsed_seconds, 12);
}

#[test]
fn batch_result_treats_unreadable_size_as_unknown() {
	// tamanu writes -1 when pg_database_size could not be read.
	let r = super::migration::result_from_batch(
		applied(),
		Some(1_000),
		Some(tamanu_stats(-1)),
		1_200_000,
		false,
	);
	assert_eq!(
		r.data_bytes_before, 1_200_000,
		"an unreadable before-size must not report negative growth"
	);
}

#[test]
fn batch_result_names_where_a_failed_run_stopped() {
	let mut stats = tamanu_stats(1_000);
	stats["failedMigration"] = serde_json::json!("1721000002-addBaz.ts");

	let r = super::migration::result_from_batch(applied(), None, Some(stats), 2_000, true);

	assert_eq!(
		r.failed_migration.as_deref(),
		Some("1721000002-addBaz.ts"),
		"the migration tamanu stopped at, not the last one that applied"
	);
	// No batch duration recorded, so it falls back to the sum of the timings.
	assert_eq!(r.total_elapsed_seconds, 412);
}

#[test]
fn batch_result_reports_an_unattributed_failure() {
	// A target version that records a batch only once all of it applied leaves no
	// failedMigration to read, so the failure is reported without a name rather
	// than pinned on the last migration that did apply.
	let r = super::migration::result_from_batch(
		applied(),
		None,
		Some(tamanu_stats(1_000)),
		2_000,
		true,
	);
	assert_eq!(r.failed_migration.as_deref(), Some("unknown"));
}

#[test]
fn deployment_with_redaction_runs_postgres_as_root_and_installs_anon() {
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("18".to_string()),
		..Default::default()
	});
	replica.spec.redaction = Some(RedactionSpec {
		manifest_url: "https://example.com/m.json".into(),
		version: None,
		version_query: None,
		version_fallback_to_base: false,
	});

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
	let pod = deploy
		.spec
		.as_ref()
		.unwrap()
		.template
		.spec
		.as_ref()
		.unwrap();

	let postgres = pod
		.containers
		.iter()
		.find(|c| c.name == "postgres")
		.expect("postgres container must be present");

	let sec = postgres
		.security_context
		.as_ref()
		.expect("postgres container must override securityContext when redaction is set");
	assert_eq!(sec.run_as_user, Some(0), "postgres must run as root");

	let script = &postgres.args.as_ref().unwrap()[0];
	assert!(
		script.contains("postgresql_anonymizer_18"),
		"prelude must apt-install the anon package for the restore's PG major, got: {script}"
	);
	assert!(
		script.contains("/usr/lib/postgresql/18/lib"),
		"prelude must stage anon.so into the PG-major lib dir, got: {script}"
	);
	assert!(
		script.contains("exec gosu postgres postgres"),
		"prelude must drop privileges via gosu before exec'ing postgres"
	);
}

#[test]
fn deployment_without_redaction_keeps_default_securitycontext() {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("18".to_string()),
		..Default::default()
	});

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
	let pod = deploy
		.spec
		.as_ref()
		.unwrap()
		.template
		.spec
		.as_ref()
		.unwrap();
	let postgres = pod
		.containers
		.iter()
		.find(|c| c.name == "postgres")
		.unwrap();
	assert!(
		postgres.security_context.is_none(),
		"postgres container must inherit the pod-level UID 999 when redaction is off"
	);
	let script = &postgres.args.as_ref().unwrap()[0];
	assert!(
		!script.contains("postgresql_anonymizer"),
		"the anon install prelude must not be emitted when redaction is off"
	);
	assert!(
		script.contains("exec postgres -D /pgdata/pgdata"),
		"postgres must be exec'd directly when there's no privilege to drop"
	);
}

#[test]
fn deployment_with_redaction_builds_for_pg16() {
	// Redaction used to be gated to PG 18+ when we relied on the
	// extension_control_path GUC. Now the postgres container's prelude
	// drops the files into /usr/share/postgresql/$N/extension and
	// /usr/lib/postgresql/$N/lib of its own writable layer, so any PG
	// major works.
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("16".to_string()),
		..Default::default()
	});
	replica.spec.redaction = Some(RedactionSpec {
		manifest_url: "https://example.com/m.json".into(),
		version: None,
		version_query: None,
		version_fallback_to_base: false,
	});

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.expect("redaction should build on PG 16");
	let pod = deploy
		.spec
		.as_ref()
		.unwrap()
		.template
		.spec
		.as_ref()
		.unwrap();
	let postgres = pod
		.containers
		.iter()
		.find(|c| c.name == "postgres")
		.unwrap();
	let script = &postgres.args.as_ref().unwrap()[0];
	assert!(
		script.contains("postgresql_anonymizer_16"),
		"prelude must use the restore's PG major (16), got: {script}"
	);
}

#[test]
fn deployment_with_redaction_forces_writable() {
	let (mut restore, mut replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("18".to_string()),
		..Default::default()
	});
	replica.spec.read_only = true;
	replica.spec.redaction = Some(RedactionSpec {
		manifest_url: "https://example.com/m.json".into(),
		version: None,
		version_query: None,
		version_fallback_to_base: false,
	});

	let deploy = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
	let script = deploy_init_setup_auth_script(&deploy);
	// The init script uses `if [ "<read_only>" = "true" ]` and we want
	// that variable substituted to "false" when redaction is set so the
	// conditional doesn't fire at runtime.
	assert!(
		script.contains("if [ \"false\" = \"true\" ]"),
		"redaction must defer read-only by substituting read_only=false into the init script"
	);
}

fn deploy_init_setup_auth_script(deploy: &k8s_openapi::api::apps::v1::Deployment) -> String {
	let pod = deploy
		.spec
		.as_ref()
		.unwrap()
		.template
		.spec
		.as_ref()
		.unwrap();
	let setup_auth = pod
		.init_containers
		.as_ref()
		.unwrap()
		.iter()
		.find(|c| c.name == "setup-auth")
		.unwrap();
	setup_auth.args.as_ref().unwrap()[0].clone()
}

/// Every pod the operator creates has to carry the cluster's placement
/// defaults. A builder that forgets puts a database — or the job restoring one
/// — wherever the cluster's default node pool happens to be, which is the
/// failure this configuration exists to prevent. Covers the builders reachable
/// from these fixtures; `build_snapshot_list_job` and
/// `build_schema_migration_job` are covered in their own modules.
#[test]
fn every_builder_stamps_the_placement_defaults() {
	let placement = PodPlacement::parse(
		"bes.node.purpose=workload",
		"karpenter.sh/do-not-disrupt=true",
	);
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("17".to_string()),
		..Default::default()
	});

	let deployment =
		build_deployment(&restore, "test-restore", "default", &replica, &placement).unwrap();
	let templates = vec![
		(
			"postgres deployment",
			deployment.spec.unwrap().template.clone(),
		),
		(
			"restore job",
			build_restore_job(
				&restore,
				"test-restore-restore",
				"default",
				&replica,
				"kopia:latest",
				"http://operator/cache-pressure",
				None,
				&placement,
			)
			.unwrap()
			.spec
			.unwrap()
			.template,
		),
		(
			"version detect job",
			build_version_detect_job(&restore, "detect", "default", "test-pvc", &placement)
				.spec
				.unwrap()
				.template,
		),
		(
			"credential reset job",
			super::build_credential_reset_job(
				&restore,
				&replica,
				"cred-reset",
				"default",
				&placement,
			)
			.unwrap()
			.spec
			.unwrap()
			.template,
		),
		(
			"migration job",
			super::migration::build_migration_job(
				&restore,
				&replica,
				&migration_target(),
				"tamanu",
				"default",
				&placement,
			)
			.spec
			.unwrap()
			.template,
		),
	];

	for (what, template) in templates {
		let selector = template
			.spec
			.as_ref()
			.and_then(|s| s.node_selector.as_ref())
			.unwrap_or_else(|| panic!("{what} must carry a nodeSelector"));
		assert_eq!(
			selector.get("bes.node.purpose").map(String::as_str),
			Some("workload"),
			"{what} must be pinned to the configured node purpose"
		);

		let annotations = template
			.metadata
			.as_ref()
			.and_then(|m| m.annotations.as_ref())
			.unwrap_or_else(|| panic!("{what} must carry the configured pod annotations"));
		assert_eq!(
			annotations
				.get("karpenter.sh/do-not-disrupt")
				.map(String::as_str),
			Some("true"),
			"{what} must carry the configured pod annotations"
		);
	}
}

/// The default placement must leave every builder's output as it was, so an
/// operator with no ConfigMap entries sees no behaviour change.
#[test]
fn empty_placement_adds_nothing() {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("17".to_string()),
		..Default::default()
	});
	let deployment = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();
	let template = deployment.spec.unwrap().template;
	assert!(template.spec.unwrap().node_selector.is_none());
}

/// The Service selector requires this label, so a pod that comes up without it
/// is invisible to clients until the operator notices and patches it. Declaring
/// it in the pod template means a replacement pod — eviction, node loss, OOM —
/// rejoins the Service the moment it is Ready.
#[test]
fn deployment_pod_template_carries_the_ready_for_traffic_label() {
	let (mut restore, replica) = test_restore_and_replica();
	restore.status = Some(PostgresPhysicalRestoreStatus {
		postgres_version: Some("17".to_string()),
		..Default::default()
	});

	let deployment = build_deployment(
		&restore,
		"test-restore",
		"default",
		&replica,
		&PodPlacement::default(),
	)
	.unwrap();

	let labels = deployment
		.spec
		.unwrap()
		.template
		.metadata
		.expect("pod template has metadata")
		.labels
		.expect("pod template has labels");

	assert_eq!(
		labels
			.get(crate::controllers::READY_FOR_TRAFFIC_LABEL)
			.map(String::as_str),
		Some("true"),
		"a replacement pod must rejoin the Service without waiting for a reconcile"
	);
	// The restore label is what the Deployment's own selector matches on, so
	// adding the traffic label must not have displaced it.
	assert_eq!(
		labels.get("pgro.bes.au/restore").map(String::as_str),
		Some("test-restore")
	);
}

/// A restore can land a data directory on a base image whose ICU/glibc differs
/// from the machine the snapshot came from. Postgres records a version per
/// collation and an index ordered by a mismatched one may sort wrongly, which
/// shows up as an index scan quietly missing rows. Detection has to be its own
/// flag: it is independent of the locale rewrite that drives `needs-reindex`.
#[test]
fn setup_auth_flags_collation_version_mismatches() {
	let script = setup_auth_script();

	assert!(
		script.contains("touch /pgdata/needs-collation-refresh"),
		"a version mismatch must raise its own flag, not ride on needs-reindex"
	);
	assert!(
		script.contains("pg_collation_actual_version"),
		"detection must compare the recorded version against the OS"
	);
	// A missing function fails the query at parse time however the runtime
	// branches, so the probe is what keeps this working on older servers.
	assert!(
		script.contains("to_regprocedure('pg_collation_actual_version(oid)')"),
		"detection must probe for the function rather than assume a version cutoff"
	);
	// The script runs under `set -e`; non-numeric input to the arithmetic would
	// abort the init container and fail the restore outright.
	assert!(
		script.contains("tr -cd '0-9'"),
		"the mismatch count must be sanitised before it reaches shell arithmetic"
	);
}

#[test]
fn collation_refresh_rebuilds_then_refreshes() {
	let script = postgres_container_script();

	assert!(
		script.contains("if [ -f /pgdata/needs-collation-refresh ]; then"),
		"the background block must handle the collation flag"
	);
	assert!(
		script.contains("|| [ -f /pgdata/needs-collation-refresh ]"),
		"the flag must be able to start the background block on its own, with \
		 neither reindex flag set"
	);

	let rebuild = script
		.find("Collation version refresh:")
		.expect("collation branch announces itself");
	let refresh = script
		.find("ALTER COLLATION $coll REFRESH VERSION;")
		.expect("recorded versions get refreshed");
	assert!(
		rebuild < refresh,
		"indexes must be rebuilt before their collation's version is refreshed; \
		 refreshing first trades a correctness warning for silence"
	);
}

/// The rebuild loop tolerates individual failures to keep making progress. If
/// the refresh ran anyway it would stamp the current OS version onto a
/// collation whose indexes are still wrongly ordered, and postgres would stop
/// warning about a problem that is still there.
#[test]
fn collation_refresh_is_skipped_when_a_rebuild_failed() {
	let script = postgres_container_script();

	assert!(
		script.contains("FAILED=$((FAILED + 1))"),
		"rebuild failures must be counted, not just swallowed"
	);
	assert!(
		script.contains(r#"if [ "$FAILED" = "0" ]; then"#),
		"the refresh must be gated on a clean rebuild"
	);
	// `cmd | while` runs the loop in a subshell, so the counter would be
	// discarded exactly when it matters.
	assert!(
		script.contains(r#"done < "$IDX_FILE""#),
		"the rebuild loop must read from a file so the failure count survives it"
	);
}

/// The locale branch avoids catalog indexes only incidentally — they carry the
/// C collation, which its `= 100` predicate misses. Matching on collation
/// version instead sweeps them back in, and REINDEX CONCURRENTLY cannot touch a
/// system catalog, so the exclusion has to be explicit.
#[test]
fn collation_refresh_excludes_system_namespaces() {
	let script = postgres_container_script();

	assert!(script.contains("n.nspname NOT IN ('pg_catalog', 'information_schema')"));
	assert!(script.contains("n.nspname NOT LIKE 'pg_toast%'"));
	assert!(script.contains("n.nspname NOT LIKE 'pg_temp%'"));
}

/// Database-level collation version tracking, and the statement that clears it,
/// only exist from PG 15 — but the operator restores older servers too.
#[test]
fn database_level_refresh_is_version_gated() {
	let script = postgres_container_script();

	let gate = script
		.find(r#"if [ "$SERVER_VERSION_NUM" -ge 150000 ]; then"#)
		.expect("database-level refresh is gated on server_version_num");
	let stmt = script
		.find("REFRESH COLLATION VERSION")
		.expect("database-level refresh is issued");
	assert!(gate < stmt, "the gate must precede the statement it guards");
}

/// The collation branch must not displace the existing ones: a restore can need
/// any combination of the three, and the stage bookkeeping still has to land on
/// `ready` afterwards.
#[test]
fn collation_branch_runs_alongside_the_reindex_branches() {
	let script = postgres_container_script();

	let locale_branch = script
		.find("elif [ -f /pgdata/needs-reindex ]; then")
		.expect("locale reindex branch still present");
	let collation_branch = script
		.find("if [ -f /pgdata/needs-collation-refresh ]; then")
		.expect("collation branch present");
	let ready = script
		.find("stage = 'ready'")
		.expect("stage still ends at ready");

	assert!(
		locale_branch < collation_branch,
		"the collation branch is additional to the reindex branches, not an elif on them"
	);
	assert!(
		collation_branch < ready,
		"the collation work must finish before the replica is marked ready"
	);
	assert!(script.contains("rm -f /pgdata/needs-collation-refresh"));
}

/// The operator ships two non-trivial shell scripts into containers, where a
/// syntax error surfaces as a crash-looping pod partway through a restore
/// rather than as a build failure. Parse them here instead.
///
/// This catches parse errors, not portability problems: on a host where
/// `/bin/sh` is bash, `sh -n` accepts bashisms the container's shell would
/// reject. Worth having anyway — a dropped `fi` is the failure mode that
/// actually happens when editing a script embedded in Rust.
#[test]
fn generated_shell_scripts_are_valid_posix_sh() {
	use std::io::Write;
	use std::process::{Command, Stdio};

	for (name, script) in [
		("setup-auth", setup_auth_script()),
		("postgres container", postgres_container_script()),
	] {
		let spawned = Command::new("sh")
			.arg("-n")
			.stdin(Stdio::piped())
			.stderr(Stdio::piped())
			.spawn();
		let mut child = match spawned {
			Ok(child) => child,
			// No POSIX shell on this host — nothing to check against, and
			// failing here would only punish the developer's machine.
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
			Err(e) => panic!("could not run sh: {e}"),
		};

		child
			.stdin
			.take()
			.expect("stdin piped")
			.write_all(script.as_bytes())
			.expect("write script to sh");

		let output = child.wait_with_output().expect("sh runs to completion");
		assert!(
			output.status.success(),
			"the {name} script is not valid POSIX sh:\n{}",
			String::from_utf8_lossy(&output.stderr)
		);
	}
}
