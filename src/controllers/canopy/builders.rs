//! Canopy-path Job + PVC builders.
//!
//! The CRD path (`src/controllers/restore/builders.rs`) has its own
//! `build_restore_job` deeply tied to `PostgresPhysicalReplica` /
//! `PostgresPhysicalRestore` spec fields. The canopy path has no CRDs — it
//! works from a `WorklistEntry` + labelled Namespace. This module holds the
//! canopy-side equivalents, small enough to live independently rather than
//! forcing a shared abstraction across two different data models.

use std::collections::BTreeMap;

use bestool_canopy::WorklistEntry;
use k8s_openapi::{
	api::{
		apps::v1::Deployment,
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, EmptyDirVolumeSource, EnvVar, PersistentVolumeClaim,
			PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, ResourceRequirements, Service,
			ServicePort, ServiceSpec, Volume, VolumeMount, VolumeResourceRequirements,
		},
	},
	apimachinery::pkg::{api::resource::Quantity, util::intstr::IntOrString},
};
use kube::api::ObjectMeta;

use crate::{
	controllers::{canopy::labels, kopia_writable_env},
	kopia::{ProxyConnect, kopia_connect_args_proxy},
};

/// Default kopia image used by the canopy path's restore Job when the
/// operator's dynamic `kopia_image` isn't accessible from the caller.
/// Kept in sync with `context::DEFAULT_KOPIA_IMAGE`; callers should always
/// pass the value from `ctx.kopia_image()`.
pub const KOPIA_JOB_NAME: &str = "restore";

/// Name of the canopy-path restore PVC. Namespaces are per-replica so a
/// stable short name is fine.
pub const PGDATA_PVC_NAME: &str = "pgdata";

/// Standard label used on the proxy-sidecar-carrying Pod so the operator's
/// broker NetworkPolicy can admit its ingress (see spec §4.3).
pub const PROXY_SIDECAR_POD_LABEL: (&str, &str) = ("pgro.bes.au/proxy-sidecar", "true");

/// Config the caller (worklist syncer) supplies to build a canopy restore Job.
pub struct CanopyRestoreJobConfig<'a> {
	pub entry: &'a WorklistEntry,
	pub namespace: &'a str,
	pub job_name: &'a str,
	pub kopia_image: &'a str,
	pub canopy_proxy_image: &'a str,
	/// Base URL the sidecar hits for creds, e.g.
	/// `http://postgres-restore-operator.pgro-system.svc:9091`. From
	/// `Context::canopy_broker_base_url`.
	pub broker_base_url: &'a str,
	/// Callback URL the sidecar POSTs its final TrafficStats to on
	/// shutdown, from `Context::canopy_stats_callback_url`.
	pub stats_callback_url: &'a str,
	/// Snapshot the operator wants restored — comes from the worklist
	/// entry when Provision or Refresh, but callers pass it explicitly
	/// because the syncer may know a more recent value.
	pub snapshot_id: &'a str,
	pub repo_password: &'a str,
	pub pgdata_pvc_size: &'a str,
}

/// Build the pgdata PVC for a canopy-backed replica. Namespace-scoped, one
/// PVC per replica namespace; sized from `pgdata_pvc_size` (caller decides
/// per intent).
pub fn build_pgdata_pvc(namespace: &str, size: &str) -> PersistentVolumeClaim {
	PersistentVolumeClaim {
		metadata: ObjectMeta {
			name: Some(PGDATA_PVC_NAME.into()),
			namespace: Some(namespace.into()),
			..Default::default()
		},
		spec: Some(PersistentVolumeClaimSpec {
			access_modes: Some(vec!["ReadWriteOnce".into()]),
			resources: Some(VolumeResourceRequirements {
				requests: Some(BTreeMap::from([("storage".into(), Quantity(size.into()))])),
				..Default::default()
			}),
			..Default::default()
		}),
		..Default::default()
	}
}

/// Shell wrapper the kopia container runs on the canopy path. Waits for the
/// proxy sidecar to publish its ephemeral port to `/var/run/pgro/proxy-port`
/// (30s timeout — the sidecar writes it as soon as the proxy binds, which
/// is essentially instant), reads the port, invokes kopia against the
/// loopback endpoint, then discovers PGDATA + writes `.postgres-version`
/// so the postgres Deployment can pick the right image.
fn kopia_wrapper_script() -> &'static str {
	r#"set -e

mkdir -p /tmp/kopia/config /tmp/kopia/logs /tmp/kopia/cache

PORT_FILE="/var/run/pgro/proxy-port"
for _ in $(seq 1 30); do
  [ -f "$PORT_FILE" ] && break
  sleep 1
done
if [ ! -f "$PORT_FILE" ]; then
  echo "ERROR: canopy-proxy sidecar did not write port file within 30s" >&2
  exit 1
fi
PROXY_PORT=$(cat "$PORT_FILE")
CANOPY_ENDPOINT="[::1]:${PROXY_PORT}"
echo "kopia connecting via canopy proxy at ${CANOPY_ENDPOINT}"

# Connect via proxy: kopia talks to [::1] with dummy keys, the proxy holds
# the live STS creds.
CONNECT_ARGS_FILE="/tmp/kopia-connect-args"
cat > "$CONNECT_ARGS_FILE" <<EOF
${CONNECT_ARGS}
EOF
# shellcheck disable=SC2046
kopia $(cat "$CONNECT_ARGS_FILE" | sed "s|@ENDPOINT@|${CANOPY_ENDPOINT}|")

echo "Starting restore..."
kopia snapshot restore "$SNAPSHOT_ID" /pgdata/postgres
echo "Restore complete"

# Discover PGDATA: prefer the 'current' symlink if present (org
# convention), else pick the highest version directory containing
# PG_VERSION.
echo "Locating PGDATA directory..."
PGDATA_DIR=""
if [ -L /pgdata/postgres/current ]; then
  LINK_TARGET=$(readlink /pgdata/postgres/current)
  RELATIVE=$(echo "$LINK_TARGET" | sed 's|.*/\([0-9]\{1,\}/\)|/pgdata/postgres/\1|')
  if [ -f "$RELATIVE/PG_VERSION" ]; then
    PGDATA_DIR="$RELATIVE"
    echo "Found PGDATA via 'current' symlink: $PGDATA_DIR"
  fi
fi
if [ -z "$PGDATA_DIR" ]; then
  PGDATA_DIR=$(find /pgdata/postgres -name PG_VERSION 2>/dev/null | while read -r f; do
    dir=$(dirname "$f")
    [ -d "$dir/global" ] && echo "$dir"
  done | sort -t/ -k4 -rn | head -1)
fi
if [ -z "$PGDATA_DIR" ]; then
  echo "ERROR: no PG_VERSION found in restored data" >&2
  exit 1
fi
echo "Found PGDATA at: $PGDATA_DIR"
ln -sfn "$PGDATA_DIR" /pgdata/pgdata
rm -f "$PGDATA_DIR/postmaster.pid"
VERSION=$(cat /pgdata/pgdata/PG_VERSION)
echo "Detected postgres version: $VERSION"
echo -n "$VERSION" > /pgdata/.postgres-version
# Mirror the version to the container termination message so the operator
# can read it back after the Pod terminates (via
# read_job_termination_message) and mirror it onto the namespace
# annotation the postgres Deployment reads.
echo -n "$VERSION" > /dev/termination-log
"#
}

/// Build the canopy-path restore Job. Two containers in one Pod: the kopia
/// container (talks to `[::1]:<port>` via the proxy) and the pgro-published
/// canopy-proxy sidecar. Both share an emptyDir volume for the port-file
/// coordination handshake.
pub fn build_canopy_restore_job(cfg: &CanopyRestoreJobConfig<'_>) -> Job {
	// Serialize the kopia connect args once; the wrapper script injects the
	// [::1]:<port> endpoint at runtime because the port isn't known until
	// the sidecar binds. Placeholder marker: @ENDPOINT@ (replaced by the
	// wrapper's sed).
	let connect_args = kopia_connect_args_proxy(&ProxyConnect {
		endpoint: "@ENDPOINT@",
		bucket: &cfg.entry.bucket,
		region: &cfg.entry.region,
		prefix: &cfg.entry.prefix,
		repository_password: cfg.repo_password,
		server_id: &cfg.entry.server_id.to_string(),
	});
	let connect_args_serialized = connect_args
		.into_iter()
		.map(|a| shlex_quote(&a))
		.collect::<Vec<_>>()
		.join(" \\\n  ");

	let mut pod_labels = BTreeMap::new();
	pod_labels.insert(
		PROXY_SIDECAR_POD_LABEL.0.into(),
		PROXY_SIDECAR_POD_LABEL.1.into(),
	);
	pod_labels.insert(
		labels::DECLARATION_ID.into(),
		cfg.entry.replica_id.to_string(),
	);
	pod_labels.insert(labels::SERVER.into(), cfg.entry.server_id.to_string());
	pod_labels.insert("pgro.bes.au/job-kind".into(), "canopy-restore".into());

	let script = kopia_wrapper_script().replace("${CONNECT_ARGS}", &connect_args_serialized);

	let kopia_container = Container {
		name: KOPIA_JOB_NAME.into(),
		image: Some(cfg.kopia_image.into()),
		command: Some(vec!["/bin/sh".into(), "-c".into()]),
		args: Some(vec![script]),
		env: Some(
			[
				vec![
					EnvVar {
						name: "SNAPSHOT_ID".into(),
						value: Some(cfg.snapshot_id.into()),
						..Default::default()
					},
					// `snapshot restore` runs as a separate kopia process from
					// `repository connect`, so it must re-open the repository and
					// needs the password. Headless containers have no keyring for
					// kopia to retrieve the persisted credential from, so without
					// this env var it falls back to an interactive prompt and dies
					// with "inappropriate ioctl for device". kopia reads
					// KOPIA_PASSWORD on every invocation.
					EnvVar {
						name: "KOPIA_PASSWORD".into(),
						value: Some(cfg.repo_password.into()),
						..Default::default()
					},
				],
				kopia_writable_env(),
			]
			.concat(),
		),
		volume_mounts: Some(vec![
			VolumeMount {
				name: "pgdata".into(),
				mount_path: "/pgdata".into(),
				..Default::default()
			},
			VolumeMount {
				name: "kopia-cache".into(),
				mount_path: "/tmp/kopia".into(),
				..Default::default()
			},
			VolumeMount {
				name: "proxy-shared".into(),
				mount_path: "/var/run/pgro".into(),
				..Default::default()
			},
		]),
		resources: Some(ResourceRequirements {
			requests: Some(BTreeMap::from([
				("cpu".into(), Quantity("500m".into())),
				("memory".into(), Quantity("1Gi".into())),
			])),
			limits: Some(BTreeMap::from([
				("cpu".into(), Quantity("2".into())),
				("memory".into(), Quantity("4Gi".into())),
			])),
			..Default::default()
		}),
		..Default::default()
	};

	let sidecar_container = Container {
		name: "canopy-proxy".into(),
		image: Some(cfg.canopy_proxy_image.into()),
		// Override the image's default ENTRYPOINT (`operator`) — same image
		// ships both binaries; the sidecar container runs `canopy-proxy`.
		command: Some(vec!["canopy-proxy".into()]),
		env: Some(vec![
			EnvVar {
				name: "PGRO_BROKER_URL".into(),
				value: Some(cfg.broker_base_url.into()),
				..Default::default()
			},
			EnvVar {
				name: "PGRO_GROUP".into(),
				value: Some(cfg.entry.group_id.to_string()),
				..Default::default()
			},
			EnvVar {
				name: "PGRO_TYPE".into(),
				value: Some(cfg.entry.r#type.to_string()),
				..Default::default()
			},
			EnvVar {
				name: "PGRO_REGION".into(),
				value: Some(cfg.entry.region.clone()),
				..Default::default()
			},
			EnvVar {
				name: "PGRO_STATS_CALLBACK_URL".into(),
				value: Some(cfg.stats_callback_url.into()),
				..Default::default()
			},
		]),
		volume_mounts: Some(vec![VolumeMount {
			name: "proxy-shared".into(),
			mount_path: "/var/run/pgro".into(),
			..Default::default()
		}]),
		resources: Some(ResourceRequirements {
			requests: Some(BTreeMap::from([
				("cpu".into(), Quantity("50m".into())),
				("memory".into(), Quantity("64Mi".into())),
			])),
			limits: Some(BTreeMap::from([
				("cpu".into(), Quantity("500m".into())),
				("memory".into(), Quantity("256Mi".into())),
			])),
			..Default::default()
		}),
		..Default::default()
	};

	Job {
		metadata: ObjectMeta {
			name: Some(cfg.job_name.into()),
			namespace: Some(cfg.namespace.into()),
			labels: Some(BTreeMap::from([
				("pgro.bes.au/job-kind".into(), "canopy-restore".into()),
				(
					labels::DECLARATION_ID.into(),
					cfg.entry.replica_id.to_string(),
				),
			])),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(3),
			// Longer than the CRD path's 2h — the proxy refreshes creds so
			// long restores aren't credential-bounded, only reachability-bounded.
			active_deadline_seconds: Some(14400), // 4 hours
			ttl_seconds_after_finished: Some(600),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(pod_labels),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".into()),
					// Termination grace: kopia may need a beat to flush kopia
					// caches; the sidecar exits within a second of SIGTERM.
					termination_grace_period_seconds: Some(30),
					containers: vec![kopia_container, sidecar_container],
					volumes: Some(vec![
						Volume {
							name: "pgdata".into(),
							persistent_volume_claim: Some(
								k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
									claim_name: PGDATA_PVC_NAME.into(),
									..Default::default()
								},
							),
							..Default::default()
						},
						Volume {
							name: "kopia-cache".into(),
							empty_dir: Some(EmptyDirVolumeSource::default()),
							..Default::default()
						},
						Volume {
							name: "proxy-shared".into(),
							empty_dir: Some(EmptyDirVolumeSource {
								medium: Some("Memory".into()),
								..Default::default()
							}),
							..Default::default()
						},
					]),
					..Default::default()
				}),
			},
			// The syncer polls Job status via labels; no need for a
			// Selector-based ownership contract with the operator.
			..Default::default()
		}),
		..Default::default()
	}
}

/// Name of the Postgres Deployment + Service the canopy path creates in
/// each replica namespace on restore success.
pub const POSTGRES_DEPLOYMENT_NAME: &str = "postgres";
pub const POSTGRES_SERVICE_NAME: &str = "postgres";
pub const POSTGRES_PORT: i32 = 5432;
/// Default analytics role name for canopy-backed replicas. Overridable
/// per-namespace via annotation later; for now, one name for every
/// canopy-backed replica.
pub const CANOPY_ANALYTICS_USERNAME: &str = "analytics";

/// Everything the syncer's `ensure_postgres` needs to build the postgres
/// Deployment via the shared [`crate::controllers::restore::builders::
/// build_postgres_deployment_with`]. Fills the intent-driven defaults
/// (read_only, resources, etc.) and threads the CRD-independent bits.
pub struct PostgresDeploymentConfig<'a> {
	pub namespace: &'a str,
	pub intent: &'a str,
	pub replica_id: &'a str,
	pub server_id: &'a str,
	/// Detected postgres major version, e.g. `"16"`. Read from the
	/// restore Job's termination message (via
	/// `read_job_termination_message`).
	pub postgres_major_version: &'a str,
	pub snapshot_id: &'a str,
	pub snapshot_time: &'a str,
	/// Analytics credentials Secret reference. The reporter creates the
	/// Secret ahead of the Deployment.
	pub analytics_secret: &'a k8s_openapi::api::core::v1::SecretReference,
	pub analytics_secret_key: &'a str,
}

/// Build the postgres Deployment for a canopy-backed replica by
/// forwarding to the shared CRD/canopy builder. Intent drives:
/// - `verify` / `analytics` → `read_only=true` (default_transaction_read_only)
/// - `disaster-recovery` → `read_only=false` (writable, acts as primary)
///
/// All the heavy lifting (pg_resetwal fallback, locale fixing,
/// analytics-user creation, REINDEX-on-startup) comes from the shared
/// builder — see `src/controllers/restore/builders.rs::
/// build_postgres_deployment_with`.
pub fn build_canopy_postgres_deployment(
	cfg: &PostgresDeploymentConfig<'_>,
) -> Result<Deployment, crate::error::Error> {
	use crate::controllers::restore::builders::{
		PostgresDeploymentInputs, build_postgres_deployment_with,
	};

	let read_only = matches!(cfg.intent, "verify" | "analytics");

	// Intent-driven default resources. Analytics replicas serve queries so
	// they need more headroom than a verify-only pod that just runs once
	// and gets torn down. Disaster-recovery mirrors analytics for now.
	let (cpu_req, mem_req, cpu_lim, mem_lim, shm) = match cfg.intent {
		"verify" => ("250m", "512Mi", "2", "2Gi", "512Mi"),
		"analytics" | "disaster-recovery" => ("500m", "2Gi", "4", "8Gi", "2Gi"),
		_ => ("250m", "512Mi", "2", "2Gi", "512Mi"),
	};
	let (shm_size, shared_buffers_mb) =
		crate::quantity::compute_shm_and_shared_buffers(&Some(ResourceRequirements {
			requests: Some(BTreeMap::from([
				("cpu".into(), Quantity(cpu_req.into())),
				("memory".into(), Quantity(mem_req.into())),
			])),
			limits: Some(BTreeMap::from([
				("cpu".into(), Quantity(cpu_lim.into())),
				("memory".into(), Quantity(mem_lim.into())),
			])),
			..Default::default()
		}));
	// The compute helper may pick a shm size smaller than what we want per
	// intent; take max of the two.
	let shm_size_final = if quantity_ge(&shm_size, &Quantity(shm.into())) {
		shm_size
	} else {
		Quantity(shm.into())
	};

	let mut labels: BTreeMap<String, String> = BTreeMap::new();
	labels.insert(labels::MANAGED_BY.into(), labels::MANAGED_BY_VALUE.into());
	labels.insert(labels::DECLARATION_ID.into(), cfg.replica_id.into());
	labels.insert(labels::SERVER.into(), cfg.server_id.into());
	labels.insert(labels::INTENT.into(), cfg.intent.into());
	labels.insert("app.kubernetes.io/name".into(), "postgres".into());
	labels.insert("pgro.bes.au/canopy-replica".into(), "true".into());
	let match_labels: BTreeMap<_, _> = labels
		.iter()
		.filter(|(k, _)| {
			k.as_str() == "app.kubernetes.io/name" || k.as_str() == "pgro.bes.au/canopy-replica"
		})
		.map(|(k, v)| (k.clone(), v.clone()))
		.collect();

	let inputs = PostgresDeploymentInputs {
		name: POSTGRES_DEPLOYMENT_NAME,
		namespace: cfg.namespace,
		pvc_name: PGDATA_PVC_NAME,
		pg_version: cfg.postgres_major_version,
		shm_size: shm_size_final,
		shared_buffers_mb,
		read_only,
		postgres_extra_config: None,
		analytics_username: CANOPY_ANALYTICS_USERNAME,
		analytics_password_secret: cfg.analytics_secret,
		analytics_password_key: cfg.analytics_secret_key,
		snapshot_id: cfg.snapshot_id,
		snapshot_time: cfg.snapshot_time,
		labels,
		match_labels,
		pod_annotations: None,
		// Canopy path uses namespace-cascade deletion for teardown; no
		// per-Deployment owner reference.
		owner_references: None,
		postgres_resources: Some(ResourceRequirements {
			requests: Some(BTreeMap::from([
				("cpu".into(), Quantity(cpu_req.into())),
				("memory".into(), Quantity(mem_req.into())),
			])),
			limits: Some(BTreeMap::from([
				("cpu".into(), Quantity(cpu_lim.into())),
				("memory".into(), Quantity(mem_lim.into())),
			])),
			..Default::default()
		}),
		affinity: None,
		tolerations: Vec::new(),
	};

	build_postgres_deployment_with(&inputs)
}

/// Rough Quantity comparison for the shm-size floor. Both sides are
/// short mebibyte-suffix strings we generated; if parsing fails we treat
/// LHS as smaller so the intent floor wins (safer default).
fn quantity_ge(a: &Quantity, b: &Quantity) -> bool {
	let parse = |q: &Quantity| -> Option<u64> {
		let s = q.0.trim_end_matches("Mi").trim_end_matches("Gi");
		s.parse::<u64>()
			.ok()
			.map(|n| if q.0.ends_with("Gi") { n * 1024 } else { n })
	};
	match (parse(a), parse(b)) {
		(Some(av), Some(bv)) => av >= bv,
		_ => false,
	}
}

/// ClusterIP Service exposing the postgres Deployment inside the
/// replica namespace. Selects by the same labels the shared Deployment
/// builder puts on the pod template.
pub fn build_canopy_postgres_service(namespace: &str) -> Service {
	let mut selector = BTreeMap::new();
	selector.insert("app.kubernetes.io/name".into(), "postgres".into());
	selector.insert("pgro.bes.au/canopy-replica".into(), "true".into());
	Service {
		metadata: ObjectMeta {
			name: Some(POSTGRES_SERVICE_NAME.into()),
			namespace: Some(namespace.into()),
			..Default::default()
		},
		spec: Some(ServiceSpec {
			selector: Some(selector),
			ports: Some(vec![ServicePort {
				name: Some("postgres".into()),
				port: POSTGRES_PORT,
				target_port: Some(IntOrString::String("postgres".into())),
				protocol: Some("TCP".into()),
				..Default::default()
			}]),
			..Default::default()
		}),
		..Default::default()
	}
}

/// Very small shell quoter for the connect args. kopia args have no `'` or
/// null bytes; the strings are canopy-supplied bucket/region/etc. + our
/// hardcoded dummy keys, so this is defensive rather than needed for
/// correctness.
fn shlex_quote(s: &str) -> String {
	if s.is_empty()
		|| s.chars()
			.any(|c| !(c.is_ascii_alphanumeric() || "-_.=/:[]@".contains(c)))
	{
		// Wrap in single quotes; there is no way for a `'` to appear in the
		// kopia arg space we build, so a naive single-quote wrap is safe.
		format!("'{s}'")
	} else {
		s.to_string()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use uuid::Uuid;

	fn worklist_entry() -> WorklistEntry {
		serde_json::from_value(serde_json::json!({
			"replica_id": Uuid::new_v4().to_string(),
			"group_id": Uuid::new_v4().to_string(),
			"server_id": Uuid::new_v4().to_string(),
			"type": "tamanu-postgres",
			"intent": "verify",
			"name": "test",
			"snapshot_id": "abc123",
			"snapshot_at": "2026-07-01T00:00:00Z",
			"storage": "s3",
			"bucket": "canopy-test",
			"prefix": "",
			"region": "ap-southeast-2",
		}))
		.unwrap()
	}

	#[test]
	fn build_canopy_restore_job_shape() {
		let entry = worklist_entry();
		let cfg = CanopyRestoreJobConfig {
			entry: &entry,
			namespace: "pgro-r-abc",
			job_name: "restore-1",
			kopia_image: "kopia/kopia:0.22.3",
			canopy_proxy_image: "ghcr.io/beyondessential/postgres-restore-operator:latest",
			broker_base_url: "http://postgres-restore-operator.pgro-system.svc:9091",
			stats_callback_url: "http://postgres-restore-operator.pgro-system.svc:8080/api/v1/canopy-stats/pgro-r-abc/restore-1",
			snapshot_id: "abc123",
			repo_password: "supersecret",
			pgdata_pvc_size: "10Gi",
		};
		let job = build_canopy_restore_job(&cfg);

		let spec = job.spec.as_ref().unwrap();
		let pod_spec = spec.template.spec.as_ref().unwrap();
		assert_eq!(pod_spec.containers.len(), 2);
		assert_eq!(pod_spec.containers[0].name, "restore");
		assert_eq!(pod_spec.containers[1].name, "canopy-proxy");

		// Sidecar gets the broker URL.
		let sidecar_env = pod_spec.containers[1].env.as_ref().unwrap();
		assert!(sidecar_env.iter().any(|e| e.name == "PGRO_BROKER_URL"
			&& e.value.as_deref()
				== Some("http://postgres-restore-operator.pgro-system.svc:9091")));
		assert!(
			sidecar_env
				.iter()
				.any(|e| e.name == "PGRO_TYPE" && e.value.as_deref() == Some("tamanu-postgres"))
		);

		// Pod is labeled for the broker NetworkPolicy.
		let pod_labels = job
			.spec
			.as_ref()
			.unwrap()
			.template
			.metadata
			.as_ref()
			.unwrap()
			.labels
			.as_ref()
			.unwrap();
		assert_eq!(
			pod_labels
				.get(PROXY_SIDECAR_POD_LABEL.0)
				.map(String::as_str),
			Some(PROXY_SIDECAR_POD_LABEL.1),
		);

		// Kopia container has the SNAPSHOT_ID env and mounts the shared proxy-port emptyDir.
		let kopia_env = pod_spec.containers[0].env.as_ref().unwrap();
		assert!(
			kopia_env
				.iter()
				.any(|e| e.name == "SNAPSHOT_ID" && e.value.as_deref() == Some("abc123"))
		);
		// KOPIA_PASSWORD must be on the container env so `snapshot restore`
		// (a separate process from `repository connect`) can re-open the
		// repository without an interactive password prompt.
		assert!(
			kopia_env
				.iter()
				.any(|e| e.name == "KOPIA_PASSWORD" && e.value.as_deref() == Some("supersecret"))
		);
		let kopia_mounts = pod_spec.containers[0].volume_mounts.as_ref().unwrap();
		assert!(
			kopia_mounts
				.iter()
				.any(|m| m.name == "proxy-shared" && m.mount_path == "/var/run/pgro")
		);

		// Shell script wraps the connect args and reads the port file.
		let script = pod_spec.containers[0].args.as_ref().unwrap()[0].clone();
		assert!(script.contains("/var/run/pgro/proxy-port"));
		assert!(script.contains("kopia snapshot restore"));
		assert!(script.contains("--disable-tls"));
	}

	#[test]
	fn shlex_quote_leaves_simple_strings_bare() {
		assert_eq!(shlex_quote("abc123-_"), "abc123-_");
		assert_eq!(
			shlex_quote("--endpoint=[::1]:1234"),
			"--endpoint=[::1]:1234"
		);
	}

	#[test]
	fn shlex_quote_wraps_specials() {
		assert_eq!(shlex_quote("hello world"), "'hello world'");
		assert_eq!(shlex_quote(""), "''");
	}

	#[test]
	fn build_pgdata_pvc_has_size_and_rwo() {
		let pvc = build_pgdata_pvc("ns", "20Gi");
		let spec = pvc.spec.unwrap();
		assert_eq!(spec.access_modes, Some(vec!["ReadWriteOnce".into()]));
		let req = spec.resources.unwrap().requests.unwrap();
		assert_eq!(req.get("storage"), Some(&Quantity("20Gi".into())));
		assert_eq!(pvc.metadata.name, Some(PGDATA_PVC_NAME.into()));
		assert_eq!(pvc.metadata.namespace, Some("ns".into()));
	}
}
