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
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, EmptyDirVolumeSource, EnvVar, PersistentVolumeClaim,
			PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, ResourceRequirements, Volume,
			VolumeMount, VolumeResourceRequirements,
		},
	},
	apimachinery::pkg::api::resource::Quantity,
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
/// is essentially instant), reads the port, then invokes kopia against the
/// loopback endpoint.
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
				vec![EnvVar {
					name: "SNAPSHOT_ID".into(),
					value: Some(cfg.snapshot_id.into()),
					..Default::default()
				}],
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
			canopy_proxy_image: "ghcr.io/beyondessential/pgro-canopy-proxy:latest",
			broker_base_url: "http://postgres-restore-operator.pgro-system.svc:9091",
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
