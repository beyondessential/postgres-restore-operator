use std::collections::BTreeMap;

use k8s_openapi::{
	api::{
		apps::v1::{Deployment, DeploymentSpec},
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, ContainerPort, EmptyDirVolumeSource, EnvVar, ExecAction,
			PersistentVolumeClaim, PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe,
			ResourceRequirements, SecretReference, Volume, VolumeMount, VolumeResourceRequirements,
		},
	},
	apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
};
use kube::{ResourceExt, api::ObjectMeta};
use kube_quantity::ParsedQuantity;
use rust_decimal::Decimal;
use tracing::warn;

use super::restore_owner_reference;
use crate::{
	controllers::{env_from_secret, env_from_secret_optional, kopia_writable_env},
	error::{Error, Result},
	kopia::KopiaSource,
	quantity::compute_shm_and_shared_buffers,
	types::*,
};

/// Standard label used on Pods that carry a canopy-proxy sidecar so the
/// operator's broker NetworkPolicy can admit their ingress.
pub const PROXY_SIDECAR_POD_LABEL: (&str, &str) = ("pgro.bes.au/proxy-sidecar", "true");

/// Extra inputs the canopy path needs on top of the legacy args.
pub struct CanopyProxyArgs<'a> {
	/// Image of the canopy-proxy sidecar (same image as the operator; the
	/// container runs the `canopy-proxy` binary instead of `operator`).
	pub image: &'a str,
	/// Base URL the sidecar hits for STS creds
	/// (e.g. `http://postgres-restore-operator.pgro-system.svc:9091`).
	pub broker_base_url: &'a str,
	/// Callback URL the sidecar POSTs its final TrafficStats to on shutdown.
	pub stats_callback_url: &'a str,
	/// Callback URL the sidecar POSTs in-flight progress samples to, for the
	/// operator to relay to canopy. `None` disables sampling — used for the
	/// short snapshot-list job, where there is no download worth watching.
	pub progress_callback_url: Option<&'a str>,
	/// Canopy run-uuid for this restore run, passed to the sidecar as
	/// `PGRO_RUN_ID` so its credential requests are attributed to the run.
	/// `None` for non-run credential consumers (e.g. the snapshot-list job,
	/// which is discovery rather than a restore run).
	pub run_id: Option<&'a str>,
}

/// Name of the credential-reset Job for a given restore.
pub fn credential_reset_job_name(restore_name: &str) -> String {
	format!("{restore_name}-cred-reset")
}

/// Build a Job that resets the analytics user's password on a restore whose
/// Postgres deployment has been scaled to zero.
///
/// The job mounts the restore's data PVC directly, starts a temporary
/// `postgres --single` process (no TCP listener, no auth), runs the
/// ALTER ROLE statement, then exits. The deployment must already be scaled
/// to 0 before this job is created so that the PVC is not in use.
pub fn build_credential_reset_job(
	restore: &PostgresPhysicalRestore,
	replica: &PostgresPhysicalReplica,
	job_name: &str,
	namespace: &str,
) -> Result<Job> {
	let pvc_name = format!("{}-data", restore.name_any());

	let pg_version = restore
		.status
		.as_ref()
		.and_then(|s| s.postgres_version.as_ref())
		.cloned()
		.ok_or_else(|| Error::MissingField("status.postgresVersion".to_string()))?;

	let pg_image = format!("postgres:{pg_version}");

	let creds_secret = SecretReference {
		name: Some(format!("{}-creds", restore.spec.replica.name)),
		namespace: Some(namespace.to_string()),
	};

	// ANALYTICS_PASSWORD is operator-generated (not user input), so direct
	// shell interpolation into the SQL string is safe.
	let script = r#"set -e
PGDATA=/pgdata/pgdata

echo "Resetting analytics user password via single-user mode..."
echo "ALTER ROLE ${ANALYTICS_USERNAME} WITH PASSWORD '${ANALYTICS_PASSWORD}';" \
  | postgres --single -D "$PGDATA" postgres

echo "Credential reset complete."
"#
	.to_string();

	let labels = BTreeMap::from([
		(
			"pgro.bes.au/replica".to_string(),
			restore.spec.replica.name.clone(),
		),
		("pgro.bes.au/restore".to_string(), restore.name_any()),
		("pgro.bes.au/job-type".to_string(), "cred-reset".to_string()),
	]);

	Ok(Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(labels.clone()),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(2),
			active_deadline_seconds: Some(120),
			ttl_seconds_after_finished: Some(600),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(labels),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
						run_as_user: Some(999),
						run_as_group: Some(999),
						fs_group: Some(999),
						..Default::default()
					}),
					containers: vec![Container {
						name: "cred-reset".to_string(),
						image: Some(pg_image),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![script]),
						env: Some(vec![
							EnvVar {
								name: "ANALYTICS_USERNAME".to_string(),
								value: Some(replica.spec.analytics_username.clone()),
								..Default::default()
							},
							env_from_secret("ANALYTICS_PASSWORD", &creds_secret, "password"),
						]),
						volume_mounts: Some(vec![VolumeMount {
							name: "pgdata".to_string(),
							mount_path: "/pgdata".to_string(),
							..Default::default()
						}]),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("100m".to_string())),
								("memory".to_string(), Quantity("128Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("500m".to_string())),
								("memory".to_string(), Quantity("256Mi".to_string())),
							])),
							..Default::default()
						}),
						..Default::default()
					}],
					volumes: Some(vec![Volume {
						name: "pgdata".to_string(),
						persistent_volume_claim: Some(
							k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
								claim_name: pvc_name,
								read_only: Some(false),
							},
						),
						..Default::default()
					}]),
					affinity: replica.spec.affinity.clone(),
					tolerations: Some(replica.spec.tolerations.clone()),
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}

pub fn build_version_detect_job(
	restore: &PostgresPhysicalRestore,
	job_name: &str,
	namespace: &str,
	pvc_name: &str,
) -> Job {
	let script = r#"set -e

echo "PVC contents:"
ls -la /pgdata/ 2>&1 || true
echo "---"
ls -la /pgdata/pgdata/ 2>&1 || true
echo "---"

# If the pgdata symlink already exists, just read the version
if [ -L /pgdata/pgdata ] && [ -f /pgdata/pgdata/PG_VERSION ]; then
  VERSION=$(cat /pgdata/pgdata/PG_VERSION)
  echo "Detected postgres version: $VERSION"
  echo -n "$VERSION" > /dev/termination-log
  exit 0
fi

# Otherwise locate PGDATA and recreate the symlink
echo "pgdata symlink missing, locating PGDATA directory..."
PGDATA_DIR=""

# Prefer 'current' symlink (org convention)
if [ -L /pgdata/postgres/current ]; then
  LINK_TARGET=$(readlink /pgdata/postgres/current)
  RELATIVE=$(echo "$LINK_TARGET" | sed 's|.*/\([0-9]\{1,\}/\)|/pgdata/postgres/\1|')
  if [ -f "$RELATIVE/PG_VERSION" ]; then
    PGDATA_DIR="$RELATIVE"
    echo "Found PGDATA via 'current' symlink: $PGDATA_DIR"
  fi
fi

# Fallback: pick the highest version directory containing PG_VERSION
# Filter to cluster-root directories only (must contain a 'global' subdirectory)
if [ -z "$PGDATA_DIR" ]; then
  PGDATA_DIR=$(find /pgdata/postgres -name "PG_VERSION" 2>/dev/null | while read -r f; do
    dir=$(dirname "$f")
    [ -d "$dir/global" ] && echo "$dir"
  done | sort -t/ -k4 -rn | head -1)
fi

# Last resort: search anywhere under /pgdata
if [ -z "$PGDATA_DIR" ]; then
  echo "Searching for PG_VERSION recursively..."
  find /pgdata -name "PG_VERSION" 2>/dev/null || true
  PGDATA_DIR=$(find /pgdata -name "PG_VERSION" 2>/dev/null | while read -r f; do
    dir=$(dirname "$f")
    [ -d "$dir/global" ] && echo "$dir"
  done | sort -t/ -k4 -rn | head -1)
fi

if [ -z "$PGDATA_DIR" ]; then
  echo "ERROR: Could not detect postgres version from PVC"
  exit 1
fi

echo "Found PGDATA at: $PGDATA_DIR"
ln -sfn "$PGDATA_DIR" /pgdata/pgdata

VERSION=$(cat /pgdata/pgdata/PG_VERSION)
echo "Detected postgres version: $VERSION"
echo "$VERSION" > /pgdata/.postgres-version
echo -n "$VERSION" > /dev/termination-log
"#;

	Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				(
					"pgro.bes.au/replica".to_string(),
					restore.spec.replica.name.clone(),
				),
				("pgro.bes.au/restore".to_string(), restore.name_any()),
				(
					"pgro.bes.au/job-type".to_string(),
					"version-detect".to_string(),
				),
			])),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(2),
			active_deadline_seconds: Some(120),
			ttl_seconds_after_finished: Some(120),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(BTreeMap::from([
						(
							"pgro.bes.au/replica".to_string(),
							restore.spec.replica.name.clone(),
						),
						("pgro.bes.au/restore".to_string(), restore.name_any()),
					])),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
						run_as_user: Some(999),
						run_as_group: Some(999),
						fs_group: Some(999),
						..Default::default()
					}),
					containers: vec![Container {
						name: "version-detect".to_string(),
						image: Some("alpine:latest".to_string()),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![script.to_string()]),
						volume_mounts: Some(vec![VolumeMount {
							name: "pgdata".to_string(),
							mount_path: "/pgdata".to_string(),
							..Default::default()
						}]),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("10m".to_string())),
								("memory".to_string(), Quantity("16Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("50m".to_string())),
								("memory".to_string(), Quantity("32Mi".to_string())),
							])),
							..Default::default()
						}),
						..Default::default()
					}],
					volumes: Some(vec![Volume {
						name: "pgdata".to_string(),
						persistent_volume_claim: Some(
							k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
								claim_name: pvc_name.to_string(),
								read_only: Some(false),
							},
						),
						..Default::default()
					}]),
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	}
}

pub fn build_pvc(
	restore: &PostgresPhysicalRestore,
	pvc_name: &str,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<PersistentVolumeClaim> {
	Ok(PersistentVolumeClaim {
		metadata: ObjectMeta {
			name: Some(pvc_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				(
					"pgro.bes.au/replica".to_string(),
					restore.spec.replica.name.clone(),
				),
				("pgro.bes.au/restore".to_string(), restore.name_any()),
			])),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(PersistentVolumeClaimSpec {
			access_modes: Some(vec!["ReadWriteOnce".to_string()]),
			storage_class_name: replica.spec.storage_class.clone(),
			resources: Some(VolumeResourceRequirements {
				requests: Some(BTreeMap::from([(
					"storage".to_string(),
					restore.spec.storage_size.clone(),
				)])),
				..Default::default()
			}),
			..Default::default()
		}),
		..Default::default()
	})
}

/// Name of the per-replica kopia cache PVC. One per replica, reused across
/// every restore Job for that replica — kopia's content cache then survives
/// restore-to-restore and the next snapshot only has to download new
/// blobs. Owned by the replica (cascade-deleted with it), unlike the
/// per-restore data PVC.
pub fn kopia_cache_pvc_name(replica_name: &str) -> String {
	format!("{replica_name}-kopia-cache")
}

/// Returns true if `desired` is strictly larger than `current`. Used by
/// the restore controller to apply ratchet semantics to the cache PVC:
/// grow on snapshot growth, never shrink. Returns false on parse error
/// so a corrupt or unparsable quantity doesn't cause spurious resize
/// attempts.
pub fn cache_size_needs_grow(current: &Quantity, desired: &Quantity) -> bool {
	match (
		ParsedQuantity::try_from(current.clone()),
		ParsedQuantity::try_from(desired.clone()),
	) {
		(Ok(c), Ok(d)) => d > c,
		_ => false,
	}
}

/// Compute the per-replica kopia cache PVC size as `max(10Gi, 20% of
/// snapshot size)`. Kopia caches snapshot metadata, indices, and content
/// blobs; sizing relative to the snapshot scales naturally with the data
/// volume, and the 10Gi floor catches tiny snapshots where 20% would
/// leave no room for incremental churn.
pub fn kopia_cache_pvc_size(snapshot_size: &Quantity) -> Quantity {
	let twenty_percent = ParsedQuantity::try_from(snapshot_size.clone())
		.map(|q| q * Decimal::new(2, 1))
		.unwrap_or_else(|_| ParsedQuantity::from(Decimal::ZERO));
	let floor = ParsedQuantity::try_from("10Gi").expect("10Gi parses");
	let chosen = if twenty_percent > floor {
		twenty_percent
	} else {
		floor
	};
	chosen.into()
}

/// Minimum MB to leave free on the cache PVC. Combined with the
/// proportional reserve below — the actual reserve is the max of this
/// floor and the proportional value.
pub const KOPIA_CACHE_RESERVE_MIN_MB: u64 = 2048;
/// Fraction of the PVC capacity reserved for everything other than the
/// content cache: ext4's ~5% reserved blocks, the metadata cache, kopia's
/// CLI + content logs, the live config, and the soft-cap overshoot that
/// happens when kopia downloads new content faster than its LRU eviction
/// can keep up. A fixed 2GB reserve broke down on a 10Gi PVC (88% full,
/// failed restores) and a ~22Gi PVC (100% full); scaling with PVC size
/// keeps the headroom proportionate.
pub const KOPIA_CACHE_RESERVE_FRACTION: f64 = 0.30;
/// Hardcoded metadata-cache cap. Kopia's metadata cache is small
/// (indices and manifest data) so a fixed allocation is fine.
pub const KOPIA_METADATA_CACHE_MB: u64 = 512;
/// Floor for the content-cache cap in MB, so degenerate-small PVCs
/// still get a useful cache.
pub const KOPIA_CONTENT_CACHE_FLOOR_MB: u64 = 1024;

/// Compute the content-cache cap (MB) passed to `kopia repository
/// connect --content-cache-size-mb`. Sized to the cache PVC minus a
/// reserve for metadata cache + logs + ext4 reserved blocks + soft-cap
/// overshoot slop.
///
/// Without a cap kopia's content cache grows unbounded and eventually
/// fills the PVC, after which kopia can't even write its config and
/// every restore Job pod exits in 1–2 minutes. The cap turns that into
/// kopia's own LRU eviction, which is what we want.
pub fn kopia_content_cache_mb(snapshot_size: &Quantity) -> u64 {
	let pvc_size = kopia_cache_pvc_size(snapshot_size);
	let pvc_bytes = ParsedQuantity::try_from(pvc_size)
		.ok()
		.and_then(|q| q.to_bytes_f64())
		.unwrap_or(0.0);
	let pvc_mb = (pvc_bytes / 1024.0 / 1024.0) as u64;
	let proportional_reserve = ((pvc_mb as f64) * KOPIA_CACHE_RESERVE_FRACTION) as u64;
	let reserve = proportional_reserve.max(KOPIA_CACHE_RESERVE_MIN_MB);
	pvc_mb
		.saturating_sub(reserve)
		.max(KOPIA_CONTENT_CACHE_FLOOR_MB)
}

/// Maximum cache PVC size the operator will grow to in response to
/// cache-pressure signals. Caps [`next_cache_pvc_size_after_pressure`]
/// so a pathological scenario (e.g. a leak in the pre-flight cleanup
/// logic) can't grow the PVC unboundedly. 2× the default gives roughly
/// 40% of snapshot as cache PVC — well beyond any observed working set.
pub fn kopia_cache_pvc_max(snapshot_size: &Quantity) -> Quantity {
	let default = kopia_cache_pvc_size(snapshot_size);
	let default_pq =
		ParsedQuantity::try_from(default).unwrap_or_else(|_| ParsedQuantity::from(Decimal::ZERO));
	let two_x = default_pq * Decimal::from(2);
	two_x.into()
}

/// Multiplicative bump applied to the cache PVC's requested storage each
/// time a restore Job reports `PGRO_CACHE_PRESSURE` in its log. 1.15 →
/// ~5 pressure events to grow from the snapshot-derived default to the
/// 2× cap. Slow enough that one-off spikes don't accidentally double the
/// PVC; fast enough that a chronically-pressured replica self-tunes
/// within a few restore cycles.
pub const KOPIA_CACHE_PRESSURE_GROWTH_FACTOR: Decimal = Decimal::from_parts(115, 0, 0, false, 2);

/// Given the current cache PVC's requested storage, compute the next
/// requested storage after a cache-pressure event. Multiplies by
/// [`KOPIA_CACHE_PRESSURE_GROWTH_FACTOR`] and caps at
/// [`kopia_cache_pvc_max`]. Never shrinks: if `current` is already
/// at or above the cap, returns the cap.
pub fn next_cache_pvc_size_after_pressure(
	current: &Quantity,
	snapshot_size: &Quantity,
) -> Quantity {
	let Ok(current_pq) = ParsedQuantity::try_from(current.clone()) else {
		return current.clone();
	};
	let bumped = current_pq.clone() * KOPIA_CACHE_PRESSURE_GROWTH_FACTOR;
	let max_pq = ParsedQuantity::try_from(kopia_cache_pvc_max(snapshot_size))
		.unwrap_or_else(|_| current_pq.clone());
	let chosen = if bumped > max_pq { max_pq } else { bumped };
	chosen.into()
}

pub fn build_kopia_cache_pvc(
	replica: &PostgresPhysicalReplica,
	snapshot_size: &Quantity,
	namespace: &str,
) -> PersistentVolumeClaim {
	let replica_name = replica.name_any();
	PersistentVolumeClaim {
		metadata: ObjectMeta {
			name: Some(kopia_cache_pvc_name(&replica_name)),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("pgro.bes.au/replica".to_string(), replica_name),
				(
					"pgro.bes.au/component".to_string(),
					"kopia-cache".to_string(),
				),
			])),
			owner_references: Some(vec![replica.owner_reference()]),
			..Default::default()
		},
		spec: Some(PersistentVolumeClaimSpec {
			access_modes: Some(vec!["ReadWriteOnce".to_string()]),
			storage_class_name: replica.spec.storage_class.clone(),
			resources: Some(VolumeResourceRequirements {
				requests: Some(BTreeMap::from([(
					"storage".to_string(),
					kopia_cache_pvc_size(snapshot_size),
				)])),
				..Default::default()
			}),
			..Default::default()
		}),
		..Default::default()
	}
}

pub fn build_restore_job(
	restore: &PostgresPhysicalRestore,
	job_name: &str,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	kopia_image: &str,
	cache_pressure_callback_url: &str,
	canopy_proxy: Option<&CanopyProxyArgs<'_>>,
) -> Result<Job> {
	let source = replica.kopia_source();
	let kopia_secret = SecretReference {
		name: Some(source.secret_name().to_string()),
		namespace: None,
	};
	let pvc_name = format!("{}-data", restore.name_any());
	let cache_pvc_name = kopia_cache_pvc_name(&restore.spec.replica.name);

	// Canopy prelude waits for the sidecar to bind, then exports the
	// loopback endpoint/disable-tls so the main script's connect step
	// picks them up via ENDPOINT_ARGS. No-op on the legacy path.
	let canopy_prelude = if source.is_canopy_proxy() {
		r#"PORT_FILE="/var/run/pgro/proxy-port"
for _ in $(seq 1 30); do
  [ -f "$PORT_FILE" ] && break
  sleep 1
done
if [ ! -f "$PORT_FILE" ]; then
  echo "ERROR: canopy-proxy sidecar did not write port file within 30s" >&2
  exit 1
fi
export KOPIA_ENDPOINT="[::1]:$(cat "$PORT_FILE")"
export KOPIA_DISABLE_TLS=true
echo "kopia connecting via canopy proxy at ${KOPIA_ENDPOINT}"

"#
	} else {
		""
	};

	let restore_script_body = r#"set -e

mkdir -p /tmp/kopia/config /tmp/kopia/logs /tmp/kopia/cache

# Pre-flight: if the cache PVC is critically full, evict everything kopia
# can regenerate from S3 before starting. Without this, kopia exits in
# 1-2 seconds with "no space left on device" when it can't write its
# config file, and the restore Job retries forever. The 85% threshold
# is well below ext4 reserved-blocks territory but high enough that a
# healthy cache isn't evicted on every run. Also POST to the operator's
# cache-pressure callback so it can bump the PVC's requested storage —
# chronically-pressured replicas self-tune over a few restore cycles.
USAGE_PCT=$(df -P /tmp/kopia | awk 'NR==2 {gsub("%","",$5); print $5}')
if [ -n "$USAGE_PCT" ] && [ "$USAGE_PCT" -ge 85 ]; then
  echo "PGRO_CACHE_PRESSURE: cache PVC ${USAGE_PCT}% full — evicting regenerable content"
  rm -rf /tmp/kopia/cache /tmp/kopia/logs/content-logs
  mkdir -p /tmp/kopia/cache /tmp/kopia/logs
  if [ -n "$CACHE_PRESSURE_CALLBACK_URL" ]; then
    HTTP_CODE=$(curl -fsS -o /dev/stderr -w '%{http_code}' -X POST \
      "$CACHE_PRESSURE_CALLBACK_URL" 2>&1) || true
    echo "cache-pressure callback: HTTP $HTTP_CODE" >&2
  fi
fi

ENDPOINT_ARGS=""
if [ -n "$KOPIA_ENDPOINT" ]; then
  ENDPOINT_ARGS="--endpoint=$KOPIA_ENDPOINT"
fi
if [ "$KOPIA_DISABLE_TLS" = "true" ]; then
  ENDPOINT_ARGS="$ENDPOINT_ARGS --disable-tls --disable-tls-verification"
fi

# Global kopia flags applied to every invocation: rotate CLI logs so
# they don't fill the cache PVC. Cap at 20 most-recent files and
# 24 hours, plenty for debugging a current restore without growing
# without bound.
KOPIA_GLOBAL_FLAGS="--log-dir-max-files=20 --log-dir-max-age=24h"

echo "Connecting to kopia repository..."
# --content-cache-size-mb / --metadata-cache-size-mb are persisted to
# the local config on connect, so subsequent operations inherit the
# bound. Without them kopia's content cache grows unbounded and
# eventually fills the cache PVC (observed across multiple replicas).
kopia $KOPIA_GLOBAL_FLAGS repository connect s3 \
  --bucket="$KOPIA_BUCKET" \
  --region="$KOPIA_REGION" \
  --access-key="$AWS_ACCESS_KEY_ID" \
  --secret-access-key="$AWS_SECRET_ACCESS_KEY" \
  --password="$KOPIA_PASSWORD" \
  --content-cache-size-mb="$KOPIA_CONTENT_CACHE_MB" \
  --metadata-cache-size-mb="$KOPIA_METADATA_CACHE_MB" \
  $ENDPOINT_ARGS

echo "Starting restore..."
kopia $KOPIA_GLOBAL_FLAGS snapshot restore --parallel="$KOPIA_PARALLEL" "$SNAPSHOT_ID" /pgdata/postgres

echo "Restore complete"
ls -la /pgdata/

echo "Locating PGDATA directory..."

# Prefer the 'current' symlink if it exists (org convention)
if [ -L /pgdata/postgres/current ]; then
  # The symlink target is an absolute path from the original host, resolve it
  # relative to /pgdata/postgres by extracting the version/cluster part.
  LINK_TARGET=$(readlink /pgdata/postgres/current)
  # e.g. /var/lib/postgresql/16/main -> try /pgdata/postgres/16/main
  RELATIVE=$(echo "$LINK_TARGET" | sed 's|.*/\([0-9]\{1,\}/\)|/pgdata/postgres/\1|')
  if [ -f "$RELATIVE/PG_VERSION" ]; then
    PGDATA_DIR="$RELATIVE"
    echo "Found PGDATA via 'current' symlink: $PGDATA_DIR"
  fi
fi

# Fallback: pick the highest version directory containing PG_VERSION
# Filter to cluster-root directories only (must contain a 'global' subdirectory)
if [ -z "$PGDATA_DIR" ]; then
  PGDATA_DIR=$(find /pgdata/postgres -name "PG_VERSION" 2>/dev/null | while read -r f; do
    dir=$(dirname "$f")
    [ -d "$dir/global" ] && echo "$dir"
  done | sort -t/ -k4 -rn | head -1)
fi

if [ -z "$PGDATA_DIR" ]; then
  echo "ERROR: Could not find PG_VERSION in restored data"
  exit 1
fi
echo "Found PGDATA at: $PGDATA_DIR"
ln -sfn "$PGDATA_DIR" /pgdata/pgdata
rm -f "$PGDATA_DIR/postmaster.pid"

VERSION=$(cat /pgdata/pgdata/PG_VERSION)
echo "Detected postgres version: $VERSION"
echo "$VERSION" > /pgdata/.postgres-version
echo -n "$VERSION" > /dev/termination-log
"#;

	let restore_script = format!("{canopy_prelude}{restore_script_body}");

	// Base env: shared between legacy and canopy. env_from_secret uses the
	// same key names in both Secrets (bucket, region, accessKeyId,
	// secretAccessKey, repositoryPassword); the canopy path just fills
	// AWS creds with dummy values because the proxy re-signs upstream.
	let mut env: Vec<EnvVar> = [
		vec![
			EnvVar {
				name: "SNAPSHOT_ID".to_string(),
				value: Some(restore.spec.snapshot.clone()),
				..Default::default()
			},
			EnvVar {
				name: "KOPIA_CONTENT_CACHE_MB".to_string(),
				value: Some(kopia_content_cache_mb(&restore.spec.snapshot_size).to_string()),
				..Default::default()
			},
			EnvVar {
				name: "KOPIA_METADATA_CACHE_MB".to_string(),
				value: Some(KOPIA_METADATA_CACHE_MB.to_string()),
				..Default::default()
			},
			EnvVar {
				name: "KOPIA_PARALLEL".to_string(),
				value: Some(RESTORE_JOB_CPU_LIMIT.to_string()),
				..Default::default()
			},
			EnvVar {
				name: "CACHE_PRESSURE_CALLBACK_URL".to_string(),
				value: Some(cache_pressure_callback_url.to_string()),
				..Default::default()
			},
		],
		kopia_writable_env(),
		vec![
			env_from_secret("KOPIA_BUCKET", &kopia_secret, "bucket"),
			env_from_secret("KOPIA_REGION", &kopia_secret, "region"),
			env_from_secret("AWS_ACCESS_KEY_ID", &kopia_secret, "accessKeyId"),
			env_from_secret("AWS_SECRET_ACCESS_KEY", &kopia_secret, "secretAccessKey"),
			env_from_secret("KOPIA_PASSWORD", &kopia_secret, "repositoryPassword"),
		],
	]
	.concat();

	// Legacy Secrets may carry KOPIA_ENDPOINT / KOPIA_DISABLE_TLS as an
	// escape hatch (e.g. for MinIO); canopy sets them via the shell prelude
	// after the proxy binds, so we skip these optional keys on the canopy
	// path to avoid a needless lookup.
	if !source.is_canopy_proxy() {
		env.push(env_from_secret_optional(
			"KOPIA_ENDPOINT",
			&kopia_secret,
			"endpoint",
		));
		env.push(env_from_secret_optional(
			"KOPIA_DISABLE_TLS",
			&kopia_secret,
			"disableTls",
		));
	}

	let volume_mounts = vec![
		VolumeMount {
			name: "pgdata".to_string(),
			mount_path: "/pgdata".to_string(),
			..Default::default()
		},
		VolumeMount {
			name: "kopia-cache".to_string(),
			mount_path: "/tmp/kopia".to_string(),
			..Default::default()
		},
	];
	let mut volumes = vec![
		Volume {
			name: "pgdata".to_string(),
			persistent_volume_claim: Some(
				k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
					claim_name: pvc_name,
					read_only: Some(false),
				},
			),
			..Default::default()
		},
		Volume {
			name: "kopia-cache".to_string(),
			persistent_volume_claim: Some(
				k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
					claim_name: cache_pvc_name,
					read_only: Some(false),
				},
			),
			..Default::default()
		},
	];

	let mut pod_labels = BTreeMap::from([
		(
			"pgro.bes.au/replica".to_string(),
			restore.spec.replica.name.clone(),
		),
		("pgro.bes.au/restore".to_string(), restore.name_any()),
	]);

	let mut containers = Vec::with_capacity(1);
	// The canopy-proxy runs as a native sidecar (an init container with
	// restartPolicy: Always) so that when the main `restore` container
	// exits the kubelet SIGTERMs the proxy and the Pod completes on the
	// main container's exit code. A plain sidecar container would keep the
	// Pod Running forever and the Job would never succeed.
	let mut init_containers: Vec<Container> = Vec::new();
	containers.push(Container {
		name: "restore".to_string(),
		image: Some(kopia_image.to_string()),
		command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
		args: Some(vec![restore_script]),
		env: Some(env),
		volume_mounts: Some(volume_mounts),
		resources: Some(restore_job_resources(&restore.spec.snapshot_size)),
		..Default::default()
	});

	if let KopiaSource::CanopyProxy {
		group, backup_type, ..
	} = &source
	{
		let proxy = canopy_proxy.expect(
			"build_restore_job called with canopy_source but no CanopyProxyArgs; caller must \
			 thread proxy config from Context when replica.spec.canopy_source is set",
		);

		containers[0]
			.volume_mounts
			.as_mut()
			.unwrap()
			.push(VolumeMount {
				name: "proxy-shared".to_string(),
				mount_path: "/var/run/pgro".to_string(),
				..Default::default()
			});

		init_containers.push(Container {
			name: "canopy-proxy".to_string(),
			image: Some(proxy.image.to_string()),
			// Native sidecar: an init container that never exits on its
			// own. `restartPolicy: Always` tells the kubelet to keep it
			// running alongside the main containers and to SIGTERM it once
			// they complete.
			restart_policy: Some("Always".to_string()),
			// Same image as the operator; run the `canopy-proxy` binary
			// instead of the default `operator` entrypoint.
			command: Some(vec!["canopy-proxy".to_string()]),
			env: Some({
				let mut env = vec![
					EnvVar {
						name: "PGRO_BROKER_URL".to_string(),
						value: Some(proxy.broker_base_url.to_string()),
						..Default::default()
					},
					EnvVar {
						name: "PGRO_GROUP".to_string(),
						value: Some(group.clone()),
						..Default::default()
					},
					EnvVar {
						name: "PGRO_TYPE".to_string(),
						value: Some(backup_type.clone()),
						..Default::default()
					},
					// Region comes from the canopy-creds Secret the syncer
					// materialises alongside this Job; the sidecar signs S3
					// requests for it before forwarding upstream.
					crate::controllers::jobs::env_from_secret_name(
						"PGRO_REGION",
						source.secret_name(),
						"region",
					),
					EnvVar {
						name: "PGRO_STATS_CALLBACK_URL".to_string(),
						value: Some(proxy.stats_callback_url.to_string()),
						..Default::default()
					},
				];
				if let Some(run_id) = proxy.run_id {
					env.push(EnvVar {
						name: "PGRO_RUN_ID".to_string(),
						value: Some(run_id.to_string()),
						..Default::default()
					});
				}
				if let Some(url) = proxy.progress_callback_url {
					env.push(EnvVar {
						name: "PGRO_PROGRESS_CALLBACK_URL".to_string(),
						value: Some(url.to_string()),
						..Default::default()
					});
				}
				env
			}),
			volume_mounts: Some(vec![VolumeMount {
				name: "proxy-shared".to_string(),
				mount_path: "/var/run/pgro".to_string(),
				..Default::default()
			}]),
			resources: Some(ResourceRequirements {
				requests: Some(BTreeMap::from([
					("cpu".to_string(), Quantity("50m".to_string())),
					("memory".to_string(), Quantity("64Mi".to_string())),
				])),
				limits: Some(BTreeMap::from([
					("cpu".to_string(), Quantity("500m".to_string())),
					("memory".to_string(), Quantity("256Mi".to_string())),
				])),
				..Default::default()
			}),
			..Default::default()
		});

		volumes.push(Volume {
			name: "proxy-shared".to_string(),
			empty_dir: Some(EmptyDirVolumeSource {
				medium: Some("Memory".to_string()),
				..Default::default()
			}),
			..Default::default()
		});

		pod_labels.insert(
			PROXY_SIDECAR_POD_LABEL.0.to_string(),
			PROXY_SIDECAR_POD_LABEL.1.to_string(),
		);
	}

	// Canopy's proxy refreshes creds so long restores aren't
	// credential-bounded, only reachability-bounded — bump to 4 h.
	let active_deadline_seconds = if source.is_canopy_proxy() {
		14400
	} else {
		7200
	};

	Ok(Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(pod_labels.clone()),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(3),
			active_deadline_seconds: Some(active_deadline_seconds),
			ttl_seconds_after_finished: Some(120),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(pod_labels),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
						run_as_user: Some(999),
						run_as_group: Some(999),
						fs_group: Some(999),
						..Default::default()
					}),
					init_containers: if init_containers.is_empty() {
						None
					} else {
						Some(init_containers)
					},
					// Give the native sidecar time to flush its final stats
					// on SIGTERM before the kubelet SIGKILLs it.
					termination_grace_period_seconds: Some(30),
					containers,
					volumes: Some(volumes),
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}

/// All the inputs `build_postgres_deployment_with` needs, factored out so
/// both the CRD path and the canopy path can call the shared builder
/// without pretending to be each other. The CRD path fills this from
/// `PostgresPhysicalRestore` + `PostgresPhysicalReplica`; the canopy path
/// fills it from a `WorklistEntry` + intent defaults.
pub struct PostgresDeploymentInputs<'a> {
	pub name: &'a str,
	pub namespace: &'a str,
	pub pvc_name: &'a str,
	/// Postgres major version, e.g. `"16"`. Used as the image tag.
	pub pg_version: &'a str,
	pub shm_size: Quantity,
	pub shared_buffers_mb: u64,
	/// True when the replica is meant to serve read-only clients (verify /
	/// analytics intents). Sets `default_transaction_read_only = on` and
	/// swaps the analytics-user grant to `pg_read_all_data` on PG ≥ 14.
	pub read_only: bool,
	/// Extra postgresql.conf lines appended after the base config.
	pub postgres_extra_config: Option<&'a str>,
	/// Analytics role provisioned (or updated) by the setup-auth
	/// initContainer. Its password comes from `analytics_password_secret`.
	pub analytics_username: &'a str,
	pub analytics_password_secret: &'a SecretReference,
	pub analytics_password_key: &'a str,
	pub snapshot_id: &'a str,
	pub snapshot_time: &'a str,
	pub labels: BTreeMap<String, String>,
	pub match_labels: BTreeMap<String, String>,
	pub pod_annotations: Option<BTreeMap<String, String>>,
	pub owner_references:
		Option<Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference>>,
	/// Resource requests/limits for the main postgres container.
	pub postgres_resources: Option<ResourceRequirements>,
	pub affinity: Option<k8s_openapi::api::core::v1::Affinity>,
	pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,
}

/// CRD-path Deployment builder. Fills the shared `PostgresDeploymentInputs`
/// from the restore + replica CRs and delegates to
/// [`build_postgres_deployment_with`].
pub fn build_deployment(
	restore: &PostgresPhysicalRestore,
	name: &str,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<Deployment> {
	let pvc_name = format!("{name}-data");
	// Memory scales with the snapshot this restore holds, so shm and
	// shared_buffers (both derived from it) scale with it too.
	let postgres_resources = resolve_postgres_resources(replica, &restore.spec.snapshot_size);
	let (computed_shm, computed_shared_buffers_mb) =
		compute_shm_and_shared_buffers(&postgres_resources);
	let (shm_size, shared_buffers_mb) = apply_shm_floor(
		&computed_shm,
		computed_shared_buffers_mb,
		replica.spec.shm_size_floor.as_ref(),
	);
	let creds_secret = SecretReference {
		name: Some(format!("{}-creds", restore.spec.replica.name)),
		namespace: Some(namespace.to_string()),
	};

	let pg_version = restore
		.status
		.as_ref()
		.and_then(|s| s.postgres_version.as_ref())
		.cloned()
		.ok_or_else(|| Error::MissingField("status.postgresVersion".to_string()))?;

	// persistent_schemas needs write access to receive the migrated data, and a
	// restore with a migration target exists to be written to: on PG >= 14
	// read-only grants the app user `pg_read_all_data`, which has no DDL, so
	// migrations cannot run under it whatever the transaction default says.
	// These restores are ephemeral and discarded once verified.
	let effective_read_only = replica.spec.read_only
		&& replica.spec.persistent_schemas.is_none()
		&& restore.spec.migrate_to.is_none();

	let labels = BTreeMap::from([
		(
			"pgro.bes.au/replica".to_string(),
			restore.spec.replica.name.clone(),
		),
		("pgro.bes.au/restore".to_string(), name.to_string()),
	]);
	let match_labels = BTreeMap::from([("pgro.bes.au/restore".to_string(), name.to_string())]);

	build_postgres_deployment_with(&PostgresDeploymentInputs {
		name,
		namespace,
		pvc_name: &pvc_name,
		pg_version: &pg_version,
		shm_size,
		shared_buffers_mb,
		read_only: effective_read_only,
		postgres_extra_config: replica.spec.postgres_extra_config.as_deref(),
		analytics_username: &replica.spec.analytics_username,
		analytics_password_secret: &creds_secret,
		analytics_password_key: "password",
		snapshot_id: &restore.spec.snapshot,
		snapshot_time: restore.spec.snapshot_time.as_deref().unwrap_or(""),
		labels,
		match_labels,
		pod_annotations: replica.spec.pod_annotations.clone(),
		owner_references: Some(vec![restore_owner_reference(restore)]),
		postgres_resources,
		affinity: replica.spec.affinity.clone(),
		tolerations: replica.spec.tolerations.clone(),
	})
}

/// CPU the kopia restore container is allowed to use. Also the parallelism
/// kopia is told to use: left to itself it sizes its worker pool from the CPUs
/// visible on the *node*, so it spawns several times more workers than the
/// cgroup will ever schedule.
pub const RESTORE_JOB_CPU_LIMIT: &str = "2";
const RESTORE_JOB_CPU_REQUEST: &str = "500m";
/// Floor and cap on the kopia restore container's memory. kopia streams rather
/// than buffering whole files, so its demand grows far more slowly than the
/// snapshot does — hence a much lower ratio than postgres gets.
const RESTORE_JOB_MEMORY_FLOOR: &str = "4Gi";
const RESTORE_JOB_MEMORY_CAP: &str = "16Gi";

/// Resources for the kopia restore container, with memory scaled to the
/// snapshot being restored. Falls back to the floor if the size can't be
/// parsed.
fn restore_job_resources(snapshot_size: &Quantity) -> ResourceRequirements {
	let floor = Quantity(RESTORE_JOB_MEMORY_FLOOR.to_string());
	let memory = crate::quantity::scale_memory_for_snapshot(
		snapshot_size,
		&floor,
		&Quantity(RESTORE_JOB_MEMORY_CAP.to_string()),
	)
	.and_then(|r| r.limits?.get("memory").cloned())
	.unwrap_or(floor);

	ResourceRequirements {
		requests: Some(BTreeMap::from([
			(
				"cpu".to_string(),
				Quantity(RESTORE_JOB_CPU_REQUEST.to_string()),
			),
			("memory".to_string(), Quantity("1Gi".to_string())),
		])),
		limits: Some(BTreeMap::from([
			(
				"cpu".to_string(),
				Quantity(RESTORE_JOB_CPU_LIMIT.to_string()),
			),
			("memory".to_string(), memory),
		])),
		..Default::default()
	}
}

/// Default cap on snapshot-derived postgres memory when the replica doesn't
/// set `resourcesMaximum`. Keeps a pathological snapshot from requesting more
/// than any node can offer and sitting unschedulable forever.
const DEFAULT_RESOURCES_MAXIMUM: &str = "64Gi";

/// Resolve the postgres pod's resources for a restore.
///
/// `spec.resources` pins the values outright when set. Otherwise memory is
/// derived from the snapshot size, floored by `spec.resourcesFloor` and capped
/// by `spec.resourcesMaximum`; CPU is carried over from the floor unchanged,
/// since it tracks query concurrency rather than data volume.
///
/// Falls back to the floor if the snapshot size can't be parsed — sizing off a
/// bogus value is worse than not scaling at all.
pub fn resolve_postgres_resources(
	replica: &PostgresPhysicalReplica,
	snapshot_size: &Quantity,
) -> Option<ResourceRequirements> {
	if let Some(pinned) = replica.spec.resources.as_ref() {
		return Some(pinned.clone());
	}
	let floor = replica.spec.resources_floor.as_ref()?;
	let floor_memory = floor
		.limits
		.as_ref()
		.and_then(|m| m.get("memory"))
		.cloned()
		.unwrap_or_else(|| Quantity("0".to_string()));
	let cap = replica
		.spec
		.resources_maximum
		.clone()
		.unwrap_or_else(|| Quantity(DEFAULT_RESOURCES_MAXIMUM.to_string()));

	let Some(scaled) =
		crate::quantity::scale_memory_for_snapshot(snapshot_size, &floor_memory, &cap)
	else {
		warn!(
			snapshot_size = %snapshot_size.0,
			"could not derive postgres memory from snapshot size; using the floor"
		);
		return Some(floor.clone());
	};

	// Memory from the derived value, CPU from the floor.
	let merge = |derived: Option<&BTreeMap<String, Quantity>>,
	             from_floor: Option<&BTreeMap<String, Quantity>>| {
		let mut out = BTreeMap::new();
		if let Some(cpu) = from_floor.and_then(|m| m.get("cpu")) {
			out.insert("cpu".to_string(), cpu.clone());
		}
		if let Some(memory) = derived.and_then(|m| m.get("memory")) {
			out.insert("memory".to_string(), memory.clone());
		}
		(!out.is_empty()).then_some(out)
	};

	Some(ResourceRequirements {
		requests: merge(scaled.requests.as_ref(), floor.requests.as_ref()),
		limits: merge(scaled.limits.as_ref(), floor.limits.as_ref()),
		..Default::default()
	})
}

/// Apply the caller's `shm_size_floor` (if any) to the resource-derived
/// shm + shared_buffers pair. `shared_buffers` scales linearly with shm
/// (postgres wants ~70 % of shm), so bumping shm bumps
/// `shared_buffers_mb` proportionally.
fn apply_shm_floor(
	computed_shm: &Quantity,
	computed_shared_buffers_mb: u64,
	floor: Option<&Quantity>,
) -> (Quantity, u64) {
	let Some(floor) = floor else {
		return (computed_shm.clone(), computed_shared_buffers_mb);
	};
	let computed_bytes = ParsedQuantity::try_from(computed_shm.clone())
		.ok()
		.and_then(|q| q.to_bytes_f64());
	let floor_bytes = ParsedQuantity::try_from(floor.clone())
		.ok()
		.and_then(|q| q.to_bytes_f64());
	match (computed_bytes, floor_bytes) {
		(Some(c), Some(f)) if f > c => {
			let ratio = f / c;
			let scaled_shared_buffers =
				((computed_shared_buffers_mb as f64) * ratio).floor() as u64;
			(floor.clone(), scaled_shared_buffers.max(16))
		}
		_ => (computed_shm.clone(), computed_shared_buffers_mb),
	}
}

/// Shared postgres Deployment builder — no CR dependencies. Both the
/// CRD path (`build_deployment`) and the canopy path
/// (`controllers::canopy::builders::build_canopy_postgres_deployment`) go
/// through this.
pub fn build_postgres_deployment_with(cfg: &PostgresDeploymentInputs<'_>) -> Result<Deployment> {
	let PostgresDeploymentInputs {
		name,
		namespace,
		pvc_name,
		pg_version,
		shm_size,
		shared_buffers_mb,
		read_only,
		postgres_extra_config,
		analytics_username,
		analytics_password_secret,
		analytics_password_key,
		snapshot_id,
		snapshot_time,
		labels,
		match_labels,
		pod_annotations,
		owner_references,
		postgres_resources,
		affinity,
		tolerations,
	} = cfg;
	let shm_size = shm_size.clone();
	let shared_buffers_mb = *shared_buffers_mb;
	let read_only = *read_only;

	let pg_image = format!("postgres:{pg_version}");

	let locale_script = r#"set -ex
echo "Creating Windows-compatible locales..."
for lang in \
  "English_United States" \
  "English_United Kingdom" \
; do
  localedef -i en_US -f CP1250 "${lang}.1250" 2>/dev/null || true
  localedef -i en_US -f CP1251 "${lang}.1251" 2>/dev/null || true
  localedef -i en_US -f CP1252 "${lang}.1252" 2>/dev/null || true
  localedef -i en_US -f CP1253 "${lang}.1253" 2>/dev/null || true
  localedef -i en_US -f CP1254 "${lang}.1254" 2>/dev/null || true
  localedef -i en_US -f CP1255 "${lang}.1255" 2>/dev/null || true
  localedef -i en_US -f CP1256 "${lang}.1256" 2>/dev/null || true
  localedef -i en_US -f CP1257 "${lang}.1257" 2>/dev/null || true
  localedef -i en_US -f CP1258 "${lang}.1258" 2>/dev/null || true
  localedef -i en_US -f UTF-8  "${lang}.65001" 2>/dev/null || true
done
echo "Copying locale data to shared volume..."
cp -a /usr/lib/locale/* /locale-data/
"#
	.to_string();

	let extra_config_block = if let Some(extra) = postgres_extra_config {
		format!(
			r#"echo "Appending extra postgresql.conf settings..."
cat >> "$PGDATA/postgresql.conf" << 'EXTRACONFEOF'
{extra}
EXTRACONFEOF"#
		)
	} else {
		String::new()
	};

	let init_script = format!(
		r#"set -ex
PGDATA=/pgdata/pgdata

chmod 0750 "$PGDATA"

if [ ! -f "$PGDATA/postgresql.conf" ]; then
  echo "Creating minimal postgresql.conf (Debian-style installs keep config in /etc)..."
  cat > "$PGDATA/postgresql.conf" << 'CONFEOF'
listen_addresses = '*'
port = 5432
max_connections = 100
max_prepared_transactions = 16
shared_buffers = {shared_buffers_mb}MB
dynamic_shared_memory_type = posix
log_timezone = 'UTC'
datestyle = 'iso, mdy'
timezone = 'UTC'
lc_messages = 'C'
lc_monetary = 'C'
lc_numeric = 'C'
lc_time = 'C'
CONFEOF
fi

{extra_config_block}

echo "Stripping source-host config overrides from postgresql.conf..."
sed -i \
  -e '/^[[:space:]]*hba_file[[:space:]]*=/d' \
  -e '/^[[:space:]]*ident_file[[:space:]]*=/d' \
  -e '/^[[:space:]]*data_directory[[:space:]]*=/d' \
  -e '/^[[:space:]]*dynamic_shared_memory_type[[:space:]]*=/d' \
  -e '/^[[:space:]]*shared_buffers[[:space:]]*=/d' \
  -e '/^[[:space:]]*log_destination[[:space:]]*=/d' \
  -e '/^[[:space:]]*logging_collector[[:space:]]*=/d' \
  -e '/^[[:space:]]*log_directory[[:space:]]*=/d' \
  -e '/^[[:space:]]*log_filename[[:space:]]*=/d' \
  -e '/^[[:space:]]*log_file_mode[[:space:]]*=/d' \
  -e '/^[[:space:]]*log_rotation_age[[:space:]]*=/d' \
  -e '/^[[:space:]]*log_rotation_size[[:space:]]*=/d' \
  -e '/^[[:space:]]*log_truncate_on_rotation[[:space:]]*=/d' \
  -e '/^[[:space:]]*archive_command[[:space:]]*=/d' \
  -e '/^[[:space:]]*restore_command[[:space:]]*=/d' \
  -e '/^[[:space:]]*archive_cleanup_command[[:space:]]*=/d' \
  -e '/^[[:space:]]*lc_[a-z]*[[:space:]]*=/d' \
  -e '/^[[:space:]]*default_transaction_read_only[[:space:]]*=/d' \
  -e '/^[[:space:]]*password_encryption[[:space:]]*=/d' \
  -e '/^[[:space:]]*listen_addresses[[:space:]]*=/d' \
  "$PGDATA/postgresql.conf"

echo "Configuring stderr logging..."
# prepend a newline so the first appended setting can't merge onto a last line that lacks a trailing newline
echo >> "$PGDATA/postgresql.conf"
echo "log_destination = 'stderr'" >> "$PGDATA/postgresql.conf"
echo "password_encryption = 'scram-sha-256'" >> "$PGDATA/postgresql.conf"
echo "logging_collector = off" >> "$PGDATA/postgresql.conf"
echo "shared_buffers = {shared_buffers_mb}MB" >> "$PGDATA/postgresql.conf"
echo "listen_addresses = '*'" >> "$PGDATA/postgresql.conf"

PG_MAJOR=$(cat "$PGDATA/PG_VERSION")

echo "Truncating postgresql.auto.conf to discard ALTER SYSTEM overrides from source..."
: > "$PGDATA/postgresql.auto.conf"

if [ ! -f "$PGDATA/pg_ident.conf" ]; then
  echo "Creating empty pg_ident.conf..."
  touch "$PGDATA/pg_ident.conf"
fi

echo "Configuring pg_hba.conf..."
cat > "$PGDATA/pg_hba.conf" << 'HBAEOF'
# TYPE  DATABASE        USER            ADDRESS                 METHOD
# trust for local: pod runs as UID 999 which has no passwd entry, so peer auth cannot resolve it
local   all             all                                     trust
host    all             all             0.0.0.0/0               scram-sha-256
host    all             all             ::/0                    scram-sha-256
HBAEOF

if [ ! -d "$PGDATA/pg_wal" ]; then
  echo "pg_wal directory missing (snapshot may be from a Windows host with WAL on a separate path), creating empty pg_wal..."
  mkdir -p "$PGDATA/pg_wal"
  touch /pgdata/fix-recreated-pg-wal
fi

# Run a postgres --single SQL command with a two-stage pg_resetwal -f
# fallback for snapshots whose trailing WAL is missing (e.g. taken
# mid-online-backup). For an analytics replica the priority is
# availability over byte-perfect consistency — pg_resetwal leaves the
# data dir at whatever state the snapshot captured, which is good
# enough for read-only analytics, and the alternative is a permanently
# unrecoverable replica.
#
# Every pg_resetwal -f invocation also touches /pgdata/needs-reindex-all:
# pg_resetwal bypasses WAL replay, so any index update that was in flight
# at snapshot time may have left torn pages ("unexpected zero page at
# block N" surfaces in queries later, which is hard to debug after the
# fact). The main container's startup hook picks up that flag and runs
# REINDEX DATABASE on every user database before marking the replica
# Ready.
#
# Stage 1: if the first attempt fails with a WAL-recovery signature,
# short-circuit straight to pg_resetwal + retry. Retrying the same
# command won't help when recovery itself is the blocker.
# Stage 2: if the first attempt fails for some other reason, try once
# more (could be transient — locked catalog, transient I/O blip).
# If the retry still fails, pg_resetwal as a last resort, then retry.
postgres_single_or_resetwal() {{
  local sql_input="$1"
  local logfile
  logfile=$(mktemp)
  set +e
  echo "$sql_input" | postgres --single -D "$PGDATA" postgres > "$logfile" 2>&1
  local rc=$?
  set -e
  cat "$logfile"
  if [ "$rc" -eq 0 ]; then
    rm -f "$logfile"
    return 0
  fi

  if grep -qE 'WAL ends before end of online backup|invalid record length at|database system was interrupted while in recovery|could not locate required checkpoint record' "$logfile"; then
    echo "WAL recovery failed (snapshot likely captured mid-online-backup) — running pg_resetwal -f and retrying" >&2
    rm -f "$logfile"
    pg_resetwal -f "$PGDATA"
    touch /pgdata/needs-reindex-all
    touch /pgdata/fix-reset-wal
    echo "$sql_input" | postgres --single -D "$PGDATA" postgres
    return $?
  fi

  echo "postgres --single failed without a recognised WAL signature — retrying once before falling back to pg_resetwal" >&2
  rm -f "$logfile"
  logfile=$(mktemp)
  set +e
  echo "$sql_input" | postgres --single -D "$PGDATA" postgres > "$logfile" 2>&1
  rc=$?
  set -e
  cat "$logfile"
  if [ "$rc" -eq 0 ]; then
    rm -f "$logfile"
    return 0
  fi

  echo "second attempt also failed — running pg_resetwal -f as a last resort and retrying" >&2
  rm -f "$logfile"
  pg_resetwal -f "$PGDATA"
  touch /pgdata/needs-reindex-all
  touch /pgdata/fix-reset-wal
  echo "$sql_input" | postgres --single -D "$PGDATA" postgres
}}

echo "Fixing database locales incompatible with this OS (single-user mode)..."
if [ "$PG_MAJOR" -ge 13 ]; then
  postgres_single_or_resetwal "UPDATE pg_database SET datcollate = 'C.UTF-8', datctype = 'C.UTF-8', datcollversion = NULL WHERE datcollate NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST') OR datctype NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST');"
else
  postgres_single_or_resetwal "UPDATE pg_database SET datcollate = 'C.UTF-8', datctype = 'C.UTF-8' WHERE datcollate NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST') OR datctype NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST');"
fi
LOCALE_CHANGED=1

echo "Starting temporary postgres to configure analytics user..."
pg_ctl -D "$PGDATA" -o "-c listen_addresses='' -c log_min_messages=WARNING" -w start

echo "Clearing restored role passwords..."
psql -U postgres -d postgres -At -c "SELECT rolname FROM pg_roles WHERE rolcanlogin AND rolname <> 'postgres' AND rolpassword IS NOT NULL" \
| while IFS= read -r role; do
  psql -U postgres -d postgres -c "ALTER ROLE \"$role\" WITH PASSWORD NULL;" 2>&1 || true
done

echo "Fixing database locales (post-startup fallback)..."
if [ "$PG_MAJOR" -ge 13 ]; then
LOCALE_CHANGED=$(psql -U postgres -d postgres -At << 'LOCALEEOF'
WITH updated AS (
  UPDATE pg_database
     SET datcollate = 'C.UTF-8', datctype = 'C.UTF-8', datcollversion = NULL
   WHERE datcollate NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST') OR datctype NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST')
  RETURNING 1
)
SELECT count(*) FROM updated;
LOCALEEOF
)
else
LOCALE_CHANGED=$(psql -U postgres -d postgres -At << 'LOCALEEOF'
WITH updated AS (
  UPDATE pg_database
     SET datcollate = 'C.UTF-8', datctype = 'C.UTF-8'
   WHERE datcollate NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST') OR datctype NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST')
  RETURNING 1
)
SELECT count(*) FROM updated;
LOCALEEOF
)
fi

for db in $(psql -U postgres -d postgres -At -c "SELECT datname FROM pg_database WHERE datallowconn AND datname <> 'template0'"); do
  echo "Fixing collations in database: $db"
  psql -U postgres -d "$db" << 'COLLEOF'
UPDATE pg_collation
   SET collcollate = 'C.UTF-8', collctype = 'C.UTF-8'
 WHERE collname = 'default';
COLLEOF
done

if [ "${{LOCALE_CHANGED:-0}}" != "0" ]; then
  echo "Locale was changed, flagging for background reindex after startup"
  touch /pgdata/needs-reindex
fi

echo "Detected PG major version: $PG_MAJOR"

# ANALYTICS_PASSWORD is generated by the operator (see replica.rs generate_password)
# and stored in a Kubernetes secret - it is not user-controlled input, so
# interpolating it directly into the SQL string is safe.
psql -U postgres -d postgres << SQLEOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${{ANALYTICS_USERNAME}}') THEN
    CREATE ROLE ${{ANALYTICS_USERNAME}} WITH LOGIN PASSWORD '${{ANALYTICS_PASSWORD}}';
  ELSE
    ALTER ROLE ${{ANALYTICS_USERNAME}} WITH PASSWORD '${{ANALYTICS_PASSWORD}}';
  END IF;
END
\$\$;
SQLEOF

if [ "$PG_MAJOR" -ge 14 ] && [ "{read_only}" = "true" ]; then
  # PG >= 14 read-only: granular read role keeps the surface area minimal
  psql -U postgres -d postgres << SQLEOF
GRANT pg_read_all_data TO ${{ANALYTICS_USERNAME}};
SQLEOF
  echo "Read-only mode with PG >= 14, granted pg_read_all_data"

else
  # Read-write (any PG version) and PG < 14 read-only both go to superuser.
  # The analytics user needs DDL on existing schemas it does not own
  # (e.g. CREATE TABLE in public on PG >= 15, schema drops for
  # persistent_schemas migration, etc.), which the predefined roles
  # don't cover.
  echo "Granting superuser to analytics user..."
  psql -U postgres -d postgres << SQLEOF
ALTER ROLE ${{ANALYTICS_USERNAME}} WITH SUPERUSER;
SQLEOF
fi

# Record which "fix" steps this restore had to apply, so the operator can
# read them back (SELECT from _pgro.restore_info) and forward them to
# canopy in the restore-verification health_details. Stored as a jsonb map
# so adding a new fix is one shell line here plus recording its flag — no
# schema change, no operator change (the operator forwards the map as-is).
# Each fix is keyed by a flag file the fix step touches:
#   locale  — the post-startup locale rewrite actually changed rows
#   reindex — REINDEX ran (after pg_resetwal, or a locale rewrite)
#   reset_wal — pg_resetwal -f ran (snapshot's trailing WAL was unusable)
#   recreated_pg_wal — an empty pg_wal was created (Windows-host snapshot)
if [ -f /pgdata/needs-reindex ] || [ -f /pgdata/needs-reindex-all ]; then
  PGRO_STAGE=restored
  PGRO_REINDEX=true
else
  PGRO_STAGE=ready
  PGRO_REINDEX=false
fi
if [ "${{LOCALE_CHANGED:-0}}" != "0" ]; then PGRO_LOCALE=true; else PGRO_LOCALE=false; fi
if [ -f /pgdata/fix-reset-wal ]; then PGRO_RESET_WAL=true; else PGRO_RESET_WAL=false; fi
if [ -f /pgdata/fix-recreated-pg-wal ]; then PGRO_RECREATED_WAL=true; else PGRO_RECREATED_WAL=false; fi
PGRO_FIXES="{{\"locale\": ${{PGRO_LOCALE}}, \"reindex\": ${{PGRO_REINDEX}}, \"reset_wal\": ${{PGRO_RESET_WAL}}, \"recreated_pg_wal\": ${{PGRO_RECREATED_WAL}}}}"

echo "Writing restore metadata (stage=${{PGRO_STAGE}} fixes=${{PGRO_FIXES}})..."
psql -U postgres -d postgres << SQLEOF
CREATE SCHEMA IF NOT EXISTS _pgro;
CREATE TABLE IF NOT EXISTS _pgro.restore_info (
  id integer PRIMARY KEY DEFAULT 1,
  snapshot_id text NOT NULL,
  snapshot_time timestamptz,
  restored_at timestamptz NOT NULL DEFAULT now(),
  stage text NOT NULL DEFAULT 'restored',
  last_transition_time timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE _pgro.restore_info ADD COLUMN IF NOT EXISTS stage text NOT NULL DEFAULT 'restored';
ALTER TABLE _pgro.restore_info ADD COLUMN IF NOT EXISTS last_transition_time timestamptz NOT NULL DEFAULT now();
ALTER TABLE _pgro.restore_info ADD COLUMN IF NOT EXISTS fixes jsonb;
INSERT INTO _pgro.restore_info (id, snapshot_id, snapshot_time, stage, last_transition_time, fixes)
VALUES (1, '${{PGRO_SNAPSHOT_ID}}', CASE WHEN '${{PGRO_SNAPSHOT_TIME}}' = '' THEN NULL ELSE '${{PGRO_SNAPSHOT_TIME}}'::timestamptz END, '${{PGRO_STAGE}}', now(), '${{PGRO_FIXES}}'::jsonb)
ON CONFLICT (id) DO UPDATE
  SET snapshot_id = EXCLUDED.snapshot_id,
      snapshot_time = EXCLUDED.snapshot_time,
      restored_at = now(),
      stage = EXCLUDED.stage,
      last_transition_time = now(),
      fixes = EXCLUDED.fixes;
SQLEOF

echo "Stopping temporary postgres..."
pg_ctl -D "$PGDATA" -w stop

if [ "{read_only}" = "true" ]; then
  echo "Enabling read-only mode..."
  # Remove any existing setting to avoid duplicates across restarts
  sed -i '/^default_transaction_read_only/d' "$PGDATA/postgresql.conf"
  echo "default_transaction_read_only = on" >> "$PGDATA/postgresql.conf"
fi

echo "Auth setup complete"
"#
	);

	let init_env = vec![
		EnvVar {
			name: "ANALYTICS_USERNAME".to_string(),
			value: Some((*analytics_username).to_string()),
			..Default::default()
		},
		env_from_secret(
			"ANALYTICS_PASSWORD",
			analytics_password_secret,
			analytics_password_key,
		),
		EnvVar {
			name: "READ_ONLY".to_string(),
			value: Some(read_only.to_string()),
			..Default::default()
		},
		EnvVar {
			name: "PGRO_SNAPSHOT_ID".to_string(),
			value: Some((*snapshot_id).to_string()),
			..Default::default()
		},
		EnvVar {
			name: "PGRO_SNAPSHOT_TIME".to_string(),
			value: Some((*snapshot_time).to_string()),
			..Default::default()
		},
	];

	Ok(Deployment {
		metadata: ObjectMeta {
			name: Some((*name).to_string()),
			namespace: Some((*namespace).to_string()),
			labels: Some(labels.clone()),
			owner_references: owner_references.clone(),
			..Default::default()
		},
		spec: Some(DeploymentSpec {
			replicas: Some(1),
			selector: LabelSelector {
				match_labels: Some(match_labels.clone()),
				..Default::default()
			},
			progress_deadline_seconds: Some(3600),
			strategy: Some(k8s_openapi::api::apps::v1::DeploymentStrategy {
				type_: Some("Recreate".to_string()),
				..Default::default()
			}),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(labels.clone()),
					annotations: pod_annotations.clone(),
					..Default::default()
				}),
				spec: Some(PodSpec {
					security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
						run_as_user: Some(999),
						run_as_group: Some(999),
						fs_group: Some(999),
						..Default::default()
					}),
					init_containers: Some(vec![
						Container {
							name: "fix-locale".to_string(),
							image: Some(pg_image.clone()),
							command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
							args: Some(vec![locale_script]),
							security_context: Some(
								k8s_openapi::api::core::v1::SecurityContext {
									run_as_user: Some(0),
									run_as_group: Some(0),
									..Default::default()
								},
							),
							volume_mounts: Some(vec![VolumeMount {
								name: "locale-data".to_string(),
								mount_path: "/locale-data".to_string(),
								..Default::default()
							}]),
							resources: Some(ResourceRequirements {
								requests: Some(BTreeMap::from([
									("cpu".to_string(), Quantity("50m".to_string())),
									("memory".to_string(), Quantity("64Mi".to_string())),
								])),
								..Default::default()
							}),
							..Default::default()
						},
						Container {
							name: "setup-auth".to_string(),
							image: Some(pg_image.clone()),
							command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
							args: Some(vec![init_script]),
							env: Some(init_env),
							volume_mounts: Some(vec![
								VolumeMount {
									name: "pgdata".to_string(),
									mount_path: "/pgdata".to_string(),
									..Default::default()
								},
								VolumeMount {
									name: "locale-data".to_string(),
									mount_path: "/usr/lib/locale".to_string(),
									..Default::default()
								},
							]),
							resources: Some(ResourceRequirements {
								requests: Some(BTreeMap::from([
									("cpu".to_string(), Quantity("100m".to_string())),
									("memory".to_string(), Quantity("128Mi".to_string())),
								])),
								limits: Some(BTreeMap::from([
									("cpu".to_string(), Quantity("500m".to_string())),
									("memory".to_string(), Quantity("256Mi".to_string())),
								])),
								..Default::default()
							}),
							..Default::default()
						},
					]),
					containers: vec![Container {
						name: "postgres".to_string(),
						image: Some(pg_image),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![r#"
if [ -f /pgdata/needs-reindex ] || [ -f /pgdata/needs-reindex-all ]; then
  PG_MAJOR=$(cat /pgdata/pgdata/PG_VERSION)
  (
    while ! pg_isready -q -U postgres -d postgres; do sleep 2; done
    psql -U postgres -d postgres -c "UPDATE _pgro.restore_info SET stage = 'reindexing', last_transition_time = now() WHERE id = 1;"
    # needs-reindex-all (pg_resetwal aftermath) can leave torn pages in
    # ANY index, not just collation-dependent ones. We tried a "smart
    # pass" using the amcheck contrib extension (scan each btree, queue
    # only the corrupt ones for REINDEX) — empirically that hits the
    # same postgres-internal pathology that wedges other vanilla DDL on
    # this dataset: bt_index_check itself burns 100% CPU forever on
    # specific indexes with no visible progress, blocking the whole
    # reindex behind it.
    #
    # Fall back to blind REINDEX DATABASE. REINDEX reads the heap and
    # rebuilds the index from scratch (different code path from amcheck,
    # which reads the corrupt index pages directly) and so isn't subject
    # to the same wedge. Slow on prod-sized DBs but it makes progress;
    # the alternative was a permanently-stuck restore.
    #
    # Crucially this branch does NOT remove needs-reindex-all at the
    # top of the work — the readiness probe ignores -all for exactly
    # this reason (see the probe spec below). The pod becomes Ready as
    # soon as postgres accepts connections; clients hitting a not-yet-
    # reindexed corrupt index get the explicit "unexpected zero page"
    # error, retry, succeed once the rebuild lands. After REINDEX
    # DATABASE completes for every user db, the flag is cleared and
    # _pgro.restore_info.stage flips to ready.
    if [ -f /pgdata/needs-reindex-all ]; then
      # CONCURRENTLY (PG ≥ 12) builds replacement indexes alongside the
      # existing ones and atomically swaps. Clients can keep using the
      # old indexes during the rebuild — they'll see "unexpected zero
      # page" only if a query happens to hit a corrupt page on the old
      # side; once the swap lands the corruption is gone.
      #
      # REINDEX DATABASE CONCURRENTLY skips system catalogs (PG won't
      # CONCURRENTLY them). For an analytics replica that's the right
      # trade: user-data indexes matter for client queries, system
      # catalog corruption shows up as different errors and is rare.
      for db in $(psql -U postgres -d postgres -At -c "SELECT datname FROM pg_database WHERE datallowconn AND datname <> 'template0'"); do
        echo "Reindex after pg_resetwal: $db (REINDEX DATABASE CONCURRENTLY)"
        if [ "$PG_MAJOR" -ge 12 ]; then
          psql -U postgres -d "$db" -c "REINDEX DATABASE CONCURRENTLY \"$db\";" 2>&1 || true
        else
          psql -U postgres -d "$db" -c "REINDEX DATABASE \"$db\";" 2>&1 || true
        fi
      done
      rm -f /pgdata/needs-reindex-all
      # needs-reindex (collation-dependent only) is a strict subset of
      # what we just did, so clear it too.
      rm -f /pgdata/needs-reindex
    elif [ -f /pgdata/needs-reindex ]; then
      for db in $(psql -U postgres -d postgres -At -c "SELECT datname FROM pg_database WHERE datallowconn AND datname <> 'template0'"); do
        INDEXES=$(psql -U postgres -d "$db" -At -c "
          SELECT DISTINCT indexrelid::regclass::text
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indexrelid
          WHERE a.attcollation <> 0 AND i.indisvalid;
        ")
        COUNT=$(echo "$INDEXES" | grep -c . || true)
        echo "Reindex after locale change: $db ($COUNT collation-dependent indexes)"
        N=0
        echo "$INDEXES" | while IFS= read -r idx; do
          [ -z "$idx" ] && continue
          N=$((N + 1))
          echo "  [$N/$COUNT] $db: $idx"
          if [ "$PG_MAJOR" -ge 14 ]; then
            psql -U postgres -d "$db" -c "REINDEX INDEX CONCURRENTLY $idx;" 2>&1 || true
          else
            psql -U postgres -d "$db" -c "REINDEX INDEX $idx;" 2>&1 || true
          fi
        done
      done
      rm -f /pgdata/needs-reindex
    fi
    psql -U postgres -d postgres -c "UPDATE _pgro.restore_info SET stage = 'ready', last_transition_time = now() WHERE id = 1;"
    echo "Background reindex complete"
  ) &
fi
exec postgres -D /pgdata/pgdata ${PGRO_LOG_LEVEL:+-c log_min_messages=$PGRO_LOG_LEVEL}
"#.to_string()]),
						env: Some(vec![
							EnvVar {
								name: "PGDATA".to_string(),
								value: Some("/pgdata/pgdata".to_string()),
								..Default::default()
							},
							EnvVar {
								name: "POSTGRES_HOST_AUTH_METHOD".to_string(),
								value: Some("scram-sha-256".to_string()),
								..Default::default()
							},
						]),
						ports: Some(vec![ContainerPort {
							name: Some("postgres".to_string()),
							container_port: 5432,
							protocol: Some("TCP".to_string()),
							..Default::default()
						}]),
						volume_mounts: Some(vec![
							VolumeMount {
								name: "pgdata".to_string(),
								mount_path: "/pgdata".to_string(),
								..Default::default()
							},
							VolumeMount {
								name: "locale-data".to_string(),
								mount_path: "/usr/lib/locale".to_string(),
								..Default::default()
							},
							VolumeMount {
								name: "dshm".to_string(),
								mount_path: "/dev/shm".to_string(),
								..Default::default()
							},
						]),
						readiness_probe: Some(Probe {
							exec: Some(ExecAction {
								command: Some(vec![
									"/bin/sh".to_string(),
									"-c".to_string(),
									// Gate readiness on the locale-only needs-reindex flag
								// (small, fast, finishes in seconds-to-minutes) but NOT on
								// needs-reindex-all (post-pg_resetwal blind REINDEX DATABASE
								// — takes hours on prod-sized indexes; gating here would
								// trip the operator's deployment_ready_timeout). The -all
								// reindex runs in the background; clients hitting a
								// not-yet-reindexed corrupt index see the explicit
								// "unexpected zero page" error and retry.
								"pg_isready -U postgres -d postgres && [ ! -f /pgdata/needs-reindex ]".to_string(),
								]),
							}),
							initial_delay_seconds: Some(5),
							period_seconds: Some(5),
							timeout_seconds: Some(3),
							failure_threshold: Some(6),
							..Default::default()
						}),
						liveness_probe: Some(Probe {
							exec: Some(ExecAction {
								command: Some(vec![
									"pg_isready".to_string(),
									"-U".to_string(),
									"postgres".to_string(),
									"-d".to_string(),
									"postgres".to_string(),
								]),
							}),
							initial_delay_seconds: Some(30),
							period_seconds: Some(10),
							timeout_seconds: Some(3),
							failure_threshold: Some(3),
							..Default::default()
						}),
						resources: postgres_resources.clone(),
						..Default::default()
					}],
					volumes: Some(vec![
						Volume {
							name: "pgdata".to_string(),
							persistent_volume_claim: Some(
								k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
									claim_name: (*pvc_name).to_string(),
									read_only: Some(false),
								},
							),
							..Default::default()
						},
						Volume {
							name: "locale-data".to_string(),
							empty_dir: Some(
								k8s_openapi::api::core::v1::EmptyDirVolumeSource::default(),
							),
							..Default::default()
						},
						Volume {
							name: "dshm".to_string(),
							empty_dir: Some(
								k8s_openapi::api::core::v1::EmptyDirVolumeSource {
										medium: Some("Memory".to_string()),
										size_limit: Some(shm_size),
									},
							),
							..Default::default()
						},
					]),
					affinity: affinity.clone(),
					tolerations: Some(tolerations.clone()),
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}
