use std::collections::BTreeMap;

use jiff::Timestamp;
use k8s_openapi::{
	ByteString,
	api::{
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, EmptyDirVolumeSource, EnvVar, LocalObjectReference, Pod, PodSpec,
			PodTemplateSpec, ResourceRequirements, Secret, SecretReference, Service, ServicePort,
			ServiceSpec, Volume, VolumeMount,
		},
	},
	apimachinery::pkg::{api::resource::Quantity, util::intstr::IntOrString},
};
use kube::{
	Api, Client, ResourceExt,
	api::{ListParams, ObjectMeta, Patch, PatchParams, PostParams},
};
use kube_quantity::ParsedQuantity;
use rust_decimal::Decimal;
use tracing::{info, warn};

use super::generate_password;
use crate::{
	controllers::{
		READY_FOR_TRAFFIC_LABEL, env_from_secret, env_from_secret_optional,
		restore::builders::{CanopyProxyArgs, PROXY_SIDECAR_POD_LABEL},
	},
	error::Result,
	kopia::KopiaSource,
	placement::PodPlacement,
	types::*,
};

/// Hard cap on how many `PostgresPhysicalRestore` objects may exist for a
/// single replica before the operator refuses to create more. In steady
/// state a replica has 1 (current) or transiently 2 (current + a switching
/// or in-grace-period predecessor); 3 is already a degenerate case, and the
/// guardrail exists so a pruning bug can't silently fill a cluster with
/// orphan PVCs again.
pub const MAX_RESTORES_PER_REPLICA: usize = 3;

#[derive(Debug, serde::Deserialize)]
pub struct SnapshotInfo {
	pub id: String,
	pub size: u64,
	pub start_time: String,
}

impl SnapshotInfo {
	pub fn bytes(&self) -> ParsedQuantity {
		ParsedQuantity::from(Decimal::from(self.size))
	}
}

/// Size the restore PVC for a snapshot.
///
/// `override_size` (`spec.storageSizeOverride`) is a floor, not a replacement:
/// the canopy path always sets it from the intent config, so treating it as an
/// exact size would truncate any replica whose snapshot outgrew it.
///
/// `maximum` bounds the result. It fails the restore only when the
/// *snapshot-derived* size exceeds it — a replica genuinely too big for its
/// configured cap, where truncating would restore into a volume that fills
/// partway through. A floor above the maximum is merely contradictory config
/// and clamps instead.
pub fn compute_storage_size(
	snapshot_bytes: ParsedQuantity,
	override_size: Option<&Quantity>,
	maximum: &Quantity,
	persistent_schemas: bool,
	measured_schema_delta: Option<ParsedQuantity>,
) -> Result<Quantity> {
	let max_pvc_size = ParsedQuantity::try_from(maximum.clone())
		.unwrap_or_else(|_| ParsedQuantity::try_from("2Ti").unwrap());

	let computed_size = if persistent_schemas {
		// Persistent schemas are migrated into the restore PVC.
		// Formula: snapshot + max(10% of snapshot, last measured delta) + 5Gi
		let ten_percent = snapshot_bytes.clone() * Decimal::new(1, 1);
		let measured = measured_schema_delta.unwrap_or_else(|| ParsedQuantity::from(Decimal::ZERO));
		let overhead = if measured > ten_percent {
			measured
		} else {
			ten_percent
		};
		snapshot_bytes + overhead + ParsedQuantity::try_from("5Gi").unwrap()
	} else {
		snapshot_bytes * Decimal::new(11, 1) // 1.1x
	};

	// The maximum guards against a snapshot too large to fit — that's the case
	// where truncating would restore into a volume that fills partway through.
	if computed_size > max_pvc_size {
		return Err(crate::error::Error::StorageLimitExceeded {
			computed: computed_size.into(),
			maximum: max_pvc_size.into(),
		});
	}

	let floor = override_size.and_then(|q| ParsedQuantity::try_from(q.clone()).ok());
	let chosen = match floor {
		Some(floor) if floor > computed_size => floor,
		_ => computed_size,
	};

	// A floor above the maximum is contradictory configuration rather than a
	// too-big replica, so the maximum simply wins. Failing instead would wedge
	// a small replica whose operator capped it below the intent's floor.
	Ok(if chosen > max_pvc_size {
		max_pvc_size.into()
	} else {
		chosen.into()
	})
}

pub fn build_snapshot_list_job(
	replica: &PostgresPhysicalReplica,
	job_name: &str,
	namespace: &str,
	kopia_image: &str,
	callback_url: &str,
	canopy_proxy: Option<&CanopyProxyArgs<'_>>,
	placement: &PodPlacement,
) -> Result<Job> {
	let source = replica.kopia_source();
	let kopia_secret = SecretReference {
		name: Some(source.secret_name().to_string()),
		namespace: None,
	};
	let replica_name = replica.name_any();

	let mut env_vars = vec![
		env_from_secret("KOPIA_BUCKET", &kopia_secret, "bucket"),
		env_from_secret("KOPIA_REGION", &kopia_secret, "region"),
		env_from_secret("AWS_ACCESS_KEY_ID", &kopia_secret, "accessKeyId"),
		env_from_secret("AWS_SECRET_ACCESS_KEY", &kopia_secret, "secretAccessKey"),
		env_from_secret("KOPIA_PASSWORD", &kopia_secret, "repositoryPassword"),
	];
	if !source.is_canopy_proxy() {
		env_vars.push(env_from_secret_optional(
			"KOPIA_ENDPOINT",
			&kopia_secret,
			"endpoint",
		));
		env_vars.push(env_from_secret_optional(
			"KOPIA_DISABLE_TLS",
			&kopia_secret,
			"disableTls",
		));
	}
	env_vars.push(EnvVar {
		name: "SNAPSHOT_CALLBACK_URL".to_string(),
		value: Some(callback_url.to_string()),
		..Default::default()
	});

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
echo "kopia snapshot-list via canopy proxy at ${KOPIA_ENDPOINT}" >&2

"#
	} else {
		""
	};

	let script_body = r#"set -e

ENDPOINT_ARGS=""
if [ -n "$KOPIA_ENDPOINT" ]; then
  ENDPOINT_ARGS="--endpoint=$KOPIA_ENDPOINT"
fi
if [ "$KOPIA_DISABLE_TLS" = "true" ]; then
  ENDPOINT_ARGS="$ENDPOINT_ARGS --disable-tls --disable-tls-verification"
fi

# Global kopia flags: rotate CLI logs so they don't accumulate over
# many snapshot-list invocations.
KOPIA_GLOBAL_FLAGS="--log-dir-max-files=20 --log-dir-max-age=24h"

kopia $KOPIA_GLOBAL_FLAGS repository connect s3 \
  --bucket="$KOPIA_BUCKET" \
  --region="$KOPIA_REGION" \
  --access-key="$AWS_ACCESS_KEY_ID" \
  --secret-access-key="$AWS_SECRET_ACCESS_KEY" \
  --password="$KOPIA_PASSWORD" \
  $ENDPOINT_ARGS \
  >&2

SNAP_FILE=$(mktemp)
trap 'rm -f "$SNAP_FILE"' EXIT
kopia $KOPIA_GLOBAL_FLAGS snapshot list --json --all > "$SNAP_FILE" || echo "[]" > "$SNAP_FILE"
cat "$SNAP_FILE"
if [ -n "$SNAPSHOT_CALLBACK_URL" ]; then
  SNAP_SIZE=$(wc -c < "$SNAP_FILE")
  echo "Posting snapshot results (${SNAP_SIZE} bytes) to $SNAPSHOT_CALLBACK_URL" >&2
  HTTP_CODE=$(curl -s -o /dev/stderr -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    -d @"$SNAP_FILE" \
    "$SNAPSHOT_CALLBACK_URL" 2>&1) || true
  echo "Callback response: HTTP $HTTP_CODE" >&2
fi
"#;
	let script = format!("{canopy_prelude}{script_body}");

	let mut kopia_volume_mounts: Vec<VolumeMount> = Vec::new();
	let mut volumes: Vec<Volume> = Vec::new();
	let mut pod_labels = BTreeMap::from([
		("pgro.bes.au/replica".to_string(), replica_name.clone()),
		(
			"pgro.bes.au/job-type".to_string(),
			"snapshot-list".to_string(),
		),
	]);
	let mut containers = vec![Container {
		name: "snapshot-list".to_string(),
		image: Some(kopia_image.to_string()),
		command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
		args: Some(vec![script]),
		env: Some(env_vars),
		volume_mounts: Some(kopia_volume_mounts.clone()),
		resources: Some(ResourceRequirements {
			requests: Some(BTreeMap::from([
				("cpu".to_string(), Quantity("50m".to_string())),
				("memory".to_string(), Quantity("64Mi".to_string())),
			])),
			..Default::default()
		}),
		..Default::default()
	}];
	// The canopy-proxy runs as a native sidecar (init container with
	// restartPolicy: Always) so the Pod completes once the main
	// snapshot-list container exits; a plain sidecar container would keep
	// the Pod Running and the Job would never succeed.
	let mut init_containers: Vec<Container> = Vec::new();

	if let KopiaSource::CanopyProxy {
		group, backup_type, ..
	} = &source
	{
		let proxy = canopy_proxy.expect(
			"build_snapshot_list_job called with canopy_source but no CanopyProxyArgs; caller \
			 must thread proxy config from Context when replica.spec.canopy_source is set",
		);

		kopia_volume_mounts.push(VolumeMount {
			name: "proxy-shared".to_string(),
			mount_path: "/var/run/pgro".to_string(),
			..Default::default()
		});
		containers[0].volume_mounts = Some(kopia_volume_mounts);

		init_containers.push(Container {
			name: "canopy-proxy".to_string(),
			image: Some(proxy.image.to_string()),
			restart_policy: Some("Always".to_string()),
			command: Some(vec!["canopy-proxy".to_string()]),
			env: Some(vec![
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
			]),
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

	let mut job = Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(pod_labels.clone()),
			owner_references: Some(vec![replica.owner_reference()]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(2),
			active_deadline_seconds: Some(300),
			ttl_seconds_after_finished: Some(120),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(pod_labels),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					init_containers: if init_containers.is_empty() {
						None
					} else {
						Some(init_containers)
					},
					termination_grace_period_seconds: Some(30),
					containers,
					volumes: if volumes.is_empty() {
						None
					} else {
						Some(volumes)
					},
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	};
	placement.apply_to_job(&mut job);
	Ok(job)
}

impl PostgresPhysicalReplica {
	/// Create a new `PostgresPhysicalRestore` for this replica from the given
	/// snapshot. Returns `Ok(true)` when a restore was created, `Ok(false)`
	/// when creation was refused because the replica already has
	/// [`MAX_RESTORES_PER_REPLICA`] or more restore objects (the
	/// too-many-restores guardrail). The caller should skip post-create side
	/// effects (metrics, events) when `false` is returned.
	pub async fn create_restore_for_snapshot(
		&self,
		client: &Client,
		snapshot: &SnapshotInfo,
	) -> Result<bool> {
		let replica_name = self.name_any();

		// Guardrail: refuse to create another restore if we already have too
		// many *live* ones. Pruning normally keeps this at 1; if it
		// doesn't, capping here prevents runaway PVC creation while the
		// underlying issue is fixed.
		//
		// Failed restores are excluded from the count. They are
		// operator-owned and cleaned up by the failed-restore sweep within
		// minutes — counting them would cause the guardrail to spuriously
		// trip during sustained-failure backoff (e.g. 1 active + 1 failed
		// pending cleanup + 1 new attempt). The invariant we actually want
		// to enforce is on live restores (Pending/Restoring/Ready/
		// Switching/Active).
		let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), &self.ns());
		let existing_all = restores
			.list(&ListParams::default().labels(&format!("pgro.bes.au/replica={replica_name}")))
			.await?;
		let live: Vec<&PostgresPhysicalRestore> = existing_all
			.items
			.iter()
			.filter(|r| {
				!matches!(
					r.status.as_ref().and_then(|s| s.phase.as_ref()),
					Some(&RestorePhase::Failed)
				)
			})
			.collect();
		if live.len() >= MAX_RESTORES_PER_REPLICA {
			let names: Vec<String> = live.iter().map(|r| r.name_any()).collect();
			warn!(
				replica = replica_name,
				existing = ?names,
				limit = MAX_RESTORES_PER_REPLICA,
				"refusing to create new restore: too many already exist for this replica"
			);
			self.update_condition(
				client,
				"RestoreCreationBlocked",
				"True",
				"TooManyRestores",
				&format!(
					"Refusing to create new restore: {} live restores already exist for this \
					 replica (limit {}). Existing: [{}]. Reduce the count by deleting stale \
					 restores or fix the pruning issue.",
					live.len(),
					MAX_RESTORES_PER_REPLICA,
					names.join(", "),
				),
			)
			.await?;
			return Ok(false);
		}

		let timestamp = Timestamp::now().strftime("%Y%m%d-%H%M%S").to_string();
		let restore_name = format!("{replica_name}-{timestamp}");

		let measured_schema_delta = self
			.status
			.as_ref()
			.and_then(|s| s.persistent_schema_data_size.as_ref())
			.and_then(|q| ParsedQuantity::try_from(q.clone()).ok());
		let storage_size = compute_storage_size(
			snapshot.bytes(),
			self.spec.storage_size_override.as_ref(),
			&self.spec.storage_size_maximum,
			self.spec.persistent_schemas.is_some(),
			measured_schema_delta,
		)?;

		let mut restore = PostgresPhysicalRestore::new(
			&restore_name,
			PostgresPhysicalRestoreSpec {
				replica: LocalObjectReference {
					name: replica_name.clone(),
				},
				snapshot: snapshot.id.clone(),
				snapshot_size: snapshot.bytes().into(),
				snapshot_time: if snapshot.start_time.is_empty() {
					None
				} else {
					Some(snapshot.start_time.clone())
				},
				storage_size,
				// Snapshotted, not read live: the version this restore is proving
				// must not change under it if canopy's plan moves mid-restore.
				migrate_to: self.spec.migrate_to.clone(),
			},
		);

		restore.metadata.namespace = self.metadata.namespace.clone();
		restore.metadata.owner_references = Some(vec![self.owner_reference()]);
		restore
			.labels_mut()
			.insert("pgro.bes.au/replica".into(), self.name_any());

		Api::<PostgresPhysicalRestore>::namespaced(client.clone(), &self.ns())
			.create(&PostParams::default(), &restore)
			.await?;

		info!(
			replica = replica_name,
			restore = restore_name,
			snapshot = snapshot.id,
			"created restore resource"
		);

		// Clear the guardrail condition once we successfully created a restore
		// — the count is now below the limit again.
		self.update_condition(
			client,
			"RestoreCreationBlocked",
			"False",
			"Healthy",
			"Restore count is below the per-replica limit",
		)
		.await?;

		Ok(true)
	}

	pub async fn ensure_credentials_secret(&self, client: &Client) -> Result<()> {
		let secret_name = &self.creds_secret_name();
		let secrets: Api<Secret> = Api::namespaced(client.clone(), &self.ns());

		if secrets.get_opt(secret_name).await?.is_some() {
			return Ok(());
		}

		info!(
			replica = self.name_any(),
			secret = secret_name,
			"creating credentials secret"
		);

		let password = generate_password();
		let secret = Secret {
			metadata: ObjectMeta {
				name: Some(secret_name.into()),
				namespace: self.metadata.namespace.clone(),
				labels: Some(BTreeMap::from([(
					"pgro.bes.au/replica".into(),
					self.name_any(),
				)])),
				owner_references: Some(vec![self.owner_reference()]),
				..Default::default()
			},
			data: Some(BTreeMap::from([
				(
					"username".into(),
					ByteString(self.spec.analytics_username.as_bytes().to_vec()),
				),
				("password".into(), ByteString(password.as_bytes().to_vec())),
			])),
			..Default::default()
		};

		secrets.create(&PostParams::default(), &secret).await?;
		Ok(())
	}

	/// Create one Secret per `persistentUsers` entry and delete the Secrets of
	/// users that have been removed from the spec.
	///
	/// Passwords are generated once and then left alone: the whole point of the
	/// feature is that a consumer's credential survives switchovers, so an
	/// existing Secret is never rewritten. Secrets are labelled and
	/// owner-referenced like the replica creds Secret, so deleting the replica
	/// still cascades.
	pub async fn ensure_persistent_user_secrets(&self, client: &Client) -> Result<()> {
		let secrets: Api<Secret> = Api::namespaced(client.clone(), &self.ns());
		let replica_name = self.name_any();

		let mut wanted = BTreeMap::new();
		for user in &self.spec.persistent_users {
			wanted.insert(user.secret_name(&replica_name), user);
		}

		for (secret_name, user) in &wanted {
			if secrets.get_opt(secret_name).await?.is_some() {
				continue;
			}

			info!(
				replica = replica_name,
				user = user.name,
				secret = secret_name,
				"creating persistent user secret"
			);

			let secret = Secret {
				metadata: ObjectMeta {
					name: Some(secret_name.clone()),
					namespace: self.metadata.namespace.clone(),
					labels: Some(BTreeMap::from([
						("pgro.bes.au/replica".into(), replica_name.clone()),
						("pgro.bes.au/persistent-user".into(), user.name.clone()),
					])),
					owner_references: Some(vec![self.owner_reference()]),
					..Default::default()
				},
				data: Some(BTreeMap::from([
					("username".into(), ByteString(user.name.as_bytes().to_vec())),
					(
						"password".into(),
						ByteString(generate_password().into_bytes()),
					),
				])),
				..Default::default()
			};
			secrets.create(&PostParams::default(), &secret).await?;
		}

		let owned = secrets
			.list(&ListParams::default().labels(&format!(
				"pgro.bes.au/replica={replica_name},pgro.bes.au/persistent-user"
			)))
			.await?;
		for secret in owned.items {
			let secret_name = secret.name_any();
			if wanted.contains_key(&secret_name) {
				continue;
			}
			info!(
				replica = replica_name,
				secret = secret_name,
				"deleting secret for removed persistent user"
			);
			if let Err(e) = secrets.delete(&secret_name, &Default::default()).await {
				warn!(secret = secret_name, error = %e, "failed to delete persistent user secret");
			}
		}

		Ok(())
	}

	pub async fn ensure_service(&self, client: &Client) -> Result<()> {
		let services: Api<Service> = Api::namespaced(client.clone(), &self.ns());

		if services.get_opt(&self.name_any()).await?.is_some() {
			// Service exists; update annotations if needed
			let patch = serde_json::json!({
				"metadata": {
					"annotations": self.spec.service_annotations,
				}
			});
			services
				.patch(
					&self.name_any(),
					&PatchParams::apply("postgres-restore-operator"),
					&Patch::Merge(&patch),
				)
				.await?;
			return Ok(());
		}

		info!(replica = self.name_any(), "creating stable service");
		let service = Service {
			metadata: ObjectMeta {
				name: Some(self.name_any()),
				namespace: self.metadata.namespace.clone(),
				labels: Some(BTreeMap::from([(
					"pgro.bes.au/replica".into(),
					self.name_any(),
				)])),
				annotations: self.spec.service_annotations.clone(),
				owner_references: Some(vec![self.owner_reference()]),
				..Default::default()
			},
			spec: Some(ServiceSpec {
				type_: Some("ClusterIP".into()),
				ports: Some(vec![ServicePort {
					name: Some("postgres".into()),
					port: 5432,
					target_port: Some(IntOrString::Int(5432)),
					protocol: Some("TCP".into()),
					..Default::default()
				}]),
				// No selector initially — set during switchover
				..Default::default()
			}),
			..Default::default()
		};

		services.create(&PostParams::default(), &service).await?;
		Ok(())
	}
}

impl PostgresPhysicalRestore {
	/// Patch this restore's postgres pod to add the [`READY_FOR_TRAFFIC_LABEL`].
	/// Idempotent and resilient to the pod not existing yet (the restore's
	/// deployment may be mid-rollout); callers that need the label to be
	/// present should retry on the next reconcile pass.
	pub async fn mark_pod_ready_for_traffic(&self, client: &Client) -> Result<()> {
		let pods: Api<Pod> = Api::namespaced(client.clone(), &self.ns());
		let selector = format!("pgro.bes.au/restore={}", self.name_any());
		let list = pods.list(&ListParams::default().labels(&selector)).await?;
		let pod = list
			.items
			.into_iter()
			.find(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"));
		let Some(pod) = pod else {
			warn!(
				restore = self.name_any(),
				"no running pod for restore yet; will retry next reconcile"
			);
			return Ok(());
		};
		let pod_name = pod.name_any();
		let patch = serde_json::json!({
			"metadata": {
				"labels": {
					READY_FOR_TRAFFIC_LABEL: "true",
				}
			}
		});
		pods.patch(&pod_name, &PatchParams::default(), &Patch::Merge(&patch))
			.await?;
		info!(
			restore = self.name_any(),
			pod = pod_name,
			"marked restore pod ready for traffic"
		);
		Ok(())
	}

	pub async fn update_service_selector(&self, client: &Client, service_name: &str) -> Result<()> {
		let services: Api<Service> = Api::namespaced(client.clone(), &self.ns());
		let patch = serde_json::json!({
			"spec": {
				"selector": {
					"pgro.bes.au/restore": self.name_any(),
					READY_FOR_TRAFFIC_LABEL: "true",
				}
			}
		});
		services
			.patch(
				service_name,
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		Ok(())
	}
}
