use std::collections::BTreeMap;

use chrono::Utc;
use k8s_openapi::{
	ByteString,
	api::{
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, EnvVar, PodSpec, PodTemplateSpec, ResourceRequirements, Secret, Service,
			ServicePort, ServiceSpec,
		},
	},
	apimachinery::pkg::{
		api::resource::Quantity, apis::meta::v1::OwnerReference, util::intstr::IntOrString,
	},
};
use kube::{
	Api, Client, ResourceExt,
	api::{ObjectMeta, Patch, PatchParams, PostParams},
};
use tracing::{info, warn};

use super::generate_password;
use crate::{
	controllers::{env_from_secret, env_from_secret_optional},
	error::Result,
	types::*,
};

#[derive(Debug, serde::Deserialize)]
pub struct SnapshotInfo {
	pub id: String,
	pub size: u64,
}

pub fn format_bytes(bytes: u64) -> String {
	const GI: u64 = 1024 * 1024 * 1024;
	const MI: u64 = 1024 * 1024;
	if bytes >= GI {
		let gi = bytes.div_ceil(GI);
		format!("{gi}Gi")
	} else {
		let mi = bytes.div_ceil(MI).max(1);
		format!("{mi}Mi")
	}
}

pub fn owner_reference(replica: &PostgresPhysicalReplica) -> OwnerReference {
	OwnerReference {
		api_version: "pgro.bes.au/v1alpha1".to_string(),
		kind: "PostgresPhysicalReplica".to_string(),
		name: replica.name_any(),
		uid: replica.uid().unwrap_or_default(),
		controller: Some(true),
		block_owner_deletion: Some(true),
	}
}

pub fn build_snapshot_list_job(
	replica: &PostgresPhysicalReplica,
	job_name: &str,
	namespace: &str,
) -> Result<Job> {
	let kopia_secret = &replica.spec.kopia_secret_ref;
	let replica_name = replica.name_any();

	let mut env_vars = vec![
		env_from_secret("KOPIA_BUCKET", kopia_secret, "bucket"),
		env_from_secret("KOPIA_REGION", kopia_secret, "region"),
		env_from_secret("AWS_ACCESS_KEY_ID", kopia_secret, "accessKeyId"),
		env_from_secret("AWS_SECRET_ACCESS_KEY", kopia_secret, "secretAccessKey"),
		env_from_secret("KOPIA_PASSWORD", kopia_secret, "repositoryPassword"),
		env_from_secret_optional("KOPIA_ENDPOINT", kopia_secret, "endpoint"),
		env_from_secret_optional("KOPIA_DISABLE_TLS", kopia_secret, "disableTls"),
	];

	if let Some(ref filter) = replica.spec.snapshot_filter {
		if let Some(ref pattern) = filter.host_pattern {
			env_vars.push(EnvVar {
				name: "FILTER_HOST_PATTERN".to_string(),
				value: Some(pattern.clone()),
				..Default::default()
			});
		}
		if let Some(ref tags) = filter.tags {
			let tag_str = tags
				.iter()
				.map(|(k, v)| format!("{k}={v}"))
				.collect::<Vec<_>>()
				.join(",");
			env_vars.push(EnvVar {
				name: "FILTER_TAGS".to_string(),
				value: Some(tag_str),
				..Default::default()
			});
		}
	}

	let script = r#"set -e

apt-get update -qq && apt-get install -y -qq jq >/dev/null 2>&1

ENDPOINT_ARGS=""
if [ -n "$KOPIA_ENDPOINT" ]; then
  ENDPOINT_ARGS="--endpoint=$KOPIA_ENDPOINT"
fi
if [ "$KOPIA_DISABLE_TLS" = "true" ]; then
  ENDPOINT_ARGS="$ENDPOINT_ARGS --disable-tls --disable-tls-verification"
fi

kopia repository connect s3 \
  --bucket="$KOPIA_BUCKET" \
  --region="$KOPIA_REGION" \
  --access-key="$AWS_ACCESS_KEY_ID" \
  --secret-access-key="$AWS_SECRET_ACCESS_KEY" \
  --password="$KOPIA_PASSWORD" \
  $ENDPOINT_ARGS

SNAPSHOTS=$(kopia snapshot list --json --all 2>/dev/null || echo "[]")

if [ -n "$FILTER_HOST_PATTERN" ]; then
  REGEX=$(printf '%s' "$FILTER_HOST_PATTERN" | sed 's/\./\\./g; s/\*/\.\*/g; s/\?/\./g')
  SNAPSHOTS=$(echo "$SNAPSHOTS" | jq -c --arg pat "^${REGEX}$" '[.[] | select(.source.host != null and (.source.host | test($pat)))]')
fi

if [ -n "$FILTER_TAGS" ]; then
  IFS=',' read -r TAG_LIST <<EOF
$FILTER_TAGS
EOF
  for tag in $TAG_LIST; do
    KEY="${tag%%=*}"
    VALUE="${tag#*=}"
    SNAPSHOTS=$(echo "$SNAPSHOTS" | jq -c --arg k "$KEY" --arg v "$VALUE" '[.[] | select(.tags[$k] == $v or .tags["tag:" + $k] == $v)]')
  done
fi

LATEST=$(echo "$SNAPSHOTS" | jq -c 'sort_by(.startTime) | last // empty')

if [ -z "$LATEST" ] || [ "$LATEST" = "null" ]; then
  echo "No matching snapshots found"
  printf '{}' > /dev/termination-log
  exit 0
fi

ID=$(echo "$LATEST" | jq -r '.id')
SIZE=$(echo "$LATEST" | jq -r '.stats.totalSize // 0')
echo "Latest snapshot: id=$ID size=$SIZE"
printf '{"id":"%s","size":%s}' "$ID" "$SIZE" > /dev/termination-log
"#;

	Ok(Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("pgro.bes.au/replica".to_string(), replica_name.clone()),
				(
					"pgro.bes.au/job-type".to_string(),
					"snapshot-list".to_string(),
				),
			])),
			owner_references: Some(vec![owner_reference(replica)]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(2),
			active_deadline_seconds: Some(300),
			ttl_seconds_after_finished: Some(120),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(BTreeMap::from([
						("pgro.bes.au/replica".to_string(), replica_name),
						(
							"pgro.bes.au/job-type".to_string(),
							"snapshot-list".to_string(),
						),
					])),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					containers: vec![Container {
						name: "snapshot-list".to_string(),
						image: Some("kopia/kopia:latest".to_string()),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![script.to_string()]),
						env: Some(env_vars),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("50m".to_string())),
								("memory".to_string(), Quantity("64Mi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("200m".to_string())),
								("memory".to_string(), Quantity("128Mi".to_string())),
							])),
							..Default::default()
						}),
						..Default::default()
					}],
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}

pub async fn create_restore_for_snapshot(
	client: &Client,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	snapshot: &SnapshotInfo,
) -> Result<()> {
	let replica_name = replica.name_any();
	let timestamp = Utc::now().format("%Y%m%d-%H%M%S");
	let restore_name = format!("{replica_name}-{timestamp}");

	const MAX_PVC_BYTES: u64 = 2 * 1024 * 1024 * 1024 * 1024; // 2 TiB

	let snapshot_size = format_bytes(snapshot.size);
	let storage_size = match &replica.spec.storage_size_override {
		Some(override_size) => override_size.clone(),
		None => {
			let computed = (snapshot.size as f64 * 1.1) as u64;
			if computed > MAX_PVC_BYTES {
				warn!(
					replica = replica.name_any(),
					snapshot = snapshot.id,
					computed_bytes = computed,
					max_bytes = MAX_PVC_BYTES,
					"computed PVC size exceeds 2TiB ceiling, capping"
				);
				format_bytes(MAX_PVC_BYTES)
			} else {
				format_bytes(computed)
			}
		}
	};

	let restores: Api<PostgresPhysicalRestore> = Api::namespaced(client.clone(), namespace);
	let restore = PostgresPhysicalRestore::new(
		&restore_name,
		PostgresPhysicalRestoreSpec {
			replica: replica_name.clone(),
			snapshot: snapshot.id.clone(),
			snapshot_size,
			storage_size,
		},
	);

	let mut restore_obj = serde_json::to_value(&restore)?;
	if let Some(meta) = restore_obj
		.as_object_mut()
		.and_then(|o| o.get_mut("metadata"))
		.and_then(|m| m.as_object_mut())
	{
		meta.insert(
			"namespace".to_string(),
			serde_json::Value::String(namespace.to_string()),
		);
		meta.insert(
			"labels".to_string(),
			serde_json::json!({ "pgro.bes.au/replica": replica_name }),
		);
		meta.insert(
			"ownerReferences".to_string(),
			serde_json::json!([{
				"apiVersion": "pgro.bes.au/v1alpha1",
				"kind": "PostgresPhysicalReplica",
				"name": replica.name_any(),
				"uid": replica.uid().unwrap_or_default(),
				"controller": true,
				"blockOwnerDeletion": true,
			}]),
		);
	}

	let restore_resource: PostgresPhysicalRestore = serde_json::from_value(restore_obj)?;
	restores
		.create(&PostParams::default(), &restore_resource)
		.await?;

	info!(
		replica = replica_name,
		restore = restore_name,
		snapshot = snapshot.id,
		"created restore resource"
	);

	Ok(())
}

pub async fn ensure_credentials_secret(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	secret_name: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<()> {
	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

	if secrets.get_opt(secret_name).await?.is_some() {
		return Ok(());
	}

	info!(
		replica = replica_name,
		secret = secret_name,
		"creating credentials secret"
	);

	let password = generate_password();
	let secret = Secret {
		metadata: ObjectMeta {
			name: Some(secret_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([(
				"pgro.bes.au/replica".to_string(),
				replica_name.to_string(),
			)])),
			owner_references: Some(vec![owner_reference(replica)]),
			..Default::default()
		},
		data: Some(BTreeMap::from([
			(
				"username".to_string(),
				ByteString(replica.spec.analytics_username.as_bytes().to_vec()),
			),
			(
				"password".to_string(),
				ByteString(password.as_bytes().to_vec()),
			),
		])),
		..Default::default()
	};

	secrets.create(&PostParams::default(), &secret).await?;
	Ok(())
}

pub async fn ensure_service(
	client: &Client,
	namespace: &str,
	replica_name: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<()> {
	let services: Api<Service> = Api::namespaced(client.clone(), namespace);

	if services.get_opt(replica_name).await?.is_some() {
		// Service exists; update annotations if needed
		let mut annotations = BTreeMap::new();
		if let Some(sa) = &replica.spec.service_annotations {
			for (k, v) in sa {
				annotations.insert(k.clone(), v.clone());
			}
		}
		let patch = serde_json::json!({
			"metadata": {
				"annotations": annotations,
			}
		});
		services
			.patch(
				replica_name,
				&PatchParams::apply("postgres-restore-operator"),
				&Patch::Merge(&patch),
			)
			.await?;
		return Ok(());
	}

	info!(replica = replica_name, "creating stable service");

	let mut annotations = BTreeMap::new();
	if let Some(sa) = &replica.spec.service_annotations {
		for (k, v) in sa {
			annotations.insert(k.clone(), v.clone());
		}
	}

	let service = Service {
		metadata: ObjectMeta {
			name: Some(replica_name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([(
				"pgro.bes.au/replica".to_string(),
				replica_name.to_string(),
			)])),
			annotations: Some(annotations),
			owner_references: Some(vec![owner_reference(replica)]),
			..Default::default()
		},
		spec: Some(ServiceSpec {
			type_: Some("ClusterIP".to_string()),
			ports: Some(vec![ServicePort {
				name: Some("postgres".to_string()),
				port: 5432,
				target_port: Some(IntOrString::Int(5432)),
				protocol: Some("TCP".to_string()),
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

pub async fn update_service_selector(
	client: &Client,
	namespace: &str,
	service_name: &str,
	restore_name: &str,
) -> Result<()> {
	let services: Api<Service> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"spec": {
			"selector": {
				"pgro.bes.au/restore": restore_name,
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
