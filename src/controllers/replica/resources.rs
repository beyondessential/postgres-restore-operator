use std::collections::BTreeMap;

use jiff::Timestamp;
use k8s_openapi::{
	ByteString,
	api::{
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, EnvVar, LocalObjectReference, PodSpec, PodTemplateSpec,
			ResourceRequirements, Secret, Service, ServicePort, ServiceSpec,
		},
	},
	apimachinery::pkg::{api::resource::Quantity, util::intstr::IntOrString},
};
use kube::{
	Api, Client, ResourceExt,
	api::{ObjectMeta, Patch, PatchParams, PostParams},
};
use kube_quantity::ParsedQuantity;
use rust_decimal::Decimal;
use tracing::info;

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

impl SnapshotInfo {
	pub fn bytes(&self) -> ParsedQuantity {
		ParsedQuantity::from(Decimal::from(self.size))
	}
}

pub fn build_snapshot_list_job(
	replica: &PostgresPhysicalReplica,
	job_name: &str,
	namespace: &str,
	kopia_image: &str,
	callback_url: &str,
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

	env_vars.push(EnvVar {
		name: "SNAPSHOT_CALLBACK_URL".to_string(),
		value: Some(callback_url.to_string()),
		..Default::default()
	});

	let script = r#"set -e

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
  $ENDPOINT_ARGS \
  >&2

SNAP_FILE=$(mktemp)
trap 'rm -f "$SNAP_FILE"' EXIT
kopia snapshot list --json --all > "$SNAP_FILE" || echo "[]" > "$SNAP_FILE"
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
			owner_references: Some(vec![replica.owner_reference()]),
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
						image: Some(kopia_image.to_string()),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![script.to_string()]),
						env: Some(env_vars),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("50m".to_string())),
								("memory".to_string(), Quantity("64Mi".to_string())),
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

impl PostgresPhysicalReplica {
	pub async fn create_restore_for_snapshot(
		&self,
		client: &Client,
		snapshot: &SnapshotInfo,
	) -> Result<()> {
		let replica_name = self.name_any();
		let timestamp = Timestamp::now().strftime("%Y%m%d-%H%M%S").to_string();
		let restore_name = format!("{replica_name}-{timestamp}");

		let max_pvc_size = ParsedQuantity::try_from(self.spec.storage_size_maximum.clone())
			.unwrap_or_else(|_| ParsedQuantity::try_from("2Ti").unwrap());

		let snapshot_bytes = snapshot.bytes();
		let storage_size = match &self.spec.storage_size_override {
			Some(override_size) => override_size.clone(),
			None => {
				let computed_size = if self.spec.persistent_schemas.is_some() {
					// Persistent schemas are migrated into the restore PVC.
					// Formula: snapshot + max(10% of snapshot, last measured delta) + 5Gi
					let ten_percent = snapshot_bytes.clone() * Decimal::new(1, 1);
					let measured = self
						.status
						.as_ref()
						.and_then(|s| s.persistent_schema_data_size.as_ref())
						.and_then(|q| ParsedQuantity::try_from(q.clone()).ok())
						.unwrap_or_else(|| ParsedQuantity::from(Decimal::ZERO));
					let overhead = if measured > ten_percent {
						measured
					} else {
						ten_percent
					};
					snapshot_bytes + overhead + ParsedQuantity::try_from("5Gi").unwrap()
				} else {
					snapshot_bytes * Decimal::new(11, 1) // 1.1x
				};

				if computed_size > max_pvc_size {
					return Err(crate::error::Error::StorageLimitExceeded {
						computed: computed_size.into(),
						maximum: max_pvc_size.into(),
					});
				}

				computed_size.into()
			}
		};

		let mut restore = PostgresPhysicalRestore::new(
			&restore_name,
			PostgresPhysicalRestoreSpec {
				replica: LocalObjectReference {
					name: replica_name.clone(),
				},
				snapshot: snapshot.id.clone(),
				snapshot_size: snapshot.bytes().into(),
				storage_size,
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

		Ok(())
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
	pub async fn update_service_selector(&self, client: &Client, service_name: &str) -> Result<()> {
		let services: Api<Service> = Api::namespaced(client.clone(), &self.ns());
		let patch = serde_json::json!({
			"spec": {
				"selector": {
					"pgro.bes.au/restore": self.name_any(),
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
