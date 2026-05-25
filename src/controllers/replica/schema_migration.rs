use std::collections::BTreeMap;

use k8s_openapi::api::{
	batch::v1::{Job, JobSpec},
	core::v1::{Container, PodSecurityContext, PodSpec, PodTemplateSpec, ResourceRequirements},
};
use kube::{ResourceExt, api::ObjectMeta};
use tracing::info;

use crate::{
	controllers::jobs::{env_from_secret_name, env_literal},
	types::PostgresPhysicalReplica,
};

const MIGRATION_JOB_CONTAINER: &str = "migrate";

pub fn migration_job_name(replica_name: &str) -> String {
	format!("{replica_name}-schema-migration")
}

/// Shell script that runs `pg_dump | psql` to migrate schemas between restores.
///
/// This script:
/// - Iterates through comma-separated schema names in $SCHEMAS
/// - Uses pg_dump with --schema flags to dump each schema from source
/// - Pipes to psql to load into target
/// - Reports success/failure via callback URL
///
/// Environment variables:
///   SOURCE_HOST, SOURCE_USER, SOURCE_PASSWORD, SOURCE_DB
///   TARGET_HOST, TARGET_USER, TARGET_PASSWORD, TARGET_DB
///   SCHEMAS (comma-separated list)
///   MIGRATION_CALLBACK_URL
static MIGRATION_SCRIPT: &str = r#"#!/bin/bash
# pipefail is intentionally NOT set: psql's per-statement failures don't
# propagate into the script's exit code (see ON_ERROR_STOP discussion
# below), and pg_dump can fail mid-stream after producing partial output
# — we still want psql to apply whatever it did receive, then exit
# normally so the replica can come up.

# Parse comma-separated schema list
IFS=',' read -ra SCHEMA_ARRAY <<< "$SCHEMAS"

report_result() {
  local body="$1"
  if [ -n "$MIGRATION_CALLBACK_URL" ]; then
    curl -sf -X POST --max-time 10 \
      -H 'Content-Type: text/plain' \
      --data-binary "$body" \
      "$MIGRATION_CALLBACK_URL" 2>/dev/null || true
  fi
}

echo "=== Schema Migration: $SOURCE_RESTORE → $TARGET_RESTORE ==="
echo "Schemas to migrate: ${SCHEMA_ARRAY[*]}"
echo ""

# Build pg_dump schema args
SCHEMA_ARGS=()
for schema in "${SCHEMA_ARRAY[@]}"; do
  SCHEMA_ARGS+=(--schema="$schema")
  echo "Migrating schema: $schema"
done

# Capture psql's stderr for visibility on partial failures.
PSQL_STDERR=$(mktemp)

# ON_ERROR_STOP is deliberately NOT set: persistent_schemas like dbt
# contain views derived from upstream tables, and across upstream schema
# changes (renamed columns, dropped tables) some view DDL in the old
# replica's schema becomes invalid against the new restore's source
# tables. Failing the whole migration on the first such error blocks the
# replica from coming up at all. Tolerance trades schema completeness
# for replica availability — clients can regenerate the broken views
# afterward, but the replica must be reachable.
PGPASSWORD="$SOURCE_PASSWORD" pg_dump \
  -h "$SOURCE_HOST" -p 5432 -U "$SOURCE_USER" -d "$SOURCE_DB" \
  "${SCHEMA_ARGS[@]}" \
  --no-owner --no-privileges \
  --no-publications --no-subscriptions \
  --verbose \
| PGPASSWORD="$TARGET_PASSWORD" psql \
  -h "$TARGET_HOST" -p 5432 -U "$TARGET_USER" -d "$TARGET_DB" \
  --quiet 2> >(tee "$PSQL_STDERR" >&2)

PSQL_EXIT=$?
PSQL_ERROR_COUNT=$(grep -c '^ERROR:' "$PSQL_STDERR" 2>/dev/null || echo 0)
PSQL_ERROR_COUNT=${PSQL_ERROR_COUNT:-0}
rm -f "$PSQL_STDERR"

echo ""
if [ "$PSQL_EXIT" -ne 0 ]; then
  echo "=== psql exited non-zero ($PSQL_EXIT); proceeding so the replica can come up ===" >&2
fi

if [ "$PSQL_ERROR_COUNT" -gt 0 ]; then
  echo "=== Schema migration tolerated $PSQL_ERROR_COUNT statement error(s); some objects may need regenerating ===" >&2
  report_result "partial: $PSQL_ERROR_COUNT statement error(s)"
else
  echo "=== Schema migration completed successfully ==="
  report_result 'success'
fi

# Always exit 0: any non-fatal issues are reported via the callback
# above. Treating partial migrations as Job failures puts the operator
# into a retry loop that never converges (the same views keep failing).
exit 0
"#;

/// Build the schema migration Job spec.
///
/// The Job runs a PostgreSQL container that connects to both source and target
/// restores, dumping specified schemas from source and loading into target.
#[expect(
	clippy::too_many_arguments,
	reason = "internal builder with tightly-coupled params"
)]
pub fn build_schema_migration_job(
	replica: &PostgresPhysicalReplica,
	namespace: &str,
	source_restore_name: &str,
	target_restore_name: &str,
	source_dbname: &str,
	target_dbname: &str,
	schemas: &[String],
	reader_secret_name: &str,
	target_superuser_secret_name: &str,
	callback_url: &str,
	pg_version: i32,
) -> Job {
	let replica_name = replica.name_any();
	let job_name = migration_job_name(&replica_name);
	let image = format!("ghcr.io/cloudnative-pg/postgresql:{pg_version}");

	let source_host = format!("{source_restore_name}.{namespace}.svc");
	let target_host = format!("{target_restore_name}.{namespace}.svc");
	let schemas_csv = schemas.join(",");

	info!(
		replica = %replica_name,
		source = %source_restore_name,
		target = %target_restore_name,
		schemas = ?schemas,
		"building schema migration Job"
	);

	Job {
		metadata: ObjectMeta {
			name: Some(job_name),
			namespace: Some(namespace.to_string()),
			labels: Some(BTreeMap::from([
				("pgro.bes.au/replica".to_string(), replica_name.clone()),
				(
					"pgro.bes.au/component".to_string(),
					"schema-migration".to_string(),
				),
			])),
			owner_references: Some(vec![replica.owner_reference()]),
			..Default::default()
		},
		spec: Some(JobSpec {
			backoff_limit: Some(0),
			ttl_seconds_after_finished: Some(300),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(BTreeMap::from([
						("pgro.bes.au/replica".to_string(), replica_name),
						(
							"pgro.bes.au/component".to_string(),
							"schema-migration".to_string(),
						),
					])),
					..Default::default()
				}),
				spec: Some(PodSpec {
					restart_policy: Some("Never".to_string()),
					security_context: Some(PodSecurityContext {
						run_as_non_root: Some(true),
						run_as_user: Some(26),
						run_as_group: Some(26),
						..Default::default()
					}),
					containers: vec![Container {
						name: MIGRATION_JOB_CONTAINER.to_string(),
						image: Some(image),
						command: Some(vec!["bash".to_string(), "-c".to_string()]),
						args: Some(vec![MIGRATION_SCRIPT.to_string()]),
						env: Some(vec![
							env_from_secret_name("SOURCE_USER", reader_secret_name, "username"),
							env_from_secret_name("SOURCE_PASSWORD", reader_secret_name, "password"),
							env_from_secret_name(
								"TARGET_USER",
								target_superuser_secret_name,
								"username",
							),
							env_from_secret_name(
								"TARGET_PASSWORD",
								target_superuser_secret_name,
								"password",
							),
							env_literal("SOURCE_HOST", &source_host),
							env_literal("SOURCE_DB", source_dbname),
							env_literal("TARGET_HOST", &target_host),
							env_literal("TARGET_DB", target_dbname),
							env_literal("SCHEMAS", &schemas_csv),
							env_literal("SOURCE_RESTORE", source_restore_name),
							env_literal("TARGET_RESTORE", target_restore_name),
							env_literal("MIGRATION_CALLBACK_URL", callback_url),
						]),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								(
									"cpu".to_string(),
									k8s_openapi::apimachinery::pkg::api::resource::Quantity(
										"100m".to_string(),
									),
								),
								(
									"memory".to_string(),
									k8s_openapi::apimachinery::pkg::api::resource::Quantity(
										"128Mi".to_string(),
									),
								),
							])),
							limits: Some(BTreeMap::from([
								(
									"cpu".to_string(),
									k8s_openapi::apimachinery::pkg::api::resource::Quantity(
										"1".to_string(),
									),
								),
								(
									"memory".to_string(),
									k8s_openapi::apimachinery::pkg::api::resource::Quantity(
										"512Mi".to_string(),
									),
								),
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
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn make_replica(schemas: Vec<&str>) -> PostgresPhysicalReplica {
		use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta as K8sObjectMeta;
		PostgresPhysicalReplica {
			metadata: K8sObjectMeta {
				name: Some("test-replica".into()),
				namespace: Some("default".into()),
				uid: Some("test-uid".into()),
				..Default::default()
			},
			spec: crate::types::PostgresPhysicalReplicaSpec {
				kopia_secret_ref: Default::default(),
				snapshot_filter: None,
				schedule: "0 * * * *".into(),
				schedule_jitter: crate::util::TimeSpan(jiff::Span::new()),
				minimum_ttl: None,
				switchover_grace_period: crate::util::TimeSpan(jiff::Span::new()),
				analytics_username: "analytics".into(),
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
				storage_size_maximum: k8s_openapi::apimachinery::pkg::api::resource::Quantity(
					"2Ti".to_string(),
				),

				persistent_schemas: Some(schemas.into_iter().map(String::from).collect()),
				event_publisher: None,
			},
			status: None,
		}
	}

	#[test]
	fn migration_script_is_tolerant_to_statement_errors() {
		// The migration script must NOT use `ON_ERROR_STOP=1`. Persistent
		// schemas (e.g. dbt) contain views derived from upstream tables;
		// when upstream schema migrations rename or drop those columns,
		// some view recreations fail. Aborting the entire migration on
		// the first such error blocks the replica from coming up, which
		// is a worse outcome than a partial migration that clients can
		// patch up afterwards.
		assert!(
			!MIGRATION_SCRIPT.contains("ON_ERROR_STOP=1"),
			"migration script must not enable ON_ERROR_STOP=1 — statement errors should be tolerated so the replica can come up"
		);
		assert!(
			MIGRATION_SCRIPT.contains("exit 0"),
			"migration script must exit 0 on completion; non-fatal errors are reported via the callback body"
		);
		assert!(
			MIGRATION_SCRIPT.contains("partial"),
			"migration script must report partial migrations via the callback so the operator can surface them"
		);
	}

	#[test]
	fn migration_job_name_format() {
		assert_eq!(
			migration_job_name("my-replica"),
			"my-replica-schema-migration"
		);
	}

	#[test]
	fn build_migration_job_structure() {
		let replica = make_replica(vec!["schema1", "schema2"]);

		let job = build_schema_migration_job(
			&replica,
			"test-ns",
			"old-restore",
			"new-restore",
			"mydb",
			"mydb",
			&["schema1".to_string(), "schema2".to_string()],
			"reader-secret",
			"superuser-secret",
			"http://operator.svc:8080/api/v1/schema-migration-results/test-ns/test-replica",
			17,
		);

		let meta = &job.metadata;
		assert_eq!(meta.name.as_deref(), Some("test-replica-schema-migration"));
		assert_eq!(meta.namespace.as_deref(), Some("test-ns"));

		let labels = meta.labels.as_ref().unwrap();
		assert_eq!(labels.get("pgro.bes.au/replica").unwrap(), "test-replica");
		assert_eq!(
			labels.get("pgro.bes.au/component").unwrap(),
			"schema-migration"
		);

		let spec = job.spec.as_ref().unwrap();
		assert_eq!(spec.backoff_limit, Some(0));
		assert!(spec.active_deadline_seconds.is_none());

		let pod_spec = spec.template.spec.as_ref().unwrap();
		let container = &pod_spec.containers[0];
		assert_eq!(container.name, "migrate");
		assert_eq!(
			container.image.as_deref(),
			Some("ghcr.io/cloudnative-pg/postgresql:17")
		);

		let env = container.env.as_ref().unwrap();
		let env_names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
		assert!(env_names.contains(&"SOURCE_USER"));
		assert!(env_names.contains(&"SOURCE_PASSWORD"));
		assert!(env_names.contains(&"TARGET_USER"));
		assert!(env_names.contains(&"TARGET_PASSWORD"));
		assert!(env_names.contains(&"SOURCE_HOST"));
		assert!(env_names.contains(&"SOURCE_DB"));
		assert!(env_names.contains(&"TARGET_HOST"));
		assert!(env_names.contains(&"TARGET_DB"));
		assert!(env_names.contains(&"SCHEMAS"));
		assert!(env_names.contains(&"MIGRATION_CALLBACK_URL"));

		let schemas_env = env.iter().find(|e| e.name == "SCHEMAS").unwrap();
		assert_eq!(schemas_env.value.as_deref(), Some("schema1,schema2"));

		let source_host_env = env.iter().find(|e| e.name == "SOURCE_HOST").unwrap();
		assert_eq!(
			source_host_env.value.as_deref(),
			Some("old-restore.test-ns.svc")
		);

		let target_host_env = env.iter().find(|e| e.name == "TARGET_HOST").unwrap();
		assert_eq!(
			target_host_env.value.as_deref(),
			Some("new-restore.test-ns.svc")
		);

		let owner_refs = meta.owner_references.as_ref().unwrap();
		assert_eq!(owner_refs.len(), 1);
		assert_eq!(owner_refs[0].kind, "PostgresPhysicalReplica");
		assert_eq!(owner_refs[0].name, "test-replica");
	}

	#[test]
	fn build_migration_job_single_schema() {
		let replica = make_replica(vec!["myschema"]);

		let job = build_schema_migration_job(
			&replica,
			"test-ns",
			"old-restore",
			"new-restore",
			"mydb",
			"mydb",
			&["myschema".to_string()],
			"reader-secret",
			"superuser-secret",
			"http://operator.svc:8080/callback",
			17,
		);

		let env = job
			.spec
			.as_ref()
			.unwrap()
			.template
			.spec
			.as_ref()
			.unwrap()
			.containers[0]
			.env
			.as_ref()
			.unwrap();
		let schemas_env = env.iter().find(|e| e.name == "SCHEMAS").unwrap();
		assert_eq!(schemas_env.value.as_deref(), Some("myschema"));
	}

	#[test]
	fn build_migration_job_multiple_schemas() {
		let replica = make_replica(vec!["s1", "s2", "s3"]);
		let schemas = vec!["s1".to_string(), "s2".to_string(), "s3".to_string()];

		let job = build_schema_migration_job(
			&replica,
			"test-ns",
			"old-restore",
			"new-restore",
			"mydb",
			"mydb",
			&schemas,
			"reader-secret",
			"superuser-secret",
			"http://operator.svc:8080/callback",
			17,
		);

		let env = job
			.spec
			.as_ref()
			.unwrap()
			.template
			.spec
			.as_ref()
			.unwrap()
			.containers[0]
			.env
			.as_ref()
			.unwrap();
		let schemas_env = env.iter().find(|e| e.name == "SCHEMAS").unwrap();
		assert_eq!(schemas_env.value.as_deref(), Some("s1,s2,s3"));
	}

	#[test]
	fn build_migration_job_pg_version() {
		let replica = make_replica(vec!["myschema"]);

		for (pg_version, expected_image) in [
			(16, "ghcr.io/cloudnative-pg/postgresql:16"),
			(17, "ghcr.io/cloudnative-pg/postgresql:17"),
			(18, "ghcr.io/cloudnative-pg/postgresql:18"),
		] {
			let job = build_schema_migration_job(
				&replica,
				"test-ns",
				"old-restore",
				"new-restore",
				"mydb",
				"mydb",
				&["myschema".to_string()],
				"reader-secret",
				"superuser-secret",
				"http://operator.svc:8080/callback",
				pg_version,
			);

			let container = &job
				.spec
				.as_ref()
				.unwrap()
				.template
				.spec
				.as_ref()
				.unwrap()
				.containers[0];
			assert_eq!(container.image.as_deref(), Some(expected_image));
		}
	}
}
