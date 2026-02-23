use std::collections::BTreeMap;

use k8s_openapi::{
	api::{
		apps::v1::{Deployment, DeploymentSpec},
		batch::v1::{Job, JobSpec},
		core::v1::{
			Container, ContainerPort, EnvVar, ExecAction, PersistentVolumeClaim,
			PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe, ResourceRequirements,
			SecretReference, Volume, VolumeMount, VolumeResourceRequirements,
		},
	},
	apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
};
use kube::{ResourceExt, api::ObjectMeta};

use super::restore_owner_reference;
use crate::{
	controllers::{env_from_secret, env_from_secret_optional, kopia_writable_env, overlay},
	error::{Error, Result},
	types::*,
};

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

pub fn build_restore_job(
	restore: &PostgresPhysicalRestore,
	job_name: &str,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
	kopia_image: &str,
) -> Result<Job> {
	let kopia_secret = &replica.spec.kopia_secret_ref;
	let pvc_name = format!("{}-data", restore.name_any());

	let restore_script = r#"set -e

mkdir -p /tmp/kopia/config /tmp/kopia/logs /tmp/kopia/cache

ENDPOINT_ARGS=""
if [ -n "$KOPIA_ENDPOINT" ]; then
  ENDPOINT_ARGS="--endpoint=$KOPIA_ENDPOINT"
fi
if [ "$KOPIA_DISABLE_TLS" = "true" ]; then
  ENDPOINT_ARGS="$ENDPOINT_ARGS --disable-tls --disable-tls-verification"
fi

echo "Connecting to kopia repository..."
kopia repository connect s3 \
  --bucket="$KOPIA_BUCKET" \
  --region="$KOPIA_REGION" \
  --access-key="$AWS_ACCESS_KEY_ID" \
  --secret-access-key="$AWS_SECRET_ACCESS_KEY" \
  --password="$KOPIA_PASSWORD" \
  $ENDPOINT_ARGS

echo "Starting restore..."
kopia snapshot restore "$SNAPSHOT_ID" /pgdata/postgres

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

	Ok(Job {
		metadata: ObjectMeta {
			name: Some(job_name.to_string()),
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
		spec: Some(JobSpec {
			backoff_limit: Some(3),
			active_deadline_seconds: Some(7200),   // 2 hours
			ttl_seconds_after_finished: Some(120), // safety net if operator misses deletion
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
						name: "restore".to_string(),
						image: Some(kopia_image.to_string()),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![restore_script.to_string()]),
						env: Some(
							[
								vec![EnvVar {
									name: "SNAPSHOT_ID".to_string(),
									value: Some(restore.spec.snapshot.clone()),
									..Default::default()
								}],
								kopia_writable_env(),
								vec![
									env_from_secret("KOPIA_BUCKET", kopia_secret, "bucket"),
									env_from_secret("KOPIA_REGION", kopia_secret, "region"),
									env_from_secret(
										"AWS_ACCESS_KEY_ID",
										kopia_secret,
										"accessKeyId",
									),
									env_from_secret(
										"AWS_SECRET_ACCESS_KEY",
										kopia_secret,
										"secretAccessKey",
									),
									env_from_secret(
										"KOPIA_PASSWORD",
										kopia_secret,
										"repositoryPassword",
									),
									env_from_secret_optional(
										"KOPIA_ENDPOINT",
										kopia_secret,
										"endpoint",
									),
									env_from_secret_optional(
										"KOPIA_DISABLE_TLS",
										kopia_secret,
										"disableTls",
									),
								],
							]
							.concat(),
						),
						volume_mounts: Some(vec![VolumeMount {
							name: "pgdata".to_string(),
							mount_path: "/pgdata".to_string(),
							..Default::default()
						}]),
						resources: Some(ResourceRequirements {
							requests: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("500m".to_string())),
								("memory".to_string(), Quantity("1Gi".to_string())),
							])),
							limits: Some(BTreeMap::from([
								("cpu".to_string(), Quantity("2".to_string())),
								("memory".to_string(), Quantity("4Gi".to_string())),
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
					..Default::default()
				}),
			},
			..Default::default()
		}),
		..Default::default()
	})
}

pub fn build_deployment(
	restore: &PostgresPhysicalRestore,
	name: &str,
	namespace: &str,
	replica: &PostgresPhysicalReplica,
) -> Result<Deployment> {
	let pvc_name = format!("{name}-data");
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

	let pg_image = format!("postgres:{pg_version}");

	let locale_script = r#"set -ex
PGDATA=/pgdata/pgdata

echo "Creating any missing locales found in cluster..."
pg_controldata "$PGDATA" | grep -E '^LC_(COLLATE|CTYPE)' | sed 's/.*:[[:space:]]*//' | sort -u | while IFS= read -r loc; do
  if locale -a 2>/dev/null | grep -qxF "$loc"; then
    echo "Locale '$loc' already exists"
    continue
  fi
  codepage=$(echo "$loc" | grep -oE '[0-9]+$')
  case "$codepage" in
    1250) charset="CP1250" ;;
    1251) charset="CP1251" ;;
    1252) charset="CP1252" ;;
    1253) charset="CP1253" ;;
    1254) charset="CP1254" ;;
    1255) charset="CP1255" ;;
    1256) charset="CP1256" ;;
    1257) charset="CP1257" ;;
    1258) charset="CP1258" ;;
    65001) charset="UTF-8" ;;
    *) charset="UTF-8" ;;
  esac
  echo "Creating locale '$loc' (charset: $charset)..."
  localedef -i en_US -f "$charset" "$loc" || true
done
"#
	.to_string();

	// persistent_schemas needs write access to receive the migrated data
	let effective_read_only = replica.spec.read_only && replica.spec.persistent_schemas.is_none();
	let read_only = effective_read_only.to_string();

	let has_overlay = replica.spec.overlay_database.is_some();
	let reader_secret = SecretReference {
		name: Some(overlay::overlay_reader_secret_name(
			&restore.spec.replica.name,
		)),
		namespace: Some(namespace.to_string()),
	};

	let reader_user_block = if has_overlay {
		r#"
if [ -n "$READER_USERNAME" ] && [ -n "$READER_PASSWORD" ]; then
  echo "Creating overlay read-only user..."
  psql -U postgres -d postgres << READEREOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${READER_USERNAME}') THEN
    CREATE ROLE ${READER_USERNAME} WITH LOGIN PASSWORD '${READER_PASSWORD}';
  ELSE
    ALTER ROLE ${READER_USERNAME} WITH PASSWORD '${READER_PASSWORD}';
  END IF;
END
\$\$;
GRANT pg_read_all_data TO ${READER_USERNAME};
READEREOF
fi
"#
		.to_string()
	} else {
		String::new()
	};

	let extra_config_block = if let Some(ref extra) = replica.spec.postgres_extra_config {
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
shared_buffers = 128MB
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
  "$PGDATA/postgresql.conf"

echo "Configuring stderr logging..."
echo "log_destination = 'stderr'" >> "$PGDATA/postgresql.conf"
echo "logging_collector = off" >> "$PGDATA/postgresql.conf"

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

echo "Fixing database locales incompatible with this OS (single-user mode)..."
echo "UPDATE pg_database SET datcollate = 'C.UTF-8', datctype = 'C.UTF-8', datcollversion = NULL WHERE datcollate NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST') OR datctype NOT IN ('C', 'C.UTF-8', 'PG_UNICODE_FAST');" \
  | postgres --single -D "$PGDATA" postgres
LOCALE_CHANGED=1

echo "Starting temporary postgres to configure analytics user..."
pg_ctl -D "$PGDATA" -o "-c listen_addresses='' -c log_min_messages=WARNING" -w start

echo "Fixing database locales (post-startup fallback)..."
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

if [ "$PG_MAJOR" -ge 14 ]; then
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
GRANT pg_read_all_data TO ${{ANALYTICS_USERNAME}};
SQLEOF

  if [ "{read_only}" = "true" ]; then
    echo "Read-only mode with PG >= 14, granted pg_read_all_data"
  else
    echo "Read-write mode with PG >= 14, granting pg_write_all_data + CREATE ON DATABASE..."
    psql -U postgres -d postgres << SQLEOF
GRANT pg_write_all_data TO ${{ANALYTICS_USERNAME}};
DO \$\$
DECLARE
  dbname text;
BEGIN
  FOR dbname IN SELECT d.datname FROM pg_database d WHERE d.datname NOT IN ('template0', 'template1')
  LOOP
    EXECUTE format('GRANT CREATE ON DATABASE %I TO %I', dbname, '${{ANALYTICS_USERNAME}}');
  END LOOP;
END
\$\$;
SQLEOF
  fi
else
  echo "PG < 14, granting superuser to analytics user..."
  psql -U postgres -d postgres << SQLEOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${{ANALYTICS_USERNAME}}') THEN
    CREATE ROLE ${{ANALYTICS_USERNAME}} WITH LOGIN SUPERUSER PASSWORD '${{ANALYTICS_PASSWORD}}';
  ELSE
    ALTER ROLE ${{ANALYTICS_USERNAME}} WITH SUPERUSER PASSWORD '${{ANALYTICS_PASSWORD}}';
  END IF;
END
\$\$;
SQLEOF
fi
{reader_user_block}
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

	let labels = BTreeMap::from([
		(
			"pgro.bes.au/replica".to_string(),
			restore.spec.replica.name.clone(),
		),
		("pgro.bes.au/restore".to_string(), name.to_string()),
	]);

	let mut init_env = vec![
		EnvVar {
			name: "ANALYTICS_USERNAME".to_string(),
			value: Some(replica.spec.analytics_username.clone()),
			..Default::default()
		},
		env_from_secret("ANALYTICS_PASSWORD", &creds_secret, "password"),
		EnvVar {
			name: "READ_ONLY".to_string(),
			value: Some(read_only.to_string()),
			..Default::default()
		},
	];

	if has_overlay {
		init_env.push(env_from_secret(
			"READER_USERNAME",
			&reader_secret,
			"username",
		));
		init_env.push(env_from_secret(
			"READER_PASSWORD",
			&reader_secret,
			"password",
		));
	}

	Ok(Deployment {
		metadata: ObjectMeta {
			name: Some(name.to_string()),
			namespace: Some(namespace.to_string()),
			labels: Some(labels.clone()),
			owner_references: Some(vec![restore_owner_reference(restore)]),
			..Default::default()
		},
		spec: Some(DeploymentSpec {
			replicas: Some(1),
			selector: LabelSelector {
				match_labels: Some(BTreeMap::from([(
					"pgro.bes.au/restore".to_string(),
					name.to_string(),
				)])),
				..Default::default()
			},
			strategy: Some(k8s_openapi::api::apps::v1::DeploymentStrategy {
				type_: Some("Recreate".to_string()),
				..Default::default()
			}),
			template: PodTemplateSpec {
				metadata: Some(ObjectMeta {
					labels: Some(labels),
					annotations: replica.spec.pod_annotations.clone(),
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
								name: "pgdata".to_string(),
								mount_path: "/pgdata".to_string(),
								read_only: Some(true),
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
						},
					]),
					containers: vec![Container {
						name: "postgres".to_string(),
						image: Some(pg_image),
						command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
						args: Some(vec![r#"
if [ -f /pgdata/needs-reindex ]; then
  PG_MAJOR=$(cat /pgdata/pgdata/PG_VERSION)
  (
    while ! pg_isready -q -U postgres -d postgres; do sleep 2; done
    for db in $(psql -U postgres -d postgres -At -c "SELECT datname FROM pg_database WHERE datallowconn AND datname <> 'template0'"); do
      echo "Background reindex after locale change: $db"
      if [ "$PG_MAJOR" -ge 14 ]; then
        psql -U postgres -d "$db" -c "REINDEX DATABASE CONCURRENTLY \"$db\";" 2>&1 || true
      else
        psql -U postgres -d "$db" -c "REINDEX DATABASE \"$db\";" 2>&1 || true
      fi
    done
    rm -f /pgdata/needs-reindex
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
						volume_mounts: Some(vec![VolumeMount {
							name: "pgdata".to_string(),
							mount_path: "/pgdata".to_string(),
							..Default::default()
						}]),
						readiness_probe: Some(Probe {
							exec: Some(ExecAction {
								command: Some(vec![
									"pg_isready".to_string(),
									"-U".to_string(),
									"postgres".to_string(),
									"-d".to_string(),
									"postgres".to_string(),
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
						resources: replica.spec.resources.clone(),
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
