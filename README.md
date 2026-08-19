# pgro — PostgreSQL Restore Operator

Monitors a Kopia backup repository for "physical" backups of PostgreSQL databases and restores them regularly within a Kubernetes cluster.

> [!WARNING]
> This is an internal project of BES International, used in our data and analytics infrastructure, as well as for backup operations and testing purposes, and not supported outside that.

## Install

Apply the operator manifest:

```
kubectl apply -f operator.yaml
```

The operator applies its own CRDs on startup, so no separate `crds.yaml`
step is needed. If you'd rather manage CRD lifecycle out-of-band (e.g.
gating schema changes at install time), generate + apply them manually:

```
cargo run --bin gen-crds > crds.yaml
kubectl apply -f crds.yaml
```

## Quick start

Make a new namespace:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: pgro-example
```

Create a Secret containing the Kopia repository credentials:

```yaml
apiVersion: v1
kind: Secret
metadata:
  namespace: pgro-example
  name: kopia-credentials
type: Opaque
stringData:
  bucket: example-bucket
  region: ap-southeast-2
  repositoryPassword: super-secret-repo-password-123
  accessKeyId: AKIAIOSFODNN7EXAMPLE
  secretAccessKey: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Create a PostgreSQL Physical Replica instance:

```yaml
apiVersion: pgro.bes.au/v1alpha1
kind: PostgresPhysicalReplica
metadata:
  namespace: pgro-example
  name: test
spec:
  kopiaSecretRef:
    name: kopia-credentials
  schedule: '* */6 * * *'
  snapshotFilter:
    tags:
      area: postgres
```

This will restore the latest snapshot matching the filter, create a new PostgreSQL instance with the restored data, and then do that again every 6 hours.

## CRDs

There are two CRDs:

- `PostgresPhysicalReplica`, the main entry point
- `PostgresPhysicalRestore`, managed by the operator, represents a single restore operation and result

### PostgresPhysicalReplica

The main user-facing resource.
Defines a continuously-refreshed replica of a PostgreSQL database restored from Kopia snapshots.

#### Spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `kopiaSecretRef` | `SecretReference` | One of | — | Reference to a Secret containing Kopia repository credentials (`bucket`, `region`, `repositoryPassword`, `accessKeyId`, `secretAccessKey`). Mutually exclusive with `canopySource`. |
| `canopySource` | `CanopySource` | One of | — | Route kopia through the canopy-mediated proxy sidecar instead of a static Secret. `{ group, type }`. Managed by the canopy worklist syncer; humans usually don't hand-author these. Mutually exclusive with `kopiaSecretRef`. |
| `snapshotFilter` | `SnapshotFilter` | No | — | Filter criteria to select which Kopia snapshot to restore. |
| `schedule` | `string` | Yes | — | Cron expression controlling how often new restores are triggered. |
| `scheduleJitter` | `string` | No | `"10m"` | Random jitter added to scheduled restores (friendly duration, e.g. `"5m"`, `"1h"`). |
| `minimumTtl` | `string` | No | — | Don't restore a new snapshot within this duration of the last restore completing. |
| `switchoverGracePeriod` | `string` | No | `"5m"` | How long to wait before deleting the old restore after a switchover. |
| `analyticsUsername` | `string` | No | `"analytics"` | Username created for analytics connections. |
| `storageClass` | `string` | No | — | Kubernetes StorageClass for the restore PVCs. |
| `storageSizeOverride` | `Quantity` | No | — | Lower bound on the PVC size. The size is still calculated from the snapshot, and this is used only when it is larger — so a replica whose snapshot outgrows it is sized from the snapshot rather than truncated. Still capped by `storageSizeMaximum`. |
| `storageSizeMaximum` | `Quantity` | No | `2Ti` | Maximum allowed PVC size. The restore will fail if the computed size exceeds this limit. |
| `resources` | `ResourceRequirements` | No | — | Pin the PostgreSQL pods' CPU/memory. When set these are used verbatim; when unset, memory is derived from the snapshot size (bounded by `resourcesFloor` and `resourcesMaximum`) and CPU comes from `resourcesFloor`. The derived memory request equals the derived limit, making the pod Guaranteed QoS — the request is what the scheduler, the autoscaler's instance selection and consolidation, and the kubelet's eviction ordering all read, so it has to reflect the pod's real size. |
| `resourcesFloor` | `ResourceRequirements` | No | — | Lower bound on the snapshot-derived resources, and the source of CPU — CPU tracks query concurrency rather than data volume, so it isn't scaled. Omit the CPU limit to let the pod burst past its request; a CPU ceiling on a database throttles bursty queries without reserving anything the request doesn't already guarantee. Ignored when `resources` is set. |
| `resourcesMaximum` | `Quantity` | No | `64Gi` | Cap on the snapshot-derived memory, so an unexpectedly large snapshot can't request more than a node can offer and leave the pod unschedulable. |
| `deploymentReadyTimeout` | `string` | No | derived | How long to wait for the restore's PostgreSQL Deployment to become Ready before failing the restore (friendly duration, e.g. `"45m"`). When unset, derived from the snapshot size — a larger data dir takes longer to open and replay WAL — floored at the operator-wide `DEPLOYMENT_READY_TIMEOUT_SECS`. |
| `shmSizeFloor` | `Quantity` | No | — | Floor on the postgres pod's `/dev/shm` sizing. When set, the Deployment uses `max(computed, shmSizeFloor)` — the computed value is derived from `resources` by [`compute_shm_and_shared_buffers`]. Rarely needed: with request equal to limit the computed value already lands at 36% of memory (`shared_buffers` ~25%). Reach for it only when `resources` is pinned to a shape that derives too little shm. |
| `serviceAnnotations` | `map[string]string` | No | — | Annotations applied to the Service. |
| `podAnnotations` | `map[string]string` | No | — | Annotations applied to the PostgreSQL pods. |
| `affinity` | `Affinity` | No | — | Pod scheduling affinity rules. |
| `tolerations` | `[]Toleration` | No | `[]` | Pod tolerations. |
| `readOnly` | `bool` | No | `true` | Set the restored database to read-only mode. |
| `ephemeral` | `bool` | No | `false` | Tear the restore down once it reaches `Active` (postgres came up healthy) instead of keeping it running. The replica only restores again when a newer snapshot is offered (canopy path) or the schedule next fires (legacy path). Used by the `verify` intent, whose job is just to prove the snapshot restores, and by `upgrade`, which additionally migrates it. |
| `postgresExtraConfig` | `string` | No | — | Extra lines appended to `postgresql.conf` (e.g. `shared_preload_libraries`). |
| `notifications` | `[]NotificationConfig` | No | `[]` | Notification targets called on restore events. |
| `persistentSchemas` | `[]string` | No | — | List of schema names to migrate from the previous restore to the new restore on each switchover. See [Persistent schemas](#persistent-schemas) below for the migration time budget and what happens on timeout. |
| `redaction` | `RedactionSpec` | No | — | If set, apply a Tamanu/dbt-shaped masking manifest to the restored data via the `postgresql_anonymizer` extension before switchover. See [RedactionSpec](#redactionspec) below. |
| `migrateTo` | `MigrationTarget` | No | — | Apply a Tamanu version's schema migrations to each restore of this replica and report how they went. `{ version, versionId }`. Set by the canopy worklist syncer for the `upgrade` intent, from the target version canopy names on the worklist entry. A restore carrying this is built read-write, since migrations are DDL, and is torn down once verified. |

The cron expression is parsed using the [cronexpr](https://docs.rs/cronexpr) crate.
It has two interesting features:
- you can append a timezone (we default to UTC): `20 15 * * * Pacific/Auckland`;
- you can use `H` in any field to use an arbitrary quantity which is derived from the replica's identity, e.g. `H 15 * * *`.

Jitter is applied to the scheduled time after the cron expression is evaluated.
The jitter is a random duration between -time/2 and +time/2.
For example, `10m` will result in a jitter between -5m and 5m.
When using `H` in the cron expression, you might want to set the jitter to zero to properly take advantage of the spread-but-stable behaviour.

#### Persistent schemas

Each switchover normally drops the new restore (so it carries only what was in the snapshot) and is fast.
The `persistentSchemas` field opts a schema (e.g. `dbt`) into being **carried across restores** via a `pg_dump | psql` migration Job that runs between the previous restore and the new one.
A healthy migration takes seconds.

The migration has a **hard time budget of 20% of the cron interval** (e.g. ~72 min on a 6-hourly schedule, ~5 h on a daily one).
If the budget is exceeded — most realistically because some external upstream condition wedges postgres mid-migration — the operator:

1. Cancels the migration Job.
2. Runs `DROP SCHEMA <name> CASCADE` for each persistent schema on the new restore.
3. Records a `SchemaMigrationTimedOut` Warning event on the replica.
4. Sets `status.schemaMigrationPhase = "timeout-skipped"`.
5. Proceeds with the switchover.

The intent is that **a usable replica beats carrying the schema through**.
The next restore cycle will re-attempt the migration if the schemas have been regenerated on the source in the meantime; until then the replica is up and serving the snapshot contents.

##### Checking that schemas actually carried across

The same "a usable replica beats carrying the schema through" tolerance means a migration can lose a schema and still let the switchover proceed — a schema that wasn't on the previous restore when the migration ran has nothing to carry across, and a `pg_dump` that dies mid-stream hands `psql` a truncated dump that `psql` applies and exits cleanly on.
So the operator does not take the Job's word for it: once the Job finishes it queries the new restore for each configured schema and settles the migration on what is actually there.

- `status.schemaMigrationPhase` is `complete` only when every configured schema is present on the new restore, and `partial` otherwise.
- `status.persistentSchemasMissing` names the ones that are not, and outlives the sweep that resets the phase.
- A `PersistentSchemaMissingOnSource` Warning event fires when a configured schema isn't on the previous restore at migration time, and `SchemaMigrationPartial` when one doesn't reach the new one.

A schema that is repeatedly reported missing is normally a cadence problem rather than an operator one: whatever generates it (e.g. a dbt build) has not run since the last switchover, so there was nothing to carry.

#### RedactionSpec

Configures applying a column-masking manifest to the restored data using the [postgresql_anonymizer](https://gitlab.com/dalibo/postgresql_anonymizer) extension.
The manifest follows the [Tamanu masking spec](https://github.com/beyondessential/tamanu/tree/main/database#masking) — any dbt project that publishes the same `meta.masking` annotation shape can be pointed at.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `manifestUrl` | `string` | Yes | — | HTTP(S) URL of the masking manifest. May contain a literal `{version}` placeholder. |
| `version` | `string` | No | — | Pinned version substituted into `{version}`. Mutually exclusive with `versionQuery`. |
| `versionQuery` | `string` | No | — | SQL query that returns one row, one text column with the version string. Run as the operator's superuser against the restore. Mutually exclusive with `version`. |
| `versionFallbackToBase` | `bool` | No | `false` | If the manifest URL with the discovered/pinned version 404s, retry with the `major.minor.0` base version. |

Example (Tamanu):

```yaml
spec:
  redaction:
    manifestUrl: "https://docs.data.bes.au/tamanu/v{version}/manifest.json"
    versionQuery: "SELECT value FROM local_system_facts WHERE key = 'currentVersion'"
    versionFallbackToBase: true
```

Notes:

- Works on any PostgreSQL major the operator otherwise supports. There's no PG-version gate because the prelude apt-installs `postgresql_anonymizer_$N` from [Dalibo Labs](https://apt.dalibo.org/labs/) per the running restore's PG version and copies the files into the standard system extension dirs (`/usr/share/postgresql/$N/extension`, `/usr/lib/postgresql/$N/lib`).
- The download is cached on the restore PVC at `/pgdata/.anon-cache/`, so a pod restart doesn't re-fetch the package — it just re-copies the cached files into the (fresh) container writable layer.
- The postgres container runs as root for the prelude (to apt-install and write to system paths) then drops back to UID 999 via `gosu` before exec'ing `postgres`. `gosu` is preinstalled in the official `postgres` image.
- During redaction the database is writable; once anonymisation completes, the operator sets `default_transaction_read_only = on` at the database level and demotes the analytics user back to non-superuser when `spec.readOnly` is true. Replicas that also set `persistentSchemas` stay writable, because the schema migration runs after redaction and writes to the same database — matching what those replicas do without redaction.
- If a redaction run fails, the switchover is held: the replica keeps serving its previous restore, a `RedactionFailed` Warning event is recorded, and the next reconcile retries. An unredacted restore is never made live.

#### SnapshotFilter

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tags` | `map[string]string` | No | Key-value tags that the snapshot must match. |
| `hostPattern` | `string` | No | Glob pattern for filtering snapshot hosts. |
| `descriptionPattern` | `string` | No | Glob pattern for filtering snapshot descriptions. |
| `pathPattern` | `string` | No | Glob pattern for filtering snapshot source paths. Windows paths are normalised to Unix style (e.g. `D:\Full` becomes `/D/Full`). |

#### NotificationConfig

A tagged union on the `target` field. Common fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `target` | `"webhook"` \| `"graphQL"` | Yes | Notification target type. |
| `url` | `string` | Yes | URL to send the notification to. |
| `headers` | `map[string]HeaderValue` | No | HTTP headers. Values can be plain strings or `{ secretKeyRef: { name, key } }`. |
| `includePassword` | `bool` | No | Include the database password in the notification payload. |

Additional fields for `target: webhook`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `method` | `string` | `"POST"` | HTTP method. |

Additional fields for `target: graphQL`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mutation` | `string` | Yes | GraphQL mutation string. |
| `variablesTemplate` | `string` | Yes | Template for the GraphQL variables payload. |

#### Status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `Pending` \| `Restoring` \| `Ready` \| `Failed` | Current phase of the replica. |
| `currentRestore` | `string` | Name of the current `PostgresPhysicalRestore` resource. |
| `previousRestore` | `string` | Name of the previous restore (pending deletion after switchover). |
| `serviceName` | `string` | Name of the Kubernetes Service pointing to the active restore. |
| `lastRestoreCompletedAt` | `Time` | When the last restore completed. |
| `nextScheduledRestore` | `Time` | When the next scheduled restore will occur. |
| `latestAvailableSnapshot` | `string` | Snapshot ID of the latest available snapshot matching the filter. |
| `canopyDesiredSnapshotId` | `string` | For canopy-sourced replicas: the snapshot the canopy worklist syncer wants restored. The reconciler triggers a new restore when this differs from the current one. |
| `verifiedSnapshotId` | `string` | For `ephemeral` replicas: the last snapshot that was verified and then torn down. Gates re-restore — the reconciler only restores again when the desired snapshot differs from this. |
| `connectionInfo` | `ConnectionInfo` | Connection details (host, port, database, username, password secret). |
| `queuePosition` | `uint32` | Position in the global restore queue. |
| `notifications` | `[]NotificationStatus` | Status of each configured notification target. |
| `conditions` | `[]Condition` | Standard Kubernetes conditions. |
| `schemaMigrationJob` | `string` | Name of the active schema migration Job (set while migration is in progress). |
| `schemaMigrationPhase` | `string` | Phase of the schema migration (`active`, `complete`, `partial`, `timeout-skipped`, or `failed: <reason>`). `complete` means every configured schema is present on the new restore; `partial` means at least one is not, and `persistentSchemasMissing` names which. See [Persistent schemas](#persistent-schemas). |
| `persistentSchemasMissing` | `[]string` | The configured `persistentSchemas` that were absent from the new restore once the last migration settled. Empty when everything carried across. Not cleared when the sweep resets `schemaMigrationPhase`, so it stays readable long after the switchover that dropped them. |
| `persistentSchemaDataSize` | `Quantity` | Measured size of persistent schema data from the last successful migration. Used to size the next restore PVC. |
| `redactionPhase` | `string` | Phase of the current restore's redaction (`active`, `complete`, `partial`, or `failed: <reason>`). `partial` means masking ran but some columns were skipped and tolerated as errors (e.g. a column the manifest names that this database doesn't have). `failed:` holds the switchover — the replica keeps serving its previous restore rather than an unredacted one — and the next reconcile retries. |
| `redactionVersion` | `string` | The manifest version resolved during the last redaction run (when `manifestUrl` is version-templated). |
| `redactionColumnsApplied` | `uint32` | Number of columns the last redaction run attempted to mask. |
| `consecutiveRestoreFailures` | `uint32` | Number of consecutive restore failures. Reset to 0 on success. After 3 consecutive failures the operator stops scheduling new restores until the counter is reset (automatically on next successful restore, or manually via `kubectl patch --subresource=status`). |

---

### PostgresPhysicalRestore

Managed by the operator.
Each resource represents a single restore operation from a Kopia snapshot.
Users should not create these directly.
Deleting this resource will drop the restored database and prompt the Replica to create a new Restore immediately.

#### Spec

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `replica` | `LocalObjectReference` | Yes | Reference to the parent `PostgresPhysicalReplica`. |
| `snapshot` | `string` | Yes | Kopia snapshot ID to restore. |
| `snapshotSize` | `Quantity` | Yes | Size of the snapshot from Kopia metadata. |
| `storageSize` | `Quantity` | Yes | Calculated PVC size (snapshot size × 1.1). |
| `migrateTo` | `MigrationTarget` | No | Copied from the parent replica: the Tamanu version whose schema migrations this restore should apply. Its presence is what sends the restore through `Migrating`. |

#### Status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `Pending` \| `Restoring` \| `Migrating` \| `Ready` \| `Switching` \| `Active` \| `Failed` | Current phase of the restore. `Migrating` is only reached when `migrateTo` is set. |
| `runId` | `string` | Canopy run-uuid minted when the restore Job is created, reused for the run's credential requests and verification report so canopy can correlate them. Canopy-backed restores only. |
| `postgresVersion` | `string` | Detected PostgreSQL major version from the restored data. |
| `createdAt` | `Time` | When the restore resource was created. |
| `restoredAt` | `Time` | When the restore job completed. |
| `activatedAt` | `Time` | When the service switched to this restore. |
| `restoreJob` | `JobStatus` | Status of the Kubernetes Job performing the restore (`name`, `phase`, `completedAt`). |
| `pvc` | `string` | Name of the PVC holding the restored data. |
| `deployment` | `string` | Name of the Deployment running PostgreSQL on the restored data. |
| `credentialsSecret` | `string` | Shared credentials secret (owned by parent replica). |
| `migrationJob` | `JobStatus` | Status of the Job that applied the target version's migrations (`name`, `phase`, `completedAt`). |
| `migrationBaseline` | `string` | Newest `logs.migrations.logged_at` the snapshot already held when the migration Job was created, as postgres-rendered text. Only a batch newer than this is attributed to the Job, so a job that dies before migrating reports nothing rather than the source deployment's last real upgrade. Absent when the restored database had no batches. |
| `migrationResult` | `MigrationResult` | What the migrations did: `totalElapsedSeconds`, `failedMigration` (absent on success; `unknown` when the target version does not record the migration it stopped at), `dataBytesBefore`/`dataBytesAfter`, and `timings` (one `{ name, elapsedSeconds }` per migration, in the order they ran). Read out of Tamanu's own `logs.migrations` audit table. |
| `conditions` | `[]Condition` | Standard Kubernetes conditions. |
