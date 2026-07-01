# pgro — PostgreSQL Restore Operator

Monitors a Kopia backup repository for "physical" backups of PostgreSQL databases and restores them regularly within a Kubernetes cluster.

> [!WARNING]
> This is an internal project of BES International, used in our data and analytics infrastructure, as well as for backup operations and testing purposes, and not supported outside that.

## Install

Generate the CRDs:

```
cargo run --bin gen-crds > crds.yaml
```

Apply both the CRDs and the operator:

```
kubectl apply -f crds.yaml
kubectl apply -f operator.yaml
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
| `storageSizeOverride` | `Quantity` | No | — | Override dynamic sizing with a fixed PVC size. When absent, PVC size is calculated from snapshot size. |
| `storageSizeMaximum` | `Quantity` | No | `2Ti` | Maximum allowed PVC size. The restore will fail if the computed size exceeds this limit. |
| `resources` | `ResourceRequirements` | No | — | CPU/memory resource requirements for the PostgreSQL pods. |
| `shmSizeFloor` | `Quantity` | No | — | Floor on the postgres pod's `/dev/shm` sizing. When set, the Deployment uses `max(computed, shmSizeFloor)` — the computed value is derived from `resources` by [`compute_shm_and_shared_buffers`]. Useful when a workload's `shared_buffers` needs more shm than the resource-derived value provides, without wanting to bump the container's memory request. |
| `serviceAnnotations` | `map[string]string` | No | — | Annotations applied to the Service. |
| `podAnnotations` | `map[string]string` | No | — | Annotations applied to the PostgreSQL pods. |
| `affinity` | `Affinity` | No | — | Pod scheduling affinity rules. |
| `tolerations` | `[]Toleration` | No | `[]` | Pod tolerations. |
| `readOnly` | `bool` | No | `true` | Set the restored database to read-only mode. |
| `postgresExtraConfig` | `string` | No | — | Extra lines appended to `postgresql.conf` (e.g. `shared_preload_libraries`). |
| `notifications` | `[]NotificationConfig` | No | `[]` | Notification targets called on restore events. |
| `persistentSchemas` | `[]string` | No | — | List of schema names to migrate from the previous restore to the new restore on each switchover. See [Persistent schemas](#persistent-schemas) below for the migration time budget and what happens on timeout. |

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
| `connectionInfo` | `ConnectionInfo` | Connection details (host, port, database, username, password secret). |
| `queuePosition` | `uint32` | Position in the global restore queue. |
| `notifications` | `[]NotificationStatus` | Status of each configured notification target. |
| `conditions` | `[]Condition` | Standard Kubernetes conditions. |
| `schemaMigrationJob` | `string` | Name of the active schema migration Job (set while migration is in progress). |
| `schemaMigrationPhase` | `string` | Phase of the schema migration (`active`, `complete`, `partial`, `timeout-skipped`, or `failed: <reason>`). See [Persistent schemas](#persistent-schemas). |
| `persistentSchemaDataSize` | `Quantity` | Measured size of persistent schema data from the last successful migration. Used to size the next restore PVC. |
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

#### Status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `Pending` \| `Restoring` \| `Ready` \| `Switching` \| `Active` \| `Failed` | Current phase of the restore. |
| `postgresVersion` | `string` | Detected PostgreSQL major version from the restored data. |
| `createdAt` | `Time` | When the restore resource was created. |
| `restoredAt` | `Time` | When the restore job completed. |
| `activatedAt` | `Time` | When the service switched to this restore. |
| `restoreJob` | `JobStatus` | Status of the Kubernetes Job performing the restore (`name`, `phase`, `completedAt`). |
| `pvc` | `string` | Name of the PVC holding the restored data. |
| `deployment` | `string` | Name of the Deployment running PostgreSQL on the restored data. |
| `credentialsSecret` | `string` | Shared credentials secret (owned by parent replica). |
| `conditions` | `[]Condition` | Standard Kubernetes conditions. |
