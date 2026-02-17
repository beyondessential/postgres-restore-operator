# pgro.bes.au — PostgreSQL Restore Operator

Monitors a Kopia backup repository for "physical" backups of PostgreSQL databases and restores them regularly within a Kubernetes cluster.

> [!WARNING]
> This is an internal project of BES.au, used in our data and analytics infrastructure, as well as for backup operations and testing purposes.
> As such no guarantees are made about stability beyond our internals.

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
| `kopiaSecretRef` | `SecretReference` | Yes | — | Reference to a Secret containing Kopia repository credentials (`bucket`, `region`, `repositoryPassword`, `accessKeyId`, `secretAccessKey`). |
| `snapshotFilter` | `SnapshotFilter` | No | — | Filter criteria to select which Kopia snapshot to restore. |
| `schedule` | `string` | Yes | — | Cron expression controlling how often new restores are triggered. |
| `scheduleJitter` | `string` | No | `"10m"` | Random jitter added to scheduled restores (friendly duration, e.g. `"5m"`, `"1h"`). |
| `minimumTtl` | `string` | No | — | Don't restore a new snapshot within this duration of the last restore completing. |
| `switchoverGracePeriod` | `string` | No | `"5m"` | How long to wait before deleting the old restore after a switchover. |
| `analyticsUsername` | `string` | No | `"analytics"` | Username created for analytics connections. |
| `storageClass` | `string` | No | — | Kubernetes StorageClass for the restore PVCs. |
| `storageSizeOverride` | `Quantity` | No | — | Override dynamic sizing with a fixed PVC size. When absent, PVC size is calculated from snapshot size. |
| `resources` | `ResourceRequirements` | No | — | CPU/memory resource requirements for the PostgreSQL pods. |
| `serviceAnnotations` | `map[string]string` | No | — | Annotations applied to the Service. |
| `podAnnotations` | `map[string]string` | No | — | Annotations applied to the PostgreSQL pods. |
| `affinity` | `Affinity` | No | — | Pod scheduling affinity rules. |
| `tolerations` | `[]Toleration` | No | `[]` | Pod tolerations. |
| `readOnly` | `bool` | No | `true` | Set the restored database to read-only mode. |
| `postgresExtraConfig` | `string` | No | — | Extra lines appended to `postgresql.conf` (e.g. `shared_preload_libraries`). |
| `notifications` | `[]NotificationConfig` | No | `[]` | Notification targets called on restore events. |
| `overlayDatabase` | `OverlayDatabaseConfig` | No | — | Optional overlay database configuration (persistent database via CNPG). |

Using the `overlayDatabase` requires the CloudNative-PG operator to be installed and configured.
Installing the CNPG cluster-level catalogs is optional but recommended.

The cron expression is parsed using the [cronexpr](https://docs.rs/cronexpr) crate.
It has two interesting features:
- you can append a timezone (we default to UTC): `20 15 * * * Pacific/Auckland`;
- you can use `H` in any field to use an arbitrary quantity which is derived from the replica's identity, e.g. `H 15 * * *`.

Jitter is applied to the scheduled time after the cron expression is evaluated.
The jitter is a random duration between -time/2 and +time/2.
For example, `10m` will result in a jitter between -5m and 5m.
When using `H` in the cron expression, you might want to set the jitter to zero to properly take advantage of the spread-but-stable behaviour.

#### SnapshotFilter

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tags` | `map[string]string` | No | Key-value tags that the snapshot must match. |
| `hostPattern` | `string` | No | Glob pattern for filtering snapshot hosts. |
| `descriptionPattern` | `string` | No | Glob pattern for filtering snapshot descriptions. |

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

#### OverlayDatabaseConfig

Configures an overlay database backed by a CNPG Cluster that imports schemas from the restored replica.
Two strategies are available: `fdw` (Foreign Data Wrappers, the default) and `copy` (`pg_dump | psql`).
The `copy` strategy tolerates PostgreSQL version differences and does not require the restore to stay alive after the copy completes.
This can be used to persistently write data in other schemas in the overlay without interfacing between two databases.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `strategy` | `"fdw"` \| `"copy"` | No | `"fdw"` | Strategy for populating the overlay from the restore. `fdw` creates foreign tables (restore must stay alive). `copy` uses `pg_dump \| psql` to copy data (tolerates version differences, restore can be discarded). |
| `postgresVersion` | `uint32` | No | Resolved from image catalog, or `17` | PostgreSQL major version for the CNPG cluster. |
| `imageCatalog` | `ImageCatalogRef` | No | ClusterImageCatalog | CNPG image catalog for PG version discovery and image resolution. |
| `storageSizeOverride` | `Quantity` | No | Auto-sized | Override for the overlay PVC size. Auto-sizing: `5Gi + ceil(snapshotSize / 10)`, ratchets up only. |
| `storageClass` | `string` | No | — | Storage class for the overlay database PVC. |
| `resources` | `ResourceRequirements` | No | — | Resource requirements for the overlay database pods. |
| `affinity` | `Affinity` | No | — | Pod affinity rules for the overlay database. |
| `tolerations` | `[]Toleration` | No | `[]` | Tolerations for the overlay database pods. |
| `serviceAnnotations` | `map[string]string` | No | — | Annotations for the overlay database's `-rw` Service. |
| `schemaMapping` | `map[string]string` | No | All schemas | Schema import mapping. Key = remote schema, Value = local schema in overlay DB. If absent, all user schemas are imported at their original names. |
| `importGenerated` | `bool` | No | `false` | Include `GENERATED` column expressions when importing foreign schemas (FDW only). Ignored for `copy` strategy. |
| `retainRestore` | `bool` | No | `true` | When `false` and strategy is `copy`, delete the restore Deployment and PVC after a successful copy. The restore CR is kept for bookkeeping. Ignored for `fdw` strategy. |

#### ImageCatalogRef

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | `string` | Yes | — | Name of the image catalog resource. |
| `kind` | `string` | No | `"ClusterImageCatalog"` | Kind of the image catalog (`ClusterImageCatalog` or `ImageCatalog`). |

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
| `connectionInfo` | `ConnectionInfo` | Connection details (host, port, database, username, password secret). |
| `queuePosition` | `uint32` | Position in the global restore queue. |
| `notifications` | `[]NotificationStatus` | Status of each configured notification target. |
| `conditions` | `[]Condition` | Standard Kubernetes conditions. |
| `overlayClusterName` | `string` | Name of the CNPG Cluster CR for the overlay database. |
| `overlayRestore` | `string` | Name of the restore whose schemas are currently imported into the overlay. |
| `overlayStorageSize` | `Quantity` | Current (possibly ratcheted) storage size of the overlay PVC. |
| `overlayPostgresVersion` | `uint32` | Resolved PG major version used for the overlay cluster. |

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
