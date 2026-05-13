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
| `storageSizeMaximum` | `Quantity` | No | `2Ti` | Maximum allowed PVC size. The restore will fail if the computed size exceeds this limit. |
| `resources` | `ResourceRequirements` | No | — | CPU/memory resource requirements for the PostgreSQL pods. |
| `serviceAnnotations` | `map[string]string` | No | — | Annotations applied to the Service. |
| `podAnnotations` | `map[string]string` | No | — | Annotations applied to the PostgreSQL pods. |
| `affinity` | `Affinity` | No | — | Pod scheduling affinity rules. |
| `tolerations` | `[]Toleration` | No | `[]` | Pod tolerations. |
| `readOnly` | `bool` | No | `true` | Set the restored database to read-only mode. |
| `postgresExtraConfig` | `string` | No | — | Extra lines appended to `postgresql.conf` (e.g. `shared_preload_libraries`). |
| `notifications` | `[]NotificationConfig` | No | `[]` | Notification targets called on restore events. |
| `persistentSchemas` | `[]string` | No | — | List of schema names to migrate from the previous restore to the new restore on each switchover. |
| `redaction` | `RedactionSpec` | No | — | If set, apply a Tamanu/dbt-shaped masking manifest to the restored data via the `postgresql_anonymizer` extension before switchover. Requires PostgreSQL 18+. |

The cron expression is parsed using the [cronexpr](https://docs.rs/cronexpr) crate.
It has two interesting features:
- you can append a timezone (we default to UTC): `20 15 * * * Pacific/Auckland`;
- you can use `H` in any field to use an arbitrary quantity which is derived from the replica's identity, e.g. `H 15 * * *`.

Jitter is applied to the scheduled time after the cron expression is evaluated.
The jitter is a random duration between -time/2 and +time/2.
For example, `10m` will result in a jitter between -5m and 5m.
When using `H` in the cron expression, you might want to set the jitter to zero to properly take advantage of the spread-but-stable behaviour.

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

- Requires PostgreSQL 18+ on the restore (uses the runtime-settable `extension_control_path` and `dynamic_library_path` GUCs).
- An `install-anon` init container apt-installs `postgresql_anonymizer_$N` from [Dalibo Labs](https://apt.dalibo.org/labs/) and stages the extension files onto the restore PVC under `/pgdata/extensions/anon/`. This adds roughly 30 seconds to each restore, and avoids needing a pre-built per-PG-version extension image. The install is idempotent across pod restarts (skipped if the files are already staged).
- During redaction the database is writable; once anonymisation completes, the operator sets `default_transaction_read_only = on` at the database level and demotes the analytics user back to non-superuser when `spec.readOnly` is true.

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
| `connectionInfo` | `ConnectionInfo` | Connection details (host, port, database, username, password secret). |
| `queuePosition` | `uint32` | Position in the global restore queue. |
| `notifications` | `[]NotificationStatus` | Status of each configured notification target. |
| `conditions` | `[]Condition` | Standard Kubernetes conditions. |
| `schemaMigrationJob` | `string` | Name of the active schema migration Job (set while migration is in progress). |
| `schemaMigrationPhase` | `string` | Phase of the schema migration (`active`, `complete`, or `failed: <reason>`). |
| `persistentSchemaDataSize` | `Quantity` | Measured size of persistent schema data from the last successful migration. Used to size the next restore PVC. |
| `redactionPhase` | `string` | Phase of the current restore's redaction (`active`, `complete`, `partial`, or `failed: <reason>`). `partial` means anonymisation ran but some per-column SECURITY LABEL statements were tolerated as errors (e.g. column missing on this DB version). `failed:` is sticky — it doesn't auto-retry; the next scheduled restore clears it. |
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
