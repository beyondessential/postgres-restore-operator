# pgro.bes.au — PostgreSQL Restore Operator

Monitors a Kopia backup repository for "physical" backups of PostgreSQL databases and restores them regularly within a Kubernetes cluster.

> [!WARNING]
> This is an internal project of BES.au, used in our data and analytics infrastructure, as well as for backup operations and testing purposes.
> As such no guarantees are made about stability beyond our internal usage.

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
