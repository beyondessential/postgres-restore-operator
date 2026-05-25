# Publish restore errors as canopy events

We have a fleet-wide event ingestion API at `https://meta.tamanu.app/api/events`
(the "canopy" public-server). Devices and operators can `POST /events` with a
small payload (`source`, `ref`, `message`, optional `description`, `severity`,
`occurredAt`, `active`) and the server folds repeated events with the same
`(source, ref)` into a single issue.

We want the postgres-restore-operator to publish to that endpoint whenever a
restore fails, so failures show up as issues in canopy without anyone having to
notice in logs.

## Authentication

The `/events` endpoint is `server-device` mTLS: the client presents a TLS
client certificate, and the server's role/identity model decides what `source`
is permitted. We don't get a bearer token or shared secret — we need a real
certificate + private key.

In Kubernetes the standard way to carry this is a `kubernetes.io/tls` Secret
with two keys: `tls.crt` (PEM-encoded certificate, possibly with a chain) and
`tls.key` (PEM-encoded private key). The user creates this Secret out of band.

## Spec

Add an optional section to `PostgresPhysicalReplicaSpec`:

```yaml
spec:
  eventPublisher:
    url: https://meta.tamanu.app/api/events
    clientCertificateSecretRef:
      name: pgro-canopy-client
    source: pgro          # optional; defaults to "pgro"
```

Fields:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | string | yes | — | Full URL of the events endpoint. |
| `clientCertificateSecretRef` | `SecretReference` | yes | — | Secret holding mTLS client cert + key. Expected keys: `tls.crt`, `tls.key`. |
| `source` | string | no | `"pgro"` | Value placed in `NewEvent.source`. |

We use `SecretReference` (same type as `kopiaSecretRef`) for consistency. The
section is optional — leaving it unset disables event publishing.

We do not need a separate "private key secret" and "certificate secret":
Kubernetes' `kubernetes.io/tls` Secret type holds both, and that's what most
issuers (cert-manager, kubelet-served, etc.) produce. The fields `tls.crt` and
`tls.key` are conventional.

## Event payload

For a restore failure we emit:

- `source`: from spec (default `"pgro"`)
- `ref`: `"<namespace>/<replica-name>/restore-failed"` — dedups so a single
  replica with repeated failures rolls up under one issue rather than spamming.
  Using the replica (not the restore) name as the ref is intentional: the
  restore name changes per attempt, but the *condition* "this replica's
  restores are failing" is one ongoing issue.
- `severity`: `"error"`
- `description`: `"Restore failed: <replica namespace>/<replica name>"`
- `message`: a few lines containing the restore name, snapshot id, and the
  failure reason (e.g. job exit code, schema migration error). For now we use
  whatever short reason we have at the `fail_restore` call site; we can
  enrich later.
- `occurredAt`: `Timestamp::now()`
- `active`: true

The endpoint folds duplicates by `(source, ref)`, so we don't need to track
sent state ourselves.

## Implementation shape

1. `src/types/replica.rs`: add `EventPublisherConfig { url, client_certificate_secret_ref, source }` and `event_publisher: Option<EventPublisherConfig>` on the spec.
2. `src/event_publisher.rs` (new module):
   - `pub struct EventPublisher` wrapping a `reqwest::Client` configured with mTLS Identity.
   - Constructor reads the cert+key from the named Secret, concatenates them, calls `reqwest::Identity::from_pem`, builds a Client with `.identity(id)`.
   - `pub async fn publish(&self, event: &NewEvent) -> Result<()>`.
   - We rebuild the client on each call site (no caching). Restore failures are rare; a fresh client per publish is fine and avoids stale-cert hassles.
3. `Cargo.toml`: enable `rustls-tls` feature on `reqwest` so `Identity::from_pem` is available. The crate already uses rustls everywhere else (kube is configured with `rustls-tls`).
4. `src/controllers/restore.rs::fail_restore`: after the existing status update / event recorder / metrics block, if the parent replica has `event_publisher` configured, fire-and-log a publish. Errors get logged at `warn`; we never let event publishing fail the reconcile.
5. README: add a new section "EventPublisher" + the row in the spec table.

## What we deliberately don't do

- No retry loop or status field for event publish success/failure. Canopy
  dedupes by `(source, ref)`; the next failed restore re-publishes anyway.
  Adding a `NotificationStatus`-style record would be churn for little gain.
- No per-restore-CRD event publisher config. It lives on the replica because
  configuration belongs to the user-facing resource; the restore CRD is
  operator-managed.
- No operator-global config. We considered making this an operator-level
  setting (one URL for the whole cluster) but that conflicts with the
  per-namespace tenancy model the operator already follows.
