# Canopy backup-credentials integration

Implementation spec for the pgro side of the Canopy "backup-credentials"
system. Canopy is BES's backup control plane: it issues short-lived,
per-group S3 credentials for kopia repositories, owns repo maintenance and
retention, and tracks backup health through three signals — *backed up* (1),
*persisted* (2), and **restorable** (3). pgro is the producer of signal 3.

The authoritative design lives in the canopy repo
(`docs/plans/backup-credentials.md`, section "External restore consumers +
restore-verification (PGRO)"). This document is the pgro-side contract and
implementation plan; it does not re-decide canopy-side shape.

> **Stage.** This is an *additive, later-stage* change in the overall
> rollout. It depends on canopy having already shipped: per-bucket roles,
> the `restore` session-policy path, repo-password ownership, the
> issues/events alerting, and a new non-server-bound device role.
> Until those exist on the canopy side there is nothing for pgro to call.
> Build pgro's side behind the CRD opt-in below so today's static-Secret
> path keeps working unchanged during migration.

## What changes, in one paragraph

Today a `PostgresPhysicalReplica` carries a `kopiaSecretRef` pointing at a
hand-created `kopia-credentials` Secret that holds **long-lived** AWS access
keys plus the kopia repository password (`src/kopia.rs`, `REQUIRED_KEYS`). The
operator copies those values into env vars and CLI flags
(`--access-key=`/`--secret-access-key=`/`--password=`) on every snapshot-list
and restore Job (`src/controllers/replica/resources.rs`,
`src/controllers/restore/builders.rs`, `src/kopia.rs::kopia_connect_args`).
This is exactly the long-lived-creds pattern the canopy system exists to
eliminate. The integration replaces that static Secret with a
**canopy-mediated, proxy-mediated** flow: the replica references a **canopy
group** instead of a Secret; alongside each kopia Job runs a
**bestool-kopia loopback SigV4 re-signing proxy** (the same S3P proxy
canopy's own maintenance Jobs and bestool's device backups/restores use)
that fetches short-lived restore credentials + the S3 target + repo password
from canopy, refreshes them transparently between requests, and re-signs
each kopia S3 request with the live creds; kopia itself talks only to
`127.0.0.1` with dummy keys, never touching real AWS material. After each
restore the operator **reports the outcome back to canopy** (signal 3,
restore-verification). pgro is a **restore-only** consumer — it never
writes to or deletes from the bucket.

## Why canopy-mediated (not pgro assuming a role directly)

The relationship is **bidirectional**: creds flow out (canopy → pgro) and
restore-verification reports flow in (pgro → canopy). Routing both over one
first-party channel keeps it a single relationship rather than two disjoint
mechanisms (an AWS role pgro assumes + a separate reporting path). pgro is
trusted first-party infra but is **not** a fleet device and is **not** a
member of the group it reads, so canopy gates pgro behind an
operator-authorized, audited **external-restore grant** ("consumer C may
read group X, read-only") — a deliberate, controlled cousin of the
cross-group restore that is banned for devices. pgro does not implement the
grant; it just authenticates as itself and names the group.

---

## Part 1 — Creds out (Canopy → pgro)

### 1.1 Canopy client: depend on `bestool-canopy`

The bestool repo already publishes a `bestool-canopy` crate (`crates.io`,
currently `0.4.0`) that owns the canopy HTTP wire types and the
`CanopyClient`. pgro takes a normal `cargo` dependency on it rather than
re-implementing. The same crate is also used by the proxy's credential
provider (`bestool-kopia`, see §1.3), so depending on it is on the path
either way.

Wire types pgro consumes verbatim from `bestool-canopy::backup`:

- `BackupCredentials` — the `credential_process`-shaped response from `POST
  /backup-credentials`: `Version, AccessKeyId, SecretAccessKey, SessionToken,
  Expiration`. PascalCase on the wire; the crate already handles that.
- `BackupTarget` — `{ storage, bucket, prefix, region, repo_password }` from
  `GET /backup-target`.
- `BackupReport` — the `POST /backup-report` body. **pgro does not use this
  type for signal-3** (see §2 for why a separate endpoint is needed); it is
  named here only because it is the de-facto shape of a run report and pgro's
  `RestoreReport` will resemble it for the fields they share.
- `Purpose::Restore` — the enum variant pgro passes to `backup_credentials`.

Client functions pgro calls on the published `CanopyClient`:

- `client.backup_credentials(base_url, backup_type, Purpose::Restore)` —
  authenticates pgro (see Part 3), returns `BackupCredentials`. Canopy maps
  the consumer + group + type to its per-bucket role, assumes it
  cross-account with the read-only restore **session policy**, and returns
  prod-account creds for that bucket. A `403`/`409` (grant absent or group
  unconfigured) is a surfaced error, not a transient retry.
- `client.backup_target(base_url)` — returns `BackupTarget` (or `Dormant`).
- The credential provider in the proxy (§1.3) drives both; pgro's operator
  code does not call them directly.

**Caveats forcing canopy-side changes before pgro can use the published
crate as-is:**

- `CanopyClient` today is device-mTLS-only (constructor takes a
  `device_key_pem`) — which **is fine for pgro**, because canopy already
  has non-server-bound device roles (`releaser-device`, `admin-device`)
  alongside `server-device`. pgro becomes a device of a new role (working
  title `backup-restore`); the existing `CanopyClient` constructor works
  unchanged. See Part 3 for the auth design.
- `BackupCredentialsRequest` is `{ type, purpose }` — no `group` field. The
  external-restore path needs canopy to either add a `group` field or
  expose a sibling endpoint that takes one (a `backup-restore`-role device
  has no implicit group, unlike `server-device`). Canopy-owed wire change,
  not a pgro one.

The base URL + device key/cert come from operator-level config (see §3.1),
held on the `Context` (`src/context.rs`) alongside the existing
`http_client`. A small `src/canopy.rs` wires the `CanopyClient` into the
operator's context and holds the report path (§2).

### 1.2 CRD change: reference a canopy group, not a Secret

Add an alternative to `kopiaSecretRef` on `PostgresPhysicalReplicaSpec`
(`src/types/replica.rs`). The static-Secret path stays valid for migration
and for non-canopy repos (e.g. the minio integration-test repo), so make the
two mutually-exclusive and keep `kopia_secret_ref` optional:

```rust
/// Reference to a Secret containing kopia repository credentials.
/// Mutually exclusive with `canopyBackup`. One of the two is required.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub kopia_secret_ref: Option<SecretReference>,

/// Fetch short-lived read-only restore credentials from Canopy instead of
/// a static Secret. Mutually exclusive with `kopiaSecretRef`.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub canopy_backup: Option<CanopyBackupRef>,
```

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CanopyBackupRef {
    /// The Canopy server-group id (UUID) whose backups this replica restores.
    pub group: String,
    // Canopy base URL + auth come from operator-level config, not per-replica,
    // so every replica trusts the same canopy. (Open question 3.)
}
```

Making `kopia_secret_ref` `Option` is a CRD change touching every existing
construction site (`controllers/replica/{resources,tests}.rs`,
`controllers/restore/tests.rs`, `controllers/replica/schema_migration.rs`).
Validate "exactly one of `kopiaSecretRef` / `canopyBackup`" in the replica
reconcile (`src/controllers/replica.rs`) and surface a `Warning` event +
`Failed` phase on violation, mirroring how an invalid kopia secret is handled
today (`Error::InvalidKopiaSecret`).

Per `AGENTS.md`: a CRD spec change **must** update the README CRD tables and
regenerate CRDs (`cargo run --bin gen-crds > crds.yaml`).

### 1.3 Where the creds are consumed — the proxy-sidecar architecture

The bestool + canopy family has already solved this and pgro joins the
solution rather than picking a new one. Every kopia caller in the family —
bestool device backups/restores, canopy's maintenance/inspection/s3-metrics
Jobs — runs kopia through a small loopback **SigV4 re-signing proxy** (the
"S3P" spec at `bestool/.workhorse/specs/canopy/s3-sigv4-proxy.md`,
implementation in `bestool/crates/kopia/src/proxy.rs`). The shape:

- kopia is invoked with `--endpoint=127.0.0.1:<port>`, `--disable-tls`, and
  meaningless **dummy** access/secret keys
  (`bestool_kopia::PROXY_DUMMY_ACCESS_KEY` / `PROXY_DUMMY_SECRET_KEY`). It
  carries no real AWS material at all.
- A small in-process HTTP proxy bound to that loopback port discards kopia's
  dummy signature, re-signs each request with the **current** STS creds, and
  forwards over TLS to the real S3 host. Streaming bodies (chunked SigV4
  uploads) are re-signed chunk by chunk.
- The proxy's `CredentialProvider` (a trait in `bestool_kopia::proxy`)
  refreshes creds out-of-band as they near expiry. bestool's
  `CanopyCredentialProvider`
  (`bestool/crates/bestool/src/actions/canopy/backup/provider.rs`) hits
  canopy's `POST /backup-credentials` directly. pgro's provider plugs the
  same trait but hits the **operator's in-cluster credential broker**
  (Part 3.1.2) rather than canopy directly — because only the operator
  pod is a canopy device. Same ~2-minute refresh-margin shape.
- The kopia repo password (from canopy's `GET /backup-target`) is
  fetched once by the operator before Job-spawn and passed into the Job
  pod via env / argv. The proxy sidecar passes it to kopia via
  `--password=` on `connect`.

Under this model the credential lifetime is invisible to kopia: a refresh
between two requests changes only the signing key the next one uses. A long
restore is bounded by **canopy reachability**, not by any single credential
lifetime — so the 1-hour STS cap is a non-issue (Open Q 1 collapses), and
kopia's `AWS_SESSION_TOKEN` handling is irrelevant because kopia never sees
real AWS creds (Open Q 2 collapses).

**Where the proxy runs.** A **sidecar container in each kopia Job** —
mirroring bestool's one-proxy-per-op model. The operator templates the Job
spec to have two containers in one Pod:

- the kopia container, pointed at `127.0.0.1:<port>` with `--disable-tls`,
  the dummy keys, `--bucket`/`--prefix`/`--region` from the canopy target,
  and `--password=` set to the canopy repo password;
- a pgro-owned tiny sidecar binary linking `bestool-kopia` (for
  `proxy::spawn`) and a pgro-implemented `CredentialProvider` that calls
  the **operator's in-cluster credential-broker endpoint** (Part 3.1.2)
  rather than canopy directly. The sidecar process binds the loopback
  port, runs the proxy until the kopia container exits, and exits
  itself. Built into a pgro-published image; both bestool crates are
  ordinary cargo deps.

Loopback-only is a security invariant of S3P (§Security in the spec): the
proxy binds a loopback literal and is never exposed off-host. Sharing a Pod
puts the kopia container and the sidecar in one network namespace, which
satisfies that invariant. A k8s `Service`-fronted proxy would not, and is
ruled out.

The connect args helper (`src/kopia.rs::kopia_connect_args`) gains a
"canopy" variant emitting `--endpoint=127.0.0.1:<port>`, `--disable-tls`,
the dummy keys, and the canopy bucket/region/prefix — the legacy
`KopiaCredentials` (static keys, optional minio endpoint) stays
unmodified for the non-canopy path.

Snapshot-list Jobs use the same sidecar shape; the proxy is just as cheap
to spawn for a short op. Confirms Open Q 6.

### 1.4 Drop the static keys

Once a replica uses `canopyBackup`:
- No `accessKeyId` / `secretAccessKey` / `repositoryPassword` ever live in a
  user-managed Secret. The Job container running kopia **never sees real AWS
  credentials** — only the loopback proxy endpoint and meaningless dummy
  keys. The real creds live in the sidecar's process memory, refreshed
  transparently, and are never written to disk or to a k8s object.
- The repo password is fetched per-Job from canopy's `GET /backup-target`
  and passed to kopia via `--password=` on `connect`; it is also held only
  in the sidecar's process memory (and kopia's argv for the lifetime of the
  Job — same posture as bestool).
- `REQUIRED_KEYS` in `src/kopia.rs` still governs the legacy static path;
  the canopy path skips `validate_kopia_secret` entirely.

---

## Part 2 — Reports in (pgro → Canopy): Signal 3, restore-verification

A successful pgro restore *proves the backup is restorable* — the strongest
backup-health signal there is, stronger than signal 2's "a snapshot exists in
the repo". pgro reports each restore outcome to canopy, which records it in
its `backup_restore_checks` table and reconciles it against
`backup_repo_snapshots` / `backup_runs`. A failed or stale restorability
check becomes a high-severity **group-level** alert on the canopy side (the
server-independent incident path, like poisoning detection) — pgro does not
manage alerting, it only reports.

### 2.0 Why not reuse `POST /backup-report`

Canopy already serves `POST /backup-report` and it already accepts
`{ purpose: "restore", outcome, snapshot_id, error, run_id, ... }` — for
**devices**. The shape looks superficially close to what pgro wants, so it
is worth being explicit about why pgro needs a **new** ingest endpoint
rather than reusing `/backup-report`:

1. **Identity model is upside-down.** The handler resolves `device_id`,
   `server_id`, and `group_id` from the **authenticated mTLS context**
   (`canopy/crates/public-server/src/backup.rs:495`), not the request body.
   pgro is none of those — it has no `device_id`, no `server_id`, no
   implicit group. Wiring a `group_id` into the body would break the
   security invariant that an authenticated device cannot report a run as
   some *other* group.
2. **The schema is device-shaped.** `backup_runs` has
   `device_id UUID NOT NULL REFERENCES devices(id)` and
   `group_id NOT NULL REFERENCES server_groups(id)`. pgro reports have no
   device; the FK can't be satisfied. The only honest options are pollute
   the table with sentinel devices, drop the FK, or add a separate table —
   which is exactly the `backup_restore_checks` the canopy plan names.
3. **Two different "restore" meanings collide on `purpose`.** A device with
   `purpose=restore` (`bestool canopy restore`, used for clone / DR-test on
   the same fleet) writes its own outcome into `backup_runs`. That is NOT a
   signal-3 verification — it is a normal device-side restore and should
   not raise a group-level "the backup isn't restorable" incident. So
   `purpose=restore` alone is not a sufficient discriminator.
4. **Alerting paths diverge.** A `/backup-report` failure feeds **per-server
   staleness** (signal 1, server-scoped). Signal 3 must feed **group-scoped**
   `raise_group_event(ref = "restore-verification", severity = Error)`
   bypassing per-server `is_monitored`. Reusing the endpoint means branching
   inside the handler on actor type — at which point you have already forked
   it.
5. **Side-effects don't match.** The handler clears `BackupRequest` on every
   report (`backup.rs:534`) so the heartbeat stops re-emitting "back up now"
   for that server. Irrelevant and possibly harmful for a pgro report;
   another conditional.
6. **Payload shape is wrong for signal 3.** `ReportArgs` carries
   `bytes_uploaded` and `s3_*_bytes` (pgro's proxy will emit the same
   tallies, fine) but lacks `replica_healthy`, postgres major version, and
   `observed_at` — the very state that makes signal 3 stronger than signal
   2. Squeezing those into `error: Option<String>` loses structure for the
   load-bearing fields.
7. **`run_id` semantics don't transfer.** For devices, `run_id` is the same
   UUID across `/backup-credentials` (issuance audit) and `/backup-report`,
   minted at run start, duplicate → 409. For pgro the meaningful identity
   is the **snapshot being verified**, not the verifier's run UUID; a
   pgro-minted run UUID has no cross-table linkage to
   `backup_credential_issuances` (pgro's issuances aren't even in that
   table — different consumer type).

By the time canopy has added `group_id` to the body, relaxed (or split off)
the FKs, branched the handler on actor type, routed failures to a different
alerting path, and gated the `BackupRequest::clear` side-effect, the handler
has effectively forked. Cleaner for canopy to expose a sibling endpoint
(working title `POST /restore-verification`, canopy-side naming theirs)
writing to a new `backup_restore_checks` table and routing failures through
`raise_group_event`. The body shape pgro proposes is in §2.1.

### 2.1 Report shape (`RestoreReport`)

pgro already has all of this in `PostgresPhysicalRestoreStatus` and the
`NotificationPayload` machinery (`src/notifications.rs`); this is a new
notification *target*, not new data collection.

```rust
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreReport {
    /// The canopy group this replica restores from (CanopyBackupRef.group).
    pub group: String,
    /// The kopia snapshot id that was restored — the cross-reference key
    /// canopy joins against backup_repo_snapshots / backup_runs.
    pub snapshot_id: String,
    /// "success" | "failure".
    pub outcome: RestoreOutcome,
    /// Populated on failure (restore-job failure, deployment-never-ready,
    /// detected postgres version mismatch, etc.).
    pub error: Option<String>,
    /// Replica health after restore: did postgres come up and pass the
    /// operator's readiness gate.
    pub replica_healthy: bool,
    /// Detected postgres major version (status.postgresVersion).
    pub postgres_version: Option<String>,
    /// When the restore completed (status.restoredAt / activatedAt).
    pub observed_at: jiff::Timestamp,
}
```

`snapshot_id` is load-bearing: it is the join key canopy uses to close the
loop *backed up → persisted → restorable*. It maps directly to
`PostgresPhysicalRestoreStatus`-tracked snapshot and to the
`canopy@<server-id>:<path>` kopia source / `canopy-run` tag model canopy uses
for attribution.

### 2.2 When pgro reports

The restore controller (`src/controllers/restore.rs`) already drives a
restore through `Pending → Restoring → Ready → Switching → Active → Failed`
and counts `consecutiveRestoreFailures`. Hook the report at the terminal
transitions:
- **success**: when a restore reaches `Active` (or `Ready` and the deployment
  passes the readiness gate) — `outcome=success`, `replica_healthy=true`.
- **failure**: when a restore reaches `Failed`, or the deployment fails to
  become ready within `DEPLOYMENT_READY_TIMEOUT_SECS` — `outcome=failure`,
  `error` populated.

Report **at most once per restore** and tolerate canopy being unreachable:
record "reported" in the restore status (a new optional
`status.canopyReportedAt: Time`, mirroring `NotificationStatus`), retry on
the next reconcile if it hasn't been sent. Reporting failures must **never**
fail the restore itself (same posture as notifications today —
`NotificationStatus.last_error` is recorded, the restore proceeds).

This reuses the existing notification pattern almost exactly. Consider
modelling the canopy report as a built-in notification target rather than a
bespoke path, but keep it operator-configured (Part 3 auth), not per-replica
webhook config — the canopy relationship is first-party and trusted, not an
arbitrary webhook.

---

## Part 3 — First-party auth: a new canopy device role

pgro runs in a different k8s cluster from canopy. There is no shared
cluster, no shared ServiceAccount, no shared Secret. But canopy already
has the right shape for this. Two independent axes:

### 3.0 Identity: add a new canopy device role

Canopy's `securitySchemes` defines `server-device`, `releaser-device`, and
`admin-device` — three mTLS device roles, two of which are **not** bound
to a fleet server (`releaser-device` is used by the packaging pipelines,
`admin-device` by operators). Adding a **fourth role** for pgro (working
title `backup-restore-device` — role `backup-restore` — canopy's
naming) reuses that machinery instead of designing fresh federation.

- mTLS client cert, no server / group binding. Canopy adds it to the role
  enum and the `securitySchemes` block.
- The role gates the restore-credentials + restore-verification endpoints.
- **`purpose=backup` is rejected for this role at the API layer.** A
  `backup-restore` device that asks `/backup-credentials` with
  `purpose=backup` gets a `403`/`409`, not write-capable creds. The
  restore-only contract is enforced server-side; pgro doesn't have to ask
  nicely, and a compromised pgro can't pivot to writing/poisoning.
- The external-restore grant `(consumer, group, type)` is keyed by the
  device cert's identity, the same way device-bound endpoints key off
  `device → server → group_id`.
- pgro mounts the cert + key as a k8s Secret (operator pod + each Job's
  proxy sidecar). One identity for both directions (creds out, reports in)
  — matching the bestool model where `CanopyClient` carries `device_key`.
- Cert provisioning is one-time and operator-driven (no need for the
  TPM-bound `canopy register` device-enrolment flow bestool uses);
  matches how `releaser-device` certs are issued to packaging pipelines.
- The role is **generic, not pgro-specific**: any future restore-only
  consumer (a separate restore-test harness, an external auditor's
  read-only verifier, etc.) can use the same role with its own
  `(consumer, group, type)` grant.

This is the lowest lift on the identity side: canopy's role enum + cert
verification + `securitySchemes` plumbing already exists. The cross-cutting
canopy work is the new role itself and the external-restore grant; both
are pgro-blocking either way.

OIDC workload-identity-federation is a *different* identity model (bearer
token from a federated issuer rather than mTLS). It would be useful for
*other* consumers later (GitHub Actions → canopy and similar), but pgro
doesn't need that generality. Note it here, don't pursue it for pgro.

### 3.1 Transport: public-mTLS vs Tailscale

Canopy exposes its API on two transports, and the **authentication
mechanism differs by transport** — they don't layer:

- **Public-internet, mTLS.** Identity comes from the presented client
  certificate; the device-role lookup keys off the cert. This is how
  `server-device` / `releaser-device` reach canopy today.
- **Tailscale.** No client cert is presented; identity comes from the
  caller's **tailnet identity**, mapped server-side to a canopy device
  record. (Canopy's enrollment flow already accommodates this — see
  `BeginArgs.spki` in the openapi: required on the tailnet transport
  because there's no cert to read the key from, omitted on the mTLS
  path where the cert supplies it.)

The `backup-restore` device role still applies on either transport — it's
a property of the device record, not of the auth mechanism. The choice
determines what pgro mounts and how `CanopyClient` is constructed.

**`bestool-canopy::CanopyClient` auto-probes.** It tries the hardcoded
tailnet URL (`canopy.tail53aef.ts.net`, `crates/canopy/src/client.rs:38`)
first; if reachable, it uses the tailnet path with plain HTTPS (auth via
tailnet identity). If unreachable, it falls back to the mTLS endpoint
using the provided `device_key_pem`. So pgro doesn't pick at startup —
the client picks per-probe, and `refresh()` re-evaluates. pgro just needs
to (a) make the tailnet reachable from the pod (sidecar — see §3.1.1) or
(b) provide a device cert — or both, in which case Tailscale wins when
present and mTLS catches the gap when it isn't.

Recommend the **Tailscale** path for production since the operator +
Jobs are already long-lived in-cluster workloads that can sit on the
tailnet, it keeps canopy's public surface area smaller, and it sidesteps
managing a long-lived mTLS keypair as a k8s Secret. Public-mTLS is
viable as a fallback (provision a cert anyway and `CanopyClient` will
use it automatically when the tailnet is unreachable).

#### 3.1.1 Tailscale sidecar lives on the operator pod only

Canopy's tailnet auth identifies each caller by **tailscale node
identity** (`commons-servers/src/device_auth/tailnet.rs:52` — it resolves
the source IP via the tailnet directory and keys into
`devices.tailscale_node_id`, auto-creating an `Untrusted` device row on
first contact). Tags are only a coarse admission gate
(`TAILSCALE_REQUIRED_TAG`), not a role assignment.

So putting a Tailscale sidecar on every Job pod would mean **every Job
pod creates a new `Untrusted` canopy device row** that nobody promotes
to `backup-restore` — and the rows accumulate forever. That doesn't
work. The only workable shape is:

- **One Tailscale sidecar, on the operator Pod only.** Stable tailnet
  node identity, persisted with `TS_STATE_DIR` on a PVC (or via the
  Tailscale k8s operator's `kube:` state mode so a restarted operator
  resumes the same identity). Image
  `ghcr.io/tailscale/tailscale` in userspace mode (`TS_USERSPACE=true`)
  exposing `TS_SOCKS5_SERVER=:1055` on `localhost`. Authkey
  OAuth-issued by ops, the node tagged for ACL admission (the tag
  itself is just admission, not role).
- **The operator becomes a canopy device.** First contact creates an
  `Untrusted` row; an operator (human) promotes it once to
  `backup-restore`. One device record total.
- **kopia Job pods carry no Tailscale sidecar** and are not canopy
  devices.

#### 3.1.2 The credential broker: operator mediates for Jobs

`bestool-kopia::proxy::CredentialProvider` is a trait. bestool's
implementation
(`CanopyCredentialProvider`) calls canopy directly. pgro plugs a
**different provider** in the Job-side proxy sidecar — one that calls
the operator pod over the in-cluster network instead:

- The operator exposes a small in-cluster HTTP endpoint, e.g.
  `POST /internal/restore-creds` taking `{group, type}` and returning
  the same `BackupCredentials` shape `bestool_canopy` already
  deserializes. Backed by a per-(group, type) cache so concurrent Jobs
  don't N×-multiply canopy calls.
- The operator (running on the tailnet via its single sidecar) hits
  canopy's `POST /backup-credentials` with `purpose=restore`, returns
  the result. Canopy still enforces the role + purpose gate
  server-side; the operator-broker is purely a transport reuser.
- The Job's proxy sidecar implements `CredentialProvider` against this
  endpoint. Refresh cadence is the same (~2 min before expiry).
- Network gating: the broker endpoint is only exposed within the
  cluster (no Service.type=LoadBalancer), gated by a NetworkPolicy
  restricting access to pgro's own Job pods (matched by label) and the
  operator's namespace.

Restore reports (§2) also flow through the operator — same justification:
one canopy device, one identity.

Net effect: the Tailscale + canopy-device-role machinery is a property
of the **operator process**, not of each Job. The Jobs see only an
in-cluster HTTP endpoint that happens to broker canopy creds.

**ops-repo setup, one-time per cluster** (called out here for spec
completeness; actual implementation lives in the ops repo):
- Install the Tailscale k8s operator into pgro's cluster.
- Mint an OAuth client + ACL tag for pgro's operator pod (tag is just
  the ACL admission, not the canopy role).
- ACL allows the tag to reach `canopy.tail53aef.ts.net`.
- Canopy-side: after the operator's first contact creates its
  `Untrusted` device row, promote it to `backup-restore` (one-time
  manual step, same as `releaser-device` promotion today).

### 3.2 Operator config surface

Operator-level config, not per-CRD (open question 3.x: per-replica vs
operator-global canopy — recommend operator-global so every replica
trusts one canopy). Plumb via env on the Deployment (`operator.yaml`)
and/or the existing `postgres-restore-operator-config` ConfigMap that
`src/bin/operator.rs` already watches (`read_config`):
- `CANOPY_BASE_URL` — the public-mTLS endpoint, used by `CanopyClient`
  as the mTLS-fallback target. The tailnet hostname is hardcoded in
  `bestool-canopy`, not configurable here.
- `CANOPY_DEVICE_CERT_SECRET` — optional. Name of a k8s Secret with a
  pgro device cert + key, mounted only into the operator pod. Used by
  `CanopyClient` when the tailnet probe fails. Skip entirely if the
  operator is Tailscale-only.
- `PGRO_CREDENTIAL_BROKER_ADDR` — the cluster-internal listen address
  the operator exposes its `/internal/restore-creds` endpoint on. The
  Job builder propagates the matching Service URL into each kopia
  Job's proxy sidecar.

The Tailscale sidecar is wired only on the operator Deployment in
`operator.yaml` — annotation-driven if the Tailscale k8s operator's
`ProxyClass` is used, otherwise as an explicit second container. The
kopia Job spec has **no** Tailscale sidecar.

---

## IaC / deployment changes (`operator.yaml` + ops repo)

- **No more user-managed `kopia-credentials` Secret** for canopy-backed
  replicas. Operators stop hand-creating it; the static-Secret path remains
  only for legacy/non-canopy repos.
- **New pgro-published sidecar image** linking `bestool-kopia` (proxy) and
  `bestool-canopy` (HTTP client + credential provider). Built and pushed
  alongside the existing operator image; pinned by tag in `operator.yaml`
  via env (e.g. `CANOPY_PROXY_SIDECAR_IMAGE`). The Job builder injects this
  container into every kopia Job for canopy-backed replicas.
- **Tailscale sidecar on the operator pod only** — one stable tailnet
  node identity → one canopy device. Image
  `ghcr.io/tailscale/tailscale` in userspace mode with
  `TS_SOCKS5_SERVER=:1055`; auth via OAuth-issued authkey carrying the
  pgro ACL tag (admission, not role). Provisioned either by the
  Tailscale k8s operator's `ProxyClass` injection (preferred) or as an
  explicit second container in the operator Deployment.
- **kopia Job pods have NO Tailscale sidecar**. They reach the
  operator's in-cluster credential-broker endpoint
  (`/internal/restore-creds`) via a normal cluster Service. The
  operator forwards to canopy on their behalf.
- pgro's `reqwest` client (in the operator) is built with
  `Proxy::all("socks5://localhost:1055")` so `CanopyClient`'s
  auto-probe of `canopy.tail53aef.ts.net` succeeds via the sidecar.
- **Optional public-mTLS fallback**: a `CANOPY_DEVICE_CERT_SECRET`
  mounted into the operator pod only. `CanopyClient` uses it
  automatically when the tailnet is unreachable. Skip the cert entirely
  if pgro is comfortable being Tailscale-only.
- **NetworkPolicy gating** the credential-broker endpoint: only pgro's
  own Jobs (matched by label) in the operator's namespace can hit it.
- **RBAC delta in `operator.yaml`.** The new path stops writing transient
  per-Job Secrets (that was strategy (A) in the old framing — superseded by
  the sidecar). The ClusterRole's existing Secret verbs are still required
  for the legacy static-Secret path. No new cluster permissions for the
  canopy path itself (it's outbound HTTP from operator + sidecar).
- The auth plumbing lives in the **canopy + ops** repos, not pgro:
  canopy adds the `backup-restore` role, the external-restore grant,
  the purpose-gate, and the one-time promotion of pgro's operator-pod
  device record from `Untrusted` to `backup-restore`; ops installs the
  Tailscale k8s operator in pgro's cluster, mints the OAuth client +
  ACL tag for pgro's operator pod (admission only, not role), and
  writes the ACL allowing that tag to reach `canopy.tail53aef.ts.net`
  (canopy plan: "ops: the auth plumbing … and PGRO's read-only access path").

---

## Interfaces this component exposes / consumes

**Consumes (from canopy):**
- `POST /backup-credentials` with `(type, purpose=restore)` plus the
  external-restore grant context for `group` — returns `BackupCredentials`
  (`bestool_canopy::BackupCredentials`). Called from the sidecar's
  credential provider, not from the operator process.
- `GET /backup-target` — returns `BackupTarget` (`bestool_canopy::BackupTarget`).
  Called once per Job to populate kopia's `--bucket`/`--region`/`--prefix`
  and `--password`.
- A canopy device cert for the new non-server-bound role (Part 3).

**Consumes (from bestool, as published cargo crates):**
- `bestool-kopia` (≥0.3.2): the S3P proxy + `CredentialProvider` trait.
- `bestool-canopy` (≥0.4.0): the canopy wire types and the `CanopyClient`
  (constructed with pgro's device key — unchanged from the bestool usage).

**Provides (to canopy):**
- `RestoreReport` via a new canopy-side endpoint (working title `POST
  /restore-verification`) → canopy `backup_restore_checks` / signal-3
  detection. Cross-referenced by `snapshot_id`.
- The `PostgresPhysicalReplica.spec.canopyBackup.group` field is the operator-
  visible contract that binds a replica to a canopy group.

**Provides (to pgro operators):**
- CRD: `canopyBackup: { group }` as the alternative to `kopiaSecretRef`.
- A new pgro-published sidecar image (canopy S3P proxy + credential
  provider) used by every kopia Job for canopy-backed replicas.
- README CRD-table updates documenting it.

---

## Testing approach (per `AGENTS.md`)

`AGENTS.md`: add tests with features/fixes; integration tests **don't run
locally** — flag them to the user for CI and **add a matrix entry in
`.github/workflows/integration.yml`** for any new test file. Never write docs
except CRD-table updates. Run `cargo clippy` + `cargo fmt` before committing;
conventional commits; always work on a branch.

**Unit tests (run locally, alongside the code):**
- `src/canopy.rs`: serialize a `RestoreReport`; verify the auth-mode
  selector dispatches correctly. Wire-shape de/serialisation is covered by
  `bestool-canopy`'s own tests — pgro re-uses the types, doesn't re-test
  them.
- CRD validation: "exactly one of kopiaSecretRef/canopyBackup" — both-set
  and neither-set are errors; round-trip `CanopyBackupRef` through serde
  (mirroring `schema_migration_phase_roundtrips_*`).
- Job builder: with `canopyBackup`, the connect args carry the proxy
  endpoint, dummy keys, and `--disable-tls`; the Job spec has the proxy
  sidecar container; legacy path unchanged. Extend the existing
  `kopia_connect_args_*` tests for the canopy variant.
- Report gating: `canopyReportedAt` prevents double-reporting; a report
  failure does not fail the restore (assert status transitions unaffected).

**Integration tests (CI-only; new matrix entry required):**
- A `test-canopy-restore` namespace exercising the canopy path against a
  **stub canopy** (a small in-cluster HTTP service returning fixed
  creds for the minio repo + accepting reports), so the existing
  `tests/fixtures/minio.yaml` + `setup-kopia-repo.yaml` repo can be reused.
  Assert: replica reaches `Ready`/`Active` using fetched creds; a
  `RestoreReport` with the right `snapshot_id`/`outcome` is POSTed to the
  stub. Add the matrix entry in `.github/workflows/integration.yml` and tell
  the user it only runs in CI.
- Negative: grant-absent (stub returns 403) → replica surfaces a clear
  `Failed`/Warning, doesn't crash-loop.

---

## Open questions / decisions to make

1. **Credential lifetime vs. long restores — RESOLVED by the proxy model.**
   Chained AssumeRole sessions cap at 1 hour, but the S3P proxy refreshes
   creds out-of-band between requests, so kopia is oblivious to credential
   lifetime. A long restore is bounded by canopy reachability, not by any
   single issuance. Canopy may still want to offer non-chained direct
   web-identity creds to first-party consumers for efficiency / fewer
   refresh round-trips (and to mirror the maintenance-Job pattern), but it
   is a secondary optimisation, not a viability gate. Keep the request on
   canopy's list; don't block pgro on it.

2. **kopia S3 backend + session token — RESOLVED by the proxy model.**
   Under the S3P proxy, kopia talks only to `127.0.0.1` with dummy keys; it
   never sees a real session token. `--session-token` and
   `AWS_SESSION_TOKEN` are both irrelevant on the kopia leg. (For
   completeness: kopia *does* support `--session-token`, per the canopy
   kopia spike — but the production design doesn't use that path because
   the proxy is more general.)

3. **Per-replica vs operator-global canopy config.** Recommend
   operator-global (`CANOPY_BASE_URL` + auth on the Deployment/ConfigMap), so
   `CanopyBackupRef` carries only `group`. Revisit if pgro ever needs to talk
   to more than one canopy.

4. **Confirm the new-canopy-device-role + transport choice.** Identity is
   a new device role (`backup-restore`, generic and not pgro-specific)
   alongside `server`/`releaser`/`admin`, keyed to the external-restore
   grant, with `purpose=backup` rejected for the role at the API layer.
   The transport choice — canopy's public-mTLS surface or its Tailscale
   surface — picks the auth mechanism (mTLS cert vs tailnet identity), not
   a layering on top of mTLS. Recommend Tailscale for production. Confirm
   both with canopy before pgro builds the auth path. See Part 3.

5. **Report transport coupling.** Should the canopy report reuse the existing
   notification subsystem (a new built-in target) or be a dedicated path on
   the restore controller? Recommend a dedicated, operator-configured path —
   the canopy relationship is first-party/trusted and bidirectional, unlike
   arbitrary user webhooks — but the `NotificationStatus`/retry shape is the
   right model to copy.

6. **Snapshot-list / availability checks.** snapshot-list Jobs also need
   repo creds; they use the same proxy-sidecar pattern as restores. Confirm
   canopy is happy to serve `purpose=restore` creds for the periodic
   snapshot-list cadence (it is read-only; no objection expected), and that
   the read traffic volume is acceptable.

7. **Migration / coexistence.** During rollout a replica may switch from
   `kopiaSecretRef` to `canopyBackup`. Confirm a clean switchover (the next
   reconcile picks up the canopy path; the old Secret can be deleted by the
   operator once no replica references it) and that mixed fleets work.

8. **Region/endpoint for non-AWS repos.** The legacy path supports
   `endpoint` + `disableTls` (minio, S3-compatible). Canopy's
   `GET /backup-target` returns `{ storage, bucket, prefix, region,
   repo_password }` — no `endpoint`/`disable_tls` field. If any
   canopy-backed repo is ever non-AWS, the canopy wire type would need
   optional `endpoint`/`disable_tls`, and the bestool S3P proxy would need
   to be willing to point upstream at a non-`s3.<region>.amazonaws.com`
   host. Out of scope unless canopy supports non-AWS targets.

---

## Backup types addendum

Per the Canopy plan's "Backup types": PGRO consumes a **specific type**.

- PGRO restores the **`tamanu-postgres`** type (the same type bestool
  produces). The `PostgresPhysicalReplica` CRD references the canopy group
  **and type**.
- The external-restore grant is per `(consumer, group, type)`:
  "PGRO may read group X's `tamanu-postgres`, read-only".
- Signal-3 restore-verification is **per type** — PGRO reports which
  `(group, type)` snapshot it restored and the outcome; a failed/stale
  restorability check for that `(group, type)` is the group-level alert.
- PGRO filters the repo by the `canopy-type` tag to find its snapshots.

---

## Canopy implementation status (re-verified 2026-06-26)

Where canopy actually stands today, so this spec builds on what exists vs.
what canopy still owes. Re-checked against `canopy/crates/public-server/`
(openapi.json + src), `canopy/migrations/`, and the bestool repo on
2026-06-26: **none of the pgro-blocking surfaces have moved** since this
section was first written 2026-06-16. Canopy work in the intervening two
weeks has been operator-UI and S3-traffic tallies (PRs through #282;
migrations through `add_s3_traffic_to_backup_runs`).

**Available now (PR #224):** `POST /backup-credentials` with
`{ "type": "tamanu-postgres", "purpose": "restore" }` returns read-only
`credential_process` creds (the restore session policy: `GetObject` +
unconditioned `GetBucketLocation` + prefix-conditioned `ListBucket`), and
`GET /backup-target` returns `{ storage, bucket, prefix, region,
repo_password }`. So the *device-shaped* restore path exists — but it is
`ServerDevice` (mTLS) authenticated and the creds are chained (1-hour cap,
practically a non-issue under the proxy model — see §1.3).

**Available now in the bestool repo (published crates):** the S3P proxy
(`bestool-kopia` 0.3.2: `bestool_kopia::proxy::{spawn, CredentialProvider}`)
and the canopy HTTP client + wire types (`bestool-canopy` 0.4.0:
`CanopyClient`, `BackupCredentials`, `BackupTarget`, `BackupReport`,
`Purpose`). pgro depends on both as ordinary cargo deps; no git/vendoring.

**Still owed by canopy (not built — blocks PGRO):**
- **A new device role (`backup-restore` or equivalent).** Canopy already
  has `server-device`/`releaser-device`/`admin-device`; pgro's identity is
  a fourth, generic restore-only role keyed off the external-restore
  grant, with `purpose=backup` server-side rejected for the role. Smaller
  lift than net-new auth machinery, but still net-new role+cert plumbing
  that canopy must add (role enum, `securitySchemes` entry, route-gating,
  purpose-gating, cert-issuance flow). Until this lands, pgro cannot
  authenticate to canopy at all. **Biggest blocker.**
- **`CredentialsArgs` / external-restore wire change.** Today's
  `{ type, purpose }` body has no `group` field; the new role has no
  implicit group, so canopy needs either an additive field or a sibling
  endpoint that takes one. The `(consumer, group, type)` external-restore
  grant lookup hangs off this.
- **The external-restore grant** (`(consumer, group, type)` authz model,
  operator-authorized + audited) — no migration, no code.
- **Restore-verification ingest path.** A new endpoint (working title
  `POST /restore-verification`) writing to a new `backup_restore_checks`
  table, routing failures through `raise_group_event(ref =
  "restore-verification")`. See §2.0 for why reusing `/backup-report` is
  not viable.

**Group-level alert entrypoint is concrete (PR #225).** Signal-3 routes
through `database::backup::alerts::raise_group_event`, whose signature is:
`raise_group_event(conn, group_id, ref: &str, severity: Severity,
description: Option<&str>, message: &str, active: bool) -> Result<Issue>`.
Use `ref = "restore-verification"` (the constant in `database::backup::refs`),
severity `Error` (group-level, bypasses per-server `is_monitored`); recovery
is the same `(source="canopy", ref)` with `active: false`. Group-scoped issues
are first-class (Option B: `issues.server_group_id`), so no per-server shim is
needed.

---

## bestool-side asks

What needs to be added to the published bestool crates before pgro can
build against them. All four are additive (no breaking changes to
existing bestool consumers) and each is gated by the corresponding
canopy endpoint shipping first.

**In `bestool-canopy`:**

1. **`CanopyClient::restore_credentials(base, type, group)`** —
   group-aware variant of `backup_credentials`. The existing method
   infers group from `device → server → group_id`, which doesn't apply
   to a non-server-bound `backup-restore` device; the new method puts
   `group` in the request body to match the canopy wire change.
2. **`CanopyClient::restore_target(base, group)`** — same group issue.
3. **`CanopyClient::restore_verification(base, &RestoreVerification)`**
   — posts to canopy's new ingest endpoint (working title
   `POST /restore-verification`). See §2.0 for why this is a separate
   endpoint from `/backup-report`.
4. **`pub struct RestoreVerification`** in `bestool-canopy::backup` —
   the wire request type, mirroring whatever canopy lands.

**In `bestool-kopia`:** **no changes needed.** `proxy::spawn`, the
`CredentialProvider` trait, the `Credentials` struct, and `TrafficStats`
are already what pgro consumes. pgro implements its own
`CredentialProvider` against the operator's in-cluster broker (§3.1.2),
no changes to the proxy or the trait.

**Also unchanged:** `CanopyClient::new(... device_key_pem: Option<&str>,
...)` already accepts `None`, so pgro's tailscale-only operator (no
mTLS fallback cert) works without further changes. `Purpose::Restore`
already exists. No new crate — all four asks fit in `bestool-canopy`.

The `purpose=backup` rejection for the `backup-restore` role is purely
canopy-side enforcement; `bestool-canopy` doesn't know about role-based
purpose gating, it just sends what it's told.
