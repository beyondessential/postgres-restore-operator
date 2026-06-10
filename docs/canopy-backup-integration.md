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
> issues/events alerting, and a first-party (non-device) auth surface.
> Until those exist on the canopy side there is nothing for pgro to call.
> Build pgro's side behind the CRD opt-in below so today's static-Secret
> path keeps working unchanged during migration.

## What changes, in one paragraph

Today a `PostgresPhysicalReplica` carries a `kopiaSecretRef` pointing at a
hand-created `kopia-credentials` Secret that holds **long-lived** AWS access
keys plus the kopia repository password (`src/kopia.rs`,
`REQUIRED_KEYS`). The operator copies those values into env vars
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `KOPIA_PASSWORD`, …) on every
snapshot-list and restore Job (`src/controllers/replica/resources.rs`,
`src/controllers/restore/builders.rs`). This is exactly the long-lived-creds
pattern the canopy system exists to eliminate. The integration replaces that
static Secret with a **canopy-mediated** flow: the replica references a
**canopy group** instead of a Secret; the operator fetches **short-lived,
read-only restore credentials + target + repo password** from canopy,
refreshing them as needed; and after each restore the operator **reports the
outcome back to canopy** (signal 3, restore-verification). pgro is a
**restore-only** consumer — it never writes to or deletes from the bucket.

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

### 1.1 New canopy client module: `src/canopy.rs`

A new module owning all HTTP interaction with canopy. Follows the existing
`src/notifications.rs` / `reqwest`-based conventions (the crate already
depends on `reqwest` with `json`, and `jiff` with `serde`).

Types (wire shapes mirror canopy's device endpoints, which pgro reuses via
the external-restore grant — same response bodies, different auth):

```rust
/// Short-lived read-only credentials for a group's kopia repo, as returned
/// by canopy's restore-credentials endpoint. Mirrors the AWS SDK
/// `credential_process` output (canopy's `POST /backup-credentials` with
/// purpose=restore), plus the kopia repo password and S3 target that
/// canopy's `GET /backup-target` carries.
#[derive(Debug, Clone, Deserialize)]
pub struct CanopyRestoreCreds {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    /// RFC3339; the operator must refresh before this.
    pub expiration: jiff::Timestamp,
    pub bucket: String,
    pub prefix: String,      // normally empty (repo at bucket root)
    pub region: String,
    pub repository_password: String,
}
```

Functions:

- `async fn fetch_restore_creds(&self, group: &str) -> Result<CanopyRestoreCreds>`
  — authenticates as pgro (see Part 3), requests `purpose=restore` for
  `group`, returns the combined creds+target+password. Canopy maps the group
  to its per-bucket role, assumes it cross-account with the read-only restore
  **session policy**, and returns prod-account creds for that bucket. pgro
  treats a `403`/`409` (grant absent or group unconfigured) as a clear,
  surfaced error, not a transient retry.
- `async fn report_restore(&self, report: &RestoreReport) -> Result<()>`
  — see Part 2.

The client holds the canopy base URL + auth material (Tailscale or OIDC; Part
3) read from env/config at startup, on the `Context` (`src/context.rs`)
alongside the existing `http_client`.

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

### 1.3 Where the creds are consumed — the materialisation strategy

Jobs consume creds today as **env vars sourced from the Secret**
(`env_from_secret(... kopia_secret ...)`), then run
`kopia repository connect s3 --access-key=... --secret-access-key=...
--password=...`. The scripts read `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
/ `KOPIA_PASSWORD` from the environment.

Short-lived creds add three things the static path didn't need: a
**session token**, an **expiry**, and a **refresh** obligation. Two viable
strategies; **this spec recommends (A)** for snapshot-list and short restores,
with (B) as the escape hatch for long restores (see the 1-hour-cap open
question, 3.x):

**(A) Operator materialises a transient per-Job Secret.** Just before
building a Job, the operator calls `fetch_restore_creds(group)` and writes a
short-lived k8s Secret (owned by the replica, so it's GC'd) containing the
same keys the Job already expects, plus `sessionToken`. The Job's connect
script gains `AWS_SESSION_TOKEN` and `--session-token` (kopia/AWS SDK pick up
`AWS_SESSION_TOKEN` automatically for the SDK path; for the kopia S3 backend,
pass the session token via the `AWS_SESSION_TOKEN` env which kopia forwards —
**verify**, see open questions). This keeps the Job-builder code path almost
identical to today: `env_from_secret(...)` against a per-Job Secret name
instead of the user-provided one. The `validate_kopia_secret` /
`KopiaCredentials` plumbing in `src/kopia.rs` is reused verbatim with a
`sessionToken`/`AWS_SESSION_TOKEN` field added.

**(B) Job self-refreshes via a sidecar/credential helper.** The Job itself
calls canopy (it's the AWS `credential_process` model bestool uses on the
device side). Heavier — the Job needs the canopy auth material and network
path — but it is the only thing that survives a restore that runs **longer
than the credential lifetime** (chained AssumeRole sessions are capped at
**1 hour** regardless of role config; see the canopy plan "AWS quirks").

Restore Jobs can legitimately exceed an hour (large data dir, slow WAL
replay — the operator already has a configurable
`DEPLOYMENT_READY_TIMEOUT_SECS` defaulting to 30 min and raisable). The
kopia *restore* phase reads from S3 throughout; if its creds expire
mid-restore the restore fails. **This is the single most important design
question for pgro** and is called out in open questions 3.x. snapshot-list is
short and safely fits (A).

### 1.4 Drop the static keys

Once a replica uses `canopyBackup`:
- No `accessKeyId` / `secretAccessKey` / `repositoryPassword` ever live in a
  user-managed Secret. The transient per-Job Secret (strategy A) holds creds
  that expire within the hour and is owned/GC'd by the operator.
- `REQUIRED_KEYS` in `src/kopia.rs` still governs the legacy static path;
  the canopy path builds `KopiaCredentials` from the canopy response instead
  of `validate_kopia_secret`.

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

## Part 3 — Cross-cluster first-party auth (DESIGN LATER)

**pgro runs in a different k8s cluster from canopy.** There is no shared
cluster, no shared ServiceAccount, and no shared Secret to lean on. canopy's
device endpoints use mTLS device identity; pgro is not a device. So pgro
needs a **first-party, non-device** auth path to canopy. Two options to weigh
(the canopy plan defers the decision; pgro should be built so the auth
mechanism is swappable — isolate it behind one trait in `src/canopy.rs`):

**Option 1 — Tailscale (available today, least new machinery).** Both pgro
and canopy are on the tailnet. pgro reaches canopy's **private** API over
Tailscale, reusing existing Tailscale identity/gating (canopy already gates
its admin/private surface on Tailscale). Lowest lift: no new federation
infrastructure; the auth "material" is just being on the tailnet, and canopy
authorises by tailnet identity. The external-restore grant keys off that
identity. This is the likely **stage-1** choice.

**Option 2 — OIDC trust / workload-identity-federation (more general,
later).** Canopy becomes an **OIDC relying party** federating pgro's cluster
OIDC issuer (pgro's ServiceAccount projected token). More extensible: the
*same* mechanism generalises to **GitHub Actions** and other internal
automation authenticating to canopy — a first-party automation auth surface
beyond pgro. Likely the better long-term direction. Not designed here; pgro's
side would mount a projected SA token and present it as a bearer assertion,
which the swappable auth trait can accommodate later without touching the
creds/report logic.

Either way, **one channel carries both directions** (creds out, reports in).
pgro should not build two transports.

### 3.1 Operator config surface

Canopy base URL + auth selector come from operator-level config, not the CRD
(open question 3.x: per-replica vs operator-global canopy — recommend
operator-global so every replica trusts one canopy). Plumb via env on the
Deployment (`operator.yaml`) and/or the existing
`postgres-restore-operator-config` ConfigMap that `src/bin/operator.rs`
already watches (`read_config`):
- `CANOPY_BASE_URL`
- `CANOPY_AUTH_MODE` = `tailscale` | `oidc`
- (oidc) projected-token path / audience; (tailscale) nothing beyond tailnet
  membership.

---

## IaC / deployment changes (`operator.yaml` + ops repo)

- **No more user-managed `kopia-credentials` Secret** for canopy-backed
  replicas. Operators stop hand-creating it; the static-Secret path remains
  only for legacy/non-canopy repos.
- **Network path to canopy.** Tailscale: ensure the pgro pod can reach
  canopy's private endpoint over the tailnet (sidecar or host tailnet, per
  the cluster's existing Tailscale setup — ops repo). OIDC: expose pgro's
  cluster OIDC issuer to canopy and project a SA token into the operator pod
  (`operator.yaml` Deployment `volumes` + `serviceAccountToken` projection).
- **RBAC delta in `operator.yaml`.** Strategy (A) writes transient per-Job
  Secrets — the ClusterRole already grants full Secret verbs
  (`get/list/watch/create/update/patch/delete`), so no RBAC change is needed
  for that. No new cluster permissions for the canopy path itself (it's
  outbound HTTP).
- The auth-plumbing (OIDC trust config or Tailscale exposure) and the
  external-restore grant live in the **canopy + ops** repos, not pgro
  (canopy plan: "ops: the auth plumbing … and PGRO's read-only access path").

---

## Interfaces this component exposes / consumes

**Consumes (from canopy):**
- `fetch_restore_creds(group)` → `CanopyRestoreCreds` (read-only restore
  creds + S3 target + repo password). Canopy-side: restore session policy on
  the per-bucket role; gated by the external-restore grant for pgro's
  first-party identity.
- First-party auth surface (Tailscale identity or OIDC trust).

**Provides (to canopy):**
- `RestoreReport` via `report_restore(...)` → canopy `backup_restore_checks`
  / signal-3 detection. Cross-referenced by `snapshot_id`.
- The `PostgresPhysicalReplica.spec.canopyBackup.group` field is the operator-
  visible contract that binds a replica to a canopy group.

**Provides (to pgro operators):**
- CRD: `canopyBackup: { group }` as the alternative to `kopiaSecretRef`.
- README CRD-table updates documenting it.

---

## Testing approach (per `AGENTS.md`)

`AGENTS.md`: add tests with features/fixes; integration tests **don't run
locally** — flag them to the user for CI and **add a matrix entry in
`.github/workflows/integration.yml`** for any new test file. Never write docs
except CRD-table updates. Run `cargo clippy` + `cargo fmt` before committing;
conventional commits; always work on a branch.

**Unit tests (run locally, alongside the code):**
- `src/canopy.rs`: deserialize a canopy creds response into
  `CanopyRestoreCreds`; serialize a `RestoreReport`; expiry parsing; the
  "should-refresh" decision (creds near expiry). Mirror the table-driven
  style in `src/kopia.rs` / `src/types/replica.rs` tests.
- CRD validation: "exactly one of kopiaSecretRef/canopyBackup" — both-set and
  neither-set are errors; round-trip `CanopyBackupRef` through serde
  (mirroring `schema_migration_phase_roundtrips_*`).
- Job builder: with `canopyBackup`, the connect script/env carry
  `AWS_SESSION_TOKEN` and reference the transient Secret name; legacy path
  unchanged. Extend the existing `kopia_connect_args_*` tests for the
  session-token arg.
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

1. **Credential lifetime vs. long restores (the critical one).** Chained
   AssumeRole sessions are capped at **1 hour**. Large restores (slow WAL
   replay) can exceed that. The kopia restore reads from S3 for the whole
   run, so expired creds mid-restore fail it. Options:
   (a) Job self-refreshes (strategy B) — Job calls canopy as a
   `credential_process`-style helper; needs canopy network path + auth inside
   the Job. (b) Operator watches expiry and rotates the transient Secret +
   the kopia env mid-Job — fragile, kopia would need to re-read creds.
   (c) canopy issues longer-lived creds to first-party consumers via a
   *non-chained* direct web-identity assume (the canopy plan already uses
   direct, non-chained cross-account web-identity for **maintenance Jobs**,
   sidestepping the 1-hour cap — decision 13). **Recommend pushing canopy to
   make (c) available to the pgro external-restore grant**, since pgro is
   first-party like the maintenance Jobs. Decide before building the Job
   credential path.

2. **kopia S3 backend + session token.** Confirm kopia's S3 backend honours
   `AWS_SESSION_TOKEN` (temporary creds) — the canopy plan assumes the AWS
   SDK `credential_process` path on devices, but pgro drives kopia via CLI
   flags (`--access-key` / `--secret-access-key`). There is no
   `--session-token` flag in the current `kopia_connect_args`; verify whether
   kopia reads `AWS_SESSION_TOKEN` from the env (likely yes, via the AWS SDK
   it embeds) or whether a different invocation is needed. This gates
   strategy (A).

3. **Per-replica vs operator-global canopy config.** Recommend
   operator-global (`CANOPY_BASE_URL` + auth on the Deployment/ConfigMap), so
   `CanopyBackupRef` carries only `group`. Revisit if pgro ever needs to talk
   to more than one canopy.

4. **Tailscale vs OIDC for stage 1.** Tailscale is available now and is the
   lowest-lift stage-1 path; OIDC is the more general long-term direction
   (also unlocks GitHub Actions → canopy). Isolate behind one auth trait so
   the choice is swappable. Decision owned jointly with canopy + ops.

5. **Report transport coupling.** Should the canopy report reuse the existing
   notification subsystem (a new built-in target) or be a dedicated path on
   the restore controller? Recommend a dedicated, operator-configured path —
   the canopy relationship is first-party/trusted and bidirectional, unlike
   arbitrary user webhooks — but the `NotificationStatus`/retry shape is the
   right model to copy.

6. **Snapshot-list / availability checks.** snapshot-list Jobs also need repo
   creds. They're short and fit strategy (A) cleanly. Confirm canopy is happy
   to serve `purpose=restore` creds for the periodic snapshot-list cadence
   (it is read-only; no objection expected), and that the read traffic volume
   is acceptable.

7. **Migration / coexistence.** During rollout a replica may switch from
   `kopiaSecretRef` to `canopyBackup`. Confirm a clean switchover (the next
   reconcile picks up the canopy path; the old Secret can be deleted by the
   operator once no replica references it) and that mixed fleets work.

8. **Region/endpoint for non-AWS repos.** The legacy path supports
   `endpoint` + `disableTls` (minio, S3-compatible). Canopy's `backup-target`
   serves `bucket/prefix/region` for real S3; if any canopy-backed repo is
   ever non-AWS, `CanopyRestoreCreds` would need optional `endpoint`/
   `disableTls` too. Out of scope unless canopy supports non-AWS targets.

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
