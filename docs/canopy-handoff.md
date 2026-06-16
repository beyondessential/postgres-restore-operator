# Handoff to canopy: PGRO restore-verification integration

**From:** pgro
**To:** canopy (then canopy → bestool for the appendix)
**Status:** waiting on canopy. pgro will not start building until the
items in §4 land (or are contract-frozen) and the bestool additions in
§A ship in a published crate.

This document is the actionable subset of pgro's full integration spec
(`pgro/docs/canopy-backup-integration.md`). Read that for the
why-it-looks-like-this; read this for what to build. Anything contentious
here gets bounced back to pgro before implementation.

---

## 1. Context, brief

pgro restores tamanu-postgres physical backups out of kopia repos into
working postgres replicas. Today it authenticates with hand-set,
long-lived AWS keys + repo password in a k8s Secret — the exact
long-lived-creds pattern the canopy backup-credentials system exists to
eliminate. Bringing pgro under canopy gets two things:

1. **Eliminates the static keys** on the pgro side. canopy mediates
   restore creds the same way it mediates device backup creds.
2. **Closes the lifecycle loop end-to-end.** A successful pgro restore
   *proves the snapshot is restorable* — signal 3, the strongest
   backup-health signal there is, stronger than signal 2's
   "a snapshot exists in the repo". pgro reports per-replica restore
   outcomes back to canopy; a failed/stale restorability check becomes
   a high-severity group-level alert.

This is the integration the canopy backup-credentials plan calls out in
§"External restore consumers + restore-verification (PGRO)" — pgro is
ready to build its side once canopy's side exists.

---

## 2. Architecture pgro is building toward

Read this so the wire-shape and identity choices below make sense in
context.

- **One stable pgro operator Pod** sits on the tailnet and is the
  single canopy device. It speaks to canopy directly.
- **Each kopia restore is a k8s Job** spawned by the operator. Each Job
  Pod runs two containers: kopia, and a pgro-published proxy sidecar.
- **The proxy sidecar runs the bestool S3P loopback re-signing proxy**
  (`bestool_kopia::proxy::spawn` from the published `bestool-kopia`
  crate). kopia is pointed at `127.0.0.1` with dummy keys; the proxy
  holds the live STS creds and re-signs each request. Same model as
  bestool device backups and canopy's own maintenance jobs.
- **The proxy's `CredentialProvider`** doesn't call canopy directly.
  It calls an in-cluster HTTP endpoint on the operator
  (`/internal/restore-creds`), and the operator forwards to canopy.
  This is forced by the identity model (§3) — Job Pods are not canopy
  devices and have no way to authenticate.
- **`bestool-canopy::CanopyClient` auto-probes tailnet vs mTLS.** pgro
  uses the tailnet path (via the Tailscale sidecar on the operator
  Pod); mTLS is an optional fallback if a device cert is provisioned.

Consequences worth flagging up front:

- **The chained-STS 1-hour cap is a non-issue.** The proxy refreshes
  creds between requests; long restores are bounded by canopy
  reachability, not by any single issuance lifetime. pgro does not need
  non-chained / direct-IRSA creds.
- **kopia never sees real AWS credentials.** It carries dummy keys and
  talks to `127.0.0.1`. The `--session-token` / `AWS_SESSION_TOKEN`
  question is moot.

---

## 3. Identity model: one operator-Pod tailnet device

canopy's tailnet auth identifies callers by **tailscale node identity**
(`commons-servers/src/device_auth/tailnet.rs:52` — looks up the source
IP via the tailnet directory, keys into `devices.tailscale_node_id`,
auto-creates an `Untrusted` device row on first contact). Tags are only
a coarse admission gate (`TAILSCALE_REQUIRED_TAG`).

That means **one tailnet node = one canopy device record**. Per-Job
Tailscale sidecars would create one `Untrusted` row per Job pod,
forever — unworkable.

So pgro will run **exactly one Tailscale sidecar**, on the operator
Pod, and pgro is **one canopy device**:

- First contact creates an `Untrusted` row.
- canopy (admin) promotes it once to role `backup-restore` (working
  name — see §4.1).
- The operator brokers everything for Job Pods over the in-cluster
  network, so Job Pods never need their own canopy identity.

The mTLS path is symmetric — one operator-Pod-mounted device cert,
one canopy device, same identity. Either path works; canopy's auth
mechanism is the only thing that differs.

---

## 4. What canopy needs to build

Five items. They depend on each other roughly in the order listed.

### 4.1 New device role: `backup-restore`

Add a fourth role alongside `server` / `releaser` / `admin`.

- Generic, not pgro-specific. Any future restore-only consumer (an
  external auditor's verifier, a separate test-restore harness) shares
  the same role with its own external-restore grant.
- No server / group binding. Like `releaser-device`, the role itself
  doesn't imply membership in any group.
- Add to the device-role enum, `securitySchemes` in
  `crates/public-server/openapi.json`, route-gating macros, and the
  cert-issuance flow (one-off operator-driven cert minting — does not
  need the TPM-bound `canopy register` enrolment flow that bestool
  servers use; the `releaser-device` provisioning path is the right
  model).
- **`purpose=backup` must be rejected at the API layer for this role.**
  A `backup-restore`-role caller hitting `/backup-credentials` with
  `purpose=backup` gets `403`/`409`, full stop. The role's read-only
  contract is server-enforced, not consumer-promised, and a compromised
  pgro can't pivot to writing/poisoning.

This is the biggest single blocker. Until this lands pgro cannot
authenticate at all.

### 4.2 Group-aware credentials + target endpoints

For server-bound roles, `device → server → group_id` resolves the group
implicitly. A `backup-restore`-role device has no implicit server and
no implicit group, so the request body has to carry `group`.

Two viable shapes; canopy picks:

- **(a) Add `group: Uuid` to the existing `CredentialsArgs` /
  `BackupTarget` paths** and accept it only from `backup-restore`-role
  callers. Smaller diff; mildly violates the principle that
  device-authenticated requests don't put authz fields in the body.
- **(b) Sibling endpoints**: e.g. `POST /restore-credentials` and
  `GET /restore-target?group=...`. Clean separation; bestool-canopy
  gets two new methods rather than overloaded ones (matches the
  appendix bestool deltas).

pgro lightly prefers (b) for clarity, but defers to canopy.

Behaviour either way: canopy verifies the `(consumer, group, type)`
external-restore grant (§4.3), then runs the same restore session
policy + per-bucket role + repo-password lookup it does today, and
returns `BackupCredentials` + `BackupTarget` unchanged.

### 4.3 The external-restore grant

The operator-authorised, audited authz primitive that says "consumer C
may read group G's type T, read-only."

- Per `(consumer_device_id, group_id, type)`. New table; canopy picks
  the name (`backup_restore_grants` or similar).
- Operator-authorised via the existing private-server UI or `canopy
  ctl` CLI; audited.
- Checked at request time for `/restore-credentials` (4.2) and
  `/restore-verification` (4.4). Absence is a clear 403, not a
  transient error.
- pgro will surface that 403 as a clear `Failed` phase + Warning event
  on the replica; the operator who set up the replica diagnoses by
  going to canopy and inspecting / creating the grant.

### 4.4 Restore-verification ingest endpoint + `backup_restore_checks`

#### 4.4.1 Why NOT reuse `POST /backup-report`

`/backup-report` already accepts `{ purpose: "restore", outcome,
snapshot_id, error, run_id }` — for **devices**. The shape looks close
to what pgro wants, but reusing it is wrong for seven concrete reasons:

1. **Identity is auth-context-derived, not body-derived.** The handler
   resolves `device_id`, `server_id`, and `group_id` from the
   authenticated mTLS context (`crates/public-server/src/backup.rs:495`),
   not the body. A `backup-restore`-role caller has no implicit server
   or group; threading them through the body would break the invariant
   that a device can't report a run as some *other* group.
2. **Schema is device-shaped.** `backup_runs` has `device_id UUID NOT
   NULL REFERENCES devices(id)` and `group_id NOT NULL REFERENCES
   server_groups(id)`. The pgro device row exists but it's not
   "running" a backup for any server; satisfying the FKs requires
   either sentinel data or schema changes.
3. **Two different "restore" meanings collide on `purpose`.** A device
   with `purpose=restore` (e.g. `bestool canopy restore` for clone /
   DR-test on the same fleet) writes to `backup_runs`. That is NOT a
   signal-3 verification — it's a normal device-side restore and
   should not raise a group-level "the backup isn't restorable"
   incident. `purpose=restore` alone is not a sufficient discriminator
   between device-restore-runs and signal-3 verifications.
4. **Alerting paths diverge.** `/backup-report` failure feeds per-server
   staleness (signal 1, server-scoped). Signal 3 must feed group-scoped
   `raise_group_event(ref = "restore-verification")` bypassing
   per-server `is_monitored`.
5. **Side-effects don't match.** The handler clears `BackupRequest`
   (`backup.rs:534`) so the heartbeat stops re-emitting "back up now"
   for that server. Irrelevant for a pgro report.
6. **Payload shape is wrong.** `ReportArgs` carries `bytes_uploaded` +
   `s3_*_bytes` (good — pgro's proxy emits those too) but lacks
   `replica_healthy`, postgres major version, `observed_at` — the
   load-bearing fields that make signal 3 stronger than signal 2.
7. **`run_id` semantics don't transfer.** For devices, `run_id` is the
   same UUID across `/backup-credentials` (issuance audit) and
   `/backup-report`, minted at run start, dup → 409. pgro's natural
   identity is the snapshot being verified, not a per-run UUID; a
   pgro-minted run UUID has no cross-table linkage to
   `backup_credential_issuances`.

By the time `/backup-report` has been extended to take `group_id`,
relaxed (or split off) the FKs, branched the handler on actor type,
routed failures differently, and gated the `BackupRequest::clear`
side-effect, the handler has forked. Cleaner to expose a sibling.

#### 4.4.2 New endpoint

Working title `POST /restore-verification` (canopy picks the name).
Authenticated as `backup-restore`-role; gated by the external-restore
grant for the body's `(group, type)`.

Request body (proposed):

```json
{
  "group": "<uuid>",
  "type": "tamanu-postgres",
  "snapshot_id": "<kopia snapshot id>",
  "outcome": "success" | "failure",
  "error": "<string, only on failure>",
  "replica_healthy": true,
  "postgres_version": "<major, e.g. \"15\">",
  "observed_at": "<RFC3339>",
  "s3_sent_raw_bytes": 12345,
  "s3_sent_payload_bytes": 12300,
  "s3_received_raw_bytes": 98765,
  "s3_received_payload_bytes": 98700
}
```

- `snapshot_id` is the join key into `backup_repo_snapshots` /
  `backup_runs`. Load-bearing for closing the loop *backed up →
  persisted → restorable*.
- `outcome=success` with `replica_healthy=true` means kopia restored
  successfully AND postgres came up AND the operator's readiness gate
  passed.
- `outcome=failure` with an `error` string covers restore-job failure,
  deployment-never-ready, postgres-version mismatch, etc.
- S3 byte tallies come from the bestool proxy's `TrafficStats` (already
  there in `bestool-kopia`). pgro emits them on success and failure,
  same as `/backup-report`.

#### 4.4.3 New table: `backup_restore_checks`

Roughly:

```sql
CREATE TABLE backup_restore_checks (
    id              BIGSERIAL PRIMARY KEY,
    consumer_device_id UUID NOT NULL REFERENCES devices(id),
    group_id        UUID NOT NULL REFERENCES server_groups(id),
    type            TEXT NOT NULL,
    snapshot_id     TEXT NOT NULL,
    outcome         TEXT NOT NULL CHECK (outcome IN ('success','failure')),
    error           TEXT,
    replica_healthy BOOLEAN NOT NULL,
    postgres_version TEXT,
    observed_at     TIMESTAMPTZ NOT NULL,
    s3_sent_raw_bytes      BIGINT,
    s3_sent_payload_bytes  BIGINT,
    s3_received_raw_bytes  BIGINT,
    s3_received_payload_bytes BIGINT,
    reported_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON backup_restore_checks (group_id, type, observed_at DESC);
CREATE INDEX ON backup_restore_checks (snapshot_id);
```

Exact shape is canopy's call. pgro just needs the endpoint to accept
the body in §4.4.2 and reject 4xx clearly on grant/role failure.

#### 4.4.4 Alert routing

Plumb `outcome=failure` (and staleness — see §6 / "Open questions")
into:

```rust
raise_group_event(
    conn, group_id,
    ref: "restore-verification",     // const in database::backup::refs
    severity: Severity::Error,        // group-level; bypasses per-server is_monitored
    description: ...,
    message: ...,
    active: true,
);
```

Already concrete in PR #225, no new plumbing on the alerting side —
just call it from the new handler. Recovery (`active: false`) on the
next successful report for the same `(group, type)`.

### 4.5 Wire-type stability

For pgro's side: please freeze the wire shapes for §4.2 and §4.4.2
before merging the bestool changes (Appendix A). Mid-flight name churn
on `BackupCredentials` / `BackupTarget` fields would also cause
collateral damage — pgro is going to consume `bestool_canopy`'s
existing types verbatim, so renames there propagate.

---

## 5. What pgro is NOT asking for

These have come up in earlier rounds and pgro has explicitly **decided
against** them:

- **Non-chained / longer-lived STS creds for pgro.** The proxy refreshes
  out-of-band; the 1-hour chained cap is fine in practice. Don't burn
  effort here on pgro's account. (canopy may still want it for its own
  reasons — that's a canopy call.)
- **Reusing `/backup-report` for signal 3.** §4.4.1 covers why.
- **Server-side cred caching across pgro Jobs.** pgro's operator
  already caches in-process for the broker (§Architecture); canopy
  doesn't need to.
- **A new auth federation (OIDC).** pgro is happy with mTLS + tailnet.
  OIDC would be useful for *other* future first-party consumers and
  canopy can pursue it independently, but pgro doesn't need it.

---

## 6. Open questions canopy owns

Pick before / during implementation; flag back to pgro if any of these
change pgro-visible shape.

1. **4.2 (a) vs (b):** group in body of existing endpoints, or sibling
   `/restore-*` endpoints. pgro mildly prefers (b).
2. **Naming.** Role: `backup-restore` (pgro suggestion) vs whatever
   canopy prefers. Endpoint: `/restore-verification` vs
   `/backup-restore-check` vs… Table name: `backup_restore_checks` vs
   `restore_verifications`. pgro doesn't care, just needs them stable
   before bestool ships.
3. **Staleness detection for signal 3.** A successful report is
   straightforward. "Stale" (no recent successful verification for a
   `(group, type)`) is a periodic check canopy needs to run — out of
   pgro's scope, but in scope for the alerting story. Define the
   cadence + threshold canopy-side.
4. **`backup_restore_checks` retention.** pgro suggests indefinite
   (audit trail, small rows); canopy decides.
5. **Per-Pod identity for audit.** pgro is intentionally one canopy
   device; per-Job audit lives in pgro's own k8s record (CRD status,
   events). If canopy wants to split per-Pod, pgro can include a
   `consumer_instance` opaque string in the body — but the cost is
   real and the value is unclear. Default: don't.
6. **Cert-issuance flow for the new role.** pgro will be tailscale-only
   in normal operation; mTLS cert is the fallback. If canopy doesn't
   want to build cert minting for the new role at all (tailscale-only,
   period), pgro is fine with that — just confirm.

---

## 7. Pgro-side commitments (so canopy knows what to expect)

- pgro will be one canopy device. First contact creates `Untrusted`;
  canopy admin promotes once.
- pgro will report `outcome=success` only when the deployment actually
  passes the readiness gate (not on bare-kopia-success). Failure
  reporting is best-effort and never blocks restore progression.
- pgro will at-most-once-per-restore, with retry across reconciles
  until the report lands (status-tracked).
- pgro will not write or delete from any bucket. The proxy is fed by
  the restore session policy; even if pgro is compromised it has no
  write capability (compounded by §4.1's role-level `purpose=backup`
  rejection).

---

## Appendix A — Hand off to bestool

Once §4 has landed (or shipped to a feature branch with frozen wire
shapes), canopy passes this list to bestool. All four are additive in
the published `bestool-canopy` crate; no breaking changes to existing
consumers, no new crate. `bestool-kopia` needs no changes.

### A.1 `bestool_canopy::backup::RestoreVerification` (new)

Public wire type mirroring §4.4.2:

```rust
#[derive(Debug, Clone, Serialize)]
pub struct RestoreVerification<'a> {
    pub group: Uuid,
    pub r#type: &'a str,
    pub snapshot_id: &'a str,
    pub outcome: RunOutcome,                // reuse existing enum
    pub error: Option<&'a str>,
    pub replica_healthy: bool,
    pub postgres_version: Option<&'a str>,
    pub observed_at: jiff::Timestamp,
    pub s3_sent_raw_bytes: Option<i64>,
    pub s3_sent_payload_bytes: Option<i64>,
    pub s3_received_raw_bytes: Option<i64>,
    pub s3_received_payload_bytes: Option<i64>,
}
```

Field-renaming-via-serde to whatever canopy lands; the Rust shape is
indicative.

### A.2 `CanopyClient::restore_credentials(base, type, group) -> Result<BackupCredentials>`

Group-aware variant of `backup_credentials`. Posts to whichever
endpoint canopy picks in §4.2 (a) or (b); the response type is the
existing `BackupCredentials` unchanged.

```rust
pub async fn restore_credentials(
    &self,
    base_url: &Url,
    backup_type: &str,
    group: Uuid,
) -> Result<BackupCredentials> { ... }
```

### A.3 `CanopyClient::restore_target(base, group) -> Result<TargetOutcome>`

Same group issue for target lookup. Response is the existing
`TargetOutcome` (Ready/Dormant) — Dormant maps to grant-absent or
group-unconfigured.

```rust
pub async fn restore_target(
    &self,
    base_url: &Url,
    group: Uuid,
) -> Result<TargetOutcome> { ... }
```

### A.4 `CanopyClient::restore_verification(base, &RestoreVerification) -> Result<()>`

Posts to canopy's new ingest endpoint. 204 on success; surface
4xx body as error.

```rust
pub async fn restore_verification(
    &self,
    base_url: &Url,
    report: &RestoreVerification<'_>,
) -> Result<()> { ... }
```

### A.5 What does NOT change in bestool

- `bestool-kopia` — no changes. `proxy::spawn`, `CredentialProvider`,
  `Credentials`, `TrafficStats` are exactly what pgro consumes.
- `CanopyClient::new(...)` — already accepts `device_key_pem:
  Option<&str>`, so pgro's tailscale-only operator works as-is.
- `Purpose::Restore` — already there.
- `BackupCredentials` / `BackupTarget` shapes — pgro consumes these
  verbatim; please don't reshape them mid-flight (see §4.5).

### A.6 Suggested release shape

One bestool-canopy minor version bump containing all four additions,
landing after canopy's endpoints exist on at least a feature branch
with frozen wire shapes. Tag and publish; pgro depends on `^X.Y`.

---

## Next round

Once §4 + Appendix A have shipped, ping pgro. pgro will:

1. Read the as-implemented wire + types (any drift from this doc is
   fine, just needs to be visible).
2. Re-evaluate the open questions in
   `pgro/docs/canopy-backup-integration.md` and tighten the spec to
   match what canopy actually shipped.
3. Start building Part 1 (canopy client wiring + CRD field + sidecar
   image) and Part 2 (the restore-verification reporter) against the
   real surfaces.
