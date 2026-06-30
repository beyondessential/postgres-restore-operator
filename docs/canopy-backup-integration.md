# Canopy backup-credentials integration

pgro's side of the Canopy "restore-replicas" system. Canopy is the source
of truth for which postgres replicas should exist; pgro reconciles
cluster state against canopy's declaration set and reports per-replica
restore-verification (signal 3) back. This document is pgro's
implementation contract, grounded against:

- canopy `crates/public-server/src/restore.rs` on `origin/main` (the
  shipped endpoints: `restore-capabilities`, `restore-worklist`,
  `restore-credentials`, `restore-verification`).
- `bestool-canopy` 0.4.2 (the published client, our cargo dep).
- `bestool-kopia` 0.3.3 (the S3P loopback re-signing proxy, our cargo
  dep — unchanged for this integration).

History note: this spec was rewritten 2026-06-30 after the design
inverted (canopy became the desired-state owner) and after canopy +
bestool both shipped their sides. The earlier handoff (`canopy-handoff.md`)
and pgro's sign-off (`canopy-handoff-response.md`) record how we got here;
this doc supersedes both for *implementation* purposes.

---

## What changes, in one paragraph

Today a `PostgresPhysicalReplica` CR with a `kopiaSecretRef` is the
operator-authored unit of work. pgro's controllers reconcile against it,
fetch the kopia secret, run snapshot-list + restore Jobs with long-lived
AWS keys + the kopia repo password baked into env vars. The new
"canopy-backed" path inverts ownership: an operator declares a
**restore-replica** in canopy's operator UI (`(group, server | all,
type, intent, name, freshness)`), and pgro discovers that declaration
via `GET /restore-worklist`, materialises a labelled k8s `Namespace` per
declaration, drives a kopia Job whose data-path goes through a
**bestool S3P loopback proxy sidecar** (kopia talks to `[::1]` with
dummy keys; the sidecar holds refreshing STS creds), and reports the
outcome to canopy via `POST /restore-verification` (signal 3,
group-level restorability alert). The legacy `kopiaSecretRef` CR path
is untouched — both coexist; new replicas should be declared in canopy.

---

## Architecture overview

### Identity and transport

- The operator pod runs a **Tailscale sidecar**. One canopy device,
  role `backup-restore` (canopy-side enum value), promoted from
  `Untrusted` once by an operator. Kopia Job pods are **not** on the
  tailnet and are not canopy devices.
- `bestool-canopy::CanopyClient` auto-probes the tailnet URL
  (`canopy.tail53aef.ts.net`) and falls back to mTLS if a
  `device_key_pem` was provided. pgro is tailnet-primary; mTLS cert is
  optional and used only if provided.
- The operator's reqwest client is built with
  `Proxy::all("socks5://[::1]:1055")` so the auto-probe succeeds
  via the Tailscale sidecar.

### Control loop

A new third controller (`src/controllers/canopy.rs`) ticks on a jittered
interval (~30s), fetches `restore_worklist()`, lists `Namespaces` with
the pgro-managed label, and reconciles the diff:

- worklist entry without a namespace → provision.
- namespace without a worklist entry → tear down.
- both present, freshness exceeded or snapshot id changed → refresh.

No CRDs are involved on the canopy-backed path. Cluster state — the
labelled `Namespace` plus the objects inside it — is the runtime model;
the worklist is the desired state.

### Data path (kopia)

Each kopia Job pod has two containers:

- the kopia container, invoked with `--endpoint=[::1]:<port>`,
  `--disable-tls`, dummy access/secret keys, the canopy bucket/region/
  prefix from the worklist entry, and `--password=` set to
  `RestoreCredentials.repo_password`;
- a pgro-published **proxy sidecar** (new binary, see §6.2) linking
  `bestool-kopia::proxy::spawn` with a pgro `CredentialProvider`
  implementation that calls the operator's in-cluster
  **credential broker** (§4) — *not* canopy directly. The broker hands
  back `BackupCredentials` (the AWS `credential_process` shape) that
  the proxy uses for re-signing. Refresh margin: ~2 minutes before
  expiry, matching bestool.

Consequences of the proxy model:
- kopia never sees real AWS material — only dummy keys and a loopback
  endpoint.
- Long restores survive credential expiry: the proxy refreshes between
  requests, so the 1-hour STS cap is bounded by canopy reachability,
  not by any single issuance.
- The proxy binds a loopback literal in userspace mode; no `NET_ADMIN`
  for the sidecar.

### Reporting (signal 3)

After each restore reaches a terminal phase (`Active` / `Failed` /
deployment-never-ready), pgro builds a `RestoreVerification` and POSTs
it to `/restore-verification`. The report is at-most-once-per-restore,
tracked by an annotation on the replica's Namespace; canopy unreachable
→ retry next reconcile, never fail the restore for the report.

### Capability registration

On operator startup (and on any later change to the supported set), the
operator POSTs `/restore-capabilities` with the intents pgro implements:
`verify`, `analytics`, `disaster-recovery`. Canopy then only dispatches
worklist entries whose intent is in the set; declarations stranded on
unsupported intents become operator-facing configuration *gaps*, not
restore-health failures.

---

## Part 1 — Canopy client wiring

### 1.1 Operator-level config (`operator.yaml` + ConfigMap)

Plumb via env on the Deployment and/or the existing
`postgres-restore-operator-config` ConfigMap that `src/bin/operator.rs`
already reads:

- `CANOPY_BASE_URL` — public-mTLS base URL (used only when the tailnet
  probe fails); the tailnet URL is hardcoded in `bestool-canopy`.
- `CANOPY_DEVICE_CERT_SECRET` — optional. Name of a k8s Secret with
  cert + key, mounted only into the operator pod. Skip entirely for
  tailscale-only operation.
- `CANOPY_RECONCILE_INTERVAL_SECS` — default 30. The worklist-syncer
  tick cadence; jittered ±20% to avoid stampedes.

### 1.2 `bestool-canopy::CanopyClient` construction

In `src/canopy.rs` (new module):

- Construct a `reqwest::Client::builder()` with
  `Proxy::all("socks5://[::1]:1055")` and pass the closure to
  `CanopyClient::new(tamanu_version, device_key_pem_opt, builder)`.
- Stash the `CanopyClient` plus its base `Url` on the `Context`
  (`src/context.rs`) alongside the existing `http_client`.
- Expose thin async methods: `worklist()`, `restore_credentials(group,
  type)`, `restore_verification(&report)`, `restore_capabilities(&intents)`.
  Each is a one-line forward to the underlying `CanopyClient` method;
  the wrapper exists for testability (it's the seam where the
  worklist-syncer integration tests inject a stub).

### 1.3 Tailscale sidecar on the operator Pod

`operator.yaml` Deployment gains a second container, ideally via the
Tailscale k8s operator's `ProxyClass` annotation. Image
`ghcr.io/tailscale/tailscale`, userspace mode (`TS_USERSPACE=true`),
`TS_SOCKS5_SERVER=:1055`, state in `kube:` mode so a restart resumes
the same identity. Authkey OAuth-issued by ops, the node tagged for
ACL admission (tag does not bind canopy role — canopy resolves the
role from the device record after promotion).

**IPv6 verification point**: pgro's k8s cluster is IPv6-only internally
(`[[ipv6-only-cluster]]`), so the Tailscale sidecar's SOCKS5 listener
must bind v6. `TS_SOCKS5_SERVER=:1055` binds all interfaces by default
in Go's net package (dual-stack), so it should work as-is, but verify
once the sidecar is up that `nc -6 ::1 1055` from the operator
container reaches it. If not, set `TS_SOCKS5_SERVER=[::]:1055` explicitly.

---

## Part 2 — Worklist syncer (the new controller)

### 2.1 Controller shape

`src/controllers/canopy.rs`. Not kube-rs CRD-watched (no CR for the
canopy path); instead a `tokio::time::interval` loop that:

1. Calls `ctx.canopy.worklist().await` — returns `Vec<WorklistEntry>`.
2. Lists `Namespaces` with label
   `pgro.bes.au/managed-by=pgro-canopy`. Discovery is the same on
   every tick; no in-memory cache across ticks beyond what's needed
   inside the function.
3. Computes the diff: worklist entries indexed by `replica_id`,
   namespaces indexed by `pgro.bes.au/declaration-id` annotation.
4. Concurrently dispatches per-entry reconciliation; bounds concurrency
   to avoid k8s-apiserver thundering herd (~8 in-flight).

The loop is single-threaded by construction; if a tick is still running
when the next one fires, the next is skipped (`tokio::time::interval`
default behaviour is fine).

### 2.2 Cluster-as-state model

For each worklist entry, the per-replica state lives in a Namespace:

**Namespace name**: `<slug>-<hex>` where:
- `slug` is `WorklistEntry.name` slugified: lowercased, non-alphanumeric
  runs collapsed to `-`, trailing `-` trimmed, truncated to 50 chars. The
  declaration name is operator-set in canopy's UI (e.g. `Nauru prod
  analytics`) — already the human-recognisable label for this replica.
- `hex` is 8 hex chars from `SHA-256(replica_id || server_id)[..4]`. Both
  ids are needed because a `server_id=NULL` declaration in canopy expands
  to one `WorklistEntry` per live server in the group, all sharing
  `replica_id` and `name` (`canopy/crates/public-server/src/restore.rs:175-211`).

Example: `nauru-prod-analytics-a3f9c2d1`. DNS-1123 valid; ≤59 chars.

**Labels** (immutable):
- `pgro.bes.au/managed-by=pgro-canopy` — discovery key.
- `pgro.bes.au/declaration-id=<replica_id-uuid>`
- `pgro.bes.au/group=<group_id-uuid>`
- `pgro.bes.au/server=<server_id-uuid>`
- `pgro.bes.au/intent=verify|analytics|disaster-recovery`

**Annotations** (mutable, runtime state):
- `pgro.bes.au/desired-snapshot-id` / `pgro.bes.au/desired-snapshot-at` —
  the worklist's snapshot for this entry on the last successful sync.
- `pgro.bes.au/last-restored-snapshot-id` / `pgro.bes.au/last-restored-at`
- `pgro.bes.au/restore-state` = `pending|restoring|active|failed`
- `pgro.bes.au/last-verification-reported-at` (RFC3339) —
  at-most-once gate; absent means owed.
- `pgro.bes.au/last-verification-error` — last report failure string.

Other namespace contents (Deployment, Service, PVC, current restore
Job) are conventional k8s objects; they carry their own pgro labels for
discovery but the Namespace is the authoritative root.

### 2.3 Provisioning a replica (worklist entry, no namespace)

1. Create the Namespace with labels + initial annotations
   (`restore-state=pending`, `desired-snapshot-id` from worklist).
2. Create a PVC sized per intent (default per-intent table; overridable
   by ConfigMap entry).
3. Create the restore Job (§6 — same Job builder shape as legacy, with
   the sidecar variant).
4. Subsequent ticks observe the Job and update `restore-state`. On
   Job-success: bring up the postgres Deployment, run readiness gate.
   On Job-failure: mark `restore-state=failed`, store the error in an
   annotation, schedule retry per error policy.

### 2.4 Refreshing (entry + namespace both present)

Refresh triggers, in order of precedence:
- `desired-snapshot-id` from worklist differs from
  `last-restored-snapshot-id` — newer backup available.
- `now > last_restored_at + freshness_seconds` — overdue.
- Manual: an operator annotates the Namespace with
  `pgro.bes.au/force-refresh=now` (last-resort escape hatch).

The refresh creates a new restore Job alongside the existing
Deployment, drains traffic on success, swaps Service endpoints, deletes
the old PVC. Intent-specific cutover policy lives in the existing
`controllers/restore.rs` switching machinery — reused unchanged.

### 2.5 Teardown (namespace, no entry)

The worklist no longer references this replica. Mark
`restore-state=terminating`, drain queries (intent-specific grace
period), delete the Deployment + Service + PVCs, finally delete the
Namespace. A k8s finalizer on the Namespace blocks deletion until pgro
has confirmed teardown; this is the only finalizer pgro adds.

### 2.6 Worklist-fetch failure handling

Treat any error from `restore_worklist()` as transient. Log + metric
the failure, skip the tick, continue. **Do not** tear down replicas
because the worklist was empty due to a fetch error; the tick is a
no-op if the worklist can't be retrieved. Persistent failure for >5
minutes raises a pgro-side metric/alert (no canopy-side alert path
exists for "consumer can't read its worklist").

---

## Part 3 — Restore-verification reporter (signal 3)

### 3.1 When to report

The restore controller (`src/controllers/restore.rs`) drives a restore
through `Pending → Restoring → Ready → Switching → Active → Failed`.
Hook the report at terminal transitions:

- `Active` (or `Ready` + readiness gate passed) — `outcome=success`,
  `replica_healthy=true`.
- `Failed`, or deployment-never-ready within
  `DEPLOYMENT_READY_TIMEOUT_SECS` — `outcome=failure`, `error`
  populated.

### 3.2 Building the body

From the Namespace's labels (declaration-id, group, server, intent,
type) and the restore's tracked state (snapshot_id from
`desired-snapshot-id`, postgres major version from the Deployment's
status, observed_at from terminal-transition time). S3 byte tallies
come from the proxy sidecar — the sidecar writes them as Job
annotations on its own Job at exit; the reporter reads them when
present (absent if the Job died before the sidecar could write).

Reuse the published `bestool_canopy::RestoreVerification<'a>` shape
verbatim. Set `replica_id` from the namespace's
`pgro.bes.au/declaration-id` label.

### 3.3 At-most-once with retry across reconciles

Gate on the namespace annotation
`pgro.bes.au/last-verification-reported-at`. If set, report has landed.
If absent and a terminal transition has been observed, the
worklist-syncer's per-replica step calls
`ctx.canopy.restore_verification(...)`; on 2xx, stamp the annotation;
on error, record the error in
`pgro.bes.au/last-verification-error` and retry next tick. Reporting
failure **never** fails the restore — same posture as the existing
`notifications.rs` retry shape, but at a different cadence (worklist
tick rather than in-loop sleep).

---

## Part 4 — Credential broker (operator-side HTTP endpoint)

### 4.1 The endpoint

Add `POST /internal/restore-creds` to the existing axum router in
`src/bin/operator.rs:307 build_router`. Body: `{ group: Uuid, type:
String }`. Response: the verbatim `RestoreCredentials` body from
`bestool_canopy::restore_credentials` (creds + repo_password). 4xx
errors mirror canopy's; the broker is a transport reuser, not an
authorizer (canopy's `RestoreReplica::authorizes` is the source of
truth, gating the upstream call).

### 4.2 Caching

A small per-(group, type) cache in-process keyed by `(group, type)`,
storing the last response with its `Expiration`. Concurrent Job
sidecars asking close together should not multiply canopy calls.
Cache entries expire 2 minutes before the `Expiration` (same margin as
the sidecar). Cache is best-effort — no persistence; a restart
forgets — that's fine, the sidecar will just see a brief refresh.

### 4.3 In-cluster gating

A `NetworkPolicy` in `operator.yaml` restricts ingress to the broker
port to pods carrying `pgro.bes.au/proxy-sidecar=true` in the same
cluster. The Job builder labels the sidecar containers' pods
accordingly; nothing else in the cluster can hit the broker. The
operator's other endpoints (`/metrics`, `/livez`, etc.) live on the
same listener; the policy gates by path is not feasible at the
NetworkPolicy layer — separating ports is the easiest robust gate.
**Decision:** the broker binds a **separate port** from the existing
metrics router (e.g. `:9091` vs `:9090`); NetworkPolicy gates the
broker port to proxy-sidecar pods, leaves metrics open to the
Prometheus selectors.

---

## Part 5 — Capability registration

### 5.1 On startup

Right after the operator initializes its `CanopyClient` (post-config
load, pre-controller-start), call `restore_capabilities(&["verify",
"analytics", "disaster-recovery"])`. Failure is logged but
non-fatal — the worklist-syncer will start with an empty (or stale)
set on canopy's side, but the next tick that succeeds in
`restore_capabilities` will repopulate.

### 5.2 Change handling

The supported intent set is fixed in the operator binary for now (pgro
implements all three). If the set ever shrinks in a future release,
re-push at startup with the smaller set; canopy turns previously-handled
declarations into config gaps automatically. No runtime
intent-toggling.

---

## Part 6 — Job builders + proxy sidecar

### 6.1 `KopiaSource` enum

Lift the legacy job-builder code in
`src/controllers/restore/builders.rs:508 build_restore_job` and
`src/controllers/replica/resources.rs:51 build_snapshot_list_job` (the
canopy path doesn't need snapshot-list — canopy provides the snapshot —
so only `build_restore_job` matters here) to take a `KopiaSource`:

```rust
enum KopiaSource {
    /// Legacy: env-from-secret + connect-args from validate_kopia_secret.
    Secret { kopia_secret: SecretReference, creds: KopiaCredentials },
    /// Canopy: proxy sidecar + dummy keys + broker URL.
    CanopyProxy {
        broker_url: String,
        repo_password: SecretKeySelector,  // password from a per-Job Secret materialised by the syncer
        bucket: String,
        region: String,
        prefix: String,
        server_id: Uuid,
    },
}
```

`kopia_connect_args` (`src/kopia.rs:211`) gains a parallel
`kopia_connect_args_proxy` that emits `--endpoint=[::1]:<port>`,
`--disable-tls`, dummy keys, `--password=$(REPO_PASSWORD)`,
`--override-username=canopy`, `--override-hostname=<server_id>`. The
legacy connect-args helper is unchanged.

### 6.2 The proxy sidecar binary

A new `[[bin]]` in `Cargo.toml`: `canopy-proxy`. Single small Rust
binary linking `bestool-kopia` (for `proxy::spawn`,
`CredentialProvider`, `Credentials`, `TrafficStats`) and `bestool-canopy`
(only for the `BackupCredentials` shape; the sidecar doesn't call canopy
directly). What it does:

1. Reads `PGRO_BROKER_URL`, `PGRO_GROUP`, `PGRO_TYPE`, `PGRO_REGION`,
   `PGRO_LISTEN_PORT` from env.
2. Constructs a `BrokerCredentialProvider` (new pgro type, implements
   `bestool_kopia::proxy::CredentialProvider`) that calls
   `<broker_url>/internal/restore-creds` with `{group, type}` and
   caches per the same 2-minute margin.
3. Calls `bestool_kopia::proxy::spawn` with that provider, the
   `S3ProxyConfig { upstream: "https://s3.<region>.amazonaws.com",
   upstream_host: "s3.<region>.amazonaws.com", region }`, bound to
   `[::1]:<port>`.
4. Waits on a graceful shutdown signal (the kopia container exiting
   should propagate; sidecar's restart policy lets it exit cleanly
   after kopia is done). On exit, writes the proxy's `TrafficStats` to
   a known annotation on its own Job so the verification reporter can
   read it.

**Upstream dependency — bestool-kopia bind address.** Today
`bestool_kopia::proxy::spawn` hardcodes
`TcpListener::bind(("127.0.0.1", 0))` at
`bestool/crates/kopia/src/proxy.rs:174`. pgro's k8s cluster is
**IPv6-only internally** (`[[ipv6-only-cluster]]`), so the v4 loopback
bind fails.

Fixed in `beyondessential/bestool#616` ("fix(kopia): bind proxy to
IPv6 loopback, falling back to IPv4"): a new private `bind_loopback()`
helper tries `::1` first, falls back to `127.0.0.1`. No API change,
no caller-side decision — pgro just depends on whatever
`bestool-kopia` version ships next (≥0.3.4 expected). pgro doesn't
pass a bind option.

This is the only external blocker for the pgro PR; track #616 to
merge + release.

### 6.3 Job spec changes

The canopy-variant Job has two containers in one Pod:
- `kopia` (existing image, connect-args from
  `kopia_connect_args_proxy`)
- `canopy-proxy` (new pgro-published image)

Both containers share `network=Pod` so `[::1]` works. Resource
limits: the sidecar is very small (~50Mi memory typical, single core
adequate even for streaming uploads — kopia restore is GET-heavy and
the proxy just re-signs).

The Pod gets the label `pgro.bes.au/proxy-sidecar=true` so the
NetworkPolicy in §4.3 admits it.

### 6.4 Image build + release

Pgro's existing CD workflow (`.github/workflows/cd.yml:74`) builds a
single binary. Update the matrix to build both binaries
(`operator` + `canopy-proxy`) and to push two images, sharing the
multi-arch cross-compile work. Containerfile gains a second stage with
the new entrypoint; image tag aligns with the operator's tag for
release-version coupling.

---

## Coexistence with the legacy `kopiaSecretRef` path

Untouched. The CRDs `PostgresPhysicalReplica` / `PostgresPhysicalRestore`
keep their existing shapes; the existing CR-driven controllers
(`controllers/replica.rs`, `controllers/restore.rs`,
`controllers/postgres.rs`, `controllers/jobs.rs`) reconcile them
exactly as today. The new worklist-syncer is **additive** — a third
controller that operates on Namespaces with a different label key,
producing Job specs through the same builder code but via the
`KopiaSource::CanopyProxy` variant.

There is no migration path between the two — operators wishing to
move a replica off the legacy path delete the `PostgresPhysicalReplica`
CR and declare the equivalent restore-replica in canopy's UI. The
legacy `kopia-credentials` Secret is theirs to delete.

---

## Interfaces this component exposes / consumes

**Consumes (from canopy via `bestool-canopy` 0.4.2):**
- `CanopyClient::restore_capabilities(base, &[intents])`
- `CanopyClient::restore_worklist(base) -> Vec<WorklistEntry>`
- `CanopyClient::restore_credentials(base, type, group) -> RestoreCredentials`
- `CanopyClient::restore_verification(base, &RestoreVerification)`

**Consumes (from bestool, published cargo crates):**
- `bestool-canopy` ≥0.4.2 — the four methods above + the wire types
  (`WorklistEntry`, `RestoreCredentials`, `RestoreVerification`,
  `RestoreCapabilitiesRequest`, `RestoreCredentialsRequest`).
- `bestool-kopia` ≥0.3.3 — `proxy::spawn`, `CredentialProvider` trait,
  `Credentials`, `TrafficStats`, `PROXY_DUMMY_ACCESS_KEY`/`SECRET_KEY`.

**Provides internally:**
- `POST /internal/restore-creds` on the operator's broker port —
  consumed only by the proxy sidecar in canopy-backed restore Jobs.

**Provides operationally:**
- A new sidecar image (`ghcr.io/beyondessential/pgro-canopy-proxy`)
  alongside the existing operator image.
- Namespaces labelled `pgro.bes.au/managed-by=pgro-canopy` —
  observable via `kubectl get ns`.

---

## IaC / deployment changes (`operator.yaml` + ops repo)

- **Operator Deployment**: add the Tailscale sidecar container (or
  the Tailscale-operator `ProxyClass` annotation); add env vars
  `CANOPY_BASE_URL`, `CANOPY_RECONCILE_INTERVAL_SECS`, and (optionally)
  `CANOPY_DEVICE_CERT_SECRET`; add a second container port for the
  broker (`9091`).
- **NetworkPolicy** restricting the broker port to pods labelled
  `pgro.bes.au/proxy-sidecar=true` within the cluster (deny ingress
  otherwise).
- **ClusterRole delta**: the operator already has full Namespace +
  Deployment + Job + Secret verbs cluster-wide for the legacy path;
  the same RBAC covers the canopy-backed path. Verify
  `verbs=[create, delete]` on Namespaces is present.
- **ops-repo, one-time per cluster**: install the Tailscale k8s
  operator; mint an OAuth client + ACL tag for pgro's operator pod;
  ACL allows the tag to reach `canopy.tail53aef.ts.net`. After the
  operator's first contact creates the canopy `Untrusted` device row,
  promote to role `backup-restore` (one-time manual step).

---

## Testing approach (per `AGENTS.md`)

**Unit tests (local):**
- `src/canopy.rs`: wrapper round-trip against an axum stub server that
  serves canned `WorklistEntry` / `RestoreCredentials` JSON; assert
  serde + URL routing. `bestool-canopy`'s own tests cover wire-shape
  correctness, so pgro only tests its wrapper layer.
- Worklist syncer reconciliation: table-driven tests with synthesized
  `Vec<WorklistEntry>` + `Vec<Namespace>` inputs, asserting the
  expected provision/refresh/teardown decisions (no apiserver, no
  reconcile actions — just the diff function).
- `KopiaSource::CanopyProxy` job-builder snapshot tests, mirroring the
  existing `kopia_connect_args_*` tests for the proxy connect-args
  shape.
- Reporter at-most-once: assert that a stamped
  `last-verification-reported-at` prevents re-report; reset → report
  → annotation written.
- Capability registration on startup: the operator pushes
  `["verify","analytics","disaster-recovery"]`; transient failure is
  retried, fatal config error is surfaced.

**Integration tests (CI-only; new matrix entry required):**

- `tests/canopy_integration.rs` — a `test-canopy-restore` namespace
  exercising the canopy path against a **stub canopy** (a small
  in-cluster HTTP service implementing the four endpoints with canned
  responses, reusing the existing `tests/fixtures/minio.yaml` +
  `setup-kopia-repo.yaml` as the S3 backing store). Assert:
  - worklist tick discovers no namespaces, creates one; restore Job
    reaches `Active`; verification stub receives a `RestoreVerification`
    with the expected `replica_id`/`snapshot_id`/`outcome=success`.
  - worklist entry removed → namespace torn down within ~2 ticks.
  - `RestoreReplica::authorizes`-style 403 from the stub → pgro
    surfaces a clear failure (annotation + event) and doesn't
    crash-loop.
- Matrix entry added in `.github/workflows/integration.yml`. Flag to
  the user that this only runs in CI.

---

## Open questions / decisions

1. **Per-intent provisioning defaults.** Storage size, postgres
   resource requests, retention of the previous Deployment on refresh
   — intent-dependent. Worklist entries don't carry these; pgro picks
   from a defaults table per intent, with a per-replica ConfigMap
   override mechanism. Define the defaults table during Part 2
   implementation.

2. **Snapshot-list Job retirement.** Canopy supplies the snapshot id,
   so `build_snapshot_list_job` is unused for canopy-backed replicas.
   It stays for the legacy path. Decide whether to refactor
   `controllers/replica/resources.rs` to make the variance explicit,
   or leave the legacy path alone and just never call it from the new
   syncer.

3. **Reporter latency vs reconciles.** Verification reports go out on
   the next worklist tick after a terminal transition (up to ~30s
   delay). If that's too slow, the restore controller can call the
   reporter inline at the terminal transition — same idempotency
   guard. Default: tick-driven (simpler), inline if real users
   complain.

4. **Per-replica freshness drift.** The worklist's `freshness_seconds`
   is canopy's. If a replica is mid-restore when the freshness window
   opens for the next refresh, pgro should not start a second restore
   in parallel — serialize per-namespace. The existing restore phase
   machine enforces this implicitly (one CR + one Job), but the
   canopy path needs an equivalent serialization on the Namespace.

5. **NetworkPolicy in dev/test clusters.** Some integration test
   environments don't enforce NetworkPolicy (no CNI plugin). The
   broker should be deployable without NetworkPolicy in those
   environments; in production, NetworkPolicy is the only thing
   between the broker and arbitrary in-cluster callers. Document
   this clearly.

---

## Sequencing

One PR. Order within the PR:

1. `src/kopia.rs` — `kopia_connect_args_proxy` + tests.
2. `src/canopy.rs` — `CanopyClient` wrapper, types re-exported from
   `bestool-canopy`.
3. `Cargo.toml` — add `bestool-canopy = "0.4"`, `bestool-kopia = "0.3"`;
   add `[[bin]] name = "canopy-proxy"`.
4. `src/bin/canopy_proxy.rs` — the sidecar binary.
5. `src/controllers/canopy.rs` — the worklist-syncer controller.
6. `src/bin/operator.rs` — wire the new controller; register
   capabilities; mount the broker route on a separate port.
7. Job-builder `KopiaSource` refactor + sidecar container injection.
8. `tests/canopy_integration.rs` + stub canopy + `integration.yml`
   matrix entry.
9. `operator.yaml` — Tailscale sidecar, broker port, NetworkPolicy.
10. `.github/workflows/cd.yml` — second image build + push.

Legacy CRD path stays compiling at every commit; the canopy path is
behind no feature flag (it's only exercised when canopy hands out a
worklist, which requires a promoted `backup-restore` device — gated by
canopy-side configuration, not pgro code).
