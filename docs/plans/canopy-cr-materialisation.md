# Plan: canopy path via CR materialisation

## Problem

PR #73 shipped a parallel canopy path that duplicates most of what the
CRD path already does — a separate syncer, a separate reporter, a
separate postgres Deployment builder, a separate Service builder, a
separate restore-Job builder, a separate state machine (Namespace
labels + annotations). Result: `persistentSchemas`, `switchoverGracePeriod`,
blue/green refresh, credential-reset, analytics-user provisioning,
schema migration — all of which the CRD path implements — need to be
re-implemented on the canopy path from scratch. That's what caused
the "in-place PVC overwrite" (unacceptable), the disabled
`persistent_schemas`, and the current warn-and-hope situation.

The CR path already has blue/green, phase machine, switchover with
grace, schema migration, analytics-user, resources, tolerations,
affinity, `serviceAnnotations`, `minimumTtl`, deletion cascade via
ownerRefs. Everything the canopy path wants.

## Approach

Make the canopy syncer materialise `PostgresPhysicalReplica` CRs from
the worklist. The CR is pgro-internal state — canopy remains the
operator interface. Two things about the pipeline change; everything
else is untouched:

1. **Credential source** in the Job builders: proxy-sidecar variant
   (dummy keys + `[::1]:<port>` endpoint) when a new `canopy_source`
   field is set on the CR, instead of env-from-Secret.
2. **Snapshot selection**: skip snapshot-list when `canopy_source` is
   set; take snapshot id from a new `Status.canopy_desired_snapshot_id`
   field written by the syncer each tick.

Everything else — reconciler, Restore CR machinery, switchover, schema
migration, notifications — works unchanged.

## Concrete changes

### 1. Extend `PostgresPhysicalReplicaSpec`

- Add `canopy_source: Option<CanopySource>` where
  `CanopySource { group: Uuid, type: String }`.
- Make `kopia_secret_ref` optional; exactly one of the two must be set.
- Validation: at admission time (webhook or reconcile-time check —
  reconcile-time is simpler, matches existing `Error::InvalidKopiaSecret`
  pattern), reject a spec that sets both or neither.
- Add `Status.canopy_desired_snapshot_id: Option<String>` — the
  syncer writes it, the reconciler consumes it.
- CRD regeneration + README table update per AGENTS.md.

### 2. Job builders: proxy-sidecar branch

- Introduce a `KopiaSource` enum (or similar) consumed by
  `build_restore_job` and `build_snapshot_list_job` (though
  snapshot-list gets skipped for canopy, keep the option for
  symmetry).
- `KopiaSource::Secret { kopia_secret_ref, creds }` — current
  behaviour, env-from-Secret.
- `KopiaSource::CanopyProxy { broker_url, region, group, type,
  repo_password_source, server_id }` — dummy keys, `[::1]:<port>`
  endpoint via a pgro-canopy-proxy sidecar container, port-file
  handshake, kopia connects via loopback.
- Pull the shell wrapper + sidecar container spec from
  `src/controllers/canopy/builders.rs` into the CRD-path Job builder,
  then delete the canopy-path builder.

### 3. Replica reconciler: skip snapshot-list when canopy-sourced

- If `spec.canopy_source.is_some()`: skip the snapshot-list Job
  entirely. Take the snapshot from `status.canopy_desired_snapshot_id`.
- When `status.canopy_desired_snapshot_id` changes (or on schedule
  fire), create a new `PostgresPhysicalRestore` CR pointing at the new
  snapshot — same code path as today, just a different snapshot source.
- Everything else in the reconciler (Restore phase machine, switchover,
  grace, schema migration, credential reset, analytics user) stays
  untouched.

### 4. Canopy syncer becomes tiny

Replace ~1000 lines of parallel machinery with:

```rust
async fn tick(&self) -> Result<()> {
    let entries = self.ctx.canopy.worklist().await?;
    let existing = self.list_canopy_managed_replicas().await?;

    for entry in &entries {
        self.ensure_replica_cr(entry).await?;
    }
    for cr in &existing {
        if !worklist_covers(cr, &entries) {
            self.delete_replica_cr(cr).await?;
        }
    }
    Ok(())
}
```

Where `ensure_replica_cr`:
- Namespace: `pgro-r-<slug(name)>-<8hex>` (as today).
- Spec: from `IntentConfig(entry.intent)` — resources, service
  annotations, persistent_schemas, min_ttl, switchover_grace,
  read_only, analytics_username. Plus `canopy_source` = { group, type }.
- Status patch: `canopy_desired_snapshot_id = entry.snapshot_id`.
- Label: `pgro.bes.au/managed-by=pgro-canopy` (discovery key).

The syncer no longer touches PVCs, Jobs, Deployments, Services,
Secrets — the existing CR reconciler owns all of that.

### 5. Guardrail: reject user edits to canopy-managed CRs

Simplest: in the replica reconciler, if the CR has label
`pgro.bes.au/managed-by=pgro-canopy` and the spec differs from what
the intent config would produce, re-apply the intent config. The
canopy syncer's next tick re-asserts anyway.

Not building an admission webhook yet — the reconcile-time re-assert
is enough to converge, and users can't hurt themselves for long.

### 6. Restore-verification reporter

Two options:
- **(a)** New notification target `RestoreCanopyReport` in
  `notifications.rs` alongside webhook / graphQL. Reuses the existing
  retry / status-tracking pipeline.
- **(b)** Small dedicated controller watching Restore CRs, POSTs on
  terminal phase transitions.

(a) is simpler; use it. The report body is built from Replica +
Restore CR fields (already have everything: replica_id from label,
group/server from spec.canopy_source + status, snapshot_id from
restore.spec, postgres_version from restore.status, observed_at
from restore.status.activatedAt).

## Retire

- `src/controllers/canopy/builders.rs` — delete. Job builder branch
  moves to CRD-path builder; postgres Deployment / Service / PVC
  builders die (CRD path has them).
- `src/controllers/canopy/reporter.rs` — delete. Replaced by the
  notification target.
- Most of `src/controllers/canopy.rs` — the diff/action/dispatch
  machinery + provision/refresh/teardown functions all die. What
  remains: the tick loop + `ensure_replica_cr` / `delete_replica_cr`.
- `Context.canopy_broker_base_url` / `canopy_proxy_image` /
  `canopy_pgdata_pvc_size` / `canopy_stats` — most stay, but the
  PVC-size default moves to being a CR spec field the syncer sets from
  IntentConfig.

## Keep

- `src/controllers/canopy/intent.rs` — still drives per-intent CR
  spec generation. `IntentConfig` gets a helper method:
  `fn to_replica_spec_patch(&self, entry: &WorklistEntry) -> serde_json::Value`
  (or similar).
- `src/bin/canopy_proxy.rs` — the sidecar binary.
- Broker route + `Context.canopy_stats` — the sidecar callback.
- Tailscale sidecar in `operator.yaml`.
- `src/canopy.rs` client wrapper.

## Sequence

Order that minimises "broken states" between commits:

1. **Add `canopy_source` + status field to the CRD.** New optional
   fields; existing behaviour unchanged. CRD regen + README update.
2. **Extend the Job builders with `KopiaSource` enum.** Both paths
   route through it; legacy path uses `KopiaSource::Secret`. No
   behaviour change yet.
3. **Job builder gains `CanopyProxy` variant.** Emits the sidecar
   spec + dummy keys. Not called yet.
4. **Reconciler: skip snapshot-list when `canopy_source` is set.**
   Take snapshot from `Status.canopy_desired_snapshot_id`. Guarded so
   legacy path is unaffected.
5. **`IntentConfig::to_replica_spec_patch`** — converts an intent + a
   worklist entry into a Replica-spec JSON patch.
6. **Rewrite canopy syncer** to materialise CRs. All old logic
   deleted; only ensure_replica_cr / delete_replica_cr remain.
7. **Retire** `canopy/builders.rs` + `canopy/reporter.rs`.
8. **Add canopy notification target** in `notifications.rs`; hook it
   at the Restore terminal-transition callsites.
9. **CI + README updates.**

Each step's `cargo check` + `cargo test --lib` must pass. Fmt + clippy
before each commit.

## Verification

- Unit tests survive at every commit (176+).
- Legacy `kopiaSecretRef` integration tests continue to pass — steps
  1-4 are additive to that path.
- Canopy path integration test in a follow-up (still gated on the
  stub-canopy server).
- Manual: apply a canopy-labelled Replica CR by hand pointing at a
  test worklist; watch the reconciler run through Restoring →
  Ready → Switching → Active exactly as with a legacy Replica.

## What NOT to build in this PR

- Admission webhook. Reconcile-time re-assert is the guardrail.
- Blue-green refactor of the canopy path — the CR path already does
  blue/green.
- Anything about `disaster-recovery` intent — separately gone in #79.

## Follow-up (out of scope for this plan)

- Stub-canopy server + wire integration test back into CI.
- Publish a snippet in the ops handoff about how canopy declarations
  map onto pgro's internal CRs, and how to poke at them via kubectl
  for debugging.
