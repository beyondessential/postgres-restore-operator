# Restore prune fix + too-many-restores guardrail

## Background

In production, `tamanu-replica-nauru-prod` accumulated 41 stale `Active` PostgresPhysicalRestore objects between 2026-05-02 and 2026-05-08 (each owns a Deployment + ~40 GB PVC on EBS). The replica was creating one new restore every 3h on schedule, but never deleting the previous ones.

The 41 restores were manually cleaned up live; this plan addresses why pruning broke and adds a guardrail to make recurrence visible.

## Root cause (confirmed by code reading)

The cleanup of the previous restore in `src/controllers/replica.rs` (`reconcile_replica`, around L274–330) requires `migration_complete` before it will delete `previousRestore`:

```rust
let migration_complete = if replica.spec.persistent_schemas.is_some() {
    replica
        .status
        .as_ref()
        .and_then(|s| s.schema_migration_phase.as_ref())
        .is_some_and(|p| p == "complete")
} else {
    true
};
```

When `persistent_schemas` is configured but **all** configured schemas are missing on the source DB (the nauru-prod situation: spec has `[dbt]`, source DB does not yet have it), `reconcile_schema_migration` takes this branch in `src/controllers/replica.rs` at L1209–1215:

```rust
if schemas.is_empty() {
    info!(replica = %replica_name, "no persistent schemas exist on source, skipping migration");
    return Ok(true);
}
```

It returns `Ok(true)` so the switchover proceeds, but **never patches `schemaMigrationPhase = "complete"` into status**. The cleanup branch therefore sees `migration_complete = false` forever and the previous restore is never deleted.

Worse: each subsequent switchover overwrites `previousRestore` in status with the now-old `currentRestore`, *without* first cleaning up the prior `previousRestore`. So even fixing the missing status update would leave older restores orphaned. The `previousRestore` field is a single pointer — there is no mechanism to track "all restores awaiting cleanup".

## Goals

1. nauru-prod-style replicas (persistent_schemas configured but all missing on source) prune correctly.
2. Cleanup is robust to transitions where the previous-previous restore had not yet hit grace period — i.e. the system should converge to "exactly one Active restore" (the current one) regardless of how it got into a degraded state.
3. A degenerate case where many restores already exist must not cause the operator to keep making it worse — provide a backstop.

## Plan

### Phase 1: Fix the missing status update (minimum bug fix)

In the `schemas.is_empty()` branch in `reconcile_schema_migration`, patch `schemaMigrationPhase: "complete"` into status before returning. This is the smallest fix that unblocks the existing cleanup logic for the nauru-prod case.

This alone is NOT sufficient — the `previousRestore` overwrite issue remains — but it's the local bug fix.

### Phase 2: Make cleanup sweep-based instead of single-pointer

Rather than only deleting `status.previousRestore`, in the same cleanup block sweep all restores in the namespace owned by this replica that:
- are NOT `status.currentRestore`
- are in phase `Active` (i.e. were activated and didn't fail — Failed restores are handled separately at L332+)
- have an `activated_at` (or `restored_at`) older than `switchover_grace_period`
- AND `migration_complete` is true (preserves the persistent-schemas safety property)

Delete each one. This converges naturally regardless of how many leftovers exist, and removes the dependency on the `previousRestore` single-pointer being perfectly maintained across overwrites.

We can keep the `previousRestore` status field as-is for backwards compat / observability, but cleanup decisions stop relying on it being the only thing to delete.

Edge case: when sweeping, never delete a restore that is currently `Switching` (in progress) or `Pending`/`Restoring`/`Ready` (in-progress new restore). Phase filter to `Active` only handles this, since switchover-related phases aren't `Active`.

### Phase 3: Too-many-restores guardrail

In `create_restore_for_snapshot` (in `src/controllers/replica/resources.rs`), before creating a new restore, count the existing restores for this replica. If >= 3, skip creation, log a warning, and emit a Kubernetes Event so it's visible without log-diving. Also bubble the state up via a status condition (e.g. `RestoreCreationBlocked: True` with reason `TooManyRestores`) so `kubectl describe` shows it.

Threshold of 3 picked per user request: in steady state a replica should have 1 (current) or transiently 2 (current + switching/previous in grace). 3+ is already pathological.

The check happens inside the create function, not in the scheduling logic, because the scheduling logic is the wrong layer — multiple paths could feasibly call create. (Verify there is exactly one caller in current code; if so we can put it at either layer, but keeping it close to the create is more defensive.)

### Phase 4: Tests

Existing tests live in `src/controllers/replica/tests.rs` and `src/controllers/restore/tests.rs`. Add:

- Test: persistent_schemas configured but missing on source → after switchover, `schemaMigrationPhase` is set to `complete` in replica status (covers Phase 1).
- Test: replica with N>3 stale Active restores has them swept up after one reconcile (covers Phase 2). Or simpler: replica with `previousRestore` + one extra Active restore both get swept after grace period.
- Test: `create_restore_for_snapshot` refuses to create when 3 restores already exist for the replica; emits status condition (covers Phase 3).

If existing test scaffolding doesn't trivially support these (e.g. needs more fakes around the schema migration paths), call that out and trim scope.

### Phase 5: Status condition for observability

Add a `RestoreCreationBlocked` condition to the replica status when the guardrail trips. This makes the degraded state visible via `kubectl describe` instead of buried in logs.

## Out of scope / consciously deferred

- Adding a configurable retention / `keep_count` field to the spec. The current model is "1 active restore at a time" (plus transient overlap). If users later want N>1 historical restores kept, that's a separate feature.
- Backporting / migration: existing replicas with leftover `previousRestore` referring to a now-deleted restore object will self-heal once cleanup runs (delete will 404, `previousRestore` will be cleared in the existing branch).
- Re-evaluating whether the `previousRestore` single pointer should be replaced with a list. Phase 2 makes the cleanup independent of it, which is enough for now.

## Acceptance

- Phase 1+2 deployed: nauru-prod (and any future replica with all-skipped persistent_schemas) prunes correctly. Verified by watching restore counts stay at 1–2 over 24h.
- Phase 3 deployed: if anything goes wrong again, restore count caps at 3 and surfaces a clear `RestoreCreationBlocked` condition.

## Branching

Repo uses jj. Work on a new branch off main.

## Implementation status

- Phase 1: ✅ shipped. `mark_schema_migration_complete` helper called from all three early-return branches in `reconcile_schema_migration` (first restore, empty config, all schemas missing on source).
- Phase 2: ✅ shipped. Cleanup block replaced with sweep over all Active restores != currentRestore. Refuses to sweep if no live Active matches `status.currentRestore`.
- Phase 3: ✅ shipped. `MAX_RESTORES_PER_REPLICA = 3` constant. `create_restore_for_snapshot` returns `Result<bool>` — `false` on guardrail. `RestoreCreationBlocked` condition + Warning Event. Caller skips post-create side effects on `false`.
- Phase 4 tests:
  - Phase 1+2: ✅ integration test `persistent_schemas_all_missing_prunes_previous_restore` in `tests/persistent_schemas.rs`. Matrix entry extended.
  - Phase 3: ⏳ deferred. End-to-end exercise requires rotating the kopia snapshot mid-test (a `setup-second-snapshot.yaml` fixture). The guardrail logic itself is 5 lines and inspected; field exposure via the new condition makes it self-evident in production. Worth adding when next we touch the test fixtures.
- Phase 5: ✅ shipped (`RestoreCreationBlocked` condition).
