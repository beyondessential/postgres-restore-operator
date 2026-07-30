# Scale replica resources with snapshot size

## Problem

`storageSizeOverride` was a fixed per-intent constant that capped the restore
PVC instead of raising it, so any replica whose snapshot outgrew it failed
every restore. That is fixed (the override is now a floor and the size is
computed from the snapshot).

The same anti-pattern remains in three other places on the canopy path. All of
them are compile-time constants in `intent.rs` or `builders.rs`, applied
identically to every canopy replica regardless of how much data it holds, with
no canopy parameter to change them:

1. **Postgres resources + shm floor.** `analytics` gets requests 500m/2Gi,
   limits 4/8Gi, shm floor 2Gi; `verify` gets 250m/512Mi, 2/2Gi, 512Mi. A
   replica holding a few hundred MB reserves exactly the same as one holding
   over a hundred GB. `shared_buffers` derives from these (70% of shm), so
   every analytics replica runs an identical postgres memory config across
   three orders of magnitude of data.
2. **Deployment readiness timeout.** `DEPLOYMENT_READY_TIMEOUT_SECS`, default
   30 min, is a single cluster-wide env var. A large replica has to open its
   data dir and replay WAL inside the same window a tiny one gets, or the
   restore is marked `Failed` and the whole cycle restarts. Raising it for the
   large replicas blunts failure detection for every small one.
3. **Restore Job resources.** Fixed at requests 500m/1Gi, limits 2 CPU/4Gi
   regardless of snapshot size. kopia reports `parallelism=8` — it sizes
   workers from visible CPUs, not the cgroup limit — while capped at 2 CPUs.

## Design

Agreed shape: **snapshot-derived default, canopy parameter overrides.**

Both inputs are already available where they are needed — `build_deployment`
and `build_restore_job` each receive the `PostgresPhysicalRestore` (carrying
`spec.snapshotSize`) and the `PostgresPhysicalReplica`. No new plumbing is
required to compute from observed size.

Precedence, highest first:

1. An explicit value on the replica spec (`spec.resources`), set either by a
   canopy parameter or hand-authored on a legacy replica.
2. The snapshot-derived value.
3. A floor, so a small replica never lands below something postgres can run in.

This mirrors the storage fix: the per-intent constant becomes a lower bound
rather than the answer.

### What scales, and what does not

Only **memory** scales with snapshot size. Memory tracks data volume — buffer
cache, working set, WAL replay. **CPU does not**: it tracks query concurrency,
which is a property of the workload, not of how much data is on disk. CPU
therefore stays on the intent constant with a canopy parameter to override it.
Scaling CPU with bytes would be cargo-culting the memory rule.

### The curve

`memory_limit = clamp(snapshot_bytes * RATIO, floor, cap)`, with
`memory_request = memory_limit / 4` (the ratio the current constants already
use: 2Gi request against an 8Gi limit).

`RATIO` starts at 10%. On the current fleet that puts the largest replicas
somewhat above today's 8Gi and drops the small ones to their floor.

**This curve is a guess.** There is no measured working-set data behind it, and
postgres memory demand is not linear in database size. It is picked to be
directionally right and bounded on both ends rather than to be correct. It
should be revisited once there are real numbers; the canopy parameter is the
escape hatch until then.

### Risk: shrinking currently-working replicas

Small canopy replicas run at an 8Gi limit today. Under the curve they drop to
their floor. That is the intended saving, but it is a live change to running
workloads, so the floor must stay comfortably above what a small postgres
needs, and the change should go out where it can be watched.

## Work items

### 1. Snapshot-derived postgres memory

- `src/types/replica.rs`: add `resourcesFloor: Option<ResourceRequirements>`,
  the lower bound on the derived value. Follows the existing `shmSizeFloor`
  precedent (a spec field that floors a computed value).
- `src/quantity.rs`: add `scale_memory_for_snapshot(snapshot, floor, cap)`
  returning `ResourceRequirements`. Pure; unit-tested across the size range.
- `src/controllers/restore/builders.rs` (`build_deployment`): use
  `spec.resources` when set, else the derived value floored by
  `spec.resourcesFloor`. `compute_shm_and_shared_buffers` then runs on the
  result unchanged, so `shared_buffers` follows automatically.
- `src/controllers/canopy/intent.rs`: stop setting `resources` unconditionally.
  Set it only when a canopy parameter supplies it; always set `resourcesFloor`
  from the intent constant. Add `memory_request`, `memory_limit`, `cpu_request`,
  `cpu_limit` to `params` and to the advertised schema.

### 2. Per-replica readiness timeout

- `src/types/replica.rs`: add `deploymentReadyTimeout: Option<String>`
  (friendly duration, like `switchoverGracePeriod`).
- `src/controllers/restore.rs`: at the readiness-timeout check, resolve in
  order: spec field, else derived from snapshot size, else the global env
  default. Derived starts at `30m + 15m per 100GiB` — again a conservative
  guess, not a measurement.
- `src/controllers/canopy/intent.rs`: add a `deployment_ready_timeout` param so
  canopy can pin it per site.

### 3. Restore Job resources and kopia parallelism

- `src/controllers/restore/builders.rs` (`build_restore_job`): derive the
  memory limit from snapshot size on the same helper as work item 1, floored at
  today's 4Gi. Keep CPU on the constant.
- Pass `--parallel` to the kopia restore explicitly, matching the container's
  CPU limit rather than letting kopia infer 8 workers from node CPUs it cannot
  use. This is the actual defect in item 3 — the resource numbers are secondary.

### 4. Docs

- Update the `PostgresPhysicalReplica` spec table in `README.md` for
  `resourcesFloor` and `deploymentReadyTimeout`, and amend the `resources` row
  to say it is now an override over a derived default.

## Out of scope

- Retuning the kopia cache PVC formula (`max(snapshot * 0.2, 10Gi)`). It already
  scales with snapshot size, so it is not this bug. It is empirically low at the
  top end — a large restore hit ENOSPC on the cache partway through and was
  rescued by the pressure-driven autogrow — but changing the ratio wants
  measurement, not a guess bundled into this change.
- Canopy-side work to actually populate the new parameters. pgro advertises them
  and reads them; sites keep getting the derived default until canopy sets one.
