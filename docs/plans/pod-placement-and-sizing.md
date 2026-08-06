# Pod placement and resource sizing

Two related fixes to how the operator asks the cluster for capacity. Both came
out of investigating analytics runs that fail on their first attempt and succeed
on a retry.

## Why

The operator expresses no placement intent for any pod it creates — no
`nodeSelector`, no affinity, nothing — and it understates every postgres pod's
memory request to a quarter of the limit it derived. Together those two gaps put
restored databases on ingress-tier nodes sized for a fraction of the workload,
and make Karpenter treat the live analytics endpoint as spare capacity.

Observed per restore cycle: Karpenter provisions a node for the incoming
restore, the switchover points the Service at it, the stale-Active sweep frees
the outgoing restore, and Karpenter then consolidates the now-underutilised node
away — taking the live pod with it. Every consolidation decision in the sampled
window was `Underutilized`; none were spot interruptions.

## Part 1 — honest resource requests

Requests are what the scheduler, Karpenter's instance selection, Karpenter's
consolidation, and the QoS class all read. Deriving a limit from real data and
then declaring a quarter of it makes every one of those decisions wrong.

- Drop `MEMORY_REQUEST_FRACTION` from `src/quantity.rs`.
  `scale_memory_for_snapshot` returns the clamped snapshot-derived value as both
  request and limit.
- `compute_shm_mib` needs no change, but its result shifts: with request equal
  to limit, `min(request/2, 0.36 × max(request, limit))` collapses to `0.36 × M`,
  so shm becomes 36% of memory and `shared_buffers` 25.2%. That is the intended
  outcome, not a side effect to suppress.
- Analytics intent floor in `src/controllers/canopy/intent.rs` becomes
  `requests {cpu: 2, memory: 2Gi}`, `limits {memory: 8Gi}` — CPU request raised,
  CPU limit removed. CPU limits on a database cause CFS throttling on bursty
  analytical queries without buying anything the request doesn't already
  guarantee.
- The `resources()` helper takes four `&str`; the CPU limit parameter becomes
  `Option<&str>` so an absent CPU limit is expressible.
- `resolve_postgres_resources` in `src/controllers/restore/builders.rs` already
  copies CPU from `floor.requests` and `floor.limits` independently, so an absent
  CPU limit propagates with no change. Add a test pinning that, since it is
  load-bearing and implicit.
- Remove `shm_size_floor: 2Gi` from the analytics `IntentConfig`. It exists only
  to compensate for the low request and is dead once 36% of the 8Gi floor
  (2949Mi) clears it. The CRD field stays for legacy replicas.
- Delete the comment at `intent.rs:336` justifying a low request to avoid "the
  k8s scheduling cost", which is the antipattern being removed.
- Update tests that encode the 4× gap: `request_is_a_quarter_of_the_limit` in
  `src/quantity.rs`, the shm and `shared_buffers` expectations around it, the
  intent floor assertions, and add coverage for the postgres container carrying
  no CPU limit.
- Update README rows for `resources`, `resourcesFloor`, `resourcesMaximum` and
  `shmSizeFloor`, which describe the old derivation.

Scope includes the `verify` and `upgrade` intents, which get Guaranteed memory
too. Their floor limit is 2Gi against an 8Gi cap and they are ephemeral, so the
cost is bounded and short-lived. Their CPU is left alone.

## Part 2 — operator-wide pod placement

Every node in the cluster carries `bes.node.purpose` (`workload`, `ingress`, or
`system`), including the non-Karpenter bootstrap node, so one label selector
covers all four workload nodepools across both architectures and both capacity
types without pinning to a nodepool name.

Configuration goes in the existing `postgres-restore-operator-config` ConfigMap,
which is already watched with hot reload for `maxConcurrentRestores`,
`kopiaImage` and `usePortForward`. Placement changes then need no operator
rollout, which matters because a restart interrupts in-flight reconciles.

```yaml
data:
    nodeSelector: "bes.node.purpose=workload"
    podAnnotations: "karpenter.sh/do-not-disrupt=true"
```

- Add `extract_node_selector` and `extract_pod_annotations` to
  `src/bin/operator.rs`, following the shape of the existing extractors. Values
  parse as comma-separated `k=v`; a malformed pair is warned about and skipped
  rather than failing the whole key.
- Store both on `Context` behind `Arc<RwLock<BTreeMap<String, String>>>`, matching
  `kopia_image`. The ConfigMap watcher updates them live and reverts to empty
  when the key or the ConfigMap disappears.
- Apply to all seven pod builders: `build_postgres_deployment_with`,
  `build_restore_job`, `build_schema_migration_job`, `build_migration_job`,
  `build_snapshot_list_job`, `build_credential_reset_job`,
  `build_version_detect_job`.
- Merge pod annotations with any the builder already sets, and with
  `spec.podAnnotations`; the replica spec wins on key collision.
- Document both keys in `operator.yaml`, whose ConfigMap is currently `data: {}`.
- Tests: extraction for valid, empty, malformed-pair, missing-key and
  missing-ConfigMap inputs; one per builder asserting the selector and
  annotations land on the pod spec and are absent when unconfigured; the watcher
  path updating `Context`.

No new CRD field. A per-replica `spec.nodeSelector` would be unpopulatable —
canopy hardcodes the scheduling fields to `None` and re-asserts the spec on every
tick — so it waits until something needs it. `spec.affinity` and
`spec.tolerations` keep working and compose with the operator-wide selector.

Applying `do-not-disrupt` uniformly across all seven builders is deliberate: a
50-minute kopia restore is no more disposable mid-flight than the serving
postgres pod.

## Expected consequences

Restore Jobs currently ride free on an existing bootstrap node; forcing them onto
the workload tier means Karpenter provisions workload capacity for them or they
pack onto existing workload nodes. Combined with Part 1, the two largest replicas
requesting roughly 11Gi each will need real workload capacity — budget for two to
three `m*.xlarge` rather than one.

Spot is retained deliberately. `do-not-disrupt` prevents voluntary disruption
(consolidation, drift, expiry) but not spot interruption; the sampled window
contained no spot interruptions, so consolidation is the failure mode worth
closing.

## Out of scope

- `ingress-arm` is untainted at weight 50 and is therefore the default
  destination for every unpinned arm64 pod in the cluster. It should carry a
  taint. That is a nodepool change outside this repo and protects more than pgro.
- The `dbt` persistent schema is never carried across on two of the four
  analytics replicas — every switchover logs "persistent schemas not found on
  source". Needs the analytics team to confirm which schema dbt targets there.
- Custom collations (for example `public.en_numeric`) are never reindexed after
  restore; the post-restore reindex only covers the database default collation
  (`attcollation = 100`).
- `pgro.bes.au/ready-for-traffic` is stamped on the pod by the operator rather
  than set in the Deployment pod template, so a replaced pod leaves the Service
  with no endpoints until the next reconcile.
- Streaming replication and failover. The disruption being fixed here is
  voluntary and removable by configuration; revisit if unplanned node loss
  remains a problem afterwards, and prefer handing the postgres lifecycle to
  CloudNativePG over reimplementing promotion.
