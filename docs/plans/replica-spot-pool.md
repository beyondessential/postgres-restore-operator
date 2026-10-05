# Replicas on a dedicated memory-dense spot pool

## Background

Analytics replicas each occupy a whole 4 vCPU / 16 GiB on-demand node while using a few percent of its CPU.
Three things put them there:

- The `analytics` intent floor requests 2 CPU, so two replicas never share a node and the request alone rules out anything smaller than an xlarge.
- The operator-wide placement stamps only a `nodeSelector` and annotations. Replica pods never tolerate the spot taint, and Karpenter treats an untolerated `PreferNoSchedule` taint as a hard filter until every other relaxation has failed, so the spot pool is never chosen for new capacity.
- The only on-demand workload shape is `m*g.xlarge` (1:4), while replica demand is roughly 1 vCPU per 16 GiB.

The target is one replica per node on memory-dense Graviton spot (`x8g.medium`, `x8g.large`, `r6g.large` as a diversification fallback, all with low interruption rates in the region), falling back to on-demand on insufficient capacity.
One replica per node keeps today's behaviour under `do-not-disrupt` and the per-cycle pod replacement: no packing to fragment.

## pgro

1. Spec: add `.workhorse/specs/operator/placement.md` describing the operator-wide scheduling defaults from the ConfigMap (`nodeSelector`, `podAnnotations`, and the new `tolerations`), applied to every pod the operator builds, with existing keys on the pod winning.
2. Tolerations in `PodPlacement` (`src/placement.rs`): a `tolerations` ConfigMap key, comma-separated `key=value:Effect` (operator `Equal`) or `key:Effect` (operator `Exists`), malformed entries warned and skipped like the other keys. Appended to the pod's tolerations unless an identical toleration is already present. Plumb through `src/bin/operator.rs` alongside the existing two keys. Tests for parsing and application.
3. `analytics` CPU floor: lower the request in `config_for("analytics")` from `2` to `500m`, still with no CPU limit. Update the intents spec if it states the figure, and the descriptor text if it advertises it.
4. Fix `pinned_resources`: a canopy `cpu_request` / `cpu_limit` alone must not pin memory. Today it fills memory from the floor's request and limit separately, which drops the snapshot-derived memory and breaks request == limit. CPU-only parameters should instead override the CPU in the floor, leaving memory derivation untouched; memory parameters keep pinning as now. Tests for both paths.

## ops

1. Spec (`docs/spec/kubernetes/nodes.md`): a new requirement for a dedicated replica pool (memory-dense shapes, spot preferred with on-demand fallback, dedicated by a `NoSchedule` taint), and an exception in `k[capacity.workload-node-floor]` for dedicated pools, whose shapes are sized to the workload that opts in rather than for amortisation across many pods. `tracey bump` and re-verify affected annotations.
2. Node class `db-replica`: workload subnets, the workload disk size, `kubelet.maxPods: 15` to reclaim kubelet memory reservation (so a 12 Gi replica fits a 16 GiB node), and distinctive `bes.node.purpose`-style instance tags for cost attribution.
3. NodePool `db-replica-arm`: purpose `workload`, arch `arm64`, `capacityTypes: ['spot', 'on-demand']`, instance types `x8g.medium`, `x8g.large`, `r6g.large`, spot taint plus `bes.node.group=db-replica:NoSchedule`, `WhenEmpty` consolidation, modest limits.
4. pgro stack ConfigMap (`pulumi/data/pgro/src/manager.ts`): `nodeSelector: bes.node.group=db-replica`, `tolerations: bes.node.group=db-replica:NoSchedule,spotInstance=true:PreferNoSchedule`, keep `podAnnotations`. This must ship together with an operator image that understands `tolerations`; on an older image the pods would select a tainted pool they don't tolerate and sit Pending.

## Out of band

- Canopy: set `cpu_request` to `1` on the one replica with sustained daytime query load, so it lands on an `x8g.large` rather than a 1-vCPU node. Depends on pgro step 4.
- Release pgro, bump the image in the pgro stack, preview and apply ops.
