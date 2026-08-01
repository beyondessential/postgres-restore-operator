---
name: pgro-status
description: Check the operational health of a postgres-restore-operator deployment and the replicas/restores it manages. Use when the user asks to "check the replicas", "status check", "is pgro healthy", or similar — anything about pgro operational state in a live cluster.
---

# pgro-status

A standard workflow for inspecting a running postgres-restore-operator and its `PostgresPhysicalReplica` / `PostgresPhysicalRestore` resources via `kubectl`.

## Preconditions

- `kubectl` is configured with a context that can see the namespace where pgro is installed (commonly `pgro-system`) and the namespaces holding the replica CRs.
- If the user's `kubectl` is split across contexts, **confirm the active context first** — pointing at the wrong cluster is the most common reason a check looks "broken" when it isn't.

If the API server is unreachable (timeouts, DNS failures, etc.), report the underlying error and stop; the rest of the workflow won't work.

## Workflow

Run two phases: **overview** (one query per resource kind, single pass) and **per-replica detail** (one targeted block per replica that looks anomalous).

### Phase 1 — overview

```bash
kubectl get deployment -n pgro-system postgres-restore-operator \
  -o jsonpath='image: {.spec.template.spec.containers[*].image}{"\n"}'
kubectl get pods -n pgro-system
kubectl get postgresphysicalreplicas.pgro.bes.au -A
kubectl get postgresphysicalrestores.pgro.bes.au -A
kubectl get pods -A --field-selector=status.phase=Pending --no-headers | wc -l
```

Look for:

- Operator image version and pod age. A recent restart means operator logs only cover the post-restart window, which limits debugging.
- Any replica not in `Ready` phase.
- Restore objects: each replica should have **exactly one** `Active` restore in steady state. A transient `Pending` / `Restoring` / `Ready` / `Switching` restore is normal during a cycle. More than one `Active` indicates the sweep isn't pruning.
- Pending pod count > 0 is worth digging into before reporting healthy — could be a scheduling problem (Karpenter, taints, resource pressure).

**A `Ready` phase replica is not necessarily healthy.** `Ready` only means the operator's switchover state machine is at rest — the previous restore is still serving traffic. If `consecutiveRestoreFailures > 0` and growing, *every restore attempt since the last good one has failed*, so the data is staler than its `lastRestoreCompletedAt` claims. To users, "the replica isn't working" usually means the data is days behind, not that connections are refused. Always cross-check `consecutiveRestoreFailures` against `lastRestoreCompletedAt` and the replica's expected cadence before calling a `Ready` replica healthy.

### Phase 2 — per-replica detail

For each replica that looks off — and whenever a thorough check is requested — fetch the key status fields and conditions:

```bash
NS=<replica-namespace>
kubectl get postgresphysicalreplica.pgro.bes.au -n "$NS" replica \
  -o jsonpath='phase: {.status.phase}{"\n"}currentRestore: {.status.currentRestore}{"\n"}previousRestore: {.status.previousRestore}{"\n"}lastRestoreCompletedAt: {.status.lastRestoreCompletedAt}{"\n"}nextScheduledRestore: {.status.nextScheduledRestore}{"\n"}consecutiveRestoreFailures: {.status.consecutiveRestoreFailures}{"\n"}schemaMigrationPhase: {.status.schemaMigrationPhase}{"\n"}'
kubectl get postgresphysicalreplica.pgro.bes.au -n "$NS" replica \
  -o jsonpath='conditions:{"\n"}{range .status.conditions[*]}  {.type}: {.status} ({.reason}){"\n"}{end}'
```

To enumerate the namespaces holding replicas, list them once with:

```bash
kubectl get postgresphysicalreplicas.pgro.bes.au -A \
  -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}'
```

then iterate.

## Signals to flag

| Signal | What it usually means |
|---|---|
| `consecutiveRestoreFailures > 0` | One or more recent failures; check operator logs for failure mode |
| `consecutiveRestoreFailures` growing across checks | Persistent failure mode — investigate root cause |
| `lastRestoreCompletedAt` far older than the cron schedule | Either upstream isn't producing new snapshots, or every recent attempt failed |
| `phase: Restoring` for > 30 min | kopia restore Job pod is slow or stuck; check the Job pod logs |
| `phase: Switching` for > 30 min | Schema migration likely stuck, or postgres Deployment not coming Ready |
| `RestoreCreationBlocked: True` | Too-many-restores guardrail tripped — sweep isn't pruning |
| `RestoreSchedulingSuspended: True` | Legacy condition (older operator versions only — suspension has been removed); means failure backoff is in effect |
| `SchemaMigrationPartial` events on the replica | Migration succeeded but some statement errors were tolerated; some objects may need regenerating upstream |

## Investigating failures

When `consecutiveRestoreFailures` is non-zero or a restore is stuck, bucket recent failure types from operator logs:

```bash
kubectl logs -n pgro-system deployment/postgres-restore-operator --tail=800 > /tmp/op.txt
grep -aE '"level":"(WARN|ERROR)"' /tmp/op.txt \
  | grep -aoE '"message":"[^"]+"' \
  | sort | uniq -c | sort -rn | head -10
```

Filter to a specific replica:

```bash
grep -aE "$NS" /tmp/op.txt \
  | grep -aoE '"message":"[^"]+"' \
  | sort | uniq -c | sort -rn | head -10
```

Events for the replica's namespace (Kubernetes default TTL ~1h, so only recent activity):

```bash
kubectl get events -n "$NS" --sort-by=.lastTimestamp | tail -20
```

For an in-flight kopia restore Job, the pod log shows live progress (download bytes, throughput, ETA):

```bash
POD=$(kubectl get pods -n "$NS" \
  -l pgro.bes.au/restore=<restore-name>,job-name -o jsonpath='{.items[0].metadata.name}')
kubectl logs -n "$NS" "$POD" --tail=20
```

For a postgres Deployment that's not coming Ready after kopia finishes, check init container logs (`fix-locale`, `setup-auth`) and the postgres container itself:

```bash
DEPLOY=<active-or-switching-restore-name>
kubectl logs -n "$NS" deployment/"$DEPLOY" -c setup-auth --tail=80
kubectl logs -n "$NS" deployment/"$DEPLOY" --tail=80
```

## Reporting back

Keep summaries short and high-signal. A small table covering replica / phase / last-restore-age / failure count is usually enough. Examples of how to phrase:

- "All replicas healthy" — one line, no further detail needed.
- "X replicas healthy, Y need attention" — followed by a short table only for the Y.
- For a flagged replica: name it, give the specific anomaly, point to the next step (Job logs / events / etc.).

Do **not** include cluster/site/customer identifiers, real data sizes, or production failure counts in commit messages or PR bodies if working in a public repo — keep operational specifics in the conversation with the user.
