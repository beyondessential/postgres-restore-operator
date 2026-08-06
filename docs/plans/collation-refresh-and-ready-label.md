# Collation refresh, and the ready-for-traffic label

Two fixes to what happens after a restore comes up, both found while
investigating analytics runs that fail on their first attempt.

## Part 1 — the ready-for-traffic label belongs in the pod template

`pgro.bes.au/ready-for-traffic` is stamped onto the pod by the operator rather
than declared in the Deployment's pod template, so any pod the ReplicaSet
replaces — eviction, node loss, OOM — comes up without it and stays out of the
replica Service until the next reconcile patches it back on.

The label's documented purpose is to keep a restore in `Switching` unreachable
while operator-side prep runs. It does not actually do that. `ensure_service`
creates the stable Service with no selector at all, and
`update_service_selector` sets `{restore, ready-for-traffic}` together, only
after prep has finished. The per-restore Service — the one the migration Job
uses — selects on the restore name alone. The restore-name component of the
selector is what gates traffic: while a new restore is switching, the stable
Service either has no selector yet or still names the previous restore.

- Add `READY_FOR_TRAFFIC_LABEL: "true"` to the pod template labels in
  `build_postgres_deployment_with`, so every pod carries it from creation.
- Leave the Service selector and `mark_pod_ready_for_traffic` alone. Removing
  the label mechanism would be the tidier change but is unsafe on a running
  cluster: existing Services carry the two-key selector, and a merge patch
  cannot drop a selector key without an explicit null, so there would be a
  window where pods lacking the label match nothing.
- Update the `READY_FOR_TRAFFIC_LABEL` doc comment, which currently claims the
  label is what keeps a switching restore unreachable.
- Tests: the built Deployment's pod template carries the label; a pod template
  label set is not otherwise disturbed.

## Part 2 — rebuild and refresh indexes on version-mismatched collations

A physical restore lands a data directory on whatever base image the restore
runs, which may carry a different ICU or glibc from the machine the snapshot
came from. Postgres notices and warns per session:

    WARNING: collation "..." has version mismatch
    DETAIL: The collation in the database was created using version X, but the
      operating system provides version Y.
    HINT: Rebuild all objects affected by this collation and run
      ALTER COLLATION ... REFRESH VERSION

Index ordering for that collation may be wrong, which shows up as an index scan
missing rows rather than as an error. Nothing in the operator handles it today:

- The reindex step only runs behind `/pgdata/needs-reindex`, which is set when
  the **locale was rewritten**. A collation version mismatch needs no locale
  rewrite, so replicas that never took that path are never reindexed.
- The index query is `WHERE a.attcollation = 100`, the database default
  collation only, so a user-defined collation is never rebuilt even when the
  reindex does run.
- Nothing ever runs `ALTER COLLATION ... REFRESH VERSION`, so postgres keeps
  warning regardless.

### Detection

In the `setup-auth` init container, alongside the existing collation fix step
(a server is already up there): for every connectable database, count
collations whose recorded version differs from the one the OS reports. Any hits
set `/pgdata/needs-collation-refresh`, following the existing flag-file pattern.

### Repair

A new branch in the post-startup background block, beside the existing
`needs-reindex-all` and `needs-reindex` branches. Per database:

1. Select invalid-ordering candidates: indexes whose collation is one of the
   mismatched ones, excluding `pg_catalog`, `information_schema` and toast
   namespaces by name.
2. Rebuild them, `CONCURRENTLY` where the server supports it, matching the
   existing branch's version handling.
3. Only if every rebuild in that database succeeded, run
   `ALTER COLLATION ... REFRESH VERSION` per mismatched collation and
   `ALTER DATABASE ... REFRESH COLLATION VERSION`.

The catalog exclusion is not optional once the predicate widens. The current
query avoids catalog indexes only incidentally — they carry the C collation, so
`= 100` misses them — and the existing comment records that including them
produced dozens of swallowed errors per database that read like progress.

Refreshing only after a clean rebuild is the point of the failure tracking: the
rebuild loop swallows errors to keep making progress, and refreshing a version
whose indexes did not rebuild would replace a correctness warning with silence.

### Version handling

`pg_collation_actual_version` and `ALTER DATABASE ... REFRESH COLLATION VERSION`
are not available on every server version this operator can restore. Gate in
SQL rather than guessing cutoffs in shell: skip detection when
`to_regprocedure('pg_collation_actual_version(oid)')` is null, and gate the
database-level refresh on `current_setting('server_version_num')::int`. The step
self-disables on older servers instead of erroring.

### Tests

Script-shape assertions, matching how the existing init and postgres-container
scripts are tested:

- Detection writes the flag file and is guarded by the `to_regprocedure` check.
- The repair branch excludes catalog and toast namespaces.
- `REFRESH VERSION` appears only after the rebuild loop, and is conditional on
  the failure counter.
- The database-level refresh is gated on `server_version_num`.
- The existing `needs-reindex` and `needs-reindex-all` branches still run, and
  the stage bookkeeping still ends at `ready`.

## Expected consequences

On a replica whose collations mismatch, the post-Ready background window gets
longer: more indexes are rebuilt than before. It uses the same `CONCURRENTLY`
path already in place and the pod is Ready throughout, so switchover is not
extended — only the background work after it. On a replica with no mismatch the
detection query is the only added cost.

## Out of scope

- Persistent schemas silently carry nothing across when the named schema does
  not exist on the source; every switchover logs it and the replica comes up
  without the schema.
- Clients connected across a switchover keep talking to the outgoing instance
  until it is swept, so writes made after the persistent-schema dump are lost.
