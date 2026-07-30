# Report restore progress to canopy

## Problem

A restore of a large snapshot takes ~25 minutes, of which ~95% is the S3
download. Canopy sees nothing between dispatching the run and receiving the
final `RestoreVerification`: a row that says "in progress" with no figures and
no indication of whether it is moving or wedged.

Canopy added `POST /backup-progress` for this. bestool already reports against
it; pgro does not.

## What canopy expects

`ProgressArgs` requires `run_id` and `type`. `purpose` distinguishes `Backup`
from `Restore`.

Every counter is **cumulative from the start of the run**, never an interval
delta, so a dropped or repeated sample costs resolution but never the accuracy
of a total. Counters that are not measured must be **omitted rather than sent
as zero**. Canopy timestamps each sample on receipt.

## What bestool does

`crates/bestool/src/actions/canopy/backup/progress.rs` runs a `ProgressReporter`
background task on a fixed 30s interval. Failures are logged and the run carries
on. On stop the task is aborted rather than drained, so a slow post can never
delay the run's completion.

Its counters come from two places: kopia's own progress line, and the re-signing
proxy's live S3 tallies. **For restores it reports the S3 tallies only** — the
restore path passes no engine cell, because kopia's restore output is a
different shape from the backup status line bestool parses, and "bytes received"
is the is-it-moving signal that matters.

pgro follows the same choice, for the same reason plus one more: S3 bytes are a
download measure, not a completion measure. Content served from the local kopia
cache advances the restore without moving the counter, so on a partly-cached
re-restore the figure understates. This is deliberately a liveness signal, not a
percentage.

## Design

The traffic counters live in the canopy-proxy sidecar, inside the restore Job
pod. The canopy client lives in the operator, and the sidecar holds no canopy
credentials — it reaches S3 credentials through the operator's broker. So
samples go sidecar → operator → canopy, which keeps canopy credentials in the
operator alone and reuses the push direction that already exists.

```
restore Job pod                        operator                    canopy
┌──────────────────────┐
│ canopy-proxy sidecar │ every 30s  ┌──────────────────┐
│  proxy.traffic()     │ ─────────► │ POST /api/v1/    │ ProgressArgs
│  + run_id + type     │            │  canopy-progress │ ─────────────► POST
└──────────────────────┘            │  /{ns}/{job}     │            /backup-progress
        │                           └──────────────────┘
        │ on SIGTERM (unchanged)
        └──────────────────────────► /api/v1/canopy-stats/{ns}/{job}
                                       → final RestoreVerification
```

The sidecar already has `PGRO_RUN_ID` and `PGRO_TYPE` in its config, so a sample
is self-describing and the operator needs no Kubernetes lookup per post.

`proxy.traffic()` is a cheap repeatable snapshot (`self.traffic.snapshot()`), so
sampling on a timer needs nothing new from `bestool-kopia`.

The existing exit-time stats callback is left alone: the `RestoreVerification`
path does not change.

## Work items

### 1. Bump `bestool-canopy` to 0.7.8

Needed for `backup_progress` and `ProgressArgs`. This crate generates its types
at build time from canopy's OpenAPI, so a bump can surface unrelated schema
drift — keep it as its own commit so that drift is separable from the feature.

### 2. Sidecar posts progress samples

- `src/bin/canopy_proxy.rs`: read a new `PGRO_PROGRESS_CALLBACK_URL` (optional —
  absent disables sampling). Spawn a task alongside the shutdown wait that every
  30s samples `proxy.traffic()` and POSTs the cumulative totals plus `run_id`
  and `type`.
- Skip entirely when `run_id` is absent: `ProgressArgs` requires it, and its
  absence is also what distinguishes a non-canopy run.
- The task is never awaited by the shutdown path, so a slow or hanging post
  cannot delay the run.

### 3. Operator forwards to canopy

- `src/canopy.rs`: add a `backup_progress` wrapper, matching the existing
  one-line-forward style of the other `restore_*` methods.
- `src/bin/operator.rs`: add `POST /api/v1/canopy-progress/{namespace}/{job}`
  next to the existing `canopy-stats` route. Build `ProgressArgs` with
  `purpose: Restore` and the four `s3_*` counters; omit every other counter.
  No `snapshot_taken_at` — a restore has no freeze moment.
- Failures are logged, never escalated.

### 4. Wire the callback URL through

- `src/context.rs`: add a `canopy_progress_callback_url` builder beside
  `canopy_stats_callback_url`.
- `src/controllers/restore/builders.rs`: carry it on `CanopyProxyArgs` and set
  the env var on the sidecar container.

## Testing

Unit tests mirroring bestool's:

- a traffic sample maps onto the right `ProgressArgs` fields
- counters pgro does not measure come back `None`, not `Some(0)`
- a sample with no `run_id` produces no post

No integration test: this is telemetry with no observable effect on the restore.

## Out of scope

- Parsing kopia's restore progress line for `bytes_estimated` / `files_*`. It
  would give canopy a true percentage and ETA rather than a liveness signal, but
  it needs a parser pgro would have to write itself (bestool's handles only
  backup lines, and is unpublished), and a way to get the line out of the Job
  pod. Worth revisiting if the liveness signal proves insufficient.
- Progress for the legacy (non-canopy) path. There is no sidecar there and no
  canopy run to report against.
