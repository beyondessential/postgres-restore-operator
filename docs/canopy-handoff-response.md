# PGRO sign-off on canopy's restore-replicas response

**From:** pgro
**To:** canopy
**Re:** `canopy/docs/plans/pgro-restore-replicas-canopy-response.md`
**Status:** signed off. Canopy may freeze wire shapes and proceed with
PR1 + PR2 as described in §6 of the response.

## What pgro accepts

- **§1 corrections.** Noted: four device roles (untrusted included);
  role column is plain `TEXT`, no migration; `releaser`-style operator
  trust-promotion replaces the imagined "cert-issuance flow"; reason 7
  in §4.4.1 of the handoff was wrong (`run_id` isn't shared between
  issuance and report) — the conclusion stands on the other six;
  `raise_group_event` + `restore-verification` ref already merged.
- **§2.1 — canopy supplies the snapshot id.** pgro will not list the
  repo to discover what to restore; it consumes the snapshot id from
  the worklist entry.
- **§2.2 — per-server, not per-group.** Worklist entries, restore
  targeting, and `/restore-verification` reports are per-server.
  Credentials remain per-(group, type).
- **§3 — the inversion, fully.** Canopy is the source of truth for
  *which replicas should exist*, including `analytics` and
  `disaster-recovery` intents — not just `verify`. Operator UX is
  canopy's, not k8s-CRD's. The boundary canopy proposed (canopy owns
  *what/why/how-fresh*; pgro owns *how*) is the contract.

## What this changes on pgro's side (for canopy's awareness)

These are pgro-internal consequences of accepting the inversion; canopy
doesn't need to do anything with them, but they shape what pgro will
build later.

- **pgro becomes a worklist-reconciler.** `GET /restore-worklist` is
  the only desired-state input. No CRD for canopy-backed replicas.
- **Cluster state is pgro's runtime model.** Each replica is a
  labelled k8s `Namespace`
  (`pgro/declaration-id`/`group`/`server`/`type`/`intent`), with
  per-replica status in annotations
  (`last-restored-snapshot-id`, `last-restored-at`,
  `restore-state`). On boot pgro discovers via
  `LIST Namespaces label=pgro/managed-by=pgro` and reconciles against
  the worklist. No intermediate CR layer.
- **Legacy `kopiaSecretRef` CRD path coexists.** Pre-canopy replicas
  keep their existing CRDs and the existing CR-driven controllers;
  the canopy-backed path is greenfield and uses the worklist model.
  No migration tool — operators decommission old CRDs as they convert
  declarations into canopy.

## Resolved: unsupported intents (was: pgro's open question)

Canopy took the structured route in §7 of the response: pgro registers
its supported intents via `POST /restore-capabilities` on startup and
on change; canopy persists them, dispatches only matching worklist
entries, and surfaces stranded declarations as operator-facing
configuration *gaps* rather than restore-health incidents. The right
call — conflating capability mismatch with "backup unrestorable" would
page operators for what is a configuration concern.

Consequences pgro will implement:
- `restore_capabilities` registration on operator startup, re-pushed
  on any change to the supported intent set.
- `/restore-verification` outcome stays `success`/`failure` only; no
  `unsupported` variant.
- A fifth bestool addition: `CanopyClient::restore_capabilities(base,
  &[intents])` in Appendix A.

## What canopy is unblocked to do

- Freeze the §4 wire shapes (now per-server with canopy-supplied
  snapshot id).
- Build PR1 (`backup-restore` role + declared-replica model + operator
  UI + `GET /restore-worklist` + `POST /restore-credentials`).
- Build PR2 (`backup_restore_checks` + `POST /restore-verification` +
  alert routing + freshness sweep).
- Restate Appendix A and hand off to bestool.

When PR1 + PR2 + bestool's additions are merged, ping pgro. pgro will
then rewrite `pgro/docs/canopy-backup-integration.md` against the
as-shipped surfaces and start building.
