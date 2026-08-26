# Upgrade intent: persist the replica and target a version

Design notes for adding two capabilities to the `upgrade` intent: keeping the
migrated replica alive after the upgrade test, and targeting a specific Tamanu
version rather than whatever canopy names next.

## How the upgrade intent works today

The three intents pgro advertises to canopy (`src/controllers/canopy/intent.rs`,
`descriptors()`):

- **verify** — restore the snapshot, prove it comes up, discard it. Semantics
  `check`, `once`. No params.
- **upgrade** — restore, apply the next version's schema migrations to prove the
  upgrade survives this deployment's data, then discard. Semantics `check`,
  `once`, `migrate`. No params.
- **analytics** — long-lived read-only query replica. Semantics `check`, `url`,
  `redact`. Fully parametrised (TTL, expose, resources, redaction, etc).

**Where the version comes from.** pgro does not pick the version. The `migrate`
semantic means canopy names it: each worklist entry carries `target_version` (a
semver string) and `target_version_id` (canopy's uuid for it). In
`to_replica_spec` that pair becomes `migrate_to: MigrationTarget { version,
version_id }` on the replica spec; each restore snapshots it into its own spec at
creation. Canopy withholds the entry entirely when the server has no candidate
version, so "an entry with a target present" is the whole signal to migrate.

**What the migration does.** A restore whose deployment comes up healthy with
`migrateTo` set enters `Migrating` before `Switching`. A Job runs
`ghcr.io/beyondessential/tamanu-central:v{version} migrate` against the restored
database (`src/controllers/restore/migration.rs`), then the outcome is read back
out of the `logs.migrations` audit table tamanu writes itself.

**Why the version_id matters.** The result is reported to canopy keyed by
`target_version_id` (`verification.rs`, `migration_args`), so canopy can join the
outcome back to the exact version it asked about. A raw semver with no id can't be
reported this way — this is the crux of the targeting question below.

**Why it's discarded.** The `upgrade` intent sets `ephemeral: true`
(`config_for`). Once a restore reaches `Active` (postgres healthy and, for canopy
replicas, verification reported), the replica controller tears the restore down
and records `verifiedSnapshotId` (`src/controllers/replica.rs`). The replica CR
stays and only restores again when a newer snapshot is offered.

## Chosen shape: a `migrate_to` parameter on the analytics intent

The persistent "upgraded query replica" is an **analytics replica that migrates to
an operator-chosen version**. The analytics intent gains a `migrate_to` text param
(a Tamanu semver). When set, each restore of that replica migrates to the version;
when unset, it is a plain analytics replica.

This is cheap because **pgro's migration path is already intent-agnostic**:

- `to_replica_spec` sets `migrate_to` on the replica spec, and each restore
  snapshots it; the restore controller runs the migration Job whenever it is set.
  None of that is `upgrade`-specific — the `only_a_named_target_migrates` test
  confirms an analytics replica carrying a target *would* migrate today.
- So persistence (`ephemeral: false`), expose, sizing, `minimum_ttl` throttling and
  snapshot-following all already exist on the analytics path. The delta is reading
  the target from a param and choosing not to report it.

### Why a param, not the entry / `migrate` semantic

Canopy names `upgrade`'s target on the entry (`target_version`) because it
*computes* it — the "next" version. Analytics's target is *operator-chosen*, so it
is a plain param the operator types on the analytics form. Consequences:

- **The `migrate` semantic is untouched.** No `migrate?`, no withholding concern —
  a param does not gate canopy's dispatch. `upgrade` keeps entry-based canopy-named
  targets; analytics reads its param.
- **No canopy change is needed for this card.** Canopy renders advertised params on
  the form automatically, so advertising `migrate_to` in the analytics param schema
  is the whole surfacing story.

### No migration report for analytics

The migration report is the upgrade workflow's version-readiness signal. An
analytics migration is a convenience — an upgraded database to work against — not a
verification, so its outcome is **not** reported to canopy. The report is gated on
the intent: only `upgrade` sends the migration block.

### version_id stays, for now

`MigrationTarget` keeps `version_id` this card (don't touch the upgrade intake or
report). The analytics param path has no canopy version id; since analytics
migrations aren't reported, the id is never read for them, so the analytics target
is built with an empty id as an interim. Removing `version_id` from pgro entirely —
intake and report — is the **follow-up card**, to land once canopy drops the
requirement from the verification contract.

## Implementation checklist (this card)

- [ ] Add `migrate_to` (text) to `analytics_param_schema()` and its
      `params::` name constant.
- [ ] In `to_replica_spec`, build the replica's migration target from the analytics
      `migrate_to` param (empty version id), keeping the existing entry-based target
      for `upgrade`.
- [ ] In `verification.rs`, omit the migration block unless the intent is `upgrade`.
- [ ] Tests: analytics with `migrate_to` migrates and does not report; analytics
      without it is unchanged; `upgrade` still reports.
- [ ] Update the README analytics param table with `migrate_to`.

## Follow-up card (separate)

Once canopy drops the `version_id` requirement from the restore-verification
contract, remove `version_id` from pgro entirely — the `MigrationTarget` intake
from the worklist entry and the field on the verification report — and report the
`upgrade` migration keyed by semver alone. Blocked on the canopy change landing.
