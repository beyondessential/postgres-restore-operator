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

## Chosen shape: a migration target on the existing analytics intent

The persistent "upgraded query replica" is just an **analytics replica that also
migrates to a target version**. Rather than a new intent duplicating analytics'
field set, the analytics intent gains the ability to carry a migration target: the
operator sets a "migrate to this Tamanu version" field on the analytics form, and
that analytics replica runs the version's migrations after each restore.

This is cheap because **pgro's migration path is already intent-agnostic**:

- `to_replica_spec` sets `migrate_to` from the entry's `target_version` /
  `target_version_id` regardless of intent (intent.rs:478). The
  `only_a_named_target_migrates` test confirms an analytics entry carrying a target
  *would* migrate today — canopy just never names one for it.
- The restore controller runs the migration Job whenever `migrate_to` is set, and
  the verification reporter forwards the result keyed by `target_version_id`. None
  of that is `upgrade`-specific.

So persistence (`ephemeral: false`), expose, sizing, `minimum_ttl` throttling,
snapshot-following and the migration + reporting all already exist on the analytics
path. When no target is set the analytics replica behaves exactly as today (no
`Migrating` phase); when one is set it re-migrates on each new snapshot it follows.

**The target still rides on `entry.target_version` / `entry.target_version_id`,
not a params entry** — canopy resolves the operator's chosen version to its version
id and populates those top-level fields (as it does for `upgrade`). That keeps the
verification report keyed by a real version id, which an operator-typed raw semver
param could not provide.

### The `migrate` semantic: needs an optional variant

The blocker is canopy's handling of the `migrate` semantic: it **withholds the
entry when the server has no candidate version**. That withholding is *correct* for
the ephemeral `upgrade` intent — an upgrade test with no version to aim at has
nothing to prove — so `migrate` must keep it there. Analytics needs the opposite:
migrate *if* a version is set, but dispatch normally when it isn't.

So introduce a distinct optional-migrate semantic: `migrate?`.

- `upgrade` keeps `migrate` (mandatory; withhold when no candidate version).
- `analytics` gains `migrate?` (optional; name a version if the operator set one,
  otherwise dispatch as a plain analytics replica).

pgro's only change is declaring the new semantic on the analytics descriptor.
Canopy owns the two behaviours and surfaces the version field on the analytics
form.

### What each ask maps to

1. **Keep the replica alive** → inherent to analytics (`ephemeral: false`,
   long-lived, exposed via the `expose` param, follows new snapshots throttled by
   `minimum_ttl`). Nothing new: setting a migration target on an analytics replica
   gives a database that stays upgraded and queryable, following the latest backup,
   for running read-only processes against.

2. **Target a particular terminal version** → canopy's, via the new form field +
   the `migrate?` semantic. pgro reads the resulting target through the existing
   path.

The existing ephemeral `upgrade` intent stays unchanged — it remains the
restore-test-discard check gated by mandatory `migrate`.

## Canopy-side work (outside this repo)

- Surface a "migrate to Tamanu version" field on the analytics intent form, and
  resolve it to `target_version` + `target_version_id` on the worklist entry.
- Give the `migrate?` semantic optional behaviour (migrate if a version is set,
  don't withhold otherwise).
