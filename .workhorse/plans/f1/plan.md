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

## Chosen shape: a long-lived migrating intent that reuses analytics

Rather than grow the throwaway `upgrade` intent into a second parametrised
long-lived intent (duplicating analytics' whole param plumbing), the persistent
"upgraded query replica" is **analytics with a migration target**. A new intent
reuses the existing `analytics_param_schema()` and analytics-shaped config, and
adds the `migrate` (and `url`) semantics so canopy names a version for it.

This is cheap because **pgro's migration path is already intent-agnostic**:

- `to_replica_spec` sets `migrate_to` from the entry's `target_version` /
  `target_version_id` regardless of intent (intent.rs:478). The
  `only_a_named_target_migrates` test confirms an analytics entry carrying a target
  *would* migrate today — canopy just never names one for it.
- The restore controller runs the migration Job whenever `migrate_to` is set, and
  the verification reporter forwards the result keyed by `target_version_id`. None
  of that is `upgrade`-specific.

So the new intent needs almost no new machinery: the migration, persistence
(`ephemeral: false`, inherited from the analytics config), expose (`expose` param),
sizing, `minimum_ttl` throttling and snapshot-following all come from the analytics
path already. The delta is a new descriptor + a `config_for` arm.

### Why a new intent, not a flag on `analytics`

The `migrate` semantic makes canopy **withhold the entry when the server has no
candidate version**. Adding it to the existing `analytics` intent would therefore
stop plain analytics replicas dispatching for any server without a candidate
version — a regression. A separate intent keeps plain `analytics` running
everywhere while the migrating variant only dispatches when a version is named
(which, with an operator-pinned terminal version, it always is).

### What each ask maps to

1. **Keep the replica alive** → the new intent's config is analytics-shaped:
   `ephemeral: false`, long-lived, exposed (via the `expose` param), follows new
   snapshots and re-migrates on each, throttled by `minimum_ttl`. This is what the
   user asked for: a database that stays upgraded and queryable, following the
   latest backup, for running read-only processes against.

2. **Target a particular terminal version** → canopy's, no pgro change. Canopy
   names the version + id on the worklist entry as it does for `upgrade`; its intent
   form currently hides the version field. *Action: coordinate with the canopy team
   to surface that field.* Tracked outside this card / repo.

The existing ephemeral `upgrade` intent stays unchanged — it remains the
restore-test-discard check that runs for every server with a candidate version.

## Remaining decisions

- **Name for the new intent.** `upgrade` is taken by the ephemeral test and
  `analytics` by the plain query replica. Working options: `analytics-upgrade`,
  `upgraded-analytics`. Needs picking (and coordinating with canopy, since canopy
  offers the intent by name).
- **Which analytics params carry over verbatim.** The schema is reused whole, so
  `expose`, `minimum_ttl`, `switchover_grace`, sizing, `storage_size_maximum`,
  `deployment_ready_timeout` all apply. `persistent_schemas` and the `redaction_*`
  params come along too — harmless, and redaction may even be wanted for an
  upgraded replica serving deidentified data. Confirm none should be dropped.
- **Resource floor.** The analytics floor (2 CPU / 2Gi, 8Gi limit) is larger than
  the verify/upgrade floor. The user said the read processes are light and need no
  extra space; decide whether the new intent keeps the analytics floor or takes a
  smaller one.
