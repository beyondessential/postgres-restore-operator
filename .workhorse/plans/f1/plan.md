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

## The two asks

### 1. Keep the replica alive after the upgrade — pgro

`upgrade` grows a single boolean param, `ephemeral`, **defaulting to true** so the
existing "restore, test, discard" behaviour is unchanged. Setting it false
materialises `PostgresPhysicalReplicaSpec.ephemeral = false`, so the replica
controller keeps the migrated restore running instead of tearing it down once it
reaches `Active`.

Decisions that follow from the "run read processes against an upgraded database"
use case:

- **Exposed.** A persisted upgrade replica is reachable on the tailnet, just like
  an analytics replica. This means the `upgrade` intent gains the `url` semantic,
  and pgro sets the expose annotations + reports the replica URL whenever the
  replica is persisted (`ephemeral = false`). Exposure is tied to persisting, not
  a separate param — an ephemeral upgrade has no lasting URL to expose.
- **Sizing unchanged.** The processes run against it are read-only and light, so
  it keeps the verify/upgrade resource floor (250m / 512Mi, 2Gi limit). No bump.
- **Follows new snapshots.** With `ephemeral = false` the replica behaves like a
  normal long-lived replica: each new snapshot canopy offers is restored,
  re-migrated to the target version, and switched over to. See the open question
  below — this re-runs the upgrade on every new backup.

The purpose is running (read-only) processes against an already-upgraded database,
not pointing a staging app at it.

### 2. Target a particular terminal version — canopy, no pgro change

Version selection is canopy's, and the report is keyed by canopy's
`target_version_id`, so pgro needs no change here. Canopy already names the version
+ id on the worklist entry; its intent form currently hides the version field. The
work is to have canopy surface that field so an operator can pin a terminal
version, after which the existing pgro path runs and reports it unchanged.

*Action: coordinate with the canopy team to expose the version field on the intent
form.* Tracked outside this card / repo.

## Open question (for the user)

- A persisted upgrade replica following new snapshots re-runs the whole migration
  on every new backup (potentially hours, and canopy re-reports each). Is that the
  intent — always the latest backup, freshly upgraded — or should it stay **pinned**
  to the one snapshot it first upgraded, giving a stable database to work against?
  If it should follow, a `minimum_ttl` to throttle re-migration is worth adding.
