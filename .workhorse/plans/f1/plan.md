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

### 1. Keep the replica alive after the upgrade

This is pgro-side. `upgrade` currently takes no params; it would gain a boolean
(working name `persist`) that, when set, materialises `ephemeral: false` so the
migrated restore is not torn down after the test passes.

Flipping `ephemeral` is the small part. The open question is what a kept-alive
upgrade replica *is*, since `upgrade` is built as a throwaway:

- **Re-restore behaviour.** analytics restores the latest snapshot repeatedly and
  switches over. An upgrade replica migrated to a fixed version probably wants to
  stay pinned on the snapshot it upgraded, not chase new snapshots — otherwise
  each new snapshot re-runs the whole migration. Needs deciding against the `once`
  semantic.
- **Access.** `upgrade` has no `url` semantic and no `expose` param, so a
  kept-alive replica is reachable only in-cluster. If the point is to poke at the
  upgraded database, it may want exposing like analytics.
- **Sizing.** `upgrade` uses the verify resource floor (250m / 512Mi), sized to
  prove-it-boots, not to serve queries.

### 2. Target a particular terminal version

Version selection is canopy's, and the report is keyed by canopy's
`target_version_id`. So this half is mostly a **canopy** decision, not pgro:

- If canopy grows a per-entry "pin this version" setting, it just names that
  version + id on the worklist entry and pgro needs no change at all.
- If instead an operator sets a raw target version in pgro (a text param on the
  `upgrade` intent), pgro has a version to run but no `version_id` to report
  against, so the migration outcome can't be joined back in canopy. That's a real
  gap that needs a reporting story before it's viable.

## Open questions (for the user)

1. Is the version targeting meant to be a **canopy** feature (canopy pins and
   names the version, pgro unchanged) or a **pgro** override? If pgro, how should
   the result be reported without a canopy version id?
2. What is the kept-alive replica *for* — manual poking, staging app, data
   inspection? That decides expose / sizing / re-restore.
