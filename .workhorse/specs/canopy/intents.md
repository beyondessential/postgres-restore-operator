---
id: INT
---

# Canopy restore intents

pgro advertises a set of restore intents to canopy at startup. Each intent names
what a restored replica is for, the canopy semantics it opts into, and, where
parametrised, the fields an operator fills in. Canopy dispatches only intents pgro
advertises, collects their declared parameters on the worklist entry, and applies
the behaviours of the semantics it recognises.

## The intents

- [ ] pgro advertises three intents: `verify`, `upgrade`, and `analytics`.
- [ ] `verify` restores a snapshot, proves it comes up healthy, and discards it. It
  takes no parameters.
- [ ] `upgrade` restores a snapshot, applies a target version's schema migrations to
  prove the upgrade survives the deployment's own data, and discards it.
- [ ] `analytics` keeps a long-lived read-only query replica restored from the
  latest snapshot, following new snapshots as they arrive. It is parametrised
  (restore cadence, exposure, resource sizing, storage caps, redaction, and a
  migration target).

## Semantics

Semantics are carried as plain strings. Canopy acts only on the ones it recognises
and stores the rest untouched, so pgro can advertise a capability before canopy
supports it.

- [ ] `check` — the intent produces a restore-health report that canopy holds to
  its overdue bound.
- [ ] `once` — a given snapshot is dispatched at most once; canopy suppresses the
  entry once that snapshot has a healthy report.
- [ ] `url` — the health report carries a link to the running replica, which canopy
  surfaces to operators.
- [ ] `migrate` — canopy names a target version on the entry, and the restore is
  migrated to it. Canopy withholds the entry entirely when the server has no
  candidate version, so an intent carrying `migrate` runs only where there is a
  version to aim at.
- [ ] `redact` — the intent may de-identify the restored data before serving it.
- [ ] `verify` opts into `check` and `once`.
- [ ] `upgrade` opts into `check`, `once`, and `migrate`.
- [ ] `analytics` opts into `check`, `url`, and `redact`.

## Migrating a restore to a target version

A restore can apply a target version's schema migrations once its database is
healthy and before any switchover. Two intents set a target, by different routes.

- [ ] `upgrade`'s target is named by canopy on the worklist entry, via the
  `migrate` semantic — the version canopy computes the deployment should prove it
  can move to.
- [ ] `analytics`'s target is the operator's `migrate_to` parameter, a Tamanu
  version the operator chooses for that replica.
- [ ] When a restore has a target version by either route, it applies that
  version's schema migrations once the database is healthy and before any
  switchover.
- [ ] `upgrade` reports the migration outcome to canopy against the version it
  targeted, as a version-readiness signal. Backup health and version readiness stay
  separate signals: a restore whose backup came up healthy but whose migrations
  failed reports the failing migration as the verdict, not a failed restore.
- [ ] The `analytics` migration outcome is not reported to canopy. An analytics
  replica migrates to give the operator an upgraded database to work against, not to
  verify a version, so it carries no version-readiness signal.

## Upgrade replica: a persistent, migrated analytics replica

An operator who wants an upgraded database to run read-only processes against sets
the `migrate_to` parameter on an analytics replica. The result is an analytics
replica that stays upgraded and queryable rather than a throwaway upgrade check.

- [ ] The `analytics` intent advertises a `migrate_to` text parameter: the Tamanu
  version to migrate the replica to.
- [ ] An analytics replica with `migrate_to` set migrates each restore to that
  version and keeps the migrated replica running, reachable on the tailnet when the
  replica is exposed, the same as any analytics replica.
- [ ] It follows new snapshots like any analytics replica, re-migrating each restore
  to the target version, with restore cadence throttled by the replica's minimum
  TTL.
- [ ] An analytics replica with `migrate_to` unset is a plain query replica, with no
  migration step.
- [ ] A migrated analytics replica draws on the same parameters as any analytics
  replica for exposure and resource sizing; no separate configuration surface is
  introduced for it.
