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
- [ ] An optional-migrate semantic behaves like `migrate` but does not withhold:
  canopy names a target version when an operator has set one, and otherwise
  dispatches the entry with no target. (Working token `migrate?`; the exact string
  is canopy's contract to settle.)
- [ ] `redact` — the intent may de-identify the restored data before serving it.
- [ ] `verify` opts into `check` and `once`.
- [ ] `upgrade` opts into `check`, `once`, and `migrate`.
- [ ] `analytics` opts into `check`, `url`, `redact`, and the optional-migrate
  semantic.

## Migrating a restore to a target version

A restore whose entry names a target version applies that version's schema
migrations, proving the upgrade against real data and reporting how it went.

- [ ] When an entry carries a target version, the restore applies that version's
  schema migrations once the database is healthy and before any switchover, then
  reports the outcome alongside the replica's health.
- [ ] The migration runs whenever a target version is present on the entry,
  independent of which intent dispatched it.
- [ ] The target version is carried on the worklist entry itself — the version plus
  canopy's identifier for it — not as an intent parameter. The migration outcome is
  reported to canopy keyed by that identifier, so canopy joins the result to the
  version it asked about. A bare version string with no identifier cannot be
  reported this way, which is why the target does not travel as a parameter.
- [ ] Backup health and version readiness are separate signals: a restore whose
  backup came up healthy but whose migrations failed reports the failing migration
  as the verdict, not a failed restore.

## Upgrade replica: a persistent, migrated analytics replica

An operator who wants to keep an upgraded database to run read-only processes
against sets a target version on an analytics replica. The result is an analytics
replica that stays upgraded and queryable rather than a throwaway upgrade check.

- [ ] An analytics replica with a target version set migrates each restore to that
  version and keeps the migrated replica running, reachable on the tailnet when the
  replica is exposed, the same as any analytics replica.
- [ ] It follows new snapshots like any analytics replica, re-migrating each restore
  to the target version, with restore cadence throttled by the replica's minimum
  TTL.
- [ ] An analytics replica with no target version set is a plain query replica, with
  no migration step.
- [ ] A migrated analytics replica draws on the same parameters as any analytics
  replica for exposure and resource sizing; no separate configuration surface is
  introduced for it.
