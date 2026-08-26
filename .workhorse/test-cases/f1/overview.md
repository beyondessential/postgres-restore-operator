# Upgrade intent: persist replica and target a version

Scenarios verifying the `analytics` `migrate_to` param — a persistent, migrated
query replica — and that only `upgrade` reports its migration outcome.

## Analytics migration target

- [x] An analytics replica with `migrate_to` set builds a migration target for that
  version, with an empty canopy version id (verifies spec: INT).
- [x] An analytics replica with `migrate_to` blank, whitespace, or unset builds no
  migration target and is a plain query replica (verifies spec: INT).
- [ ] An analytics replica with `migrate_to` set keeps running after the migration
  and is reachable on the tailnet when exposed (verifies spec: INT).
- [ ] The replica re-migrates to the target version on each new snapshot it follows,
  throttled by its minimum TTL (verifies spec: INT).
- [ ] A migrating analytics restore is built read-write so the DDL can run, the same
  as a `persistentSchemas` replica.

## Migration reporting

- [x] The `upgrade` intent reports its migration outcome to canopy against the
  targeted version (verifies spec: INT).
- [x] An analytics migration outcome is not reported to canopy (verifies spec: INT).
- [x] A non-migrating intent reports no migration block.
- [x] `upgrade` reports no migration block until the migration Job has run and its
  result has been read back.
