# B1: Arbitrary users with custom permissions

Minimal first cut (see spec `replica/users.md`, id USER). Adds an `extraUsers`
list to the replica: each named role is provisioned once per restore as a
`LOGIN SUPERUSER`, with an operator-generated password in a per-user Secret
`<replica>-user-<name>-creds`. Surfaced to Canopy via the `analytics` intent's
`extra_users` text param.

## Design notes

- **CR field** `extraUsers: []string` on `PostgresPhysicalReplicaSpec`, mirroring
  the `persistentSchemas` shape (array in the CR, comma-separated text in Canopy).
- **Provisioning happens in the restore init (`setup-auth`)**, alongside the
  analytics user — not reconciled, never dropped (matches the USER spec and the
  three-card plan).
- **Passwords reach the init container via env**, one pair per user
  (`EXTRA_USER_NAME_<i>` / `EXTRA_USER_PW_<i>`), sourced from each user's Secret.
- **Role SQL uses psql variables + `format('%I','%L')`** inside a quoted heredoc,
  so an arbitrary username can't break the SQL or the shell.
- **Analytics user wins**: an extra user whose name equals `analyticsUsername`
  is dropped from the list, and duplicates are de-duped.

## Checklist

- [x] Add `extra_users` field + helpers (`extra_users()`, `extra_user_secret_name`) to `types/replica.rs`
- [x] `ensure_extra_user_secrets` on the replica; call it from `reconcile`
- [x] Canopy `extra_users` text param → parse into `extra_users` in `to_replica_spec`
- [x] Deployment builder: env vars + create-or-update SUPERUSER SQL in `setup-auth`
- [x] Update `crds.yaml` and the README CRD table
- [x] Unit tests (intent parse, `extra_users()` dedup/exclude, deployment builder)
- [x] Integration test `tests/extra_users.rs` (matrix entry in `integration.yml` added manually by the user)
- [ ] `cargo fmt` + `cargo clippy` (cannot run locally — must pass in CI)
