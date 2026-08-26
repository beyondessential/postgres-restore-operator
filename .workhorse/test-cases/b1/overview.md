# B1: Arbitrary users with custom permissions

Scenarios verifying the `extraUsers` feature (spec: USER). The unit cases run in
`cargo test`; the end-to-end case runs in the `extra_users` integration matrix
entry (needs a cluster with MinIO + kopia, so CI only).

## Canopy intent

- [x] `extra_users` text param splits a comma-separated list into the replica's `extraUsers` (verifies spec: USER)
- [x] unset or blank `extra_users` leaves the list empty (verifies spec: USER)
- [x] the `analytics` intent advertises `extra_users` as a `text` param

## Replica helpers

- [x] `extra_users()` trims, de-dupes, and drops a name equal to `analyticsUsername` (verifies spec: USER)
- [x] the per-user Secret name is `<replica>-user-<name>-creds` (verifies spec: USER)

## Deployment / setup-auth

- [x] each extra user gets indexed name/password env, password sourced from its Secret (verifies spec: USER)
- [x] a new extra user is created, an existing one updated, both as `LOGIN SUPERUSER` (verifies spec: USER)
- [x] extra users are provisioned as SUPERUSER even when the replica is read-only (verifies spec: USER)
- [x] an extra user's role sets `default_transaction_read_only = off`, applied before read-only mode is enabled (verifies spec: USER)
- [x] a replica with no extra users carries no extra-user env or SQL

## End-to-end (integration)

- [ ] an extra user's credentials Secret is created before the restore initialises, carrying `username`/`password` (verifies spec: USER)
- [ ] the extra user exists as a superuser and can write despite a read-only replica, without disabling read-only mode itself (verifies spec: USER)
- [ ] the analytics user's sessions stay read-only on the same replica (verifies spec: USER)
- [ ] the per-user Secret is cleaned up when the replica is deleted (owner reference) (verifies spec: USER)
