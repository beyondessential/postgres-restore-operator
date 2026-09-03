---
id: USER
---

# Replica database users

The login roles a `PostgresPhysicalReplica` provisions in each restored database, and how their credentials are exposed for connection.

Users are provisioned once, as part of the restore's initialisation, rather than continuously reconciled. Changing the user configuration takes effect on the next restore. The operator does not alter roles in an already-running restore, and never drops a role it previously created — this leaves whatever process is using the database free to make its own user customisations without the operator overwriting them.

## Analytics user

The primary login role, named by `analyticsUsername` (default `analytics`).

- The operator generates its password and stores it in the replica's credentials Secret `<replica>-creds`, under `username` and `password` keys.
- The role is reset to a fixed state on every restore, created first if it doesn't already exist: attributes (`LOGIN`, and none of `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION`, `BYPASSRLS`), role memberships (all revoked), and password. A name colliding with a role in the source cluster is reset the same way, so it doesn't keep production's attributes or grants.
- Permissions are then granted on top of that base, following the restore's *effective* read-only state rather than `readOnly` alone: on PostgreSQL 14 or above, a restore that is both `readOnly` and carries none of `persistentSchemas`, `redaction`, or `migrate_to` grants the role `pg_read_all_data`; every other case — `readOnly: false`, any PostgreSQL below 14, or a `readOnly: true` restore that sets `persistentSchemas`, `redaction`, or `migrate_to` — grants `SUPERUSER` for at least the restore's initial provisioning.
- `pg_read_all_data` covers `SELECT` on every table/view/sequence and `USAGE` on every schema, but not function execution — that reached the analytics user only via `PUBLIC` by default, and the database-wide lockdown below closes that off, so the `pg_read_all_data` branch also explicitly grants `EXECUTE` on every function to compensate.
- Only `redaction` ever demotes the role back down afterwards, and only when `readOnly` is true and `persistentSchemas` is unset — `migrate_to` doesn't affect whether that demotion happens, so a `readOnly: true` restore with both `redaction` and `migrate_to` set (no `persistentSchemas`) still ends at `pg_read_all_data` once redaction completes. `persistentSchemas` set, or `migrate_to` set without `redaction`, keeps the role at `SUPERUSER` for as long as the restore runs, regardless of `readOnly` — a known gap.
- Its connection details are surfaced in `status.connectionInfo`.

## Extra users

A replica may declare additional login roles through `extraUsers`, a list of `{name, schemas}` entries. The `analytics` canopy intent carries this as the `extra_users` text parameter: comma-separated entries, each `name` (no schema access) or `name:schema1+schema2` (a `+`-joined schema list after a colon) — e.g. `reporting,dbt_reader:dbt+staging`.

- Each name is reset to a fixed state on every restore, created first if it doesn't already exist — the same treatment as the analytics role, so a name colliding with a role in the source cluster doesn't keep production's attributes or grants. The role ends up with `LOGIN` and none of `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION`, `BYPASSRLS`; every role membership it holds is revoked. It can connect and read the system catalogues, and nothing else, until its own `schemas` are granted back (see below) or something else grants it more. The operator does not decide what these accounts may see beyond `schemas`, and because every restore is a fresh database, any grant made to one has to be reapplied to the next restore.
- Its sessions are read-only, regardless of the replica's `readOnly` setting, so a grant made later widens what the role can read without letting it write. This applies to the extra user's own sessions only.
- `schemas` grants `USAGE` on the schema plus `SELECT` on all its current tables and sequences, in whichever databases that schema exists in — silently skipped in a database where it doesn't. Also sets `ALTER DEFAULT PRIVILEGES FOR ROLE <analyticsUsername> IN SCHEMA <schema> GRANT SELECT ON TABLES`, so tables the persistent-schemas migration creates afterwards (which runs as the analytics role) are covered too, without a second grant step once that migration finishes. This only covers objects the analytics role creates; a table created by some other role in that schema still needs its own grant. These grants are applied last, after the database-wide lockdown below strips the role's inherited ownership and direct grants — granting first would just have `DROP OWNED BY` revoke it again immediately.
- The operator generates a password for each extra user and stores it in a per-user Secret named `<replica>-user-<slugged name>-creds` — the username is slugged to fit Kubernetes object-name rules, but the Secret's `username` key carries the real, unslugged role name. The Secret is owned by the replica, so it is cleaned up when the replica is deleted.
- Each extra user's Secret is created before the restore's initialisation runs, so the generated password is available when the role is created.
- An extra user whose name matches the analytics user is ignored — that role is already provisioned.
- Extra users are not surfaced in `status`; each one's Secret name is derived from the replica name and the username.

## Database-wide lockdown

Because this is a *physical* restore, the snapshot carries the source cluster's catalog rows verbatim, not just its data — role attributes are reset above, but ownership, direct grants, and anything granted broadly to `PUBLIC` all survive untouched unless something removes them. A single lockdown step does both, running in every connectable database of every restore, **unconditionally** — whether or not the replica declares any extra users, and regardless of `persistentSchemas`, `redaction`, or `migrate_to`.

It runs unconditionally rather than only when `extraUsers` is set, so there is never a "was this applied for this restore" question a later reconcile could get wrong: gating it on `extraUsers` would mean a replica that starts with extra users and later has them all removed keeps its `PUBLIC` grants revoked forever, with nothing recorded anywhere and no way back, since removing the last extra user provides no signal to reverse it.

- **Ownership and direct grants.** For the analytics user and each extra user, `REASSIGN OWNED BY` hands every object that role owns to `postgres` (keeping the objects — this is not `DROP OWNED` on its own, which would drop them), then `DROP OWNED BY` revokes whatever direct grants remain. Both are no-ops for a role with nothing owned or granted, so this runs the same way whether the role is fresh or collided with a production one.
- **`PUBLIC`'s own grants.** Privileges are additive in PostgreSQL and there is no per-role deny, so a privilege `PUBLIC` holds cannot be taken away from one role — the grant has to come off `PUBLIC` itself. This covers every schema except the system ones, every table/view/sequence/materialized view/foreign table, every function, and default privileges for objects not yet created — not just the `public` schema's own ACL.
- `pg_catalog`, `information_schema`, and the `pg_toast`/`pg_temp` families are excluded throughout: system-catalogue access via `PUBLIC` is the one thing an otherwise ungranted role is meant to keep.
- Nothing that reaches a restore loses access it's meant to have. The analytics user holds `SUPERUSER` (which bypasses every ACL check this touches) or `pg_read_all_data` plus the explicit `EXECUTE` grant described above; every role restored from the source cluster has had its password nulled by this point, so none of them can authenticate to spend whatever `PUBLIC` privilege they might otherwise still see.
- `template1` is deliberately included, not excluded: `CREATE DATABASE` with no explicit `TEMPLATE` copies `template1` byte for byte, so excluding it would mean every database created later in that instance's life starts back at the open, unrestricted default.
- Last, for each extra user with a non-empty `schemas`, its declared schemas are granted back — see the extra users section above. This has to come after the ownership/direct-grant strip above, or `DROP OWNED BY` would immediately revoke the grant it just made.
