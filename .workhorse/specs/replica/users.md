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
- Only `redaction` ever demotes the role back down afterwards, and only when `readOnly` is true and `persistentSchemas` is unset — `migrate_to` doesn't affect whether that demotion happens, so a `readOnly: true` restore with both `redaction` and `migrate_to` set (no `persistentSchemas`) still ends at `pg_read_all_data` once redaction completes. `persistentSchemas` set, or `migrate_to` set without `redaction`, keeps the role at `SUPERUSER` for as long as the restore runs, regardless of `readOnly` — this is a known gap the spec previously didn't describe.
- Its connection details are surfaced in `status.connectionInfo`.

## Extra users

A replica may declare additional login roles through `extraUsers`, a list of usernames. The `analytics` canopy intent carries this as the `extra_users` text parameter, a comma-separated list of usernames the operator splits into the list.

- Each name is reset to a fixed state on every restore, created first if it doesn't already exist — the same treatment as the analytics role, so a name colliding with a role in the source cluster doesn't keep production's attributes or grants. The role ends up with `LOGIN` and none of `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION`, `BYPASSRLS`; every role membership it holds is revoked; and it carries no grants of its own. It can connect and read the system catalogues, and nothing else until something grants it access. The operator does not decide what these accounts may see, and because every restore is a fresh database, any grant made to one has to be reapplied to the next restore.
- Its sessions are read-only, regardless of the replica's `readOnly` setting, so a grant made later widens what the role can read without letting it write. This applies to the extra user's own sessions only.
- Declaring at least one extra user revokes the `public` schema from the `PUBLIC` pseudo-role, in every connectable database of that restore. Privileges in PostgreSQL are additive and there is no per-role deny, so a privilege held via `PUBLIC` cannot be withheld from one role — closing the schema at `PUBLIC` is the only way an ungranted extra user stays out of it. Nothing that reaches the restore loses access: the analytics user holds `pg_read_all_data`, which carries schema `USAGE` in its own right, or `SUPERUSER`, which bypasses the check, and every role restored from the source cluster has had its password nulled. A replica that declares no extra users leaves the schema exactly as restored.
- The operator generates a password for each extra user and stores it in a per-user Secret named `<replica>-user-<slugged name>-creds` — the username is slugged to fit Kubernetes object-name rules, but the Secret's `username` key carries the real, unslugged role name. The Secret is owned by the replica, so it is cleaned up when the replica is deleted.
- Each extra user's Secret is created before the restore's initialisation runs, so the generated password is available when the role is created.
- An extra user whose name matches the analytics user is ignored — that role is already provisioned.
- Extra users are not surfaced in `status`; each one's Secret name is derived from the replica name and the username.
