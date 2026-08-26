---
id: USER
---

# Replica database users

The login roles a `PostgresPhysicalReplica` provisions in each restored database, and how their credentials are exposed for connection.

Users are provisioned once, as part of the restore's initialisation, rather than continuously reconciled. Changing the user configuration takes effect on the next restore. The operator does not alter roles in an already-running restore, and never drops a role it previously created — this leaves whatever process is using the database free to make its own user customisations without the operator overwriting them.

## Analytics user

The primary login role, named by `analyticsUsername` (default `analytics`).

- The operator generates its password and stores it in the replica's credentials Secret `<replica>-creds`, under `username` and `password` keys.
- Its permissions follow the replica's `readOnly` setting: on PostgreSQL 14 or above a read-only replica grants the role `pg_read_all_data`; a read-write replica, and any replica on a PostgreSQL below 14, grants the role `SUPERUSER`.
- Its connection details are surfaced in `status.connectionInfo`.

## Extra users

A replica may declare additional login roles through `extraUsers`, a comma-separated list of usernames. The `analytics` canopy intent carries this as the `extra_users` text parameter, which the operator parses into the list.

- Each named user is created as a `LOGIN` role with `SUPERUSER`, giving it write access. This holds regardless of the replica's `readOnly` setting.
- The operator generates a password for each extra user and stores it in a per-user Secret named `<replica>-user-<name>-creds`, with `username` and `password` keys. The Secret is owned by the replica, so it is cleaned up when the replica is deleted.
- Each extra user's Secret is created before the restore's initialisation runs, so the generated password is available when the role is created.
- An extra user whose name matches the analytics user is ignored — that role is already provisioned.
- Extra users are not surfaced in `status`; each one's Secret name is derived from the replica name and the username.
