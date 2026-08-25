//! Recreation of read-only login roles on each restore.
//!
//! Every restore is a fresh cluster built from the snapshot, so a role that
//! only ever existed on a previous restore is gone once the switchover
//! completes. The operator therefore reapplies each `persistentUsers` entry
//! against the incoming restore, using a password held in a per-user Secret so
//! the credential itself stays stable across switchovers.
//!
//! This runs after the `persistentSchemas` migration Job and before the new
//! restore is labelled ready for traffic: the schemas these roles read from are
//! written by that Job, so granting any earlier would apply to nothing.

use tracing::{info, warn};

use super::super::postgres::{self, quote_ident, quote_literal};
use crate::{error::Result, types::PersistentUser};

/// SQL that creates or updates the role itself.
///
/// `CREATE ROLE` is not idempotent and `IF NOT EXISTS` does not exist for
/// roles, so the create is wrapped in a `DO` block that falls through to
/// `ALTER ROLE` when the role survived in the snapshot's `pg_authid`. The
/// attribute list is repeated on both paths so a role restored with, say,
/// `CREATEDB` from the source is stripped back down to a read-only shape.
fn role_sql(user: &PersistentUser, password: &str) -> String {
	let ident = quote_ident(&user.name);
	let literal = quote_literal(&user.name);
	let password = quote_literal(password);
	const ATTRS: &str = "NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS";
	format!(
		"DO $pgro$\n\
		 BEGIN\n\
		 \x20 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = {literal}) THEN\n\
		 \x20   CREATE ROLE {ident} LOGIN PASSWORD {password} {ATTRS};\n\
		 \x20 ELSE\n\
		 \x20   ALTER ROLE {ident} WITH LOGIN PASSWORD {password} {ATTRS};\n\
		 \x20 END IF;\n\
		 END\n\
		 $pgro$;"
	)
}

/// SQL granting connect rights on the restored database.
fn connect_sql(user: &PersistentUser, dbname: &str) -> String {
	format!(
		"GRANT CONNECT ON DATABASE {} TO {};",
		quote_ident(dbname),
		quote_ident(&user.name)
	)
}

/// SQL granting read access on one schema, covering both what the migration
/// Job just wrote and whatever `owner` creates in the schema later.
///
/// The default-privilege grants name `owner` explicitly rather than going
/// through `SET ROLE`: `ALTER DEFAULT PRIVILEGES` without `FOR ROLE` applies to
/// the *current* role only, so running it as anyone other than the schema owner
/// silently affects nothing.
fn schema_grants_sql(user: &PersistentUser, schema: &str, owner: &str) -> Vec<String> {
	let role = quote_ident(&user.name);
	let schema = quote_ident(schema);
	let owner = quote_ident(owner);
	vec![
		format!("GRANT USAGE ON SCHEMA {schema} TO {role};"),
		format!("GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role};"),
		format!("GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA {schema} TO {role};"),
		format!(
			"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} \
			 GRANT SELECT ON TABLES TO {role};"
		),
		format!(
			"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} \
			 GRANT SELECT, USAGE ON SEQUENCES TO {role};"
		),
	]
}

/// SQL pinning the role's `search_path`, so it can query unqualified names.
fn search_path_sql(user: &PersistentUser) -> Option<String> {
	if user.search_path.is_empty() {
		return None;
	}
	let path = user
		.search_path
		.iter()
		.map(|s| quote_ident(s))
		.collect::<Vec<_>>()
		.join(", ");
	Some(format!(
		"ALTER ROLE {} SET search_path = {path};",
		quote_ident(&user.name)
	))
}

/// Full statement list for one user against a restore whose main database is
/// `dbname`.
///
/// `present_schemas` holds `(schema, owner)` for the subset of
/// `user.read_schemas` that actually exists in the restore; callers resolve it
/// against the live database so a schema that never made it through migration
/// is skipped rather than aborting the switchover on an undefined-object error.
pub fn statements_for(
	user: &PersistentUser,
	password: &str,
	dbname: &str,
	present_schemas: &[(String, String)],
) -> Vec<String> {
	let mut sql = vec![role_sql(user, password), connect_sql(user, dbname)];
	for (schema, owner) in present_schemas {
		sql.extend(schema_grants_sql(user, schema, owner));
	}
	sql.extend(search_path_sql(user));
	sql
}

/// One user paired with its password and the `(schema, owner)` entries that
/// were found to exist in the restore.
pub type ResolvedUser<'a> = (&'a PersistentUser, &'a String, Vec<(String, String)>);

/// Every statement for a whole provisioning session, in execution order.
///
/// Kept separate from [`apply`] so the ordering — in particular that
/// [`ALLOW_WRITES`] precedes all DDL — is testable without a database.
pub fn session_statements(resolved: &[ResolvedUser<'_>], dbname: &str) -> Vec<String> {
	let mut sql = vec![ALLOW_WRITES.to_string()];
	for (user, password, schemas) in resolved {
		sql.extend(statements_for(user, password, dbname, schemas));
	}
	sql
}

/// Session GUC issued before any provisioning DDL.
///
/// A `readOnly` replica (the default) has `default_transaction_read_only = on`
/// baked into its `postgresql.conf` by the restore init script, which would
/// fail every `CREATE ROLE` and `GRANT` here with "cannot execute … in a
/// read-only transaction". The restore is a promoted standalone rather than a
/// standby, so the setting is a plain `USERSET` GUC the operator's own session
/// can turn off; the replica stays read-only for everyone else.
const ALLOW_WRITES: &str = "SET default_transaction_read_only = off;";

/// Apply every persistent user to an open connection against the restore's
/// main database.
///
/// `passwords` supplies each user's password, read from its per-user Secret by
/// the caller. Returns the schemas that were skipped because they are absent
/// from the restore, so the caller can raise a Warning event.
pub async fn apply(
	pg: &tokio_postgres::Client,
	users: &[PersistentUser],
	passwords: &[String],
	dbname: &str,
) -> Result<Vec<String>> {
	let mut skipped = Vec::new();
	let mut resolved = Vec::with_capacity(users.len());

	for (user, password) in users.iter().zip(passwords) {
		let present = postgres::schema_owners_on(pg, &user.read_schemas).await?;
		skipped.extend(
			user.read_schemas
				.iter()
				.filter(|s| !present.iter().any(|(found, _)| found == *s))
				.map(|s| format!("{}/{s}", user.name)),
		);
		info!(
			user = %user.name,
			schemas = ?present,
			"provisioning persistent user"
		);
		resolved.push((user, password, present));
	}

	for stmt in session_statements(&resolved, dbname) {
		pg.batch_execute(&stmt).await?;
	}

	if !skipped.is_empty() {
		warn!(
			skipped = ?skipped,
			"persistent user read schemas absent from restore; grants skipped"
		);
	}

	Ok(skipped)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn user() -> PersistentUser {
		PersistentUser {
			name: "tupaia_read".into(),
			read_schemas: vec!["public_tupaia".into()],
			search_path: vec!["public_tupaia".into()],
			secret_name: None,
		}
	}

	#[test]
	fn role_is_created_read_only_and_reset_when_present() {
		let sql = role_sql(&user(), "hunter2");
		assert!(
			sql.contains("CREATE ROLE \"tupaia_read\" LOGIN PASSWORD 'hunter2' NOSUPERUSER"),
			"create branch must set the password and strip privileges: {sql}"
		);
		assert!(
			sql.contains("ALTER ROLE \"tupaia_read\" WITH LOGIN PASSWORD 'hunter2' NOSUPERUSER"),
			"alter branch must reset a role that survived in the snapshot: {sql}"
		);
		assert!(
			sql.contains("NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"),
			"read-only attributes must be spelled out: {sql}"
		);
	}

	#[test]
	fn default_privileges_name_the_owner_explicitly() {
		// Without FOR ROLE, ALTER DEFAULT PRIVILEGES applies to the connected
		// role and silently fails to cover tables the owner creates later.
		let sql = schema_grants_sql(&user(), "public_tupaia", "analytics");
		let defaults: Vec<_> = sql
			.iter()
			.filter(|s| s.contains("ALTER DEFAULT PRIVILEGES"))
			.collect();
		assert_eq!(defaults.len(), 2, "tables and sequences both need defaults");
		for stmt in defaults {
			assert!(
				stmt.contains("FOR ROLE \"analytics\""),
				"default privileges must name the schema owner: {stmt}"
			);
		}
	}

	#[test]
	fn absent_schemas_produce_no_grants() {
		let sql = statements_for(&user(), "pw", "tamanu", &[]);
		assert!(
			!sql.iter().any(|s| s.contains("public_tupaia")
				&& (s.contains("GRANT USAGE") || s.contains("ALTER DEFAULT"))),
			"a schema missing from the restore must not be granted on: {sql:?}"
		);
		assert!(
			sql.iter().any(|s| s.contains("GRANT CONNECT")),
			"the role and its connect grant still apply: {sql:?}"
		);
	}

	#[test]
	fn search_path_is_optional() {
		assert!(search_path_sql(&user()).is_some());
		let mut bare = user();
		bare.search_path.clear();
		assert!(
			search_path_sql(&bare).is_none(),
			"an empty searchPath must leave the role's setting untouched"
		);
	}

	#[test]
	fn identifiers_and_passwords_are_quoted() {
		let hostile = PersistentUser {
			name: "we\"ird".into(),
			read_schemas: vec![],
			search_path: vec![],
			secret_name: None,
		};
		let sql = role_sql(&hostile, "pass'word");
		assert!(
			sql.contains("\"we\"\"ird\""),
			"role name must be quoted as an identifier: {sql}"
		);
		assert!(
			sql.contains("'pass''word'"),
			"password must be quoted as a literal: {sql}"
		);
	}

	#[test]
	fn statements_cover_the_full_provisioning_sequence() {
		let sql = statements_for(
			&user(),
			"pw",
			"tamanu",
			&[("public_tupaia".into(), "analytics".into())],
		);
		assert_eq!(
			sql.len(),
			8,
			"role + connect + 5 schema grants + search_path: {sql:?}"
		);
		assert!(sql[1].contains("GRANT CONNECT ON DATABASE \"tamanu\""));
		assert!(sql.last().expect("non-empty").contains("search_path"));
	}

	#[test]
	fn writes_are_re_enabled_before_any_ddl() {
		// readOnly replicas (the default) carry default_transaction_read_only
		// = on, which fails CREATE ROLE and every GRANT. The session must turn
		// it off first or provisioning blocks the switchover outright.
		let user = user();
		let password = "pw".to_string();
		let sql = session_statements(
			&[(
				&user,
				&password,
				vec![("public_tupaia".into(), "analytics".into())],
			)],
			"tamanu",
		);
		assert_eq!(
			sql.first().map(String::as_str),
			Some("SET default_transaction_read_only = off;"),
			"writes must be enabled before anything else: {sql:?}"
		);
		assert!(
			sql[1..]
				.iter()
				.all(|s| !s.contains("default_transaction_read_only")),
			"the GUC should only be set once, up front: {sql:?}"
		);
	}

	#[test]
	fn session_covers_every_user_once() {
		let a = PersistentUser {
			name: "reader_a".into(),
			read_schemas: vec![],
			search_path: vec![],
			secret_name: None,
		};
		let b = PersistentUser {
			name: "reader_b".into(),
			read_schemas: vec![],
			search_path: vec![],
			secret_name: None,
		};
		let (pw_a, pw_b) = ("pa".to_string(), "pb".to_string());
		let sql = session_statements(&[(&a, &pw_a, vec![]), (&b, &pw_b, vec![])], "tamanu");
		for name in ["reader_a", "reader_b"] {
			assert_eq!(
				sql.iter()
					.filter(|s| s.contains("CREATE ROLE") && s.contains(name))
					.count(),
				1,
				"{name} must be created exactly once: {sql:?}"
			);
		}
	}

	#[test]
	fn default_privileges_follow_the_actual_schema_owner() {
		// The owner comes from pg_namespace, not from a hardcoded analytics
		// user, so a schema owned by someone else still grants correctly.
		let sql = statements_for(
			&user(),
			"pw",
			"tamanu",
			&[("public_tupaia".into(), "dbt_runner".into())],
		);
		assert!(
			sql.iter()
				.any(|s| s.contains("ALTER DEFAULT PRIVILEGES FOR ROLE \"dbt_runner\"")),
			"default privileges must follow the real owner: {sql:?}"
		);
	}
}
