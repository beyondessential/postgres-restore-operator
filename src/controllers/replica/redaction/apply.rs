//! Apply parsed [`Manifest`] entries against a live restore database.
//!
//! This is the only file in the `redaction` module that talks to a real
//! Postgres. It is invoked by the reconciler after a restore reaches the
//! `Ready` phase and before the switchover branch.

use std::collections::HashMap;

use tokio_postgres::types::Type;
use tracing::{info, warn};

use super::manifest::Manifest;
use super::mask::{ColumnInfo, ColumnMask, fragment_for};
use crate::controllers::postgres::{PgConnection, quote_ident};
use crate::error::{Error, Result};

#[derive(Debug, Default)]
pub struct Outcome {
	pub columns_attempted: u32,
	pub columns_failed: u32,
	pub tables_attempted: u32,
	pub tables_failed: u32,
}

impl Outcome {
	pub fn is_partial(&self) -> bool {
		self.columns_failed > 0 || self.tables_failed > 0
	}
}

/// Apply a manifest against the live database that `conn` is attached to.
///
/// The connection must be made as a superuser (CREATE EXTENSION, SECURITY
/// LABEL, TRUNCATE and `anon.anonymize_database()` all require it).
pub async fn apply(conn: &PgConnection, manifest: &Manifest) -> Result<Outcome> {
	let mut outcome = Outcome::default();

	conn.client
		.simple_query("CREATE EXTENSION IF NOT EXISTS anon CASCADE")
		.await
		.map_err(|e| Error::Redaction(format!("CREATE EXTENSION anon failed: {}", pg_error(&e))))?;

	conn.client
		.simple_query("SELECT anon.init()")
		.await
		.map_err(|e| Error::Redaction(format!("anon.init() failed: {}", pg_error(&e))))?;

	for table in &manifest.tables {
		outcome.tables_attempted += 1;
		if table.kind != "truncate" {
			warn!(
				schema = %table.schema,
				table = %table.table,
				kind = %table.kind,
				"unsupported table-level mask kind, skipping"
			);
			outcome.tables_failed += 1;
			continue;
		}
		let stmt = format!(
			"TRUNCATE TABLE {}.{} CASCADE",
			quote_ident(&table.schema),
			quote_ident(&table.table)
		);
		if let Err(e) = conn.client.simple_query(&stmt).await {
			warn!(
				schema = %table.schema,
				table = %table.table,
				error = %pg_error(&e),
				"table truncate failed, continuing"
			);
			outcome.tables_failed += 1;
		}
	}

	let column_infos = lookup_column_infos(conn, &manifest.columns).await?;

	for mask in &manifest.columns {
		outcome.columns_attempted += 1;
		let info = column_infos.get(&col_key(mask));
		if info.is_none() {
			warn!(
				schema = %mask.schema,
				table = %mask.table,
				column = %mask.column,
				"column not present in restore, skipping"
			);
			outcome.columns_failed += 1;
			continue;
		}
		let fragment = match fragment_for(mask, info) {
			Ok(f) => f,
			Err(reason) => {
				warn!(
					schema = %mask.schema,
					table = %mask.table,
					column = %mask.column,
					kind = %mask.kind,
					%reason,
					"could not build mask fragment, skipping"
				);
				outcome.columns_failed += 1;
				continue;
			}
		};

		let label = format!(
			"SECURITY LABEL FOR anon ON COLUMN {}.{}.{} IS {}",
			quote_ident(&mask.schema),
			quote_ident(&mask.table),
			quote_ident(&mask.column),
			quote_sql_literal(&fragment.render()),
		);
		if let Err(e) = conn.client.simple_query(&label).await {
			// Log the rule as well as the error: this path is tolerated,
			// so the log is all an operator gets to work out why a column
			// they expected to be masked came through in the clear.
			warn!(
				schema = %mask.schema,
				table = %mask.table,
				column = %mask.column,
				kind = %mask.kind,
				rule = %fragment.render(),
				error = %pg_error(&e),
				"SECURITY LABEL failed, continuing"
			);
			outcome.columns_failed += 1;
		}
	}

	info!(
		columns = manifest.columns.len(),
		tables = manifest.tables.len(),
		failed_columns = outcome.columns_failed,
		failed_tables = outcome.tables_failed,
		"running anon.anonymize_database()"
	);

	conn.client
		.simple_query("SELECT anon.anonymize_database()")
		.await
		.map_err(|e| {
			Error::Redaction(format!(
				"anon.anonymize_database() failed: {}",
				pg_error(&e)
			))
		})?;

	Ok(outcome)
}

/// Lock the freshly-redacted database back to read-only by:
/// - setting the DB-level `default_transaction_read_only` GUC, and
/// - granting `pg_read_all_data` to the analytics user and demoting it
///   back to NOSUPERUSER (matching the role posture the restore init
///   script applies when `effective_read_only` is true).
///
/// This connection *is* the analytics user — redaction needs the
/// superuser it holds while the restore is writable — so the grant has to
/// land before the demotion. `pg_read_all_data` has no admin option
/// outside superuser, and postgres re-reads the session's superuser bit
/// from the catalog on the very next statement, so demoting first makes
/// the grant fail with "must have admin option on role pg_read_all_data"
/// and leaves the replica with no read access at all.
pub async fn enforce_read_only(
	conn: &PgConnection,
	dbname: &str,
	analytics_user: &str,
) -> Result<()> {
	let alter_db = format!(
		"ALTER DATABASE {} SET default_transaction_read_only = on",
		quote_ident(dbname),
	);
	conn.client
		.simple_query(&alter_db)
		.await
		.map_err(|e| Error::Redaction(format!("ALTER DATABASE for read-only failed: {e}")))?;

	let grant = format!(
		"GRANT pg_read_all_data TO {user}",
		user = quote_ident(analytics_user),
	);
	conn.client
		.simple_query(&grant)
		.await
		.map_err(|e| Error::Redaction(format!("granting pg_read_all_data failed: {e}")))?;

	let demote = format!(
		"ALTER ROLE {user} WITH NOSUPERUSER",
		user = quote_ident(analytics_user),
	);
	conn.client
		.simple_query(&demote)
		.await
		.map_err(|e| Error::Redaction(format!("demoting analytics user failed: {e}")))?;

	Ok(())
}

/// Key used to join `ColumnMask` with the `information_schema` results.
fn col_key(m: &ColumnMask) -> (String, String, String) {
	(m.schema.clone(), m.table.clone(), m.column.clone())
}

/// Look up `data_type`, `is_nullable`, and `column_default` for every
/// masked column in a single batch query. Columns absent from the
/// restore's schema simply don't appear in the returned map.
async fn lookup_column_infos(
	conn: &PgConnection,
	masks: &[ColumnMask],
) -> Result<HashMap<(String, String, String), ColumnInfo>> {
	let mut out = HashMap::new();
	if masks.is_empty() {
		return Ok(out);
	}

	let schemas: Vec<String> = masks.iter().map(|m| m.schema.clone()).collect();
	let tables: Vec<String> = masks.iter().map(|m| m.table.clone()).collect();
	let columns: Vec<String> = masks.iter().map(|m| m.column.clone()).collect();

	let stmt = "
		SELECT c.table_schema, c.table_name, c.column_name,
		       c.data_type, c.is_nullable,
		       pg_get_expr(d.adbin, d.adrelid) AS column_default
		FROM information_schema.columns c
		LEFT JOIN pg_catalog.pg_attribute a
		       ON a.attrelid = (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass
		      AND a.attname = c.column_name
		      AND NOT a.attisdropped
		LEFT JOIN pg_catalog.pg_attrdef d
		       ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE (c.table_schema, c.table_name, c.column_name)
		      IN (SELECT s, t, col
		          FROM UNNEST($1::text[], $2::text[], $3::text[])
		          AS u(s, t, col))
	";

	let rows = conn
		.client
		.query_typed(
			stmt,
			&[
				(&schemas, Type::TEXT_ARRAY),
				(&tables, Type::TEXT_ARRAY),
				(&columns, Type::TEXT_ARRAY),
			],
		)
		.await?;

	for row in rows {
		let schema: String = row.get("table_schema");
		let table: String = row.get("table_name");
		let column: String = row.get("column_name");
		let data_type: String = row.get("data_type");
		let nullable: String = row.get("is_nullable");
		let default: Option<String> = row.get("column_default");

		out.insert(
			(schema, table, column),
			ColumnInfo {
				data_type,
				is_nullable: nullable == "YES",
				column_default: default,
			},
		);
	}

	Ok(out)
}

/// Render a postgres error with the message the server actually sent.
/// `tokio_postgres::Error`'s own Display flattens every server-side
/// failure to "db error", which is useless in the tolerated-failure
/// paths where the log is the only diagnostic.
fn pg_error(e: &tokio_postgres::Error) -> String {
	let Some(db) = e.as_db_error() else {
		return e.to_string();
	};
	let mut out = format!("{} ({})", db.message(), db.code().code());
	if let Some(detail) = db.detail() {
		out.push_str(&format!("; detail: {detail}"));
	}
	if let Some(hint) = db.hint() {
		out.push_str(&format!("; hint: {hint}"));
	}
	out
}

/// Quote a string for inclusion as a SQL literal (single-quoted).
fn quote_sql_literal(s: &str) -> String {
	let escaped = s.replace('\'', "''");
	format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn quote_sql_literal_escapes_single_quotes() {
		assert_eq!(quote_sql_literal("ab'c"), "'ab''c'");
	}

	#[test]
	fn quote_sql_literal_wraps_normal_text() {
		assert_eq!(quote_sql_literal("hello"), "'hello'");
	}

	#[test]
	fn outcome_is_partial_when_anything_failed() {
		let mut o = Outcome::default();
		assert!(!o.is_partial());
		o.columns_failed = 1;
		assert!(o.is_partial());
		o.columns_failed = 0;
		o.tables_failed = 1;
		assert!(o.is_partial());
	}
}
