//! Apply parsed [`Manifest`] entries against a live restore database.
//!
//! This is the only file in the `redaction` module that talks to a real
//! Postgres. It is invoked by the reconciler after a restore reaches the
//! `Ready` phase and before the switchover branch.

use std::collections::{BTreeMap, HashMap};

use tokio_postgres::types::Type;
use tracing::warn;

use super::manifest::Manifest;
use super::mask::{ColumnInfo, ColumnMask, mask_expression};
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
/// Masking is done with our own `UPDATE`s rather than anon's
/// `SECURITY LABEL` + `anonymize_database()`: anon only accepts a
/// schema-qualified call to a trusted function as a masking rule, which
/// can't express the null-preserving `CASE` wrapper (or any of the
/// derived-value kinds) that the manifest contract needs. anon is still
/// the source of the fake data itself.
///
/// The connection must be made as a superuser: CREATE EXTENSION, TRUNCATE
/// and rewriting other roles' tables all require it.
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

	// Group by table so each table is rewritten once, however many of its
	// columns are masked — the same shape of work anon's static masking
	// does internally, and the difference between one heap pass and one
	// per column on a wide table.
	let mut by_table: BTreeMap<(&str, &str), Vec<(&ColumnMask, String)>> = BTreeMap::new();
	for mask in &manifest.columns {
		outcome.columns_attempted += 1;
		let Some(info) = column_infos.get(&col_key(mask)) else {
			warn!(
				schema = %mask.schema,
				table = %mask.table,
				column = %mask.column,
				"column not present in restore, skipping"
			);
			outcome.columns_failed += 1;
			continue;
		};
		match mask_expression(mask, Some(info)) {
			Ok(expr) => by_table
				.entry((&mask.schema, &mask.table))
				.or_default()
				.push((mask, expr)),
			Err(reason) => {
				warn!(
					schema = %mask.schema,
					table = %mask.table,
					column = %mask.column,
					kind = %mask.kind,
					%reason,
					"could not build mask expression, skipping"
				);
				outcome.columns_failed += 1;
			}
		}
	}

	for ((schema, table), assignments) in &by_table {
		outcome.columns_failed += apply_table_masks(conn, schema, table, assignments).await;
	}

	Ok(outcome)
}

/// Mask every column of one table in a single pass over its heap.
///
/// A statement error takes the whole batch down with it, so on failure we
/// retry column by column: one column whose expression doesn't fit its
/// type (an `email` mask on an integer, say) shouldn't cost the rest of
/// the table its masking. Returns how many columns are still unmasked.
async fn apply_table_masks(
	conn: &PgConnection,
	schema: &str,
	table: &str,
	assignments: &[(&ColumnMask, String)],
) -> u32 {
	let batched = update_statement(
		schema,
		table,
		assignments
			.iter()
			.map(|(m, e)| (m.column.as_str(), e.as_str())),
	);
	let Err(err) = conn.client.simple_query(&batched).await else {
		return 0;
	};

	warn!(
		schema,
		table,
		columns = assignments.len(),
		error = %pg_error(&err),
		"batched mask update failed, retrying one column at a time"
	);

	let mut failed = 0;
	for (mask, expr) in assignments {
		let stmt = update_statement(schema, table, [(mask.column.as_str(), expr.as_str())]);
		if let Err(err) = conn.client.simple_query(&stmt).await {
			// This path is tolerated, so the log is all an operator gets
			// to work out why a column they expected to be masked came
			// through in the clear.
			warn!(
				schema,
				table,
				column = %mask.column,
				kind = %mask.kind,
				expression = %expr,
				error = %pg_error(&err),
				"mask update failed, continuing"
			);
			failed += 1;
		}
	}
	failed
}

/// `UPDATE <schema>.<table> SET <col> = <expr>, …`. Column names are
/// quoted; the expressions come from the mask registry, not from the
/// manifest, so they're ours to trust.
fn update_statement<'a>(
	schema: &str,
	table: &str,
	assignments: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> String {
	let sets = assignments
		.into_iter()
		.map(|(column, expr)| format!("{} = {expr}", quote_ident(column)))
		.collect::<Vec<_>>()
		.join(", ");
	format!(
		"UPDATE {}.{} SET {sets}",
		quote_ident(schema),
		quote_ident(table)
	)
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn update_statement_quotes_identifiers_and_joins_assignments() {
		let stmt = update_statement(
			"public",
			"users",
			[("email", "anon.fake_email()"), ("dob", "NULL")],
		);
		assert_eq!(
			stmt,
			r#"UPDATE "public"."users" SET "email" = anon.fake_email(), "dob" = NULL"#
		);
	}

	#[test]
	fn update_statement_handles_a_single_column() {
		let stmt = update_statement("s", "t", [("c", "0")]);
		assert_eq!(stmt, r#"UPDATE "s"."t" SET "c" = 0"#);
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
