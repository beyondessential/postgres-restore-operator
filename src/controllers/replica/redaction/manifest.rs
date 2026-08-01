//! Parse a Tamanu/dbt manifest document into [`ColumnMask`] and
//! [`TableMask`] entries. The shape of the document is defined at
//! <https://github.com/beyondessential/tamanu/tree/main/database#masking>.

use serde_json::Value;
use tracing::warn;

use super::mask::{ColumnMask, TableMask, parse_range};

#[derive(Debug, Default)]
pub struct Manifest {
	pub columns: Vec<ColumnMask>,
	pub tables: Vec<TableMask>,
}

/// Parse a manifest JSON string. Sources missing `schema` or `name` are
/// skipped (warning logged). Column entries with unrecognised mask shapes
/// are kept (carrying the verbatim kind string) so the apply phase can
/// count them as tolerated errors with useful context.
pub fn parse_manifest(json: &str) -> Result<Manifest, serde_json::Error> {
	let doc: Value = serde_json::from_str(json)?;
	let mut out = Manifest::default();

	let Some(sources) = doc.get("sources").and_then(Value::as_object) else {
		return Ok(out);
	};

	for (source_id, source) in sources {
		let Some(schema) = source.get("schema").and_then(Value::as_str) else {
			warn!(
				source = source_id,
				"manifest source has no `schema`, skipping"
			);
			continue;
		};
		let Some(name) = source.get("name").and_then(Value::as_str) else {
			warn!(
				source = source_id,
				"manifest source has no `name`, skipping"
			);
			continue;
		};

		if let Some(mask) = meta_masking(source)
			&& let Some(kind) = mask_kind(&mask)
		{
			out.tables.push(TableMask {
				schema: schema.into(),
				table: name.into(),
				kind: kind.into(),
			});
		}

		let Some(columns) = source.get("columns").and_then(Value::as_object) else {
			continue;
		};

		for (col_name, col) in columns {
			let Some(mask) = meta_masking(col) else {
				continue;
			};
			let Some(kind) = mask_kind(&mask) else {
				warn!(
					source = source_id,
					column = col_name,
					"manifest masking has no `kind`, skipping"
				);
				continue;
			};

			let range = mask
				.as_object()
				.and_then(|o| o.get("range"))
				.and_then(Value::as_str)
				.and_then(parse_range);

			out.columns.push(ColumnMask {
				schema: schema.into(),
				table: name.into(),
				column: col_name.into(),
				kind: kind.into(),
				range,
			});
		}
	}

	Ok(out)
}

/// Read `<node>.config.meta.masking`, falling back to `<node>.meta.masking`.
fn meta_masking(node: &Value) -> Option<Value> {
	if let Some(v) = node
		.get("config")
		.and_then(|c| c.get("meta"))
		.and_then(|m| m.get("masking"))
	{
		return Some(v.clone());
	}
	node.get("meta").and_then(|m| m.get("masking")).cloned()
}

/// Short-form (`"name"`) vs extended-form (`{"kind":"name", …}`) both
/// reduce to a single kind string.
fn mask_kind(v: &Value) -> Option<&str> {
	match v {
		Value::String(s) => Some(s.as_str()),
		Value::Object(o) => o.get("kind").and_then(Value::as_str),
		_ => None,
	}
}

/// Derive a base version (`major.minor.0`) from a `MAJOR.MINOR.PATCH`
/// version. Returns `None` if the input doesn't match that shape, or if
/// the patch is already `0`.
pub fn base_version(v: &str) -> Option<String> {
	let parts: Vec<&str> = v.split('.').collect();
	if parts.len() != 3 {
		return None;
	}
	let minor: u32 = parts[1].parse().ok()?;
	let patch: u32 = parts[2].parse().ok()?;
	if patch == 0 {
		return None;
	}
	Some(format!("{}.{}.0", parts[0], minor))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_short_form_string() {
		let m = parse_manifest(
			r#"{
				"sources": {
					"any.id": {
						"schema": "public",
						"name": "users",
						"columns": {
							"email": {"config": {"meta": {"masking": "email"}}}
						}
					}
				}
			}"#,
		)
		.unwrap();

		assert_eq!(m.columns.len(), 1);
		assert_eq!(m.columns[0].schema, "public");
		assert_eq!(m.columns[0].table, "users");
		assert_eq!(m.columns[0].column, "email");
		assert_eq!(m.columns[0].kind, "email");
		assert_eq!(m.columns[0].range, None);
	}

	#[test]
	fn parses_extended_form_with_range() {
		let m = parse_manifest(
			r#"{
				"sources": {
					"x": {
						"schema": "public",
						"name": "vitals",
						"columns": {
							"heart_rate": {"config":{"meta":{"masking":{"kind":"float","range":"60-200"}}}}
						}
					}
				}
			}"#,
		)
		.unwrap();

		assert_eq!(m.columns[0].kind, "float");
		assert_eq!(m.columns[0].range, Some((60.0, 200.0)));
	}

	#[test]
	fn parses_extended_form_with_float_range() {
		let m = parse_manifest(
			r#"{
				"sources": {
					"x": {
						"schema": "public",
						"name": "vitals",
						"columns": {
							"urine_sg": {"config":{"meta":{"masking":{"kind":"float","range":"1.001-1.03"}}}}
						}
					}
				}
			}"#,
		)
		.unwrap();

		assert_eq!(m.columns[0].range, Some((1.001, 1.03)));
	}

	#[test]
	fn parses_table_level_truncate() {
		let m = parse_manifest(
			r#"{
				"sources": {
					"x": {
						"schema": "public",
						"name": "sync_lookup",
						"config": {"meta": {"masking": "truncate"}},
						"columns": {}
					}
				}
			}"#,
		)
		.unwrap();

		assert_eq!(m.tables.len(), 1);
		assert_eq!(m.tables[0].schema, "public");
		assert_eq!(m.tables[0].table, "sync_lookup");
		assert_eq!(m.tables[0].kind, "truncate");
	}

	#[test]
	fn parses_table_level_truncate_via_meta_fallback() {
		let m = parse_manifest(
			r#"{
				"sources": {
					"x": {
						"schema": "public",
						"name": "t",
						"meta": {"masking": "truncate"},
						"columns": {}
					}
				}
			}"#,
		)
		.unwrap();

		assert_eq!(m.tables.len(), 1);
		assert_eq!(m.tables[0].table, "t");
	}

	#[test]
	fn skips_source_missing_schema_or_name() {
		let m = parse_manifest(
			r#"{
				"sources": {
					"a": {"name": "t", "columns": {"c": {"config":{"meta":{"masking":"email"}}}}},
					"b": {"schema": "s", "columns": {"c": {"config":{"meta":{"masking":"email"}}}}}
				}
			}"#,
		)
		.unwrap();

		assert_eq!(m.columns.len(), 0);
	}

	#[test]
	fn keeps_unknown_kind_verbatim() {
		let m = parse_manifest(
			r#"{
				"sources": {
					"x": {
						"schema": "public",
						"name": "t",
						"columns": {
							"c": {"config":{"meta":{"masking":"brand_new"}}}
						}
					}
				}
			}"#,
		)
		.unwrap();

		assert_eq!(m.columns[0].kind, "brand_new");
	}

	#[test]
	fn base_version_strips_patch() {
		assert_eq!(base_version("2.41.7"), Some("2.41.0".to_string()));
	}

	#[test]
	fn base_version_returns_none_when_patch_is_zero() {
		assert_eq!(base_version("2.41.0"), None);
	}

	#[test]
	fn base_version_returns_none_on_bad_shape() {
		assert_eq!(base_version("2.41"), None);
		assert_eq!(base_version("not-a-version"), None);
		assert_eq!(base_version("2.x.7"), None);
	}
}
