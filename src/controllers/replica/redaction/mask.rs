//! Mask types parsed out of a Tamanu/dbt manifest, and the registry that
//! turns them into `SECURITY LABEL` fragments for postgresql_anonymizer.
//!
//! The canonical contract for `meta.masking` is documented at
//! <https://github.com/beyondessential/tamanu/tree/main/database#masking>.

use crate::controllers::postgres::quote_ident;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMask {
	pub schema: String,
	pub table: String,
	pub column: String,
	pub kind: String,
	pub range: Option<(f64, f64)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableMask {
	pub schema: String,
	pub table: String,
	pub kind: String,
}

/// Resolved column metadata used by the type-dispatched kinds
/// (`zero`, `empty`, `default`, `nil`).
#[derive(Debug, Clone)]
pub struct ColumnInfo {
	pub data_type: String,
	pub is_nullable: bool,
	pub column_default: Option<String>,
}

/// The SQL right-hand side of a `SECURITY LABEL … IS '<this>'`.
#[derive(Debug, Clone, PartialEq)]
pub enum Fragment {
	Function(String),
	Value(String),
}

impl Fragment {
	pub fn render(&self) -> String {
		match self {
			Self::Function(expr) => format!("MASKED WITH FUNCTION {expr}"),
			Self::Value(expr) => format!("MASKED WITH VALUE {expr}"),
		}
	}
}

/// Parse `"L-H"` (e.g. `"20-50"`, `"1.001-1.03"`) into a pair of `f64`s,
/// splitting on the **last** `-` so floats decompose correctly. Returns
/// `None` on parse failure.
pub fn parse_range(s: &str) -> Option<(f64, f64)> {
	let (lo, hi) = s.rsplit_once('-')?;
	let lo: f64 = lo.parse().ok()?;
	let hi: f64 = hi.parse().ok()?;
	Some((lo, hi))
}

/// Build the `Fragment` for a column mask. `info` is only consulted for
/// kinds that need column-type knowledge (`zero`, `empty`, `default`,
/// `nil`); for other kinds it can be `None` (used by unit tests).
///
/// Returns `Err` with a short diagnostic when the kind is unsupported or
/// when type-dependent kinds are missing required `info`.
pub fn fragment_for(mask: &ColumnMask, info: Option<&ColumnInfo>) -> Result<Fragment, String> {
	let col = quote_ident(&mask.column);

	match mask.kind.as_str() {
		"date" => Ok(Fragment::Function(null_pres(
			&col,
			"anon.random_date()".into(),
		))),

		"datetime" => Ok(Fragment::Function(null_pres(
			&col,
			format!("date_trunc('day', {col}) + (floor(random() * 86400) || ' seconds')::interval"),
		))),

		"text" => Ok(Fragment::Function(null_pres(
			&col,
			format!("anon.lorem_ipsum(characters := length({col}))"),
		))),

		"string" => Ok(Fragment::Function(null_pres(
			&col,
			format!("anon.random_string(length({col}))"),
		))),

		"email" => Ok(Fragment::Function(null_pres(
			&col,
			"anon.fake_email()".into(),
		))),

		"name" => Ok(Fragment::Function(null_pres(
			&col,
			format!(
				"CASE WHEN {col} LIKE '% %' \
				 THEN anon.fake_first_name() || ' ' || anon.fake_last_name() \
				 ELSE anon.fake_first_name() END"
			),
		))),

		"phone" => Ok(Fragment::Function(null_pres(
			&col,
			format!("anon.partial({col}, 2, '****', 2)"),
		))),

		"place" => Ok(Fragment::Function(null_pres(
			&col,
			"anon.fake_city()".into(),
		))),

		"url" => Ok(Fragment::Function(null_pres(
			&col,
			"'https://example.invalid/' || anon.random_string(8)".into(),
		))),

		"integer" => {
			let (lo, hi) = mask.range.unwrap_or((i32::MIN as f64, i32::MAX as f64));
			Ok(Fragment::Function(null_pres(
				&col,
				format!("(floor(random() * ({hi} - {lo} + 1)) + {lo})::int"),
			)))
		}

		"float" => {
			let (lo, hi) = mask.range.unwrap_or((0.0, 1.0));
			Ok(Fragment::Function(null_pres(
				&col,
				format!("(random() * ({hi} - {lo}) + {lo})::numeric"),
			)))
		}

		"money" => {
			let (lo, hi) = mask.range.unwrap_or((0.0, 10_000.0));
			Ok(Fragment::Function(null_pres(
				&col,
				format!("round((random() * ({hi} - {lo}) + {lo})::numeric, 2)"),
			)))
		}

		"zero" => {
			let info = info.ok_or_else(|| "zero mask needs column type".to_string())?;
			match data_type_family(&info.data_type) {
				DataTypeFamily::Bytea => Ok(Fragment::Function(format!(
					"repeat(E'\\x00'::bytea, length({col}))"
				))),
				DataTypeFamily::Text => {
					Ok(Fragment::Function(format!("repeat('0', length({col}))")))
				}
				DataTypeFamily::Numeric => Ok(Fragment::Value("0".into())),
				DataTypeFamily::Other => {
					Err(format!("zero mask unsupported for type {}", info.data_type))
				}
			}
		}

		"empty" => {
			let info = info.ok_or_else(|| "empty mask needs column type".to_string())?;
			match data_type_family(&info.data_type) {
				DataTypeFamily::Numeric => Ok(Fragment::Value("0".into())),
				DataTypeFamily::Text => Ok(Fragment::Value("''".into())),
				DataTypeFamily::Bytea => Ok(Fragment::Value("E'\\\\x'::bytea".into())),
				DataTypeFamily::Other => match info.data_type.as_str() {
					"json" | "jsonb" => Ok(Fragment::Value(format!("'{{}}'::{}", info.data_type))),
					"ARRAY" => Ok(Fragment::Value("'{}'".into())),
					_ => Err(format!(
						"empty mask unsupported for type {}",
						info.data_type
					)),
				},
			}
		}

		"nil" => {
			let info = info.ok_or_else(|| "nil mask needs column type".to_string())?;
			if !info.is_nullable {
				return Err("nil mask on non-nullable column".into());
			}
			Ok(Fragment::Value("NULL".into()))
		}

		"default" => {
			let info = info.ok_or_else(|| "default mask needs column type".to_string())?;
			match info.column_default.as_deref() {
				Some(d) => Ok(Fragment::Value(d.into())),
				None => Err("default mask on column without default".into()),
			}
		}

		other => Err(format!("unknown mask kind: {other}")),
	}
}

/// Wrap an expression in a null-preserving CASE.
fn null_pres(col: &str, expr: String) -> String {
	format!("CASE WHEN {col} IS NULL THEN NULL ELSE {expr} END")
}

enum DataTypeFamily {
	Numeric,
	Text,
	Bytea,
	Other,
}

/// Group `information_schema.columns.data_type` strings into the families
/// that determine how `zero`/`empty` are realised.
fn data_type_family(s: &str) -> DataTypeFamily {
	match s {
		"smallint" | "integer" | "bigint" | "real" | "double precision" | "numeric" | "decimal" => {
			DataTypeFamily::Numeric
		}
		"character varying" | "character" | "text" | "citext" => DataTypeFamily::Text,
		"bytea" => DataTypeFamily::Bytea,
		_ => DataTypeFamily::Other,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn cm(kind: &str, range: Option<(f64, f64)>) -> ColumnMask {
		ColumnMask {
			schema: "public".into(),
			table: "t".into(),
			column: "c".into(),
			kind: kind.into(),
			range,
		}
	}

	fn info(data_type: &str, nullable: bool, default: Option<&str>) -> ColumnInfo {
		ColumnInfo {
			data_type: data_type.into(),
			is_nullable: nullable,
			column_default: default.map(str::to_string),
		}
	}

	#[test]
	fn range_splits_on_last_dash_for_floats() {
		assert_eq!(parse_range("1.001-1.03"), Some((1.001, 1.03)));
		assert_eq!(parse_range("20-50"), Some((20.0, 50.0)));
		assert_eq!(parse_range("0-10.5"), Some((0.0, 10.5)));
	}

	#[test]
	fn range_handles_negative_lo() {
		assert_eq!(parse_range("-5-5"), Some((-5.0, 5.0)));
	}

	#[test]
	fn range_returns_none_on_garbage() {
		assert!(parse_range("nope").is_none());
		assert!(parse_range("1-x").is_none());
		assert!(parse_range("1.2").is_none());
	}

	#[test]
	fn fragment_email_is_null_preserving() {
		let f = fragment_for(&cm("email", None), None).unwrap();
		let rendered = f.render();
		assert!(rendered.contains("MASKED WITH FUNCTION"));
		assert!(rendered.contains("CASE WHEN"));
		assert!(rendered.contains("anon.fake_email()"));
	}

	#[test]
	fn fragment_name_detects_space() {
		let f = fragment_for(&cm("name", None), None).unwrap();
		let rendered = f.render();
		assert!(rendered.contains("LIKE '% %'"));
		// anon doesn't ship fake_name(); compose first + last for the
		// with-space branch.
		assert!(rendered.contains("fake_first_name() || ' ' || anon.fake_last_name()"));
		assert!(rendered.contains("ELSE anon.fake_first_name()"));
	}

	#[test]
	fn fragment_integer_uses_range() {
		let f = fragment_for(&cm("integer", Some((20.0, 50.0))), None).unwrap();
		let rendered = f.render();
		assert!(rendered.contains("50 - 20"));
		assert!(rendered.contains("::int"));
	}

	#[test]
	fn fragment_money_rounds_to_two_decimals() {
		let f = fragment_for(&cm("money", Some((0.0, 100.0))), None).unwrap();
		assert!(f.render().contains("round("));
	}

	#[test]
	fn fragment_zero_for_bytea_repeats() {
		let f = fragment_for(&cm("zero", None), Some(&info("bytea", true, None))).unwrap();
		assert!(matches!(f, Fragment::Function(ref s) if s.contains("repeat(E'\\x00'::bytea")));
	}

	#[test]
	fn fragment_zero_for_text_repeats_digit() {
		let f = fragment_for(&cm("zero", None), Some(&info("text", true, None))).unwrap();
		assert!(matches!(f, Fragment::Function(ref s) if s.contains("repeat('0',")));
	}

	#[test]
	fn fragment_zero_for_numeric_is_value_zero() {
		let f = fragment_for(&cm("zero", None), Some(&info("integer", true, None))).unwrap();
		assert_eq!(f, Fragment::Value("0".into()));
	}

	#[test]
	fn fragment_empty_dispatches_on_type() {
		assert_eq!(
			fragment_for(&cm("empty", None), Some(&info("integer", true, None))).unwrap(),
			Fragment::Value("0".into())
		);
		assert_eq!(
			fragment_for(&cm("empty", None), Some(&info("text", true, None))).unwrap(),
			Fragment::Value("''".into())
		);
		assert_eq!(
			fragment_for(&cm("empty", None), Some(&info("jsonb", true, None))).unwrap(),
			Fragment::Value("'{}'::jsonb".into())
		);
	}

	#[test]
	fn fragment_nil_requires_nullable() {
		assert!(fragment_for(&cm("nil", None), Some(&info("text", false, None))).is_err());
		assert_eq!(
			fragment_for(&cm("nil", None), Some(&info("text", true, None))).unwrap(),
			Fragment::Value("NULL".into())
		);
	}

	#[test]
	fn fragment_default_requires_default_expression() {
		assert!(fragment_for(&cm("default", None), Some(&info("text", true, None))).is_err());
		assert_eq!(
			fragment_for(
				&cm("default", None),
				Some(&info("text", true, Some("'hello'::text"))),
			)
			.unwrap(),
			Fragment::Value("'hello'::text".into())
		);
	}

	#[test]
	fn fragment_unknown_kind_errors() {
		assert!(fragment_for(&cm("brand_new_kind", None), None).is_err());
	}

	#[test]
	fn rendered_value_keeps_null_marker() {
		assert_eq!(
			Fragment::Value("NULL".into()).render(),
			"MASKED WITH VALUE NULL"
		);
	}
}
