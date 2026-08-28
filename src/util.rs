use std::{fmt::Display, str::FromStr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct TimeSpan(
	#[serde(serialize_with = "jiff::fmt::serde::span::friendly::compact::required")] pub jiff::Span,
);

impl FromStr for TimeSpan {
	type Err = jiff::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		jiff::fmt::friendly::SpanParser::new()
			.parse_span(s)
			.map(Self)
	}
}

impl Display for TimeSpan {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{:#}", self.0)
	}
}

/// Normalises a Windows-style path to a Unix-style path for consistent
/// glob matching. Drive letters like `C:\` become `/C/`, and backslashes
/// are replaced with forward slashes. Unix paths are returned unchanged.
pub fn normalize_windows_path(path: &str) -> String {
	let path = path.replace('\\', "/");
	// Convert drive letter prefix: "C:/..." -> "/C/..."
	if let Some(rest) = path
		.strip_prefix(|c: char| c.is_ascii_alphabetic())
		.and_then(|r| r.strip_prefix(':'))
	{
		let drive = path.chars().next().unwrap();
		return format!("/{drive}{rest}");
	}
	path
}

/// Tests whether a string matches a glob pattern.
pub fn glob_matches(pattern: &str, value: &str) -> bool {
	let Ok(glob) = globset::GlobBuilder::new(pattern)
		.literal_separator(false)
		.build()
	else {
		return false;
	};
	glob.compile_matcher().is_match(value)
}

/// Slugify to DNS-1123-label-safe: lowercased, non-alphanumeric runs → `-`,
/// leading/trailing `-` trimmed, truncated to 50 chars (leaves ≥9 chars for
/// the `-XXXXXXXX` disambiguator suffix in a 63-char label limit).
pub fn slug(s: &str) -> String {
	let mut out = String::with_capacity(s.len());
	let mut prev_dash = true;
	for c in s.chars() {
		let mapped = if c.is_ascii_alphanumeric() {
			c.to_ascii_lowercase()
		} else {
			'-'
		};
		if mapped == '-' {
			if !prev_dash {
				out.push('-');
				prev_dash = true;
			}
		} else {
			out.push(mapped);
			prev_dash = false;
		}
	}
	while out.ends_with('-') {
		out.pop();
	}
	while out.starts_with('-') {
		out.remove(0);
	}
	if out.is_empty() {
		out.push_str("replica");
	}
	if out.len() > 50 {
		out.truncate(50);
		while out.ends_with('-') {
			out.pop();
		}
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn normalize_windows_drive_path() {
		assert_eq!(normalize_windows_path(r"D:\Full"), "/D/Full");
		assert_eq!(
			normalize_windows_path(r"C:\Users\backup\data"),
			"/C/Users/backup/data"
		);
	}

	#[test]
	fn normalize_windows_drive_path_forward_slashes() {
		assert_eq!(normalize_windows_path("D:/Full"), "/D/Full");
	}

	#[test]
	fn normalize_unix_path_unchanged() {
		assert_eq!(normalize_windows_path("/mnt/data"), "/mnt/data");
		assert_eq!(
			normalize_windows_path("/home/user/backup"),
			"/home/user/backup"
		);
	}

	#[test]
	fn normalize_relative_path_unchanged() {
		assert_eq!(normalize_windows_path("data/backup"), "data/backup");
	}

	#[test]
	fn test_glob_matches_wildcard() {
		assert!(glob_matches("fiji-prod-*", "fiji-prod-01"));
		assert!(glob_matches("fiji-prod-*", "fiji-prod-abc"));
		assert!(!glob_matches("fiji-prod-*", "fiji-dev-01"));
	}

	#[test]
	fn test_glob_matches_question_mark() {
		assert!(glob_matches("ab?d", "abcd"));
		assert!(glob_matches("ab?d", "abxd"));
		assert!(!glob_matches("ab?c", "abc"));
		assert!(!glob_matches("ab?d", "abccd"));
	}

	#[test]
	fn test_glob_matches_exact() {
		assert!(glob_matches("exact", "exact"));
		assert!(!glob_matches("exact", "not-exact"));
		assert!(!glob_matches("exact", "exactnot"));
	}

	#[test]
	fn test_glob_matches_dots() {
		assert!(glob_matches("*.example.com", "foo.example.com"));
		assert!(!glob_matches("*.example.com", "foo.other.com"));
	}

	#[test]
	fn test_glob_matches_special_chars() {
		assert!(glob_matches("a+b", "a+b"));
		assert!(!glob_matches("a+b", "aab"));
	}

	#[test]
	fn slug_ascii_alnum_untouched() {
		assert_eq!(slug("hello123"), "hello123");
	}

	#[test]
	fn slug_lowercases_and_replaces_specials() {
		assert_eq!(slug("Nauru Prod Analytics!"), "nauru-prod-analytics");
	}

	#[test]
	fn slug_collapses_runs_of_specials() {
		assert_eq!(slug("a__b--c/d.e"), "a-b-c-d-e");
	}

	#[test]
	fn slug_trims_edges() {
		assert_eq!(slug("---weird---"), "weird");
	}

	#[test]
	fn slug_empty_becomes_replica() {
		assert_eq!(slug(""), "replica");
		assert_eq!(slug("...!!!"), "replica");
	}

	#[test]
	fn slug_truncates_at_50_without_trailing_dash() {
		let out = slug(&"a".repeat(60));
		assert_eq!(out.len(), 50);
		let out = slug(&format!("{}{}", "a".repeat(48), "--"));
		assert!(out.len() <= 50);
		assert!(!out.ends_with('-'));
	}
}
