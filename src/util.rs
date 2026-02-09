use std::time::Duration;

use crate::error::Error;

/// Parses a human-friendly duration string like "6h", "15m", "5m30s", "2h30m".
/// Falls back to treating a bare number as seconds.
pub fn parse_duration(s: &str) -> Result<Duration, Error> {
    let s = s.trim();
    if s.is_empty() {
        return Err(Error::InvalidDuration(s.to_string()));
    }

    // Bare number without unit is treated as seconds
    if let Ok(n) = s.parse::<u64>() {
        return if n == 0 {
            Err(Error::InvalidDuration(s.to_string()))
        } else {
            Ok(Duration::from_secs(n))
        };
    }

    let span: jiff::Span = jiff::fmt::friendly::SpanParser::new()
        .parse_span(s)
        .map_err(|_| Error::InvalidDuration(s.to_string()))?;

    let total_secs = span.get_days() as u64 * 86400
        + span.get_hours() as u64 * 3600
        + span.get_minutes() as u64 * 60
        + span.get_seconds() as u64;

    if total_secs == 0 {
        return Err(Error::InvalidDuration(s.to_string()));
    }

    Ok(Duration::from_secs(total_secs))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("6h").unwrap(), Duration::from_secs(6 * 3600));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(15 * 60));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(5 * 60));
        assert_eq!(
            parse_duration("2h30m").unwrap(),
            Duration::from_secs(2 * 3600 + 30 * 60)
        );
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86400));
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn parse_duration_compound() {
        assert_eq!(
            parse_duration("1d2h3m4s").unwrap(),
            Duration::from_secs(86400 + 7200 + 180 + 4)
        );
    }

    #[test]
    fn parse_duration_whitespace_trimmed() {
        assert_eq!(parse_duration("  5m  ").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn parse_duration_zero_is_error() {
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("0").is_err());
    }

    #[test]
    fn parse_duration_invalid_unit() {
        assert!(parse_duration("5x").is_err());
    }

    #[test]
    fn parse_duration_leading_unit_is_error() {
        assert!(parse_duration("h5").is_err());
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
        assert!(!glob_matches("ab?d", "abd"));
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
}
