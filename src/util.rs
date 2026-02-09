use std::time::Duration;

use crate::error::Error;

/// Parses a human-friendly duration string like "6h", "15m", "5m30s", "2h30m".
pub fn parse_duration(s: &str) -> Result<Duration, Error> {
    let s = s.trim();
    if s.is_empty() {
        return Err(Error::InvalidDuration(s.to_string()));
    }

    let mut total_secs: u64 = 0;
    let mut current_num = String::new();

    for c in s.chars() {
        if c.is_ascii_digit() {
            current_num.push(c);
        } else {
            if current_num.is_empty() {
                return Err(Error::InvalidDuration(s.to_string()));
            }
            let n: u64 = current_num
                .parse()
                .map_err(|_| Error::InvalidDuration(s.to_string()))?;
            current_num.clear();

            match c {
                'd' => total_secs += n * 86400,
                'h' => total_secs += n * 3600,
                'm' => total_secs += n * 60,
                's' => total_secs += n,
                _ => return Err(Error::InvalidDuration(s.to_string())),
            }
        }
    }

    // Bare number without unit is treated as seconds
    if !current_num.is_empty() {
        let n: u64 = current_num
            .parse()
            .map_err(|_| Error::InvalidDuration(s.to_string()))?;
        total_secs += n;
    }

    if total_secs == 0 {
        return Err(Error::InvalidDuration(s.to_string()));
    }

    Ok(Duration::from_secs(total_secs))
}

/// Converts a glob pattern to a regex string.
pub fn glob_to_regex(glob: &str) -> String {
    let mut regex = String::from("^");
    for c in glob.chars() {
        match c {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex
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
    fn test_glob_to_regex() {
        assert_eq!(glob_to_regex("fiji-prod-*"), "^fiji-prod-.*$");
        assert_eq!(glob_to_regex("*.example.com"), "^.*\\.example\\.com$");
        assert_eq!(glob_to_regex("host-?"), "^host-.$");
    }
}
