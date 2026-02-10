use std::time::Duration;

use chrono::Utc;
use kube::ResourceExt;
use tracing::{debug, info, warn};

use crate::{types::*, util::parse_duration};

/// Calculate stable jitter for a replica name.
pub fn calculate_jitter(replica_name: &str, max_jitter: Duration) -> Duration {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};

	let mut hasher = DefaultHasher::new();
	replica_name.hash(&mut hasher);
	let hash = hasher.finish();

	let max_secs = max_jitter.as_secs();
	if max_secs == 0 {
		return Duration::ZERO;
	}
	Duration::from_secs(hash % max_secs)
}

/// Normalize a cron expression to the 7-field format expected by the `cron` crate.
///
/// Standard crontab uses 5 fields (min hour dom month dow).
/// The `cron` crate expects 6-7 fields (sec min hour dom month dow [year]).
/// This prepends `0` for seconds and appends `*` for year when needed.
pub fn normalize_cron(expr: &str) -> String {
	match expr.split_whitespace().count() {
		5 => format!("0 {expr} *"),
		6 => format!("0 {expr}"),
		_ => expr.to_string(),
	}
}

pub fn compute_next_scheduled_restore(schedule: &str) -> Option<chrono::DateTime<Utc>> {
	let cron_schedule = normalize_cron(schedule).parse::<cron::Schedule>().ok()?;
	cron_schedule.upcoming(Utc).next()
}

pub fn should_trigger_scheduled_restore(replica: &PostgresPhysicalReplica) -> bool {
	let name = replica.name_any();
	let Some(schedule) = &replica.spec.schedule else {
		debug!(replica = %name, "no schedule configured, skipping scheduled restore");
		return false;
	};

	let status = replica.status.as_ref();

	// Check minimumTTL (only if configured)
	if let Some(ref ttl_str) = replica.spec.minimum_ttl
		&& let Some(last_completed) = status.and_then(|s| s.last_restore_completed_at.as_ref())
		&& let Ok(last_completed) = last_completed.parse::<chrono::DateTime<Utc>>()
	{
		if let Ok(minimum_ttl) = parse_duration(ttl_str) {
			let elapsed = Utc::now().signed_duration_since(last_completed);
			if elapsed.to_std().unwrap_or_default() < minimum_ttl {
				let remaining = minimum_ttl - elapsed.to_std().unwrap_or_default();
				debug!(
					replica = %name,
					minimum_ttl_secs = minimum_ttl.as_secs(),
					remaining_secs = remaining.as_secs(),
					"minimum TTL not elapsed since last restore, skipping"
				);
				return false;
			}
		} else {
			warn!(replica = %name, minimum_ttl = ttl_str, "invalid minimumTTL value, ignoring");
		}
	}

	// Check cron schedule
	let Ok(cron_schedule) = normalize_cron(schedule).parse::<cron::Schedule>() else {
		warn!(replica = %name, schedule = schedule, "invalid cron expression");
		return false;
	};

	let jitter = calculate_jitter(
		&name,
		parse_duration(&replica.spec.schedule_jitter).unwrap_or(Duration::from_secs(600)),
	);

	let now = Utc::now();

	if let Some(next_scheduled) = status.and_then(|s| s.next_scheduled_restore.as_ref())
		&& let Ok(next) = next_scheduled.parse::<chrono::DateTime<Utc>>()
	{
		// Add jitter to the scheduled time
		let trigger_at = next + chrono::Duration::from_std(jitter).unwrap_or_default();
		if now >= trigger_at {
			info!(
				replica = %name,
				next_scheduled = %next,
				trigger_at = %trigger_at,
				"scheduled restore time reached, triggering"
			);
			return true;
		}
		debug!(
			replica = %name,
			next_scheduled = %next,
			trigger_at = %trigger_at,
			remaining_secs = (trigger_at - now).num_seconds(),
			"scheduled restore not due yet, skipping"
		);
		return false;
	}

	// Initial seed: nextScheduledRestore not yet set (first reconciliation or field was cleared).
	// Fall back to checking whether a cron occurrence falls within a 24h lookback window.
	let jittered_now = now - chrono::Duration::from_std(jitter).unwrap_or_default();
	if let Some(prev) = cron_schedule
		.after(&(jittered_now - chrono::Duration::hours(24)))
		.next()
		&& prev <= now
	{
		info!(
			replica = %name,
			schedule = schedule,
			"no nextScheduledRestore set, found cron occurrence in 24h lookback window, triggering"
		);
		return true;
	}

	debug!(
		replica = %name,
		schedule = schedule,
		"no nextScheduledRestore set and no cron occurrence in 24h lookback window, skipping"
	);
	false
}
