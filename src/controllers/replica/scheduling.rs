use std::hash::{DefaultHasher, Hasher};

use cronexpr::{ParseOptions, parse_crontab_with};
use jiff::{SignedDuration, SpanTotal, Timestamp, Unit, tz::TimeZone};
use kube::ResourceExt;
use rand::distr::uniform::SampleRange;
use tracing::{debug, info, warn};

use crate::types::*;

impl PostgresPhysicalReplica {
	pub fn compute_next_scheduled_restore(&self, now: Timestamp) -> Option<Timestamp> {
		let schedule = self.spec.schedule.as_deref()?;

		let Ok(cron) = parse_crontab_with(schedule, {
			let mut options = ParseOptions::default();
			options.fallback_timezone_option = cronexpr::FallbackTimezoneOption::UTC;
			options.hashed_value = Some({
				let mut hasher = DefaultHasher::new();
				if let Some(uid) = &self.metadata.uid {
					hasher.write(uid.as_bytes());
				} else if let (Some(ns), Some(name)) =
					(&self.metadata.namespace, &self.metadata.name)
				{
					hasher.write(ns.as_bytes());
					hasher.write(name.as_bytes());
				} else {
					hasher.write(b"unknown");
				}
				hasher.finish()
			});
			options
		})
		.inspect_err(|err| {
			warn!("'{schedule}' is invalid cron expression: {err}");
		}) else {
			return None;
		};

		let Ok(mut next) = cron.find_next(now) else {
			warn!("'{schedule}' does not have a valid next scheduled time");
			return None;
		};

		match self
			.spec
			.schedule_jitter
			.0
			.total(SpanTotal::from(Unit::Second).days_are_24_hours())
		{
			Ok(jitter_secs) => {
				let offset = SignedDuration::from_secs_f64(
					((jitter_secs / -2.0)..(jitter_secs / 2.0))
						.sample_single(&mut rand::rng())
						.unwrap_or_default(),
				);
				next += offset;
			}
			Err(err) => {
				warn!(
					"jitter value '{:?}' is not resolvable to seconds: {err}",
					self.spec.schedule_jitter.0
				);
			}
		}

		Some(next.into())
	}

	pub fn should_trigger_scheduled_restore(&self) -> bool {
		let name = self.name_any();
		let Some(schedule) = &self.spec.schedule else {
			debug!(replica = %name, "no schedule configured, skipping scheduled restore");
			return false;
		};

		let status = self.status.as_ref();

		let now = Timestamp::now().to_zoned(TimeZone::UTC);

		// Check minimumTTL (only if configured)
		if let Some(ref minimum_ttl) = self.spec.minimum_ttl
			&& let Some(last_completed) = status.and_then(|s| s.last_restore_completed_at.as_ref())
		{
			let not_before = last_completed
				.0
				.to_zoned(TimeZone::UTC)
				.saturating_add(minimum_ttl.0);
			if not_before < now {
				debug!(
					replica = %name,
					last_completed = %last_completed.0,
					%minimum_ttl,
					%not_before,
					"last restore completed within minimum TTL, skipping"
				);
				return false;
			}
		}

		if let Some(next_scheduled) = status.and_then(|s| s.next_scheduled_restore.as_ref()) {
			if now >= next_scheduled.0.to_zoned(TimeZone::UTC) {
				info!(
					replica = %name,
					next_scheduled = %next_scheduled.0,
					"scheduled restore time reached, triggering"
				);
			} else {
				debug!(
					replica = %name,
					next_scheduled = %next_scheduled.0,
					"scheduled restore not due yet, skipping"
				);
				return false;
			}
		} else {
			info!(
				replica = %name,
				schedule = schedule,
				"no nextScheduledRestore set, triggering"
			);
		}

		true
	}
}
