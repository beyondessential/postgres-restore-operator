use std::hash::{DefaultHasher, Hash, Hasher};

use cronexpr::{ParseOptions, parse_crontab_with};
use jiff::{SignedDuration, SpanTotal, Timestamp, Unit, tz::TimeZone};
use kube::ResourceExt;
use rand::distr::uniform::SampleRange;
use tracing::{debug, info, warn};

use crate::types::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleDecision {
	NotDue,
	Trigger,
	SkippedByTtl,
}

impl PostgresPhysicalReplica {
	/// Compute a stable hash of the schedule inputs (`schedule` + `scheduleJitter`)
	/// so we can detect when the user changes either field without storing raw values.
	pub fn schedule_input_hash(&self) -> String {
		let mut hasher = DefaultHasher::new();
		self.spec.schedule.hash(&mut hasher);
		self.spec.schedule_jitter.to_string().hash(&mut hasher);
		format!("{:016x}", hasher.finish())
	}

	pub fn compute_next_scheduled_restore(&self, now: Timestamp) -> Option<Timestamp> {
		let schedule = &self.spec.schedule;

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

	pub fn check_schedule(&self) -> ScheduleDecision {
		let name = self.name_any();
		let schedule = &self.spec.schedule;
		let status = self.status.as_ref();
		let now = Timestamp::now().to_zoned(TimeZone::UTC);

		let Some(next_scheduled) = status.and_then(|s| s.next_scheduled_restore.as_ref()) else {
			info!(
				replica = %name,
				schedule = schedule,
				"no nextScheduledRestore set, triggering"
			);
			return ScheduleDecision::Trigger;
		};

		if now < next_scheduled.0.to_zoned(TimeZone::UTC) {
			debug!(
				replica = %name,
				next_scheduled = %next_scheduled.0,
				"scheduled restore not due yet, skipping"
			);
			return ScheduleDecision::NotDue;
		}

		info!(
			replica = %name,
			next_scheduled = %next_scheduled.0,
			"scheduled restore time reached"
		);

		// Check minimumTTL: prevent restoring too soon after the last one
		if let Some(ref minimum_ttl) = self.spec.minimum_ttl
			&& let Some(last_completed) = status.and_then(|s| s.last_restore_completed_at.as_ref())
		{
			let not_before = last_completed
				.0
				.to_zoned(TimeZone::UTC)
				.saturating_add(minimum_ttl.0);
			if now < not_before {
				info!(
					replica = %name,
					last_completed = %last_completed.0,
					%minimum_ttl,
					%not_before,
					"last restore completed within minimum TTL, skipping"
				);
				return ScheduleDecision::SkippedByTtl;
			}
		}

		ScheduleDecision::Trigger
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use jiff::Span;
	use k8s_openapi::{api::core::v1::SecretReference, apimachinery::pkg::apis::meta::v1::Time};
	use kube::api::ObjectMeta;

	use crate::util::TimeSpan;

	use super::*;

	fn make_replica(
		schedule: &str,
		next_scheduled: Option<Timestamp>,
		last_completed: Option<Timestamp>,
		minimum_ttl: Option<TimeSpan>,
	) -> PostgresPhysicalReplica {
		PostgresPhysicalReplica {
			metadata: ObjectMeta {
				name: Some("test-replica".into()),
				namespace: Some("default".into()),
				uid: Some("test-uid-123".into()),
				..Default::default()
			},
			spec: PostgresPhysicalReplicaSpec {
				kopia_secret_ref: SecretReference {
					name: Some("test-secret".into()),
					namespace: None,
				},
				snapshot_filter: None,
				schedule: schedule.into(),
				schedule_jitter: TimeSpan(Span::new().seconds(0)),
				minimum_ttl,
				switchover_grace_period: TimeSpan(Span::new().minutes(5)),
				analytics_username: "analytics".into(),
				storage_class: None,
				storage_size_override: None,
				resources: None,
				service_annotations: None,
				pod_annotations: None,
				affinity: None,
				tolerations: Vec::new(),
				read_only: true,
				postgres_extra_config: None,
				notifications: Vec::new(),
				storage_size_maximum: k8s_openapi::apimachinery::pkg::api::resource::Quantity(
					"2Ti".to_string(),
				),

				persistent_schemas: None,
			},
			status: Some(PostgresPhysicalReplicaStatus {
				next_scheduled_restore: next_scheduled.map(Time),
				last_restore_completed_at: last_completed.map(Time),
				..Default::default()
			}),
		}
	}

	#[test]
	fn check_schedule_no_next_scheduled_triggers() {
		let replica = make_replica("0 */6 * * *", None, None, None);
		assert_eq!(replica.check_schedule(), ScheduleDecision::Trigger);
	}

	#[test]
	fn check_schedule_not_due_yet() {
		let future = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().hours(3))
			.timestamp();
		let replica = make_replica("0 */6 * * *", Some(future), None, None);
		assert_eq!(replica.check_schedule(), ScheduleDecision::NotDue);
	}

	#[test]
	fn check_schedule_due_triggers() {
		let past = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().hours(-1))
			.timestamp();
		let replica = make_replica("0 */6 * * *", Some(past), None, None);
		assert_eq!(replica.check_schedule(), ScheduleDecision::Trigger);
	}

	#[test]
	fn check_schedule_due_but_within_ttl_skips() {
		let past = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().hours(-1))
			.timestamp();
		// Last restore completed 30 minutes ago, TTL is 2 hours
		let last_completed = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().minutes(-30))
			.timestamp();
		let ttl = TimeSpan(Span::new().hours(2));
		let replica = make_replica("0 */6 * * *", Some(past), Some(last_completed), Some(ttl));
		assert_eq!(replica.check_schedule(), ScheduleDecision::SkippedByTtl);
	}

	#[test]
	fn check_schedule_due_and_past_ttl_triggers() {
		let past = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().hours(-1))
			.timestamp();
		// Last restore completed 3 hours ago, TTL is 2 hours
		let last_completed = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().hours(-3))
			.timestamp();
		let ttl = TimeSpan(Span::new().hours(2));
		let replica = make_replica("0 */6 * * *", Some(past), Some(last_completed), Some(ttl));
		assert_eq!(replica.check_schedule(), ScheduleDecision::Trigger);
	}

	#[test]
	fn check_schedule_no_ttl_configured_triggers() {
		let past = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().hours(-1))
			.timestamp();
		// Last restore completed recently but no TTL configured
		let last_completed = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().minutes(-10))
			.timestamp();
		let replica = make_replica("0 */6 * * *", Some(past), Some(last_completed), None);
		assert_eq!(replica.check_schedule(), ScheduleDecision::Trigger);
	}

	#[test]
	fn compute_next_scheduled_restore_valid_cron() {
		let replica = make_replica("0 */6 * * *", None, None, None);
		let now = Timestamp::now();
		let next = replica.compute_next_scheduled_restore(now);
		assert!(next.is_some());
		assert!(next.unwrap() > now);
	}

	#[test]
	fn compute_next_scheduled_restore_invalid_cron() {
		let replica = make_replica("not a cron", None, None, None);
		let now = Timestamp::now();
		assert!(replica.compute_next_scheduled_restore(now).is_none());
	}

	#[test]
	fn compute_next_scheduled_restore_with_jitter() {
		let mut replica = make_replica("0 */6 * * *", None, None, None);
		replica.spec.schedule_jitter = TimeSpan(Span::new().minutes(30));
		let now = Timestamp::now();

		let mut results: Vec<Timestamp> = (0..10)
			.filter_map(|_| replica.compute_next_scheduled_restore(now))
			.collect();
		results.sort();
		results.dedup();

		// With 30 minutes of jitter, repeated calls should produce varying results
		assert!(
			results.len() > 1,
			"jitter should produce different results across calls"
		);
	}

	#[test]
	fn check_schedule_ttl_with_no_previous_restore_triggers() {
		let past = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().hours(-1))
			.timestamp();
		// TTL configured but no last_completed timestamp (first restore)
		let ttl = TimeSpan(Span::new().hours(2));
		let replica = make_replica("0 */6 * * *", Some(past), None, Some(ttl));
		assert_eq!(replica.check_schedule(), ScheduleDecision::Trigger);
	}

	#[test]
	fn check_schedule_uses_tags_for_filtering() {
		// Verify that check_schedule works with snapshot filter tags
		// (tags are used elsewhere but shouldn't affect scheduling)
		let past = Timestamp::now()
			.to_zoned(jiff::tz::TimeZone::UTC)
			.saturating_add(Span::new().hours(-1))
			.timestamp();
		let mut replica = make_replica("0 */6 * * *", Some(past), None, None);
		replica.spec.snapshot_filter = Some(SnapshotFilter {
			tags: Some(HashMap::from([("env".into(), "prod".into())])),
			host_pattern: None,
			description_pattern: None,
			path_pattern: None,
		});
		assert_eq!(replica.check_schedule(), ScheduleDecision::Trigger);
	}
}
