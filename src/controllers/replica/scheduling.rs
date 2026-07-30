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

/// Geometric backoff to apply after consecutive restore failures, replacing
/// the normal cron schedule until a restore succeeds.
///
/// Formula: `clamp(60s * 2^n, 2min, 1h)` where `n` is the consecutive
/// failure count. Returns `None` for `n == 0` (no backoff needed; the cron
/// schedule applies).
///
/// Hits the 1-hour ceiling at 6 consecutive failures, which means a
/// sustained failure mode caps compute at ~1 retry/hour while a transient
/// blip still gets a fast (2 min) retry. The operator never permanently
/// suspends — failures are bounded by retry rate, not by a manual reset
/// gate — because most failure modes in this system are *external*
/// (Karpenter, AWS API, upstream snapshots) and resolve themselves; pgro
/// must keep trying so it picks up the fix without human intervention.
pub fn failure_backoff_delay(consecutive_failures: u32) -> Option<SignedDuration> {
	if consecutive_failures == 0 {
		return None;
	}
	const BASE_SECS: u64 = 60;
	const MIN_SECS: u64 = 120;
	const MAX_SECS: u64 = 3600;
	// Saturate exponent so we don't overflow on absurd failure counts.
	let exponent = consecutive_failures.min(20);
	let unclamped = BASE_SECS.saturating_mul(1u64 << exponent);
	let clamped = unclamped.clamp(MIN_SECS, MAX_SECS);
	Some(SignedDuration::from_secs(clamped as i64))
}

/// True when a failure backoff recorded by `fail_restore` is still in effect.
///
/// `fail_restore` advances `nextScheduledRestore` by [`failure_backoff_delay`]
/// on every failure. Triggers that bypass the schedule (notably the
/// never-restored immediate trigger) must consult this, or a replica that
/// fails every attempt retries as fast as it can fail.
pub fn backoff_pending(status: Option<&PostgresPhysicalReplicaStatus>, now: Timestamp) -> bool {
	let Some(status) = status else {
		return false;
	};
	if status.consecutive_restore_failures.unwrap_or(0) == 0 {
		return false;
	}
	status
		.next_scheduled_restore
		.as_ref()
		.is_some_and(|next| now < next.0)
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

	/// Wall-clock budget for the schema migration step inside a single
	/// restore cycle. Returns 20% of the interval between consecutive
	/// cron firings; e.g. a `0 */6 * * *` schedule (every 6h) gets a
	/// ~72 min budget. A healthy migration completes in seconds, so this
	/// is a generous backstop, not a tight SLA — the goal is to keep a
	/// pathological migration (postgres backend stuck on a single DDL,
	/// for example) from blocking the replica from coming up at all.
	/// Falls back to 1h if the cron expression can't be parsed or there
	/// is no schedule. When the timeout fires, the operator drops the
	/// `persistent_schemas` on the new restore (DROP SCHEMA … CASCADE)
	/// and proceeds to switchover. The next restore reattempts the
	/// migration if the schemas were regenerated upstream in between.
	pub fn schema_migration_timeout(&self) -> SignedDuration {
		const FALLBACK: SignedDuration = SignedDuration::from_secs(3600);
		const BUDGET_FRACTION_DENOMINATOR: i64 = 5; // 1/5 == 20%
		let Some(interval) = self.cron_interval(Timestamp::now()) else {
			return FALLBACK;
		};
		SignedDuration::from_secs(interval.as_secs() / BUDGET_FRACTION_DENOMINATOR)
	}

	/// Interval between two consecutive cron firings of this replica's
	/// schedule, measured from `now`. Returns `None` when the schedule
	/// can't be parsed or doesn't have a second next-fire.
	fn cron_interval(&self, now: Timestamp) -> Option<SignedDuration> {
		let schedule = &self.spec.schedule;
		let cron = parse_crontab_with(schedule, {
			let mut options = ParseOptions::default();
			options.fallback_timezone_option = cronexpr::FallbackTimezoneOption::UTC;
			options
		})
		.ok()?;
		let next = cron.find_next(now).ok()?;
		let next_ts = next.timestamp();
		let after = cron
			.find_next(next_ts + SignedDuration::from_secs(1))
			.ok()?;
		Some(after.timestamp().duration_since(next_ts))
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

	/// True when the replica's `minimum_ttl` is configured and the last
	/// restore completed within that window relative to `now`. Used by both
	/// the cron path (via [`Self::check_schedule`]) and the canopy path
	/// where TTL gates the desired-snapshot trigger.
	pub fn within_minimum_ttl(&self, now: Timestamp) -> bool {
		let Some(ref minimum_ttl) = self.spec.minimum_ttl else {
			return false;
		};
		let Some(last_completed) = self
			.status
			.as_ref()
			.and_then(|s| s.last_restore_completed_at.as_ref())
		else {
			return false;
		};
		let not_before = last_completed
			.0
			.to_zoned(TimeZone::UTC)
			.saturating_add(minimum_ttl.0);
		now.to_zoned(TimeZone::UTC) < not_before
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
				migrate_to: None,
				kopia_secret_ref: Some(SecretReference {
					name: Some("test-secret".into()),
					namespace: None,
				}),
				canopy_source: None,
				snapshot_filter: None,
				schedule: schedule.into(),
				schedule_jitter: TimeSpan(Span::new().seconds(0)),
				minimum_ttl,
				switchover_grace_period: TimeSpan(Span::new().minutes(5)),
				analytics_username: "analytics".into(),
				storage_class: None,
				storage_size_override: None,
				resources: None,
				shm_size_floor: None,
				service_annotations: None,
				pod_annotations: None,
				affinity: None,
				tolerations: Vec::new(),
				read_only: true,
				ephemeral: false,
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

	/// A replica that has never restored successfully triggers immediately so
	/// a freshly created one doesn't idle until the first cron tick. That must
	/// not survive a failure: without this, a replica failing every attempt
	/// retries as fast as it can fail, ignoring the backoff it just recorded.
	#[test]
	fn failure_backoff_applies_to_a_replica_that_never_restored() {
		let now = Timestamp::now();
		let mut replica = make_replica(
			"H * * * *",
			Some(now + SignedDuration::from_secs(600)),
			None,
			None,
		);
		replica
			.status
			.as_mut()
			.unwrap()
			.consecutive_restore_failures = Some(3);

		assert!(
			backoff_pending(replica.status.as_ref(), now),
			"a pending backoff must suppress the never-restored immediate trigger"
		);
	}

	/// The immediate trigger still has to work for a genuinely new replica,
	/// which has no failures and a cron-derived nextScheduledRestore.
	#[test]
	fn no_backoff_pending_for_a_fresh_replica() {
		let now = Timestamp::now();
		let replica = make_replica(
			"H * * * *",
			Some(now + SignedDuration::from_secs(600)),
			None,
			None,
		);

		assert!(!backoff_pending(replica.status.as_ref(), now));
	}

	/// Once the backoff window elapses the replica is free to retry.
	#[test]
	fn no_backoff_pending_once_the_window_elapsed() {
		let now = Timestamp::now();
		let mut replica = make_replica(
			"H * * * *",
			Some(now - SignedDuration::from_secs(1)),
			None,
			None,
		);
		replica
			.status
			.as_mut()
			.unwrap()
			.consecutive_restore_failures = Some(3);

		assert!(!backoff_pending(replica.status.as_ref(), now));
	}

	#[test]
	fn failure_backoff_progression() {
		// No failures → no backoff (cron applies).
		assert_eq!(failure_backoff_delay(0), None);
		// Doubles each step until clamped at 1h (6+ failures).
		assert_eq!(
			failure_backoff_delay(1),
			Some(SignedDuration::from_secs(120))
		);
		assert_eq!(
			failure_backoff_delay(2),
			Some(SignedDuration::from_secs(240))
		);
		assert_eq!(
			failure_backoff_delay(3),
			Some(SignedDuration::from_secs(480))
		);
		assert_eq!(
			failure_backoff_delay(4),
			Some(SignedDuration::from_secs(960))
		);
		assert_eq!(
			failure_backoff_delay(5),
			Some(SignedDuration::from_secs(1920))
		);
		// Capped at 1h from here on.
		assert_eq!(
			failure_backoff_delay(6),
			Some(SignedDuration::from_secs(3600))
		);
		assert_eq!(
			failure_backoff_delay(100),
			Some(SignedDuration::from_secs(3600))
		);
		// Absurd values must not overflow.
		assert_eq!(
			failure_backoff_delay(u32::MAX),
			Some(SignedDuration::from_secs(3600))
		);
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
	fn schema_migration_timeout_six_hourly_cron_is_twenty_percent() {
		// `0 */6 * * *` fires every 6h → 21600s → 20% = 4320s = 72min.
		let replica = make_replica("0 */6 * * *", None, None, None);
		let timeout = replica.schema_migration_timeout();
		assert_eq!(timeout, SignedDuration::from_secs(4320));
	}

	#[test]
	fn schema_migration_timeout_daily_cron_is_twenty_percent() {
		// Daily at midnight → 86400s → 20% = 17280s = 288min.
		let replica = make_replica("0 0 * * *", None, None, None);
		let timeout = replica.schema_migration_timeout();
		assert_eq!(timeout, SignedDuration::from_secs(17280));
	}

	#[test]
	fn schema_migration_timeout_falls_back_on_invalid_cron() {
		let replica = make_replica("not a cron", None, None, None);
		let timeout = replica.schema_migration_timeout();
		assert_eq!(timeout, SignedDuration::from_secs(3600));
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
