use jiff::{SignedDuration, Timestamp};

/// How long a locale reindex holding readiness may go without its progress
/// counters moving before the restore is failed. Every phase of a concurrent
/// index build advances a counter except the in-memory sorts, which on the
/// largest tables measured so far finish well inside this.
pub const STALL_TIMEOUT: SignedDuration = SignedDuration::from_secs(30 * 60);

/// Minimum gap between two queries of the same restore's progress, so the
/// usual 10s requeue doesn't open a database connection every time.
pub const OBSERVE_INTERVAL: SignedDuration = SignedDuration::from_secs(60);

/// A watch nobody has looked at for this long belongs to a restore that is no
/// longer waiting (deleted, superseded); a live wait refreshes it every
/// [`OBSERVE_INTERVAL`].
const ABANDONED_AFTER: SignedDuration = SignedDuration::from_secs(10 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watch {
	/// Opaque rendering of the locale reindex's progress, compared only for
	/// change. `None` when no locale reindex is holding readiness.
	progress: Option<String>,
	observed_at: Timestamp,
	changed_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
	Wait,
	NotReindexing,
	Stalled,
}

impl Watch {
	/// Fold a fresh observation into the previous one. The stall clock
	/// restarts whenever the progress changed.
	pub fn record(previous: Option<&Watch>, progress: Option<String>, now: Timestamp) -> Self {
		let changed_at = match previous {
			Some(prev) if prev.progress == progress => prev.changed_at,
			_ => now,
		};
		Self {
			progress,
			observed_at: now,
			changed_at,
		}
	}

	/// An observation that failed. It reads as no change, so a transient
	/// error neither fails a reindex that was moving nor rescues one that had
	/// stalled; the stall clock decides.
	pub fn unobserved(previous: &Watch, now: Timestamp) -> Self {
		Self {
			observed_at: now,
			..previous.clone()
		}
	}

	pub fn needs_observation(&self, now: Timestamp) -> bool {
		now.duration_since(self.observed_at) >= OBSERVE_INTERVAL
	}

	pub fn is_abandoned(&self, now: Timestamp) -> bool {
		now.duration_since(self.observed_at) > ABANDONED_AFTER
	}

	pub fn verdict(&self, now: Timestamp) -> Verdict {
		if self.progress.is_none() {
			Verdict::NotReindexing
		} else if now.duration_since(self.changed_at) > STALL_TIMEOUT {
			Verdict::Stalled
		} else {
			Verdict::Wait
		}
	}
}

/// What to do with a restore whose Deployment is past its readiness `budget`:
/// `None` keeps waiting, `Some` is the failure message. `verdict` is `None`
/// when the reindex was not consulted, because an explicit timeout is the
/// last word.
pub fn past_budget(
	elapsed: SignedDuration,
	budget: SignedDuration,
	verdict: Option<Verdict>,
) -> Option<String> {
	let not_ready = format!(
		"postgres Deployment was not Ready {}s after restore completion (budget {}s)",
		elapsed.as_secs(),
		budget.as_secs()
	);
	match verdict {
		Some(Verdict::Wait) => None,
		Some(Verdict::Stalled) => Some(format!(
			"{not_ready}; its locale reindex made no progress for {}s",
			STALL_TIMEOUT.as_secs()
		)),
		Some(Verdict::NotReindexing) | None => Some(not_ready),
	}
}
