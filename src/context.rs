use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, Utc};
use kube::Client;
use tokio::sync::RwLock;

use crate::metrics::Metrics;

pub struct Context {
	pub client: Client,
	pub metrics: Metrics,
	pub restore_queue: Arc<RwLock<RestoreQueue>>,
	pub max_concurrent_restores: Arc<AtomicUsize>,
	pub http_client: reqwest::Client,
}

impl Context {
	pub fn new(client: Client, max_concurrent_restores: usize) -> Self {
		Self {
			client,
			metrics: Metrics::new(),
			restore_queue: Arc::new(RwLock::new(RestoreQueue::default())),
			max_concurrent_restores: Arc::new(AtomicUsize::new(max_concurrent_restores)),
			http_client: reqwest::Client::new(),
		}
	}

	pub fn max_concurrent_restores(&self) -> usize {
		self.max_concurrent_restores.load(Ordering::Relaxed)
	}

	/// Remove a restore from the queue, promote the next pending one if there
	/// is capacity, and update the related gauges. Returns the name of the
	/// promoted restore, if any.
	pub async fn release_restore_slot(&self, replica_name: &str) -> Option<String> {
		let mut queue = self.restore_queue.write().await;
		queue.remove(replica_name);
		let promoted = queue.try_promote(self.max_concurrent_restores());
		self.metrics.active_restores.set(queue.active.len() as i64);
		self.metrics.queue_depth.set(queue.pending.len() as i64);
		promoted
	}
}

#[derive(Default)]
pub struct RestoreQueue {
	/// Restore names currently running (in Restoring phase)
	pub active: Vec<String>,
	/// (restore name, created_at) for pending restores, FIFO order
	pub pending: Vec<(String, DateTime<Utc>)>,
}

impl RestoreQueue {
	pub fn can_start(&self, max: usize) -> bool {
		self.active.len() < max
	}

	/// Returns 1-based queue position, or None if not in the pending queue.
	pub fn position(&self, name: &str) -> Option<u32> {
		self.pending
			.iter()
			.position(|(n, _)| n == name)
			.map(|p| (p + 1) as u32)
	}

	pub fn enqueue(&mut self, name: String) {
		if !self.pending.iter().any(|(n, _)| n == &name) {
			self.pending.push((name, Utc::now()));
		}
	}

	pub fn mark_active(&mut self, name: &str) {
		self.pending.retain(|(n, _)| n != name);
		if !self.active.contains(&name.to_string()) {
			self.active.push(name.to_string());
		}
	}

	pub fn remove(&mut self, name: &str) {
		self.active.retain(|n| n != name);
		self.pending.retain(|(n, _)| n != name);
	}

	/// Promotes the next pending restore to active if there's capacity.
	/// Returns the name of the promoted restore, if any.
	pub fn try_promote(&mut self, max: usize) -> Option<String> {
		if self.active.len() < max && !self.pending.is_empty() {
			let (name, _) = self.pending.remove(0);
			self.active.push(name.clone());
			Some(name)
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn make_queue() -> RestoreQueue {
		RestoreQueue::default()
	}

	#[test]
	fn empty_queue_can_start() {
		let q = make_queue();
		assert!(q.can_start(3));
	}

	#[test]
	fn full_queue_cannot_start() {
		let mut q = make_queue();
		q.active = vec!["a".into(), "b".into(), "c".into()];
		assert!(!q.can_start(3));
	}

	#[test]
	fn position_returns_none_for_missing() {
		let q = make_queue();
		assert_eq!(q.position("nope"), None);
	}

	#[test]
	fn position_is_one_based() {
		let mut q = make_queue();
		q.enqueue("first".into());
		q.enqueue("second".into());
		q.enqueue("third".into());
		assert_eq!(q.position("first"), Some(1));
		assert_eq!(q.position("second"), Some(2));
		assert_eq!(q.position("third"), Some(3));
	}

	#[test]
	fn enqueue_deduplicates() {
		let mut q = make_queue();
		q.enqueue("a".into());
		q.enqueue("a".into());
		q.enqueue("a".into());
		assert_eq!(q.pending.len(), 1);
	}

	#[test]
	fn mark_active_moves_from_pending() {
		let mut q = make_queue();
		q.enqueue("restore-1".into());
		q.enqueue("restore-2".into());
		assert_eq!(q.pending.len(), 2);
		assert_eq!(q.active.len(), 0);

		q.mark_active("restore-1");
		assert_eq!(q.pending.len(), 1);
		assert_eq!(q.active.len(), 1);
		assert_eq!(q.active[0], "restore-1");
		assert_eq!(q.position("restore-1"), None);
	}

	#[test]
	fn mark_active_is_idempotent() {
		let mut q = make_queue();
		q.enqueue("a".into());
		q.mark_active("a");
		q.mark_active("a");
		assert_eq!(q.active.len(), 1);
	}

	#[test]
	fn remove_clears_from_both() {
		let mut q = make_queue();
		q.enqueue("pending-one".into());
		q.enqueue("active-one".into());
		q.mark_active("active-one");

		q.remove("pending-one");
		assert_eq!(q.pending.len(), 0);

		q.remove("active-one");
		assert_eq!(q.active.len(), 0);
	}

	#[test]
	fn try_promote_fifo_order() {
		let mut q = make_queue();
		q.enqueue("first".into());
		q.enqueue("second".into());
		q.enqueue("third".into());

		assert_eq!(q.try_promote(2), Some("first".into()));
		assert_eq!(q.try_promote(2), Some("second".into()));
		// Now at capacity (2 active)
		assert_eq!(q.try_promote(2), None);
	}

	#[test]
	fn try_promote_empty_returns_none() {
		let mut q = make_queue();
		assert_eq!(q.try_promote(5), None);
	}

	#[test]
	fn try_promote_at_capacity_returns_none() {
		let mut q = make_queue();
		q.active = vec!["a".into(), "b".into()];
		q.enqueue("c".into());
		assert_eq!(q.try_promote(2), None);
	}

	#[test]
	fn remove_then_promote_frees_slot() {
		let mut q = make_queue();
		q.enqueue("a".into());
		q.enqueue("b".into());
		q.mark_active("a");
		// a is active, b is pending, max=1
		assert_eq!(q.try_promote(1), None);
		q.remove("a");
		assert_eq!(q.try_promote(1), Some("b".into()));
	}
}
