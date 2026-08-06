use std::{
	collections::HashSet,
	sync::{
		Arc, RwLock,
		atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
	},
};

use jiff::Timestamp;
use kube::Client;
use kube::runtime::events::{Recorder, Reporter};

use crate::{controllers::jobs::CallbackStore, metrics::Metrics, placement::PodPlacement};

pub const DEFAULT_KOPIA_IMAGE: &str = "kopia/kopia:0.22.3";
pub const DEFAULT_DEPLOYMENT_READY_TIMEOUT_SECS: u64 = 30 * 60;
/// The canopy-proxy sidecar binary ships in the same image as the operator
/// (Containerfile copies both binaries). By default, use the same image tag;
/// operators can override with `CANOPY_PROXY_IMAGE` if they want to pin the
/// sidecar to a different tag from the operator itself.
pub const DEFAULT_CANOPY_PROXY_IMAGE: &str =
	"ghcr.io/beyondessential/postgres-restore-operator:latest";

pub struct Context {
	pub client: Client,
	pub metrics: Metrics,
	pub recorder: Recorder,
	pub restore_queue: Arc<tokio::sync::RwLock<RestoreQueue>>,
	pub max_concurrent_restores: Arc<AtomicUsize>,
	pub kopia_image: Arc<RwLock<String>>,
	pub use_port_forward: Arc<AtomicBool>,
	/// Scheduling defaults stamped onto every pod the operator creates, from
	/// the operator ConfigMap. Empty by default, which reproduces the
	/// pre-existing "no placement intent at all" behaviour.
	pub pod_placement: Arc<RwLock<PodPlacement>>,
	pub http_client: reqwest::Client,
	/// Canopy integration client — `None` when the operator is running in
	/// legacy-only mode (no canopy config provided). Populated at startup
	/// by [`crate::canopy::Client::from_config`].
	pub canopy: Option<Arc<crate::canopy::Client>>,
	/// Base URL the canopy-path proxy sidecar hits for STS creds, e.g.
	/// `http://postgres-restore-operator.pgro-system.svc:9091`. Empty when
	/// canopy is not configured.
	pub canopy_broker_base_url: String,
	/// Image reference for the pgro-canopy-proxy sidecar container. Set
	/// by the operator startup from `CANOPY_PROXY_IMAGE`.
	pub canopy_proxy_image: String,
	/// In-memory store for snapshot-list results POSTed by jobs.
	pub snapshot_results: Arc<CallbackStore>,
	/// In-memory store for schema migration results POSTed by jobs.
	pub schema_migration_results: Arc<CallbackStore>,
	/// In-memory store for canopy-proxy sidecar TrafficStats keyed by
	/// `{namespace}/{job}`. Written on sidecar exit via the operator's
	/// `/api/v1/canopy-stats/...` callback; read by the canopy
	/// notification target when building `RestoreVerification.s3_*_bytes`.
	pub canopy_stats: Arc<CallbackStore>,
	/// Base URL the operator is reachable at from within the cluster,
	/// e.g. `http://postgres-restore-operator.pgro-system.svc:8080`.
	pub callback_base_url: String,
	/// Seconds to wait for a restore's postgres Deployment to become Ready
	/// after the kopia restore Job completes, before marking the restore
	/// Failed. Configurable via the `DEPLOYMENT_READY_TIMEOUT_SECS` env
	/// var so it can be raised for replicas with large data dirs (slower
	/// WAL replay) without needing a code release.
	pub deployment_ready_timeout_secs: u64,
	/// Unix timestamp of the last successful entry into a reconcile function.
	/// Used by `/livez` to detect a stuck reconciliation loop.
	pub last_reconcile: Arc<AtomicI64>,
	/// Filesystem path to the tailscale sidecar's LocalAPI unix socket, used
	/// to look up the tailnet MagicDNS suffix for the `url` semantic. `None`
	/// disables URL reporting. Set at startup from `PGRO_TAILSCALED_SOCKET`.
	pub tailscaled_socket: Option<String>,
	/// Cached tailnet MagicDNS suffix. The suffix is constant per tailnet, so
	/// it's fetched once from the LocalAPI and reused.
	pub magic_dns_suffix: Arc<tokio::sync::RwLock<Option<String>>>,
}

impl Context {
	pub fn new(
		client: Client,
		max_concurrent_restores: usize,
		kopia_image: String,
		use_port_forward: bool,
		callback_base_url: String,
		deployment_ready_timeout_secs: u64,
	) -> Self {
		let reporter = Reporter::from("postgres-restore-operator");
		let recorder = Recorder::new(client.clone(), reporter);

		Self {
			client,
			metrics: Metrics::new(),
			recorder,
			restore_queue: Arc::new(tokio::sync::RwLock::new(RestoreQueue::default())),
			max_concurrent_restores: Arc::new(AtomicUsize::new(max_concurrent_restores)),
			kopia_image: Arc::new(RwLock::new(kopia_image)),
			use_port_forward: Arc::new(AtomicBool::new(use_port_forward)),
			pod_placement: Arc::new(RwLock::new(PodPlacement::default())),
			http_client: reqwest::Client::new(),
			canopy: None,
			canopy_broker_base_url: String::new(),
			canopy_proxy_image: DEFAULT_CANOPY_PROXY_IMAGE.to_string(),
			snapshot_results: Arc::new(CallbackStore::default()),
			schema_migration_results: Arc::new(CallbackStore::default()),
			canopy_stats: Arc::new(CallbackStore::default()),
			callback_base_url,
			deployment_ready_timeout_secs,
			last_reconcile: Arc::new(AtomicI64::new(Timestamp::now().as_second())),
			tailscaled_socket: None,
			magic_dns_suffix: Arc::new(tokio::sync::RwLock::new(None)),
		}
	}

	/// The tailnet MagicDNS suffix, fetched from the tailscale sidecar's
	/// LocalAPI socket and cached. `None` when no socket is configured or the
	/// lookup fails — callers then omit the replica URL.
	pub async fn magic_dns_suffix(&self) -> Option<String> {
		if let Some(cached) = self.magic_dns_suffix.read().await.clone() {
			return Some(cached);
		}
		let socket = self.tailscaled_socket.as_deref()?;
		let suffix = crate::tailscale::magic_dns_suffix(socket).await?;
		*self.magic_dns_suffix.write().await = Some(suffix.clone());
		Some(suffix)
	}

	pub fn max_concurrent_restores(&self) -> usize {
		self.max_concurrent_restores.load(Ordering::Relaxed)
	}

	pub fn kopia_image(&self) -> String {
		self.kopia_image.read().unwrap().clone()
	}

	pub fn use_port_forward(&self) -> bool {
		self.use_port_forward.load(Ordering::Relaxed)
	}

	pub fn pod_placement(&self) -> PodPlacement {
		self.pod_placement.read().unwrap().clone()
	}

	/// Build the full callback URL for a snapshot-list job to POST results to.
	pub fn snapshot_callback_url(&self, namespace: &str, replica: &str) -> String {
		format!(
			"{}/api/v1/snapshot-results/{namespace}/{replica}",
			self.callback_base_url
		)
	}

	/// Build the callback URL for a schema migration job to POST results to.
	pub fn schema_migration_callback_url(&self, namespace: &str, replica: &str) -> String {
		format!(
			"{}/api/v1/schema-migration-results/{namespace}/{replica}",
			self.callback_base_url
		)
	}

	/// Build the callback URL a restore Job hits when it had to evict cache
	/// content pre-flight (PGRO_CACHE_PRESSURE). Keyed off the restore name
	/// — the handler fetches the restore to get its replica + snapshot size
	/// before patching the cache PVC.
	pub fn cache_pressure_callback_url(&self, namespace: &str, restore: &str) -> String {
		format!(
			"{}/api/v1/cache-pressure/{namespace}/{restore}",
			self.callback_base_url
		)
	}

	/// Callback URL the canopy-proxy sidecar POSTs its final TrafficStats
	/// to on shutdown. Keyed by `{namespace}/{job}` so the reporter can
	/// look them up when building `RestoreVerification`.
	pub fn canopy_stats_callback_url(&self, namespace: &str, job: &str) -> String {
		format!(
			"{}/api/v1/canopy-stats/{namespace}/{job}",
			self.callback_base_url
		)
	}

	/// Callback URL the canopy-proxy sidecar POSTs periodic progress samples
	/// to while a restore is in flight. Separate from the stats callback so
	/// the once-per-run verification path is unaffected by the sampling.
	pub fn canopy_progress_callback_url(&self, namespace: &str, job: &str) -> String {
		format!(
			"{}/api/v1/canopy-progress/{namespace}/{job}",
			self.callback_base_url
		)
	}

	/// Remove a restore from the queue, promote the next pending one if there
	/// is capacity, and update the related gauges. Returns the promoted
	/// replica, if any.
	pub async fn release_restore_slot(&self, key: &ReplicaKey) -> Option<ReplicaKey> {
		let mut queue = self.restore_queue.write().await;
		queue.remove(key);
		let promoted = queue.try_promote(self.max_concurrent_restores());
		self.metrics.active_restores.set(queue.active.len() as i64);
		self.metrics.queue_depth.set(queue.pending.len() as i64);
		promoted
	}
}

/// Identifies a replica across the whole cluster.
///
/// Replica CRs are named per-intent rather than per-site — every
/// canopy-managed replica is called `canopy-replica` — so a bare name is not
/// unique. This exists so the namespace can't be left out at a call site.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicaKey(String);

impl ReplicaKey {
	pub fn new(namespace: &str, name: &str) -> Self {
		Self(format!("{namespace}/{name}"))
	}
}

impl std::fmt::Display for ReplicaKey {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.0)
	}
}

#[derive(Default)]
pub struct RestoreQueue {
	/// Replicas whose restore is currently running (in Restoring phase)
	pub active: Vec<ReplicaKey>,
	/// (replica, created_at) for pending restores, FIFO order
	pub pending: Vec<(ReplicaKey, Timestamp)>,
}

impl RestoreQueue {
	pub fn can_start(&self, max: usize) -> bool {
		self.active.len() < max
	}

	/// Returns 1-based queue position, or None if not in the pending queue.
	pub fn position(&self, key: &ReplicaKey) -> Option<u32> {
		self.pending
			.iter()
			.position(|(k, _)| k == key)
			.map(|p| (p + 1) as u32)
	}

	pub fn enqueue(&mut self, key: ReplicaKey) {
		if !self.pending.iter().any(|(k, _)| k == &key) {
			self.pending.push((key, Timestamp::now()));
		}
	}

	pub fn mark_active(&mut self, key: &ReplicaKey) {
		self.pending.retain(|(k, _)| k != key);
		if !self.active.contains(key) {
			self.active.push(key.clone());
		}
	}

	pub fn remove(&mut self, key: &ReplicaKey) {
		self.active.retain(|k| k != key);
		self.pending.retain(|(k, _)| k != key);
	}

	/// Drop active slots that no longer correspond to a live restore,
	/// returning the ones dropped.
	///
	/// Slots are released explicitly on failure, switchover and ephemeral
	/// teardown, but a restore deleted while Restoring bypasses all three and
	/// leaks its slot. The queue is in-memory, so a leak persists until the
	/// operator restarts — and at the default limit of 2, two leaked slots
	/// stall every replica in the cluster. Reconciling against observed state
	/// heals that regardless of how the slot was lost.
	pub fn retain_active(&mut self, live: &HashSet<ReplicaKey>) -> Vec<ReplicaKey> {
		let (kept, dropped) = std::mem::take(&mut self.active)
			.into_iter()
			.partition(|k| live.contains(k));
		self.active = kept;
		dropped
	}

	/// Promotes the next pending restore to active if there's capacity.
	/// Returns the promoted replica, if any.
	pub fn try_promote(&mut self, max: usize) -> Option<ReplicaKey> {
		if self.active.len() < max && !self.pending.is_empty() {
			let (key, _) = self.pending.remove(0);
			self.active.push(key.clone());
			Some(key)
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

	fn key(name: &str) -> ReplicaKey {
		ReplicaKey::new("ns", name)
	}

	#[test]
	fn empty_queue_can_start() {
		let q = make_queue();
		assert!(q.can_start(3));
	}

	/// Replica CRs are named per-intent, not per-site: every canopy-managed
	/// replica is called `canopy-replica`. Keying the queue on the bare name
	/// collapses the whole fleet onto one slot, so a handful of occupied
	/// slots starve every other namespace indefinitely.
	#[test]
	fn same_name_in_different_namespaces_does_not_share_a_slot() {
		let mut q = make_queue();
		let first = ReplicaKey::new("site-a", "canopy-replica");
		let second = ReplicaKey::new("site-b", "canopy-replica");

		q.mark_active(&first);
		q.mark_active(&second);
		assert_eq!(
			q.active.len(),
			2,
			"distinct replicas must hold distinct slots"
		);

		q.remove(&first);
		assert_eq!(
			q.active,
			vec![second],
			"releasing one replica must not free another's slot"
		);
	}

	/// The queue lives in memory and is only released on failure, switchover
	/// or ephemeral teardown. A restore deleted while Restoring takes its slot
	/// with it, and nothing short of an operator restart gets it back — two
	/// leaked slots at the default limit starve the entire fleet.
	#[test]
	fn retain_active_drops_slots_with_no_live_restore() {
		let mut q = make_queue();
		q.mark_active(&key("gone"));
		q.mark_active(&key("live"));

		let dropped = q.retain_active(&[key("live")].into_iter().collect());

		assert_eq!(dropped, vec![key("gone")]);
		assert_eq!(q.active, vec![key("live")]);
	}

	#[test]
	fn retain_active_keeps_everything_when_all_are_live() {
		let mut q = make_queue();
		q.mark_active(&key("a"));
		q.mark_active(&key("b"));

		let dropped = q.retain_active(&[key("a"), key("b")].into_iter().collect());

		assert!(dropped.is_empty());
		assert_eq!(q.active.len(), 2);
	}

	#[test]
	fn enqueue_deduplicates_per_namespace() {
		let mut q = make_queue();
		q.enqueue(ReplicaKey::new("ns-a", "canopy-replica"));
		q.enqueue(ReplicaKey::new("ns-a", "canopy-replica"));
		q.enqueue(ReplicaKey::new("ns-b", "canopy-replica"));
		assert_eq!(q.pending.len(), 2);
	}

	#[test]
	fn full_queue_cannot_start() {
		let mut q = make_queue();
		q.active = vec![key("a"), key("b"), key("c")];
		assert!(!q.can_start(3));
	}

	#[test]
	fn position_returns_none_for_missing() {
		let q = make_queue();
		assert_eq!(q.position(&key("nope")), None);
	}

	#[test]
	fn position_is_one_based() {
		let mut q = make_queue();
		q.enqueue(key("first"));
		q.enqueue(key("second"));
		q.enqueue(key("third"));
		assert_eq!(q.position(&key("first")), Some(1));
		assert_eq!(q.position(&key("second")), Some(2));
		assert_eq!(q.position(&key("third")), Some(3));
	}

	#[test]
	fn enqueue_deduplicates() {
		let mut q = make_queue();
		q.enqueue(key("a"));
		q.enqueue(key("a"));
		q.enqueue(key("a"));
		assert_eq!(q.pending.len(), 1);
	}

	#[test]
	fn mark_active_moves_from_pending() {
		let mut q = make_queue();
		q.enqueue(key("restore-1"));
		q.enqueue(key("restore-2"));
		assert_eq!(q.pending.len(), 2);
		assert_eq!(q.active.len(), 0);

		q.mark_active(&key("restore-1"));
		assert_eq!(q.pending.len(), 1);
		assert_eq!(q.active.len(), 1);
		assert_eq!(q.active[0], key("restore-1"));
		assert_eq!(q.position(&key("restore-1")), None);
	}

	#[test]
	fn mark_active_is_idempotent() {
		let mut q = make_queue();
		q.enqueue(key("a"));
		q.mark_active(&key("a"));
		q.mark_active(&key("a"));
		assert_eq!(q.active.len(), 1);
	}

	#[test]
	fn remove_clears_from_both() {
		let mut q = make_queue();
		q.enqueue(key("pending-one"));
		q.enqueue(key("active-one"));
		q.mark_active(&key("active-one"));

		q.remove(&key("pending-one"));
		assert_eq!(q.pending.len(), 0);

		q.remove(&key("active-one"));
		assert_eq!(q.active.len(), 0);
	}

	#[test]
	fn try_promote_fifo_order() {
		let mut q = make_queue();
		q.enqueue(key("first"));
		q.enqueue(key("second"));
		q.enqueue(key("third"));

		assert_eq!(q.try_promote(2), Some(key("first")));
		assert_eq!(q.try_promote(2), Some(key("second")));
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
		q.active = vec![key("a"), key("b")];
		q.enqueue(key("c"));
		assert_eq!(q.try_promote(2), None);
	}

	#[test]
	fn remove_then_promote_frees_slot() {
		let mut q = make_queue();
		q.enqueue(key("a"));
		q.enqueue(key("b"));
		q.mark_active(&key("a"));
		// a is active, b is pending, max=1
		assert_eq!(q.try_promote(1), None);
		q.remove(&key("a"));
		assert_eq!(q.try_promote(1), Some(key("b")));
	}
}
