use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Router, routing::get};
use futures::StreamExt;
use jiff::Timestamp;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::Patch;
use kube::runtime::reflector::ObjectRef;
use kube::{
	Api, Client,
	runtime::{controller::Controller, watcher, watcher::Config},
};
use prometheus::Encoder;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

use postgres_restore_operator::{
	canopy::{self, DEFAULT_SOCKS5_PROXY},
	context::{
		Context, DEFAULT_CANOPY_PROXY_IMAGE, DEFAULT_DEPLOYMENT_READY_TIMEOUT_SECS,
		DEFAULT_KOPIA_IMAGE, ReplicaKey,
	},
	controllers::{self, canopy::intent},
	placement::PodPlacement,
	types::{PostgresPhysicalReplica, PostgresPhysicalRestore, RestorePhase},
};

// Use mimalloc instead of the default glibc allocator. Long-running Rust
// services on glibc commonly hold significantly more RSS than their actual
// live heap due to fragmentation and retained chunks; mimalloc keeps RSS
// closer to working-set size, which matters for an operator that runs
// indefinitely and was previously OOMKilled at a tight limit.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DEFAULT_MAX_CONCURRENT_RESTORES: usize = 2;
const DEFAULT_METRICS_ADDR: &str = "[::]:8080";
const DEFAULT_METRICS_PORT: u16 = 8080;
const DEFAULT_BROKER_ADDR: &str = "[::]:9091";
const DEFAULT_CANOPY_RECONCILE_INTERVAL_SECS: u64 = 30;
const CONFIGMAP_NAME: &str = "postgres-restore-operator-config";

/// Annotate the operator's own pod with the running version.
async fn annotate_own_pod(client: &Client, namespace: &str) {
	let pod_name = match std::env::var("HOSTNAME") {
		Ok(name) if !name.is_empty() => name,
		_ => {
			debug!("HOSTNAME not set, skipping pod self-annotation");
			return;
		}
	};

	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let patch = serde_json::json!({
		"metadata": {
			"annotations": {
				"pgro.bes.au/version": env!("CARGO_PKG_VERSION")
			}
		}
	});

	match pods
		.patch(&pod_name, &Default::default(), &Patch::Merge(&patch))
		.await
	{
		Ok(_) => info!(
			pod = pod_name,
			version = env!("CARGO_PKG_VERSION"),
			"annotated own pod with version"
		),
		Err(e) => warn!(pod = pod_name, error = %e, "failed to annotate own pod with version"),
	}
}

fn operator_namespace() -> String {
	if let Ok(ns) = std::env::var("OPERATOR_NAMESPACE") {
		return ns;
	}

	std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		.map(|s| s.trim().to_string())
		.unwrap_or_else(|_| "postgres-restore-operator".to_string())
}

fn extract_max_concurrent_restores(cm: &ConfigMap) -> usize {
	cm.data
		.as_ref()
		.and_then(|d| d.get("maxConcurrentRestores"))
		.and_then(|v| {
			v.parse::<usize>()
				.map_err(|e| {
					warn!(value = v, error = %e, "invalid maxConcurrentRestores in ConfigMap, using default");
					e
				})
				.ok()
		})
		.unwrap_or(DEFAULT_MAX_CONCURRENT_RESTORES)
}

fn extract_kopia_image(cm: &ConfigMap) -> String {
	cm.data
		.as_ref()
		.and_then(|d| d.get("kopiaImage"))
		.filter(|v| !v.is_empty())
		.cloned()
		.unwrap_or_else(|| DEFAULT_KOPIA_IMAGE.to_string())
}

fn extract_use_port_forward(cm: &ConfigMap) -> bool {
	cm.data
		.as_ref()
		.and_then(|d| d.get("usePortForward"))
		.is_some_and(|v| v == "true")
}

/// Scheduling defaults for every pod the operator creates. Absent keys give
/// the empty placement, which leaves pod specs exactly as they were before
/// this was configurable.
fn extract_pod_placement(cm: &ConfigMap) -> PodPlacement {
	let get = |key: &str| {
		cm.data
			.as_ref()
			.and_then(|d| d.get(key))
			.map_or("", String::as_str)
	};
	PodPlacement::parse(get("nodeSelector"), get("podAnnotations"))
}

/// The live-config slots on [`Context`] that the ConfigMap watcher writes.
///
/// Split out of the watcher body so the hot-reload path is testable without a
/// cluster: `apply` and `reset_to_defaults` are the whole of what the watcher
/// does with an event.
struct ConfigTargets {
	max_concurrent_restores: Arc<AtomicUsize>,
	kopia_image: Arc<RwLock<String>>,
	use_port_forward: Arc<AtomicBool>,
	pod_placement: Arc<RwLock<PodPlacement>>,
}

impl ConfigTargets {
	/// Adopt the values in `cm`, logging only what actually changed.
	fn apply(&self, cm: &ConfigMap) {
		let new_val = extract_max_concurrent_restores(cm);
		let old_val = self
			.max_concurrent_restores
			.swap(new_val, Ordering::Relaxed);
		if old_val != new_val {
			info!(
				old = old_val,
				new = new_val,
				"max_concurrent_restores updated from ConfigMap"
			);
		}

		let new_image = extract_kopia_image(cm);
		let mut image = self.kopia_image.write().unwrap();
		if *image != new_image {
			info!(old = %*image, new = new_image, "kopia_image updated from ConfigMap");
			*image = new_image;
		}

		let new_pf = extract_use_port_forward(cm);
		let old_pf = self.use_port_forward.swap(new_pf, Ordering::Relaxed);
		if old_pf != new_pf {
			info!(
				old = old_pf,
				new = new_pf,
				"use_port_forward updated from ConfigMap"
			);
		}

		let new_placement = extract_pod_placement(cm);
		let mut placement = self.pod_placement.write().unwrap();
		if *placement != new_placement {
			info!(
				node_selector = ?new_placement.node_selector,
				pod_annotations = ?new_placement.annotations,
				"pod_placement updated from ConfigMap"
			);
			*placement = new_placement;
		}
	}

	/// The ConfigMap went away, so every value it fed reverts to its built-in
	/// default. For placement that means pods stop carrying any scheduling
	/// constraint, which is worth a warning rather than an info.
	fn reset_to_defaults(&self) {
		let old_val = self
			.max_concurrent_restores
			.swap(DEFAULT_MAX_CONCURRENT_RESTORES, Ordering::Relaxed);
		if old_val != DEFAULT_MAX_CONCURRENT_RESTORES {
			info!(
				old = old_val,
				new = DEFAULT_MAX_CONCURRENT_RESTORES,
				"ConfigMap deleted, reverted max_concurrent_restores to default"
			);
		}

		let mut image = self.kopia_image.write().unwrap();
		if *image != DEFAULT_KOPIA_IMAGE {
			info!(
				old = %*image,
				new = DEFAULT_KOPIA_IMAGE,
				"ConfigMap deleted, reverted kopia_image to default"
			);
			*image = DEFAULT_KOPIA_IMAGE.to_string();
		}

		let old_pf = self.use_port_forward.swap(false, Ordering::Relaxed);
		if old_pf {
			info!(
				old = old_pf,
				new = false,
				"ConfigMap deleted, reverted use_port_forward to default"
			);
		}

		let mut placement = self.pod_placement.write().unwrap();
		if !placement.is_empty() {
			warn!(
				"ConfigMap deleted, cleared pod placement: created pods will land \
				 wherever the cluster's default lands them"
			);
			*placement = PodPlacement::default();
		}
	}
}

async fn read_config(client: &Client, namespace: &str) -> (usize, String, bool, PodPlacement) {
	let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
	match api.get(CONFIGMAP_NAME).await {
		Ok(cm) => (
			extract_max_concurrent_restores(&cm),
			extract_kopia_image(&cm),
			extract_use_port_forward(&cm),
			extract_pod_placement(&cm),
		),
		Err(e) => {
			warn!(error = %e, "failed to read ConfigMap {CONFIGMAP_NAME}, using defaults");
			(
				DEFAULT_MAX_CONCURRENT_RESTORES,
				DEFAULT_KOPIA_IMAGE.to_string(),
				false,
				PodPlacement::default(),
			)
		}
	}
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	tracing_subscriber::fmt()
		.json()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
		)
		.init();

	let metrics_addr =
		std::env::var("METRICS_ADDR").unwrap_or_else(|_| DEFAULT_METRICS_ADDR.to_string());

	let client = Client::try_default().await?;

	postgres_restore_operator::crd_install::ensure_crds(&client).await?;

	let namespace = operator_namespace();
	let (max_concurrent_restores, kopia_image, use_port_forward, pod_placement) =
		read_config(&client, &namespace).await;

	let callback_base_url = if let Ok(url) = std::env::var("CALLBACK_BASE_URL") {
		url
	} else if let Ok(svc) = std::env::var("OPERATOR_SERVICE_NAME") {
		let port: u16 = metrics_addr
			.rsplit_once(':')
			.and_then(|(_, p)| p.parse().ok())
			.unwrap_or(DEFAULT_METRICS_PORT);
		format!("http://{svc}.{namespace}.svc:{port}")
	} else {
		panic!(
			"either CALLBACK_BASE_URL or OPERATOR_SERVICE_NAME must be set \
			 so that snapshot-list jobs can POST results back to the operator"
		);
	};

	info!(
		max_concurrent_restores,
		kopia_image,
		use_port_forward,
		%callback_base_url,
		metrics_addr,
		operator_namespace = namespace,
		version = env!("CARGO_PKG_VERSION"),
		"starting postgres-restore-operator"
	);

	annotate_own_pod(&client, &namespace).await;

	let deployment_ready_timeout_secs = std::env::var("DEPLOYMENT_READY_TIMEOUT_SECS")
		.ok()
		.and_then(|v| {
			v.parse::<u64>()
				.map_err(
					|e| warn!(value = v, error = %e, "invalid DEPLOYMENT_READY_TIMEOUT_SECS, using default"),
				)
				.ok()
		})
		.unwrap_or(DEFAULT_DEPLOYMENT_READY_TIMEOUT_SECS);
	info!(
		deployment_ready_timeout_secs,
		"deployment readiness timeout configured"
	);

	let mut ctx = Context::new(
		client.clone(),
		max_concurrent_restores,
		kopia_image,
		use_port_forward,
		callback_base_url,
		deployment_ready_timeout_secs,
	);
	if pod_placement.is_empty() {
		warn!(
			"no nodeSelector or podAnnotations in ConfigMap {CONFIGMAP_NAME}: created pods carry no \
			 placement intent and land wherever the cluster's default lands them"
		);
	} else {
		info!(
			node_selector = ?pod_placement.node_selector,
			pod_annotations = ?pod_placement.annotations,
			"pod placement defaults configured"
		);
	}
	*ctx.pod_placement.write().unwrap() = pod_placement;

	ctx.canopy = load_canopy_client(&client, &namespace)
		.await
		.unwrap_or_else(|err| {
			warn!(error = %err, "canopy client not configured; running in legacy-only mode");
			None
		});
	ctx.canopy_proxy_image = std::env::var("CANOPY_PROXY_IMAGE")
		.unwrap_or_else(|_| DEFAULT_CANOPY_PROXY_IMAGE.to_string());
	// Path to the tailscale sidecar's LocalAPI socket (shared via an emptyDir),
	// used to resolve the tailnet MagicDNS suffix for the `url` semantic.
	// Defaults to containerboot's fixed location; set empty to disable.
	ctx.tailscaled_socket = match std::env::var("PGRO_TAILSCALED_SOCKET") {
		Ok(s) if s.is_empty() => None,
		Ok(s) => Some(s),
		Err(_) => Some("/var/run/tailscale/tailscaled.sock".to_string()),
	};
	ctx.canopy_broker_base_url = if let Ok(url) = std::env::var("CANOPY_BROKER_BASE_URL") {
		url
	} else if let Ok(svc) = std::env::var("OPERATOR_SERVICE_NAME") {
		// Broker listens on its own port; parse from PGRO_BROKER_LISTEN_ADDR
		// (default [::]:9091).
		let broker_addr = std::env::var("PGRO_BROKER_LISTEN_ADDR")
			.unwrap_or_else(|_| DEFAULT_BROKER_ADDR.to_string());
		let broker_port: u16 = broker_addr
			.rsplit_once(':')
			.and_then(|(_, p)| p.parse().ok())
			.unwrap_or(9091);
		format!("http://{svc}.{namespace}.svc:{broker_port}")
	} else {
		String::new()
	};
	let ctx = Arc::new(ctx);

	// Heartbeat: a background task updates this timestamp every 5s.
	// If the runtime is deadlocked, the timestamp goes stale and /livez fails.
	let heartbeat = Arc::new(AtomicI64::new(Timestamp::now().as_second()));
	let heartbeat_writer = heartbeat.clone();
	tokio::spawn(async move {
		let mut interval = tokio::time::interval(Duration::from_secs(5));
		loop {
			interval.tick().await;
			heartbeat_writer.store(Timestamp::now().as_second(), Ordering::Relaxed);
		}
	});

	// Watch the operator ConfigMap for dynamic config updates
	let config_api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
	let config_watcher_config =
		Config::default().fields(&format!("metadata.name={CONFIGMAP_NAME}"));
	let config_targets = ConfigTargets {
		max_concurrent_restores: ctx.max_concurrent_restores.clone(),
		kopia_image: ctx.kopia_image.clone(),
		use_port_forward: ctx.use_port_forward.clone(),
		pod_placement: ctx.pod_placement.clone(),
	};
	tokio::spawn(async move {
		let stream = watcher::watcher(config_api, config_watcher_config);
		futures::pin_mut!(stream);
		while let Some(event) = stream.next().await {
			match event {
				Ok(watcher::Event::Apply(cm) | watcher::Event::InitApply(cm)) => {
					config_targets.apply(&cm);
				}
				Ok(watcher::Event::Delete(_)) => {
					config_targets.reset_to_defaults();
				}
				Ok(watcher::Event::Init | watcher::Event::InitDone) => {}
				Err(e) => {
					warn!(error = %e, "ConfigMap watcher error, will retry");
				}
			}
		}
		warn!("ConfigMap watcher stream ended unexpectedly");
	});

	// Start metrics / API server
	let metrics_registry = ctx.metrics.registry.clone();
	let metrics_addr_clone = metrics_addr.clone();
	let server_state = ServerState {
		heartbeat,
		ctx: ctx.clone(),
	};
	tokio::spawn(async move {
		let app = build_router(server_state, metrics_registry);

		let listener = tokio::net::TcpListener::bind(&metrics_addr_clone)
			.await
			.expect("failed to bind server");
		info!(addr = metrics_addr_clone, "server listening");
		if let Err(e) = axum::serve(listener, app).await {
			tracing::error!(error = %e, "server exited with error");
		}
	});

	// Register pgro's supported intents with canopy + start the worklist
	// syncer. Only runs when a canopy client was successfully constructed.
	if ctx.canopy.is_some() {
		let register_ctx = ctx.clone();
		tokio::spawn(async move {
			register_capabilities(register_ctx).await;
		});

		let interval_secs = std::env::var("CANOPY_RECONCILE_INTERVAL_SECS")
			.ok()
			.and_then(|v| v.parse::<u64>().ok())
			.unwrap_or(DEFAULT_CANOPY_RECONCILE_INTERVAL_SECS);
		let syncer_ctx = ctx.clone();
		tokio::spawn(async move {
			let syncer = controllers::canopy::CanopyController::new(
				syncer_ctx,
				Duration::from_secs(interval_secs),
			);
			syncer.run_forever().await;
			warn!("canopy worklist syncer exited");
		});
	}

	// Broker HTTP server on a separate listener/port, gated by NetworkPolicy
	// to accept only the proxy sidecars in canopy-backed Job pods.
	let broker_addr = std::env::var("PGRO_BROKER_LISTEN_ADDR")
		.unwrap_or_else(|_| DEFAULT_BROKER_ADDR.to_string());
	let broker_ctx = ctx.clone();
	tokio::spawn(async move {
		let state = BrokerState::new(broker_ctx);
		let app = broker_router(state);
		match tokio::net::TcpListener::bind(&broker_addr).await {
			Ok(listener) => {
				info!(addr = broker_addr, "credential broker listening");
				if let Err(e) = axum::serve(listener, app).await {
					tracing::error!(error = %e, "broker exited with error");
				}
			}
			Err(e) => tracing::error!(error = %e, addr = broker_addr, "broker bind failed"),
		}
	});

	// Reconcile the in-memory restore queue against observed cluster state.
	// Slots are released explicitly on failure, switchover and ephemeral
	// teardown, but a restore deleted while Restoring bypasses all three and
	// leaks its slot permanently — the queue only clears on restart, and at
	// the default limit of 2 a pair of leaked slots stalls the whole fleet.
	// Freeing a slot a little early is far cheaper than that, so this runs on
	// a slow interval and lets transient races settle.
	let queue_ctx = ctx.clone();
	tokio::spawn(async move {
		let mut interval = tokio::time::interval(Duration::from_secs(60));
		let restores: Api<PostgresPhysicalRestore> = Api::all(queue_ctx.client.clone());
		loop {
			interval.tick().await;
			let Ok(list) = restores
				.list(&Default::default())
				.await
				.inspect_err(|error| {
					warn!(%error, "could not list restores to reconcile the queue");
				})
			else {
				continue;
			};
			let live: HashSet<ReplicaKey> = list
				.items
				.iter()
				.filter(|r| {
					matches!(
						r.status.as_ref().and_then(|s| s.phase.as_ref()),
						Some(&RestorePhase::Restoring)
					)
				})
				.filter_map(|r| {
					Some(ReplicaKey::new(
						r.metadata.namespace.as_deref()?,
						&r.spec.replica.name,
					))
				})
				.collect();

			let mut queue = queue_ctx.restore_queue.write().await;
			let dropped = queue.retain_active(&live);
			if !dropped.is_empty() {
				let freed: Vec<String> = dropped.iter().map(ToString::to_string).collect();
				warn!(
					?freed,
					active = queue.active.len(),
					"released restore queue slots with no live restore"
				);
			}
			// Deliberately no try_promote here: `active` mirrors observed
			// state, and promoting would put a key back with no restore
			// behind it for the next tick to drop again. Freeing the slot is
			// enough — each replica re-checks capacity on its own reconcile.
			queue_ctx
				.metrics
				.active_restores
				.set(queue.active.len() as i64);
			queue_ctx
				.metrics
				.queue_depth
				.set(queue.pending.len() as i64);
		}
	});

	// Start controllers
	let replica_api: Api<PostgresPhysicalReplica> = Api::all(client.clone());
	let restore_api: Api<PostgresPhysicalRestore> = Api::all(client.clone());

	let replica_ctx = ctx.clone();
	let restore_ctx = ctx.clone();

	let replica_controller = Controller::new(replica_api, Config::default())
		.watches(
			Api::<PostgresPhysicalRestore>::all(client.clone()),
			Config::default(),
			|restore| {
				let replica_name = restore.spec.replica.name.clone();
				let namespace = restore.metadata.namespace.clone();
				namespace.map(|ns| ObjectRef::new(&replica_name).within(&ns))
			},
		)
		// Scope the Job watch to pgro-owned Jobs. Without a label selector
		// kube-rs caches every Job in every namespace (CI runners, batch
		// jobs, cert-manager, etc.) in the in-memory store, which scales
		// with cluster activity rather than pgro's working set. Limiting
		// to Jobs that carry the pgro.bes.au/replica label cuts that to
		// only restore Jobs, snapshot-list Jobs, schema-migration Jobs,
		// and credential-reset Jobs — all of which the operator builds.
		.watches(
			Api::<Job>::all(client.clone()),
			Config::default().labels("pgro.bes.au/replica"),
			|job| {
				let labels = job.metadata.labels.as_ref()?;
				let replica_name = labels.get("pgro.bes.au/replica")?;
				let namespace = job.metadata.namespace.as_ref()?;
				Some(ObjectRef::new(replica_name).within(namespace))
			},
		)
		.run(
			|obj, ctx| controllers::catching_panics(controllers::replica::reconcile(obj, ctx)),
			controllers::replica::error_policy,
			replica_ctx,
		)
		.for_each(|res| async {
			match res {
				Ok((_obj, _action)) => {}
				Err(e) => tracing::warn!(error = %e, "replica controller error"),
			}
		});

	let restore_controller = Controller::new(restore_api, Config::default())
		.run(
			|obj, ctx| controllers::catching_panics(controllers::restore::reconcile(obj, ctx)),
			controllers::restore::error_policy,
			restore_ctx,
		)
		.for_each(|res| async {
			match res {
				Ok((_obj, _action)) => {}
				Err(e) => tracing::warn!(error = %e, "restore controller error"),
			}
		});

	info!("controllers started");

	tokio::select! {
		_ = replica_controller => {
			tracing::error!("replica controller exited unexpectedly");
		}
		_ = restore_controller => {
			tracing::error!("restore controller exited unexpectedly");
		}
	}

	Ok(())
}

#[derive(Clone)]
struct ServerState {
	heartbeat: Arc<AtomicI64>,
	ctx: Arc<Context>,
}

fn build_router(state: ServerState, metrics_registry: prometheus::Registry) -> Router {
	Router::new()
		.route(
			"/metrics",
			get(move || {
				let registry = metrics_registry.clone();
				async move {
					let mut buffer = Vec::new();
					let encoder = prometheus::TextEncoder::new();
					let metric_families = registry.gather();
					encoder.encode(&metric_families, &mut buffer).unwrap();
					String::from_utf8(buffer).unwrap()
				}
			}),
		)
		.route("/livez", get(livez))
		.route("/readyz", get(readyz))
		.route(
			"/api/v1/snapshot-results/{namespace}/{replica}",
			axum::routing::post(post_snapshot_results),
		)
		.route(
			"/api/v1/schema-migration-results/{namespace}/{replica}",
			axum::routing::post(post_schema_migration_results),
		)
		.route(
			"/api/v1/schema-build-results/{namespace}/{replica}",
			axum::routing::post(post_schema_build_results),
		)
		.route(
			"/api/v1/cache-pressure/{namespace}/{restore}",
			axum::routing::post(post_cache_pressure),
		)
		.route(
			"/api/v1/canopy-progress/{namespace}/{job}",
			axum::routing::post(post_canopy_progress),
		)
		.route(
			"/api/v1/canopy-stats/{namespace}/{job}",
			axum::routing::post(post_canopy_stats),
		)
		.with_state(state)
		.layer(TraceLayer::new_for_http())
}

/// Accept snapshot-list JSON POSTed by a job.
async fn post_snapshot_results(
	State(state): State<ServerState>,
	Path((namespace, replica)): Path<(String, String)>,
	body: String,
) -> StatusCode {
	info!(
		namespace = namespace,
		replica = replica,
		bytes = body.len(),
		"received snapshot results callback"
	);
	state.ctx.snapshot_results.store(&namespace, &replica, body);
	StatusCode::NO_CONTENT
}

async fn post_schema_migration_results(
	State(state): State<ServerState>,
	Path((namespace, replica)): Path<(String, String)>,
	body: String,
) -> StatusCode {
	info!(
		namespace = namespace,
		replica = replica,
		bytes = body.len(),
		"received schema migration results callback"
	);
	state
		.ctx
		.schema_migration_results
		.store(&namespace, &replica, body);
	StatusCode::NO_CONTENT
}

async fn post_schema_build_results(
	State(state): State<ServerState>,
	Path((namespace, replica)): Path<(String, String)>,
	body: String,
) -> StatusCode {
	info!(
		namespace = namespace,
		replica = replica,
		bytes = body.len(),
		"received reporting schema build callback"
	);
	state
		.ctx
		.schema_build_results
		.store(&namespace, &replica, body);
	StatusCode::NO_CONTENT
}

/// Accept the cache-pressure callback a restore Job POSTs when its
/// pre-flight check had to evict cache content. Bumps the replica's
/// cache PVC requested storage so chronically-pressured replicas
/// self-tune over a few restore cycles.
async fn post_cache_pressure(
	State(state): State<ServerState>,
	Path((namespace, restore)): Path<(String, String)>,
) -> StatusCode {
	info!(
		namespace = namespace,
		restore = restore,
		"received cache-pressure callback"
	);
	controllers::restore::grow_cache_pvc_after_pressure(&state.ctx.client, &namespace, &restore)
		.await;
	StatusCode::NO_CONTENT
}

/// Forward a canopy-proxy sidecar's in-flight progress sample to canopy.
///
/// The sidecar has the traffic counters but no canopy credentials — those live
/// only here — so it posts to the operator and the operator relays. The sample
/// is self-describing (it carries its own `run_id` and type), so this needs no
/// Kubernetes lookup.
///
/// Best-effort telemetry: a rejected or failed relay is logged and the restore
/// is unaffected. Always answers the sidecar with 204 so a canopy outage can't
/// turn into sidecar retry pressure.
async fn post_canopy_progress(
	State(state): State<ServerState>,
	Path((namespace, job)): Path<(String, String)>,
	body: String,
) -> StatusCode {
	let sample: canopy::ProgressSample = match serde_json::from_str(&body) {
		Ok(sample) => sample,
		Err(error) => {
			warn!(%namespace, %job, %error, "malformed canopy progress sample, dropping");
			return StatusCode::NO_CONTENT;
		}
	};
	let Some(client) = state.ctx.canopy.as_ref() else {
		debug!(%namespace, %job, "canopy not configured, dropping progress sample");
		return StatusCode::NO_CONTENT;
	};

	debug!(
		%namespace,
		%job,
		run_id = %sample.run_id,
		received_raw_bytes = sample.received_raw_bytes,
		"relaying canopy progress sample"
	);
	if let Err(error) = client.backup_progress(&sample.to_args()).await {
		debug!(%namespace, %job, %error, "posting progress to canopy failed (ignored)");
	}
	StatusCode::NO_CONTENT
}

/// Accept the canopy-proxy sidecar's final TrafficStats POST on shutdown.
/// The body is opaque JSON — the canopy notification target deserializes
/// it when building the RestoreVerification.
async fn post_canopy_stats(
	State(state): State<ServerState>,
	Path((namespace, job)): Path<(String, String)>,
	body: String,
) -> StatusCode {
	info!(
		namespace = namespace,
		job = job,
		bytes = body.len(),
		"received canopy-proxy stats callback"
	);
	state.ctx.canopy_stats.store(&namespace, &job, body);
	StatusCode::NO_CONTENT
}

/// Liveness: checks that the async runtime isn't deadlocked by verifying
/// a background heartbeat was updated within the last 30 seconds, and that
/// the reconciliation loop has run at least once in the last 5 minutes.
async fn livez(State(state): State<ServerState>) -> (StatusCode, &'static str) {
	let now = Timestamp::now().as_second();

	let heartbeat_last = state.heartbeat.load(Ordering::Relaxed);
	let heartbeat_age = now - heartbeat_last;
	if heartbeat_age > 30 {
		tracing::warn!(
			heartbeat_age_secs = heartbeat_age,
			"liveness check failed: heartbeat stale"
		);
		return (StatusCode::INTERNAL_SERVER_ERROR, "heartbeat stale");
	}

	let reconcile_last = state.ctx.last_reconcile.load(Ordering::Relaxed);
	let reconcile_age = now - reconcile_last;
	if reconcile_age > 300 {
		tracing::warn!(
			reconcile_age_secs = reconcile_age,
			"liveness check failed: no reconciliation in the last 5 minutes"
		);
		return (StatusCode::INTERNAL_SERVER_ERROR, "reconcile loop stale");
	}

	debug!(
		heartbeat_age_secs = heartbeat_age,
		reconcile_age_secs = reconcile_age,
		"livez ok"
	);
	(StatusCode::OK, "ok")
}

/// Try to build the canopy client from env config. `Ok(None)` means the
/// integration is intentionally not configured (no `CANOPY_BASE_URL`); pgro
/// runs in legacy-only mode. `Err(_)` means configuration was attempted but
/// failed — logged and downgraded to `None` at the call site.
async fn load_canopy_client(
	client: &Client,
	operator_namespace: &str,
) -> anyhow::Result<Option<Arc<canopy::Client>>> {
	let Ok(base_url_str) = std::env::var("CANOPY_BASE_URL") else {
		return Ok(None);
	};
	let base_url = reqwest::Url::parse(&base_url_str)
		.map_err(|e| anyhow::anyhow!("CANOPY_BASE_URL is not a valid URL: {e}"))?;

	let socks5_proxy =
		std::env::var("CANOPY_SOCKS5_PROXY").unwrap_or_else(|_| DEFAULT_SOCKS5_PROXY.to_string());

	let device_key_pem = if let Ok(secret_name) = std::env::var("CANOPY_DEVICE_CERT_SECRET") {
		let secrets: Api<k8s_openapi::api::core::v1::Secret> =
			Api::namespaced(client.clone(), operator_namespace);
		let sec = secrets.get(&secret_name).await?;
		let data = sec.data.and_then(|d| d.into_iter().next());
		match data {
			Some((_, v)) => Some(String::from_utf8(v.0).map_err(|e| {
				anyhow::anyhow!("device cert secret {secret_name} not valid UTF-8: {e}")
			})?),
			None => {
				warn!(secret = secret_name, "device cert secret has no data");
				None
			}
		}
	} else {
		None
	};

	let cfg = canopy::CanopyConfig {
		base_url,
		socks5_proxy,
		device_key_pem,
	};
	let cli = canopy::Client::from_config(Some(cfg)).await?;
	Ok(cli.map(Arc::new))
}

/// POST `/restore-capabilities` to canopy with the intents pgro implements.
/// Retries on transient failure with exponential-ish backoff up to ~5 min;
/// past that, gives up and logs — the next operator restart will re-attempt.
async fn register_capabilities(ctx: Arc<Context>) {
	let Some(canopy) = ctx.canopy.as_ref() else {
		return;
	};
	let descriptors = intent::descriptors();
	let intent_names: Vec<&str> = descriptors.iter().map(|d| d.intent.as_str()).collect();
	let mut delay = Duration::from_secs(1);
	let max_delay = Duration::from_secs(300);
	for attempt in 1..=8u32 {
		match canopy.restore_capabilities(&descriptors).await {
			Ok(_) => {
				info!(
					intents = ?intent_names,
					"registered supported intents with canopy"
				);
				return;
			}
			Err(err) => {
				warn!(
					attempt,
					error = %err,
					"canopy restore_capabilities failed; retrying"
				);
				tokio::time::sleep(delay).await;
				delay = std::cmp::min(delay * 2, max_delay);
			}
		}
	}
	warn!("gave up registering supported intents with canopy after 8 attempts");
}

/// State passed to the credential-broker Router. Holds the operator's canopy
/// client and a per-(group, type) cache so concurrent Job sidecars don't
/// multiply upstream canopy calls.
/// Broker creds cache key: `(group, type, run_id)`. Including the run_id keeps
/// within-run STS refreshes on a cache hit while giving each restore run its
/// own canopy-attributed credentials.
type CredsCacheKey = (String, String, Option<uuid::Uuid>);

#[derive(Clone)]
struct BrokerState {
	ctx: Arc<Context>,
	cache: Arc<tokio::sync::Mutex<std::collections::HashMap<CredsCacheKey, CachedCreds>>>,
}

#[derive(Clone)]
struct CachedCreds {
	body: serde_json::Value,
	/// Cached response expires this long before the STS creds' own expiry
	/// so the sidecar's next refresh call gets a fresh cache miss.
	expires_at: jiff::Timestamp,
}

impl BrokerState {
	fn new(ctx: Arc<Context>) -> Self {
		Self {
			ctx,
			cache: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
		}
	}
}

fn broker_router(state: BrokerState) -> Router {
	Router::new()
		.route(
			"/internal/restore-creds",
			axum::routing::post(post_restore_creds),
		)
		.route("/healthz", get(|| async { StatusCode::OK }))
		.with_state(state)
		.layer(TraceLayer::new_for_http())
}

#[derive(serde::Deserialize)]
struct BrokerCredsRequest {
	group: uuid::Uuid,
	r#type: String,
	/// Canopy run-uuid of the restore run this sidecar serves, forwarded to
	/// canopy so its credential grant is attributed to the run. Optional to
	/// tolerate an older sidecar (mid-rollout) that doesn't send one.
	#[serde(default)]
	run_id: Option<uuid::Uuid>,
}

/// Broker endpoint the proxy sidecar hits to refresh its STS creds. Forwards
/// to canopy's `POST /restore-credentials`, caches the response per-(group,
/// type) up to 2 minutes before its expiry. 4xx failures propagate the
/// upstream status verbatim so a missing external-restore grant surfaces
/// clearly at the sidecar.
async fn post_restore_creds(
	State(state): State<BrokerState>,
	axum::Json(req): axum::Json<BrokerCredsRequest>,
) -> (StatusCode, axum::Json<serde_json::Value>) {
	let Some(canopy) = state.ctx.canopy.as_ref() else {
		return (
			StatusCode::SERVICE_UNAVAILABLE,
			axum::Json(serde_json::json!({
				"error": "canopy client not configured on operator",
			})),
		);
	};

	let key = (req.group.to_string(), req.r#type.clone(), req.run_id);
	{
		let cache = state.cache.lock().await;
		if let Some(cached) = cache.get(&key)
			&& cached.expires_at > jiff::Timestamp::now()
		{
			return (StatusCode::OK, axum::Json(cached.body.clone()));
		}
	}

	match canopy
		.restore_credentials(&req.r#type, req.group, req.run_id)
		.await
	{
		Ok(resp) => {
			let expires_at = resp
				.credentials
				.expiration
				.checked_sub(jiff::SignedDuration::from_secs(120))
				.unwrap_or(resp.credentials.expiration);
			// bestool-canopy's RestoreCredentials only derives Deserialize; build
			// the response JSON manually with the same shape the sidecar expects.
			let body = serde_json::json!({
				"credentials": {
					"Version": resp.credentials.version,
					"AccessKeyId": resp.credentials.access_key_id,
					"SecretAccessKey": resp.credentials.secret_access_key.0,
					"SessionToken": resp.credentials.session_token.0,
					"Expiration": resp.credentials.expiration.to_string(),
				},
				"repo_password": resp.repo_password.0,
			});
			let mut cache = state.cache.lock().await;
			cache.insert(
				key,
				CachedCreds {
					body: body.clone(),
					expires_at,
				},
			);
			(StatusCode::OK, axum::Json(body))
		}
		Err(err) => {
			warn!(error = %err, group = %req.group, r#type = %req.r#type, "broker: canopy restore_credentials failed");
			(
				StatusCode::BAD_GATEWAY,
				axum::Json(serde_json::json!({ "error": err.to_string() })),
			)
		}
	}
}

/// Readiness: checks that the heartbeat is fresh (runtime is responsive).
async fn readyz(State(state): State<ServerState>) -> (StatusCode, &'static str) {
	let last = state.heartbeat.load(Ordering::Relaxed);
	let age = Timestamp::now().as_second() - last;
	if age <= 30 {
		debug!(heartbeat_age_secs = age, "readyz ok");
		(StatusCode::OK, "ok")
	} else {
		tracing::warn!(
			heartbeat_age_secs = age,
			"readiness check failed: heartbeat stale"
		);
		(StatusCode::SERVICE_UNAVAILABLE, "not ready")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn dummy_client() -> kube::Client {
		let svc = tower::service_fn(|_req: http::Request<kube::client::Body>| async {
			Ok::<_, std::convert::Infallible>(http::Response::new(http_body_util::Empty::<
				bytes::Bytes,
			>::new()))
		});
		kube::Client::new(svc, "default")
	}

	fn configmap(data: Option<&[(&str, &str)]>) -> ConfigMap {
		ConfigMap {
			data: data.map(|pairs| {
				pairs
					.iter()
					.map(|(k, v)| ((*k).to_string(), (*v).to_string()))
					.collect()
			}),
			..Default::default()
		}
	}

	#[test]
	fn pod_placement_from_configmap_keys() {
		let cm = configmap(Some(&[
			("nodeSelector", "bes.node.purpose=workload"),
			("podAnnotations", "karpenter.sh/do-not-disrupt=true"),
			("kopiaImage", "kopia/kopia:1.2.3"),
		]));
		let placement = extract_pod_placement(&cm);
		assert_eq!(
			placement.node_selector.get("bes.node.purpose").unwrap(),
			"workload"
		);
		assert_eq!(
			placement
				.annotations
				.get("karpenter.sh/do-not-disrupt")
				.unwrap(),
			"true"
		);
	}

	/// A ConfigMap that sets other keys but neither placement key, and one with
	/// no `data` at all, must both leave pods exactly as they were before
	/// placement was configurable.
	#[test]
	fn absent_placement_keys_give_the_empty_placement() {
		assert!(
			extract_pod_placement(&configmap(Some(&[("maxConcurrentRestores", "2")]))).is_empty()
		);
		assert!(extract_pod_placement(&configmap(None)).is_empty());
		assert!(extract_pod_placement(&configmap(Some(&[]))).is_empty());
	}

	/// Only one placement key set is a normal configuration, not a broken one.
	#[test]
	fn placement_keys_are_independent() {
		let selector_only = extract_pod_placement(&configmap(Some(&[("nodeSelector", "a=b")])));
		assert_eq!(selector_only.node_selector.len(), 1);
		assert!(selector_only.annotations.is_empty());

		let annotations_only =
			extract_pod_placement(&configmap(Some(&[("podAnnotations", "a=b")])));
		assert!(annotations_only.node_selector.is_empty());
		assert_eq!(annotations_only.annotations.len(), 1);
	}

	/// An unreadable ConfigMap must not leave the operator guessing: the
	/// fallback is the empty placement, same as no keys.
	#[tokio::test]
	async fn read_config_falls_back_to_the_empty_placement() {
		let (_, _, _, placement) = read_config(&dummy_client(), "default").await;
		assert!(placement.is_empty());
	}

	fn targets() -> ConfigTargets {
		ConfigTargets {
			max_concurrent_restores: Arc::new(AtomicUsize::new(DEFAULT_MAX_CONCURRENT_RESTORES)),
			kopia_image: Arc::new(RwLock::new(DEFAULT_KOPIA_IMAGE.to_string())),
			use_port_forward: Arc::new(AtomicBool::new(false)),
			pod_placement: Arc::new(RwLock::new(PodPlacement::default())),
		}
	}

	/// Editing the ConfigMap has to take effect without an operator restart —
	/// that is the whole reason placement lives here rather than in env vars.
	#[test]
	fn applying_a_configmap_updates_placement_in_place() {
		let targets = targets();
		targets.apply(&configmap(Some(&[
			("nodeSelector", "bes.node.purpose=workload"),
			("podAnnotations", "karpenter.sh/do-not-disrupt=true"),
		])));

		let placement = targets.pod_placement.read().unwrap();
		assert_eq!(
			placement.node_selector.get("bes.node.purpose").unwrap(),
			"workload"
		);
		assert_eq!(
			placement
				.annotations
				.get("karpenter.sh/do-not-disrupt")
				.unwrap(),
			"true"
		);
	}

	/// Removing the key from a still-present ConfigMap has to clear the
	/// placement, not leave the last-known value latched.
	#[test]
	fn removing_the_key_clears_placement() {
		let targets = targets();
		targets.apply(&configmap(Some(&[("nodeSelector", "a=b")])));
		assert!(!targets.pod_placement.read().unwrap().is_empty());

		targets.apply(&configmap(Some(&[("kopiaImage", "kopia/kopia:1.2.3")])));
		assert!(targets.pod_placement.read().unwrap().is_empty());
	}

	/// Deleting the ConfigMap reverts every value it fed, placement included.
	#[test]
	fn deleting_the_configmap_reverts_every_value() {
		let targets = targets();
		targets.apply(&configmap(Some(&[
			("nodeSelector", "a=b"),
			("maxConcurrentRestores", "7"),
			("kopiaImage", "kopia/kopia:1.2.3"),
			("usePortForward", "true"),
		])));

		targets.reset_to_defaults();

		assert!(targets.pod_placement.read().unwrap().is_empty());
		assert_eq!(
			targets.max_concurrent_restores.load(Ordering::Relaxed),
			DEFAULT_MAX_CONCURRENT_RESTORES
		);
		assert_eq!(*targets.kopia_image.read().unwrap(), DEFAULT_KOPIA_IMAGE);
		assert!(!targets.use_port_forward.load(Ordering::Relaxed));
	}

	#[tokio::test]
	async fn router_is_constructible() {
		let client = dummy_client();
		let ctx = Arc::new(Context::new(
			client,
			DEFAULT_MAX_CONCURRENT_RESTORES,
			DEFAULT_KOPIA_IMAGE.to_string(),
			false,
			"http://test.svc:8080".to_string(),
			DEFAULT_DEPLOYMENT_READY_TIMEOUT_SECS,
		));
		let heartbeat = Arc::new(AtomicI64::new(Timestamp::now().as_second()));
		let state = ServerState { heartbeat, ctx };
		let registry = prometheus::Registry::new();

		let _router = build_router(state, registry);
	}
}
