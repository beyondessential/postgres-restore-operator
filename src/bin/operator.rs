use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
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
		DEFAULT_KOPIA_IMAGE,
	},
	controllers::{self, canopy::intent::SUPPORTED as PGRO_SUPPORTED_INTENTS},
	types::{PostgresPhysicalReplica, PostgresPhysicalRestore},
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

async fn read_config(client: &Client, namespace: &str) -> (usize, String, bool) {
	let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
	match api.get(CONFIGMAP_NAME).await {
		Ok(cm) => (
			extract_max_concurrent_restores(&cm),
			extract_kopia_image(&cm),
			extract_use_port_forward(&cm),
		),
		Err(e) => {
			warn!(error = %e, "failed to read ConfigMap {CONFIGMAP_NAME}, using defaults");
			(
				DEFAULT_MAX_CONCURRENT_RESTORES,
				DEFAULT_KOPIA_IMAGE.to_string(),
				false,
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
	let (max_concurrent_restores, kopia_image, use_port_forward) =
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
	ctx.canopy = load_canopy_client(&client, &namespace)
		.await
		.unwrap_or_else(|err| {
			warn!(error = %err, "canopy client not configured; running in legacy-only mode");
			None
		});
	ctx.canopy_proxy_image = std::env::var("CANOPY_PROXY_IMAGE")
		.unwrap_or_else(|_| DEFAULT_CANOPY_PROXY_IMAGE.to_string());
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
	let max_concurrent_ref = ctx.max_concurrent_restores.clone();
	let kopia_image_ref = ctx.kopia_image.clone();
	let use_port_forward_ref = ctx.use_port_forward.clone();
	tokio::spawn(async move {
		let stream = watcher::watcher(config_api, config_watcher_config);
		futures::pin_mut!(stream);
		while let Some(event) = stream.next().await {
			match event {
				Ok(watcher::Event::Apply(cm) | watcher::Event::InitApply(cm)) => {
					let new_val = extract_max_concurrent_restores(&cm);
					let old_val = max_concurrent_ref.swap(new_val, Ordering::Relaxed);
					if old_val != new_val {
						info!(
							old = old_val,
							new = new_val,
							"max_concurrent_restores updated from ConfigMap"
						);
					}

					let new_image = extract_kopia_image(&cm);
					let mut image = kopia_image_ref.write().unwrap();
					if *image != new_image {
						info!(
							old = %*image,
							new = new_image,
							"kopia_image updated from ConfigMap"
						);
						*image = new_image;
					}

					let new_pf = extract_use_port_forward(&cm);
					let old_pf = use_port_forward_ref.swap(new_pf, Ordering::Relaxed);
					if old_pf != new_pf {
						info!(
							old = old_pf,
							new = new_pf,
							"use_port_forward updated from ConfigMap"
						);
					}
				}
				Ok(watcher::Event::Delete(_)) => {
					let old_val =
						max_concurrent_ref.swap(DEFAULT_MAX_CONCURRENT_RESTORES, Ordering::Relaxed);
					if old_val != DEFAULT_MAX_CONCURRENT_RESTORES {
						info!(
							old = old_val,
							new = DEFAULT_MAX_CONCURRENT_RESTORES,
							"ConfigMap deleted, reverted max_concurrent_restores to default"
						);
					}

					let mut image = kopia_image_ref.write().unwrap();
					if *image != DEFAULT_KOPIA_IMAGE {
						info!(
							old = %*image,
							new = DEFAULT_KOPIA_IMAGE,
							"ConfigMap deleted, reverted kopia_image to default"
						);
						*image = DEFAULT_KOPIA_IMAGE.to_string();
					}

					let old_pf = use_port_forward_ref.swap(false, Ordering::Relaxed);
					if old_pf {
						info!(
							old = old_pf,
							new = false,
							"ConfigMap deleted, reverted use_port_forward to default"
						);
					}
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
			controllers::replica::reconcile,
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
			controllers::restore::reconcile,
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
			"/api/v1/cache-pressure/{namespace}/{restore}",
			axum::routing::post(post_cache_pressure),
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
	let mut delay = Duration::from_secs(1);
	let max_delay = Duration::from_secs(300);
	for attempt in 1..=8u32 {
		match canopy.restore_capabilities(PGRO_SUPPORTED_INTENTS).await {
			Ok(_) => {
				info!(
					intents = ?PGRO_SUPPORTED_INTENTS,
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
#[derive(Clone)]
struct BrokerState {
	ctx: Arc<Context>,
	cache: Arc<tokio::sync::Mutex<std::collections::HashMap<(String, String), CachedCreds>>>,
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

	let key = (req.group.to_string(), req.r#type.clone());
	{
		let cache = state.cache.lock().await;
		if let Some(cached) = cache.get(&key)
			&& cached.expires_at > jiff::Timestamp::now()
		{
			return (StatusCode::OK, axum::Json(cached.body.clone()));
		}
	}

	match canopy.restore_credentials(&req.r#type, req.group).await {
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
