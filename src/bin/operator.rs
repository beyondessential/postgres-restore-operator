use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::{Router, routing::get};
use futures::StreamExt;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
	Api, Client,
	runtime::{controller::Controller, watcher, watcher::Config},
};
use prometheus::Encoder;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

use postgres_restore_operator::{
	context::Context,
	controllers,
	types::{PostgresPhysicalReplica, PostgresPhysicalRestore},
};

const DEFAULT_MAX_CONCURRENT_RESTORES: usize = 2;
const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:8080";
const CONFIGMAP_NAME: &str = "postgres-restore-operator-config";

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

async fn read_max_concurrent_restores(client: &Client, namespace: &str) -> usize {
	let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
	match api.get(CONFIGMAP_NAME).await {
		Ok(cm) => extract_max_concurrent_restores(&cm),
		Err(e) => {
			warn!(error = %e, "failed to read ConfigMap {CONFIGMAP_NAME}, defaulting to {DEFAULT_MAX_CONCURRENT_RESTORES}");
			DEFAULT_MAX_CONCURRENT_RESTORES
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

	let namespace = operator_namespace();
	let max_concurrent_restores = read_max_concurrent_restores(&client, &namespace).await;

	info!(
		max_concurrent_restores,
		metrics_addr,
		operator_namespace = namespace,
		"starting postgres-restore-operator"
	);

	let ctx = Arc::new(Context::new(client.clone(), max_concurrent_restores));

	// Heartbeat: a background task updates this timestamp every 5s.
	// If the runtime is deadlocked, the timestamp goes stale and /livez fails.
	let heartbeat = Arc::new(AtomicI64::new(chrono::Utc::now().timestamp()));
	let heartbeat_writer = heartbeat.clone();
	tokio::spawn(async move {
		let mut interval = tokio::time::interval(Duration::from_secs(5));
		loop {
			interval.tick().await;
			heartbeat_writer.store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
		}
	});

	// Watch the operator ConfigMap for dynamic config updates
	let config_api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
	let config_watcher_config =
		Config::default().fields(&format!("metadata.name={CONFIGMAP_NAME}"));
	let max_concurrent_ref = ctx.max_concurrent_restores.clone();
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
				}
				Ok(watcher::Event::Init | watcher::Event::InitDone) => {}
				Err(e) => {
					warn!(error = %e, "ConfigMap watcher error, will retry");
				}
			}
		}
		warn!("ConfigMap watcher stream ended unexpectedly");
	});

	// Start metrics server
	let metrics_registry = ctx.metrics.registry.clone();
	let metrics_addr_clone = metrics_addr.clone();
	let probe_state = ProbeState { heartbeat };
	tokio::spawn(async move {
		let app = Router::new()
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
			.with_state(probe_state)
			.layer(TraceLayer::new_for_http());

		let listener = tokio::net::TcpListener::bind(&metrics_addr_clone)
			.await
			.expect("failed to bind metrics server");
		info!(addr = metrics_addr_clone, "metrics server listening");
		axum::serve(listener, app).await.unwrap();
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
				let replica_name = restore.spec.replica.clone();
				let namespace = restore.metadata.namespace.clone();
				namespace
					.map(|ns| kube::runtime::reflector::ObjectRef::new(&replica_name).within(&ns))
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
struct ProbeState {
	heartbeat: Arc<AtomicI64>,
}

/// Liveness: checks that the async runtime isn't deadlocked by verifying
/// a background heartbeat was updated within the last 30 seconds.
async fn livez(State(state): State<ProbeState>) -> (StatusCode, &'static str) {
	let last = state.heartbeat.load(Ordering::Relaxed);
	let age = chrono::Utc::now().timestamp() - last;
	if age <= 30 {
		(StatusCode::OK, "ok")
	} else {
		tracing::warn!(
			heartbeat_age_secs = age,
			"liveness check failed: heartbeat stale"
		);
		(StatusCode::INTERNAL_SERVER_ERROR, "heartbeat stale")
	}
}

/// Readiness: checks that the heartbeat is fresh (runtime is responsive).
async fn readyz(State(state): State<ProbeState>) -> (StatusCode, &'static str) {
	let last = state.heartbeat.load(Ordering::Relaxed);
	let age = chrono::Utc::now().timestamp() - last;
	if age <= 30 {
		(StatusCode::OK, "ok")
	} else {
		tracing::warn!(
			heartbeat_age_secs = age,
			"readiness check failed: heartbeat stale"
		);
		(StatusCode::SERVICE_UNAVAILABLE, "not ready")
	}
}
