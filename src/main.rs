use std::sync::Arc;

use axum::{Router, routing::get};
use futures::StreamExt;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Api, Client,
    runtime::{controller::Controller, watcher::Config},
};
use prometheus::Encoder;
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

async fn read_max_concurrent_restores(client: &Client, namespace: &str) -> usize {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    match api.get(CONFIGMAP_NAME).await {
        Ok(cm) => cm
            .data
            .as_ref()
            .and_then(|d| d.get("maxConcurrentRestores"))
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| {
                info!("maxConcurrentRestores not set in ConfigMap, defaulting to {DEFAULT_MAX_CONCURRENT_RESTORES}");
                DEFAULT_MAX_CONCURRENT_RESTORES
            }),
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

    // Start metrics server
    let metrics_registry = ctx.metrics.registry.clone();
    let metrics_addr_clone = metrics_addr.clone();
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
            .route("/healthz", get(|| async { "ok" }))
            .route("/readyz", get(|| async { "ok" }));

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
