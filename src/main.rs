use std::sync::Arc;

use axum::{Router, routing::get};
use clap::Parser;
use futures::StreamExt;
use kube::runtime::controller::Controller;
use kube::runtime::watcher::Config;
use kube::{Api, Client};
use prometheus::Encoder;
use tracing::info;

use postgres_restore_operator::context::Context;
use postgres_restore_operator::controllers;
use postgres_restore_operator::types::{PostgresPhysicalReplica, PostgresPhysicalRestore};

#[derive(Parser)]
#[command(name = "postgres-restore-operator")]
#[command(about = "Kubernetes operator for managing PostgreSQL restores from kopia snapshots")]
struct Args {
    /// Maximum number of concurrent restores
    #[arg(long, default_value = "2")]
    max_concurrent_restores: usize,

    /// Metrics server bind address
    #[arg(long, default_value = "0.0.0.0:9090")]
    metrics_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured JSON logging
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    info!(
        max_concurrent_restores = args.max_concurrent_restores,
        metrics_addr = args.metrics_addr,
        "starting postgres-restore-operator"
    );

    let client = Client::try_default().await?;
    let ctx = Arc::new(Context::new(client.clone(), args.max_concurrent_restores));

    // Start metrics server
    let metrics_registry = ctx.metrics.registry.clone();
    let metrics_addr = args.metrics_addr.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(move || {
                let registry = metrics_registry.clone();
                async move {
                    let mut buffer = Vec::new();
                    let encoder = prometheus::TextEncoder::new();
                    let metric_families = registry.gather();
                    encoder.encode(&metric_families, &mut buffer).unwrap();
                    String::from_utf8(buffer).unwrap()
                }
            }))
            .route("/healthz", get(|| async { "ok" }))
            .route("/readyz", get(|| async { "ok" }));

        let listener = tokio::net::TcpListener::bind(&metrics_addr)
            .await
            .expect("failed to bind metrics server");
        info!(addr = metrics_addr, "metrics server listening");
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
                // Map restore events to the parent replica
                let replica_name = restore.spec.replica.clone();
                let namespace = restore.metadata.namespace.clone();
                namespace.map(|ns| {
                    kube::runtime::reflector::ObjectRef::new(&replica_name).within(&ns)
                })
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

    // Run both controllers concurrently
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
