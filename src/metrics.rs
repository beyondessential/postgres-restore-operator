use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,

    // Controller metrics
    pub reconciliations_total: IntCounterVec,
    pub reconciliation_errors_total: IntCounterVec,
    pub reconciliation_duration: Histogram,

    // Queue metrics
    pub queue_depth: IntGauge,
    pub active_restores: IntGauge,

    // Restore metrics
    pub restores_started_total: IntCounter,
    pub restores_completed_total: IntCounter,
    pub restores_failed_total: IntCounter,
    pub switchovers_total: IntCounter,

    // Notification metrics
    pub notifications_sent_total: IntCounterVec,
    pub notifications_failed_total: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let reconciliations_total = IntCounterVec::new(
            Opts::new(
                "postgres_restore_reconciliations_total",
                "Total reconciliations by controller",
            ),
            &["controller"],
        )
        .unwrap();

        let reconciliation_errors_total = IntCounterVec::new(
            Opts::new(
                "postgres_restore_reconciliation_errors_total",
                "Total reconciliation errors by controller",
            ),
            &["controller"],
        )
        .unwrap();

        let reconciliation_duration = Histogram::with_opts(
            HistogramOpts::new(
                "postgres_restore_reconciliation_duration_seconds",
                "Duration of reconciliation in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]),
        )
        .unwrap();

        let queue_depth = IntGauge::new(
            "postgres_restore_queue_depth",
            "Number of restores waiting in queue",
        )
        .unwrap();

        let active_restores = IntGauge::new(
            "postgres_restore_active_count",
            "Number of restores currently running",
        )
        .unwrap();

        let restores_started_total = IntCounter::new(
            "postgres_restore_started_total",
            "Total restores started",
        )
        .unwrap();

        let restores_completed_total = IntCounter::new(
            "postgres_restore_completed_total",
            "Total restores completed successfully",
        )
        .unwrap();

        let restores_failed_total = IntCounter::new(
            "postgres_restore_failed_total",
            "Total restores that failed",
        )
        .unwrap();

        let switchovers_total = IntCounter::new(
            "postgres_restore_switchovers_total",
            "Total blue-green switchovers completed",
        )
        .unwrap();

        let notifications_sent_total = IntCounterVec::new(
            Opts::new(
                "postgres_restore_notifications_sent_total",
                "Total notifications sent",
            ),
            &["name", "event"],
        )
        .unwrap();

        let notifications_failed_total = IntCounterVec::new(
            Opts::new(
                "postgres_restore_notifications_failed_total",
                "Total notifications that failed",
            ),
            &["name", "event"],
        )
        .unwrap();

        registry
            .register(Box::new(reconciliations_total.clone()))
            .unwrap();
        registry
            .register(Box::new(reconciliation_errors_total.clone()))
            .unwrap();
        registry
            .register(Box::new(reconciliation_duration.clone()))
            .unwrap();
        registry.register(Box::new(queue_depth.clone())).unwrap();
        registry
            .register(Box::new(active_restores.clone()))
            .unwrap();
        registry
            .register(Box::new(restores_started_total.clone()))
            .unwrap();
        registry
            .register(Box::new(restores_completed_total.clone()))
            .unwrap();
        registry
            .register(Box::new(restores_failed_total.clone()))
            .unwrap();
        registry
            .register(Box::new(switchovers_total.clone()))
            .unwrap();
        registry
            .register(Box::new(notifications_sent_total.clone()))
            .unwrap();
        registry
            .register(Box::new(notifications_failed_total.clone()))
            .unwrap();

        Self {
            registry,
            reconciliations_total,
            reconciliation_errors_total,
            reconciliation_duration,
            queue_depth,
            active_restores,
            restores_started_total,
            restores_completed_total,
            restores_failed_total,
            switchovers_total,
            notifications_sent_total,
            notifications_failed_total,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
