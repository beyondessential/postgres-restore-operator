use std::sync::Arc;

use chrono::{DateTime, Utc};
use kube::Client;
use tokio::sync::RwLock;

use crate::metrics::Metrics;

pub struct Context {
    pub client: Client,
    pub metrics: Metrics,
    pub restore_queue: Arc<RwLock<RestoreQueue>>,
    pub max_concurrent_restores: usize,
    pub http_client: reqwest::Client,
}

impl Context {
    pub fn new(client: Client, max_concurrent_restores: usize) -> Self {
        Self {
            client,
            metrics: Metrics::new(),
            restore_queue: Arc::new(RwLock::new(RestoreQueue::default())),
            max_concurrent_restores,
            http_client: reqwest::Client::new(),
        }
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
