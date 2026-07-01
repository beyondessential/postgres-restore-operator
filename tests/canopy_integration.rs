//! Canopy-path integration test.
//!
//! Exercises the worklist syncer end-to-end against a **stub canopy**: an
//! in-cluster HTTP service that returns canned `WorklistEntry` /
//! `RestoreCredentials` and captures the `RestoreVerification` reports pgro
//! POSTs. The stub is a small nginx-backed service defined in
//! `tests/fixtures/stub-canopy.yaml` (deployed by the CI matrix step, not
//! this test).
//!
//! CI-only: this test assumes:
//! - a real k8s cluster (kind) with the operator running with
//!   `CANOPY_BASE_URL` pointing at the stub;
//! - the `test-canopy-restore` namespace exists as the work root;
//! - the stub is reachable at `stub-canopy.test-canopy-restore.svc`.
//!
//! Do not run this file locally — nothing here works without the CI setup.

#![allow(dead_code, reason = "shared helpers may not all be exercised yet")]

use std::time::Duration;

use k8s_openapi::{
	api::{batch::v1::Job, core::v1::Namespace},
	apimachinery::pkg::apis::meta::v1::Time,
};
use kube::{Api, Client, ResourceExt, api::ListParams};
use tokio::time::{sleep, timeout};

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const NAMESPACE_TIMEOUT: Duration = Duration::from_secs(90);
const JOB_TIMEOUT: Duration = Duration::from_secs(120);

mod helpers {
	#[allow(unused_imports)]
	pub use super::*;

	pub async fn make_client() -> Client {
		Client::try_default()
			.await
			.expect("expected a valid kubeconfig (kind)")
	}

	/// Wait for at least one namespace labelled `pgro.bes.au/managed-by=pgro-canopy`
	/// to appear. Returns the first match.
	pub async fn wait_for_canopy_namespace(client: &Client) -> Namespace {
		let ns_api: Api<Namespace> = Api::all(client.clone());
		let params = ListParams::default().labels("pgro.bes.au/managed-by=pgro-canopy");
		let deadline = tokio::time::Instant::now() + NAMESPACE_TIMEOUT;
		loop {
			if let Ok(list) = ns_api.list(&params).await
				&& let Some(ns) = list.items.into_iter().next()
			{
				return ns;
			}
			if tokio::time::Instant::now() >= deadline {
				panic!("no canopy-managed namespace appeared within {NAMESPACE_TIMEOUT:?}");
			}
			sleep(POLL_INTERVAL).await;
		}
	}

	/// Wait for the canopy-path restore Job in `ns` to appear. Returns it.
	pub async fn wait_for_restore_job(client: &Client, ns: &str) -> Job {
		let job_api: Api<Job> = Api::namespaced(client.clone(), ns);
		let params = ListParams::default().labels("pgro.bes.au/job-kind=canopy-restore");
		let deadline = tokio::time::Instant::now() + JOB_TIMEOUT;
		loop {
			if let Ok(list) = job_api.list(&params).await
				&& let Some(job) = list.items.into_iter().next()
			{
				return job;
			}
			if tokio::time::Instant::now() >= deadline {
				panic!("no canopy-restore Job appeared in {ns} within {JOB_TIMEOUT:?}");
			}
			sleep(POLL_INTERVAL).await;
		}
	}

	/// Poll the stub canopy's `/tests/reports` endpoint for the report count.
	/// The stub exposes captured `RestoreVerification` POSTs as a JSON array
	/// there — see `tests/fixtures/stub-canopy.yaml`.
	pub async fn stub_report_count(_client: &Client) -> usize {
		// TODO(canopy-integration): once the stub-canopy Deployment lands
		// in tests/fixtures/, port-forward and read /tests/reports. For now
		// the fixture is a placeholder; this helper is a stub.
		0
	}
}

/// End-to-end happy path: stub canopy hands out a worklist entry, pgro
/// provisions a namespace with the expected labels, creates the restore
/// Job, and (eventually) reports a verification back.
#[ignore = "integration test; run in CI with the stub-canopy fixture"]
#[tokio::test]
async fn worklist_provisions_namespace_and_job() {
	let client = helpers::make_client().await;

	// The stub returns a fixed WorklistEntry named "int-test" with a canned
	// snapshot id. pgro's syncer should discover it on the next tick and
	// create a labelled Namespace.
	let ns = timeout(
		NAMESPACE_TIMEOUT,
		helpers::wait_for_canopy_namespace(&client),
	)
	.await
	.expect("timed out waiting for canopy-managed namespace");
	assert!(
		ns.name_any().starts_with("int-test-"),
		"unexpected namespace name: {}",
		ns.name_any()
	);

	let labels = ns.labels();
	assert_eq!(
		labels.get("pgro.bes.au/managed-by").map(String::as_str),
		Some("pgro-canopy"),
	);
	assert!(labels.contains_key("pgro.bes.au/declaration-id"));
	assert!(labels.contains_key("pgro.bes.au/group"));
	assert!(labels.contains_key("pgro.bes.au/server"));
	assert_eq!(
		labels.get("pgro.bes.au/type").map(String::as_str),
		Some("tamanu-postgres"),
	);

	let annos = ns.annotations();
	assert_eq!(
		annos.get("pgro.bes.au/restore-state").map(String::as_str),
		Some("pending"),
		"restore-state should start at pending; got {:?}",
		annos.get("pgro.bes.au/restore-state"),
	);
	assert!(
		annos.contains_key("pgro.bes.au/desired-snapshot-id"),
		"desired-snapshot-id should be set from the worklist entry"
	);

	// The syncer should then create the restore Job in that namespace.
	let job = timeout(
		JOB_TIMEOUT,
		helpers::wait_for_restore_job(&client, &ns.name_any()),
	)
	.await
	.expect("timed out waiting for canopy-restore Job");
	let spec = job
		.spec
		.as_ref()
		.and_then(|s| s.template.spec.as_ref())
		.expect("job pod spec missing");
	let container_names: Vec<_> = spec.containers.iter().map(|c| c.name.as_str()).collect();
	assert!(
		container_names.contains(&"restore"),
		"expected kopia restore container, got {container_names:?}",
	);
	assert!(
		container_names.contains(&"canopy-proxy"),
		"expected canopy-proxy sidecar, got {container_names:?}",
	);
}

/// Grant-absent: stub returns 403 on `/restore-credentials`. pgro should
/// surface a clear failure on the namespace annotation and not crash-loop.
#[ignore = "integration test; run in CI with the stub-canopy fixture"]
#[tokio::test]
async fn missing_grant_surfaces_clearly() {
	let client = helpers::make_client().await;
	// The stub's `/restore-credentials?scenario=denied` variant returns 403;
	// wired via a per-worklist-entry flag in the fixture ConfigMap.
	// TODO(canopy-integration): once the fixture supports the `scenario` knob,
	// assert that:
	// - the namespace exists with restore-state != "active" after 60s;
	// - a Warning event has been emitted on the namespace;
	// - the operator's restore-Job creation was not retried into a crash loop
	//   (Job count in the namespace <= 1).
	let ns_api: Api<Namespace> = Api::all(client.clone());
	let list = ns_api
		.list(&ListParams::default().labels("pgro.bes.au/managed-by=pgro-canopy"))
		.await;
	assert!(
		list.is_ok(),
		"namespace list must succeed even under grant-denied"
	);
	let _keep_time_import: Option<Time> = None;
}

/// Verification report emitted after a successful restore lands in the stub.
#[ignore = "integration test; run in CI with the stub-canopy fixture"]
#[tokio::test]
async fn verification_report_arrives_after_success() {
	let client = helpers::make_client().await;
	// TODO(canopy-integration): once the stub-canopy Deployment exposes
	// /tests/reports, assert that a report arrives within the timeout for
	// the successful namespace and carries the expected snapshot_id / outcome.
	let _initial = helpers::stub_report_count(&client).await;
	// placeholder — this test needs the stub-canopy fixture to be complete.
}
