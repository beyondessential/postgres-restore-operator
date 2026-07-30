//! Applying a target version's schema migrations to a restored replica.
//!
//! Canopy names the version; this runs it. A `Restoring` replica that comes up
//! healthy with `spec.migrateTo` set enters `Migrating` instead of `Ready`: a
//! Job runs the tamanu image at that version against the replica, then the
//! result is read back out of the `logs.migrations` audit table tamanu writes
//! itself, so nothing here parses logs.

use std::time::Duration;

use kube::runtime::controller::Action;

use crate::{context::Context, error::Result, types::PostgresPhysicalRestore};

pub async fn reconcile_migrating(
	_restore: &PostgresPhysicalRestore,
	_ctx: &Context,
	_name: &str,
	_namespace: &str,
) -> Result<Action> {
	Ok(Action::requeue(Duration::from_secs(15)))
}
