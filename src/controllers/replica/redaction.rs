//! Replica redaction: fetch a Tamanu/dbt masking manifest and apply it to
//! a freshly-restored Postgres database using the `postgresql_anonymizer`
//! extension.
//!
//! See `docs/plans/replica-redaction.md` for the full design.

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, ResourceExt as _};
use tracing::{debug, info, warn};

use crate::context::Context;
use crate::controllers::postgres::{
	self, PgConnection, discover_restore_database, read_secret_field,
};
use crate::error::{Error, Result};
use crate::types::{PostgresPhysicalReplica, RedactionSpec};

use self::manifest::{Manifest, base_version, parse_manifest};

pub use self::apply::Outcome;

mod apply;
pub mod manifest;
pub mod mask;

const VERSION_PLACEHOLDER: &str = "{version}";

/// Run the full redaction step against the given restore.
///
/// Returns the resolved manifest version (if any) and the apply
/// [`Outcome`]. Errors abort the step and let the reconciler retry on
/// the next pass; per-statement issues during apply are tolerated and
/// surface as `outcome.is_partial()`.
pub async fn reconcile_redaction(
	ctx: &Context,
	replica: &PostgresPhysicalReplica,
	restore_name: &str,
) -> Result<(Option<String>, Outcome)> {
	let spec = replica
		.spec
		.redaction
		.as_ref()
		.expect("reconcile_redaction called with no spec.redaction");

	validate_spec(spec)?;

	let namespace = replica.namespace().expect("replica is namespaced");

	let creds_name = replica.creds_secret_name();
	let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &namespace);
	let creds = secrets.get(&creds_name).await?;
	let user = read_secret_field(&creds, "username")?;
	let password = read_secret_field(&creds, "password")?;

	let dbname = discover_restore_database(
		&ctx.client,
		&namespace,
		restore_name,
		&user,
		&password,
		ctx.use_port_forward(),
	)
	.await?;

	let conn = postgres::connect_to_restore(
		&ctx.client,
		&namespace,
		restore_name,
		&dbname,
		&user,
		&password,
		ctx.use_port_forward(),
	)
	.await?;

	let version = resolve_version(spec, &conn).await?;
	let resolved_url = resolve_url(spec, version.as_deref())?;

	info!(
		replica = %replica.name_any(),
		restore = %restore_name,
		url = %resolved_url,
		"fetching redaction manifest"
	);

	let manifest = fetch_manifest(ctx, spec, version.as_deref(), &resolved_url).await?;

	info!(
		columns = manifest.columns.len(),
		tables = manifest.tables.len(),
		"manifest parsed"
	);

	let outcome = apply::apply(&conn, &manifest, &dbname).await?;

	Ok((version, outcome))
}

fn validate_spec(spec: &RedactionSpec) -> Result<()> {
	let templated = spec.manifest_url.contains(VERSION_PLACEHOLDER);
	let has_literal = spec.version.is_some();
	let has_query = spec.version_query.is_some();

	if has_literal && has_query {
		return Err(Error::Redaction(
			"redaction spec: `version` and `versionQuery` are mutually exclusive".into(),
		));
	}
	if templated && !(has_literal || has_query) {
		return Err(Error::Redaction(
			"redaction spec: `manifestUrl` contains `{version}` but no `version` or `versionQuery` provided".into(),
		));
	}
	if !templated && (has_literal || has_query) {
		return Err(Error::Redaction(
			"redaction spec: `version`/`versionQuery` set but `manifestUrl` has no `{version}` placeholder".into(),
		));
	}
	Ok(())
}

async fn resolve_version(spec: &RedactionSpec, conn: &PgConnection) -> Result<Option<String>> {
	if let Some(v) = spec.version.clone() {
		return Ok(Some(v));
	}
	let Some(query) = spec.version_query.as_deref() else {
		return Ok(None);
	};

	let rows = conn
		.client
		.simple_query(query)
		.await
		.map_err(|e| Error::Redaction(format!("versionQuery failed: {e}")))?;

	for msg in rows {
		if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
			let value = row
				.get(0)
				.ok_or_else(|| Error::Redaction("versionQuery returned no columns".into()))?;
			return Ok(Some(value.to_string()));
		}
	}
	Err(Error::Redaction("versionQuery returned no rows".into()))
}

fn resolve_url(spec: &RedactionSpec, version: Option<&str>) -> Result<String> {
	match version {
		Some(v) => Ok(spec.manifest_url.replace(VERSION_PLACEHOLDER, v)),
		None => Ok(spec.manifest_url.clone()),
	}
}

async fn fetch_manifest(
	ctx: &Context,
	spec: &RedactionSpec,
	version: Option<&str>,
	url: &str,
) -> Result<Manifest> {
	let resp = ctx.http_client.get(url).send().await?;
	let status = resp.status();

	if status == reqwest::StatusCode::NOT_FOUND
		&& spec.version_fallback_to_base
		&& let Some(v) = version
		&& let Some(base) = base_version(v)
	{
		let base_url = spec.manifest_url.replace(VERSION_PLACEHOLDER, &base);
		warn!(
			version = %v,
			base = %base,
			"manifest 404, retrying with base version"
		);
		debug!(url = %base_url, "fetching redaction manifest (base)");
		let base_resp = ctx.http_client.get(&base_url).send().await?;
		let base_resp = base_resp.error_for_status()?;
		let body = base_resp.text().await?;
		return parse_manifest(&body).map_err(Into::into);
	}

	let resp = resp.error_for_status()?;
	let body = resp.text().await?;
	parse_manifest(&body).map_err(Into::into)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn spec(url: &str, ver: Option<&str>, vq: Option<&str>) -> RedactionSpec {
		RedactionSpec {
			manifest_url: url.into(),
			version: ver.map(str::to_string),
			version_query: vq.map(str::to_string),
			version_fallback_to_base: false,
			extension_image: None,
		}
	}

	#[test]
	fn validate_accepts_static_url() {
		assert!(validate_spec(&spec("https://x/m.json", None, None)).is_ok());
	}

	#[test]
	fn validate_accepts_templated_with_literal_version() {
		assert!(validate_spec(&spec("https://x/v{version}.json", Some("1.0.0"), None)).is_ok());
	}

	#[test]
	fn validate_accepts_templated_with_query() {
		assert!(validate_spec(&spec("https://x/v{version}.json", None, Some("SELECT 1"))).is_ok());
	}

	#[test]
	fn validate_rejects_templated_without_version() {
		assert!(validate_spec(&spec("https://x/v{version}.json", None, None)).is_err());
	}

	#[test]
	fn validate_rejects_static_with_version() {
		assert!(validate_spec(&spec("https://x/m.json", Some("1.0.0"), None)).is_err());
	}

	#[test]
	fn validate_rejects_both_version_and_query() {
		assert!(
			validate_spec(&spec(
				"https://x/v{version}.json",
				Some("1.0.0"),
				Some("SELECT 1")
			))
			.is_err()
		);
	}

	#[test]
	fn resolve_url_substitutes_version() {
		let s = spec("https://x/v{version}/m.json", Some("2.41.0"), None);
		assert_eq!(
			resolve_url(&s, Some("2.41.0")).unwrap(),
			"https://x/v2.41.0/m.json"
		);
	}

	#[test]
	fn resolve_url_passes_through_when_no_version() {
		let s = spec("https://x/m.json", None, None);
		assert_eq!(resolve_url(&s, None).unwrap(), "https://x/m.json");
	}
}
