//! Thin wrapper around `bestool_canopy::CanopyClient`.
//!
//! Owns construction (the SOCKS5 proxy wiring to the Tailscale sidecar, plus
//! optional mTLS fallback) and exposes the four restore-* endpoints pgro
//! consumes: `restore_capabilities`, `restore_worklist`, `restore_credentials`,
//! `restore_verification`. Each is a one-line forward; the wrapper exists as
//! the integration seam tests inject a stub at, and as the place to hang
//! pgro-specific logging / retry / cache concerns later.

use bestool_canopy::{
	CanopyClient, TAILSCALE_URL,
	schema::{
		IntentDescriptor, RestoreCapabilitiesArgs, RestoreCredentials, RestoreCredentialsArgs,
		VerificationArgs, WorklistEntry,
	},
};
use reqwest::Url;
use uuid::Uuid;

use crate::error::{Error, Result};

/// Default SOCKS5 proxy the operator's Tailscale sidecar listens on (IPv6
/// loopback). Override via `CANOPY_SOCKS5_PROXY` for tests / non-sidecar
/// dev setups; set to empty to disable proxy entirely.
pub const DEFAULT_SOCKS5_PROXY: &str = "socks5://[::1]:1055";

/// Configuration for the canopy client at startup.
#[derive(Debug, Clone)]
pub struct CanopyConfig {
	/// The public-mTLS base URL. Used by `bestool_canopy::CanopyClient` as
	/// the fallback endpoint when the tailnet probe fails. The tailnet URL
	/// itself is hardcoded in `bestool-canopy`.
	pub base_url: Url,
	/// SOCKS5 URL of the Tailscale sidecar's userspace proxy. Empty string
	/// means no proxy.
	pub socks5_proxy: String,
	/// Optional device cert + key (PEM, concatenated). Used only if the
	/// tailnet probe fails.
	pub device_key_pem: Option<String>,
}

/// Build the inner `bestool_canopy::CanopyClient`. The SOCKS5 proxy URL is
/// captured into the builder factory and applied **only for tailnet-hosted
/// URLs** (`*.ts.net`) — the mTLS fallback endpoint must be reachable
/// directly even when the Tailscale sidecar is down, which is the whole
/// point of it being a fallback. Without the per-URL predicate, a broken
/// SOCKS proxy takes down both paths at once.
async fn build_inner(cfg: &CanopyConfig) -> Result<CanopyClient> {
	let socks5 = cfg.socks5_proxy.clone();
	let make_builder = move || {
		let mut b = reqwest::Client::builder();
		if !socks5.is_empty() {
			let socks5 = socks5.clone();
			let proxy = reqwest::Proxy::custom(move |url| {
				if url.host_str().is_some_and(|h| h.ends_with(".ts.net")) {
					Some(socks5.clone())
				} else {
					None
				}
			});
			b = b.proxy(proxy);
		}
		b
	};

	let tailscale_url: Url = TAILSCALE_URL
		.parse()
		.expect("bestool-canopy TAILSCALE_URL is a valid URL");
	let inner = CanopyClient::with_urls(
		cfg.base_url.clone(),
		tailscale_url,
		cfg.device_key_pem.as_deref(),
		make_builder,
	)
	.await
	.map_err(|err| Error::Canopy(format!("constructing canopy client: {err}")))?
	.ok_or_else(|| {
		Error::Canopy(
			"canopy client unconfigured: tailnet unreachable and no device cert provided".into(),
		)
	})?;
	Ok(inner)
}

/// pgro's canopy client wrapper around the live `bestool_canopy::CanopyClient`
/// (which bakes in the mTLS base URL and the tailnet endpoint at construction).
pub struct Client {
	inner: CanopyClient,
}

impl std::fmt::Debug for Client {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("canopy::Client").finish_non_exhaustive()
	}
}

impl Client {
	/// Build a client from operator-level config. Returns `Ok(None)` if no
	/// canopy integration is configured (no config provided) — the operator
	/// then runs in legacy-only mode.
	pub async fn from_config(cfg: Option<CanopyConfig>) -> Result<Option<Self>> {
		let Some(cfg) = cfg else { return Ok(None) };
		let inner = build_inner(&cfg).await?;
		Ok(Some(Self { inner }))
	}

	/// Register the intent descriptors this consumer supports. Replaces the
	/// registered set wholesale (per canopy's semantics). Each descriptor
	/// carries the intent name, the canopy semantics it opts into, and its
	/// typed parameter schema.
	pub async fn restore_capabilities(&self, intents: &[IntentDescriptor]) -> Result<()> {
		let body = RestoreCapabilitiesArgs::builder()
			.intents(intents.to_vec())
			.build();
		self.inner
			.restore_capabilities(&body)
			.await
			.map_err(|err| Error::Canopy(format!("restore_capabilities: {err}")))
	}

	/// Fetch the consumer's desired-state worklist. Each entry is one
	/// concrete replica to maintain.
	pub async fn worklist(&self) -> Result<Vec<WorklistEntry>> {
		self.inner
			.restore_worklist()
			.await
			.map_err(|err| Error::Canopy(format!("restore_worklist: {err}")))
	}

	/// Fetch short-lived read-only STS creds plus the repo password for a
	/// `(group, type)`. Authorized iff a declaration covers it. `run_id` is the
	/// canopy run-uuid when the fetch belongs to a restore run (the restore
	/// job's sidecar); `None` for non-run fetches such as the reconcile-time
	/// repo-password poll.
	pub async fn restore_credentials(
		&self,
		backup_type: &str,
		group: Uuid,
		run_id: Option<Uuid>,
	) -> Result<RestoreCredentials> {
		let body = RestoreCredentialsArgs::builder()
			.group(group)
			.type_(backup_type.to_string())
			.maybe_run_id(run_id)
			.build();
		self.inner.restore_credentials(&body).await.map_err(|err| {
			Error::Canopy(format!(
				"restore_credentials({backup_type}, {group}): {err}"
			))
		})
	}

	/// Report a restore outcome (signal 3, restore-verification). `args` is
	/// the typed [`bestool_canopy::schema::VerificationArgs`], including the
	/// free-form `health_details`.
	pub async fn restore_verification_typed(&self, args: &VerificationArgs) -> Result<()> {
		self.inner
			.restore_verification(args)
			.await
			.map_err(|err| Error::Canopy(format!("restore_verification: {err}")))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default_socks5_proxy_is_v6_loopback() {
		assert_eq!(DEFAULT_SOCKS5_PROXY, "socks5://[::1]:1055");
	}

	#[test]
	fn default_socks5_proxy_parses_as_reqwest_proxy() {
		// Catches a typo in the constant — reqwest insists on a real URL.
		reqwest::Proxy::all(DEFAULT_SOCKS5_PROXY).expect("DEFAULT_SOCKS5_PROXY must parse");
	}
}
