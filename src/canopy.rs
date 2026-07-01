//! Thin wrapper around `bestool_canopy::CanopyClient`.
//!
//! Owns construction (the SOCKS5 proxy wiring to the Tailscale sidecar, plus
//! optional mTLS fallback) and exposes the four restore-* endpoints pgro
//! consumes: `restore_capabilities`, `restore_worklist`, `restore_credentials`,
//! `restore_verification`. Each is a one-line forward; the wrapper exists as
//! the integration seam tests inject a stub at, and as the place to hang
//! pgro-specific logging / retry / cache concerns later.

use bestool_canopy::{
	CanopyClient, RestoreCredentials, RestoreVerification, WorklistEntry, client_builder,
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
/// captured into the builder factory so every probe + reconnect uses it.
async fn build_inner(cfg: &CanopyConfig) -> Result<CanopyClient> {
	let socks5 = cfg.socks5_proxy.clone();
	let version = env!("CARGO_PKG_VERSION").to_string();
	let make_builder = move || {
		let mut b = client_builder(&version);
		if !socks5.is_empty() {
			match reqwest::Proxy::all(&socks5) {
				Ok(proxy) => b = b.proxy(proxy),
				Err(err) => {
					tracing::error!(socks5_proxy = %socks5, error = %err, "CanopyConfig: invalid SOCKS5 proxy URL");
				}
			}
		}
		b
	};

	let inner = CanopyClient::new(
		env!("CARGO_PKG_VERSION"),
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

/// pgro's canopy client wrapper. Holds the live `bestool_canopy::CanopyClient`
/// plus the public-mTLS base URL — the bestool client uses its own hardcoded
/// tailnet URL on the tailnet path; the base URL is the mTLS-leg fallback.
pub struct Client {
	inner: CanopyClient,
	base_url: Url,
}

impl std::fmt::Debug for Client {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("canopy::Client")
			.field("base_url", &self.base_url.as_str())
			.finish_non_exhaustive()
	}
}

impl Client {
	/// Build a client from operator-level config. Returns `Ok(None)` if no
	/// canopy integration is configured (no base URL set) — the operator
	/// then runs in legacy-only mode.
	pub async fn from_config(cfg: Option<CanopyConfig>) -> Result<Option<Self>> {
		let Some(cfg) = cfg else { return Ok(None) };
		let inner = build_inner(&cfg).await?;
		Ok(Some(Self {
			inner,
			base_url: cfg.base_url,
		}))
	}

	/// Register the intents this consumer supports. Replaces the registered
	/// set wholesale (per canopy's semantics).
	pub async fn restore_capabilities(&self, intents: &[&str]) -> Result<()> {
		self.inner
			.restore_capabilities(&self.base_url, intents)
			.await
			.map_err(|err| Error::Canopy(format!("restore_capabilities: {}", chain(&err))))
	}

	/// Fetch the consumer's desired-state worklist. Each entry is one
	/// concrete replica to maintain.
	pub async fn worklist(&self) -> Result<Vec<WorklistEntry>> {
		self.inner
			.restore_worklist(&self.base_url)
			.await
			.map_err(|err| Error::Canopy(format!("restore_worklist: {}", chain(&err))))
	}

	/// Fetch short-lived read-only STS creds plus the repo password for a
	/// `(group, type)`. Authorized iff a declaration covers it.
	pub async fn restore_credentials(
		&self,
		backup_type: &str,
		group: Uuid,
	) -> Result<RestoreCredentials> {
		self.inner
			.restore_credentials(&self.base_url, backup_type, group)
			.await
			.map_err(|err| {
				Error::Canopy(format!(
					"restore_credentials({backup_type}, {group}): {}",
					chain(&err)
				))
			})
	}

	/// Report a restore outcome (signal 3, restore-verification).
	pub async fn restore_verification(&self, report: &RestoreVerification<'_>) -> Result<()> {
		self.inner
			.restore_verification(&self.base_url, report)
			.await
			.map_err(|err| Error::Canopy(format!("restore_verification: {}", chain(&err))))
	}

	/// Direct access to the public-mTLS base URL the client is configured
	/// against. The tailnet path uses its own hardcoded URL inside
	/// `bestool-canopy`.
	pub fn base_url(&self) -> &Url {
		&self.base_url
	}
}

/// Flatten a `miette::Report` (bestool-canopy's error type) to a single
/// `outer: inner: root` line by walking its source chain. bestool wraps
/// its errors with `.wrap_err(...)` which layers context around the
/// underlying reqwest / io error, and plain `Display` on the report
/// only shows the outermost message — losing the actually diagnostic
/// bit.
fn chain(err: &miette::Report) -> String {
	err.chain()
		.map(|e| e.to_string())
		.collect::<Vec<_>>()
		.join(": ")
}

/// Re-export the wire types pgro consumes verbatim from `bestool-canopy`.
pub use bestool_canopy::{
	BackupCredentials as Credentials, Outcome, RestoreCredentials as CredsResponse,
	RestoreVerification as Verification, WorklistEntry as Entry,
};

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
