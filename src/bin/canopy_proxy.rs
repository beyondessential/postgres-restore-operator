//! `canopy-proxy` — sidecar binary that runs the bestool S3P loopback
//! re-signing proxy for one kopia run.
//!
//! Lifecycle:
//!
//! 1. Read configuration from env (broker URL, group, type, region, file paths).
//! 2. Spawn `bestool_kopia::proxy::spawn` with a [`BrokerCredentialProvider`]
//!    that fetches creds from the operator's in-cluster broker.
//! 3. Write the ephemeral port to `PGRO_PROXY_PORT_FILE` (atomic rename) so
//!    the sibling kopia container can discover it.
//! 4. Wait for SIGTERM (kopia container completing → pod termination).
//! 5. On shutdown, write `TrafficStats` to `PGRO_PROXY_STATS_FILE` for the
//!    operator's restore-verification reporter to pick up.
//!
//! The kopia container is responsible for waiting on the port file before
//! invoking kopia (job builder injects a shell wrapper, see Step 7 of the
//! integration spec).

use std::{path::PathBuf, pin::Pin, process::ExitCode, sync::Arc, time::Duration};

use bestool_canopy::schema::CredentialProcessOutput as BackupCredentials;
use bestool_kopia::proxy::{self, BoxError, CredentialProvider, Credentials, S3ProxyConfig};
use jiff::{Timestamp, ToSpan};
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{error, info};

/// Refresh creds this far ahead of their `Expiration` (mirrors bestool's
/// 2-minute margin).
const REFRESH_MARGIN_MINUTES: i64 = 2;

#[derive(Debug)]
struct Config {
	broker_url: String,
	group: String,
	backup_type: String,
	region: String,
	port_file: PathBuf,
	/// URL the operator serves the stats-callback on; the sidecar POSTs
	/// its final TrafficStats there on shutdown so the reporter can
	/// include them in the RestoreVerification. Constructed by the Job
	/// builder from `Context::canopy_stats_callback_url`.
	stats_callback_url: String,
}

impl Config {
	fn from_env() -> Result<Self, String> {
		Ok(Self {
			broker_url: env_required("PGRO_BROKER_URL")?,
			group: env_required("PGRO_GROUP")?,
			backup_type: env_required("PGRO_TYPE")?,
			region: env_required("PGRO_REGION")?,
			port_file: env_or("PGRO_PROXY_PORT_FILE", "/var/run/pgro/proxy-port").into(),
			stats_callback_url: env_required("PGRO_STATS_CALLBACK_URL")?,
		})
	}
}

fn env_required(name: &str) -> Result<String, String> {
	std::env::var(name).map_err(|_| format!("{name} not set"))
}

fn env_or(name: &str, default: &str) -> String {
	std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Pulls creds from the operator's broker. Caches the response in-process and
/// refreshes within the margin of expiry.
struct BrokerCredentialProvider {
	http: reqwest::Client,
	url: String,
	group: String,
	backup_type: String,
	cache: Mutex<Option<BackupCredentials>>,
}

impl BrokerCredentialProvider {
	fn new(broker_url: &str, group: &str, backup_type: &str) -> Self {
		Self {
			http: reqwest::Client::builder()
				.timeout(Duration::from_secs(10))
				.build()
				.expect("static reqwest client builder"),
			url: format!(
				"{}/internal/restore-creds",
				broker_url.trim_end_matches('/')
			),
			group: group.to_string(),
			backup_type: backup_type.to_string(),
			cache: Mutex::new(None),
		}
	}

	async fn fetch(&self) -> Result<BackupCredentials, BoxError> {
		#[derive(Serialize)]
		struct Req<'a> {
			group: &'a str,
			r#type: &'a str,
		}
		let resp = self
			.http
			.post(&self.url)
			.json(&Req {
				group: &self.group,
				r#type: &self.backup_type,
			})
			.send()
			.await?
			.error_for_status()?;
		// Broker returns `RestoreCredentials { credentials, repo_password }`;
		// the sidecar only needs the credentials field (the repo password is
		// already in kopia's argv via the Job spec).
		#[derive(serde::Deserialize)]
		struct Resp {
			credentials: BackupCredentials,
		}
		let body: Resp = resp.json().await?;
		Ok(body.credentials)
	}
}

fn needs_refresh(cached: &Option<BackupCredentials>, now: Timestamp) -> bool {
	match cached {
		None => true,
		Some(creds) => creds.expiration <= now + REFRESH_MARGIN_MINUTES.minutes(),
	}
}

impl CredentialProvider for BrokerCredentialProvider {
	fn credentials(
		&self,
	) -> Pin<Box<dyn Future<Output = Result<Credentials, BoxError>> + Send + '_>> {
		Box::pin(async move {
			let mut cached = self.cache.lock().await;
			if needs_refresh(&cached, Timestamp::now()) {
				let fresh = self.fetch().await?;
				*cached = Some(fresh);
			}
			let creds = cached.as_ref().expect("cache populated above");
			Ok(Credentials {
				access_key: creds.access_key_id.clone(),
				secret_key: creds.secret_access_key.0.clone(),
				session_token: Some(creds.session_token.0.clone()),
			})
		})
	}
}

#[derive(Serialize)]
struct StatsFile {
	sent_raw_bytes: u64,
	sent_payload_bytes: u64,
	received_raw_bytes: u64,
	received_payload_bytes: u64,
}

/// Block until SIGTERM (kubelet, native-sidecar termination) or SIGINT
/// (interactive ctrl-c) arrives, whichever comes first.
async fn wait_for_shutdown() -> Result<(), String> {
	use tokio::signal::unix::{SignalKind, signal};
	let mut sigterm = signal(SignalKind::terminate())
		.map_err(|err| format!("installing SIGTERM handler: {err}"))?;
	let mut sigint = signal(SignalKind::interrupt())
		.map_err(|err| format!("installing SIGINT handler: {err}"))?;
	tokio::select! {
		_ = sigterm.recv() => {}
		_ = sigint.recv() => {}
	}
	Ok(())
}

/// Write `port` to `port_file` atomically (write `.tmp` then rename) so a
/// partial read by the kopia container's wait-loop can't observe a torn value.
fn write_port_atomic(port_file: &std::path::Path, port: u16) -> std::io::Result<()> {
	if let Some(parent) = port_file.parent() {
		std::fs::create_dir_all(parent)?;
	}
	let tmp = port_file.with_extension("tmp");
	std::fs::write(&tmp, port.to_string())?;
	std::fs::rename(&tmp, port_file)
}

/// POST the sidecar's final stats to the operator's callback endpoint.
/// Best-effort with a short timeout; failures are logged, not fatal.
async fn post_stats(url: &str, stats: &StatsFile) -> Result<(), String> {
	let client = reqwest::Client::builder()
		.timeout(Duration::from_secs(5))
		.build()
		.map_err(|e| format!("building stats client: {e}"))?;
	let resp = client
		.post(url)
		.json(stats)
		.send()
		.await
		.map_err(|e| format!("posting stats: {e}"))?;
	let status = resp.status();
	if !status.is_success() {
		return Err(format!("stats callback returned {status}"));
	}
	Ok(())
}

async fn run(cfg: Config) -> Result<(), String> {
	let group = cfg.group.clone();
	let backup_type = cfg.backup_type.clone();
	let provider = Arc::new(BrokerCredentialProvider::new(
		&cfg.broker_url,
		&group,
		&backup_type,
	));
	let s3_cfg = S3ProxyConfig {
		upstream: format!("https://s3.{}.amazonaws.com", cfg.region),
		upstream_host: format!("s3.{}.amazonaws.com", cfg.region),
		region: cfg.region.clone(),
	};
	let proxy = proxy::spawn(s3_cfg, provider)
		.await
		.map_err(|err| format!("spawning S3P proxy: {err}"))?;
	let addr = proxy.addr();
	let port = addr.port();
	info!(%addr, port, "S3P proxy bound");

	write_port_atomic(&cfg.port_file, port)
		.map_err(|err| format!("writing port file {}: {err}", cfg.port_file.display()))?;

	// Wait for shutdown. As a native sidecar the kubelet sends SIGTERM once
	// the main container exits; interactive runs get SIGINT. Handle both —
	// ctrl_c() alone only catches SIGINT, so under k8s the proxy would hang
	// until SIGKILL and lose its stats.
	wait_for_shutdown().await?;
	info!("shutdown signal received");

	let traffic = proxy.traffic();
	let stats = StatsFile {
		sent_raw_bytes: traffic.sent_raw,
		sent_payload_bytes: traffic.sent_payload,
		received_raw_bytes: traffic.received_raw,
		received_payload_bytes: traffic.received_payload,
	};
	if let Err(err) = post_stats(&cfg.stats_callback_url, &stats).await {
		error!(
			error = %err,
			stats_callback_url = %cfg.stats_callback_url,
			"posting stats callback failed"
		);
	} else {
		info!(
			sent_raw_bytes = stats.sent_raw_bytes,
			received_raw_bytes = stats.received_raw_bytes,
			"posted stats callback"
		);
	}
	Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
		)
		.with_target(false)
		.init();

	let cfg = match Config::from_env() {
		Ok(c) => c,
		Err(err) => {
			eprintln!("canopy-proxy: {err}");
			return ExitCode::from(2);
		}
	};
	if let Err(err) = run(cfg).await {
		error!(error = %err, "canopy-proxy exiting with error");
		return ExitCode::FAILURE;
	}
	ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
	use super::*;

	fn creds_at(expiration: &str) -> BackupCredentials {
		// Build via JSON: BackupCredentials' fields use wrapper types
		// (Redacted, etc.) that aren't all re-exported at the crate root.
		serde_json::from_value(serde_json::json!({
			"Version": 1,
			"AccessKeyId": "AKIA",
			"SecretAccessKey": "secret",
			"SessionToken": "session",
			"Expiration": expiration,
		}))
		.unwrap()
	}

	#[test]
	fn needs_refresh_when_absent() {
		let now: Timestamp = "2026-07-01T00:00:00Z".parse().unwrap();
		assert!(needs_refresh(&None, now));
	}

	#[test]
	fn needs_refresh_within_margin() {
		let now: Timestamp = "2026-07-01T00:00:00Z".parse().unwrap();
		assert!(needs_refresh(&Some(creds_at("2026-07-01T00:01:00Z")), now));
	}

	#[test]
	fn does_not_need_refresh_beyond_margin() {
		let now: Timestamp = "2026-07-01T00:00:00Z".parse().unwrap();
		assert!(!needs_refresh(&Some(creds_at("2026-07-01T01:00:00Z")), now));
	}

	#[test]
	fn write_port_atomic_roundtrip() {
		let tmp = tempfile::tempdir().unwrap();
		let path = tmp.path().join("subdir").join("port");
		write_port_atomic(&path, 12345).unwrap();
		let content = std::fs::read_to_string(&path).unwrap();
		assert_eq!(content, "12345");
	}
}
