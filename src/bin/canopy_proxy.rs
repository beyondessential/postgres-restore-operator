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
//! 4. While the run is in flight, POST a cumulative traffic sample to the
//!    operator every [`PROGRESS_INTERVAL`], which relays it to canopy so a long
//!    restore shows as moving. Best-effort, and skipped when the operator
//!    supplied no progress callback URL or no `run_id`.
//! 5. Wait for SIGTERM (kopia container completing → pod termination).
//! 6. On shutdown, POST the final `TrafficStats` to the operator's stats
//!    callback for its restore-verification reporter to pick up.
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
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use postgres_restore_operator::canopy::ProgressSample;

/// Refresh creds this far ahead of their `Expiration` (mirrors bestool's
/// 2-minute margin).
const REFRESH_MARGIN_MINUTES: i64 = 2;

#[derive(Debug)]
struct Config {
	broker_url: String,
	group: String,
	backup_type: String,
	/// Canopy run-uuid for this restore run (`PGRO_RUN_ID`), forwarded with
	/// every creds request so canopy attributes the grant to the run. Optional
	/// so a sidecar scheduled by an older operator (no env set) still works.
	run_id: Option<String>,
	region: String,
	port_file: PathBuf,
	/// URL the operator serves the stats-callback on; the sidecar POSTs
	/// its final TrafficStats there on shutdown so the reporter can
	/// include them in the RestoreVerification. Constructed by the Job
	/// builder from `Context::canopy_stats_callback_url`.
	stats_callback_url: String,
	/// URL the operator serves the progress-callback on. The sidecar POSTs a
	/// cumulative sample here every [`PROGRESS_INTERVAL`] so canopy can show a
	/// long restore advancing. Optional: absent (or an absent `run_id`)
	/// disables sampling, which is also what a sidecar scheduled by an older
	/// operator sees.
	progress_callback_url: Option<String>,
}

impl Config {
	fn from_env() -> Result<Self, String> {
		Ok(Self {
			broker_url: env_required("PGRO_BROKER_URL")?,
			group: env_required("PGRO_GROUP")?,
			backup_type: env_required("PGRO_TYPE")?,
			run_id: std::env::var("PGRO_RUN_ID").ok().filter(|s| !s.is_empty()),
			region: env_required("PGRO_REGION")?,
			port_file: env_or("PGRO_PROXY_PORT_FILE", "/var/run/pgro/proxy-port").into(),
			stats_callback_url: env_required("PGRO_STATS_CALLBACK_URL")?,
			progress_callback_url: std::env::var("PGRO_PROGRESS_CALLBACK_URL")
				.ok()
				.filter(|s| !s.is_empty()),
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
	run_id: Option<String>,
	cache: Mutex<Option<BackupCredentials>>,
}

impl BrokerCredentialProvider {
	fn new(broker_url: &str, group: &str, backup_type: &str, run_id: Option<&str>) -> Self {
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
			run_id: run_id.map(str::to_string),
			cache: Mutex::new(None),
		}
	}

	async fn fetch(&self) -> Result<BackupCredentials, BoxError> {
		#[derive(Serialize)]
		struct Req<'a> {
			group: &'a str,
			r#type: &'a str,
			#[serde(skip_serializing_if = "Option::is_none")]
			run_id: Option<&'a str>,
		}
		let resp = self
			.http
			.post(&self.url)
			.json(&Req {
				group: &self.group,
				r#type: &self.backup_type,
				run_id: self.run_id.as_deref(),
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

/// How often the sidecar posts a cumulative progress sample while the restore
/// is in flight. Matches bestool's cadence, and fixed rather than configurable
/// so a misconfiguration can't post faster.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);

/// Where to post progress for this run, or `None` when it can't report: no
/// callback URL (older operator), no `run_id` (canopy requires one, and its
/// absence marks a non-canopy run), or a `run_id` that isn't a uuid.
fn progress_target(cfg: &Config) -> Option<(String, Uuid)> {
	let url = cfg.progress_callback_url.clone()?;
	let Some(run_id) = cfg.run_id.as_deref() else {
		debug!("no run_id; not sampling progress");
		return None;
	};
	match run_id.parse() {
		Ok(run_id) => Some((url, run_id)),
		Err(error) => {
			warn!(%run_id, %error, "run_id is not a uuid; not sampling progress");
			None
		}
	}
}

/// Start posting cumulative progress samples, or return `None` when
/// [`progress_target`] says this run can't report.
fn spawn_progress_sampler(
	cfg: &Config,
	proxy: &Arc<proxy::RunningProxy>,
) -> Option<JoinHandle<()>> {
	let (url, run_id) = progress_target(cfg)?;
	let backup_type = cfg.backup_type.clone();
	let proxy = Arc::clone(proxy);
	Some(tokio::spawn(async move {
		loop {
			tokio::time::sleep(PROGRESS_INTERVAL).await;
			let t = proxy.traffic();
			let sample = ProgressSample {
				run_id,
				type_: backup_type.clone(),
				sent_raw_bytes: t.sent_raw,
				sent_payload_bytes: t.sent_payload,
				received_raw_bytes: t.received_raw,
				received_payload_bytes: t.received_payload,
			};
			if let Err(err) = post_progress(&url, &sample).await {
				debug!(error = %err, "posting progress sample failed (ignored)");
			}
		}
	}))
}

/// POST one cumulative progress sample to the operator, which relays it to
/// canopy. Best-effort with a short timeout.
async fn post_progress(url: &str, sample: &ProgressSample) -> Result<(), String> {
	let client = reqwest::Client::builder()
		.timeout(Duration::from_secs(5))
		.build()
		.map_err(|e| format!("building progress client: {e}"))?;
	let resp = client
		.post(url)
		.json(sample)
		.send()
		.await
		.map_err(|e| format!("posting progress: {e}"))?;
	let status = resp.status();
	if !status.is_success() {
		return Err(format!("progress callback returned {status}"));
	}
	Ok(())
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
		cfg.run_id.as_deref(),
	));
	let s3_cfg = S3ProxyConfig {
		upstream: format!("https://s3.{}.amazonaws.com", cfg.region),
		upstream_host: format!("s3.{}.amazonaws.com", cfg.region),
		region: cfg.region.clone(),
	};
	let proxy = Arc::new(
		proxy::spawn(s3_cfg, provider)
			.await
			.map_err(|err| format!("spawning S3P proxy: {err}"))?,
	);
	let addr = proxy.addr();
	let port = addr.port();
	info!(%addr, port, "S3P proxy bound");

	write_port_atomic(&cfg.port_file, port)
		.map_err(|err| format!("writing port file {}: {err}", cfg.port_file.display()))?;

	// Sample S3 traffic to canopy while the restore runs, so a long download
	// shows as moving rather than merely in progress. Detached deliberately:
	// nothing below awaits it, so a slow or hanging post can never delay the
	// run's own completion, and the task dies with the process.
	let progress = spawn_progress_sampler(&cfg, &proxy);

	// Wait for shutdown. As a native sidecar the kubelet sends SIGTERM once
	// the main container exits; interactive runs get SIGINT. Handle both —
	// ctrl_c() alone only catches SIGINT, so under k8s the proxy would hang
	// until SIGKILL and lose its stats.
	wait_for_shutdown().await?;
	info!("shutdown signal received");

	// Stop sampling before the final stats post. Aborted rather than drained:
	// a last sample is redundant once the run is over, and waiting on one
	// would let a slow post delay shutdown.
	if let Some(progress) = progress {
		progress.abort();
	}

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

	fn cfg(progress_callback_url: Option<&str>, run_id: Option<&str>) -> Config {
		Config {
			broker_url: "http://operator:9091".into(),
			group: "11111111-1111-1111-1111-111111111111".into(),
			backup_type: "tamanu-postgres".into(),
			run_id: run_id.map(str::to_string),
			region: "ap-southeast-2".into(),
			port_file: "/var/run/pgro/proxy-port".into(),
			stats_callback_url: "http://operator:8080/api/v1/canopy-stats/ns/job".into(),
			progress_callback_url: progress_callback_url.map(str::to_string),
		}
	}

	const URL: &str = "http://operator:8080/api/v1/canopy-progress/ns/job";
	const RUN: &str = "22222222-2222-2222-2222-222222222222";

	#[test]
	fn progress_target_resolves_when_url_and_run_id_are_set() {
		let (url, run_id) = progress_target(&cfg(Some(URL), Some(RUN))).expect("target");
		assert_eq!(url, URL);
		assert_eq!(run_id.to_string(), RUN);
	}

	/// `ProgressArgs` requires a run_id, and its absence is also what marks a
	/// run canopy isn't tracking — so there is nothing to report against.
	#[test]
	fn no_progress_target_without_a_run_id() {
		assert!(progress_target(&cfg(Some(URL), None)).is_none());
	}

	/// A sidecar scheduled by an older operator gets no URL, and must not
	/// start sampling into the void.
	#[test]
	fn no_progress_target_without_a_callback_url() {
		assert!(progress_target(&cfg(None, Some(RUN))).is_none());
	}

	#[test]
	fn no_progress_target_when_run_id_is_not_a_uuid() {
		assert!(progress_target(&cfg(Some(URL), Some("not-a-uuid"))).is_none());
	}

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
