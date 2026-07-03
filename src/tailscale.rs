//! Minimal tailscale LocalAPI client.
//!
//! Used by the canopy verification reporter to build the URL of a replica
//! exposed on the tailnet (the `url` semantic): the tailnet's MagicDNS suffix
//! (e.g. `tailnet-abc.ts.net`) plus the Service's `tailscale.com/hostname`
//! give `https://<hostname>.<suffix>`. The suffix is constant per tailnet, so
//! callers cache it after the first successful fetch.
//!
//! tailscaled's LocalAPI is served on a unix socket, not TCP: the userspace
//! sidecar only exposes SOCKS5 (and optional metrics/health) over TCP. So we
//! reach it over the socket the sidecar shares via an `emptyDir`, which
//! containerboot always symlinks to `/var/run/tailscale/tailscaled.sock`.
//! `GET /localapi/v0/status` is a read endpoint, and reads over the unix
//! socket are permitted regardless of peer uid (tailscale
//! `ipnserver`: `IsUnixSock()` ⇒ read always allowed), so the operator
//! container needs no special identity — only access to the socket file.
//! LocalAPI expects the `Host: local-tailscaled.sock` header, which the
//! request URL's host supplies.

use serde::Deserialize;

/// Host LocalAPI expects (the connection is over the socket; the host is only
/// the `Host` header and is never resolved).
const LOCALAPI_HOST: &str = "local-tailscaled.sock";

/// Subset of the tailscale LocalAPI `/localapi/v0/status` response we read.
#[derive(Debug, Deserialize)]
struct Status {
	#[serde(rename = "CurrentTailnet")]
	current_tailnet: Option<CurrentTailnet>,
}

#[derive(Debug, Deserialize)]
struct CurrentTailnet {
	#[serde(rename = "MagicDNSSuffix")]
	magic_dns_suffix: String,
}

/// Fetch the tailnet's MagicDNS suffix from the tailscale sidecar's LocalAPI
/// over the given unix socket path.
///
/// Best-effort: an unbuildable client, a network / decode error, a non-2xx
/// response, or an empty suffix all yield `None`, so a missing socket just
/// omits the URL from the health report rather than failing the report.
pub async fn magic_dns_suffix(socket_path: &str) -> Option<String> {
	let client = reqwest::Client::builder()
		.unix_socket(socket_path)
		.build()
		.ok()?;
	let resp = client
		.get(format!("http://{LOCALAPI_HOST}/localapi/v0/status"))
		.send()
		.await
		.ok()?;
	if !resp.status().is_success() {
		return None;
	}
	let status: Status = resp.json().await.ok()?;
	let suffix = status.current_tailnet?.magic_dns_suffix;
	(!suffix.is_empty()).then_some(suffix)
}

/// Build the HTTPS URL of an exposed replica from its tailnet hostname and the
/// MagicDNS suffix. Pure; tolerates a leading dot on the suffix.
pub fn replica_url(hostname: &str, magic_dns_suffix: &str) -> String {
	format!(
		"https://{hostname}.{}",
		magic_dns_suffix.trim_start_matches('.')
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn builds_https_url() {
		assert_eq!(
			replica_url("infra-replica-site", "tailnet-abc.ts.net"),
			"https://infra-replica-site.tailnet-abc.ts.net"
		);
	}

	#[test]
	fn tolerates_leading_dot_on_suffix() {
		assert_eq!(
			replica_url("host", ".tailnet-abc.ts.net"),
			"https://host.tailnet-abc.ts.net"
		);
	}

	#[test]
	fn parses_magic_dns_suffix_from_status_json() {
		let json = serde_json::json!({
			"CurrentTailnet": { "Name": "example", "MagicDNSSuffix": "tailnet-abc.ts.net" },
			"Self": { "DNSName": "operator.tailnet-abc.ts.net." }
		});
		let status: Status = serde_json::from_value(json).unwrap();
		assert_eq!(
			status.current_tailnet.unwrap().magic_dns_suffix,
			"tailnet-abc.ts.net"
		);
	}

	#[test]
	fn missing_current_tailnet_is_none() {
		let status: Status = serde_json::from_value(serde_json::json!({})).unwrap();
		assert!(status.current_tailnet.is_none());
	}
}
