//! Publishes operator events to a canopy-style `/events` endpoint.
//!
//! Canopy is the BES fleet metadata server at <https://meta.tamanu.app>.
//! Its event API accepts `(source, ref, message, ...)` tuples authenticated by
//! mTLS client certificate, and folds duplicates with the same `(source, ref)`
//! into a single rolling issue.

use jiff::Timestamp;
use k8s_openapi::api::core::v1::{Secret, SecretReference};
use kube::{Api, Client};
use serde::Serialize;

use crate::{
	error::{Error, Result},
	types::EventPublisherConfig,
};

/// Severity matching canopy's RFC 5424 enum.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
	Emergency,
	Alert,
	Critical,
	Error,
	Warning,
	Notice,
	Info,
	Debug,
}

/// Payload accepted by `POST /events`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NewEvent {
	pub source: String,
	/// Dedup key. Repeated events with the same `(source, ref)` roll up
	/// into a single issue server-side.
	#[serde(rename = "ref")]
	pub ref_: String,
	pub message: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub description: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub severity: Option<Severity>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub occurred_at: Option<Timestamp>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub active: Option<bool>,
}

/// Send a single event. Builds a fresh mTLS-configured `reqwest::Client` per
/// call — these publishes are rare (one per failed restore) so caching gains
/// nothing and a fresh client always picks up rotated certs.
pub async fn publish(
	client: &Client,
	config: &EventPublisherConfig,
	event: &NewEvent,
) -> Result<()> {
	let identity_pem = load_identity_pem(client, &config.client_certificate_secret_ref).await?;
	let identity = reqwest::Identity::from_pem(&identity_pem)
		.map_err(|e| Error::EventPublisher(format!("invalid client certificate: {e}")))?;

	let http = reqwest::Client::builder()
		.identity(identity)
		.build()
		.map_err(|e| Error::EventPublisher(format!("failed to build http client: {e}")))?;

	let resp = http.post(&config.url).json(event).send().await?;
	let status = resp.status();
	if !status.is_success() {
		let body = resp.text().await.unwrap_or_default();
		return Err(Error::EventPublisher(format!(
			"events endpoint returned {status}: {body}"
		)));
	}
	Ok(())
}

/// Pull `tls.crt` and `tls.key` out of the referenced Secret and concatenate
/// them into a single PEM buffer suitable for `Identity::from_pem`.
async fn load_identity_pem(client: &Client, secret_ref: &SecretReference) -> Result<Vec<u8>> {
	let name = secret_ref.name.as_deref().ok_or_else(|| {
		Error::EventPublisher("clientCertificateSecretRef.name is required".into())
	})?;
	let namespace = secret_ref.namespace.as_deref().ok_or_else(|| {
		Error::EventPublisher("clientCertificateSecretRef.namespace is required".into())
	})?;

	let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
	let secret = secrets.get(name).await?;
	let data = secret
		.data
		.as_ref()
		.ok_or_else(|| Error::EventPublisher(format!("secret {name} has no data")))?;

	let cert = data
		.get("tls.crt")
		.ok_or_else(|| Error::EventPublisher(format!("secret {name} missing key tls.crt")))?;
	let key = data
		.get("tls.key")
		.ok_or_else(|| Error::EventPublisher(format!("secret {name} missing key tls.key")))?;

	let mut buf = Vec::with_capacity(cert.0.len() + key.0.len() + 1);
	buf.extend_from_slice(&cert.0);
	if !cert.0.ends_with(b"\n") {
		buf.push(b'\n');
	}
	buf.extend_from_slice(&key.0);
	Ok(buf)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn event_serializes_with_expected_field_names() {
		let event = NewEvent {
			source: "pgro".into(),
			ref_: "ns/replica/restore-failed".into(),
			message: "boom".into(),
			description: Some("Restore failed".into()),
			severity: Some(Severity::Error),
			occurred_at: Some(Timestamp::from_second(1_700_000_000).unwrap()),
			active: Some(true),
		};
		let json = serde_json::to_value(&event).unwrap();
		assert_eq!(json["source"], "pgro");
		assert_eq!(json["ref"], "ns/replica/restore-failed");
		assert_eq!(json["message"], "boom");
		assert_eq!(json["description"], "Restore failed");
		assert_eq!(json["severity"], "error");
		assert_eq!(json["active"], true);
		assert!(json.get("occurredAt").is_some());
	}

	#[test]
	fn optional_fields_skipped_when_none() {
		let event = NewEvent {
			source: "pgro".into(),
			ref_: "r".into(),
			message: "m".into(),
			description: None,
			severity: None,
			occurred_at: None,
			active: None,
		};
		let json = serde_json::to_value(&event).unwrap();
		let obj = json.as_object().unwrap();
		assert!(!obj.contains_key("description"));
		assert!(!obj.contains_key("severity"));
		assert!(!obj.contains_key("occurredAt"));
		assert!(!obj.contains_key("active"));
	}
}
