use std::collections::HashMap;

use handlebars::Handlebars;
use jiff::Timestamp;
use k8s_openapi::{api::core::v1::Secret, apimachinery::pkg::apis::meta::v1::Time};
use kube::{Api, Client};
use serde_json::json;
use tracing::{info, warn};

use crate::{
	error::Error,
	types::{
		ConnectionInfo, GraphQLConfig, HeaderValue, NotificationConfig, NotificationStatus,
		WebhookConfig,
	},
};

/// Payload sent with notifications.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationPayload {
	pub timestamp: Timestamp,
	pub replica: ReplicaRef,
	pub restore: RestoreRef,
	pub connection_info: ConnectionInfoPayload,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplicaRef {
	pub name: String,
	pub namespace: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreRef {
	pub name: String,
	pub snapshot: String,
	pub postgres_version: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfoPayload {
	pub host: String,
	pub port: u16,
	pub database: String,
	pub username: String,
	pub password_secret: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub password: Option<String>,
}

impl ConnectionInfoPayload {
	pub fn from_connection_info(info: &ConnectionInfo, password: Option<String>) -> Self {
		Self {
			host: info.host.clone(),
			port: info.port,
			database: info.database.clone(),
			username: info.username.clone(),
			password_secret: info.password_secret.clone(),
			password,
		}
	}
}

/// Retry delays for notification attempts (in seconds).
const RETRY_DELAYS: &[u64] = &[0, 30, 120, 600, 3600];

/// Send a notification, retrying on failure.
pub async fn send_notification(
	client: &Client,
	http_client: &reqwest::Client,
	namespace: &str,
	config: &NotificationConfig,
	payload: &NotificationPayload,
) -> NotificationStatus {
	let now = Time(jiff::Timestamp::now());

	for (attempt, delay) in RETRY_DELAYS.iter().enumerate() {
		if *delay > 0 {
			tokio::time::sleep(std::time::Duration::from_secs(*delay)).await;
		}

		let result = match config {
			NotificationConfig::Webhook(webhook) => {
				send_webhook(client, http_client, namespace, webhook, payload).await
			}
			NotificationConfig::GraphQL(graphql) => {
				send_graphql(client, http_client, namespace, graphql, payload).await
			}
		};

		match result {
			Ok(()) => {
				info!(
					notification = config.name(),
					attempt = attempt + 1,
					"notification sent successfully"
				);
				return NotificationStatus {
					name: config.name(),
					last_sent_at: Some(now),
					success: true,
					last_error: None,
				};
			}
			Err(e) => {
				warn!(
					notification = config.name(),
					attempt = attempt + 1,
					error = %e,
					"notification failed, will retry"
				);
			}
		}
	}

	NotificationStatus {
		name: config.name(),
		last_sent_at: Some(now),
		success: false,
		last_error: Some("max retries exhausted".to_string()),
	}
}

async fn resolve_headers(
	client: &Client,
	namespace: &str,
	headers: &Option<HashMap<String, HeaderValue>>,
) -> Result<HashMap<String, String>, Error> {
	let mut resolved = HashMap::new();
	let Some(headers) = headers else {
		return Ok(resolved);
	};

	for (key, value) in headers {
		let v = match value {
			HeaderValue::Plain(s) => s.clone(),
			HeaderValue::Secret { secret_key_ref } => {
				let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
				let secret = secrets.get(&secret_key_ref.name).await?;
				let data = secret
					.data
					.as_ref()
					.ok_or_else(|| Error::Notification("secret has no data".to_string()))?;
				let bytes = data.get(&secret_key_ref.key).ok_or_else(|| {
					Error::Notification(format!(
						"secret {} missing key {}",
						secret_key_ref.name, secret_key_ref.key
					))
				})?;
				String::from_utf8(bytes.0.clone())
					.map_err(|_| Error::Notification("secret value not UTF-8".to_string()))?
			}
		};
		resolved.insert(key.clone(), v);
	}

	Ok(resolved)
}

async fn send_webhook(
	client: &Client,
	http_client: &reqwest::Client,
	namespace: &str,
	config: &WebhookConfig,
	payload: &NotificationPayload,
) -> Result<(), Error> {
	let headers = resolve_headers(client, namespace, &config.headers).await?;

	let mut req = match config.method.to_uppercase().as_str() {
		"POST" => http_client.post(&config.url),
		"PUT" => http_client.put(&config.url),
		_ => http_client.post(&config.url),
	};

	for (k, v) in &headers {
		req = req.header(k.as_str(), v.as_str());
	}

	let resp = req.json(payload).send().await?;

	if !resp.status().is_success() {
		return Err(Error::Notification(format!(
			"webhook returned status {}",
			resp.status()
		)));
	}

	Ok(())
}

async fn send_graphql(
	client: &Client,
	http_client: &reqwest::Client,
	namespace: &str,
	config: &GraphQLConfig,
	payload: &NotificationPayload,
) -> Result<(), Error> {
	let headers = resolve_headers(client, namespace, &config.headers).await?;

	// Render variables template
	let hbs = Handlebars::new();
	let template_data = json!({
		"Replica": payload.replica.name,
		"ConnectionInfo": {
			"Host": payload.connection_info.host,
			"Port": payload.connection_info.port,
			"Database": payload.connection_info.database,
			"Username": payload.connection_info.username,
			"PasswordSecret": payload.connection_info.password_secret,
			"Password": payload.connection_info.password,
		},
		"IncludePassword": payload.connection_info.password.is_some(),
		"Timestamp": payload.timestamp,
	});

	let variables_json = hbs
		.render_template(&config.variables_template, &template_data)
		.map_err(|e| Error::Notification(format!("template rendering failed: {e}")))?;

	let variables: serde_json::Value = serde_json::from_str(&variables_json)
		.map_err(|e| Error::Notification(format!("invalid variables JSON: {e}")))?;

	let body = json!({
		"query": config.mutation,
		"variables": variables,
	});

	let mut req = http_client.post(&config.url);
	for (k, v) in &headers {
		req = req.header(k.as_str(), v.as_str());
	}

	let resp = req.json(&body).send().await?;

	if !resp.status().is_success() {
		return Err(Error::Notification(format!(
			"GraphQL endpoint returned status {}",
			resp.status()
		)));
	}

	Ok(())
}
