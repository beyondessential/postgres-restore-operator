use std::collections::HashMap;

use handlebars::Handlebars;
use k8s_openapi::api::core::v1::Secret;
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
    pub event: String,
    pub timestamp: String,
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
    let now = chrono::Utc::now().to_rfc3339();

    for (attempt, delay) in RETRY_DELAYS.iter().enumerate() {
        if *delay > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(*delay)).await;
        }

        let result = if let Some(webhook) = &config.webhook {
            send_webhook(client, http_client, namespace, webhook, payload).await
        } else if let Some(graphql) = &config.graphql {
            send_graphql(client, http_client, namespace, graphql, payload).await
        } else {
            Err(Error::Notification(
                "no webhook or graphql config".to_string(),
            ))
        };

        match result {
            Ok(()) => {
                info!(
                    notification = config.name,
                    attempt = attempt + 1,
                    "notification sent successfully"
                );
                return NotificationStatus {
                    name: config.name.clone(),
                    last_sent_at: Some(now),
                    success: true,
                    last_error: None,
                };
            }
            Err(e) => {
                warn!(
                    notification = config.name,
                    attempt = attempt + 1,
                    error = %e,
                    "notification failed, will retry"
                );
            }
        }
    }

    NotificationStatus {
        name: config.name.clone(),
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
        "Event": payload.event,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_info_payload_from_connection_info() {
        let info = ConnectionInfo {
            host: "my-svc.ns.svc.cluster.local".into(),
            port: 5432,
            database: "mydb".into(),
            username: "analytics".into(),
            password_secret: "my-secret".into(),
        };
        let payload = ConnectionInfoPayload::from_connection_info(&info, Some("hunter2".into()));
        assert_eq!(payload.host, "my-svc.ns.svc.cluster.local");
        assert_eq!(payload.port, 5432);
        assert_eq!(payload.database, "mydb");
        assert_eq!(payload.username, "analytics");
        assert_eq!(payload.password_secret, "my-secret");
        assert_eq!(payload.password, Some("hunter2".into()));
    }

    #[test]
    fn connection_info_payload_without_password() {
        let info = ConnectionInfo {
            host: "host".into(),
            port: 5432,
            database: "db".into(),
            username: "user".into(),
            password_secret: "sec".into(),
        };
        let payload = ConnectionInfoPayload::from_connection_info(&info, None);
        assert_eq!(payload.password, None);
    }

    #[test]
    fn notification_payload_serializes_camel_case() {
        let payload = NotificationPayload {
            event: "RestoreComplete".into(),
            timestamp: "2024-01-01T00:00:00Z".into(),
            replica: ReplicaRef {
                name: "my-replica".into(),
                namespace: "default".into(),
            },
            restore: RestoreRef {
                name: "my-restore".into(),
                snapshot: "snap-123".into(),
                postgres_version: "16".into(),
            },
            connection_info: ConnectionInfoPayload {
                host: "svc.ns".into(),
                port: 5432,
                database: "mydb".into(),
                username: "user".into(),
                password_secret: "secret".into(),
                password: None,
            },
        };
        let json = serde_json::to_value(&payload).unwrap();
        // camelCase keys
        assert!(json.get("connectionInfo").is_some());
        assert!(json.get("connection_info").is_none());
        let ci = json.get("connectionInfo").unwrap();
        assert!(ci.get("passwordSecret").is_some());
        // password should be omitted when None
        assert!(ci.get("password").is_none());
    }

    #[test]
    fn notification_payload_includes_password_when_set() {
        let payload = NotificationPayload {
            event: "RestoreComplete".into(),
            timestamp: "t".into(),
            replica: ReplicaRef {
                name: "r".into(),
                namespace: "ns".into(),
            },
            restore: RestoreRef {
                name: "x".into(),
                snapshot: "s".into(),
                postgres_version: "16".into(),
            },
            connection_info: ConnectionInfoPayload {
                host: "h".into(),
                port: 5432,
                database: "d".into(),
                username: "u".into(),
                password_secret: "ps".into(),
                password: Some("mypass".into()),
            },
        };
        let json = serde_json::to_value(&payload).unwrap();
        let ci = json.get("connectionInfo").unwrap();
        assert_eq!(ci.get("password").unwrap(), "mypass");
    }

    #[test]
    fn restore_ref_serializes_camel_case() {
        let r = RestoreRef {
            name: "n".into(),
            snapshot: "s".into(),
            postgres_version: "15".into(),
        };
        let json = serde_json::to_value(&r).unwrap();
        assert!(json.get("postgresVersion").is_some());
        assert!(json.get("postgres_version").is_none());
    }
}
