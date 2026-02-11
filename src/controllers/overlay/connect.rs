use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::{
	Api, Client, ResourceExt,
	api::{ListParams, Portforwarder},
};
use tokio_postgres::NoTls;
use tracing::{info, warn};

use crate::error::{Error, Result};

/// Connect to a Postgres instance inside a Kubernetes pod via the kube
/// API port-forward mechanism.  This works both in-cluster and
/// out-of-cluster (e.g. CI with a kind cluster).
pub async fn connect_via_pod(
	client: &Client,
	namespace: &str,
	pod_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
) -> Result<(tokio_postgres::Client, Portforwarder)> {
	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let mut pf = pods.portforward(pod_name, &[5432]).await?;
	let stream = pf
		.take_stream(5432)
		.ok_or_else(|| Error::MissingField("port-forward stream not available on 5432".into()))?;

	let mut config = tokio_postgres::Config::new();
	config.user(user);
	config.password(password);
	config.dbname(dbname);

	let (pg, conn) = config.connect_raw(stream, NoTls).await?;
	tokio::spawn(async move {
		if let Err(e) = conn.await {
			warn!(error = %e, "port-forwarded database connection error");
		}
	});

	Ok((pg, pf))
}

/// Find the name of a running pod that matches the given label selector.
pub async fn find_pod_by_label(
	client: &Client,
	namespace: &str,
	label_selector: &str,
) -> Result<String> {
	let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
	let lp = ListParams::default().labels(label_selector);
	let list = pods.list(&lp).await?;
	let pod = list
		.items
		.into_iter()
		.find(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"))
		.ok_or_else(|| {
			Error::MissingField(format!(
				"no running pod found with label {label_selector} in {namespace}"
			))
		})?;
	Ok(pod.name_any())
}

pub async fn connect_overlay(
	client: &Client,
	cluster_name: &str,
	namespace: &str,
	su_secret: &Secret,
) -> Result<(tokio_postgres::Client, Portforwarder)> {
	let overlay_user = read_secret_field(su_secret, "username")?;
	let overlay_password = read_secret_field(su_secret, "password")?;
	let pod_name = format!("{cluster_name}-1");

	info!(pod = %pod_name, "connecting to overlay database via port-forward");
	let (pg, pf) = connect_via_pod(
		client,
		namespace,
		&pod_name,
		"app",
		&overlay_user,
		&overlay_password,
	)
	.await?;
	info!(pod = %pod_name, "connected to overlay database");
	Ok((pg, pf))
}

/// Read a UTF-8 string field from a Kubernetes Secret.
pub fn read_secret_field(secret: &Secret, key: &str) -> Result<String> {
	let data = secret
		.data
		.as_ref()
		.ok_or_else(|| Error::MissingField("secret has no data".to_string()))?;
	let bytes = data
		.get(key)
		.ok_or_else(|| Error::MissingField(format!("secret missing key: {key}")))?;
	String::from_utf8(bytes.0.clone())
		.map_err(|_| Error::MissingField(format!("secret key {key} is not valid UTF-8")))
}
