use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::{
	Api, Client, ResourceExt,
	api::{ListParams, Portforwarder},
};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use tracing::{info, warn};

use crate::error::{Error, Result};

/// Holds a Postgres client and any resources that must stay alive for the
/// duration of the connection (e.g. a port-forwarder).
pub struct PgConnection {
	pub client: tokio_postgres::Client,
	_port_forwarder: Option<Portforwarder>,
}

/// Connect to a Postgres instance inside a Kubernetes pod via the kube
/// API port-forward mechanism.  This works both in-cluster and
/// out-of-cluster (e.g. CI with a kind cluster).
async fn connect_via_port_forward(
	client: &Client,
	namespace: &str,
	pod_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
) -> Result<PgConnection> {
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

	Ok(PgConnection {
		client: pg,
		_port_forwarder: Some(pf),
	})
}

/// Connect to a Postgres instance via direct TCP (in-cluster networking).
async fn connect_via_tcp(
	host: &str,
	port: u16,
	dbname: &str,
	user: &str,
	password: &str,
) -> Result<PgConnection> {
	let addr = format!("{host}:{port}");
	let stream = TcpStream::connect(&addr)
		.await
		.map_err(|e| Error::MissingField(format!("failed to connect to {addr}: {e}")))?;

	let mut config = tokio_postgres::Config::new();
	config.user(user);
	config.password(password);
	config.dbname(dbname);

	let (pg, conn) = config.connect_raw(stream, NoTls).await?;
	tokio::spawn(async move {
		if let Err(e) = conn.await {
			warn!(error = %e, "TCP database connection error");
		}
	});

	Ok(PgConnection {
		client: pg,
		_port_forwarder: None,
	})
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

/// Find the IP of a running pod that matches the given label selector.
pub async fn find_pod_ip_by_label(
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
	let ip = pod
		.status
		.as_ref()
		.and_then(|s| s.pod_ip.as_ref())
		.ok_or_else(|| Error::MissingField(format!("pod {} has no podIP", pod.name_any())))?;
	Ok(ip.clone())
}

/// Connect to the overlay CNPG database.
///
/// When `use_port_forward` is true, connects via the kube API port-forward
/// mechanism (useful for out-of-cluster test runners). Otherwise connects
/// directly via TCP to the CNPG `-rw` service.
pub async fn connect_overlay(
	client: &Client,
	cluster_name: &str,
	namespace: &str,
	su_secret: &Secret,
	use_port_forward: bool,
) -> Result<PgConnection> {
	let overlay_user = read_secret_field(su_secret, "username")?;
	let overlay_password = read_secret_field(su_secret, "password")?;

	if use_port_forward {
		let pod_name = format!("{cluster_name}-1");
		info!(pod = %pod_name, "connecting to overlay database via port-forward");
		connect_via_port_forward(
			client,
			namespace,
			&pod_name,
			"app",
			&overlay_user,
			&overlay_password,
		)
		.await
	} else {
		let host = format!("{cluster_name}-rw.{namespace}.svc");
		info!(host = %host, "connecting to overlay database via TCP");
		connect_via_tcp(&host, 5432, "app", &overlay_user, &overlay_password).await
	}
}

/// Connect to a restore's Postgres instance.
///
/// When `use_port_forward` is true, finds the restore pod by label and
/// connects via port-forward. Otherwise connects directly via the pod IP.
pub async fn connect_to_restore(
	client: &Client,
	namespace: &str,
	restore_name: &str,
	dbname: &str,
	user: &str,
	password: &str,
	use_port_forward: bool,
) -> Result<PgConnection> {
	let label_selector = format!("pgro.bes.au/restore={restore_name}");

	if use_port_forward {
		let pod_name = find_pod_by_label(client, namespace, &label_selector).await?;
		info!(pod = %pod_name, "connecting to restore database via port-forward");
		connect_via_port_forward(client, namespace, &pod_name, dbname, user, password).await
	} else {
		let pod_ip = find_pod_ip_by_label(client, namespace, &label_selector).await?;
		info!(ip = %pod_ip, restore = %restore_name, "connecting to restore database via TCP");
		connect_via_tcp(&pod_ip, 5432, dbname, user, password).await
	}
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
