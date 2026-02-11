use std::collections::BTreeMap;

use k8s_openapi::{
	api::core::v1::{Affinity, ResourceRequirements, Toleration},
	apimachinery::pkg::api::resource::Quantity,
};
use serde::{Deserialize, Serialize};

/// Minimal representation of a CNPG Cluster spec for creating overlay databases.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgClusterSpec {
	pub instances: i32,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub image_catalog_ref: Option<CnpgImageCatalogRef>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub image_name: Option<String>,

	pub storage: CnpgStorageSpec,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub postgresql: Option<CnpgPostgresqlSpec>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub resources: Option<ResourceRequirements>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub affinity: Option<Affinity>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub tolerations: Vec<Toleration>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub enable_superuser_access: Option<bool>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub managed: Option<CnpgManagedSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgImageCatalogRef {
	pub name: String,
	pub kind: String,
	pub major: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgStorageSpec {
	pub size: Quantity,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub storage_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgPostgresqlSpec {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub shared_preload_libraries: Vec<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parameters: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgManagedSpec {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub roles: Vec<CnpgManagedRole>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgManagedRole {
	pub name: String,

	#[serde(default = "ensure_present")]
	pub ensure: String,

	#[serde(default)]
	pub login: bool,

	#[serde(default)]
	pub superuser: bool,

	#[serde(default)]
	pub createdb: bool,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub password_secret: Option<CnpgPasswordSecretRef>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub in_roles: Vec<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connection_limit: Option<i64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub comment: Option<String>,
}

fn ensure_present() -> String {
	"present".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgPasswordSecretRef {
	pub name: String,
}

/// Minimal representation of CNPG Cluster status fields we need to check readiness.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CnpgClusterStatus {
	#[serde(default)]
	pub phase: Option<String>,

	#[serde(default)]
	pub ready_instances: Option<i32>,

	#[serde(default)]
	pub instances: Option<i32>,
}

impl CnpgClusterStatus {
	pub fn is_ready(&self) -> bool {
		let phase_ok = self
			.phase
			.as_deref()
			.is_some_and(|p| p == "Cluster in healthy state");
		let instances_ok = match (self.ready_instances, self.instances) {
			(Some(ready), Some(total)) => ready >= total && total > 0,
			_ => false,
		};
		phase_ok || instances_ok
	}
}

/// Minimal representation of a CNPG ClusterImageCatalog / ImageCatalog spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgImageCatalogSpec {
	pub images: Vec<CnpgCatalogImage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CnpgCatalogImage {
	pub image: String,
	pub major: i32,
}

/// API resource constants for CNPG types.
pub mod api {
	use kube::api::ApiResource;

	pub fn cluster_resource() -> ApiResource {
		ApiResource {
			group: "postgresql.cnpg.io".into(),
			version: "v1".into(),
			api_version: "postgresql.cnpg.io/v1".into(),
			kind: "Cluster".into(),
			plural: "clusters".into(),
		}
	}

	pub fn cluster_image_catalog_resource() -> ApiResource {
		ApiResource {
			group: "postgresql.cnpg.io".into(),
			version: "v1".into(),
			api_version: "postgresql.cnpg.io/v1".into(),
			kind: "ClusterImageCatalog".into(),
			plural: "clusterimagecatalogs".into(),
		}
	}

	pub fn image_catalog_resource() -> ApiResource {
		ApiResource {
			group: "postgresql.cnpg.io".into(),
			version: "v1".into(),
			api_version: "postgresql.cnpg.io/v1".into(),
			kind: "ImageCatalog".into(),
			plural: "imagecatalogs".into(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn cluster_status_ready_by_phase() {
		let status = CnpgClusterStatus {
			phase: Some("Cluster in healthy state".into()),
			ready_instances: None,
			instances: None,
		};
		assert!(status.is_ready());
	}

	#[test]
	fn cluster_status_ready_by_instances() {
		let status = CnpgClusterStatus {
			phase: Some("Setting up primary".into()),
			ready_instances: Some(1),
			instances: Some(1),
		};
		assert!(status.is_ready());
	}

	#[test]
	fn cluster_status_not_ready() {
		let status = CnpgClusterStatus {
			phase: Some("Setting up primary".into()),
			ready_instances: Some(0),
			instances: Some(1),
		};
		assert!(!status.is_ready());
	}

	#[test]
	fn cluster_status_default_not_ready() {
		let status = CnpgClusterStatus::default();
		assert!(!status.is_ready());
	}
}
