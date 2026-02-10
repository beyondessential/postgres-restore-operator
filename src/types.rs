//! Common types shared across CRDs

use std::{borrow::Cow, collections::HashMap};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};

pub use self::{replica::*, restore::*};

pub mod cnpg;
mod replica;
mod restore;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
	#[serde(rename = "type")]
	pub type_: String,
	pub status: String,
	pub reason: String,
	pub message: String,
	pub last_transition_time: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub requests: Option<HashMap<String, String>>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub limits: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub key: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub operator: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub value: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub effect: Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub toleration_seconds: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeySelector {
	pub name: String,
	pub key: String,
}

/// Kubernetes pod/node affinity rules.
///
/// Wraps an arbitrary JSON object with `x-kubernetes-preserve-unknown-fields`
/// so the full `k8s_openapi::api::core::v1::Affinity` structure passes through
/// without needing a 1-to-1 mirror of every nested type.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Affinity(pub serde_json::Value);

impl JsonSchema for Affinity {
	fn inline_schema() -> bool {
		true
	}

	fn schema_name() -> Cow<'static, str> {
		"Affinity".into()
	}

	fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
		json_schema!({
			"type": "object",
			"x-kubernetes-preserve-unknown-fields": true
		})
	}
}

impl Affinity {
	/// Convert into the k8s-openapi Affinity type for use in PodSpec.
	///
	/// Returns `None` if deserialization fails (malformed user input).
	pub fn to_k8s(&self) -> Option<k8s_openapi::api::core::v1::Affinity> {
		serde_json::from_value(self.0.clone()).ok()
	}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum HeaderValue {
	Plain(String),
	#[serde(rename_all = "camelCase")]
	Secret {
		secret_key_ref: SecretKeySelector,
	},
}

impl JsonSchema for HeaderValue {
	fn inline_schema() -> bool {
		true
	}

	fn schema_name() -> Cow<'static, str> {
		"HeaderValue".into()
	}

	fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
		// Kubernetes structural schemas require `type` on `additionalProperties`,
		// but HeaderValue is a string-or-object union so no single type works.
		// `x-kubernetes-preserve-unknown-fields` exempts us from that requirement.
		// The `anyOf` items must not set `type` (structural schema rule).
		json_schema!({
			"x-kubernetes-preserve-unknown-fields": true,
			"anyOf": [
				{},
				{
					"required": ["secretKeyRef"],
					"properties": {
						"secretKeyRef": {
							"type": "object",
							"properties": {
								"name": { "type": "string" },
								"key": { "type": "string" }
							},
							"required": ["name", "key"]
						}
					}
				}
			]
		})
	}
}
