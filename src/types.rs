//! Common types shared across CRDs

use std::{borrow::Cow, collections::HashMap};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};

pub use self::{replica::*, restore::*};

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
		// Kubernetes structural schemas forbid `type` inside `anyOf` items,
		// so we emit the branches without top-level `type`.
		json_schema!({
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
