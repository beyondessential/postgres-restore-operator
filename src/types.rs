//! Common types shared across CRDs

use std::borrow::Cow;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};

pub use self::{replica::*, restore::*};

pub mod cnpg;
mod replica;
mod restore;

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
