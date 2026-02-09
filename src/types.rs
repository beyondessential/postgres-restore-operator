//! Common types shared across CRDs

use std::collections::HashMap;

use schemars::JsonSchema;
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

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum HeaderValue {
	Plain(String),
	#[serde(rename_all = "camelCase")]
	Secret {
		secret_key_ref: SecretKeySelector,
	},
}
