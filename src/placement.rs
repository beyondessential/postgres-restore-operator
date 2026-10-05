//! Operator-wide scheduling defaults for the pods the operator creates.
//!
//! Absence of a `nodeSelector` is not "let the scheduler decide sensibly" — in
//! a cluster with a weighted autoscaler it means "whichever node pool has the
//! highest weight and happens to match", which is a placement decision nobody
//! made. Restored databases and the jobs that populate them belong on the
//! cluster's workload tier, not wherever the default falls.
//!
//! Configured through the operator ConfigMap rather than the CRD: this is a
//! property of the cluster the operator runs in, not of any individual replica,
//! and canopy-managed replicas have their spec re-asserted on every tick so a
//! per-replica field would not survive anyway.

use std::collections::BTreeMap;

use k8s_openapi::api::{
	apps::v1::Deployment,
	batch::v1::Job,
	core::v1::{PodTemplateSpec, Toleration},
};
use kube::api::ObjectMeta;
use tracing::warn;

/// Scheduling defaults stamped onto every pod template the operator builds.
///
/// Empty in every field is the no-op default, which is what an operator with no
/// ConfigMap entries gets — the pre-existing behaviour.
// spec: PLACE
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PodPlacement {
	pub node_selector: BTreeMap<String, String>,
	pub annotations: BTreeMap<String, String>,
	pub tolerations: Vec<Toleration>,
}

/// Parse the `key=value,key=value` form used by the operator ConfigMap.
///
/// A malformed entry is warned about and skipped rather than failing the whole
/// key: dropping one bad pair leaves the rest of the placement in force, while
/// rejecting the lot would silently revert every pod to unconstrained
/// scheduling — the failure mode this module exists to prevent.
fn parse_pairs(raw: &str, field: &str) -> BTreeMap<String, String> {
	raw.split(',')
		.map(str::trim)
		.filter(|entry| !entry.is_empty())
		.filter_map(|entry| match entry.split_once('=') {
			Some((k, v)) if !k.trim().is_empty() => {
				Some((k.trim().to_string(), v.trim().to_string()))
			}
			_ => {
				warn!(
					field,
					entry, "ignoring malformed key=value pair in ConfigMap"
				);
				None
			}
		})
		.collect()
}

const TAINT_EFFECTS: [&str; 3] = ["NoSchedule", "PreferNoSchedule", "NoExecute"];

/// Parse the `key=value:Effect,key:Effect` form used by the operator ConfigMap,
/// mirroring `kubectl taint` syntax. `key=value` tolerates that exact taint;
/// a bare `key` tolerates the key with any value. Malformed entries are skipped
/// for the same reason as in [`parse_pairs`].
fn parse_tolerations(raw: &str) -> Vec<Toleration> {
	raw.split(',')
		.map(str::trim)
		.filter(|entry| !entry.is_empty())
		.filter_map(|entry| {
			let parsed = entry.rsplit_once(':').and_then(|(taint, effect)| {
				let effect = effect.trim();
				if !TAINT_EFFECTS.contains(&effect) {
					return None;
				}
				let (key, value) = match taint.split_once('=') {
					Some((k, v)) => (k.trim(), Some(v.trim())),
					None => (taint.trim(), None),
				};
				if key.is_empty() || value.is_some_and(str::is_empty) {
					return None;
				}
				Some(Toleration {
					key: Some(key.to_string()),
					operator: Some(if value.is_some() { "Equal" } else { "Exists" }.to_string()),
					value: value.map(str::to_string),
					effect: Some(effect.to_string()),
					toleration_seconds: None,
				})
			});
			if parsed.is_none() {
				warn!(
					field = "tolerations",
					entry, "ignoring malformed toleration in ConfigMap"
				);
			}
			parsed
		})
		.collect()
}

impl PodPlacement {
	/// Build from the raw ConfigMap strings.
	pub fn parse(node_selector: &str, annotations: &str, tolerations: &str) -> Self {
		Self {
			node_selector: parse_pairs(node_selector, "nodeSelector"),
			annotations: parse_pairs(annotations, "podAnnotations"),
			tolerations: parse_tolerations(tolerations),
		}
	}

	pub fn is_empty(&self) -> bool {
		self.node_selector.is_empty() && self.annotations.is_empty() && self.tolerations.is_empty()
	}

	/// Stamp the defaults onto a pod template.
	///
	/// Existing keys win. A builder that has already made a deliberate choice
	/// (or a replica spec that set `podAnnotations`) knows something the
	/// cluster-wide default doesn't, so the default only fills gaps.
	pub fn apply(&self, template: &mut PodTemplateSpec) {
		if self.is_empty() {
			return;
		}

		if !self.node_selector.is_empty() {
			let spec = template.spec.get_or_insert_with(Default::default);
			let selector = spec.node_selector.get_or_insert_with(Default::default);
			for (k, v) in &self.node_selector {
				selector.entry(k.clone()).or_insert_with(|| v.clone());
			}
		}

		if !self.annotations.is_empty() {
			let meta = template.metadata.get_or_insert_with(ObjectMeta::default);
			let annotations = meta.annotations.get_or_insert_with(Default::default);
			for (k, v) in &self.annotations {
				annotations.entry(k.clone()).or_insert_with(|| v.clone());
			}
		}

		if !self.tolerations.is_empty() {
			let spec = template.spec.get_or_insert_with(Default::default);
			let tolerations = spec.tolerations.get_or_insert_with(Default::default);
			for toleration in &self.tolerations {
				if !tolerations.contains(toleration) {
					tolerations.push(toleration.clone());
				}
			}
		}
	}

	/// Stamp the defaults onto a Job's pod template.
	pub fn apply_to_job(&self, job: &mut Job) {
		if let Some(spec) = job.spec.as_mut() {
			self.apply(&mut spec.template);
		}
	}

	/// Stamp the defaults onto a Deployment's pod template.
	pub fn apply_to_deployment(&self, deployment: &mut Deployment) {
		if let Some(spec) = deployment.spec.as_mut() {
			self.apply(&mut spec.template);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn placement(selector: &str, annotations: &str) -> PodPlacement {
		PodPlacement::parse(selector, annotations, "")
	}

	fn toleration(key: &str, value: Option<&str>, effect: &str) -> Toleration {
		Toleration {
			key: Some(key.to_string()),
			operator: Some(if value.is_some() { "Equal" } else { "Exists" }.to_string()),
			value: value.map(str::to_string),
			effect: Some(effect.to_string()),
			toleration_seconds: None,
		}
	}

	#[test]
	fn parses_a_single_pair() {
		let p = placement("bes.node.purpose=workload", "");
		assert_eq!(p.node_selector.get("bes.node.purpose").unwrap(), "workload");
		assert!(p.annotations.is_empty());
	}

	#[test]
	fn parses_multiple_pairs_and_trims() {
		let p = placement("a=1, b=2 ,c=3", " x=y ");
		assert_eq!(p.node_selector.len(), 3);
		assert_eq!(p.node_selector.get("b").unwrap(), "2");
		assert_eq!(p.annotations.get("x").unwrap(), "y");
	}

	/// Annotation values legitimately contain `=`, so only the first one splits.
	#[test]
	fn value_may_contain_equals() {
		let p = placement("", "k=a=b");
		assert_eq!(p.annotations.get("k").unwrap(), "a=b");
	}

	/// One bad pair must not disarm the rest: reverting every pod to
	/// unconstrained scheduling is a worse outcome than dropping one entry.
	#[test]
	fn malformed_pairs_are_skipped_not_fatal() {
		let p = placement("good=yes,garbage,=novalue,also=fine", "");
		assert_eq!(p.node_selector.len(), 2);
		assert_eq!(p.node_selector.get("good").unwrap(), "yes");
		assert_eq!(p.node_selector.get("also").unwrap(), "fine");
	}

	#[test]
	fn empty_input_is_the_no_op_default() {
		let p = placement("", "");
		assert!(p.is_empty());
		assert_eq!(p, PodPlacement::default());
	}

	#[test]
	fn applies_selector_and_annotations_to_a_bare_template() {
		let p = placement(
			"bes.node.purpose=workload",
			"karpenter.sh/do-not-disrupt=true",
		);
		let mut template = PodTemplateSpec::default();
		p.apply(&mut template);

		let spec = template.spec.expect("spec created");
		assert_eq!(
			spec.node_selector.unwrap().get("bes.node.purpose").unwrap(),
			"workload"
		);
		assert_eq!(
			template
				.metadata
				.unwrap()
				.annotations
				.unwrap()
				.get("karpenter.sh/do-not-disrupt")
				.unwrap(),
			"true"
		);
	}

	/// A deliberate choice already on the template knows something the
	/// cluster-wide default doesn't.
	#[test]
	fn existing_keys_win() {
		let p = placement("tier=default", "owner=operator");
		let mut template = PodTemplateSpec {
			metadata: Some(ObjectMeta {
				annotations: Some(BTreeMap::from([(
					"owner".to_string(),
					"replica-spec".to_string(),
				)])),
				..Default::default()
			}),
			spec: Some(k8s_openapi::api::core::v1::PodSpec {
				node_selector: Some(BTreeMap::from([(
					"tier".to_string(),
					"explicit".to_string(),
				)])),
				..Default::default()
			}),
		};
		p.apply(&mut template);

		assert_eq!(
			template
				.spec
				.unwrap()
				.node_selector
				.unwrap()
				.get("tier")
				.unwrap(),
			"explicit"
		);
		assert_eq!(
			template
				.metadata
				.unwrap()
				.annotations
				.unwrap()
				.get("owner")
				.unwrap(),
			"replica-spec"
		);
	}

	/// Unrelated keys already present survive the merge.
	#[test]
	fn merges_alongside_existing_keys() {
		let p = placement("added=yes", "");
		let mut template = PodTemplateSpec {
			spec: Some(k8s_openapi::api::core::v1::PodSpec {
				node_selector: Some(BTreeMap::from([("kept".to_string(), "yes".to_string())])),
				..Default::default()
			}),
			..Default::default()
		};
		p.apply(&mut template);

		let selector = template.spec.unwrap().node_selector.unwrap();
		assert_eq!(selector.len(), 2);
		assert_eq!(selector.get("kept").unwrap(), "yes");
		assert_eq!(selector.get("added").unwrap(), "yes");
	}

	/// The default placement must leave a template byte-identical, so an
	/// operator with no ConfigMap entries behaves exactly as before.
	#[test]
	fn empty_placement_leaves_the_template_untouched() {
		let mut template = PodTemplateSpec::default();
		PodPlacement::default().apply(&mut template);
		assert_eq!(template, PodTemplateSpec::default());
	}

	#[test]
	fn parses_value_and_exists_tolerations() {
		let p = PodPlacement::parse(
			"",
			"",
			"bes.node.group=db-replica:NoSchedule, spotInstance:PreferNoSchedule",
		);
		assert_eq!(
			p.tolerations,
			vec![
				toleration("bes.node.group", Some("db-replica"), "NoSchedule"),
				toleration("spotInstance", None, "PreferNoSchedule"),
			]
		);
		assert!(!p.is_empty());
	}

	#[test]
	fn malformed_tolerations_are_skipped_not_fatal() {
		let p = PodPlacement::parse(
			"",
			"",
			"good=yes:NoExecute,noeffect=x,bad=x:Sometimes,=x:NoSchedule,k=:NoSchedule,:NoSchedule",
		);
		assert_eq!(
			p.tolerations,
			vec![toleration("good", Some("yes"), "NoExecute")]
		);
	}

	#[test]
	fn appends_tolerations_without_duplicating() {
		let p = PodPlacement::parse("", "", "a=b:NoSchedule,c:NoExecute");
		let existing = Toleration {
			key: Some("node.kubernetes.io/not-ready".to_string()),
			operator: Some("Exists".to_string()),
			effect: Some("NoExecute".to_string()),
			toleration_seconds: Some(300),
			..Default::default()
		};
		let mut template = PodTemplateSpec {
			spec: Some(k8s_openapi::api::core::v1::PodSpec {
				tolerations: Some(vec![
					existing.clone(),
					toleration("a", Some("b"), "NoSchedule"),
				]),
				..Default::default()
			}),
			..Default::default()
		};
		p.apply(&mut template);

		assert_eq!(
			template.spec.unwrap().tolerations.unwrap(),
			vec![
				existing,
				toleration("a", Some("b"), "NoSchedule"),
				toleration("c", None, "NoExecute"),
			]
		);
	}
}
