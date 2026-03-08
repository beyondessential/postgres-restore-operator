use std::fmt;

use k8s_openapi::{
	api::core::v1::ResourceRequirements, apimachinery::pkg::api::resource::Quantity,
};
use kube_quantity::ParsedQuantity;

/// Kubernetes resource quantity units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityUnit {
	Ki,
	Mi,
	Gi,
	Ti,
	Pi,
	Ei,
}

impl QuantityUnit {
	fn bytes(self) -> u64 {
		match self {
			Self::Ki => 1 << 10,
			Self::Mi => 1 << 20,
			Self::Gi => 1 << 30,
			Self::Ti => 1 << 40,
			Self::Pi => 1 << 50,
			Self::Ei => 1 << 60,
		}
	}
}

impl fmt::Display for QuantityUnit {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(match self {
			Self::Ki => "Ki",
			Self::Mi => "Mi",
			Self::Gi => "Gi",
			Self::Ti => "Ti",
			Self::Pi => "Pi",
			Self::Ei => "Ei",
		})
	}
}

pub trait ParsedQuantityExt {
	/// Round up to the nearest whole multiple of `unit`.
	fn ceil_to(self, unit: QuantityUnit) -> ParsedQuantity;

	/// Create a quantity of the given value and unit
	fn from_unit(value: i64, unit: QuantityUnit) -> ParsedQuantity {
		// this is very dumb, we should be able to do it with Decimal
		// directly, but i can't figure it out right at this moment
		format!("{value}{unit}")
			.as_str()
			.try_into()
			.expect("formatted quantity must parse")
	}
}

impl ParsedQuantityExt for ParsedQuantity {
	fn ceil_to(self, unit: QuantityUnit) -> ParsedQuantity {
		let bytes = self.to_bytes_f64().unwrap_or(0.0);
		let unit_bytes = unit.bytes() as f64;
		let whole = bytes.div_euclid(unit_bytes);
		let has_remainder = bytes.rem_euclid(unit_bytes) > 0.0;
		let ceiled = whole as u64 + has_remainder as u64;
		format!("{ceiled}{unit}")
			.as_str()
			.try_into()
			.expect("formatted quantity must parse")
	}
}

const DEFAULT_MEMORY_REQUEST: &str = "1Gi";

/// Resolve the memory request from resource requirements, defaulting to 1Gi.
fn memory_request(resources: &Option<ResourceRequirements>) -> ParsedQuantity {
	resources
		.as_ref()
		.and_then(|r| r.requests.as_ref())
		.and_then(|r| r.get("memory"))
		.map(|q| -> ParsedQuantity { q.0.as_str().try_into().expect("valid memory quantity") })
		.unwrap_or_else(|| DEFAULT_MEMORY_REQUEST.try_into().expect("default parses"))
}

/// Resolve the memory limit from resource requirements.
fn memory_limit(resources: &Option<ResourceRequirements>) -> Option<ParsedQuantity> {
	resources
		.as_ref()
		.and_then(|r| r.limits.as_ref())
		.and_then(|r| r.get("memory"))
		.map(|q| -> ParsedQuantity { q.0.as_str().try_into().expect("valid memory quantity") })
}

/// Compute the SHM size for the restore database container.
///
/// `min(memory_request / 2, 36% of max(memory_request, memory_limit))`
///
/// Result is ceiled to whole MiB.
pub fn compute_shm_size(resources: &Option<ResourceRequirements>) -> Quantity {
	let request = memory_request(resources);
	let limit = memory_limit(resources);

	let request_bytes = request.to_bytes_f64().unwrap_or(0.0);
	let limit_bytes = limit.as_ref().and_then(|l| l.to_bytes_f64()).unwrap_or(0.0);

	let half_request = request_bytes / 2.0;
	let effective_max = request_bytes.max(limit_bytes);
	let thirty_six_pct = effective_max * 0.36;

	let shm_bytes = half_request.min(thirty_six_pct);

	let mib = (shm_bytes / (1 << 20) as f64).ceil() as u64;
	let mib = mib.max(16); // floor at 16Mi to avoid unusable values

	ParsedQuantity::from_unit(mib as i64, QuantityUnit::Mi).into()
}

/// Compute the `shared_buffers` PostgreSQL setting: 70% of SHM, as whole MB
/// (PostgreSQL uses base-10 MB for this setting).
pub fn compute_shared_buffers_mb(resources: &Option<ResourceRequirements>) -> u64 {
	let shm: Quantity = compute_shm_size(resources);
	let shm_pq: ParsedQuantity = shm.0.as_str().try_into().expect("shm quantity parses");
	let shm_bytes = shm_pq.to_bytes_f64().unwrap_or(0.0);

	let sb_bytes = shm_bytes * 0.70;
	// PostgreSQL shared_buffers uses base-10 MB (1 MB = 1_000_000 in docs,
	// but the server actually uses 1 MB = 1048576). Use binary MiB to be safe
	// and consistent with how PG interprets "MB".
	let mb = (sb_bytes / 1_048_576.0).floor() as u64;
	mb.max(16) // floor at 16MB
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

	use super::*;

	fn ceil(input: &str, unit: QuantityUnit) -> String {
		let pq: ParsedQuantity = input.try_into().unwrap();
		let result: Quantity = pq.ceil_to(unit).into();
		result.0
	}

	#[test]
	fn exact_gi() {
		assert_eq!(ceil("5Gi", QuantityUnit::Gi), "5Gi");
	}

	#[test]
	fn fractional_gi_rounds_up() {
		assert_eq!(ceil("5120Mi", QuantityUnit::Gi), "5Gi");
		assert_eq!(ceil("5121Mi", QuantityUnit::Gi), "6Gi");
	}

	#[test]
	fn sub_unit_rounds_up_to_one() {
		assert_eq!(ceil("500Mi", QuantityUnit::Gi), "1Gi");
		assert_eq!(ceil("1Mi", QuantityUnit::Gi), "1Gi");
	}

	#[test]
	fn zero_stays_zero() {
		assert_eq!(ceil("0", QuantityUnit::Gi), "0Gi");
	}

	#[test]
	fn mi_rounding() {
		assert_eq!(ceil("1025Ki", QuantityUnit::Mi), "2Mi");
		assert_eq!(ceil("1024Ki", QuantityUnit::Mi), "1Mi");
	}

	#[test]
	fn large_value_ti() {
		assert_eq!(ceil("2048Gi", QuantityUnit::Ti), "2Ti");
		assert_eq!(ceil("2049Gi", QuantityUnit::Ti), "3Ti");
	}

	#[test]
	fn cross_format_decimal_to_binary() {
		// 1G = 1_000_000_000 bytes, ceil(1_000_000_000 / 1Gi) = 1
		assert_eq!(ceil("1G", QuantityUnit::Gi), "1Gi");
		// 2G = 2_000_000_000 bytes, ceil(2_000_000_000 / 1Gi) = 2
		assert_eq!(ceil("2G", QuantityUnit::Gi), "2Gi");
	}

	fn resources_with(request: Option<&str>, limit: Option<&str>) -> Option<ResourceRequirements> {
		let requests =
			request.map(|r| BTreeMap::from([("memory".to_string(), Quantity(r.to_string()))]));
		let limits =
			limit.map(|l| BTreeMap::from([("memory".to_string(), Quantity(l.to_string()))]));
		Some(ResourceRequirements {
			requests,
			limits,
			..Default::default()
		})
	}

	#[test]
	fn shm_defaults_to_1gi_request() {
		// No resources: defaults to 1Gi request
		// min(1Gi/2, 36% of 1Gi) = min(512Mi, ceil(368.64)Mi) = 369Mi
		let shm = compute_shm_size(&None);
		assert_eq!(shm.0, "369Mi");
	}

	#[test]
	fn shm_request_only_2gi() {
		// 2Gi request, no limit
		// min(2Gi/2, 36% of 2Gi) = min(1Gi, ceil(737.28)Mi) = 738Mi
		let res = resources_with(Some("2Gi"), None);
		let shm = compute_shm_size(&res);
		assert_eq!(shm.0, "738Mi");
	}

	#[test]
	fn shm_request_and_limit() {
		// 2Gi request, 4Gi limit
		// min(2Gi/2, 36% of 4Gi) = min(1Gi, 1474Mi) = 1024Mi
		let res = resources_with(Some("2Gi"), Some("4Gi"));
		let shm = compute_shm_size(&res);
		assert_eq!(shm.0, "1024Mi");
	}

	#[test]
	fn shm_limit_smaller_than_request() {
		// 4Gi request, 2Gi limit (unusual but possible)
		// max(4Gi, 2Gi) = 4Gi, 36% of 4Gi = ceil(1474.56)Mi = 1475Mi
		// min(4Gi/2, 1475Mi) = min(2Gi, 1475Mi) = 1475Mi
		let res = resources_with(Some("4Gi"), Some("2Gi"));
		let shm = compute_shm_size(&res);
		assert_eq!(shm.0, "1475Mi");
	}

	#[test]
	fn shm_small_request_floors_at_16mi() {
		let res = resources_with(Some("32Mi"), None);
		let shm = compute_shm_size(&res);
		assert_eq!(shm.0, "16Mi");
	}

	#[test]
	fn shared_buffers_defaults() {
		// SHM with no resources = 369Mi
		// 70% of 369Mi = 258.3 -> floor = 258MB
		let sb = compute_shared_buffers_mb(&None);
		assert_eq!(sb, 258);
	}

	#[test]
	fn shared_buffers_2gi_request() {
		// SHM = 738Mi, 70% of 738Mi = 516.6 -> floor = 516MB
		let res = resources_with(Some("2Gi"), None);
		let sb = compute_shared_buffers_mb(&res);
		assert_eq!(sb, 516);
	}

	#[test]
	fn shared_buffers_small_floors_at_16() {
		let res = resources_with(Some("32Mi"), None);
		let sb = compute_shared_buffers_mb(&res);
		assert_eq!(sb, 16);
	}
}
