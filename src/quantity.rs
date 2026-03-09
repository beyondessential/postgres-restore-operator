use std::fmt;

use k8s_openapi::{
	api::core::v1::ResourceRequirements, apimachinery::pkg::api::resource::Quantity,
};
use kube_quantity::ParsedQuantity;
use tracing::warn;

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

fn default_memory_request() -> ParsedQuantity {
	DEFAULT_MEMORY_REQUEST
		.try_into()
		.expect("default memory request parses")
}

/// Resolve the memory request from resource requirements, defaulting to 1Gi.
fn memory_request(resources: &Option<ResourceRequirements>) -> ParsedQuantity {
	resources
		.as_ref()
		.and_then(|r| r.requests.as_ref())
		.and_then(|r| r.get("memory"))
		.and_then(|q| match q.0.as_str().try_into() {
			Ok(pq) => Some(pq),
			Err(e) => {
				warn!(quantity = %q.0, error = %e, "failed to parse memory request, using default");
				None
			}
		})
		.unwrap_or_else(default_memory_request)
}

/// Resolve the memory limit from resource requirements.
fn memory_limit(resources: &Option<ResourceRequirements>) -> Option<ParsedQuantity> {
	resources
		.as_ref()
		.and_then(|r| r.limits.as_ref())
		.and_then(|r| r.get("memory"))
		.and_then(|q| match q.0.as_str().try_into() {
			Ok(pq) => Some(pq),
			Err(e) => {
				warn!(quantity = %q.0, error = %e, "failed to parse memory limit, ignoring");
				None
			}
		})
}

/// Compute SHM size in MiB.
///
/// `min(memory_request / 2, 36% of max(memory_request, memory_limit))`
///
/// When a memory limit is present, also caps at `limit / 2` to avoid OOM
/// (memory-backed emptyDir counts against the container's cgroup limit).
///
/// Result is ceiled to whole MiB, floored at 16 MiB.
fn compute_shm_mib(resources: &Option<ResourceRequirements>) -> u64 {
	let request = memory_request(resources);
	let limit = memory_limit(resources);

	let request_bytes = match request.to_bytes_f64() {
		Some(b) => b,
		None => {
			warn!("failed to convert memory request to bytes, using default SHM");
			let default: ParsedQuantity = DEFAULT_MEMORY_REQUEST
				.try_into()
				.expect("default memory request parses");
			return compute_shm_mib_from_bytes(default.to_bytes_f64().unwrap(), None);
		}
	};
	let limit_bytes = limit.as_ref().and_then(|l| {
		l.to_bytes_f64().or_else(|| {
			warn!("failed to convert memory limit to bytes, ignoring limit");
			None
		})
	});

	compute_shm_mib_from_bytes(request_bytes, limit_bytes)
}

fn compute_shm_mib_from_bytes(request_bytes: f64, limit_bytes: Option<f64>) -> u64 {
	let half_request = request_bytes / 2.0;
	let effective_max = limit_bytes.map_or(request_bytes, |l| request_bytes.max(l));
	let thirty_six_pct = effective_max * 0.36;

	let mut shm_bytes = half_request.min(thirty_six_pct);

	if let Some(lb) = limit_bytes {
		shm_bytes = shm_bytes.min(lb / 2.0);
	}

	let mib = (shm_bytes / (1 << 20) as f64).ceil() as u64;
	mib.max(16)
}

/// Compute both the SHM size ([`Quantity`] for emptyDir `sizeLimit`) and the
/// `shared_buffers` PostgreSQL setting (whole MB, base-10) from a single
/// parse of the resource requirements.
///
/// PostgreSQL interprets `MB` as 10^6 bytes (1 MB = 1,000,000), so
/// `shared_buffers` is computed in true megabytes.
pub fn compute_shm_and_shared_buffers(resources: &Option<ResourceRequirements>) -> (Quantity, u64) {
	let shm_mib = compute_shm_mib(resources);
	let shm_quantity = ParsedQuantity::from_unit(shm_mib as i64, QuantityUnit::Mi).into();
	let shm_bytes = shm_mib as f64 * (1 << 20) as f64;
	let sb_mb = (shm_bytes * 0.70 / 1_000_000.0).floor() as u64;
	(shm_quantity, sb_mb)
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
		let (shm, _) = compute_shm_and_shared_buffers(&None);
		assert_eq!(shm.0, "369Mi");
	}

	#[test]
	fn shm_request_only_2gi() {
		// 2Gi request, no limit
		// min(2Gi/2, 36% of 2Gi) = min(1Gi, ceil(737.28)Mi) = 738Mi
		let res = resources_with(Some("2Gi"), None);
		let (shm, _) = compute_shm_and_shared_buffers(&res);
		assert_eq!(shm.0, "738Mi");
	}

	#[test]
	fn shm_request_and_limit() {
		// 2Gi request, 4Gi limit
		// min(2Gi/2, 36% of 4Gi) = min(1Gi, 1475Mi) = 1024Mi
		let res = resources_with(Some("2Gi"), Some("4Gi"));
		let (shm, _) = compute_shm_and_shared_buffers(&res);
		assert_eq!(shm.0, "1024Mi");
	}

	#[test]
	fn shm_limit_smaller_than_request() {
		// 4Gi request, 2Gi limit (unusual but possible)
		// max(4Gi, 2Gi) = 4Gi, 36% of 4Gi = ceil(1474.56)Mi = 1475Mi
		// but capped at limit/2 = 1024Mi
		let res = resources_with(Some("4Gi"), Some("2Gi"));
		let (shm, _) = compute_shm_and_shared_buffers(&res);
		assert_eq!(shm.0, "1024Mi");
	}

	#[test]
	fn shm_small_request_floors_at_16mi() {
		let res = resources_with(Some("32Mi"), None);
		let (shm, _) = compute_shm_and_shared_buffers(&res);
		assert_eq!(shm.0, "16Mi");
	}

	#[test]
	fn shared_buffers_defaults() {
		// SHM with no resources = 369Mi
		// 70% of 369 MiB in bytes = 270,847,180.8 bytes / 1,000,000 = 270MB
		let (_, sb) = compute_shm_and_shared_buffers(&None);
		assert_eq!(sb, 270);
	}

	#[test]
	fn shared_buffers_2gi_request() {
		// SHM = 738Mi
		// 70% of 738 MiB in bytes = 541,694,361.6 bytes / 1,000,000 = 541MB
		let res = resources_with(Some("2Gi"), None);
		let (_, sb) = compute_shm_and_shared_buffers(&res);
		assert_eq!(sb, 541);
	}

	#[test]
	fn shared_buffers_small_request() {
		// SHM = 16Mi
		// 70% of 16 MiB in bytes = 11,744,051.2 bytes / 1,000,000 = 11MB
		let res = resources_with(Some("32Mi"), None);
		let (_, sb) = compute_shm_and_shared_buffers(&res);
		assert_eq!(sb, 11);
	}

	#[test]
	fn returns_both_values_from_single_call() {
		let res = resources_with(Some("2Gi"), Some("4Gi"));
		let (shm, sb) = compute_shm_and_shared_buffers(&res);
		assert_eq!(shm.0, "1024Mi");
		// 70% of 1024 MiB in bytes = 751,619,276.8 bytes / 1,000,000 = 751MB
		assert_eq!(sb, 751);
	}

	#[test]
	fn shm_unparsable_request_falls_back_to_default() {
		let res = Some(ResourceRequirements {
			requests: Some(BTreeMap::from([(
				"memory".to_string(),
				Quantity("bogus".to_string()),
			)])),
			..Default::default()
		});
		// Falls back to 1Gi default -> 369Mi
		let (shm, _) = compute_shm_and_shared_buffers(&res);
		assert_eq!(shm.0, "369Mi");
	}

	#[test]
	fn shm_unparsable_limit_is_ignored() {
		let res = Some(ResourceRequirements {
			requests: Some(BTreeMap::from([(
				"memory".to_string(),
				Quantity("2Gi".to_string()),
			)])),
			limits: Some(BTreeMap::from([(
				"memory".to_string(),
				Quantity("bogus".to_string()),
			)])),
			..Default::default()
		});
		// Limit ignored, treated as request-only: 738Mi
		let (shm, _) = compute_shm_and_shared_buffers(&res);
		assert_eq!(shm.0, "738Mi");
	}
}
