use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube_quantity::ParsedQuantity;

use crate::quantity::{ParsedQuantityExt, QuantityUnit};

/// Compute overlay storage size from a snapshot size quantity string.
///
/// Formula: `5Gi + ceil(snapshot_size_bytes / 10)`, rounded up to whole Gi.
pub fn compute_overlay_storage_size(snapshot_size: &Quantity) -> Quantity {
	let base = ParsedQuantity::from_unit(5, QuantityUnit::Gi);
	let extra = ParsedQuantity::try_from(snapshot_size).unwrap_or_default() / 10;
	((base + extra).ceil_to(QuantityUnit::Gi)).into()
}

/// Apply ratchet logic: only increase, never shrink.
/// Returns the larger of `new_size` and `current_size`.
pub fn ratchet_storage_size<'a>(
	new_size: &'a Quantity,
	current_size: Option<&'a Quantity>,
) -> &'a Quantity {
	let Some(current) = current_size else {
		return new_size;
	};

	let new_pq: std::result::Result<ParsedQuantity, _> = new_size.try_into();
	let cur_pq: std::result::Result<ParsedQuantity, _> = current.try_into();

	match (new_pq, cur_pq) {
		(Ok(n), Ok(c)) if n > c => new_size,
		(_, Ok(_)) => current,
		_ => new_size,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn compute_overlay_storage_100gi_snapshot() {
		// 100Gi snapshot -> extra = ceil(100Gi / 10Gi) = 10Gi -> 5 + 10 = 15Gi
		let result = compute_overlay_storage_size(&Quantity("100Gi".into()));
		assert_eq!(result, Quantity("15Gi".into()));
	}

	#[test]
	fn compute_overlay_storage_1gi_snapshot() {
		// 1Gi snapshot -> extra = ceil(1Gi / 10Gi) = 1 -> 5 + 1 = 6Gi
		let result = compute_overlay_storage_size(&Quantity("1Gi".into()));
		assert_eq!(result, Quantity("6Gi".into()));
	}

	#[test]
	fn compute_overlay_storage_500mi_snapshot() {
		// 500Mi -> 500*1024*1024 = 524288000 bytes
		// extra_gi = ceil(524288000 / (10 * 1073741824)) = ceil(0.0488...) = 1
		// total = 5 + 1 = 6Gi
		let result = compute_overlay_storage_size(&Quantity("500Mi".into()));
		assert_eq!(result, Quantity("6Gi".into()));
	}

	#[test]
	fn compute_overlay_storage_zero() {
		let result = compute_overlay_storage_size(&Quantity("0".into()));
		assert_eq!(result, Quantity("5Gi".into()));
	}

	#[test]
	fn compute_overlay_storage_50gi_snapshot() {
		// 50Gi -> extra = ceil(50/10) = 5 -> 5 + 5 = 10Gi
		let result = compute_overlay_storage_size(&Quantity("50Gi".into()));
		assert_eq!(result, Quantity("10Gi".into()));
	}

	#[test]
	fn compute_overlay_storage_bad_input() {
		let result = compute_overlay_storage_size(&Quantity("not-a-quantity".into()));
		assert_eq!(result, Quantity("5Gi".into()));
	}

	#[test]
	fn ratchet_no_current() {
		assert_eq!(
			ratchet_storage_size(&Quantity("10Gi".into()), None),
			&Quantity("10Gi".into())
		);
	}

	#[test]
	fn ratchet_new_larger() {
		assert_eq!(
			ratchet_storage_size(&Quantity("15Gi".into()), Some(&Quantity("10Gi".into()))),
			&Quantity("15Gi".into())
		);
	}

	#[test]
	fn ratchet_new_smaller() {
		assert_eq!(
			ratchet_storage_size(&Quantity("8Gi".into()), Some(&Quantity("10Gi".into()))),
			&Quantity("10Gi".into())
		);
	}

	#[test]
	fn ratchet_equal() {
		assert_eq!(
			ratchet_storage_size(&Quantity("10Gi".into()), Some(&Quantity("10Gi".into()))),
			&Quantity("10Gi".into())
		);
	}

	#[test]
	fn ratchet_mixed_units() {
		// 1Gi = 1024Mi, so 1Gi > 512Mi
		assert_eq!(
			ratchet_storage_size(&Quantity("1Gi".into()), Some(&Quantity("512Mi".into()))),
			&Quantity("1Gi".into())
		);
		// 512Mi < 1Gi
		assert_eq!(
			ratchet_storage_size(&Quantity("512Mi".into()), Some(&Quantity("1Gi".into()))),
			&Quantity("1Gi".into())
		);
	}
}
