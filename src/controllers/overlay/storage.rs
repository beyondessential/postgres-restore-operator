use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube_quantity::ParsedQuantity;

use crate::quantity::{ParsedQuantityExt, QuantityUnit};

/// Compute overlay storage size for the FDW strategy.
///
/// Formula: `5Gi + ceil(snapshot_size / 10)`, rounded up to whole Gi.
/// FDW only stores metadata (foreign tables), so a fraction of the
/// snapshot size is sufficient.
pub fn compute_fdw_overlay_storage_size(snapshot_size: &Quantity) -> Quantity {
	let base = ParsedQuantity::from_unit(5, QuantityUnit::Gi);
	let extra = ParsedQuantity::try_from(snapshot_size).unwrap_or_default() / 10;
	((base + extra).ceil_to(QuantityUnit::Gi)).into()
}

/// Compute overlay storage size for the copy strategy.
///
/// Formula: `5Gi + snapshot_size`, rounded up to whole Gi.
/// Copy imports all data, so the overlay needs space for the full dataset.
pub fn compute_copy_overlay_storage_size(snapshot_size: &Quantity) -> Quantity {
	let base = ParsedQuantity::from_unit(5, QuantityUnit::Gi);
	let extra = ParsedQuantity::try_from(snapshot_size).unwrap_or_default();
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
	fn fdw_storage_100gi_snapshot() {
		// 100Gi snapshot -> extra = ceil(100Gi / 10) = 10Gi -> 5 + 10 = 15Gi
		let result = compute_fdw_overlay_storage_size(&Quantity("100Gi".into()));
		assert_eq!(result, Quantity("15Gi".into()));
	}

	#[test]
	fn fdw_storage_1gi_snapshot() {
		let result = compute_fdw_overlay_storage_size(&Quantity("1Gi".into()));
		assert_eq!(result, Quantity("6Gi".into()));
	}

	#[test]
	fn fdw_storage_500mi_snapshot() {
		let result = compute_fdw_overlay_storage_size(&Quantity("500Mi".into()));
		assert_eq!(result, Quantity("6Gi".into()));
	}

	#[test]
	fn fdw_storage_zero() {
		let result = compute_fdw_overlay_storage_size(&Quantity("0".into()));
		assert_eq!(result, Quantity("5Gi".into()));
	}

	#[test]
	fn fdw_storage_50gi_snapshot() {
		let result = compute_fdw_overlay_storage_size(&Quantity("50Gi".into()));
		assert_eq!(result, Quantity("10Gi".into()));
	}

	#[test]
	fn fdw_storage_bad_input() {
		let result = compute_fdw_overlay_storage_size(&Quantity("not-a-quantity".into()));
		assert_eq!(result, Quantity("5Gi".into()));
	}

	#[test]
	fn copy_storage_100gi_snapshot() {
		// 100Gi snapshot -> 5 + 100 = 105Gi
		let result = compute_copy_overlay_storage_size(&Quantity("100Gi".into()));
		assert_eq!(result, Quantity("105Gi".into()));
	}

	#[test]
	fn copy_storage_1gi_snapshot() {
		let result = compute_copy_overlay_storage_size(&Quantity("1Gi".into()));
		assert_eq!(result, Quantity("6Gi".into()));
	}

	#[test]
	fn copy_storage_500mi_snapshot() {
		// 500Mi rounds up to 1Gi -> 5 + 1 = 6Gi
		let result = compute_copy_overlay_storage_size(&Quantity("500Mi".into()));
		assert_eq!(result, Quantity("6Gi".into()));
	}

	#[test]
	fn copy_storage_zero() {
		let result = compute_copy_overlay_storage_size(&Quantity("0".into()));
		assert_eq!(result, Quantity("5Gi".into()));
	}

	#[test]
	fn copy_storage_50gi_snapshot() {
		let result = compute_copy_overlay_storage_size(&Quantity("50Gi".into()));
		assert_eq!(result, Quantity("55Gi".into()));
	}

	#[test]
	fn copy_storage_bad_input() {
		let result = compute_copy_overlay_storage_size(&Quantity("not-a-quantity".into()));
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
