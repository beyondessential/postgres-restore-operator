use std::fmt;

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

#[cfg(test)]
mod tests {
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
}
