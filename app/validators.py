from dataclasses import dataclass
from typing import Any, Callable
import pandas as pd


@dataclass(frozen=True)
class ValidationOutcome:
    passed: bool
    total_rows: int
    violation_count: int
    compliance_rate: float
    details: str


def _feature_map(mappings: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {m["canonical_feature"]: m for m in mappings}


def validate_min_completeness(
    df: pd.DataFrame,
    mappings: list[dict[str, Any]],
    params: dict[str, Any],
    **_: Any,
) -> ValidationOutcome:
    col = mappings[0]["local_column"]
    total = len(df)
    if total == 0:
        return ValidationOutcome(False, 0, 0, 0.0, f"Empty dataset for column '{col}'")

    series = df[col]
    missing_mask = series.isna() | series.astype(str).str.strip().eq("")
    violations = int(missing_mask.sum())
    rate = round(1.0 - (violations / total), 6)
    min_ratio = float(params["min_ratio"])
    return ValidationOutcome(
        passed=rate >= min_ratio,
        total_rows=total,
        violation_count=violations,
        compliance_rate=rate,
        details=f"Column '{col}': {rate * 100:.2f}% non-null (SLA >= {min_ratio * 100:.1f}%)",
    )


def validate_allowed_iso_codes(
    df: pd.DataFrame,
    mappings: list[dict[str, Any]],
    params: dict[str, Any],
    **_: Any,
) -> ValidationOutcome:
    col = mappings[0]["local_column"]
    total = len(df)
    allowed = set(params["allowed_values"])
    invalid_mask = ~df[col].isin(allowed)
    violations = int(invalid_mask.sum())
    rate = round(1.0 - (violations / total), 6) if total else 0.0

    bad_examples = df.loc[invalid_mask, col].dropna().unique()[:5].tolist()
    sample_note = f" (examples: {bad_examples})" if bad_examples else ""
    return ValidationOutcome(
        passed=violations == 0,
        total_rows=total,
        violation_count=violations,
        compliance_rate=rate,
        details=f"Column '{col}': {violations:,} non-ISO values{sample_note}",
    )


def validate_numeric_range(
    df: pd.DataFrame,
    mappings: list[dict[str, Any]],
    params: dict[str, Any],
    **_: Any,
) -> ValidationOutcome:
    mapping = mappings[0]
    col = mapping["local_column"]
    unit_factor = float(mapping.get("unit_factor") or 1.0)
    total = len(df)

    normalized = pd.to_numeric(df[col], errors="coerce") * unit_factor
    min_excl = float(params["min_exclusive"])
    max_incl = float(params["max_inclusive"])
    invalid_mask = normalized.isna() | (normalized <= min_excl) | (normalized > max_incl)

    violations = int(invalid_mask.sum())
    rate = round(1.0 - (violations / total), 6) if total else 0.0
    unit_note = f" [normalized x{unit_factor:.4f} -> kg]" if unit_factor != 1.0 else ""
    return ValidationOutcome(
        passed=violations == 0,
        total_rows=total,
        violation_count=violations,
        compliance_rate=rate,
        details=f"Column '{col}'{unit_note}: {violations:,} out-of-bounds ({min_excl}, {max_incl}]",
    )


def validate_max_age_hours(
    df: pd.DataFrame,
    mappings: list[dict[str, Any]],
    params: dict[str, Any],
    reference_time: pd.Timestamp | None = None,
    **_: Any,
) -> ValidationOutcome:
    mapping = mappings[0]
    col = mapping["local_column"]
    date_format = mapping.get("date_format")
    total = len(df)

    now = reference_time or pd.Timestamp.now(tz="UTC")
    parsed = pd.to_datetime(df[col], format=date_format, utc=True, errors="coerce")
    age_hours = (now - parsed).dt.total_seconds() / 3600.0
    max_hours = float(params["max_hours"])

    invalid_mask = parsed.isna() | (age_hours > max_hours)
    violations = int(invalid_mask.sum())
    rate = round(1.0 - (violations / total), 6) if total else 0.0
    return ValidationOutcome(
        passed=violations == 0,
        total_rows=total,
        violation_count=violations,
        compliance_rate=rate,
        details=f"Column '{col}' (fmt='{date_format}'): {violations:,} stale/unparseable (> {max_hours:.0f}h)",
    )


def validate_delivery_temporal_consistency(
    df: pd.DataFrame,
    mappings: list[dict[str, Any]],
    params: dict[str, Any],
    **_: Any,
) -> ValidationOutcome:
    fmap = _feature_map(mappings)
    disp_m = fmap["canonical:Shipment.dispatchTimestamp"]
    deliv_m = fmap["canonical:Shipment.deliveryTimestamp"]
    status_m = fmap["canonical:Shipment.status"]
    total = len(df)

    dispatch_ts = pd.to_datetime(
        df[disp_m["local_column"]], format=disp_m.get("date_format"), utc=True, errors="coerce"
    )
    delivery_ts = pd.to_datetime(
        df[deliv_m["local_column"]], format=deliv_m.get("date_format"), utc=True, errors="coerce"
    )
    status = df[status_m["local_column"]]

    chronology_err = delivery_ts.notna() & dispatch_ts.notna() & (delivery_ts < dispatch_ts)
    missing_deliv_err = (status == params["delivered_label"]) & delivery_ts.isna()
    invalid_mask = chronology_err | missing_deliv_err

    violations = int(invalid_mask.sum())
    rate = round(1.0 - (violations / total), 6) if total else 0.0
    return ValidationOutcome(
        passed=violations == 0,
        total_rows=total,
        violation_count=violations,
        compliance_rate=rate,
        details=(
            f"chronology_errors={int(chronology_err.sum()):,}, "
            f"delivered_without_ts={int(missing_deliv_err.sum()):,}"
        ),
    )


VALIDATOR_REGISTRY: dict[str, Callable[..., ValidationOutcome]] = {
    "min_completeness": validate_min_completeness,
    "allowed_iso_codes": validate_allowed_iso_codes,
    "numeric_range": validate_numeric_range,
    "max_age_hours": validate_max_age_hours,
    "delivery_temporal_consistency": validate_delivery_temporal_consistency,
}