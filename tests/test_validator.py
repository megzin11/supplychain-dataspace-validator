import pandas as pd
from app.validators import (
    validate_allowed_iso_codes,
    validate_delivery_temporal_consistency,
    validate_max_age_hours,
    validate_min_completeness,
    validate_numeric_range,
)


def test_min_completeness_enforces_sla_and_catches_whitespace():
    df = pd.DataFrame({"ship_id": ["S-1", "S-2", "   ", None, "S-5"]})
    mappings = [{"canonical_feature": "canonical:Shipment.id", "local_column": "ship_id"}]

    res = validate_min_completeness(df, mappings, {"min_ratio": 0.80})
    assert not res.passed
    assert res.violation_count == 2
    assert res.compliance_rate == 0.6


def test_allowed_iso_codes_rejects_non_standard_uk_code():
    df = pd.DataFrame({"country_code": ["DE", "GB", "UK", "XX"]})
    mappings = [{"canonical_feature": "canonical:Shipment.destinationCountry", "local_column": "country_code"}]

    res = validate_allowed_iso_codes(df, mappings, {"allowed_values": ["DE", "GB", "ES"]})
    assert not res.passed
    assert res.violation_count == 2
    assert "UK" in res.details


def test_numeric_range_normalizes_pounds_to_kilograms():
    # 66,000 lbs = 29,937.1 kg (valid); 70,000 lbs = 31,751.5 kg (over 30,000 kg); -5 lbs (invalid)
    df = pd.DataFrame({"weight_lbs": [1000.0, 66000.0, 70000.0, -5.0]})
    mappings = [
        {
            "canonical_feature": "canonical:Shipment.grossWeightKg",
            "local_column": "weight_lbs",
            "unit_factor": 0.45359237,
        }
    ]

    res = validate_numeric_range(df, mappings, {"min_exclusive": 0.0, "max_inclusive": 30000.0})
    assert not res.passed
    assert res.violation_count == 2
    assert "normalized x0.4536 -> kg" in res.details


def test_max_age_hours_parses_european_timestamps():
    ref_time = pd.Timestamp("2026-11-19T12:00:00Z")
    df = pd.DataFrame({"last_sync": ["19/11/2026 08:00", "17/11/2026 08:00", "bad-date"]})
    mappings = [
        {
            "canonical_feature": "canonical:Shipment.lastUpdatedUtc",
            "local_column": "last_sync",
            "date_format": "%d/%m/%Y %H:%M",
        }
    ]

    res = validate_max_age_hours(df, mappings, {"max_hours": 24.0}, reference_time=ref_time)
    assert not res.passed
    assert res.violation_count == 2


def test_delivery_temporal_consistency_catches_both_error_classes():
    df = pd.DataFrame(
        {
            "sent_date": ["10/11/2026 10:00", "10/11/2026 10:00", "10/11/2026 10:00", "10/11/2026 10:00"],
            "arrived_date": ["12/11/2026 10:00", "09/11/2026 10:00", None, None],
            "state_code": ["DELIVERED", "DELIVERED", "DELIVERED", "IN_TRANSIT"],
        }
    )
    mappings = [
        {"canonical_feature": "canonical:Shipment.dispatchTimestamp", "local_column": "sent_date", "date_format": "%d/%m/%Y %H:%M"},
        {"canonical_feature": "canonical:Shipment.deliveryTimestamp", "local_column": "arrived_date", "date_format": "%d/%m/%Y %H:%M"},
        {"canonical_feature": "canonical:Shipment.status", "local_column": "state_code"},
    ]

    res = validate_delivery_temporal_consistency(df, mappings, {"delivered_label": "DELIVERED"})
    assert not res.passed
    assert res.violation_count == 2  # row 1 (chronology) + row 2 (DELIVERED without timestamp); row 3 (IN_TRANSIT) is valid
    assert "chronology_errors=1" in res.details
    assert "delivered_without_ts=1" in res.details


def test_delivery_temporal_consistency_passes_clean_records():
    df = pd.DataFrame(
        {
            "dispatch_utc": ["2026-11-10T10:00:00Z", "2026-11-10T10:00:00Z"],
            "delivery_utc": ["2026-11-12T10:00:00Z", None],
            "ship_status": ["DELIVERED", "IN_TRANSIT"],
        }
    )
    mappings = [
        {"canonical_feature": "canonical:Shipment.dispatchTimestamp", "local_column": "dispatch_utc", "date_format": "%Y-%m-%dT%H:%M:%SZ"},
        {"canonical_feature": "canonical:Shipment.deliveryTimestamp", "local_column": "delivery_utc", "date_format": "%Y-%m-%dT%H:%M:%SZ"},
        {"canonical_feature": "canonical:Shipment.status", "local_column": "ship_status"},
    ]

    res = validate_delivery_temporal_consistency(df, mappings, {"delivered_label": "DELIVERED"})
    assert res.passed
    assert res.violation_count == 0