import argparse
from pathlib import Path
import numpy as np
import pandas as pd

VALID_ISO_CODES = ["BE", "DE", "EG", "ES", "FI", "FR", "GB", "IT", "NL", "SE", "US"]
INVALID_ISO_CODES = ["UK", "XX", "99"]  # "UK" is a common ERP mistake (ISO standard is "GB")
STATUS_CHOICES = ["IN_TRANSIT", "DELIVERED", "CUSTOMS_HOLD"]
STATUS_PROBS = [0.35, 0.55, 0.10]


def generate_supplier_a(n_rows: int, rng: np.random.Generator, now: pd.Timestamp) -> pd.DataFrame:
    """Supplier A: Clean baseline partner (metric kg, ISO-8601 UTC timestamps)."""
    ship_ids = np.array([f"SUPA-{i:07d}" for i in range(1, n_rows + 1)], dtype=object)
    # Minor 0.2% missing IDs -> 99.8% completeness (passes 99.0% SLA)
    null_mask = rng.random(n_rows) < 0.002
    ship_ids[null_mask] = None

    dest_iso = rng.choice(VALID_ISO_CODES, size=n_rows)
    gross_wt_kg = np.round(rng.uniform(25.0, 22000.0, size=n_rows), 2)
    statuses = rng.choice(STATUS_CHOICES, size=n_rows, p=STATUS_PROBS)

    dispatch_offsets = pd.to_timedelta(rng.integers(120, 480, size=n_rows), unit="h")
    dispatch_ts = pd.Series(now - dispatch_offsets)

    transit_hours = pd.to_timedelta(rng.integers(12, 96, size=n_rows), unit="h")
    delivery_ts = dispatch_ts + transit_hours
    delivery_ts = delivery_ts.where(statuses == "DELIVERED", pd.NaT)

    sync_offsets = pd.to_timedelta(rng.integers(5, 720, size=n_rows), unit="m")
    updated_ts = pd.Series(now - sync_offsets)

    iso_fmt = "%Y-%m-%dT%H:%M:%SZ"
    return pd.DataFrame({
        "ship_id": ship_ids,
        "dest_iso": dest_iso,
        "gross_wt_kg": gross_wt_kg,
        "dispatch_utc": dispatch_ts.dt.strftime(iso_fmt),
        "delivery_utc": delivery_ts.dt.strftime(iso_fmt),
        "ship_status": statuses,
        "updated_utc": updated_ts.dt.strftime(iso_fmt),
    })


def generate_supplier_b(n_rows: int, rng: np.random.Generator, now: pd.Timestamp) -> pd.DataFrame:
    """Supplier B: Heterogeneous schema (imperial lbs, DD/MM/YYYY dates) with seeded defects."""
    tracking_no = np.array([f"TRK-B-{i:07d}" for i in range(1, n_rows + 1)], dtype=object)
    # Defect 1 (Completeness): 2.5% missing tracking IDs -> fails 99.0% SLA
    tracking_no[rng.random(n_rows) < 0.025] = None

    country_code = rng.choice(VALID_ISO_CODES, size=n_rows).astype(object)
    # Defect 2 (Compliance): 1.5% invalid country codes ("UK", "XX", "99")
    bad_iso_mask = rng.random(n_rows) < 0.015
    country_code[bad_iso_mask] = rng.choice(INVALID_ISO_CODES, size=int(bad_iso_mask.sum()))

    # Normal weights in lbs (50 to 45,000 lbs ~= 22.7 to 20,411 kg)
    weight_lbs = np.round(rng.uniform(50.0, 45000.0, size=n_rows), 2)
    # Defect 3 (Accuracy/Bounds): 0.5% negative or overweight (>66,138 lbs = >30,000 kg)
    bad_wt_idx = rng.choice(n_rows, size=max(2, int(n_rows * 0.005)), replace=False)
    half_wt = len(bad_wt_idx) // 2
    weight_lbs[bad_wt_idx[:half_wt]] = -125.5
    weight_lbs[bad_wt_idx[half_wt:]] = 74500.0  # ~33,792 kg

    statuses = rng.choice(STATUS_CHOICES, size=n_rows, p=STATUS_PROBS)
    dispatch_offsets = pd.to_timedelta(rng.integers(120, 480, size=n_rows), unit="h")
    dispatch_ts = pd.Series(now - dispatch_offsets)

    transit_hours = pd.to_timedelta(rng.integers(12, 96, size=n_rows), unit="h")
    delivery_ts = dispatch_ts + transit_hours
    delivery_ts = delivery_ts.where(statuses == "DELIVERED", pd.NaT)

    # Defect 4 (Cross-Column Consistency): 1% of DELIVERED rows violate timeline or drop timestamp
    delivered_idx = np.where(statuses == "DELIVERED")[0]
    bad_deliv = rng.choice(delivered_idx, size=max(2, int(n_rows * 0.01)), replace=False)
    half_deliv = len(bad_deliv) // 2
    delivery_ts.iloc[bad_deliv[:half_deliv]] = dispatch_ts.iloc[bad_deliv[:half_deliv]] - pd.Timedelta(hours=8)
    delivery_ts.iloc[bad_deliv[half_deliv:]] = pd.NaT

    # Defect 5 (Currentness): 5% stale records (>24h old)
    sync_offsets = pd.to_timedelta(rng.integers(5, 720, size=n_rows), unit="m")
    updated_ts = pd.Series(now - sync_offsets)
    stale_mask = rng.random(n_rows) < 0.05
    stale_hours = pd.to_timedelta(rng.integers(36, 96, size=int(stale_mask.sum())), unit="h")
    updated_ts.loc[stale_mask] = now - stale_hours

    eu_fmt = "%d/%m/%Y %H:%M"
    return pd.DataFrame({
        "tracking_no": tracking_no,
        "country_code": country_code,
        "weight_lbs": weight_lbs,
        "sent_date": dispatch_ts.dt.strftime(eu_fmt),
        "arrived_date": delivery_ts.dt.strftime(eu_fmt),
        "state_code": statuses,
        "last_sync": updated_ts.dt.strftime(eu_fmt),
    })


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate heterogeneous multi-supplier CSV exports.")
    parser.add_argument("--rows", type=int, default=5000, help="Rows per supplier (default: 5000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--out-dir", type=Path, default=Path("data"), help="Output directory")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    now = pd.Timestamp.now(tz="UTC").floor("min")

    df_a = generate_supplier_a(args.rows, rng, now)
    df_b = generate_supplier_b(args.rows, rng, now)

    path_a = args.out_dir / "supplier_a_shipments.csv"
    path_b = args.out_dir / "supplier_b_shipments.csv"
    df_a.to_csv(path_a, index=False)
    df_b.to_csv(path_b, index=False)

    print(f"Generated {len(df_a):,} rows -> {path_a}")
    print(f"Generated {len(df_b):,} rows -> {path_b}")


if __name__ == "__main__":
    main()