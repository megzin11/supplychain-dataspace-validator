import argparse
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

from app.graph_store import DataSpaceGraphStore
from app.validators import VALIDATOR_REGISTRY


def run_validation_batch(
    policies_path: Path,
    contracts_path: Path,
    reset_graph: bool = False,
) -> dict[str, float]:
    batch_id = f"batch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    executed_at = datetime.now(timezone.utc).isoformat()
    reference_time = pd.Timestamp.now(tz="UTC")

    with DataSpaceGraphStore() as store:
        if reset_graph:
            store.reset_graph()
        store.init_schema()
        store.load_policies(policies_path)
        store.register_suppliers(contracts_path)

        # 1. Cypher Multi-Hop Planning
        t0 = time.perf_counter()
        plan = store.resolve_execution_plan()
        plan_ms = (time.perf_counter() - t0) * 1000.0

        # 2. Load each supplier CSV once (native in-place tabular access)
        t_io = time.perf_counter()
        dataframes: dict[str, pd.DataFrame] = {}
        total_rows_scanned = 0
        for item in plan:
            fp = item["file_path"]
            if fp not in dataframes:
                dataframes[fp] = pd.read_csv(fp, keep_default_na=True)
                total_rows_scanned += len(dataframes[fp])
        io_ms = (time.perf_counter() - t_io) * 1000.0

        # 3. Execute vectorized validators & persist audit provenance
        val_ms_total = 0.0
        audit_records: list[dict] = []

        for item in plan:
            validator_name = item["validator"]
            validator_fn = VALIDATOR_REGISTRY.get(validator_name)
            if validator_fn is None:
                raise KeyError(f"Unregistered validator function: {validator_name}")

            df = dataframes[item["file_path"]]

            t_val = time.perf_counter()
            outcome = validator_fn(
                df=df,
                mappings=item["mappings"],
                params=item["params"],
                reference_time=reference_time,
            )
            check_ms = (time.perf_counter() - t_val) * 1000.0
            val_ms_total += check_ms

            status_tag = "PASS" if outcome.passed else "FAIL"
            print(
                f"[{status_tag}] {item['product_id']:<28} | {item['dimension']:<12} | "
                f"Violations: {outcome.violation_count:>6,} ({outcome.compliance_rate * 100:6.2f}%) | "
                f"{check_ms:6.2f} ms | {outcome.details}"
            )

            audit_records.append(
                {
                    "run_id": f"{batch_id}::{item['product_id']}::{item['policy_uid']}",
                    "batch_id": batch_id,
                    "executed_at": executed_at,
                    "product_id": item["product_id"],
                    "policy_uid": item["policy_uid"],
                    "validator_fn": validator_name,
                    "passed": outcome.passed,
                    "total_rows": outcome.total_rows,
                    "violation_count": outcome.violation_count,
                    "compliance_rate": outcome.compliance_rate,
                    "duration_ms": round(check_ms, 2),
                    "details": outcome.details,
                }
            )

        t_wb = time.perf_counter()
        store.record_validation_batch(audit_records)
        writeback_ms_total = (time.perf_counter() - t_wb) * 1000.0

        # 4. Graph Blast-Radius Audit
        blast_radius = store.get_blast_radius_audit(batch_id)
        print("\n=== Neo4j Downstream Blast-Radius Audit ===")
        if not blast_radius:
            print("All data products passed governance checks. Zero downstream consumers blocked.")
        else:
            for row in blast_radius:
                consumers = ", ".join(row["impacted_consumers"])
                print(
                    f"  BLOCKED -> {row['supplier_feed']} | {row['dimension']:<12} | "
                    f"{row['violations']:,} violations -> Impacts: {consumers}"
                )

        print("\n=== Execution Latency Breakdown ===")
        print(f"  Cypher Multi-Hop Planner : {plan_ms:8.2f} ms")
        print(f"  Native CSV Load (Pandas) : {io_ms:8.2f} ms")
        print(f"  In-Place Policy Checks   : {val_ms_total:8.2f} ms")
        print(f"  Neo4j Provenance Write   : {writeback_ms_total:8.2f} ms")
        print(f"  Total End-to-End Runtime : {plan_ms + io_ms + val_ms_total + writeback_ms_total:8.2f} ms\n")

        return {
            "rows": total_rows_scanned,
            "plan_ms": plan_ms,
            "io_ms": io_ms,
            "validation_ms": val_ms_total,
            "writeback_ms": writeback_ms_total,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Neo4j-backed Data Space validation batch.")
    parser.add_argument("--policies", type=Path, default=Path("policies/supply_chain_policies.json"))
    parser.add_argument("--contracts", type=Path, default=Path("policies/supplier_contracts.json"))
    parser.add_argument("--reset", action="store_true", help="Clear existing Neo4j graph before seeding")
    args = parser.parse_args()

    run_validation_batch(args.policies, args.contracts, reset_graph=args.reset)


if __name__ == "__main__":
    main()