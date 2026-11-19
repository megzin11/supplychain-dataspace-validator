import json
import os
from pathlib import Path
from typing import Any
from neo4j import GraphDatabase


class DataSpaceGraphStore:
    """Neo4j Property Graph store for federated schemas, ODRL policies, and audit provenance."""

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "dataspace123")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self) -> None:
        self.driver.close()

    def __enter__(self) -> "DataSpaceGraphStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def reset_graph(self) -> None:
        """Remove all nodes and relationships (used for clean reproducible runs)."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def init_schema(self) -> None:
        """Create uniqueness constraints for core Data Space entities."""
        constraints = [
            "CREATE CONSTRAINT dp_id IF NOT EXISTS FOR (n:DataProduct) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT dc_id IF NOT EXISTS FOR (n:DataContract) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT cf_uid IF NOT EXISTS FOR (n:CanonicalFeature) REQUIRE n.uid IS UNIQUE",
            "CREATE CONSTRAINT qp_uid IF NOT EXISTS FOR (n:QualityPolicy) REQUIRE n.uid IS UNIQUE",
            "CREATE CONSTRAINT cons_id IF NOT EXISTS FOR (n:DataConsumer) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT sm_id IF NOT EXISTS FOR (n:SchemaMapping) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT run_id IF NOT EXISTS FOR (n:ValidationRun) REQUIRE n.run_id IS UNIQUE",
        ]
        with self.driver.session() as session:
            for stmt in constraints:
                session.run(stmt)

    def load_policies(self, policy_path: Path) -> int:
        """Ingest declarative ODRL/DQV policies and link them to CanonicalFeature nodes."""
        payload = json.loads(policy_path.read_text(encoding="utf-8"))
        policies = payload["policies"]

        query = """
        MERGE (qp:QualityPolicy {uid: $uid})
        SET qp.name = $name,
            qp.dqv_dimension = $dqv_dimension,
            qp.description = $description,
            qp.validator = $validator,
            qp.params_json = $params_json,
            qp.required_feature_count = size($target_features)
        WITH qp
        UNWIND $target_features AS feature_uid
        MERGE (cf:CanonicalFeature {uid: feature_uid})
        MERGE (qp)-[:TARGETS]->(cf)
        """
        with self.driver.session() as session:
            for p in policies:
                session.run(
                    query,
                    uid=p["uid"],
                    name=p["name"],
                    dqv_dimension=p["dqv_dimension"],
                    description=p["description"],
                    validator=p["validator"],
                    params_json=json.dumps(p["params"]),
                    target_features=p["target_features"],
                )
        return len(policies)

    def register_suppliers(self, contracts_path: Path) -> int:
        """Register suppliers, downstream consumers, and schema/unit mappings in Neo4j."""
        payload = json.loads(contracts_path.read_text(encoding="utf-8"))

        consumer_query = """
        MERGE (c:DataConsumer {id: $id})
        SET c.name = $name, c.criticality = $criticality
        """
        product_query = """
        MERGE (dp:DataProduct {id: $id})
        SET dp.name = $name, dp.supplier = $supplier, dp.file_path = $file_path
        MERGE (dc:DataContract {id: $contract_id})
        MERGE (dp)-[:HAS_CONTRACT]->(dc)
        WITH dp, dc
        UNWIND $consumers AS consumer_id
        MATCH (c:DataConsumer {id: consumer_id})
        MERGE (dp)-[:CONSUMED_BY]->(c)
        """
        mapping_query = """
        MATCH (dc:DataContract {id: $contract_id})
        MERGE (cf:CanonicalFeature {uid: $canonical_feature})
        MERGE (sm:SchemaMapping {id: $mapping_id})
        SET sm.local_column = $local_column,
            sm.unit_factor = $unit_factor,
            sm.date_format = $date_format
        MERGE (dc)-[:MAPS_VIA]->(sm)
        MERGE (sm)-[:MAPS_TO]->(cf)
        """
        with self.driver.session() as session:
            for c in payload["consumers"]:
                session.run(consumer_query, **c)

            for dp in payload["data_products"]:
                session.run(
                    product_query,
                    id=dp["id"],
                    name=dp["name"],
                    supplier=dp["supplier"],
                    file_path=dp["file_path"],
                    contract_id=dp["contract_id"],
                    consumers=dp["consumers"],
                )
                for m in dp["mappings"]:
                    session.run(
                        mapping_query,
                        contract_id=dp["contract_id"],
                        canonical_feature=m["canonical_feature"],
                        mapping_id=f"{dp['contract_id']}::{m['local_column']}",
                        local_column=m["local_column"],
                        unit_factor=m.get("unit_factor"),
                        date_format=m.get("date_format"),
                    )
        return len(payload["data_products"])

    def resolve_execution_plan(self) -> list[dict[str, Any]]:
        """
        Multi-hop Cypher Planner: resolves canonical policies to physical supplier columns
        and normalization rules without hardcoding schema names in Python.
        """
        query = """
        MATCH (dp:DataProduct)-[:HAS_CONTRACT]->(dc:DataContract)
              -[:MAPS_VIA]->(sm:SchemaMapping)-[:MAPS_TO]->(cf:CanonicalFeature)
              <-[:TARGETS]-(qp:QualityPolicy)
        WITH dp, qp,
             collect({
                 canonical_feature: cf.uid,
                 local_column: sm.local_column,
                 unit_factor: sm.unit_factor,
                 date_format: sm.date_format
             }) AS mappings
        WHERE size(mappings) = qp.required_feature_count
        RETURN dp.id AS product_id,
               dp.name AS product_name,
               dp.file_path AS file_path,
               qp.uid AS policy_uid,
               qp.name AS policy_name,
               qp.dqv_dimension AS dimension,
               qp.validator AS validator,
               qp.params_json AS params_json,
               mappings
        ORDER BY dp.id, qp.uid
        """
        with self.driver.session() as session:
            records = session.run(query)
            plan = []
            for rec in records:
                item = dict(rec)
                item["params"] = json.loads(item.pop("params_json"))
                plan.append(item)
            return plan

    def record_validation_result(self, result: dict[str, Any]) -> None:
        """Persist validation verdict and lineage back into Neo4j for auditability."""
        query = """
        MATCH (dp:DataProduct {id: $product_id})
        MATCH (qp:QualityPolicy {uid: $policy_uid})
        CREATE (vr:ValidationRun {
            run_id: $run_id,
            batch_id: $batch_id,
            executed_at: $executed_at,
            validator_fn: $validator_fn,
            passed: $passed,
            total_rows: $total_rows,
            violation_count: $violation_count,
            compliance_rate: $compliance_rate,
            duration_ms: $duration_ms,
            details: $details
        })
        CREATE (vr)-[:EVALUATED]->(dp)
        CREATE (vr)-[:CHECKED_POLICY]->(qp)
        """
        with self.driver.session() as session:
            session.run(query, **result)

    def record_validation_batch(self, results: list[dict[str, Any]]) -> None:
        """Persist all validation verdicts in a single atomic Cypher UNWIND transaction."""
        if not results:
            return
        query = """
        UNWIND $results AS r
        MATCH (dp:DataProduct {id: r.product_id})
        MATCH (qp:QualityPolicy {uid: r.policy_uid})
        CREATE (vr:ValidationRun {
            run_id: r.run_id,
            batch_id: r.batch_id,
            executed_at: r.executed_at,
            validator_fn: r.validator_fn,
            passed: r.passed,
            total_rows: r.total_rows,
            violation_count: r.violation_count,
            compliance_rate: r.compliance_rate,
            duration_ms: r.duration_ms,
            details: r.details
        })
        CREATE (vr)-[:EVALUATED]->(dp)
        CREATE (vr)-[:CHECKED_POLICY]->(qp)
        """
        with self.driver.session() as session:
            session.run(query, results=results)
    def get_blast_radius_audit(self, batch_id: str) -> list[dict[str, Any]]:
        """Trace failed policies in a batch to impacted downstream data consumers."""
        query = """
        MATCH (vr:ValidationRun {batch_id: $batch_id, passed: false})-[:EVALUATED]->(dp:DataProduct)
        MATCH (vr)-[:CHECKED_POLICY]->(qp:QualityPolicy)
        MATCH (dp)-[:CONSUMED_BY]->(c:DataConsumer)
        RETURN dp.name AS supplier_feed,
               qp.dqv_dimension AS dimension,
               qp.name AS failed_policy,
               vr.violation_count AS violations,
               vr.compliance_rate AS compliance_rate,
               collect(DISTINCT c.name + ' [' + c.criticality + ']') AS impacted_consumers
        ORDER BY dp.name, qp.dqv_dimension
        """
        with self.driver.session() as session:
            return [dict(rec) for rec in session.run(query, batch_id=batch_id)]


if __name__ == "__main__":
    with DataSpaceGraphStore() as store:
        store.reset_graph()
        store.init_schema()
        n_policies = store.load_policies(Path("policies/supply_chain_policies.json"))
        n_products = store.register_suppliers(Path("policies/supplier_contracts.json"))
        plan = store.resolve_execution_plan()

        print(f"Seeded {n_policies} ODRL policies and {n_products} supplier data products.")
        print(f"Cypher Planner resolved {len(plan)} executable policy checks:\n")
        for check in plan:
            cols = [f"{m['local_column']} -> {m['canonical_feature'].split(':')[-1]}" for m in check["mappings"]]
            print(f"  [{check['product_id']}] {check['dimension']:<12} | {check['validator']:<28} | {', '.join(cols)}")