import argparse
import json
import re
from datetime import date, datetime, timedelta

from abassurance.shared.models.history import HISTORY_MODELS
from abassurance.systems.data_integration.mapping.identifiers import make_id

DEMO_AS_OF_DATE = "2025-11-01"
DEMO_OBSERVATION_DATE = "2025-07-01"
TEMPORAL_AS_OF_DATE = "2026-05-01"
TEMPORAL_EVALUATION_DATE = "2026-01-01"

SCENARIOS = (
    "positive",
    "negative",
    "censored",
    "prior_exit",
    "announced_exit",
    "competing_exit",
    "exit_at_cutoff",
    "exit_at_horizon_end",
    "missing_followup",
    "expired",
    "incomplete_features",
    "not_known_at_cutoff",
    "retracted_cancellation",
)


def synthetic_records(contracts_per_source: int = 52, *, temporal_cohorts: bool = False) -> dict[str, list[dict]]:
    if not 13 <= contracts_per_source <= 10000 or contracts_per_source % 13:
        raise ValueError("contracts-per-source must be a multiple of 13 between 13 and 10000")

    records = {entity: [] for entity in HISTORY_MODELS}

    for source in ("abassurance", "assureplus"):
        common = {"source_system": source, "revision": 1, "deleted": False, "is_synthetic": True}

        for index in range(contracts_per_source):
            reference = f"SYNTH-C{index:05d}"
            customer_ref = f"SYNTH-U{index // 2:05d}"
            contract_id = make_id(source, reference, "contracts")
            scenario = index % len(SCENARIOS)

            if index % 2 == 0:
                records["customers"].append(
                    {
                        **common,
                        "customer_id": make_id(source, customer_ref, "customers"),
                        "source_id": customer_ref,
                        "available_at": "2024-12-01T00:00:00Z",
                    }
                )

            records["contracts"].append(
                {
                    **common,
                    "contract_id": contract_id,
                    "source_reference": reference,
                    "customer_id": make_id(source, customer_ref, "customers"),
                    "start_date": "2025-01-01",
                    "coverage_end_date": "2025-06-30" if scenario == 9 else None,
                    "available_at": "2025-07-02T00:00:00Z" if scenario == 11 else "2024-12-15T00:00:00Z",
                }
            )

            for suffix, business_date, availability in (
                ("TIMELY", "2025-05-10", "2025-05-11T00:00:00Z"),
                ("LATE", "2025-05-20", "2025-07-03T00:00:00Z"),
                ("WITHDRAWN", "2025-05-15", "2025-05-16T00:00:00Z"),
            ):
                claim_ref = f"{reference}-{suffix}"

                claim = {
                    **common,
                    "claim_id": make_id(source, claim_ref, "claims"),
                    "source_id": claim_ref,
                    "contract_id": contract_id,
                    "incident_date": business_date,
                    "available_at": availability,
                }

                records["claims"].append(claim)

                if suffix == "WITHDRAWN":
                    records["claims"].append(
                        {**claim, "revision": 2, "deleted": True, "available_at": "2025-06-01T00:00:00Z"}
                    )

            payment_ref = f"{reference}-PAY"

            payment = {
                **common,
                "payment_id": make_id(source, payment_ref, "payments"),
                "source_id": payment_ref,
                "contract_id": contract_id,
                "payment_date": "2025-01-15",
                "amount": "100.00",
                "available_at": "2025-01-16T00:00:00Z",
            }

            records["payments"].extend(
                [
                    payment,
                    {**payment, "revision": 2, "amount": "999.00", "available_at": "2025-08-02T00:00:00Z"},
                    {
                        **payment,
                        "revision": 3,
                        "deleted": True,
                        "amount": "999.00",
                        "available_at": "2025-09-01T00:00:00Z",
                    },
                ]
            )

            if scenario != 8:
                records["followup"].append(
                    {
                        **common,
                        "contract_id": contract_id,
                        "complete_from": "2025-01-01",
                        "features_complete_from": "2025-04-01" if scenario == 10 else "2025-01-01",
                        "complete_until": "2025-07-20" if scenario == 2 else "2025-10-01",
                        "available_at": "2025-10-02T00:00:00Z",
                    }
                )

            exits = {
                0: ("2025-07-31", "2025-08-01T00:00:00Z", "voluntary"),
                3: ("2025-06-20", "2025-06-21T00:00:00Z", "non_payment"),
                4: ("2025-07-10", "2025-06-01T00:00:00Z", "voluntary"),
                5: ("2025-07-15", "2025-07-16T00:00:00Z", "other_exit"),
                6: ("2025-07-01", "2025-07-02T00:00:00Z", "non_renewal"),
                7: ("2025-09-29", "2025-09-30T00:00:00Z", "voluntary"),
                12: ("2025-08-01", "2025-08-02T00:00:00Z", "voluntary"),
            }

            if scenario in exits:
                effective, availability, kind = exits[scenario]
                cancellation_ref = f"{reference}-EXIT"

                cancellation = {
                    **common,
                    "cancellation_id": make_id(source, cancellation_ref, "cancellations"),
                    "source_id": cancellation_ref,
                    "contract_id": contract_id,
                    "cancellation_date": effective,
                    "available_at": availability,
                    "cancellation_type": kind,
                }

                records["cancellations"].append(cancellation)

                if scenario == 12:
                    records["cancellations"].append(
                        {**cancellation, "revision": 2, "deleted": True, "available_at": "2025-09-15T00:00:00Z"}
                    )

    if temporal_cohorts:
        offset = (date.fromisoformat(TEMPORAL_EVALUATION_DATE) - date.fromisoformat(DEMO_OBSERVATION_DATE)).days

        def shift(value):
            if isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}", value):
                if len(value) == 10:
                    return (date.fromisoformat(value) + timedelta(days=offset)).isoformat()

                return (datetime.fromisoformat(value) + timedelta(days=offset)).strftime("%Y-%m-%dT%H:%M:%SZ")

            return value.replace("SYNTH-", "SYNTH-EVAL-") if isinstance(value, str) else value

        later = {
            entity: [{key: shift(value) for key, value in row.items()} for row in rows]
            for entity, rows in records.items()
        }

        identities = {}
        for entity, identifier in (
            ("customers", "customer_id"),
            ("contracts", "contract_id"),
            ("claims", "claim_id"),
            ("payments", "payment_id"),
            ("cancellations", "cancellation_id"),
        ):
            for old, new in zip(records[entity], later[entity], strict=True):
                reference = new["source_reference" if entity == "contracts" else "source_id"]
                identities[old[identifier]] = make_id(new["source_system"], reference, entity)
        for rows in later.values():
            for row in rows:
                for field in ("customer_id", "contract_id", "claim_id", "payment_id", "cancellation_id"):
                    if field in row:
                        row[field] = identities[row[field]]

        for contract in list(records["contracts"]):
            if contract["coverage_end_date"] is None:
                records["contracts"].append(
                    {
                        **contract,
                        "revision": 2,
                        "coverage_end_date": "2025-10-01",
                        "available_at": "2025-10-02T00:00:00Z",
                    }
                )

        for entity in records:
            records[entity].extend(later[entity])

    return records


def demo_frames(spark, contracts_per_source: int = 52, *, temporal_cohorts: bool = False):
    from abassurance.systems.data_preparation.history import history_schema

    frames = {}
    for entity, rows in synthetic_records(contracts_per_source, temporal_cohorts=temporal_cohorts).items():
        schema = history_schema(entity)
        data = [tuple(row.get(field.name) for field in schema) for row in rows]
        frames[entity] = spark.createDataFrame(data, schema)
    return frames


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish ONLY synthetic business histories to an isolated HDFS root")

    parser.add_argument("--history-dir")
    parser.add_argument("--contracts-per-source", type=int, default=52)
    parser.add_argument(
        "--temporal-cohorts", action="store_true", help="Add an independent later cohort for evaluation"
    )
    parser.add_argument("--profile", choices=("temporal-edge-cases", "predictive"), default="temporal-edge-cases")
    parser.add_argument("--seed", type=int, default=42, help="Seed for the predictive profile only")

    args = parser.parse_args()

    if args.profile == "predictive" and not args.temporal_cohorts:
        parser.error("The predictive profile requires --temporal-cohorts")

    from abassurance.systems.data_preparation.history import publish_history
    from abassurance.systems.spark_pipeline.run import create_session

    spark = create_session(kafka=False)

    try:
        generation = None
        if args.profile == "predictive":
            from abassurance.systems.data_preparation.history import history_schema
            from abassurance.systems.data_preparation.predictive import generation_recipe, predictive_records

            frames = {}
            for entity, rows in predictive_records(args.contracts_per_source, seed=args.seed).items():
                schema = history_schema(entity)
                data = [tuple(row.get(field.name) for field in schema) for row in rows]
                frames[entity] = spark.createDataFrame(data, schema)
            generation = generation_recipe(args.contracts_per_source, args.seed)

            default_root = (
                f"hdfs://namenode:9000/abassurance-synthetic/"
                f"history-predictive-v1-seed{args.seed}-n{args.contracts_per_source}"
            )
        else:
            frames = demo_frames(spark, args.contracts_per_source, temporal_cohorts=args.temporal_cohorts)
            default_root = (
                "hdfs://namenode:9000/abassurance-synthetic/history-temporal"
                if args.temporal_cohorts
                else "hdfs://namenode:9000/abassurance-synthetic/history"
            )
        result = publish_history(
            spark,
            args.history_dir or default_root,
            frames,
            is_synthetic=True,
            as_of_date=TEMPORAL_AS_OF_DATE if args.temporal_cohorts else DEMO_AS_OF_DATE,
            generation=generation,
        )

        print(json.dumps(result, indent=2))

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

