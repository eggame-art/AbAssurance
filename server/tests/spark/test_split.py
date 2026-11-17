from dataclasses import replace
from datetime import date, timedelta

import pytest
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_preparation.config import PreparationConfig

pytestmark = pytest.mark.spark_integration


def config(**changes):
    values = {
        "source_root": "hdfs://namenode:9000/source",
        "output_root": "hdfs://namenode:9000/dataset",
        "history_root": "hdfs://namenode:9000/history",
        "observation_date": "2025-01-01",
        "observation_dates": ("2025-01-01", "2025-02-01", "2025-05-01", "2025-07-01"),
        "evaluation_start": "2025-07-01",
    }

    return PreparationConfig(**(values | changes))


def observations(spark, rows):
    data = [
        (
            contract,
            customer,
            source,
            date.fromisoformat(day),
            date.fromisoformat(day) + timedelta(days=90),
            target is not None,
            target,
        )
        for contract, customer, source, day, target in rows
    ]

    schema = (
        "contract_id string, customer_id string, source_system string, "
        "observation_date date, horizon_end date, training_eligible boolean, is_cancelled int"
    )

    return spark.createDataFrame(data, schema)


@pytest.fixture
def split_rows(spark):
    return observations(
        spark,
        [
            ("A", "one", "abassurance", "2025-01-01", 0),
            ("A", "one", "abassurance", "2025-02-01", 0),
            ("B", "shared", "abassurance", "2025-01-01", 1),
            ("C", "old-customer", "abassurance", "2025-01-01", 1),
            ("D", "overlap", "abassurance", "2025-05-01", 0),
            ("T", "train-positive", "abassurance", "2025-01-01", 1),
            ("F", "unknown-eval", "abassurance", "2025-01-01", 0),
            ("E", "eval-positive", "abassurance", "2025-07-01", 1),
            ("Q", "shared", "abassurance", "2025-07-01", 0),
            ("C", "new-customer", "abassurance", "2025-07-01", 1),
            ("U", "unknown-eval", "abassurance", "2025-07-01", None),
        ],
    )


def test_purged_chronological_split_reserves_customers_and_contracts(split_rows):
    from abassurance.systems.data_preparation.split import assign_split, split_summary, validate_split

    settings = config()
    assigned = assign_split(split_rows, settings).cache()

    try:
        rows = {(row.contract_id, str(row.observation_date)): row for row in assigned.collect()}

        assert rows["A", "2025-01-01"].dataset_split == "training"
        assert rows["A", "2025-02-01"].dataset_split == "training"

        assert rows["B", "2025-01-01"].split_reason == "customer_reserved_for_evaluation"
        assert rows["C", "2025-01-01"].split_reason == "contract_reserved_for_evaluation"
        assert rows["D", "2025-05-01"].split_reason == "purged_label_horizon"
        assert rows["F", "2025-01-01"].split_reason == "customer_reserved_for_evaluation"
        assert rows["U", "2025-07-01"].dataset_split == "excluded"

        assert validate_split(assigned, settings)["customer_disjoint"]

        counts = split_summary(assigned, settings)

        assert counts["training"]["records"] == 3
        assert counts["training"]["class_counts"] == {"0": 2, "1": 1}
        assert counts["training"]["customers_per_class"] == {"0": 1, "1": 1}
        assert counts["training"]["last_label_end"] == "2025-05-02"

        assert counts["evaluation"]["records"] == 3
        assert counts["evaluation"]["class_counts"] == {"0": 1, "1": 2}

        assert assign_split(split_rows.repartition(3), settings).exceptAll(assigned).count() == 0

    finally:
        assigned.unpersist()


def test_embargo_exact_boundary_and_source_identity(spark):
    from abassurance.systems.data_preparation.split import assign_split, validate_split

    data = observations(
        spark,
        [("AB-A", "same-local", "abassurance", "2025-04-02", 0), ("AP-B", "same-local", "assureplus", "2025-07-01", 1)],
    )

    settings = config(observation_dates=("2025-04-02", "2025-07-01"))
    assigned = assign_split(data, settings)

    assert assigned.filter("dataset_split = 'training'").count() == 1
    assert validate_split(assigned, settings)["temporal_non_overlap"]

    with pytest.raises(ConfigurationError, match="overlap"):
        replace(settings, embargo_days=1)


def test_invalid_assignments_duplicates_and_empty_independent_cohort_are_refused(spark, split_rows):
    from abassurance.systems.data_preparation.split import assign_split, validate_split
    from pyspark.sql import functions as F

    settings = config()
    assigned = assign_split(split_rows, settings)

    with pytest.raises(ConfigurationError, match="Duplicate"):
        validate_split(assigned.unionByName(assigned.limit(1)), settings)

    leaking = assigned.withColumn(
        "dataset_split", F.when(F.col("contract_id") == "B", "training").otherwise(F.col("dataset_split"))
    )

    with pytest.raises(ConfigurationError, match="Customer leakage"):
        validate_split(leaking, settings)

    empty = observations(
        spark, [("A", "same", "abassurance", "2025-01-01", 0), ("B", "same", "abassurance", "2025-07-01", 1)]
    )

    with pytest.raises(ConfigurationError, match="Impossible independent split"):
        validate_split(assign_split(empty, settings), settings)


def test_readiness_counts_independent_customers_and_never_promotes_small_or_single_cohorts(split_rows):
    from abassurance.systems.data_preparation.split import assign_split, readiness, split_summary

    settings = config()
    summary = split_summary(assign_split(split_rows, settings), settings)
    flags = readiness(settings, summary, historical=True, synthetic=False)

    assert not flags["training_ready"]
    assert flags["evaluation_sample_plan"]["required_customers_per_class"] == 185
    assert not flags["evaluation_sample_plan"]["sufficient"]

    summary["evaluation"]["customers_per_class"] = {"0": 185, "1": 185}

    assert readiness(settings, summary, historical=True, synthetic=False)["training_ready"]

    synthetic = readiness(settings, summary, historical=True, synthetic=True)

    assert not synthetic["training_ready"]
    assert synthetic["synthetic_training_ready"]

    single = replace(settings, observation_dates=(), evaluation_start=None)

    assert not readiness(single, summary, historical=True, synthetic=False)["training_ready"]
