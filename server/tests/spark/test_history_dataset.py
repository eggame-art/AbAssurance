import os
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_preparation.config import PreparationConfig
from abassurance.systems.data_preparation.demo import DEMO_AS_OF_DATE, demo_frames, synthetic_records
from abassurance.systems.data_preparation.run import prepare_dataset
from abassurance.systems.spark_pipeline.hdfs import HdfsState


def historical_config(**changes: object) -> PreparationConfig:
    values = PreparationConfig(
        source_root="hdfs://namenode:9000/abassurance",
        output_root="hdfs://namenode:9000/abassurance-synthetic/datasets/churn",
        history_root="hdfs://namenode:9000/abassurance-synthetic/history",
        observation_date="2025-07-01",
        purpose="synthetic-training",
    )

    return replace(values, **changes)


def manifest():
    return {"is_synthetic": True, "as_of_date": DEMO_AS_OF_DATE}


def build(spark, frames=None, **changes):
    from abassurance.systems.data_preparation.temporal import HistoricalDataset

    return HistoricalDataset(
        frames if frames is not None else demo_frames(spark, 13), historical_config(**changes), manifest()
    )


@pytest.mark.spark_integration
def test_real_temporal_labels_censoring_and_point_in_time_versions(spark):
    from abassurance.systems.data_preparation.history import validate_history

    frames = demo_frames(spark, 13)

    assert validate_history(frames, is_synthetic=True, as_of_date=DEMO_AS_OF_DATE)["contracts"] == 26

    prepared = build(spark, frames)

    try:
        rows = {row.contract_id: row for row in prepared.dataset.collect()}

        assert len(rows) == 18

        for prefix in ("AB", "AP"):

            def row(index, prefix=prefix):
                return rows[f"{prefix}-SYNTH-C{index:05d}"]

            assert row(0).is_cancelled == row(6).is_cancelled == 1
            assert row(1).is_cancelled == row(7).is_cancelled == row(12).is_cancelled == 0

            assert row(2).is_cancelled is None
            assert row(2).label_status == "insufficient_followup"

            assert row(5).is_cancelled is None
            assert row(5).label_status == "competing_exit"

            assert row(8).is_cancelled is None
            assert row(8).label_status == "insufficient_history_at_observation"

            assert row(10).is_cancelled == 0
            assert not row(10).training_eligible
            assert row(10).eligibility_reason == "feature_history_incomplete"

            for index in (3, 4, 9, 11):
                assert f"{prefix}-SYNTH-C{index:05d}" not in rows

        assert all(row.claim_count == row.payment_count == 1 for row in rows.values())
        assert all(row.payment_amount_known_total == Decimal("100.00") for row in rows.values())
        assert all(row.is_synthetic for row in rows.values())

        report = prepared.report()

        assert report["class_counts"] == {"0": 6, "1": 4}
        assert report["unknown_targets"] == 6
        assert report["training_eligible_records"] == 10
        assert report["cohort_excluded_records"] == 6

    finally:
        prepared.close()


@pytest.mark.spark_integration
def test_configurable_horizon_and_termination_definition(spark):
    prepared = build(spark, horizon_days=30, cancellation_types=("voluntary",))

    try:
        rows = {row.contract_id: row for row in prepared.dataset.collect()}

        assert rows["AB-SYNTH-C00000"].is_cancelled == 0
        assert rows["AB-SYNTH-C00006"].is_cancelled is None
        assert rows["AB-SYNTH-C00006"].label_status == "competing_exit"

    finally:
        prepared.close()


@pytest.mark.spark_integration
def test_cutoff_availability_latest_version_and_deleted_values(spark):
    from abassurance.systems.data_preparation.history import history_schema
    from abassurance.systems.data_preparation.temporal import latest_before

    records = synthetic_records(13)["payments"]
    original = records[0]

    rows = [
        original,
        {**original, "revision": 2, "amount": "200.00", "available_at": "2025-06-30T23:59:59Z"},
        {**original, "revision": 3, "amount": "900.00", "available_at": "2025-07-01T00:00:00Z"},
    ]

    frame = spark.createDataFrame(rows, history_schema("payments"))
    selected = latest_before(frame, "payments", "2025-07-01").collect()

    assert len(selected) == 1
    assert selected[0].amount == "200.00"
    latest = latest_before(frame, "payments", "2025-08-01").first()

    assert latest is not None
    assert latest.amount == "900.00"

    deleted = spark.createDataFrame(
        [*rows, {**original, "revision": 4, "deleted": True, "available_at": "2025-08-02T00:00:00Z"}],
        history_schema("payments"),
    )

    assert latest_before(deleted, "payments", "2025-09-01").count() == 0


@pytest.mark.spark_integration
def test_partial_followup_confirms_a_positive_but_never_manufactures_a_negative(spark):
    from abassurance.systems.data_preparation.temporal import HistoricalDataset
    from pyspark.sql import functions as F

    frames = demo_frames(spark, 13)

    frames["followup"] = (
        frames["followup"]
        .withColumn("complete_until", F.lit("2025-08-10"))
        .withColumn("available_at", F.lit("2025-08-11T00:00:00Z"))
    )

    prepared = HistoricalDataset(frames, historical_config(), {"is_synthetic": True, "as_of_date": "2025-08-15"})

    try:
        rows = {row.contract_id: row for row in prepared.dataset.collect()}

        assert rows["AB-SYNTH-C00000"].is_cancelled == 1
        assert rows["AB-SYNTH-C00001"].is_cancelled is None
        assert rows["AB-SYNTH-C00001"].label_status == "insufficient_followup"

    finally:
        prepared.close()


@pytest.mark.spark_integration
@pytest.mark.parametrize(
    "problem", ["duplicate", "missing_availability", "mixed_provenance", "cross_source", "revision_order"]
)
def test_unreliable_history_fails_closed(spark, problem):
    from abassurance.systems.data_preparation.history import history_schema, validate_history

    frames = demo_frames(spark, 13)
    records = synthetic_records(13)["customers"]

    if problem == "duplicate":
        records.append({**records[0], "available_at": "2025-01-01T00:00:00Z"})

    elif problem == "missing_availability":
        records[0]["available_at"] = None

    elif problem == "mixed_provenance":
        records[0]["is_synthetic"] = False

    elif problem == "cross_source":
        records[0]["source_system"] = "assureplus"

    else:
        records.append({**records[0], "revision": 2, "available_at": "2024-11-01T00:00:00Z"})

    frames["customers"] = spark.createDataFrame(records, history_schema("customers"))

    with pytest.raises(ConfigurationError):
        validate_history(frames, is_synthetic=True, as_of_date=DEMO_AS_OF_DATE)


@pytest.fixture
def isolated_history(spark):
    root = os.environ.get("HDFS_TEST_ROOT", "hdfs://namenode:9000/abassurance-tests")
    base = HdfsState(spark, f"{root}/{uuid4().hex}")

    config = historical_config(history_root=base.path("synthetic/history"), output_root=base.path("synthetic/dataset"))

    try:
        yield base, config

    finally:
        base.fs.delete(base.java_path(""), True)


@pytest.mark.hdfs_integration
def test_hdfs_history_cumulative_replay_labelled_subset_and_provenance(spark, isolated_history):
    from abassurance.systems.data_preparation.history import load_history, publish_history
    from pyspark.sql import functions as F

    base, config = isolated_history
    frames = demo_frames(spark, 13)

    first = publish_history(spark, config.history_root, frames, is_synthetic=True, as_of_date=DEMO_AS_OF_DATE)

    assert publish_history(spark, config.history_root, frames, is_synthetic=True, as_of_date=DEMO_AS_OF_DATE) == first

    report = prepare_dataset(spark, config)

    assert not report["training_ready"]
    assert not report["synthetic_training_ready"]
    assert report["counts"]["splits"]["evaluation"]["records"] == 0
    assert report["counts"]["records"] == 18
    assert report["counts"]["training_eligible_records"] == 10

    training = spark.read.parquet(report["training_path"])

    assert training.count() == 10
    assert training.filter("NOT training_eligible OR is_cancelled IS NULL OR NOT is_synthetic").count() == 0
    assert prepare_dataset(spark, config) == report

    with pytest.raises(ConfigurationError, match="synthetic-training"):
        prepare_dataset(spark, replace(config, purpose="training"))

    with pytest.raises(ConfigurationError, match="separate synthetic"):
        prepare_dataset(spark, replace(config, output_root=base.path("business/dataset")))

    bad = {**frames, "payments": frames["payments"].withColumn("amount", F.lit("777.00"))}

    with pytest.raises(ConfigurationError, match="Conflicting"):
        publish_history(spark, config.history_root, bad, is_synthetic=True, as_of_date=DEMO_AS_OF_DATE)

    assert HdfsState(spark, config.history_root).read_json("CURRENT.json") == first

    update = {entity: frame.limit(0) for entity, frame in frames.items()}

    update["payments"] = (
        frames["payments"]
        .filter("revision = 3")
        .withColumn("revision", F.lit(4).cast("long"))
        .withColumn("available_at", F.lit("2025-11-02T00:00:00Z"))
    )

    second = publish_history(spark, config.history_root, update, is_synthetic=True, as_of_date="2025-12-01")

    assert second["previous_snapshot"] == first["snapshot"]
    assert second["counts"]["payments"] == first["counts"]["payments"] + 26

    loaded, versions = load_history(spark, HdfsState(spark, config.history_root))

    assert loaded == second
    assert versions["payments"].filter("revision = 1").count() == 26
    assert HdfsState(spark, config.history_root).exists(f"snapshots/{first['snapshot']}/report.json")


@pytest.mark.hdfs_integration
def test_business_history_single_date_does_not_claim_independent_evaluation(spark, isolated_history):
    from abassurance.systems.data_preparation.history import publish_history
    from pyspark.sql import functions as F

    base, synthetic_config = isolated_history

    config = replace(
        synthetic_config,
        history_root=base.path("business/history"),
        output_root=base.path("business/dataset"),
        purpose="training",
    )

    frames = {
        entity: frame.withColumn("is_synthetic", F.lit(False)) for entity, frame in demo_frames(spark, 13).items()
    }

    publish_history(spark, config.history_root, frames, is_synthetic=False, as_of_date=DEMO_AS_OF_DATE)

    report = prepare_dataset(spark, config)

    assert not report["training_ready"]
    assert not report["synthetic_training_ready"]
    assert any("Single observation" in reason for reason in report["training_blockers"])
    assert report["counts"]["class_counts"] == {"0": 6, "1": 4}
