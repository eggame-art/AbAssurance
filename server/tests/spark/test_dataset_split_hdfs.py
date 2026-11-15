import os
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_preparation.config import PreparationConfig
from abassurance.systems.data_preparation.demo import TEMPORAL_AS_OF_DATE, demo_frames
from abassurance.systems.data_preparation.run import prepare_dataset
from abassurance.systems.spark_pipeline.hdfs import HdfsState

pytestmark = pytest.mark.hdfs_integration


@pytest.fixture
def split_history(spark):
    from abassurance.systems.data_preparation.history import publish_history

    root = os.environ.get("HDFS_TEST_ROOT", "hdfs://namenode:9000/abassurance-tests")
    base = HdfsState(spark, f"{root}/{uuid4().hex}")
    history_root = base.path("synthetic/history")

    config = PreparationConfig(
        source_root="hdfs://namenode:9000/abassurance",
        output_root=base.path("synthetic/dataset"),
        history_root=history_root,
        purpose="synthetic-training",
        observation_date="2025-07-01",
        observation_dates=("2025-07-01", "2025-08-01", "2026-01-01"),
        evaluation_start="2026-01-01",
    )

    try:
        publish_history(
            spark,
            history_root,
            demo_frames(spark, 13, temporal_cohorts=True),
            is_synthetic=True,
            as_of_date=TEMPORAL_AS_OF_DATE,
        )

        yield config

    finally:
        base.fs.delete(base.java_path(""), True)


def test_hdfs_multiple_cohorts_exact_values_disjoint_subsets_and_replay(spark, split_history, monkeypatch):
    config = split_history
    report = prepare_dataset(spark, config)

    assert report["validation"] == {
        "temporal_non_overlap": True,
        "customer_disjoint": True,
        "contract_disjoint": True,
        "observation_key_unique": True,
        "persisted_parquet_verified": True,
    }

    assert report["counts"]["records"] == 74
    assert report["counts"]["splits"]["training"]["records"] == 12
    assert report["counts"]["splits"]["training"]["class_counts"] == {"0": 6, "1": 6}
    assert report["counts"]["splits"]["evaluation"]["records"] == 10
    assert report["counts"]["splits"]["evaluation"]["class_counts"] == {"0": 6, "1": 4}
    assert report["counts"]["unknown_targets"] == 48

    assert not report["training_ready"]
    assert not report["synthetic_training_ready"]
    assert not report["evaluation_sample_plan"]["sufficient"]

    data = spark.read.parquet(report["data_path"])
    rows = {(row.contract_id, str(row.observation_date)): row for row in data.collect()}

    before = rows["AB-SYNTH-C00007", "2025-07-01"]
    later = rows["AB-SYNTH-C00007", "2025-08-01"]

    assert before.is_cancelled == 0
    assert later.is_cancelled == 1

    assert before.claim_count == 1
    assert later.claim_count == 2

    assert before.payment_amount_known_total == later.payment_amount_known_total == Decimal("100.00")
    assert later.dataset_split == "training"

    assert rows["AB-SYNTH-EVAL-C00007", "2026-01-01"].dataset_split == "evaluation"
    assert rows["AB-SYNTH-EVAL-C00007", "2025-08-01"].is_cancelled is None

    assert all(item["label_as_of"] == "2026-01-01" for item in report["counts"]["cohorts"][:2])
    assert report["counts"]["cohorts"][2]["label_as_of"] == TEMPORAL_AS_OF_DATE

    training = spark.read.parquet(report["training_path"])
    evaluation = spark.read.parquet(report["evaluation_path"])

    assert training.filter("NOT training_eligible OR is_cancelled IS NULL").count() == 0
    assert evaluation.filter("NOT training_eligible OR is_cancelled IS NULL").count() == 0

    overlapping_customers = training.select("customer_id", "source_system").join(
        evaluation.select("customer_id", "source_system"), ["customer_id", "source_system"]
    )

    assert overlapping_customers.count() == 0

    output = HdfsState(spark, config.output_root)

    assert output.read_json("CURRENT.json") == report
    assert prepare_dataset(spark, replace(config, observation_dates=tuple(reversed(config.dates)))) == report

    from abassurance.systems.data_preparation.run import _verify_output
    from pyspark.sql import functions as F
    from pyspark.sql.readwriter import DataFrameWriter

    original = DataFrameWriter.parquet
    copied = "tamper-test"

    output.mkdir(copied)

    for name in ("data", "training", "excluded"):
        source_path = report[f"{name}_path"] if name != "data" else report["data_path"]

        spark.read.parquet(source_path).write.parquet(output.path(f"{copied}/{name}"))

    modified_evaluation = evaluation.withColumn("claim_count", F.col("claim_count") + 1)

    original(modified_evaluation.write, output.path(f"{copied}/evaluation"))

    with pytest.raises(ConfigurationError, match="evaluation subset"):
        _verify_output(spark, output, copied, report)

    assert output.read_json("CURRENT.json") == report


def test_hdfs_evaluation_write_failure_does_not_publish(spark, split_history, monkeypatch):
    from pyspark.sql.readwriter import DataFrameWriter

    config = replace(split_history, observation_dates=("2025-07-01", "2026-01-01"))

    original = DataFrameWriter.parquet

    def fail(writer, path, *args, **kwargs):
        if path.endswith("/evaluation"):
            raise OSError("Interrupted evaluation write")

        return original(writer, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, "parquet", fail)

        with pytest.raises(OSError, match="Interrupted evaluation"):
            prepare_dataset(spark, config)

    output = HdfsState(spark, config.output_root)

    assert not output.exists("CURRENT.json")
    assert not output.exists(".lock")

    recovered = prepare_dataset(spark, config)

    assert output.read_json("CURRENT.json") == recovered
    assert recovered["counts"]["splits"]["training"]["records"] == 10
    assert recovered["counts"]["splits"]["evaluation"]["records"] == 10
