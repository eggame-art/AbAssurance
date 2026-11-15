import os
from dataclasses import replace
from typing import Any
from uuid import uuid4

import pytest
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_preparation.config import PreparationConfig
from abassurance.systems.data_preparation.demo import TEMPORAL_AS_OF_DATE, demo_frames
from abassurance.systems.data_preparation.history import publish_history
from abassurance.systems.data_preparation.run import prepare_dataset
from abassurance.systems.model_training.config import TrainingConfig
from abassurance.systems.model_training.data import digest, load_dataset
from abassurance.systems.model_training.evaluation import prediction_values
from abassurance.systems.model_training.persistence import load_model, publish_model
from abassurance.systems.model_training.run import train
from abassurance.systems.model_training.training import model_input
from abassurance.systems.spark_pipeline.hdfs import HdfsState
from pyspark.ml import PipelineModel
from pyspark.ml.param import Param
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

pytestmark = pytest.mark.hdfs_integration


@pytest.fixture(scope="module")
def model_dataset(spark):
    root = os.environ.get("HDFS_TEST_ROOT", "hdfs://namenode:9000/abassurance-tests")
    base = HdfsState(spark, f"{root}/{uuid4().hex}/synthetic")

    history_root = base.path("history")

    preparation = PreparationConfig(
        source_root="hdfs://namenode:9000/abassurance",
        output_root=base.path("dataset"),
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

        report = prepare_dataset(spark, preparation)

        config = TrainingConfig(
            preparation.output_root,
            base.path("models"),
            dataset_id=report["dataset_id"],
            mode="experimental",
            max_iter=20,
        )

        yield base, config, report
    finally:
        base.fs.delete(base.java_path(""), True)


def test_hdfs_train_lineage_reload_and_failed_publication(spark, model_dataset, monkeypatch):
    base, config, dataset_report = model_dataset

    assert not dataset_report["training_ready"]
    assert not dataset_report["synthetic_training_ready"]

    with pytest.raises(ConfigurationError, match="Production training refused"):
        train(spark, replace(config, mode="production"))

    output = HdfsState(spark, config.output_root)

    assert not output.exists("CURRENT.json")

    report = train(spark, config)

    assert report["status"] == "experimental"
    assert not report["business_validated"]
    assert report["dataset"]["report"] == dataset_report
    assert report["dataset"]["report_sha256"] == digest(dataset_report)
    assert report["metrics"]["records"] == dataset_report["counts"]["splits"]["evaluation"]["records"]
    assert report["runtime"]["spark"] == spark.version
    assert report["reload_verified"]

    with pytest.raises(ConfigurationError, match="Experimental model"):
        load_model(spark, config.output_root)

    model, restored_report = load_model(spark, config.output_root, report["model_id"], allow_experimental=True)

    assert restored_report == report

    evaluation = model_input(spark.read.parquet(dataset_report["evaluation_path"]), config.features)

    assert prediction_values(model.transform(evaluation)).count() == report["metrics"]["records"]

    original_write = HdfsState.write_json_atomic

    def fail_metadata(state, relative, value):
        if state.root == output.root and relative.startswith("attempts/") and relative.endswith("report.json"):
            raise OSError("Injected metadata write failure")

        return original_write(state, relative, value)

    with monkeypatch.context() as patch:
        patch.setattr(HdfsState, "write_json_atomic", fail_metadata)

        with pytest.raises(OSError, match="Injected metadata"):
            publish_model(spark, output.root, model, {**report, "model_id": uuid4().hex}, evaluation)

    assert output.read_json("CURRENT.json") == report
    assert not output.exists(".lock")

    original_load = PipelineModel.load

    def broken_reload(path):
        restored = original_load(path)
        transform = restored.transform

        def broken_transform(dataset: DataFrame, params: dict[Param[Any], Any] | None = None) -> DataFrame:
            return transform(dataset, params).withColumn("prediction", F.lit(9.0))

        restored.transform = broken_transform

        return restored

    with monkeypatch.context() as patch:
        patch.setattr(PipelineModel, "load", broken_reload)

        with pytest.raises(ConfigurationError, match="predictions differ"):
            publish_model(spark, output.root, model, {**report, "model_id": uuid4().hex}, evaluation)

    assert output.read_json("CURRENT.json") == report


def test_hdfs_loader_pins_run_and_rejects_changed_paths_readiness_and_subsets(spark, model_dataset):
    _, config, report = model_dataset

    state = HdfsState(spark, config.dataset_root)
    folder = f"runs/{report['dataset_id']}"

    state.write_json_atomic("CURRENT.json", {"dataset_id": "invalid"})

    try:
        loaded = load_dataset(spark, config)

        try:
            assert loaded.training.count() == report["counts"]["splits"]["training"]["records"]
            assert loaded.evaluation.count() == report["counts"]["splits"]["evaluation"]["records"]
        finally:
            loaded.close()

        with pytest.raises(ConfigurationError, match="identity"):
            load_dataset(spark, replace(config, dataset_id=None))
    finally:
        state.write_json_atomic("CURRENT.json", report)

    for changes, message in (
        ({"training_path": report["evaluation_path"]}, "pinned committed"),
        ({"synthetic_training_ready": True}, "readiness"),
        ({"feature_columns": ["customer_id"]}, "feature schema"),
    ):
        state.write_json_atomic(f"{folder}/report.json", {**report, **changes})

        try:
            with pytest.raises(ConfigurationError, match=message):
                load_dataset(spark, config)
        finally:
            state.write_json_atomic(f"{folder}/report.json", report)

    original = f"{folder}/training"
    backup = f"{folder}/training-backup"

    assert state.fs.rename(state.java_path(original), state.java_path(backup))

    try:
        spark.read.parquet(report["evaluation_path"]).write.parquet(state.path(original))

        with pytest.raises(ConfigurationError, match="training subset"):
            load_dataset(spark, config)
    finally:
        state.fs.delete(state.java_path(original), True)

        assert state.fs.rename(state.java_path(backup), state.java_path(original))
