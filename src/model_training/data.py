import hashlib
import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dataset_preparation.config import FEATURE_COLUMNS, VERSION
from model_training.config import TrainingConfig
from shared.errors import ConfigurationError
from spark_pipeline.hdfs import HdfsState

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def authorize(report: dict, config: TrainingConfig) -> None:
    synthetic = report.get("is_synthetic")

    if type(synthetic) is not bool:
        raise ConfigurationError("Missing explicit dataset provenance")

    if config.mode == "production":
        if synthetic or report.get("training_ready") is not True or report.get("training_blockers") != []:
            raise ConfigurationError("Production training refused: dataset is synthetic or not training_ready")
    elif not synthetic or report.get("recipe", {}).get("purpose") != "synthetic-training":
        raise ConfigurationError("Experimental training requires an explicitly synthetic historical dataset")

    if not report.get("recipe", {}).get("evaluation_start"):
        raise ConfigurationError("Independent chronological evaluation is required")

    if report.get("target", {}).get("column") != "is_cancelled" or report["target"].get("available") is not True:
        raise ConfigurationError("Reliable binary labels are required")

    checks = (
        "temporal_non_overlap",
        "customer_disjoint",
        "contract_disjoint",
        "observation_key_unique",
        "persisted_parquet_verified",
    )

    if any(report.get("validation", {}).get(key) is not True for key in checks):
        raise ConfigurationError("Dataset temporal/identity validation is required")


@dataclass
class LoadedDataset:
    report: dict
    training: "DataFrame"
    evaluation: "DataFrame"

    def close(self) -> None:
        self.training.unpersist()
        self.evaluation.unpersist()


def load_dataset(spark: "SparkSession", config: TrainingConfig) -> LoadedDataset:
    from pyspark.sql.types import NumericType

    from dataset_preparation.history import require_namespace
    from dataset_preparation.run import _verify_output

    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ConfigurationError("Training requires spark.sql.session.timeZone=UTC")

    state = HdfsState(spark, config.dataset_root)
    dataset_id = config.dataset_id

    if dataset_id is None:
        dataset_id = state.read_json("CURRENT.json").get("dataset_id")

    if not isinstance(dataset_id, str) or not re.fullmatch(r"[0-9a-f]{64}", dataset_id):
        raise ConfigurationError("Invalid committed dataset identity")

    folder = f"runs/{dataset_id}"

    report = state.read_json(f"{folder}/report.json")
    recipe = report.get("recipe", {})

    if (
        report.get("dataset_id") != dataset_id
        or digest(recipe) != dataset_id
        or recipe.get("version") != VERSION
        or report.get("format") != "parquet"
    ):
        raise ConfigurationError("Invalid dataset recipe, identity or version")

    for name in ("data", "excluded", "training", "evaluation"):
        if report.get(f"{name}_path") != state.path(f"{folder}/{name}"):
            raise ConfigurationError("Dataset paths must reference the pinned committed run")

    if report.get("feature_columns") != FEATURE_COLUMNS:
        raise ConfigurationError("Unsupported US09 feature schema")

    authorize(report, config)

    if (
        recipe.get("source_manifest", {}).get("is_synthetic") is not report["is_synthetic"]
        or report["target"].get("horizon_days") != recipe.get("horizon_days")
        or recipe.get("purpose") != ("synthetic-training" if report["is_synthetic"] else "training")
    ):
        raise ConfigurationError("Dataset provenance or target horizon does not match its recipe")

    require_namespace(state.root, report["is_synthetic"])
    require_namespace(config.output_root, report["is_synthetic"])

    _verify_output(spark, state, folder, report)

    loaded = LoadedDataset(
        report,
        spark.read.parquet(report["training_path"]).cache(),
        spark.read.parquet(report["evaluation_path"]).cache(),
    )

    try:
        for name in ("training", "evaluation"):
            frame = getattr(loaded, name)

            if any(not isinstance(frame.schema[column].dataType, NumericType) for column in config.features):
                raise ConfigurationError("US09 model features must have numeric types")

            classes = {int(row.is_cancelled): row["count"] for row in frame.groupBy("is_cancelled").count().collect()}

            if set(classes) != {0, 1}:
                raise ConfigurationError(f"Both binary classes are required in {name}, even for experiments")

        return loaded
    except Exception:
        loaded.close()
        raise
