import argparse
import hashlib
import json
import logging
import os
import platform
from dataclasses import asdict
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from model_training.config import ALGORITHMS, VERSION, TrainingConfig
from shared.errors import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


LOGGER = logging.getLogger(__name__)


def runtime_metadata(spark: "SparkSession") -> dict:
    source = Path(__file__).resolve().parents[1]

    hashes = {
        path.relative_to(source).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(source.rglob("*.py"))
    }

    jvm = cast(Any, spark.sparkContext._jvm)

    if jvm is None:
        raise ConfigurationError("Spark JVM is unavailable")

    return {
        "python": platform.python_version(),
        "spark": spark.version,
        "numpy": version("numpy"),
        "java": jvm.java.lang.System.getProperty("java.version"),
        "source_sha256": hashes,
        "source_tree_sha256": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest(),
        "spark_master": spark.sparkContext.master,
        "spark_settings": {
            key: spark.conf.get(key) for key in ("spark.sql.session.timeZone", "spark.sql.shuffle.partitions")
        },
        "reproducibility": (
            "Same immutable dataset, source hashes, config and runtime; numerical results may vary by platform"
        ),
    }


def train(spark: "SparkSession", config: TrainingConfig) -> dict:
    from model_training.data import digest, load_dataset
    from model_training.diagnostics import feature_diagnostics, probability_diagnostics
    from model_training.evaluation import evaluate
    from model_training.persistence import publish_model
    from model_training.training import feature_influence, fit_model, model_input

    dataset = load_dataset(spark, config)
    predictions = None

    try:
        model, preprocessing, weights = fit_model(dataset.training, config)

        evaluation = model_input(dataset.evaluation, config.features)
        predictions = model.transform(evaluation).cache()

        metrics = evaluate(predictions)

        diagnostics = {
            "training_features": feature_diagnostics(dataset.training, config.features),
            "evaluation_features": feature_diagnostics(dataset.evaluation, config.features),
            "evaluation_probabilities": probability_diagnostics(predictions),
            "scope": "Descriptive only; never used to fit, select a model or tune the threshold",
        }

        metadata = {
            "version": VERSION,
            "model_id": uuid4().hex,
            "created_at": datetime.now(UTC).isoformat(),
            "status": "experimental" if config.mode == "experimental" else "data_validated",
            "business_validated": False,
            "validation_scope": "Pipeline and dataset checks only; business acceptance requires separate validation",
            "experimental_warning": (
                "Synthetic technical experiment: metrics do not measure real business performance"
                if config.mode == "experimental"
                else None
            ),
            "algorithm": config.algorithm,
            "algorithm_parameters": {param.name: value for param, value in model.stages[-1].extractParamMap().items()},
            "config": {**asdict(config), "features": list(config.features)},
            "feature_columns": list(config.features),
            "preprocessing": preprocessing,
            "class_weights": weights,
            "diagnostics": diagnostics,
            "metrics": metrics,
            "class_distribution": dataset.report["counts"]["splits"],
            "feature_influence": feature_influence(model, config),
            "dataset": {
                "root": config.dataset_root,
                "dataset_id": dataset.report["dataset_id"],
                "version": dataset.report["recipe"]["version"],
                "report_sha256": digest(dataset.report),
                "report": dataset.report,
            },
            "runtime": runtime_metadata(spark),
        }

        return publish_model(spark, config.output_root, model, metadata, evaluation)
    finally:
        if predictions is not None:
            predictions.unpersist()

        dataset.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="US10 churn training on immutable US09 training/evaluation datasets")

    parser.add_argument(
        "--dataset-dir", default=os.environ.get("DATASET_OUTPUT_DIR", "hdfs://namenode:9000/abassurance-datasets/churn")
    )

    parser.add_argument(
        "--output-dir", default=os.environ.get("MODEL_OUTPUT_DIR", "hdfs://namenode:9000/abassurance-models/churn")
    )

    parser.add_argument("--dataset-id", help="Pin an immutable dataset; defaults to CURRENT at startup")
    parser.add_argument("--mode", choices=("production", "experimental"), default="production")
    parser.add_argument("--algorithm", choices=ALGORITHMS, default="logistic-regression")
    parser.add_argument("--features", nargs="+", default=None, help="Ordered subset of the US09 allowlist")
    parser.add_argument("--imputation", choices=("median", "mean"), default="median")
    parser.add_argument("--no-standardize", action="store_true")
    parser.add_argument("--class-weight", choices=("none", "balanced"), default="balanced")
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--max-iter", type=int, default=100)
    parser.add_argument("--reg-param", type=float, default=0.1)
    parser.add_argument("--elastic-net-param", type=float, default=0.0)
    parser.add_argument("--tolerance", type=float, default=1e-6)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-trees", type=int, default=100)
    parser.add_argument("--max-depth", type=int, default=5)
    parser.add_argument("--master", default="local[2]")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    spark = None

    try:
        from spark_pipeline.run import create_session

        options = vars(args).copy()
        options["dataset_root"] = options.pop("dataset_dir")
        options["output_root"] = options.pop("output_dir")
        options["standardize"] = not options.pop("no_standardize")

        master = options.pop("master")

        if options["features"] is None:
            del options["features"]
        else:
            options["features"] = tuple(options["features"])

        config = TrainingConfig(**options)

        spark = create_session(kafka=False, master=master)
        spark.sparkContext.setLogLevel("WARN")

        print(json.dumps(train(spark, config), ensure_ascii=False, indent=2, allow_nan=False))

        return 0
    except Exception:
        LOGGER.exception("Model training failed")
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
