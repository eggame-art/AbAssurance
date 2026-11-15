import argparse
import json
from dataclasses import replace

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from abassurance.shared.errors import ConfigurationError
from abassurance.systems.model_training.config import TrainingConfig
from abassurance.systems.model_training.data import digest, load_dataset
from abassurance.systems.model_training.evaluation import evaluate, vector_values
from abassurance.systems.model_training.preprocessing import finite_value


def feature_diagnostics(frame: DataFrame, features: tuple[str, ...]) -> dict:
    expressions = [F.count("*").alias("records")]

    for index, column in enumerate(features):
        value = finite_value(column)

        expressions.extend(
            [
                F.count(value).alias(f"n{index}"),
                F.countDistinct(value).alias(f"d{index}"),
                F.min(value).alias(f"min{index}"),
                F.max(value).alias(f"max{index}"),
                F.avg(value).alias(f"mean{index}"),
                F.stddev_samp(value).alias(f"std{index}"),
            ]
        )

    stats_row = frame.agg(*expressions).first()

    if stats_row is None:
        raise ConfigurationError("Unable to compute feature diagnostics")

    stats = stats_row.asDict()

    grouped = frame.groupBy(*features).agg(F.count("*").alias("n"), F.sum("is_cancelled").alias("positives"))

    conflicts_row = grouped.agg(
        F.count("*").alias("feature_vectors"),
        F.sum(F.when((F.col("positives") > 0) & (F.col("positives") < F.col("n")), 1).otherwise(0)).alias(
            "mixed_label_vectors"
        ),
        F.sum(F.greatest("positives", F.col("n") - F.col("positives"))).alias("maximum_correct_for_identical_inputs"),
    ).first()

    if conflicts_row is None:
        raise ConfigurationError("Unable to compute feature vector conflicts")

    conflicts = conflicts_row.asDict()

    return {
        "records": stats["records"],
        "features": {
            column: {
                "finite_count": stats[f"n{i}"],
                "missing_or_nonfinite": stats["records"] - stats[f"n{i}"],
                "distinct_finite": stats[f"d{i}"],
                "min": stats[f"min{i}"],
                "max": stats[f"max{i}"],
                "mean": stats[f"mean{i}"],
                "std": stats[f"std{i}"],
            }
            for i, column in enumerate(features)
        },
        "constant_or_empty_features": [column for i, column in enumerate(features) if stats[f"d{i}"] <= 1],
        "feature_vector_conflicts": conflicts,
        "by_label": [
            row.asDict()
            for row in frame.groupBy("is_cancelled")
            .agg(F.count("*").alias("records"), *[F.avg(finite_value(c)).alias(c) for c in features])
            .orderBy("is_cancelled")
            .collect()
        ],
        "by_observation": [
            row.asDict()
            for row in frame.groupBy(F.col("observation_date").cast("string").alias("observation_date"), "is_cancelled")
            .count()
            .orderBy("observation_date", "is_cancelled")
            .collect()
        ]
        if "observation_date" in frame.columns
        else [],
        "scope": "Descriptive only; identical feature vectors with different outcomes cannot be separated",
    }


def probability_diagnostics(predictions: DataFrame) -> dict:
    values = predictions.withColumn("__risk", vector_values(predictions, "probability")[1])

    summary_row = values.agg(
        F.min("__risk").alias("min"),
        F.max("__risk").alias("max"),
        F.avg("__risk").alias("mean"),
        F.countDistinct("__risk").alias("distinct"),
        F.sum(F.col("prediction").cast("long")).alias("predicted_positives"),
    ).first()

    if summary_row is None:
        raise ConfigurationError("Unable to compute probability diagnostics")

    summary = summary_row.asDict()

    summary["by_label"] = [
        row.asDict()
        for row in values.groupBy("label")
        .agg(
            F.count("*").alias("records"),
            F.min("__risk").alias("min"),
            F.max("__risk").alias("max"),
            F.avg("__risk").alias("mean"),
        )
        .orderBy("label")
        .collect()
    ]

    return summary


def diagnose_model(spark, root: str, model_id: str | None = None, *, allow_experimental: bool = False) -> dict:
    from abassurance.systems.model_training.persistence import load_model
    from abassurance.systems.model_training.training import model_input

    model, metadata = load_model(spark, root, model_id, allow_experimental=allow_experimental)

    options = {**metadata["config"], "features": tuple(metadata["feature_columns"])}

    config = replace(TrainingConfig(**options), dataset_id=metadata["dataset"]["dataset_id"])

    dataset = load_dataset(spark, config)

    try:
        if digest(dataset.report) != metadata["dataset"]["report_sha256"]:
            raise ConfigurationError("Dataset report differs from the one used to train this model")

        result = {
            "model_id": metadata["model_id"],
            "dataset_id": dataset.report["dataset_id"],
            "status": metadata["status"],
            "algorithm": metadata["algorithm"],
            "threshold": config.threshold,
            "class_weights": metadata["class_weights"],
            "feature_influence": metadata["feature_influence"],
            "preprocessing": metadata["preprocessing"],
            "training_ready": dataset.report["training_ready"],
            "synthetic_training_ready": dataset.report["synthetic_training_ready"],
            "splits": {},
        }

        for name in ("training", "evaluation"):
            frame = getattr(dataset, name)
            predictions = model.transform(model_input(frame, config.features)).cache()

            try:
                result["splits"][name] = {
                    "features": feature_diagnostics(frame, config.features),
                    "probabilities": probability_diagnostics(predictions),
                    "metrics": evaluate(predictions),
                }
            finally:
                predictions.unpersist()

        result["warning"] = "Read-only descriptive diagnostics; evaluation must never be used for fitting or tuning"

        return result
    finally:
        dataset.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", required=True)
    parser.add_argument("--model-id")
    parser.add_argument("--allow-experimental", action="store_true")

    args = parser.parse_args()

    from abassurance.systems.spark_pipeline.run import create_session

    spark = create_session(kafka=False)
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = diagnose_model(spark, args.model_dir, args.model_id, allow_experimental=args.allow_experimental)

        print(json.dumps(result, default=str, indent=2, allow_nan=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
