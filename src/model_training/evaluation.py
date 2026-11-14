import math
from typing import TYPE_CHECKING, Any, Literal, cast

from shared.errors import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def vector_values(frame: "DataFrame", column: str):
    from pyspark.sql import Column

    jvm = cast(Any, frame.sparkSession.sparkContext._jvm)

    if jvm is None:
        raise ConfigurationError("Spark JVM is unavailable")

    return Column(jvm.org.apache.spark.ml.functions.vector_to_array(frame[column]._jc, "float64"))


def classification_metrics(tn: int, fp: int, fn: int, tp: int) -> dict:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0

    return {
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
        "confusion_matrix": {
            "tn": tn,
            "fp": fp,
            "fn": fn,
            "tp": tp,
            "rows_actual_columns_predicted": [[tn, fp], [fn, tp]],
        },
        "class_counts": {"0": tn + fp, "1": fn + tp},
        "records": tn + fp + fn + tp,
        "positive_class": 1,
        "zero_division": 0,
    }


def prediction_values(predictions: "DataFrame") -> "DataFrame":
    return predictions.select("label", "prediction", vector_values(predictions, "probability").alias("probability"))


def evaluate(predictions: "DataFrame") -> dict:
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.sql import functions as F

    probabilities = vector_values(predictions, "probability")[1]

    invalid = (
        F.col("label").isNull()
        | ~F.col("label").isin(0.0, 1.0)
        | F.col("prediction").isNull()
        | ~F.col("prediction").isin(0.0, 1.0)
        | probabilities.isNull()
        | F.isnan(probabilities)
        | ~probabilities.between(0.0, 1.0)
    )

    if predictions.filter(invalid).limit(1).count():
        raise ConfigurationError("Invalid binary predictions")

    counts = {
        (int(row.label), int(row.prediction)): row["count"]
        for row in predictions.groupBy("label", "prediction").count().collect()
    }

    result = classification_metrics(*(counts.get(pair, 0) for pair in ((0, 0), (0, 1), (1, 0), (1, 1))))

    if any(count == 0 for count in result["class_counts"].values()):
        raise ConfigurationError("Both classes are required for ROC-AUC and PR-AUC")

    metrics: tuple[tuple[str, Literal["areaUnderROC", "areaUnderPR"]], ...] = (
        ("roc_auc", "areaUnderROC"),
        ("pr_auc", "areaUnderPR"),
    )

    for name, spark_name in metrics:
        result[name] = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="probability", metricName=spark_name, numBins=0
        ).evaluate(predictions)

        if not math.isfinite(result[name]):
            raise ConfigurationError("Nonfinite evaluation metric")

    result["evaluation_unit"] = "contract observation; repeated customers may be correlated"
    result["uncertainty"] = "Descriptive metrics only; no customer-level confidence interval or business acceptance"

    return result
