from dataclasses import replace

import pytest
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F

from dataset_preparation.config import FEATURE_COLUMNS
from model_training.config import TrainingConfig
from model_training.evaluation import evaluate, prediction_values
from model_training.training import feature_influence, fit_model, model_input
from shared.errors import ConfigurationError

pytestmark = pytest.mark.spark_integration


def test_auc_and_confusion_matrix(spark):
    rows = [
        (0.0, 0.0, Vectors.dense([0.9, 0.1])),
        (0.0, 1.0, Vectors.dense([0.8, 0.2])),
        (1.0, 0.0, Vectors.dense([0.3, 0.7])),
        (1.0, 1.0, Vectors.dense([0.1, 0.9])),
    ]
    data = spark.createDataFrame(rows, ["label", "prediction", "probability"])
    metrics = evaluate(data)
    assert metrics["roc_auc"] == pytest.approx(1)
    assert metrics["pr_auc"] == pytest.approx(1)
    assert metrics["precision"] == metrics["recall"] == metrics["f1"] == 0.5
    assert metrics["confusion_matrix"]["rows_actual_columns_predicted"] == [[1, 1], [1, 1]]
    with pytest.raises(ConfigurationError, match="Both classes"):
        evaluate(data.filter("label = 0"))


@pytest.mark.parametrize("algorithm", ["logistic-regression", "random-forest"])
def test_preprocessing_fits_only_training_and_predictions_ignore_outcome_fields(spark, algorithm):
    config = TrainingConfig(
        "hdfs://namenode:9000/dataset", "hdfs://namenode:9000/models", algorithm=algorithm, num_trees=3
    )
    rows = [(float(i), None if i == 0 else float(i), float("nan"), float("inf"), 2.0, None, i % 2) for i in range(8)]
    schema = ", ".join(f"{c} double" for c in FEATURE_COLUMNS) + ", is_cancelled int"
    training = spark.createDataFrame(rows, schema)
    model, metadata, weights = fit_model(training, config)
    assert metadata["entirely_missing_training_features"] == [
        "payment_count",
        "payment_amount_known_total",
        "days_since_last_payment",
    ]
    assert metadata["surrogates"]["__ml_clean_1"] == 4.0
    assert metadata["standardized"] == (algorithm == "logistic-regression")
    if metadata["standardized"]:
        assert metadata["scaler_mean"][0] == 3.5
    assert weights == {"0": 1.0, "1": 1.0}
    inputs = model_input(training, config.features)
    assert inputs.columns == [*FEATURE_COLUMNS, "label"]
    original = prediction_values(model.transform(inputs))
    changed = prediction_values(
        model.transform(
            inputs.withColumn("customer_id", F.lit("future"))
            .withColumn("is_cancelled", F.lit(999))
            .withColumn("horizon_end", F.lit("2099-01-01"))
        )
    )
    assert original.exceptAll(changed).count() == 0
    # No outcome column is required at inference time.
    assert model.transform(training.select(*FEATURE_COLUMNS)).count() == 8
    assert list(feature_influence(model, config)["values"]) == FEATURE_COLUMNS
    assert replace(config, features=("claim_count",)).features == ("claim_count",)


def test_balanced_weights_come_from_training_only(spark):
    config = TrainingConfig("hdfs://namenode:9000/dataset", "hdfs://namenode:9000/models", features=("claim_count",))
    frame = spark.createDataFrame([(1.0, 0), (2.0, 0), (3.0, 0), (4.0, 1)], "claim_count double, is_cancelled int")
    _, _, weights = fit_model(frame, config)
    assert weights == {"0": 2 / 3, "1": 2.0}
