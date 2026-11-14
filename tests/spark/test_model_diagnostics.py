import pytest
from pyspark.ml.classification import LogisticRegressionModel

from model_training.diagnostics import feature_diagnostics, probability_diagnostics
from model_training.evaluation import evaluate
from model_training.training import fit_model, model_input

pytestmark = pytest.mark.spark_integration


def test_identical_features_with_opposite_labels_are_detected(spark):
    from model_training.config import TrainingConfig

    frame = spark.createDataFrame([(1.0, 0), (1.0, 0), (1.0, 1)], "claim_count double, is_cancelled int")

    config = TrainingConfig("hdfs://namenode:9000/dataset", "hdfs://namenode:9000/models", features=("claim_count",))

    diagnostic = feature_diagnostics(frame, config.features)

    assert diagnostic["constant_or_empty_features"] == ["claim_count"]
    assert diagnostic["feature_vector_conflicts"] == {
        "feature_vectors": 1,
        "mixed_label_vectors": 1,
        "maximum_correct_for_identical_inputs": 2,
    }

    model, _, _ = fit_model(frame, config)
    predictions = model.transform(model_input(frame, config.features))

    assert probability_diagnostics(predictions)["distinct"] == 1
    assert evaluate(predictions)["roc_auc"] == 0.5


def test_predictive_histories_have_signal_without_using_future_outcomes(spark):
    from dataset_preparation.config import PreparationConfig
    from dataset_preparation.history import history_schema, validate_history
    from dataset_preparation.predictive import predictive_records
    from dataset_preparation.temporal import HistoricalDataset
    from model_training.config import TrainingConfig

    records = predictive_records()

    frames = {entity: spark.createDataFrame(rows, history_schema(entity)) for entity, rows in records.items()}

    validate_history(frames, is_synthetic=True, as_of_date="2026-05-01")

    config = PreparationConfig(
        source_root="hdfs://namenode:9000/source",
        output_root="hdfs://namenode:9000/synthetic/data",
        history_root="hdfs://namenode:9000/synthetic/history",
        observation_date="2025-07-01",
        purpose="synthetic-training",
    )

    prepared = HistoricalDataset(frames, config, {"is_synthetic": True, "as_of_date": "2026-01-01"})

    try:
        training = prepared.dataset.filter("training_eligible")

        model_config = TrainingConfig(config.output_root, "hdfs://namenode:9000/synthetic/models")

        diagnostic = feature_diagnostics(training, model_config.features)

        assert diagnostic["records"] == 104
        assert diagnostic["features"]["days_since_last_payment"]["distinct_finite"] > 10
        assert diagnostic["constant_or_empty_features"] == []

        by_label = {row["is_cancelled"]: row for row in diagnostic["by_label"]}

        assert by_label[1]["days_since_last_payment"] > by_label[0]["days_since_last_payment"]

        model, _, _ = fit_model(training, model_config)

        classifier = model.stages[-1]

        assert isinstance(classifier, LogisticRegressionModel)
        assert classifier.coefficients.toArray()[-1] > 0

        probabilities = probability_diagnostics(model.transform(model_input(training, model_config.features)))

        assert probabilities["distinct"] > 10
    finally:
        prepared.close()
