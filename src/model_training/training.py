from typing import cast

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import ImputerModel, StandardScalerModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from model_training.config import TrainingConfig
from model_training.preprocessing import build_pipeline
from shared.errors import ConfigurationError


def make_estimator(config: TrainingConfig):
    if config.algorithm == "logistic-regression":
        return LogisticRegression(
            labelCol="label",
            featuresCol="features",
            weightCol="__ml_weight",
            family="binomial",
            maxIter=config.max_iter,
            regParam=config.reg_param,
            elasticNetParam=config.elastic_net_param,
            tol=config.tolerance,
            threshold=config.threshold,
            standardization=False,
        )

    return RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        weightCol="__ml_weight",
        seed=config.seed,
        numTrees=config.num_trees,
        maxDepth=config.max_depth,
    ).setThresholds([1.0 - config.threshold, config.threshold])


def model_input(frame: DataFrame, features: tuple[str, ...]) -> DataFrame:
    return frame.select(*features, F.col("is_cancelled").cast("double").alias("label"))


def fit_model(training: DataFrame, config: TrainingConfig):
    counts = {int(row.is_cancelled): row["count"] for row in training.groupBy("is_cancelled").count().collect()}

    total = sum(counts.values())

    weights = {str(c): total / (2 * counts[c]) if config.class_weight == "balanced" else 1.0 for c in (0, 1)}

    inputs = model_input(training, config.features).withColumn(
        "__ml_weight", F.when(F.col("label") == 1.0, weights["1"]).otherwise(weights["0"])
    )

    pipeline, preprocessing = build_pipeline(inputs, config, make_estimator(config))

    model = pipeline.fit(inputs)

    imputer = cast(ImputerModel, model.stages[1])
    surrogate = imputer.surrogateDF.first()

    if surrogate is None:
        raise ConfigurationError("Unable to retrieve imputation values")

    preprocessing["surrogates"] = surrogate.asDict()

    if preprocessing["standardized"]:
        scaler = cast(StandardScalerModel, model.stages[-2])

        preprocessing["scaler_mean"] = scaler.mean.toArray().tolist()
        preprocessing["scaler_std"] = scaler.std.toArray().tolist()

    return model, preprocessing, weights


def feature_influence(model, config: TrainingConfig) -> dict:
    classifier = model.stages[-1]

    if config.algorithm == "logistic-regression":
        values = classifier.coefficients.toArray().tolist()

        return {
            "kind": "logistic_coefficients",
            "intercept": float(classifier.intercept),
            "scale": "standardized" if config.standardize else "original",
            "values": dict(zip(config.features, values, strict=True)),
            "interpretation": "Association with log-odds of class 1; not causal importance",
        }

    return {
        "kind": "impurity_feature_importance",
        "values": dict(zip(config.features, classifier.featureImportances.toArray().tolist(), strict=True)),
        "interpretation": "Training impurity reduction; not causal importance",
    }
