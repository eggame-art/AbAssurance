from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, SQLTransformer, StandardScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from model_training.config import TrainingConfig
from shared.errors import ConfigurationError


def finite_value(column: str):
    value = F.col(column).cast("double")
    return F.when(value.isNotNull() & ~F.isnan(value) & (F.abs(value) != float("inf")), value)


def build_pipeline(training: DataFrame, config: TrainingConfig, estimator) -> tuple[Pipeline, dict]:
    counts_row = training.agg(*[F.count(finite_value(c)).alias(c) for c in config.features]).first()

    if counts_row is None:
        raise ConfigurationError("Unable to determine training feature counts")

    counts = counts_row.asDict()
    empty = [c for c in config.features if counts[c] == 0]

    clean = [f"__ml_clean_{i}" for i in range(len(config.features))]
    imputed = [f"__ml_imputed_{i}" for i in range(len(config.features))]

    expressions = []

    for column, output in zip(config.features, clean, strict=True):
        value = f"CAST(`{column}` AS DOUBLE)"

        expression = (
            "0.0"
            if column in empty
            else f"CASE WHEN NOT isnan({value}) AND abs({value}) != CAST('Infinity' AS DOUBLE) "
            f"THEN {value} ELSE NULL END"
        )

        expressions.append(f"{expression} AS `{output}`")

    stages = [
        SQLTransformer(statement="SELECT *, " + ", ".join(expressions) + " FROM __THIS__"),
        Imputer(inputCols=clean, outputCols=imputed, strategy=config.imputation, relativeError=0.0),
        VectorAssembler(inputCols=imputed, outputCol="__ml_unscaled", handleInvalid="error"),
    ]

    scaled = config.algorithm == "logistic-regression" and config.standardize

    if scaled:
        stages.append(StandardScaler(inputCol="__ml_unscaled", outputCol="features", withMean=True, withStd=True))
    else:
        stages.append(SQLTransformer(statement="SELECT *, __ml_unscaled AS features FROM __THIS__"))

    stages.append(estimator)

    return Pipeline(stages=stages), {
        "imputation": config.imputation,
        "nonfinite_as_missing": True,
        "entirely_missing_training_features": empty,
        "entirely_missing_fallback": 0.0,
        "standardized": scaled,
        "fit_split": "training",
        "imputer_relative_error": 0.0,
    }
