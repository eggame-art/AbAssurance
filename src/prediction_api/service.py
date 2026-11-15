from threading import Lock
from typing import TYPE_CHECKING, Any, Literal, cast

from prediction_api.schemas import ModelInfo, PredictionRequest, PredictionResponse

if TYPE_CHECKING:
    from pyspark.ml import PipelineModel
    from pyspark.sql import SparkSession


class UnexpectedFeatures(ValueError):
    def __init__(self, fields: set[str]):
        self.fields = sorted(fields)
        super().__init__("Features are not used by the loaded model")


class PredictionUnavailable(RuntimeError):
    pass


class PredictionService:
    def __init__(self, spark: "SparkSession", model: "PipelineModel", info: ModelInfo):
        self.spark = spark
        self.model = model
        self.info = info
        self._lock = Lock()
        self._closed = False

    @property
    def available(self) -> bool:
        if self._closed:
            return False

        try:
            jsc = cast(Any, self.spark.sparkContext._jsc)

            if jsc is None:
                return False

            return not jsc.sc().isStopped()
        except Exception:
            return False

    def predict(self, contract: PredictionRequest) -> PredictionResponse:
        unexpected = contract.model_fields_set - set(self.info.feature_columns)

        if unexpected:
            raise UnexpectedFeatures(unexpected)

        values = contract.model_dump()
        columns = self.info.feature_columns
        schema = ", ".join(f"{name} double" for name in columns)
        row = tuple(None if values[name] is None else float(values[name]) for name in columns)

        with self._lock:
            if not self.available:
                raise PredictionUnavailable("Model runtime is unavailable")

            frame = self.spark.createDataFrame([row], schema)
            result = self.model.transform(frame).select("probability", "prediction").first()

        if result is None or result.prediction not in (0.0, 1.0):
            raise RuntimeError("Pipeline returned an invalid binary prediction")

        predicted_class: Literal[0, 1] = 1 if result.prediction == 1.0 else 0

        return PredictionResponse(
            cancellation_probability=float(result.probability[1]),
            predicted_class=predicted_class,
            model_id=self.info.model_id,
            status=self.info.status,
            business_validated=self.info.business_validated,
        )

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                self.spark.stop()
