from dataset_preparation.config import FEATURE_COLUMNS
from prediction_api.config import APIConfig
from prediction_api.schemas import ModelInfo, PredictionRequest
from prediction_api.service import PredictionService
from shared.errors import ConfigurationError


def load_service(config: APIConfig) -> PredictionService:
    from model_training.persistence import load_model
    from spark_pipeline.run import create_session

    spark = create_session(kafka=False, master=config.spark_master)

    try:
        spark.sparkContext.setLogLevel("WARN")

        model, report = load_model(
            spark, config.model_root, config.model_id, allow_experimental=config.allow_experimental
        )

        info = ModelInfo.model_validate({name: report.get(name) for name in ModelInfo.model_fields})

        columns = info.feature_columns

        if (
            not columns
            or len(set(columns)) != len(columns)
            or not set(columns) <= set(FEATURE_COLUMNS)
            or set(PredictionRequest.model_fields) != set(FEATURE_COLUMNS)
            or report.get("config", {}).get("features") != columns
        ):
            raise ConfigurationError("Unsupported model feature schema")

        if info.status == "experimental" and (info.business_validated or not info.experimental_warning):
            raise ConfigurationError("Experimental models must retain their validation limits")

        return PredictionService(spark, model, info)

    except BaseException:
        spark.stop()
        raise
