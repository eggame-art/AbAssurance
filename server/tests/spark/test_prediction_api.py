import os
from unittest.mock import Mock
from uuid import uuid4

import pytest
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_preparation.config import FEATURE_COLUMNS
from abassurance.systems.model_training.config import VERSION, TrainingConfig
from abassurance.systems.model_training.persistence import load_model, publish_model
from abassurance.systems.model_training.training import fit_model
from abassurance.systems.prediction_api.app import create_app
from abassurance.systems.prediction_api.config import APIConfig
from abassurance.systems.prediction_api.schemas import ModelInfo
from abassurance.systems.prediction_api.service import PredictionService
from abassurance.systems.spark_pipeline.hdfs import HdfsState
from fastapi.testclient import TestClient
from py4j.protocol import Py4JJavaError


@pytest.fixture(scope="module", params=["logistic-regression", "random-forest", "ordered-subset"])
def inference_pipeline(spark, request):
    features = ("payment_count", "claim_count") if request.param == "ordered-subset" else tuple(FEATURE_COLUMNS)

    config = TrainingConfig(
        "hdfs://namenode:9000/datasets",
        "hdfs://namenode:9000/models",
        algorithm="random-forest" if request.param == "random-forest" else "logistic-regression",
        features=features,
        num_trees=3,
        max_iter=10,
        threshold=0.35,
    )

    rows = [(float(i + 1), float(i % 4), float(i), float(i * 20), 0.0, None, i % 2) for i in range(12)]

    schema = ", ".join(f"{name} double" for name in FEATURE_COLUMNS) + ", is_cancelled int"
    frame = spark.createDataFrame(rows, schema)

    model, _, _ = fit_model(frame, config)

    report = {
        "version": VERSION,
        "model_id": uuid4().hex,
        "algorithm": config.algorithm,
        "status": "experimental",
        "business_validated": False,
        "validation_scope": "Synthetic inference test only; no business acceptance",
        "experimental_warning": "Synthetic technical experiment; metrics are not real performance",
        "feature_columns": list(features),
        "config": {"features": list(features)},
        "metrics": {},
    }

    return model, frame.selectExpr(*features, "CAST(is_cancelled AS DOUBLE) AS label"), report


def assert_api_parity(client, spark, pipeline, report):
    payloads = [
        {
            name: value
            for name, value in zip(FEATURE_COLUMNS, (150, 2, 5, 450.75, 0, 12), strict=True)
            if name in report["feature_columns"]
        },
        {},
        dict.fromkeys(report["feature_columns"]),
    ]

    for payload in payloads:
        schema = ", ".join(f"{name} double" for name in report["feature_columns"])

        values = tuple(
            None if payload.get(name) is None else float(payload[name]) for name in report["feature_columns"]
        )

        direct = pipeline.transform(spark.createDataFrame([values], schema)).select("probability", "prediction").first()

        response = client.post("/predict", json=payload)

        assert response.status_code == 200, response.text

        result = response.json()

        assert result["cancellation_probability"] == direct.probability[1]
        assert result["predicted_class"] == int(direct.prediction)
        assert result["model_id"] == report["model_id"]
        assert result["status"] == "experimental"
        assert result["business_validated"] is False


@pytest.mark.spark_integration
def test_api_matches_us10_pipeline_without_target(spark, inference_pipeline, monkeypatch):
    pipeline, _, report = inference_pipeline

    service = PredictionService(spark, pipeline, ModelInfo.model_validate(report))

    close = Mock()
    monkeypatch.setattr(service, "close", close)

    loader = Mock(return_value=service)
    monkeypatch.setattr("abassurance.systems.prediction_api.app.load_service", loader)

    with TestClient(create_app(APIConfig(allow_experimental=True))) as client:
        assert_api_parity(client, spark, pipeline, report)

    loader.assert_called_once()
    close.assert_called_once()


@pytest.mark.hdfs_integration
def test_hdfs_startup_authorization_pinning_and_prediction_parity(spark, inference_pipeline, monkeypatch):
    pipeline, inputs, metadata = inference_pipeline

    root = os.environ.get("HDFS_TEST_ROOT", "hdfs://namenode:9000/abassurance-tests")

    state = HdfsState(spark, f"{root}/{uuid4().hex}/synthetic/models")

    monkeypatch.setattr(spark, "stop", Mock())
    monkeypatch.setattr("abassurance.systems.spark_pipeline.run.create_session", Mock(return_value=spark))

    try:
        report = publish_model(spark, state.root, pipeline, metadata, inputs)

        config = APIConfig(model_root=state.root)

        with pytest.raises(ConfigurationError, match="Experimental model"), TestClient(create_app(config)):
            pytest.fail("Experimental models require explicit API configuration")

        restored, _ = load_model(spark, state.root, allow_experimental=True)

        config = APIConfig(model_root=state.root, allow_experimental=True)

        with TestClient(create_app(config)) as client:
            assert client.get("/health").status_code == 200

            assert client.get("/model").json()["experimental_warning"] == metadata["experimental_warning"]

            state.write_json_atomic("CURRENT.json", {"model_id": "b" * 32})

            assert_api_parity(client, spark, restored, report)

        with pytest.raises(Py4JJavaError, match="File does not exist"), TestClient(create_app(config)):
            pytest.fail("A missing CURRENT model must abort startup")

        pinned = APIConfig(model_root=state.root, model_id=report["model_id"], allow_experimental=True)

        with TestClient(create_app(pinned)) as client:
            assert_api_parity(client, spark, restored, report)

        assert spark.stop.call_count == 4

    finally:
        state.fs.delete(state.java_path(""), True)
