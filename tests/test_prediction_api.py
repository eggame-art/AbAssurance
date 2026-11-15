import sys
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from dataset_preparation.config import FEATURE_COLUMNS
from prediction_api.app import create_app
from prediction_api.config import APIConfig
from prediction_api.loader import load_service
from prediction_api.schemas import ModelInfo
from prediction_api.service import PredictionService
from shared.errors import ConfigurationError


@pytest.fixture
def report():
    return {
        "model_id": "a" * 32,
        "algorithm": "logistic-regression",
        "status": "data_validated",
        "business_validated": False,
        "validation_scope": "Pipeline and dataset checks only",
        "experimental_warning": None,
        "feature_columns": FEATURE_COLUMNS.copy(),
        "config": {"features": FEATURE_COLUMNS.copy()},
        "metrics": {"roc_auc": 0.5, "confusion_matrix": {"tn": 1}},
    }


@pytest.fixture
def runtime(report):
    spark = Mock()
    spark.sparkContext._jsc.sc.return_value.isStopped.return_value = False

    model = Mock()
    model.transform.return_value.select.return_value.first.return_value = SimpleNamespace(
        probability=[0.7, 0.3], prediction=0.0
    )

    return PredictionService(spark, model, ModelInfo.model_validate(report))


@pytest.fixture
def client(runtime, monkeypatch):
    loader = Mock(return_value=runtime)
    monkeypatch.setattr("prediction_api.app.load_service", loader)

    with TestClient(create_app(APIConfig())) as test_client:
        yield test_client, runtime, loader

    runtime.spark.stop.assert_called_once()


def test_endpoints_load_once_and_reuse_pipeline_without_label(client):
    client, service, loader = client

    health = client.get("/health")

    assert health.status_code == 200
    assert health.json() == {"status": "ok", "model_loaded": True, "spark_available": True, "model_id": "a" * 32}

    assert client.get("/model").json() == service.info.model_dump()

    for _ in range(2):
        response = client.post("/predict", json={"claim_count": 2, "payment_amount_known_total": 100.25})

        assert response.status_code == 200
        assert response.json() == {
            "cancellation_probability": 0.3,
            "predicted_class": 0,
            "model_id": "a" * 32,
            "status": "data_validated",
            "business_validated": False,
        }

    loader.assert_called_once_with(APIConfig())

    rows, schema = service.spark.createDataFrame.call_args.args

    assert rows == [(None, 2.0, None, 100.25, None, None)]
    assert schema == ", ".join(f"{name} double" for name in FEATURE_COLUMNS)

    service.model.fit.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        {"claim_count": "2"},
        {"claim_count": True},
        {"claim_count": 1.5},
        {"claim_count": -1},
        {"contract_tenure_days": 2**31},
        {"payment_count": 2**63},
        {"payment_amount_known_total": "100.50"},
        {"payment_amount_known_total": False},
        {"payment_amount_known_total": -0.1},
        {"is_cancelled": 1},
        {"label": 1},
        {"cancellation_date": "2027-01-01"},
        {"horizon_end": "2027-01-01"},
        {"contract_id": "id"},
        {"features": [1, 2]},
        {"__ml_weight": 1},
        [],
        None,
    ],
)
def test_invalid_requests_never_reach_spark(client, payload):
    client, service, _ = client

    assert client.post("/predict", json=payload).status_code == 422

    service.model.transform.assert_not_called()
    service.spark.createDataFrame.assert_not_called()


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e1000"])
def test_nonfinite_json_numbers_return_serializable_validation_errors(client, token):
    client, service, _ = client

    response = client.post(
        "/predict",
        content='{"payment_amount_known_total": ' + token + "}",
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "payment_amount_known_total"]

    service.model.transform.assert_not_called()


def test_missing_and_null_values_are_passed_to_saved_preprocessing(client):
    client, service, _ = client

    for payload in ({}, dict.fromkeys(FEATURE_COLUMNS)):
        assert client.post("/predict", json=payload).status_code == 200
        assert service.spark.createDataFrame.call_args.args[0] == [(None,) * 6]


def test_subset_order_and_unused_features_are_enforced(client):
    client, service, _ = client
    service.info.feature_columns = ["payment_count", "claim_count"]

    for value in (4, None):
        assert client.post("/predict", json={"contract_tenure_days": value}).status_code == 422

    service.spark.createDataFrame.assert_not_called()

    assert client.post("/predict", json={"claim_count": 2, "payment_count": 5}).status_code == 200

    assert service.spark.createDataFrame.call_args.args == ([(5.0, 2.0)], "payment_count double, claim_count double")


def test_experimental_status_and_warning_are_not_promoted(client):
    client, service, _ = client

    service.info.status = "experimental"
    service.info.experimental_warning = "Synthetic technical experiment; not real business performance"

    assert client.get("/model").json()["experimental_warning"] == service.info.experimental_warning

    result = client.post("/predict", json={}).json()

    assert result["status"] == "experimental"
    assert result["business_validated"] is False


def test_unavailable_runtime_and_missing_service(client):
    client, service, _ = client
    service.spark.sparkContext._jsc.sc.return_value.isStopped.return_value = True

    assert client.get("/health").json()["spark_available"] is False
    assert client.get("/health").status_code == 503
    assert client.get("/model").status_code == 503
    assert client.post("/predict", json={}).status_code == 503

    client.app.state.prediction_service = None

    assert client.get("/health").json()["model_loaded"] is False


@pytest.mark.parametrize(
    "result",
    [
        None,
        SimpleNamespace(probability=[0.2, 0.8], prediction=2.0),
        SimpleNamespace(probability=[0.2, float("nan")], prediction=1.0),
    ],
)
def test_invalid_pipeline_output_returns_500(client, result):
    client, service, _ = client
    service.model.transform.return_value.select.return_value.first.return_value = result

    assert client.post("/predict", json={}).json() == {"detail": "Prediction failed"}
    assert client.post("/predict", json={}).status_code == 500


def test_internal_errors_are_not_exposed_and_resources_close(client):
    client, service, _ = client
    service.model.transform.side_effect = RuntimeError("internal HDFS path and diagnostics")

    response = client.post("/predict", json={})

    assert response.status_code == 500
    assert response.json() == {"detail": "Prediction failed"}


def test_openapi_and_malformed_json(client):
    client, _, _ = client
    schema = client.get("/openapi.json").json()

    assert {"/health", "/model", "/predict"} <= schema["paths"].keys()
    assert list(schema["components"]["schemas"]["PredictionRequest"]["properties"]) == FEATURE_COLUMNS
    assert schema["components"]["schemas"]["PredictionRequest"]["additionalProperties"] is False

    assert client.post("/predict", content="{", headers={"Content-Type": "application/json"}).status_code == 422


@pytest.mark.parametrize(
    "changes",
    [
        {"model_root": "file:///tmp/model"},
        {"model_root": "hdfs://namenode:9000/%6dodels"},
        {"model_id": "../bad"},
        {"allow_experimental": "true"},
        {"spark_master": " "},
    ],
)
def test_invalid_configuration(changes):
    with pytest.raises(ConfigurationError):
        replace(APIConfig(), **changes)


def test_environment_configuration_requires_explicit_opt_in(monkeypatch):
    monkeypatch.delenv("API_ALLOW_EXPERIMENTAL", raising=False)

    assert APIConfig.from_environment().allow_experimental is False

    monkeypatch.setenv("API_MODEL_ID", "b" * 32)
    monkeypatch.setenv("API_ALLOW_EXPERIMENTAL", "true")
    monkeypatch.setenv("API_SPARK_MASTER", "local[1]")

    config = APIConfig.from_environment()

    assert config.model_id == "b" * 32
    assert config.allow_experimental
    assert config.spark_master == "local[1]"

    monkeypatch.setenv("API_ALLOW_EXPERIMENTAL", "maybe")

    with pytest.raises(ConfigurationError):
        APIConfig.from_environment()


@pytest.fixture
def mocked_loader(report, runtime, monkeypatch):
    create = Mock(return_value=runtime.spark)
    load = Mock(return_value=(runtime.model, report))

    monkeypatch.setattr("spark_pipeline.run.create_session", create)
    monkeypatch.setitem(sys.modules, "model_training.persistence", SimpleNamespace(load_model=load))

    return runtime, create, load


@pytest.mark.parametrize("model_id", [None, "a" * 32])
@pytest.mark.parametrize("allow", [False, True])
def test_loader_forwards_selection_and_opt_in(mocked_loader, model_id, allow):
    runtime, create, load = mocked_loader
    config = APIConfig(model_id=model_id, allow_experimental=allow)

    service = load_service(config)

    create.assert_called_once_with(kafka=False, master="local[2]")

    load.assert_called_once_with(runtime.spark, config.model_root, model_id, allow_experimental=allow)

    assert service.info.model_id == runtime.info.model_id

    service.close()
    service.close()

    runtime.spark.stop.assert_called_once()


@pytest.mark.parametrize("reason", ["Experimental model cannot be used without explicit opt-in", "Missing HDFS model"])
def test_loading_failure_aborts_startup_and_closes_spark(mocked_loader, reason):
    runtime, _, load = mocked_loader
    load.side_effect = ConfigurationError(reason)

    with pytest.raises(ConfigurationError, match=reason), TestClient(create_app(APIConfig())):
        pytest.fail("Startup must not accept a missing or unauthorized model")

    runtime.spark.stop.assert_called_once()


@pytest.mark.parametrize("columns", [[], ["label"], ["claim_count", "claim_count"], ["cancellation_date"]])
def test_loader_rejects_unsafe_feature_metadata(mocked_loader, report, columns):
    runtime, _, _ = mocked_loader

    report["feature_columns"] = columns
    report["config"]["features"] = columns

    with pytest.raises(ConfigurationError, match="feature schema"):
        load_service(APIConfig())

    runtime.spark.stop.assert_called_once()


def test_loader_preserves_feature_order_and_rejects_conflicting_metadata(mocked_loader, report):
    runtime, _, _ = mocked_loader

    report["feature_columns"] = ["payment_count", "claim_count"]
    report["config"]["features"] = report["feature_columns"].copy()

    assert load_service(APIConfig()).info.feature_columns == ["payment_count", "claim_count"]

    report["config"]["features"].reverse()

    with pytest.raises(ConfigurationError, match="feature schema"):
        load_service(APIConfig())

    runtime.spark.stop.assert_called_once()


def test_loader_rejects_experimental_business_validation(mocked_loader, report):
    report.update(status="experimental", business_validated=True, experimental_warning="Synthetic experiment")

    with pytest.raises(ConfigurationError, match="validation limits"):
        load_service(APIConfig(allow_experimental=True))
