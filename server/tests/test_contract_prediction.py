from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from abassurance.systems.prediction_api import contracts
from abassurance.systems.prediction_api.routes import get_service
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

BODY = {
    "customer_id": "0ce23490-7b17-57a3-933d-829abb01a605",
    "contract_id": "5b128423-a27b-5c20-b078-25cc44399530",
    "integration_id": "a" * 32,
    "observation_date": "2025-07-01",
}


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(contracts.router)

    service = object()
    app.dependency_overrides[get_service] = lambda: service

    async def run(action, timeout):
        assert timeout == 90
        return action("spark")

    app.state.spark_runtime = SimpleNamespace(run=run)

    with TestClient(app) as test_client:
        yield test_client, service


def test_prediction_is_calculated_from_ids_and_pinned_snapshot(client, monkeypatch):
    test_client, service = client

    prediction = {
        "cancellation_probability": 0.25,
        "predicted_class": 0,
        "model_id": "b" * 32,
        "status": "experimental",
        "business_validated": False,
    }

    result = contracts.ContractResult.model_validate(
        {**BODY, "snapshot": "c" * 32, "features": {"claim_count": 1}, "prediction": prediction}
    )

    calculate = Mock(return_value=result)
    monkeypatch.setattr(contracts, "predict_contract", calculate)

    response = test_client.post("/predict/contract", json=BODY)

    assert response.status_code == 200
    assert response.json()["prediction"] == prediction
    assert response.json()["features"]["claim_count"] == 1

    calculate.assert_called_once_with("spark", contracts.ContractInput.model_validate(BODY), service)


@pytest.mark.parametrize(
    "change",
    [
        {"contract_id": "invalid"},
        {"integration_id": "../latest"},
        {"claim_count": 999},
        {"observation_date": "invalid"},
    ],
)
def test_invalid_or_forged_features_never_reach_spark(client, monkeypatch, change):
    test_client, _ = client

    calculate = Mock()
    monkeypatch.setattr(contracts, "predict_contract", calculate)

    response = test_client.post("/predict/contract", json=BODY | change)

    assert response.status_code == 422
    calculate.assert_not_called()


def test_future_observation_is_refused(client, monkeypatch):
    test_client, _ = client

    calculate = Mock()
    monkeypatch.setattr(contracts, "predict_contract", calculate)

    future = (datetime.now(UTC).date() + timedelta(days=1)).isoformat()

    response = test_client.post("/predict/contract", json={**BODY, "observation_date": future})

    assert response.status_code == 422
    calculate.assert_not_called()


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (HTTPException(404, "Contract not found for this customer."), 404),
        (FileNotFoundError("secret"), 404),
        (TimeoutError("secret"), 504),
        (RuntimeError("secret"), 503),
    ],
)
def test_errors_are_safe_and_domain_errors_keep_their_status(client, monkeypatch, error, status):
    test_client, _ = client

    calculate = Mock(side_effect=error)
    monkeypatch.setattr(contracts, "predict_contract", calculate)

    response = test_client.post("/predict/contract", json=BODY)

    assert response.status_code == status
    assert "secret" not in response.text
