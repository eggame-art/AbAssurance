from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from abassurance.systems.data_api import routes as data_routes
from abassurance.systems.data_api.schemas import ClientsResponse
from abassurance.systems.monitoring import app as web
from fastapi.testclient import TestClient


def test_missing_model_does_not_block_source_data_or_startup(monkeypatch):
    monkeypatch.setattr(web, "recover_interrupted", lambda: None)
    monkeypatch.setattr(web, "load_service", Mock(side_effect=RuntimeError("private-host secret")))
    monkeypatch.setattr(
        data_routes, "get_clients", lambda source: ClientsResponse(source=source, data_kind="demo", clients=[])
    )

    async def run(action):
        return action(object())

    runtime = SimpleNamespace(run=run, close=Mock())
    monkeypatch.setattr(web, "SparkRuntime", lambda: runtime)

    with TestClient(web.create_app()) as client:
        result = client.get("/model")

        assert result.status_code == 503
        assert "private-host" not in result.text
        assert "secret" not in result.text

        assert client.get("/data/clients?source=oracle").status_code == 200
        assert client.get("/openapi.json").status_code == 200

    runtime.close.assert_called_once()


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e1000"])
def test_web_api_keeps_prediction_validation_serializable(monkeypatch, token):
    from abassurance.systems.prediction_api.routes import get_service

    monkeypatch.setattr(web, "recover_interrupted", lambda: None)

    app = web.create_app()
    app.dependency_overrides[get_service] = lambda: Mock()

    with TestClient(app) as client:
        response = client.post(
            "/predict",
            content=f'{{"payment_amount_known_total":{token}}}',
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 422
        assert response.json()["detail"][0]["loc"] == ["body", "payment_amount_known_total"]


@pytest.fixture(autouse=True)
def isolate_route_contracts_from_authentication(monkeypatch):
    async def passthrough(request, call_next):
        return await call_next(request)

    monkeypatch.setattr("abassurance.systems.monitoring.app.protect", passthrough)
    monkeypatch.setattr("abassurance.systems.prediction_api.app.protect", passthrough)
