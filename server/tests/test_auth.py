import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from abassurance.systems.auth import routes, service
from abassurance.systems.data_api.schemas import ConsolidatedResponse
from abassurance.systems.integration_api import routes as integration
from abassurance.systems.integration_api.schemas import IntegrationRun
from abassurance.systems.monitoring import app as web
from fastapi import Request, Response
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("AUTH_DB_PATH", str(tmp_path / "auth.sqlite"))
    monkeypatch.setattr(web, "recover_interrupted", lambda: None)

    service.create_user("admin", "admin-test-password", "admin")
    service.create_user("user", "user-test-password", "user", "abassurance", "100")

    with TestClient(web.create_app(), headers={"X-Requested-With": "AbAssurance"}) as client:
        yield client


def login(client, username="user"):
    return client.post("/auth/login", json={"username": username, "password": f"{username}-test-password"})


def test_login_without_client_address(client):
    request = Request({"type": "http", "headers": [], "client": None})
    response = Response()

    result = service.login(service.Credentials(username="user", password="user-test-password"), request, response)

    assert result["username"] == "user"
    assert "HttpOnly" in response.headers["set-cookie"]


def test_cookie_session_and_logout_are_real_and_passwords_hashed(client):
    assert client.get("/data/clients?source=oracle").status_code == 401

    response = client.post("/auth/login", json={"username": "user", "password": "wrong"})
    assert response.status_code == 401

    response = login(client)

    assert response.status_code == 200
    assert "HttpOnly" in response.headers["set-cookie"]
    assert "SameSite=strict" in response.headers["set-cookie"]

    cookie = client.cookies.get(service.COOKIE)

    with service.database() as db:
        password = db.execute("SELECT password FROM users WHERE username=?", ("user",)).fetchone()[0]

        session_token = db.execute("SELECT token FROM sessions").fetchone()[0]

    assert password != "user-test-password"
    assert session_token != cookie

    assert client.get("/auth/me").json()["role"] == "user"
    assert client.post("/auth/logout").status_code == 200

    client.cookies.set(service.COOKIE, cookie)

    assert client.get("/auth/me").status_code == 401


@pytest.mark.parametrize(
    "path",
    [
        "/pipeline/services",
        "/pipeline/integrations/latest",
        "/data/clients?source=oracle",
        "/data/consolidated/clients",
        "/model",
        "/health",
    ],
)
def test_user_cannot_read_admin_endpoints(client, path):
    login(client)

    assert client.get(path).status_code == 403


def test_user_cannot_launch_integration_even_with_a_bearer_key(client):
    login(client)

    assert client.post("/pipeline/integrations", headers={"Authorization": f"Bearer {'a' * 64}"}).status_code == 403

    assert client.post("/predict", json={}).status_code == 403
    assert client.post("/predict/contract", json={}).status_code == 403


def test_admin_reaches_application_and_user_identity_is_not_supplied_by_query(client, monkeypatch):
    login(client, "admin")

    assert client.get("/data/clients?source=invalid").status_code == 422

    client.post("/auth/logout")
    login(client)

    result = ConsolidatedResponse(
        entity="clients",
        integration_id="a" * 32,
        snapshot="b" * 32,
        data_kind="demo",
        columns=[],
        records=[],
        offset=0,
        limit=25,
        total=0,
    )

    read = Mock(return_value=result)
    monkeypatch.setattr(routes, "get_consolidated", read)

    async def run(action):
        return action("spark")

    client.app.state.spark_runtime = SimpleNamespace(run=run, close=Mock())

    response = client.get("/me/data/clients?source=assureplus&source_id=101&role=admin")

    assert response.status_code == 200
    read.assert_called_once_with("spark", "clients", 0, 25, None, owner=("abassurance", "100"))


def test_expiry_csrf_and_failed_login_throttling(client):
    response = client.post(
        "/auth/login", json={"username": "user", "password": "user-test-password"}, headers={"X-Requested-With": ""}
    )

    assert response.status_code == 403

    login(client)

    with service.database() as db:
        db.execute("UPDATE sessions SET expires=?", (time.time() - 1,))
        db.commit()

    assert client.get("/auth/me").status_code == 401

    for _ in range(10):
        response = client.post("/auth/login", json={"username": "missing", "password": "wrong"})
        assert response.status_code == 401

    response = client.post("/auth/login", json={"username": "missing", "password": "wrong"})

    assert response.status_code == 429


def test_admin_session_authorizes_integration_without_shared_key(client, monkeypatch):
    run = IntegrationRun(id="a" * 32, status="queued", created_at=datetime.now(UTC))

    monkeypatch.setattr(integration, "create_run", lambda: run)

    execute = Mock()
    monkeypatch.setattr(integration, "execute_run", execute)

    login(client, "admin")

    response = client.post("/pipeline/integrations")

    assert response.status_code == 202
    execute.assert_called_once_with(run.id)
