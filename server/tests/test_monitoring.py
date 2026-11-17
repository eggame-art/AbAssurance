import json
from io import BytesIO
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from abassurance.shared.environment import Environment
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.integration_api import service as integrations
from abassurance.systems.monitoring import service
from abassurance.systems.monitoring.app import create_app
from abassurance.systems.prediction_api.app import create_app as create_prediction_app
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def integration_storage(monkeypatch, tmp_path):
    monkeypatch.setattr(integrations, "state_directory", lambda: tmp_path / "runs")


@pytest.fixture
def env():
    return Environment({"ORACLE_APP_PASSWORD": "private-oracle-password", "SQLSERVER_PASSWORD": "private-sql-password"})


@pytest.fixture
def probes(monkeypatch, env):
    monkeypatch.setattr(Environment, "from_file", lambda _: env)

    checks = {}

    for name in ("oracle", "sqlserver", "kafka", "hdfs"):
        checks[name] = Mock(return_value=None)
        monkeypatch.setattr(service, f"check_{name}", checks[name])

    for name in ("spark", "etl"):
        monkeypatch.setattr(
            service, f"check_{name}", Mock(side_effect=service.CheckNotConfigured("Check not performed."))
        )

    return checks


def test_endpoint_is_independent_of_prediction_and_returns_read_only_statuses(probes, monkeypatch):
    loader = Mock(side_effect=AssertionError("Monitoring must not load a predictive model"))
    monkeypatch.setattr("abassurance.systems.prediction_api.app.load_service", loader)

    with TestClient(create_app()) as client:
        response = client.get("/pipeline/services")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"

    rows = response.json()

    assert [row["id"] for row in rows] == ["oracle", "sqlserver", "kafka", "spark", "hdfs", "etl"]
    assert [row["status"] for row in rows] == [
        "available",
        "available",
        "available",
        "not_checked",
        "available",
        "not_checked",
    ]
    assert all(row["message"] is None for row in rows if row["status"] == "available")

    for check in probes.values():
        check.assert_called_once()

    loader.assert_not_called()
    assert "/pipeline/services" in create_prediction_app().openapi()["paths"]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (RuntimeError("private-password at internal-host"), "unavailable"),
        (TimeoutError("private-password at internal-host"), "unavailable"),
        (ConfigurationError("private-password at internal-host"), "not_checked"),
        (ImportError("private-password at internal-host"), "not_checked"),
        (service.CheckNotConfigured("private-password at internal-host"), "not_checked"),
    ],
)
def test_failure_does_not_hide_other_results_or_expose_diagnostics(probes, error, expected):
    probes["oracle"].side_effect = error

    with TestClient(create_app()) as client:
        response = client.get("/pipeline/services")

    assert response.status_code == 200

    rows = {row["id"]: row for row in response.json()}

    assert rows["oracle"]["status"] == expected
    assert all(rows[name]["status"] == "available" for name in ("sqlserver", "kafka", "hdfs"))
    assert "private-password" not in response.text
    assert "internal-host" not in response.text


def test_timeout_returns_other_services_without_waiting_for_blocked_check(probes, monkeypatch):
    release = Event()

    probes["oracle"].side_effect = lambda _: release.wait(2)
    monkeypatch.setattr(service, "CHECK_TIMEOUT", 0.05)

    with TestClient(create_app()) as client:
        try:
            response = client.get("/pipeline/services")
            rows = {row["id"]: row for row in response.json()}

            assert rows["oracle"]["status"] == "unavailable"
            assert not release.is_set()
            assert all(rows[name]["status"] == "available" for name in ("sqlserver", "kafka", "hdfs"))
        finally:
            release.set()


def test_each_refresh_runs_new_checks_and_no_action_route_exists(probes):
    with TestClient(create_app()) as client:
        assert client.get("/pipeline/services").json()[0]["status"] == "available"

        probes["oracle"].side_effect = RuntimeError("database unavailable")

        assert client.get("/pipeline/services").json()[0]["status"] == "unavailable"
        assert client.post("/pipeline/services").status_code == 405
        assert "/pipeline/services" in client.get("/openapi.json").json()["paths"]

    assert all(check.call_count == 2 for check in probes.values())


def test_oracle_executes_query_sets_timeout_and_closes_connection(env, monkeypatch):
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock()

    cursor = Mock()
    cursor.__enter__ = Mock(return_value=cursor)
    cursor.__exit__ = Mock()

    connection.cursor.return_value = cursor
    cursor.fetchone.return_value = (1,)

    connect = Mock(return_value=connection)
    monkeypatch.setattr("abassurance.systems.data_integration.extraction.oracle.connect_oracle", connect)

    service.check_oracle(env)

    cursor.execute.assert_called_once_with("SELECT 1 FROM DUAL")
    assert connection.call_timeout == 5000
    assert connect.call_args.args[0].timeout == 5

    connection.__exit__.assert_called_once()
    cursor.__exit__.assert_called_once()


def test_sqlserver_executes_query_and_closes_resources(env, monkeypatch):
    connection = Mock()
    connection.cursor.return_value.fetchone.return_value = (1,)

    connect = Mock(return_value=connection)
    monkeypatch.setattr("pyodbc.drivers", lambda: ["ODBC Driver 18 for SQL Server"])
    monkeypatch.setattr("abassurance.systems.data_integration.extraction.sqlserver.connect_sqlserver", connect)

    service.check_sqlserver(env)

    connection.cursor.return_value.execute.assert_called_once_with("SELECT 1")
    assert connect.call_args.kwargs["config"].timeout == 5

    connection.cursor.return_value.close.assert_called_once()
    connection.close.assert_called_once()


def test_sqlserver_missing_driver_is_not_a_database_outage(env, monkeypatch):
    monkeypatch.setattr("pyodbc.drivers", lambda: [])

    with pytest.raises(service.CheckNotConfigured, match="ODBC"):
        service.check_sqlserver(env)


def test_kafka_reads_metadata_without_creating_a_topic(env, monkeypatch):
    admin = Mock()
    admin.return_value.list_topics.return_value = SimpleNamespace(brokers={1: object()})

    monkeypatch.setattr("confluent_kafka.admin.AdminClient", admin)

    service.check_kafka(env)

    admin.return_value.list_topics.assert_called_once_with(timeout=5)
    admin.return_value.create_topics.assert_not_called()

    assert admin.call_args.args[0]["socket.timeout.ms"] == 5000

    admin.return_value.list_topics.return_value.brokers = {}

    with pytest.raises(ValueError):
        service.check_kafka(env)


@pytest.mark.parametrize(
    ("root", "beans", "valid"),
    [
        ({"FileStatus": {"type": "DIRECTORY"}}, {"beans": [{"NumLiveDataNodes": 1}]}, True),
        ({"FileStatus": {"type": "DIRECTORY"}}, {"beans": [{"NumLiveDataNodes": 0}]}, False),
        ({"FileStatus": {"type": "DIRECTORY"}}, {"beans": []}, False),
        ({"FileStatus": {"type": "FILE"}}, {"beans": [{"NumLiveDataNodes": 1}]}, False),
        ({}, {}, False),
    ],
)
def test_hdfs_requires_metadata_read_and_live_datanode(env, monkeypatch, root, beans, valid):
    opener = Mock(side_effect=[BytesIO(json.dumps(payload).encode()) for payload in (root, beans)])
    monkeypatch.setattr(service, "urlopen", opener)

    if valid:
        service.check_hdfs(env)

        assert "/webhdfs/v1/?op=GETFILESTATUS" in opener.call_args_list[0].args[0]
        assert "FSNamesystemState" in opener.call_args_list[1].args[0]
        assert all(call.kwargs["timeout"] == 5 for call in opener.call_args_list)
    else:
        with pytest.raises((ValueError, KeyError)):
            service.check_hdfs(env)


@pytest.mark.parametrize(
    "url", ["file:///etc/passwd", "http://user:password@host", "http://host/path", "http://host/?x=1"]
)
def test_invalid_hdfs_configuration_never_sends_request(monkeypatch, url):
    opener = Mock()
    monkeypatch.setattr(service, "urlopen", opener)

    with pytest.raises(ConfigurationError):
        service.check_hdfs(Environment({"HDFS_HTTP_URL": url}))

    opener.assert_not_called()


@pytest.fixture(autouse=True)
def isolate_route_contracts_from_authentication(monkeypatch):
    async def passthrough(request, call_next):
        return await call_next(request)

    monkeypatch.setattr("abassurance.systems.monitoring.app.protect", passthrough)
    monkeypatch.setattr("abassurance.systems.prediction_api.app.protect", passthrough)
