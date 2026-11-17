import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from abassurance.systems.data_api import consolidated, routes
from abassurance.systems.data_api.schemas import ConsolidatedResponse
from abassurance.systems.integration_api.schemas import IntegrationRun
from fastapi import FastAPI
from fastapi.testclient import TestClient


def store_run(directory, identifier, status, finished_at=None):
    run = IntegrationRun(
        id=identifier * 32,
        status=status,
        created_at=datetime.now(UTC),
        finished_at=finished_at,
        snapshot="f" * 32 if status == "succeeded" else None,
    )

    (directory / f"{run.id}.json").write_text(run.model_dump_json(), encoding="utf-8")

    return run


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(consolidated, "state_directory", lambda: tmp_path)

    app = FastAPI()
    app.include_router(routes.router)

    with TestClient(app) as test_client:
        yield test_client, tmp_path


def test_last_success_remains_readable_during_and_after_another_run(tmp_path, monkeypatch):
    monkeypatch.setattr(consolidated, "state_directory", lambda: tmp_path)

    old = store_run(tmp_path, "a", "succeeded", datetime.now(UTC) - timedelta(days=1))
    current = store_run(tmp_path, "b", "succeeded", datetime.now(UTC))

    store_run(tmp_path, "c", "running")
    store_run(tmp_path, "d", "failed", datetime.now(UTC) + timedelta(seconds=1))

    (tmp_path / "latest.json").write_text('{"status":"running"}', encoding="utf-8")

    assert consolidated.committed_run().id == current.id
    assert consolidated.committed_run(old.id).id == old.id

    with pytest.raises(FileNotFoundError):
        consolidated.committed_run("c" * 32)


def test_missing_snapshot_returns_404_without_initializing_spark(client):
    test_client, _ = client

    assert test_client.get("/data/consolidated/clients").status_code == 404


@pytest.mark.parametrize(
    "path",
    [
        "/data/consolidated/unknown",
        "/data/consolidated/clients?offset=-1",
        "/data/consolidated/clients?limit=0",
        "/data/consolidated/clients?limit=101",
        "/data/consolidated/clients?integration_id=../latest",
    ],
)
def test_page_parameters_are_validated_before_spark(client, path):
    test_client, _ = client

    assert test_client.get(path).status_code == 422


def test_page_is_pinned_before_runtime_access_and_is_not_cached(client, monkeypatch):
    test_client, directory = client
    run = store_run(directory, "a", "succeeded", datetime.now(UTC))

    assert run.snapshot is not None

    response = ConsolidatedResponse(
        entity="clients",
        integration_id=run.id,
        snapshot=run.snapshot,
        data_kind="demo",
        columns=["customer_id"],
        records=[],
        offset=25,
        limit=25,
        total=0,
    )

    read = Mock(return_value=response)
    monkeypatch.setattr(consolidated, "get_consolidated", read)

    spark = object()

    async def execute(action):
        store_run(directory, "b", "succeeded", datetime.now(UTC) + timedelta(seconds=1))
        return action(spark)

    test_client.app.state.spark_runtime = SimpleNamespace(run=execute)

    result = test_client.get("/data/consolidated/clients?offset=25&limit=25")

    assert result.status_code == 200
    assert result.headers["cache-control"] == "no-store"
    assert result.json() == response.model_dump()

    read.assert_called_once_with(spark, "clients", 25, 25, run.id)


@pytest.mark.parametrize(
    ("error", "status"), [(RuntimeError("private-host secret"), 503), (TimeoutError("private-host secret"), 504)]
)
def test_hdfs_errors_do_not_disclose_diagnostics(client, error, status):
    test_client, directory = client
    store_run(directory, "a", "succeeded", datetime.now(UTC))

    async def execute(action):
        raise error

    test_client.app.state.spark_runtime = SimpleNamespace(run=execute)

    response = test_client.get("/data/consolidated/payments")

    assert response.status_code == status
    assert "private-host" not in response.text
    assert "secret" not in response.text


def test_shared_spark_is_lazy_and_survives_an_action_failure(monkeypatch):
    from abassurance.shared.spark_runtime import SparkRuntime

    spark = Mock()
    create = Mock(return_value=spark)

    monkeypatch.setattr("abassurance.systems.spark_pipeline.run.create_session", create)

    runtime = SparkRuntime()
    create.assert_not_called()

    async def exercise():
        assert await runtime.run(lambda session: session) is spark

        with pytest.raises(RuntimeError):
            await runtime.run(Mock(side_effect=RuntimeError("Missing model")))

        assert await runtime.run(lambda session: session) is spark
        spark.stop.assert_not_called()

    try:
        asyncio.run(exercise())
    finally:
        runtime.close()

    create.assert_called_once_with(kafka=False)
    spark.stop.assert_called_once()


def test_contract_customer_filter_is_applied_inside_the_pinned_request(client, monkeypatch):
    test_client, directory = client
    run = store_run(directory, "a", "succeeded", datetime.now(UTC))
    assert run.snapshot is not None
    identifier = "0ce23490-7b17-57a3-933d-829abb01a605"
    result = ConsolidatedResponse(
        entity="contracts",
        integration_id=run.id,
        snapshot=run.snapshot,
        data_kind="demo",
        columns=[],
        records=[],
        offset=0,
        limit=25,
        total=0,
    )
    read = Mock(return_value=result)
    monkeypatch.setattr(consolidated, "get_consolidated", read)

    async def execute(action):
        return action("spark")

    test_client.app.state.spark_runtime = SimpleNamespace(run=execute)
    response = test_client.get(
        "/data/consolidated/contracts", params={"customer_id": identifier, "integration_id": run.id}
    )
    assert response.status_code == 200
    read.assert_called_once_with("spark", "contracts", 0, 25, run.id, customer_filter=identifier)
    assert test_client.get("/data/consolidated/clients", params={"customer_id": identifier}).status_code == 422
