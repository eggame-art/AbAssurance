from threading import Event
from unittest.mock import Mock

import pytest
from abassurance.shared.environment import Environment
from abassurance.shared.errors import ConfigurationError, ExtractionError
from abassurance.systems.data_api import routes, service
from abassurance.systems.data_api.schemas import ClaimsResponse, ClientsResponse, ContractsResponse, PaymentsResponse
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(routes.router)

    with TestClient(app) as client:
        yield client


@pytest.fixture
def row():
    return {
        "source_id": "100",
        "name": "Source Name",
        "first_name": None,
        "email": None,
        "phone": "0600000000",
        "status": "ACTIF",
    }


@pytest.mark.parametrize("source", ["oracle", "sqlserver"])
def test_read_endpoint_preserves_values_and_has_no_cache(client, monkeypatch, row, source):
    read = Mock(return_value=ClientsResponse(source=source, data_kind="demo", clients=[row]))
    monkeypatch.setattr(routes, "get_clients", read)

    response = client.get("/data/clients", params={"source": source})

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"source": source, "data_kind": "demo", "clients": [row]}

    read.assert_called_once_with(source)

    assert client.post("/data/clients", params={"source": source}).status_code == 405


@pytest.mark.parametrize("source", [None, "consolidated", "oracle; DROP TABLE AB_CLIENT", ""])
def test_only_supported_sources_can_reach_database(client, monkeypatch, source):
    read = Mock()
    monkeypatch.setattr(routes, "get_clients", read)

    response = client.get("/data/clients", params={} if source is None else {"source": source})

    assert response.status_code == 422

    read.assert_not_called()


def test_empty_database_is_not_replaced_with_demo_rows(client, monkeypatch):
    monkeypatch.setattr(
        routes, "get_clients", lambda source: ClientsResponse(source=source, data_kind="unspecified", clients=[])
    )

    assert client.get("/data/clients?source=oracle").json()["clients"] == []


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (ConfigurationError("private-password private-host"), 503),
        (ImportError("private-password private-host"), 503),
        (ExtractionError("private-password private-host"), 502),
        (RuntimeError("private-password private-host"), 502),
        (TimeoutError("private-password private-host"), 504),
    ],
)
def test_errors_are_public_and_another_source_remains_readable(client, monkeypatch, error, status):
    def read(source):
        if source == "oracle":
            raise error

        return ClientsResponse(source=source, data_kind="demo", clients=[])

    monkeypatch.setattr(routes, "get_clients", read)

    response = client.get("/data/clients?source=oracle")

    assert response.status_code == status
    assert "Oracle" in response.json()["detail"]
    assert "private-password" not in response.text
    assert "private-host" not in response.text
    assert client.get("/data/clients?source=sqlserver").status_code == 200


def test_slow_source_has_a_bounded_response(client, monkeypatch):
    release = Event()
    monkeypatch.setattr(routes, "READ_TIMEOUT", 0.02)
    monkeypatch.setattr(routes, "get_clients", lambda source: release.wait(2))

    try:
        assert client.get("/data/clients?source=oracle").status_code == 504
    finally:
        release.set()


@pytest.mark.parametrize("source", ["oracle", "sqlserver"])
@pytest.mark.parametrize("fail", [False, True])
def test_selected_connector_is_read_only_bounded_and_closed(monkeypatch, row, source, fail):
    env = Environment(
        {
            "ORACLE_APP_PASSWORD" if source == "oracle" else "SQLSERVER_PASSWORD": "test-secret",
            "SOURCE_DATA_KIND": "demo",
        }
    )
    monkeypatch.setattr(service.Environment, "from_file", lambda path: env)
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.description = [(field,) for field in row]
    cursor.__iter__ = Mock(return_value=iter([tuple(row.values())]))

    if fail:
        cursor.execute.side_effect = RuntimeError("read error")

    connect = Mock(return_value=connection)
    monkeypatch.setattr(f"abassurance.systems.data_integration.extraction.{source}.connect_{source}", connect)

    if fail:
        with pytest.raises(RuntimeError):
            service.get_clients(source)
    else:
        result = service.get_clients(source)
        assert result.clients[0].model_dump() == row
        assert result.source == source

    query = cursor.execute.call_args.args[0]

    assert query.lstrip().split()[0] == "SELECT"
    assert "SELECT *" not in query
    assert "ORDER BY" in query
    assert "AB_NUM_FISCAL" not in query

    config = connect.call_args.args[0] if source == "oracle" else connect.call_args.kwargs["config"]

    assert config.timeout == 5

    if source == "oracle":
        assert connection.call_timeout == 5000

    cursor.close.assert_called_once()
    connection.close.assert_called_once()


def test_original_ids_are_strings_not_javascript_numbers(monkeypatch, row):
    row["source_id"] = "9007199254740993"
    monkeypatch.setattr(
        routes, "get_clients", lambda source: ClientsResponse(source=source, data_kind="demo", clients=[row])
    )
    app = FastAPI()
    app.include_router(routes.router)

    with TestClient(app) as client:
        assert client.get("/data/clients?source=oracle").json()["clients"][0]["source_id"] == "9007199254740993"


@pytest.fixture
def contract_row():
    return {
        "source_reference": "SOURCE-001",
        "source_client_id": "9007199254740993",
        "product_type": "AUTO",
        "start_date": None,
        "end_date": None,
        "premium": "12345678.90",
        "premium_period": "annual",
        "status": None,
    }


@pytest.mark.parametrize("source", ["oracle", "sqlserver"])
def test_contract_response_preserves_reference_client_amount_and_period(client, monkeypatch, contract_row, source):
    contract_row["premium_period"] = "annual" if source == "oracle" else "monthly"
    read = Mock(return_value=ContractsResponse(source=source, data_kind="demo", contracts=[contract_row]))
    monkeypatch.setattr(routes, "get_contracts", read)

    response = client.get("/data/contracts", params={"source": source})

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"source": source, "data_kind": "demo", "contracts": [contract_row]}

    read.assert_called_once_with(source)

    assert client.post("/data/contracts", params={"source": source}).status_code == 405


@pytest.mark.parametrize("source", [None, "consolidated", "oracle; DROP TABLE AB_CONTRAT"])
def test_contract_source_is_validated_before_read(client, monkeypatch, source):
    read = Mock()
    monkeypatch.setattr(routes, "get_contracts", read)

    response = client.get("/data/contracts", params={} if source is None else {"source": source})

    assert response.status_code == 422

    read.assert_not_called()


def test_empty_contracts_remain_empty(client, monkeypatch):
    monkeypatch.setattr(
        routes, "get_contracts", lambda source: ContractsResponse(source=source, data_kind="demo", contracts=[])
    )

    assert client.get("/data/contracts?source=sqlserver").json()["contracts"] == []


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ConfigurationError("secret password and host"), 503),
        (ImportError("secret password and host"), 503),
        (RuntimeError("secret password and host"), 502),
        (TimeoutError("secret password and host"), 504),
    ],
)
def test_contract_errors_do_not_expose_diagnostics(client, monkeypatch, error, expected):
    monkeypatch.setattr(routes, "get_contracts", Mock(side_effect=error))

    response = client.get("/data/contracts?source=oracle")

    assert response.status_code == expected
    assert "secret" not in response.text
    assert "Oracle" in response.json()["detail"]


@pytest.mark.parametrize("source", ["oracle", "sqlserver"])
@pytest.mark.parametrize("fail", [False, True])
def test_contract_query_is_selected_and_resources_are_closed(monkeypatch, contract_row, source, fail):
    env = Environment({"ORACLE_APP_PASSWORD" if source == "oracle" else "SQLSERVER_PASSWORD": "test-secret"})
    monkeypatch.setattr(service.Environment, "from_file", lambda path: env)
    contract_row["premium_period"] = "annual" if source == "oracle" else "monthly"
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.description = [(field,) for field in contract_row]
    cursor.__iter__ = Mock(return_value=iter([tuple(contract_row.values())]))

    if fail:
        cursor.execute.side_effect = RuntimeError("read failure")

    connector = Mock(return_value=connection)
    monkeypatch.setattr(f"abassurance.systems.data_integration.extraction.{source}.connect_{source}", connector)

    if fail:
        with pytest.raises(RuntimeError):
            service.get_contracts(source)
    else:
        assert service.get_contracts(source).contracts[0].model_dump() == contract_row

    query = cursor.execute.call_args.args[0]

    assert query.lstrip().split()[0] == "SELECT"
    assert ("AB_CONTRAT" if source == "oracle" else "dbo.AP_CONTRACTS") in query
    assert "ORDER BY" in query and "SELECT *" not in query

    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.fixture
def claim_row():
    return {
        "source_id": "9007199254740993",
        "source_contract_reference": "SOURCE-001",
        "incident_date": "15/03/2025 09:45:00",
        "estimated_amount": "1234567890.12",
        "status": None,
        "description": "Original source description\npreserved in full.",
    }


@pytest.mark.parametrize("source", ["oracle", "sqlserver"])
def test_claims_preserve_source_values_and_contract_reference(client, monkeypatch, claim_row, source):
    read = Mock(return_value=ClaimsResponse(source=source, data_kind="demo", claims=[claim_row]))
    monkeypatch.setattr(routes, "get_claims", read)

    response = client.get("/data/claims", params={"source": source})

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"source": source, "data_kind": "demo", "claims": [claim_row]}

    read.assert_called_once_with(source)

    assert client.post("/data/claims", params={"source": source}).status_code == 405


@pytest.mark.parametrize("source", [None, "consolidated", "oracle; DROP TABLE AB_SINISTRE"])
def test_claims_validate_source_before_read(client, monkeypatch, source):
    read = Mock()
    monkeypatch.setattr(routes, "get_claims", read)

    response = client.get("/data/claims", params={} if source is None else {"source": source})

    assert response.status_code == 422

    read.assert_not_called()


def test_empty_claims_remain_empty(client, monkeypatch):
    monkeypatch.setattr(routes, "get_claims", lambda source: ClaimsResponse(source=source, data_kind="demo", claims=[]))

    assert client.get("/data/claims?source=oracle").json()["claims"] == []


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (ConfigurationError("private password and host"), 503),
        (ImportError("private password and host"), 503),
        (RuntimeError("private password and host"), 502),
        (TimeoutError("private password and host"), 504),
    ],
)
def test_claim_read_errors_are_public_and_source_independent(client, monkeypatch, error, status):
    def read(source):
        if source == "oracle":
            raise error

        return ClaimsResponse(source=source, data_kind="demo", claims=[])

    monkeypatch.setattr(routes, "get_claims", read)

    response = client.get("/data/claims?source=oracle")

    assert response.status_code == status
    assert "private" not in response.text
    assert client.get("/data/claims?source=sqlserver").status_code == 200


@pytest.mark.parametrize("source", ["oracle", "sqlserver"])
@pytest.mark.parametrize("fail", [False, True])
def test_claim_query_is_selected_and_resources_are_closed(monkeypatch, claim_row, source, fail):
    env = Environment({"ORACLE_APP_PASSWORD" if source == "oracle" else "SQLSERVER_PASSWORD": "test-secret"})
    monkeypatch.setattr(service.Environment, "from_file", lambda path: env)
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.description = [(field,) for field in claim_row]
    cursor.__iter__ = Mock(return_value=iter([tuple(claim_row.values())]))

    if fail:
        cursor.execute.side_effect = RuntimeError("query failed")

    connect = Mock(return_value=connection)
    monkeypatch.setattr(f"abassurance.systems.data_integration.extraction.{source}.connect_{source}", connect)

    if fail:
        with pytest.raises(RuntimeError):
            service.get_claims(source)
    else:
        assert service.get_claims(source).claims[0].model_dump() == claim_row

    query = cursor.execute.call_args.args[0]

    assert query.lstrip().split()[0] == "SELECT"
    assert ("AB_SINISTRE" if source == "oracle" else "dbo.AP_CLAIMS") in query
    assert "SELECT *" not in query and "ORDER BY" in query

    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize("fail", [False, True])
def test_oracle_description_is_materialized_in_full_before_close(monkeypatch, claim_row, fail):
    import oracledb

    monkeypatch.setattr(
        service.Environment, "from_file", lambda path: Environment({"ORACLE_APP_PASSWORD": "test-secret"})
    )
    connection = Mock()
    cursor = connection.cursor.return_value
    description = "Source text\n" * 1000
    lob = Mock(spec=oracledb.LOB)

    def read():
        connection.close.assert_not_called()

        cursor.close.assert_not_called()

        if fail:
            raise RuntimeError("LOB read failed")

        return description

    lob.read.side_effect = read
    claim_row["description"] = lob
    cursor.description = [(field,) for field in claim_row]
    cursor.__iter__ = Mock(return_value=iter([tuple(claim_row.values())]))
    monkeypatch.setattr(
        "abassurance.systems.data_integration.extraction.oracle.connect_oracle", lambda config: connection
    )

    if fail:
        with pytest.raises(RuntimeError):
            service.get_claims("oracle")
    else:
        assert service.get_claims("oracle").claims[0].description == description

    lob.read.assert_called_once()

    cursor.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.fixture
def payment_row():
    return {
        "source_id": "00000000-0000-0000-0000-000000002000",
        "source_contract_reference": "SOURCE-001",
        "payment_date": "2025-01-05T10:00:00",
        "amount": "1234567890.12",
        "method": "BANK_TRANSFER",
        "status": None,
    }


@pytest.mark.parametrize("source", ["oracle", "sqlserver"])
def test_payments_preserve_exact_amount_uuid_and_contract(client, monkeypatch, payment_row, source):
    read = Mock(return_value=PaymentsResponse(source=source, data_kind="demo", payments=[payment_row]))
    monkeypatch.setattr(routes, "get_payments", read)

    response = client.get("/data/payments", params={"source": source})

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"source": source, "data_kind": "demo", "payments": [payment_row]}

    read.assert_called_once_with(source)


@pytest.mark.parametrize("source", [None, "consolidated", "sqlserver; DROP TABLE AP_PAYMENTS"])
def test_payment_source_validation(client, monkeypatch, source):
    read = Mock()
    monkeypatch.setattr(routes, "get_payments", read)

    assert client.get("/data/payments", params={} if source is None else {"source": source}).status_code == 422

    read.assert_not_called()


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (ConfigurationError("private-secret"), 503),
        (ImportError("private-secret"), 503),
        (RuntimeError("private-secret"), 502),
        (TimeoutError("private-secret"), 504),
    ],
)
def test_payment_errors_are_safe_and_other_source_remains_available(client, monkeypatch, error, status):
    def read(source):
        if source == "oracle":
            raise error

        return PaymentsResponse(source=source, data_kind="demo", payments=[])

    monkeypatch.setattr(routes, "get_payments", read)

    response = client.get("/data/payments?source=oracle")

    assert response.status_code == status
    assert "private-secret" not in response.text
    assert client.get("/data/payments?source=sqlserver").json()["payments"] == []


@pytest.mark.parametrize("source", ["oracle", "sqlserver"])
@pytest.mark.parametrize("fail", [False, True])
def test_payment_query_is_read_only_and_closes_resources(monkeypatch, payment_row, source, fail):
    env = Environment({"ORACLE_APP_PASSWORD" if source == "oracle" else "SQLSERVER_PASSWORD": "test-secret"})
    monkeypatch.setattr(service.Environment, "from_file", lambda path: env)
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.description = [(field,) for field in payment_row]
    cursor.__iter__ = Mock(return_value=iter([tuple(payment_row.values())]))

    if fail:
        cursor.execute.side_effect = RuntimeError("query failed")

    monkeypatch.setattr(
        f"abassurance.systems.data_integration.extraction.{source}.connect_{source}", lambda *args, **kwargs: connection
    )

    if fail:
        with pytest.raises(RuntimeError):
            service.get_payments(source)
    else:
        assert service.get_payments(source).payments[0].model_dump() == payment_row

    query = cursor.execute.call_args.args[0]

    assert query.lstrip().split()[0] == "SELECT"
    assert ("AB_PAIEMENT" if source == "oracle" else "dbo.AP_PAYMENTS") in query
    assert "SELECT *" not in query and "ORDER BY" in query

    cursor.close.assert_called_once()
    connection.close.assert_called_once()
