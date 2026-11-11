from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from extraction import oracle, sqlserver
from extraction.common import ExtractionError
from extraction.config import Settings


@pytest.mark.parametrize("module", [oracle, sqlserver])
def test_unknown_entity_never_opens_a_connection(module, monkeypatch) -> None:
    connector = MagicMock()
    monkeypatch.setattr(module, f"connect_{module.__name__.split('.')[-1]}", connector)
    with pytest.raises(ValueError, match="Unknown entity"):
        getattr(module, f"fetch_{module.__name__.split('.')[-1]}")("customers; DROP TABLE")
    connector.assert_not_called()


@pytest.mark.parametrize("module,error", [(oracle, oracle.oracledb.DatabaseError), (sqlserver, sqlserver.pyodbc.Error)])
def test_read_failure_closes_cursor_and_connection(module, error, monkeypatch) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = error("database read error")
    name = module.__name__.split(".")[-1]
    monkeypatch.setattr(module, f"connect_{name}", lambda *args, **kwargs: connection)
    with pytest.raises(ExtractionError, match="extraction failed for customers"):
        getattr(module, f"fetch_{name}")("customers")
    cursor.close.assert_called_once()
    connection.close.assert_called_once()


def test_clob_is_materialized_before_resources_close(monkeypatch) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value
    lob = MagicMock(spec=oracle.oracledb.LOB)

    def read() -> str:
        connection.close.assert_not_called()
        cursor.close.assert_not_called()
        return "Description fictive"

    lob.read.side_effect = read
    cursor.description = [("AB_DESCRIPTION",)]
    cursor.__iter__.return_value = iter([(lob,)])
    monkeypatch.setattr(oracle, "connect_oracle", lambda config: connection)
    assert oracle.fetch_oracle("claims") == [{"AB_DESCRIPTION": "Description fictive"}]
    connection.close.assert_called_once()


def test_sqlserver_escapes_credentials_and_honors_config(settings: Settings, monkeypatch):
    connector = MagicMock()
    monkeypatch.setattr(sqlserver.pyodbc, "drivers", lambda: [settings.sqlserver.driver])
    monkeypatch.setattr(sqlserver.pyodbc, "connect", connector)
    config = replace(settings.sqlserver, password="a};b{!", port=15433, timeout=7)
    sqlserver.connect_sqlserver(config=config)
    arguments, keywords = connector.call_args
    assert "PWD={a}};b{!}" in arguments[0]
    assert "SERVER={sqlserver,15433}" in arguments[0]
    assert keywords == {"autocommit": False, "timeout": 7}


def test_missing_odbc_driver_has_actionable_error(settings: Settings, monkeypatch):
    monkeypatch.setattr(sqlserver.pyodbc, "drivers", lambda: [])
    with pytest.raises(ExtractionError, match="ODBC driver is not installed"):
        sqlserver.connect_sqlserver(config=settings.sqlserver)
