from functools import partial

import pyodbc

from extraction.common import extract_records
from extraction.config import SQLServerConfig, load_settings
from shared.errors import ExtractionError, ensure, error_context
from shared.types import Record

SQLSERVER_TABLES = {
    "customers": ("dbo.AP_USERS", "AP_USER_ID"),
    "contracts": ("dbo.AP_CONTRACTS", "AP_CONTRACT_REF"),
    "claims": ("dbo.AP_CLAIMS", "AP_SINISTRE_NUM"),
    "payments": ("dbo.AP_PAYMENTS", "AP_PAYMENT_REF"),
}


def _odbc_value(value: str) -> str:
    return "{" + value.replace("}", "}}") + "}"


def connect_sqlserver(
    database: str | None = None, *, config: SQLServerConfig | None = None, autocommit: bool = False
) -> pyodbc.Connection:
    config = config if (config is not None) else load_settings().sqlserver

    attributes = {
        "DRIVER": config.driver,
        "SERVER": f"{config.host},{config.port}",
        "DATABASE": database if (database is not None) else config.database,
        "UID": config.user,
        "PWD": config.password,
        "Encrypt": "yes" if (config.encrypt) else "no",
        "TrustServerCertificate": "yes" if (config.trust_server_certificate) else "no",
    }

    connection_string = ";".join(f"{key}={_odbc_value(value)}" for key, value in attributes.items())

    ensure(
        config.driver in pyodbc.drivers(), f"SQL Server ODBC driver is not installed: {config.driver}", ExtractionError
    )

    message = "SQL Server connection failed; check configuration and database readiness"

    with error_context(message, (pyodbc.Error,), ExtractionError):
        connection = pyodbc.connect(connection_string, autocommit=autocommit, timeout=config.timeout)
        connection.timeout = config.timeout

        return connection


def fetch_sqlserver(entity: str, config: SQLServerConfig | None = None) -> list[Record]:
    with error_context(f"SQL Server extraction failed for {entity}", (pyodbc.Error,), ExtractionError):
        return extract_records(entity, SQLSERVER_TABLES, partial(connect_sqlserver, config=config))
