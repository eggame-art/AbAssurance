from decimal import Decimal
from functools import partial
from typing import Any

import oracledb

from extraction.common import extract_records
from extraction.config import OracleConfig, load_settings
from shared.errors import ExtractionError, error_context
from shared.types import Record

ORACLE_TABLES = {
    "customers": ("AB_CLIENT", "AB_CLIENT_ID"),
    "contracts": ("AB_CONTRAT", "AB_POLICY_NUMBER"),
    "claims": ("AB_SINISTRE", "AB_CLAIM_ID"),
    "payments": ("AB_PAIEMENT", "AB_PAYMENT_ID"),
}


def connect_oracle(config: OracleConfig | None = None) -> oracledb.Connection:
    config = config if config is not None else load_settings().oracle
    message = "Oracle connection failed; check configuration and database readiness"

    with error_context(message, (oracledb.Error,), ExtractionError):
        return oracledb.connect(
            user=config.user, password=config.password, dsn=config.dsn, tcp_connect_timeout=config.timeout
        )


def _number_handler(cursor: oracledb.Cursor, metadata: Any) -> Any:
    if metadata.type_code == oracledb.DB_TYPE_NUMBER and metadata.scale > 0:
        return cursor.var(Decimal, arraysize=cursor.arraysize)

    return None


def _materialize(value: Any) -> Any:
    return value.read() if isinstance(value, oracledb.LOB) else value


def fetch_oracle(entity: str, config: OracleConfig | None = None) -> list[Record]:
    with error_context(f"Oracle extraction failed for {entity}", (oracledb.Error,), ExtractionError):
        return extract_records(
            entity,
            ORACLE_TABLES,
            partial(connect_oracle, config),
            convert=_materialize,
            output_type_handler=_number_handler,
        )
