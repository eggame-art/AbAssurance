from datetime import date, datetime
from typing import get_type_hints

from models import (
    OracleClaim,
    OracleContract,
    OracleCustomer,
    OraclePayment,
    SQLServerClaim,
    SQLServerContract,
    SQLServerCustomer,
    SQLServerPayment,
)

SOURCE_MODELS = {
    "abassurance": {
        "customers": OracleCustomer,
        "contracts": OracleContract,
        "claims": OracleClaim,
        "payments": OraclePayment,
    },
    "assureplus": {
        "customers": SQLServerCustomer,
        "contracts": SQLServerContract,
        "claims": SQLServerClaim,
        "payments": SQLServerPayment,
    },
}
DATE_FIELDS = {
    source: {
        entity: tuple(field for field, kind in get_type_hints(model).items() if kind in (date, datetime))
        for entity, model in entities.items()
    }
    for source, entities in SOURCE_MODELS.items()
}


def restore_dates(record: dict, source: str, entity: str) -> dict:
    restored = record.copy()
    for field in DATE_FIELDS[source][entity]:
        value = record.get(field)
        if isinstance(value, str):
            restored[field] = datetime.fromisoformat(value.strip())

    return restored
