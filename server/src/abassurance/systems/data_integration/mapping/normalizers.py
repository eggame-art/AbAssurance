from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from unicodedata import normalize

from abassurance.systems.data_integration.mapping.fields import optional_text

CATEGORIES = {
    "customer_status": {"ACTIF": "ACTIF", "ACTIVE": "ACTIF", "INACTIF": "INACTIF", "INACTIVE": "INACTIF"},
    "contract_status": {
        "ACTIF": "ACTIF",
        "ACTIVE": "ACTIF",
        "RESILIE": "RESILIE",
        "CANCELLED": "RESILIE",
        "CANCELED": "RESILIE",
        "EXPIRE": "EXPIRE",
        "EXPIRED": "EXPIRE",
        "SUSPENDU": "SUSPENDU",
        "SUSPENDED": "SUSPENDU",
    },
    "claim_status": {
        "OUVERT": "OUVERT",
        "OPEN": "OUVERT",
        "CLOS": "CLOS",
        "CLOSED": "CLOS",
        "EN_COURS": "EN_COURS",
        "IN_PROGRESS": "EN_COURS",
        "REJETE": "REJETE",
        "REJECTED": "REJETE",
    },
    "payment_method": {
        "VIREMENT": "VIREMENT",
        "BANK_TRANSFER": "VIREMENT",
        "CARTE": "CARTE",
        "CARD": "CARTE",
        "PRELEVEMENT": "PRELEVEMENT",
        "DIRECT_DEBIT": "PRELEVEMENT",
        "CHEQUE": "CHEQUE",
        "CHECK": "CHEQUE",
        "ESPECES": "ESPECES",
        "CASH": "ESPECES",
    },
    "transaction_status": {
        "COMPLETED": "TERMINE",
        "TERMINE": "TERMINE",
        "PENDING": "EN_ATTENTE",
        "EN_ATTENTE": "EN_ATTENTE",
        "FAILED": "ECHEC",
        "ECHEC": "ECHEC",
        "REFUNDED": "REMBOURSE",
        "REMBOURSE": "REMBOURSE",
        "CANCELLED": "ANNULE",
        "CANCELED": "ANNULE",
        "ANNULE": "ANNULE",
    },
    "product_type": {
        "AUTO": "AUTO",
        "AUTOMOBILE": "AUTO",
        "CAR": "AUTO",
        "HABITATION": "HABITATION",
        "HOME": "HABITATION",
        "SANTE": "SANTE",
        "HEALTH": "SANTE",
        "VIE": "VIE",
        "LIFE": "VIE",
    },
}


def normalize_category(value: object, field: str) -> str | None:
    text = optional_text(value, field)
    if text is None or not text.strip():
        return None
    key = normalize("NFKD", text.strip().upper()).encode("ascii", "ignore").decode()
    key = "_".join(key.replace("-", " ").split())
    if key not in CATEGORIES[field]:
        raise ValueError(f"Unsupported {field}")
    return CATEGORIES[field][key]


def normalize_date(value: object) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        value = value.date()

    if isinstance(value, date):
        return value.isoformat()

    if not isinstance(value, str):
        raise ValueError(f"Invalid date: {value!r}")

    return date.fromisoformat(value.strip()).isoformat()


def normalize_datetime(value: object) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat(timespec="seconds")

    if not isinstance(value, str):
        raise ValueError(f"Invalid datetime: {value!r}")

    return datetime.fromisoformat(value.strip()).isoformat(timespec="seconds")


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"Invalid {name}: {value!r}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid {name}: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"Invalid {name}: {value!r}")

    return result


def normalize_amount(value: object, *, multiplier: int = 1) -> str | None:
    if value is None:
        return None

    amount = _decimal(value, "amount")
    if amount < 0:
        raise ValueError(f"Invalid amount: {value!r}")

    return str((amount * multiplier).quantize(Decimal("0.01")))


def normalize_score(value: object) -> str | None:
    if value is None:
        return None

    return str(_decimal(value, "fraud score"))
