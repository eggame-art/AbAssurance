from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from unicodedata import normalize

from abassurance.systems.data_integration.mapping.fields import optional_text

CATEGORIES = {
    "customer_status": {"ACTIF": "ACTIVE", "ACTIVE": "ACTIVE", "INACTIF": "INACTIVE", "INACTIVE": "INACTIVE"},
    "contract_status": {
        "ACTIF": "ACTIVE",
        "ACTIVE": "ACTIVE",
        "RESILIE": "CANCELLED",
        "CANCELLED": "CANCELLED",
        "CANCELED": "CANCELLED",
        "EXPIRE": "EXPIRED",
        "EXPIRED": "EXPIRED",
        "SUSPENDU": "SUSPENDED",
        "SUSPENDED": "SUSPENDED",
    },
    "claim_status": {
        "OUVERT": "OPEN",
        "OPEN": "OPEN",
        "CLOS": "CLOSED",
        "CLOSED": "CLOSED",
        "EN_COURS": "IN_PROGRESS",
        "IN_PROGRESS": "IN_PROGRESS",
        "REJETE": "REJECTED",
        "REJECTED": "REJECTED",
    },
    "payment_method": {
        "VIREMENT": "BANK_TRANSFER",
        "BANK_TRANSFER": "BANK_TRANSFER",
        "CARTE": "CARD",
        "CARD": "CARD",
        "PRELEVEMENT": "DIRECT_DEBIT",
        "DIRECT_DEBIT": "DIRECT_DEBIT",
        "CHEQUE": "CHECK",
        "CHECK": "CHECK",
        "ESPECES": "CASH",
        "CASH": "CASH",
    },
    "transaction_status": {
        "COMPLETED": "COMPLETED",
        "TERMINE": "COMPLETED",
        "PENDING": "PENDING",
        "EN_ATTENTE": "PENDING",
        "FAILED": "FAILED",
        "ECHEC": "FAILED",
        "REFUNDED": "REFUNDED",
        "REMBOURSE": "REFUNDED",
        "CANCELLED": "CANCELLED",
        "CANCELED": "CANCELLED",
        "ANNULE": "CANCELLED",
    },
    "product_type": {
        "AUTO": "CAR",
        "AUTOMOBILE": "CAR",
        "CAR": "CAR",
        "HABITATION": "HOME",
        "HOME": "HOME",
        "SANTE": "HEALTH",
        "HEALTH": "HEALTH",
        "VIE": "LIFE",
        "LIFE": "LIFE",
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

    result = (amount * multiplier).quantize(Decimal("0.01"))
    if result >= Decimal("10000000000000000"):
        raise ValueError("Amount exceeds decimal(18,2)")
    return str(result)


def normalize_score(value: object) -> str | None:
    if value is None:
        return None

    result = _decimal(value, "score").quantize(Decimal("0.01"))
    if abs(result) >= Decimal("10000000000000000"):
        raise ValueError("Score exceeds decimal(18,2)")
    return str(result)
