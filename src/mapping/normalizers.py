from datetime import date, datetime
from decimal import Decimal, InvalidOperation


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
