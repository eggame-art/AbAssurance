from datetime import date, datetime
from decimal import Decimal, InvalidOperation


def normalize_date(value: object) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        value = value.date()

    if isinstance(value, date):
        return value.isoformat()

    return date.fromisoformat(str(value).strip()).isoformat()


def normalize_datetime(value: object) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")

    return datetime.fromisoformat(
        str(value).strip()
    ).isoformat(timespec="seconds")


def normalize_amount(
    value: object,
    *,
    multiplier: int = 1,
) -> str | None:
    if value is None:
        return None

    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid amount: {value}") from exc

    if not amount.is_finite() or amount < 0:
        raise ValueError(f"Invalid amount: {value}")

    return str(
        (amount * multiplier).quantize(Decimal("0.01"))
    )


def normalize_score(value: object) -> str | None:
    if value is None:
        return None

    try:
        score = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid fraud score: {value}") from exc

    if not score.is_finite():
        raise ValueError(f"Invalid fraud score: {value}")

    return str(score)
