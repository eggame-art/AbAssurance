def optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None

    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")

    return value


def optional_integer(value: object, field: str) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")

    if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
        value = int(value)
    if not isinstance(value, int) or not -(2**31) <= value < 2**31:
        raise ValueError(f"{field} must fit a 32-bit integer")
    return value
