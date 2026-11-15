SOURCE_PREFIXES = {
    "abassurance": "AB",
    "assureplus": "AP",
}


def make_id(source: str, value: object) -> str:
    if source not in SOURCE_PREFIXES:
        raise ValueError(f"Unknown source: {source}")

    if value is None or not str(value).strip():
        raise ValueError("Missing source identifier")

    return f"{SOURCE_PREFIXES[source]}-{value}"
