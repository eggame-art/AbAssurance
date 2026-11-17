from uuid import UUID, uuid5

SOURCE_PREFIXES = {"abassurance": "AB", "assureplus": "AP"}
NAMESPACE = UUID("12345678-1234-5678-1234-567812345678")
ENTITIES = {
    "customers": "customer",
    "contracts": "contract",
    "claims": "claim",
    "payments": "payment",
    "cancellations": "cancellation",
}


def create_common_id(entity: str, source: str, source_id: str) -> str:
    if entity not in ENTITIES.values() or source not in SOURCE_PREFIXES:
        raise ValueError("Unknown identity source or entity")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("Missing source identifier")
    return str(uuid5(NAMESPACE, f"{entity}:{source}:{source_id}"))


def make_id(source: str, value: object, entity: str) -> str:
    if value is None:
        raise ValueError("Missing source identifier")
    if entity not in ENTITIES:
        raise ValueError("Unknown entity")
    return create_common_id(ENTITIES[entity], source, str(value))


def matches_identity(source, reference, identifier, entity, *, allow_legacy=False) -> bool:
    try:
        return identifier == make_id(source, reference, entity) or (
            allow_legacy and identifier == f"{SOURCE_PREFIXES[source]}-{reference}"
        )
    except (ValueError, TypeError, KeyError):
        return False
