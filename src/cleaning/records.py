from math import isfinite

from pipeline.transform import map_records
from pipeline.validation import ID_FIELDS, RELATIONSHIPS, validate_record
from shared.errors import PipelineError
from shared.types import Datasets, Mapper, Record

ENTITY_ORDER = ("customers", "contracts", "claims", "payments")


def _source_record(record: Record) -> Record:
    return {
        field: value
        if value is None or isinstance(value, (str, bool, int)) or (isinstance(value, float) and isfinite(value))
        else {"type": type(value).__name__, "value": repr(value)}
        for field, value in record.items()
    }


def clean_datasets(sources: dict[str, Datasets], mappers: dict[str, Mapper]) -> tuple[Datasets, list[Record]]:
    cleaned: Datasets = {}
    rejected: list[Record] = []

    for entity in ENTITY_ORDER:
        mapper = mappers[entity]
        cleaned[entity] = []
        identifiers: set[str] = set()

        parent = RELATIONSHIPS.get(entity, ("", ""))[0]
        parents = {row[ID_FIELDS[parent]]: row for row in cleaned[parent]} if parent else {}

        for source, datasets in sources.items():
            for position, record in enumerate(datasets[entity], start=1):
                try:
                    mapped = map_records([record], source, entity, mapper)[0]
                    validate_record(mapped, entity, identifiers, parents)
                except PipelineError as exc:
                    rejected.append({
                        "entity": entity,
                        "source_system": source,
                        "row": position,
                        "reason": str(exc.__cause__ or exc),
                        "record": _source_record(record),
                    })
                    continue

                identifiers.add(mapped[ID_FIELDS[entity]])
                cleaned[entity].append(mapped)

    return cleaned, rejected
