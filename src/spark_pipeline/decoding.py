import json
from decimal import Decimal

from messaging.config import SOURCE_TABLES
from pipeline.run import MAPPERS
from pipeline.transform import map_records
from pipeline.validation import validate_record_fields
from shared.errors import PipelineError
from spark_pipeline.serialization import restore_dates


def _invalid_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


def decode_message(value: str | None, key: str | None, topic: str, prefix: str) -> dict[str, str | None]:
    result: dict[str, str | None] = {"source_system": None, "entity": None, "mapped_json": None, "reason": None}

    try:
        if value is None:
            raise ValueError("Kafka message contains no JSON value (tombstone)")

        envelope = json.loads(value, parse_float=Decimal, parse_constant=_invalid_constant)

        if not isinstance(envelope, dict):
            raise ValueError("JSON envelope must be an object")

        source, entity = envelope.get("source_system"), envelope.get("entity")

        if not isinstance(source, str) or source not in SOURCE_TABLES:
            raise ValueError("Unknown source_system")

        result["source_system"] = source

        if not isinstance(entity, str) or entity not in SOURCE_TABLES[source]:
            raise ValueError("Unknown entity")

        result["entity"] = entity

        if topic != f"{prefix}.{source}.{entity}":
            raise ValueError("Kafka topic does not match source_system/entity")

        record = envelope.get("data")

        if not isinstance(record, dict):
            raise ValueError("JSON data must be an object")

        primary_key = SOURCE_TABLES[source][entity][1]

        if key != f"{source}:{entity}:{record[primary_key]}":
            raise ValueError("Kafka key does not match source identifier")

        mapped = map_records([restore_dates(record, source, entity)], source, entity, MAPPERS[entity])[0]

        validate_record_fields(mapped, entity)

        result["mapped_json"] = json.dumps(mapped, ensure_ascii=False, allow_nan=False)

    except (ValueError, TypeError, KeyError, ArithmeticError, PipelineError) as exc:
        result["reason"] = str(exc.__cause__ or exc)

    return result
