from dataclasses import fields
from typing import get_args, get_type_hints

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from abassurance.shared.models.unified import Claim, Contract, Customer, Payment

MODELS = {"customers": Customer, "contracts": Contract, "claims": Claim, "payments": Payment}

RAW_SCHEMA = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("timestamp", TimestampType()),
    ]
)

DECODED_SCHEMA = StructType(
    [StructField(name, StringType()) for name in ("source_system", "entity", "mapped_json", "reason")]
)


def entity_schema(entity: str) -> StructType:
    model = MODELS[entity]
    hints = get_type_hints(model, include_extras=True)
    types = {"uuid": StringType(), "date": DateType(), "datetime": TimestampType(), "decimal": DecimalType(18, 2)}
    columns = []
    for field in fields(model):
        hint = hints[field.name]
        optional = type(None) in get_args(hint)
        value_type = next((kind for kind in get_args(hint) if kind is not type(None)), hint) if optional else hint
        metadata = getattr(value_type, "__metadata__", ())
        kind = types[metadata[0]] if metadata else IntegerType() if value_type is int else StringType()
        columns.append(StructField(field.name, kind, optional))
    return StructType(columns)
