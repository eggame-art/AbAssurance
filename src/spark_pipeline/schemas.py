from dataclasses import fields
from typing import get_args, get_type_hints

from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

from models.unified import Claim, Contract, Customer, Payment

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
    hints = get_type_hints(model)
    return StructType(
        [
            StructField(
                field.name,
                LongType() if hints[field.name] is int or int in get_args(hints[field.name]) else StringType(),
                type(None) in get_args(hints[field.name]),
            )
            for field in fields(model)
        ]
    )
