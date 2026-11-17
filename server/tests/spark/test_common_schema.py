import json
from datetime import date, datetime
from decimal import Decimal

import pytest
from abassurance.systems.spark_pipeline.schemas import entity_schema
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark_integration


def test_native_parquet_types_roundtrip_without_changing_api_precision(spark, tmp_path):
    cases = {
        "customers": {
            "customer_id": "id",
            "source_system": "abassurance",
            "source_id": "100",
            "birth_date": "1990-05-12",
            "created_at": "2025-01-15T10:30:00",
            "loyalty_score": "80.25",
        },
        "contracts": {
            "contract_id": "id",
            "source_system": "abassurance",
            "source_reference": "R",
            "customer_id": "parent",
            "start_date": "2025-01-01",
            "end_date": "2025-12-31",
            "annual_premium": "1234567890123456.78",
            "agency_id": 1,
        },
        "claims": {
            "claim_id": "id",
            "source_system": "abassurance",
            "source_id": "1",
            "contract_id": "parent",
            "incident_date": "2025-03-10T14:30:00",
            "estimated_amount": "2500.00",
            "fraud_score": "12.50",
        },
        "payments": {
            "payment_id": "id",
            "source_system": "abassurance",
            "source_id": "1",
            "contract_id": "parent",
            "payment_date": "2025-01-05T00:00:00",
            "amount": "1200.01",
        },
    }

    for entity, record in cases.items():
        schema = entity_schema(entity)

        frame = (
            spark.createDataFrame([(json.dumps(record),)], "value string")
            .select(F.from_json("value", schema, {"mode": "FAILFAST"}).alias("record"))
            .select("record.*")
        )

        path = str(tmp_path / entity)

        frame.write.parquet(path)

        actual = spark.read.parquet(path)
        row = actual.first().asDict()

        assert actual.schema.simpleString() == schema.simpleString()

        for field, value in record.items():
            kind = schema[field].dataType.simpleString()

            if kind.startswith("decimal"):
                assert row[field] == Decimal(value)
            elif kind == "date":
                assert row[field] == date.fromisoformat(value)
            elif kind == "timestamp":
                assert row[field] == datetime.fromisoformat(value)
            else:
                assert row[field] == value
