from dataclasses import fields
from decimal import Decimal

import pytest
from abassurance.shared.models.unified import Claim, Contract, Customer, Payment
from abassurance.systems.data_integration.mapping import map_customer
from abassurance.systems.data_integration.mapping.normalizers import normalize_amount
from abassurance.systems.spark_pipeline.schemas import entity_schema


@pytest.mark.parametrize(
    ("entity", "model", "typed"),
    [
        ("customers", Customer, {"birth_date": "date", "created_at": "timestamp", "loyalty_score": "decimal(18,2)"}),
        (
            "contracts",
            Contract,
            {"start_date": "date", "end_date": "date", "annual_premium": "decimal(18,2)", "agency_id": "int"},
        ),
        (
            "claims",
            Claim,
            {"incident_date": "timestamp", "estimated_amount": "decimal(18,2)", "fraud_score": "decimal(18,2)"},
        ),
        ("payments", Payment, {"payment_date": "timestamp", "amount": "decimal(18,2)"}),
    ],
)
def test_every_field_matches_the_business_diagram(entity, model, typed):
    schema = entity_schema(entity)
    expected_fields = {field.name for field in fields(model)}

    assert set(schema.fieldNames()) == expected_fields

    for field in schema:
        expected_type = typed.get(field.name, "string")

        assert field.dataType.simpleString() == expected_type


def test_loyalty_score_accepts_decimal_without_integer_truncation():
    record = {"AP_USER_ID": 100, "AP_LOYALTY_SCORE": Decimal("80.25")}

    result = map_customer(record, "assureplus")

    assert result["loyalty_score"] == "80.25"


def test_monetary_precision_limit_is_checked_before_spark():
    valid_amount = "9999999999999999.99"
    invalid_amount = "10000000000000000"

    assert normalize_amount(valid_amount) == valid_amount

    with pytest.raises(ValueError):
        normalize_amount(invalid_amount)
