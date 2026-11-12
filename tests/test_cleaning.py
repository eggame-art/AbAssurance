from copy import deepcopy
from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from cleaning import clean_datasets
from pipeline.run import MAPPERS
from pipeline.validation import PipelineError, validate_datasets


@pytest.fixture
def sources():
    return {
        "abassurance": {
            "customers": [{"AB_CLIENT_ID": 100, "AB_PRENOM": "Jean", "AB_NOM": "Dupont"}],
            "contracts": [{"AB_POLICY_NUMBER": "CONT-1", "AB_CLIENT_ID": 100}],
            "claims": [{"AB_CLAIM_ID": 1, "AB_POLICY_NUMBER": "CONT-1"}],
            "payments": [{"AB_PAYMENT_ID": 1, "AB_POLICY_NUMBER": "CONT-1"}],
        }
    }


@pytest.mark.parametrize("entity", MAPPERS)
def test_duplicates_keep_first_valid_record_and_report_every_extra_row(sources, entity):
    records = sources["abassurance"][entity]
    duplicate = {**records[0], "EXTRA": "original information"}
    records.extend([duplicate, deepcopy(duplicate)])

    data, rejected = clean_datasets(sources, MAPPERS)

    assert len(data[entity]) == 1
    assert [row["row"] for row in rejected] == [2, 3]
    assert all(row["record"] == duplicate for row in rejected)
    assert all("Duplicate identifier" in row["reason"] for row in rejected)

    validate_datasets(data)


@pytest.mark.parametrize(
    "entity, field, value",
    [
        ("customers", "AB_CLIENT_ID", None),
        ("customers", "AB_CLIENT_ID", "  "),
        ("contracts", "AB_CLIENT_ID", None),
    ],
)
def test_missing_identifiers_are_rejected(sources, entity, field, value):
    sources["abassurance"][entity][0][field] = value

    data, rejected = clean_datasets(sources, MAPPERS)

    assert data[entity] == []
    assert rejected[0]["entity"] == entity
    assert "Missing source identifier" in rejected[0]["reason"]

    validate_datasets(data)


@pytest.mark.parametrize(
    "entity, field, value, reason",
    [
        ("customers", "AB_DATE_NAISSANCE", "invalid", "Invalid isoformat"),
        ("claims", "AB_MONTANT_ESTIME", -1, "Invalid amount"),
        ("payments", "AB_MONTANT", float("nan"), "Invalid amount"),
    ],
)
def test_invalid_values_are_rejected(sources, entity, field, value, reason):
    sources["abassurance"][entity][0][field] = value

    data, rejected = clean_datasets(sources, MAPPERS)

    assert data[entity] == []
    assert rejected[0]["entity"] == entity
    assert reason in rejected[0]["reason"]

    validate_datasets(data)


def test_missing_field_is_reported_without_stopping_other_records(sources):
    del sources["abassurance"]["payments"][0]["AB_PAYMENT_ID"]
    sources["abassurance"]["payments"].append({
        "AB_PAYMENT_ID": 2,
        "AB_POLICY_NUMBER": "CONT-1",
    })

    data, rejected = clean_datasets(sources, MAPPERS)

    assert [row["payment_id"] for row in data["payments"]] == ["AB-2"]
    assert rejected[0]["row"] == 1
    assert "AB_PAYMENT_ID" in rejected[0]["reason"]


def test_optional_values_stay_optional_and_sources_are_not_modified(sources):
    before = deepcopy(sources)

    data, rejected = clean_datasets(sources, MAPPERS)

    assert sources == before
    assert rejected == []
    assert data["customers"][0]["birth_date"] is None
    assert data["payments"][0]["amount"] is None


def test_reversed_contract_dates_reject_contract_and_children(sources):
    sources["abassurance"]["contracts"][0].update(
        AB_DATE_DEBUT="2025-12-31",
        AB_DATE_FIN="2025-01-01",
    )

    data, rejected = clean_datasets(sources, MAPPERS)

    assert len(data["customers"]) == 1
    assert len(rejected) == 3
    assert "end_date precedes start_date" in rejected[0]["reason"]
    assert all("Orphan contract_id" in row["reason"] for row in rejected[1:])


def test_rejected_database_values_keep_their_type_and_representation(sources):
    record = sources["abassurance"]["payments"][0]
    record.update(
        AB_MONTANT=Decimal("-1.2300"),
        AB_DATE_PAIEMENT=date(2025, 1, 1),
        EXTRA_ID=UUID("123e4567-e89b-12d3-a456-426614174000"),
        EXTRA_FLOAT=float("inf"),
    )

    _, rejected = clean_datasets(sources, MAPPERS)

    for field in ("AB_MONTANT", "AB_DATE_PAIEMENT", "EXTRA_ID", "EXTRA_FLOAT"):
        assert rejected[0]["record"][field] == {
            "type": type(record[field]).__name__,
            "value": repr(record[field]),
        }


def test_rejects_inconsistent_source_identifier(sources):
    def invalid_payment(record, source):
        mapped = MAPPERS["payments"](record, source)
        mapped["payment_id"] = "AP-OTHER"
        return mapped

    mappers = {**MAPPERS, "payments": invalid_payment}

    data, rejected = clean_datasets(sources, mappers)

    assert data["payments"] == []
    assert "Inconsistent source identifier" in rejected[-1]["reason"]


def test_rejects_cross_source_contract_reference(sources):
    def contract_mapper(record, source):
        mapped = MAPPERS["contracts"](record, source)
        mapped.update(
            contract_id="AP-OTHER",
            source_system="assureplus",
            source_reference="OTHER",
        )
        mapped["customer_id"] = "AP-100"

        return mapped

    def payment_mapper(record, source):
        mapped = MAPPERS["payments"](record, source)
        mapped["contract_id"] = "AP-OTHER"
        
        return mapped

    sources["assureplus"] = {entity: [] for entity in MAPPERS}
    sources["assureplus"]["customers"] = [{"AP_USER_ID": 100}]

    mappers = {
        **MAPPERS,
        "contracts": contract_mapper,
        "payments": payment_mapper,
    }

    data, rejected = clean_datasets(sources, mappers)

    assert data["payments"] == []
    assert "Cross-source contract_id" in rejected[-1]["reason"]


def test_validation_still_rejects_duplicate_identifiers(sources):
    data, _ = clean_datasets(sources, MAPPERS)
    data["customers"].append(deepcopy(data["customers"][0]))

    with pytest.raises(PipelineError, match="Duplicate identifier"):
        validate_datasets(data)