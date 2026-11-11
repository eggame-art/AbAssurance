import json
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest

from extraction.common import ExtractionError
from extraction.config import Settings
from pipeline import run
from pipeline.validation import PipelineError


@pytest.fixture
def records(monkeypatch):
    data = {
        "abassurance": {
            "customers": [{"AB_CLIENT_ID": 100, "AB_NOM": "Dupont", "AB_PRENOM": "Jean"}],
            "contracts": [{"AB_POLICY_NUMBER": "CONT-1", "AB_CLIENT_ID": 100, "AB_PRIME_ANNUELLE": Decimal("1199.88")}],
            "claims": [{"AB_CLAIM_ID": 1, "AB_POLICY_NUMBER": "CONT-1"}],
            "payments": [{"AB_PAYMENT_ID": 1, "AB_POLICY_NUMBER": "CONT-1", "AB_MONTANT": Decimal("99.99")}],
        },
        "assureplus": {
            "customers": [{"AP_USER_ID": 100, "AP_FULL_NAME": "Marie Martin"}],
            "contracts": [{"AP_CONTRACT_REF": "CONT-1", "AP_USER_ID": 100, "AP_MONTHLY_PREMIUM": Decimal("99.99")}],
            "claims": [{"AP_SINISTRE_NUM": 1, "AP_CONTRACT_REF": "CONT-1"}],
            "payments": [
                {
                    "AP_PAYMENT_REF": UUID("123e4567-e89b-12d3-a456-426614174000"),
                    "AP_CONTRACT_REF": "CONT-1",
                    "AP_AMOUNT_PAID": Decimal("99.99"),
                    "AP_PAYMENT_DATETIME": datetime(2025, 4, 15, 14, 30),
                }
            ],
        },
    }
    monkeypatch.setattr(run, "fetch_oracle", lambda entity, config: data["abassurance"][entity])
    monkeypatch.setattr(run, "fetch_sqlserver", lambda entity, config: data["assureplus"][entity])
    return data


def test_complete_pipeline_keeps_origins_and_exact_amounts(records, settings: Settings):
    data = run.run_pipeline(settings=settings)
    assert {row["customer_id"] for row in data["customers"]} == {"AB-100", "AP-100"}
    assert {row["source_id"] for row in data["customers"]} == {"100"}
    assert {row["annual_premium"] for row in data["contracts"]} == {"1199.88"}
    assert data["payments"][1]["source_id"] == "123e4567-e89b-12d3-a456-426614174000"
    assert data["payments"][1]["payment_date"] == "2025-04-15T14:30:00"
    for entity, rows in data.items():
        exported = json.loads((settings.output_dir / f"{entity}.json").read_text("utf-8"))
        assert exported == rows
        assert len(rows) == 2
    assert len(list(settings.output_dir.iterdir())) == 4


@pytest.mark.parametrize("failure", ["mapping", "duplicate", "orphan", "extraction", "json"])
def test_failures_preserve_all_previous_exports(records, settings, monkeypatch, failure):
    run.run_pipeline(settings=settings)
    before = {path.name: path.read_bytes() for path in settings.output_dir.iterdir()}
    if (failure == "mapping"):
        records["assureplus"]["payments"][0]["AP_AMOUNT_PAID"] = -1
    elif (failure == "duplicate"):
        records["abassurance"]["claims"].append(deepcopy(records["abassurance"]["claims"][0]))
    elif (failure == "orphan"):
        records["abassurance"]["payments"][0]["AB_POLICY_NUMBER"] = "UNKNOWN"
    elif (failure == "json"):
        records["assureplus"]["customers"][0]["AP_LOYALTY_SCORE"] = object()
    else:

        def fail(entity, config):
            if (entity == "payments"):
                raise ExtractionError("SQL Server unavailable")
            return records["assureplus"][entity]

        monkeypatch.setattr(run, "fetch_sqlserver", fail)
    with pytest.raises((PipelineError, ExtractionError)):
        run.run_pipeline(settings=settings)
    after = {path.name: path.read_bytes() for path in settings.output_dir.iterdir()}
    assert before == after


def test_empty_sources_produce_empty_json_arrays(records, settings):
    for entities in records.values():
        for entity in entities:
            entities[entity] = []
    data = run.run_pipeline(settings=settings)
    assert all(rows == [] for rows in data.values())
