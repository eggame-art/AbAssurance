from dataclasses import fields

import pytest
from abassurance.shared.models.unified import Claim
from abassurance.systems.data_integration.mapping import map_claim


def test_oracle_claim_mapping():
    claim = {
        "AB_CLAIM_ID": 50,
        "AB_POLICY_NUMBER": "CONT-001",
        "AB_DATE_SINISTRE": "2025-03-15 14:30:00",
        "AB_MONTANT_ESTIME": "1500.50",
        "AB_STATUT_SINISTRE": "OUVERT",
        "AB_DESCRIPTION": "Accident automobile",
    }

    result = map_claim(claim, "abassurance")

    assert result["claim_id"] == "3c93392c-3f34-56a5-8713-e7a22540c9d9"
    assert result["contract_id"] == "eefff547-f7ef-58e7-8290-39fd86bd4899"
    assert result["incident_date"] == "2025-03-15T14:30:00"
    assert result["estimated_amount"] == "1500.50"
    assert result["claim_status"] == "OUVERT"
    assert result["fraud_score"] is None
    assert set(result) == {field.name for field in fields(Claim)}


def test_sqlserver_claim_mapping():
    claim = {
        "AP_SINISTRE_NUM": 50,
        "AP_CONTRACT_REF": "CONT-001",
        "AP_INCIDENT_DATE": "2025-03-15 14:30:00",
        "AP_ESTIMATED_AMOUNT": "2500.00",
        "AP_CLAIM_STATUS": "OPEN",
        "AP_CLAIM_COMMENT": "Vehicle damaged",
        "AP_FRAUD_SCORE": "75.50",
    }

    result = map_claim(claim, "assureplus")

    assert result["claim_id"] == "0be578dc-4ab8-55c7-8ccc-fa4f28fd906d"
    assert result["contract_id"] == "c648a223-b93a-5604-80f2-c9e6533def8e"
    assert result["estimated_amount"] == "2500.00"
    assert result["claim_status"] == "OUVERT"
    assert result["fraud_score"] == "75.50"
    assert set(result) == {field.name for field in fields(Claim)}


def test_negative_claim_amount():
    claim = {
        "AB_CLAIM_ID": 51,
        "AB_POLICY_NUMBER": "CONT-002",
        "AB_MONTANT_ESTIME": -100,
    }

    with pytest.raises(ValueError):
        map_claim(claim, "abassurance")


def test_invalid_fraud_score():
    claim = {
        "AP_SINISTRE_NUM": 52,
        "AP_CONTRACT_REF": "CONT-003",
        "AP_FRAUD_SCORE": "invalid",
    }

    with pytest.raises(ValueError):
        map_claim(claim, "assureplus")


def test_unknown_claim_source():
    with pytest.raises(ValueError):
        map_claim({}, "unknown")