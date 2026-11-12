from dataclasses import fields

import pytest

from mapping import map_claim
from models.unified import Claim


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

    assert result["claim_id"] == "AB-50"
    assert result["contract_id"] == "AB-CONT-001"
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

    assert result["claim_id"] == "AP-50"
    assert result["contract_id"] == "AP-CONT-001"
    assert result["estimated_amount"] == "2500.00"
    assert result["claim_status"] == "OPEN"
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