from dataclasses import fields
from decimal import Decimal

import pytest

from mapping import map_contract
from models.unified import Contract


def test_oracle_contract_mapping():
    contract = {
        "AB_POLICY_NUMBER": "CONT-001",
        "AB_CLIENT_ID": 100,
        "AB_TYPE_ASSURANCE": "AUTO",
        "AB_DATE_DEBUT": "2025-01-01",
        "AB_DATE_FIN": "2026-01-01",
        "AB_PRIME_ANNUELLE": 1200,
        "AB_STATUT_CONTRAT": "ACTIF",
        "AB_AGENCE_ID": 10,
    }

    result = map_contract(contract, "abassurance")

    assert result["contract_id"] == "AB-CONT-001"
    assert result["customer_id"] == "AB-100"
    assert result["product_type"] == "AUTO"
    assert result["annual_premium"] == "1200.00"
    assert result["agency_id"] == 10
    assert result["broker_code"] is None
    assert set(result) == {field.name for field in fields(Contract)}


def test_sqlserver_contract_mapping():
    contract = {
        "AP_CONTRACT_REF": "CONT-001",
        "AP_USER_ID": 100,
        "AP_PRODUCT_CODE": "AUTO",
        "AP_START_DATE": "2025-01-01",
        "AP_END_DATE": "2026-01-01",
        "AP_MONTHLY_PREMIUM": Decimal("100.00"),
        "AP_CONTRACT_STATE": "ACTIVE",
        "AP_BROKER_CODE": "BR-001",
    }

    result = map_contract(contract, "assureplus")

    assert result["contract_id"] == "AP-CONT-001"
    assert result["customer_id"] == "AP-100"
    assert result["annual_premium"] == "1200.00"
    assert result["broker_code"] == "BR-001"
    assert result["agency_id"] is None
    assert set(result) == {field.name for field in fields(Contract)}


def test_invalid_premium():
    contract = {
        "AB_POLICY_NUMBER": "CONT-002",
        "AB_CLIENT_ID": 101,
        "AB_PRIME_ANNUELLE": -500,
    }

    with pytest.raises(ValueError):
        map_contract(contract, "abassurance")


def test_monthly_premium_conversion():
    contract = {
        "AP_CONTRACT_REF": "CONT-003",
        "AP_USER_ID": 102,
        "AP_MONTHLY_PREMIUM": Decimal("99.99"),
    }

    result = map_contract(contract, "assureplus")

    assert result["annual_premium"] == "1199.88"


def test_unknown_contract_source():
    with pytest.raises(ValueError):
        map_contract({}, "unknown")