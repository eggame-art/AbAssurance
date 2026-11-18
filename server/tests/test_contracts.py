from dataclasses import fields
from decimal import Decimal

import pytest
from abassurance.shared.models.unified import Contract
from abassurance.systems.data_integration.mapping import map_contract


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

    assert result["contract_id"] == "eefff547-f7ef-58e7-8290-39fd86bd4899"
    assert result["customer_id"] == "0ce23490-7b17-57a3-933d-829abb01a605"
    assert result["product_type"] == "CAR"
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

    assert result["contract_id"] == "c648a223-b93a-5604-80f2-c9e6533def8e"
    assert result["customer_id"] == "3fff094d-64af-55c0-abc9-2e86a559959e"
    assert result["annual_premium"] == "1200.00"
    assert result["broker_code"] == "BR-001"
    assert result["agency_id"] is None
    assert set(result) == {field.name for field in fields(Contract)}


def test_invalid_premium():
    contract = {"AB_POLICY_NUMBER": "CONT-002", "AB_CLIENT_ID": 101, "AB_PRIME_ANNUELLE": -500}

    with pytest.raises(ValueError):
        map_contract(contract, "abassurance")


def test_monthly_premium_conversion():
    contract = {"AP_CONTRACT_REF": "CONT-003", "AP_USER_ID": 102, "AP_MONTHLY_PREMIUM": Decimal("99.99")}

    result = map_contract(contract, "assureplus")

    assert result["annual_premium"] == "1199.88"


def test_unknown_contract_source():
    with pytest.raises(ValueError):
        map_contract({}, "unknown")
