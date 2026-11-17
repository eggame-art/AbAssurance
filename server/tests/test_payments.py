from dataclasses import fields
from uuid import UUID

import pytest
from abassurance.shared.models.unified import Payment
from abassurance.systems.data_integration.mapping import map_payment


def test_oracle_payment_mapping():
    payment = {
        "AB_PAYMENT_ID": 500,
        "AB_POLICY_NUMBER": "CONT-001",
        "AB_DATE_PAIEMENT": "2025-04-15",
        "AB_MONTANT": "250.50",
        "AB_MODE_PAIEMENT": "VIREMENT",
    }

    result = map_payment(payment, "abassurance")

    assert result["payment_id"] == "d76ce97d-3cd4-5c30-bb8b-2963fad067f4"
    assert result["contract_id"] == "eefff547-f7ef-58e7-8290-39fd86bd4899"
    assert result["payment_date"] == "2025-04-15T00:00:00"
    assert result["amount"] == "250.50"
    assert result["payment_method"] == "BANK_TRANSFER"
    assert result["transaction_status"] is None
    assert set(result) == {field.name for field in fields(Payment)}


def test_sqlserver_payment_mapping():
    payment_id = UUID("123e4567-e89b-12d3-a456-426614174000")

    payment = {
        "AP_PAYMENT_REF": payment_id,
        "AP_CONTRACT_REF": "CONT-001",
        "AP_PAYMENT_DATETIME": "2025-04-15 14:30:00",
        "AP_AMOUNT_PAID": "150.00",
        "AP_PAYMENT_CHANNEL": "CARD",
        "AP_TRANSACTION_STATUS": "COMPLETED",
    }

    result = map_payment(payment, "assureplus")

    assert result["payment_id"] == "f8bd5dfd-10bb-51bb-b7fb-f0564fd04d68"
    assert result["contract_id"] == "c648a223-b93a-5604-80f2-c9e6533def8e"
    assert result["amount"] == "150.00"
    assert result["payment_date"] == "2025-04-15T14:30:00"
    assert result["transaction_status"] == "COMPLETED"
    assert set(result) == {field.name for field in fields(Payment)}


def test_negative_payment_amount():
    payment = {
        "AB_PAYMENT_ID": 501,
        "AB_POLICY_NUMBER": "CONT-002",
        "AB_MONTANT": -100,
    }

    with pytest.raises(ValueError):
        map_payment(payment, "abassurance")


def test_missing_payment_identifier():
    payment = {
        "AB_PAYMENT_ID": None,
        "AB_POLICY_NUMBER": "CONT-002",
    }

    with pytest.raises(ValueError):
        map_payment(payment, "abassurance")


def test_unknown_payment_source():
    with pytest.raises(ValueError):
        map_payment({}, "unknown")
