from dataclasses import fields
from uuid import UUID

import pytest

from mapping import map_payment
from models.unified import Payment


def test_oracle_payment_mapping():
    payment = {
        "AB_PAYMENT_ID": 500,
        "AB_POLICY_NUMBER": "CONT-001",
        "AB_DATE_PAIEMENT": "2025-04-15",
        "AB_MONTANT": "250.50",
        "AB_MODE_PAIEMENT": "VIREMENT",
    }

    result = map_payment(payment, "abassurance")

    assert result["payment_id"] == "AB-500"
    assert result["contract_id"] == "AB-CONT-001"
    assert result["payment_date"] == "2025-04-15T00:00:00"
    assert result["amount"] == "250.50"
    assert result["payment_method"] == "VIREMENT"
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

    assert result["payment_id"] == f"AP-{payment_id}"
    assert result["contract_id"] == "AP-CONT-001"
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
