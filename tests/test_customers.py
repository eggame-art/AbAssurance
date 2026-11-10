from dataclasses import fields

import pytest

from mapping import map_customer
from models.unified import Customer


def test_oracle_customer_mapping():
    customer = {
        "AB_CLIENT_ID": 100,
        "AB_PRENOM": "Jean",
        "AB_NOM": "Dupont",
        "AB_DATE_NAISSANCE": "1990-05-12",
        "AB_EMAIL": "jean@example.com",
    }

    result = map_customer(customer, "abassurance")

    assert result["customer_id"] == "AB-100"
    assert result["full_name"] == "Jean Dupont"
    assert result["birth_date"] == "1990-05-12"
    assert set(result) == {field.name for field in fields(Customer)}


def test_sqlserver_customer_mapping():
    customer = {
        "AP_USER_ID": 100,
        "AP_FULL_NAME": "Marie Martin",
        "AP_BIRTH_DATE": "1985-02-20",
        "AP_MAIL_ADDRESS": "marie@example.com",
    }

    result = map_customer(customer, "assureplus")

    assert result["customer_id"] == "AP-100"
    assert result["full_name"] == "Marie Martin"
    assert result["birth_date"] == "1985-02-20"
    assert set(result) == {field.name for field in fields(Customer)}


def test_complete_oracle_customer():
    customer = {
        "AB_CLIENT_ID": 100,
        "AB_PRENOM": "Jean",
        "AB_NOM": "Dupont",
        "AB_DATE_NAISSANCE": "1990-05-12",
        "AB_EMAIL": "jean@example.com",
        "AB_TELEPHONE": "0600000000",
        "AB_ADRESSE": "10 rue de Paris",
        "AB_CODE_POSTAL": "75001",
        "AB_NUM_FISCAL": "123456789",
        "AB_DATE_CREATION": "2025-01-15 10:30:00",
        "AB_STATUT_CLIENT": "ACTIF",
    }

    result = map_customer(customer, "abassurance")

    assert result["customer_id"] == "AB-100"
    assert result["source_system"] == "abassurance"
    assert result["source_id"] == "100"
    assert result["full_name"] == "Jean Dupont"
    assert result["birth_date"] == "1990-05-12"
    assert result["postal_code"] == "75001"
    assert result["tax_number"] == "123456789"
    assert result["customer_status"] == "ACTIF"
    assert result["created_at"] == "2025-01-15T10:30:00"
    assert result["loyalty_score"] is None


def test_invalid_birth_date():
    customer = {
        "AB_CLIENT_ID": 101,
        "AB_PRENOM": "Paul",
        "AB_NOM": "Martin",
        "AB_DATE_NAISSANCE": "invalid-date",
    }

    with pytest.raises(ValueError):
        map_customer(customer, "abassurance")


def test_unknown_customer_source():
    with pytest.raises(ValueError):
        map_customer({}, "unknown")