import json
import os
from datetime import datetime
from decimal import Decimal
from urllib.request import urlopen
from uuid import UUID, uuid5

import pytest

pytestmark = pytest.mark.integration


def identity(entity, source, reference):
    key = f"{entity[:-1]}:{source}:{reference}"
    return str(uuid5(UUID("12345678-1234-5678-1234-567812345678"), key))


def date_value(value, *, timestamp=False):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed.isoformat(timespec="seconds") if timestamp else parsed.date().isoformat()


def amount(value, multiplier=1):
    return None if value is None else str((Decimal(str(value)) * multiplier).quantize(Decimal("0.01")))


def test_every_common_field_matches_its_archived_source_and_all_relationships():
    base = os.environ.get("ABASSURANCE_TEST_API_URL", "http://127.0.0.1:8000")
    pages = {}
    for entity in ("clients", "contracts", "claims", "payments"):
        with urlopen(f"{base}/data/consolidated/{entity}?limit=100", timeout=60) as response:
            pages[entity] = json.load(response)
    assert len({page["snapshot"] for page in pages.values()}) == 1
    for entity, page in pages.items():
        assert page["total"] == len(page["records"]) == 4
        for record in page["records"]:
            data, raw = record["data"], record["original"]
            source = data["source_system"]
            ab = source == "abassurance"

            def value(oracle, sqlserver, row=raw, is_oracle=ab):
                return row.get(oracle if is_oracle else sqlserver)

            if entity == "clients":
                reference = str(value("AB_CLIENT_ID", "AP_USER_ID"))
                expected = dict(
                    customer_id=identity("customers", source, reference),
                    source_system=source,
                    source_id=reference,
                    full_name=f"{raw['AB_PRENOM']} {raw['AB_NOM']}" if ab else raw["AP_FULL_NAME"],
                    birth_date=date_value(value("AB_DATE_NAISSANCE", "AP_BIRTH_DATE")),
                    email=value("AB_EMAIL", "AP_MAIL_ADDRESS"),
                    phone_number=value("AB_TELEPHONE", "AP_PHONE_NUMBER"),
                    address=value("AB_ADRESSE", "AP_STREET_ADDRESS"),
                    postal_code=value("AB_CODE_POSTAL", "AP_ZIP_CODE"),
                    tax_number=raw.get("AB_NUM_FISCAL"),
                    created_at=date_value(value("AB_DATE_CREATION", "AP_CREATED_AT"), timestamp=True),
                    customer_status="ACTIVE",
                    loyalty_score=amount(raw.get("AP_LOYALTY_SCORE")),
                )
            elif entity == "contracts":
                reference = value("AB_POLICY_NUMBER", "AP_CONTRACT_REF")
                expected = dict(
                    contract_id=identity(entity, source, reference),
                    source_system=source,
                    source_reference=reference,
                    customer_id=identity("customers", source, value("AB_CLIENT_ID", "AP_USER_ID")),
                    product_type={"AUTO": "CAR", "HABITATION": "HOME"}[value("AB_TYPE_ASSURANCE", "AP_PRODUCT_CODE")],
                    start_date=date_value(value("AB_DATE_DEBUT", "AP_START_DATE")),
                    end_date=date_value(value("AB_DATE_FIN", "AP_END_DATE")),
                    annual_premium=amount(value("AB_PRIME_ANNUELLE", "AP_MONTHLY_PREMIUM"), 1 if ab else 12),
                    contract_status="ACTIVE",
                    agency_id=raw.get("AB_AGENCE_ID"),
                    broker_code=raw.get("AP_BROKER_CODE"),
                )
            elif entity == "claims":
                reference = str(value("AB_CLAIM_ID", "AP_SINISTRE_NUM"))
                expected = dict(
                    claim_id=identity(entity, source, reference),
                    source_system=source,
                    source_id=reference,
                    contract_id=identity("contracts", source, value("AB_POLICY_NUMBER", "AP_CONTRACT_REF")),
                    incident_date=date_value(value("AB_DATE_SINISTRE", "AP_INCIDENT_DATE"), timestamp=True),
                    estimated_amount=amount(value("AB_MONTANT_ESTIME", "AP_ESTIMATED_AMOUNT")),
                    claim_status={"OPEN": "OPEN", "CLOSED": "CLOSED", "OUVERT": "OPEN", "CLOS": "CLOSED"}[
                        value("AB_STATUT_SINISTRE", "AP_CLAIM_STATUS")
                    ],
                    description=value("AB_DESCRIPTION", "AP_CLAIM_COMMENT"),
                    fraud_score=amount(raw.get("AP_FRAUD_SCORE")),
                )
            else:
                reference = str(value("AB_PAYMENT_ID", "AP_PAYMENT_REF"))
                expected = dict(
                    payment_id=identity(entity, source, reference),
                    source_system=source,
                    source_id=reference,
                    contract_id=identity("contracts", source, value("AB_POLICY_NUMBER", "AP_CONTRACT_REF")),
                    payment_date=date_value(value("AB_DATE_PAIEMENT", "AP_PAYMENT_DATETIME"), timestamp=True),
                    amount=amount(value("AB_MONTANT", "AP_AMOUNT_PAID")),
                    payment_method={
                        "BANK_TRANSFER": "BANK_TRANSFER",
                        "CARD": "CARD",
                        "VIREMENT": "BANK_TRANSFER",
                        "CARTE": "CARD",
                    }[value("AB_MODE_PAIEMENT", "AP_PAYMENT_CHANNEL")],
                    transaction_status=None if ab else "COMPLETED",
                )
            assert data == expected, (entity, source, reference)

    clients = {r["data"]["customer_id"]: r["data"] for r in pages["clients"]["records"]}
    contracts = {r["data"]["contract_id"]: r["data"] for r in pages["contracts"]["records"]}
    for contract in contracts.values():
        assert clients[contract["customer_id"]]["source_system"] == contract["source_system"]
    for entity in ("claims", "payments"):
        for record in pages[entity]["records"]:
            assert contracts[record["data"]["contract_id"]]["source_system"] == record["data"]["source_system"]
