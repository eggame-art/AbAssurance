from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from mapping.identifiers import make_id
from mapping.normalizers import normalize_amount, normalize_date
from models.unified import Contract

CONTRACT_FIELDS = {
    "abassurance": {
        "id": "AB_POLICY_NUMBER",
        "customer_id": "AB_CLIENT_ID",
        "product": "AB_TYPE_ASSURANCE",
        "start_date": "AB_DATE_DEBUT",
        "end_date": "AB_DATE_FIN",
        "premium": "AB_PRIME_ANNUELLE",
        "status": "AB_STATUT_CONTRAT",
        "agency": "AB_AGENCE_ID",
        "broker": None,
        "premium_multiplier": 1,
    },
    "assureplus": {
        "id": "AP_CONTRACT_REF",
        "customer_id": "AP_USER_ID",
        "product": "AP_PRODUCT_CODE",
        "start_date": "AP_START_DATE",
        "end_date": "AP_END_DATE",
        "premium": "AP_MONTHLY_PREMIUM",
        "status": "AP_CONTRACT_STATE",
        "agency": None,
        "broker": "AP_BROKER_CODE",
        "premium_multiplier": 12,
    },
}


def map_contract(record: Mapping[str, Any], source: str) -> dict[str, Any]:
    if source not in CONTRACT_FIELDS:
        raise ValueError(f"Unknown source: {source}")

    fields = CONTRACT_FIELDS[source]
    reference = record[fields["id"]]
    customer_id = record[fields["customer_id"]]
    agency_field = fields["agency"]
    broker_field = fields["broker"]

    return asdict(
        Contract(
            contract_id=make_id(source, reference),
            source_system=source,
            source_reference=str(reference),
            customer_id=make_id(source, customer_id),
            product_type=record.get(fields["product"]),
            start_date=normalize_date(record.get(fields["start_date"])),
            end_date=normalize_date(record.get(fields["end_date"])),
            annual_premium=normalize_amount(
                record.get(fields["premium"]),
                multiplier=fields["premium_multiplier"],
            ),
            contract_status=record.get(fields["status"]),
            agency_id=record.get(agency_field) if agency_field else None,
            broker_code=record.get(broker_field) if broker_field else None,
        )
    )
