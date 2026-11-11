from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from mapping.identifiers import make_id
from mapping.normalizers import normalize_date, normalize_datetime
from models.unified import Customer

CUSTOMER_FIELDS = {
    "abassurance": {
        "id": "AB_CLIENT_ID",
        "first_name": "AB_PRENOM",
        "last_name": "AB_NOM",
        "full_name": None,
        "birth_date": "AB_DATE_NAISSANCE",
        "email": "AB_EMAIL",
        "phone": "AB_TELEPHONE",
        "address": "AB_ADRESSE",
        "postal_code": "AB_CODE_POSTAL",
        "tax_number": "AB_NUM_FISCAL",
        "created_at": "AB_DATE_CREATION",
        "status": "AB_STATUT_CLIENT",
        "loyalty_score": None,
    },
    "assureplus": {
        "id": "AP_USER_ID",
        "first_name": None,
        "last_name": None,
        "full_name": "AP_FULL_NAME",
        "birth_date": "AP_BIRTH_DATE",
        "email": "AP_MAIL_ADDRESS",
        "phone": "AP_PHONE_NUMBER",
        "address": "AP_STREET_ADDRESS",
        "postal_code": "AP_ZIP_CODE",
        "tax_number": None,
        "created_at": "AP_CREATED_AT",
        "status": "AP_CUSTOMER_STATUS",
        "loyalty_score": "AP_LOYALTY_SCORE",
    },
}


def map_customer(record: Mapping[str, Any], source: str) -> dict[str, Any]:
    if source not in CUSTOMER_FIELDS:
        raise ValueError(f"Unknown source: {source}")

    fields = CUSTOMER_FIELDS[source]
    source_id = record[fields["id"]]

    if fields["full_name"]:
        full_name = record.get(fields["full_name"])
    else:
        first_name = record[fields["first_name"]]
        last_name = record[fields["last_name"]]
        full_name = f"{first_name} {last_name}".strip()

    tax_field = fields["tax_number"]
    loyalty_field = fields["loyalty_score"]

    return asdict(
        Customer(
            customer_id=make_id(source, source_id),
            source_system=source,
            source_id=str(source_id),
            full_name=full_name,
            birth_date=normalize_date(record.get(fields["birth_date"])),
            email=record.get(fields["email"]),
            phone_number=record.get(fields["phone"]),
            address=record.get(fields["address"]),
            postal_code=record.get(fields["postal_code"]),
            tax_number=record.get(tax_field) if tax_field else None,
            created_at=normalize_datetime(record.get(fields["created_at"])),
            customer_status=record.get(fields["status"]),
            loyalty_score=record.get(loyalty_field) if loyalty_field else None,
        )
    )
