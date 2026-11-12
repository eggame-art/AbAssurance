from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from mapping.fields import optional_text
from mapping.identifiers import make_id
from mapping.normalizers import normalize_amount, normalize_datetime
from models.unified import Payment

PAYMENT_FIELDS = {
    "abassurance": {
        "id": "AB_PAYMENT_ID",
        "contract": "AB_POLICY_NUMBER",
        "date": "AB_DATE_PAIEMENT",
        "amount": "AB_MONTANT",
        "method": "AB_MODE_PAIEMENT",
        "status": None,
    },
    "assureplus": {
        "id": "AP_PAYMENT_REF",
        "contract": "AP_CONTRACT_REF",
        "date": "AP_PAYMENT_DATETIME",
        "amount": "AP_AMOUNT_PAID",
        "method": "AP_PAYMENT_CHANNEL",
        "status": "AP_TRANSACTION_STATUS",
    },
}


def map_payment(record: Mapping[str, Any], source: str) -> dict[str, Any]:
    if source not in PAYMENT_FIELDS:
        raise ValueError(f"Unknown source: {source}")

    fields = PAYMENT_FIELDS[source]
    payment_id = record[fields["id"]]
    contract_ref = record[fields["contract"]]
    status_field = fields["status"]

    return asdict(
        Payment(
            payment_id=make_id(source, payment_id),
            source_system=source,
            source_id=str(payment_id),
            contract_id=make_id(source, contract_ref),
            payment_date=normalize_datetime(record.get(fields["date"])),
            amount=normalize_amount(record.get(fields["amount"])),
            payment_method=optional_text(record.get(fields["method"]), "payment_method"),
            transaction_status=optional_text(record.get(status_field), "transaction_status") if status_field else None,
        )
    )
