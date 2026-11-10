from dataclasses import asdict
from typing import Any, Mapping

from mapping.identifiers import make_id
from mapping.normalizers import normalize_amount, normalize_datetime, normalize_score
from models.unified import Claim


CLAIM_FIELDS = {
    "abassurance": {
        "id": "AB_CLAIM_ID",
        "contract": "AB_POLICY_NUMBER",
        "date": "AB_DATE_SINISTRE",
        "amount": "AB_MONTANT_ESTIME",
        "status": "AB_STATUT_SINISTRE",
        "description": "AB_DESCRIPTION",
        "score": None,
    },
    "assureplus": {
        "id": "AP_SINISTRE_NUM",
        "contract": "AP_CONTRACT_REF",
        "date": "AP_INCIDENT_DATE",
        "amount": "AP_ESTIMATED_AMOUNT",
        "status": "AP_CLAIM_STATUS",
        "description": "AP_CLAIM_COMMENT",
        "score": "AP_FRAUD_SCORE",
    },
}


def map_claim(record: Mapping[str, Any], source: str) -> dict[str, Any]:
    if source not in CLAIM_FIELDS:
        raise ValueError(f"Unknown source: {source}")

    fields = CLAIM_FIELDS[source]
    claim_id = record[fields["id"]]
    contract_ref = record[fields["contract"]]
    score_field = fields["score"]

    return asdict(
        Claim(
            claim_id=make_id(source, claim_id),
            source_system=source,
            source_id=str(claim_id),
            contract_id=make_id(source, contract_ref),
            incident_date=normalize_datetime(record.get(fields["date"])),
            estimated_amount=normalize_amount(record.get(fields["amount"])),
            claim_status=record.get(fields["status"]),
            description=record.get(fields["description"]),
            fraud_score=normalize_score(record.get(score_field)) if score_field else None,
        )
    )
