from datetime import date, datetime
from decimal import Decimal
from typing import TypedDict


class OracleCustomer(TypedDict):
    AB_CLIENT_ID: int
    AB_NOM: str
    AB_PRENOM: str
    AB_DATE_NAISSANCE: date
    AB_EMAIL: str
    AB_TELEPHONE: str
    AB_ADRESSE: str
    AB_CODE_POSTAL: str
    AB_NUM_FISCAL: str
    AB_DATE_CREATION: datetime
    AB_STATUT_CLIENT: str


class OracleContract(TypedDict):
    AB_POLICY_NUMBER: str
    AB_CLIENT_ID: int
    AB_TYPE_ASSURANCE: str
    AB_DATE_DEBUT: date
    AB_DATE_FIN: date
    AB_PRIME_ANNUELLE: Decimal
    AB_STATUT_CONTRAT: str
    AB_AGENCE_ID: int


class OracleClaim(TypedDict):
    AB_CLAIM_ID: int
    AB_POLICY_NUMBER: str
    AB_DATE_SINISTRE: datetime
    AB_MONTANT_ESTIME: Decimal
    AB_STATUT_SINISTRE: str
    AB_DESCRIPTION: str


class OraclePayment(TypedDict):
    AB_PAYMENT_ID: int
    AB_POLICY_NUMBER: str
    AB_DATE_PAIEMENT: date
    AB_MONTANT: Decimal
    AB_MODE_PAIEMENT: str
