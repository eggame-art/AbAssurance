from datetime import datetime
from decimal import Decimal
from typing import TypedDict
from uuid import UUID


class SQLServerCustomer(TypedDict):
    AP_USER_ID: int
    AP_FULL_NAME: str
    AP_BIRTH_DATE: str
    AP_MAIL_ADDRESS: str
    AP_PHONE_NUMBER: str
    AP_STREET_ADDRESS: str
    AP_ZIP_CODE: str
    AP_CREATED_AT: datetime
    AP_CUSTOMER_STATUS: str
    AP_LOYALTY_SCORE: int


class SQLServerContract(TypedDict):
    AP_CONTRACT_REF: str
    AP_USER_ID: int
    AP_PRODUCT_CODE: str
    AP_START_DATE: datetime
    AP_END_DATE: datetime
    AP_MONTHLY_PREMIUM: Decimal
    AP_CONTRACT_STATE: str
    AP_BROKER_CODE: str


class SQLServerClaim(TypedDict):
    AP_SINISTRE_NUM: int
    AP_CONTRACT_REF: str
    AP_INCIDENT_DATE: str
    AP_ESTIMATED_AMOUNT: Decimal
    AP_CLAIM_STATUS: str
    AP_CLAIM_COMMENT: str
    AP_FRAUD_SCORE: Decimal


class SQLServerPayment(TypedDict):
    AP_PAYMENT_REF: UUID
    AP_CONTRACT_REF: str
    AP_PAYMENT_DATETIME: datetime
    AP_AMOUNT_PAID: Decimal
    AP_PAYMENT_CHANNEL: str
    AP_TRANSACTION_STATUS: str
