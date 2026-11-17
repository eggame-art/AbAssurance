from dataclasses import dataclass
from typing import Annotated

UUIDValue = Annotated[str, "uuid"]
DateValue = Annotated[str, "date"]
DateTimeValue = Annotated[str, "datetime"]
DecimalValue = Annotated[str, "decimal"]


@dataclass(frozen=True, slots=True)
class Customer:
    customer_id: UUIDValue
    source_system: str
    source_id: str
    full_name: str | None
    birth_date: DateValue | None
    email: str | None
    phone_number: str | None
    address: str | None
    postal_code: str | None
    tax_number: str | None
    created_at: DateTimeValue | None
    customer_status: str | None
    loyalty_score: DecimalValue | None


@dataclass(frozen=True, slots=True)
class Contract:
    contract_id: UUIDValue
    source_system: str
    source_reference: str
    customer_id: UUIDValue
    product_type: str | None
    start_date: DateValue | None
    end_date: DateValue | None
    annual_premium: DecimalValue | None
    contract_status: str | None
    agency_id: int | None
    broker_code: str | None


@dataclass(frozen=True, slots=True)
class Claim:
    claim_id: UUIDValue
    source_system: str
    source_id: str
    contract_id: UUIDValue
    incident_date: DateTimeValue | None
    estimated_amount: DecimalValue | None
    claim_status: str | None
    description: str | None
    fraud_score: DecimalValue | None


@dataclass(frozen=True, slots=True)
class Payment:
    payment_id: UUIDValue
    source_system: str
    source_id: str
    contract_id: UUIDValue
    payment_date: DateTimeValue | None
    amount: DecimalValue | None
    payment_method: str | None
    transaction_status: str | None
