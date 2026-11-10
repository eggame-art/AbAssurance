from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Customer:
    customer_id: str
    source_system: str
    source_id: str
    full_name: str | None
    birth_date: str | None
    email: str | None
    phone_number: str | None
    address: str | None
    postal_code: str | None
    tax_number: str | None
    created_at: str | None
    customer_status: str | None
    loyalty_score: int | None


@dataclass(frozen=True, slots=True)
class Contract:
    contract_id: str
    source_system: str
    source_reference: str
    customer_id: str
    product_type: str | None
    start_date: str | None
    end_date: str | None
    annual_premium: str | None
    contract_status: str | None
    agency_id: int | None
    broker_code: str | None


@dataclass(frozen=True, slots=True)
class Claim:
    claim_id: str
    source_system: str
    source_id: str
    contract_id: str
    incident_date: str | None
    estimated_amount: str | None
    claim_status: str | None
    description: str | None
    fraud_score: str | None


@dataclass(frozen=True, slots=True)
class Payment:
    payment_id: str
    source_system: str
    source_id: str
    contract_id: str
    payment_date: str | None
    amount: str | None
    payment_method: str | None
    transaction_status: str | None
