from dataclasses import dataclass


@dataclass(frozen=True)
class BusinessVersion:
    source_system: str
    available_at: str
    revision: int
    deleted: bool
    is_synthetic: bool


@dataclass(frozen=True)
class CustomerVersion(BusinessVersion):
    customer_id: str
    source_id: str


@dataclass(frozen=True)
class ContractVersion(BusinessVersion):
    contract_id: str
    source_reference: str
    customer_id: str
    start_date: str
    coverage_end_date: str | None


@dataclass(frozen=True)
class ClaimVersion(BusinessVersion):
    claim_id: str
    source_id: str
    contract_id: str
    incident_date: str


@dataclass(frozen=True)
class PaymentVersion(BusinessVersion):
    payment_id: str
    source_id: str
    contract_id: str
    payment_date: str
    amount: str | None


@dataclass(frozen=True)
class CancellationVersion(BusinessVersion):
    cancellation_id: str
    source_id: str
    contract_id: str
    cancellation_date: str
    cancellation_type: str


@dataclass(frozen=True)
class FollowupVersion(BusinessVersion):
    contract_id: str
    complete_from: str
    complete_until: str
    features_complete_from: str


HISTORY_MODELS = {
    "customers": CustomerVersion,
    "contracts": ContractVersion,
    "claims": ClaimVersion,
    "payments": PaymentVersion,
    "cancellations": CancellationVersion,
    "followup": FollowupVersion,
}
HISTORY_IDS = {
    "customers": "customer_id",
    "contracts": "contract_id",
    "claims": "claim_id",
    "payments": "payment_id",
    "cancellations": "cancellation_id",
    "followup": "contract_id",
}
CANCELLATION_TYPES = ("voluntary", "non_payment", "non_renewal")
EXIT_TYPES = (*CANCELLATION_TYPES, "other_exit")
