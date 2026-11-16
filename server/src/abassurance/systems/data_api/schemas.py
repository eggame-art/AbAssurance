from typing import Literal

from pydantic import BaseModel

DataSource = Literal["oracle", "sqlserver"]
DataKind = Literal["demo", "unspecified"]


class SourceClient(BaseModel):
    source_id: str
    name: str | None
    first_name: str | None
    email: str | None
    phone: str | None
    status: str | None


class ClientsResponse(BaseModel):
    source: DataSource
    data_kind: DataKind
    clients: list[SourceClient]


class SourceContract(BaseModel):
    source_reference: str
    source_client_id: str
    product_type: str | None
    start_date: str | None
    end_date: str | None
    premium: str | None
    premium_period: Literal["annual", "monthly"]
    status: str | None


class ContractsResponse(BaseModel):
    source: DataSource
    data_kind: DataKind
    contracts: list[SourceContract]


class SourceClaim(BaseModel):
    source_id: str
    source_contract_reference: str
    incident_date: str | None
    estimated_amount: str | None
    status: str | None
    description: str | None


class ClaimsResponse(BaseModel):
    source: DataSource
    data_kind: DataKind
    claims: list[SourceClaim]


class SourcePayment(BaseModel):
    source_id: str
    source_contract_reference: str
    payment_date: str | None
    amount: str | None
    method: str | None
    status: str | None


class PaymentsResponse(BaseModel):
    source: DataSource
    data_kind: DataKind
    payments: list[SourcePayment]


DataEntity = Literal["clients", "contracts", "claims", "payments"]
Scalar = str | int | float | bool | None


class ConsolidatedRecord(BaseModel):
    data: dict[str, Scalar]
    original: dict[str, Scalar]


class ConsolidatedResponse(BaseModel):
    entity: DataEntity
    integration_id: str
    snapshot: str
    data_kind: DataKind
    columns: list[str]
    records: list[ConsolidatedRecord]
    offset: int
    limit: int
    total: int
