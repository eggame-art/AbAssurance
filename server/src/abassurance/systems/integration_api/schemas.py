from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class AcceptedCounts(BaseModel):
    customers: int = Field(ge=0)
    contracts: int = Field(ge=0)
    claims: int = Field(ge=0)
    payments: int = Field(ge=0)


class IntegrationCounts(BaseModel):
    input: int = Field(ge=0)
    accepted: AcceptedCounts
    rejected: int = Field(ge=0)
    duplicates: int = Field(ge=0)
    invalid: int = Field(ge=0)


class IntegrationRun(BaseModel):
    id: str = Field(pattern=r"^[0-9a-f]{32}$")
    status: Literal["queued", "running", "succeeded", "failed"]
    stage: Literal["pending", "etl", "spark", "verification", "completed"] = "pending"
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    etl_verified_at: datetime | None = None
    spark_verified_at: datetime | None = None
    snapshot: str | None = None
    counts: IntegrationCounts | None = None
    error: str | None = None
