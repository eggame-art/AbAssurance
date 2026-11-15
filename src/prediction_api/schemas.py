from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

ModelStatus = Literal["experimental", "data_validated"]
DayCount = Annotated[int, Field(strict=True, ge=0, le=2**31 - 1)]
EventCount = Annotated[int, Field(strict=True, ge=0, le=2**63 - 1)]
Amount = Annotated[float, Field(strict=True, ge=0, allow_inf_nan=False)]


class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_tenure_days: DayCount | None = None
    claim_count: EventCount | None = None
    payment_count: EventCount | None = None
    payment_amount_known_total: Amount | None = None
    payment_amount_missing_count: EventCount | None = None
    days_since_last_payment: DayCount | None = None


class ModelInfo(BaseModel):
    model_config = ConfigDict(strict=True)

    model_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    algorithm: Literal["logistic-regression", "random-forest"]
    status: ModelStatus
    business_validated: bool
    validation_scope: str
    experimental_warning: str | None
    feature_columns: list[str]
    metrics: dict[str, Any]


class PredictionResponse(BaseModel):
    cancellation_probability: float = Field(ge=0, le=1, allow_inf_nan=False)
    predicted_class: Literal[0, 1]
    model_id: str
    status: ModelStatus
    business_validated: bool


class HealthResponse(BaseModel):
    status: Literal["ok", "unavailable"]
    model_loaded: bool
    spark_available: bool
    model_id: str | None
