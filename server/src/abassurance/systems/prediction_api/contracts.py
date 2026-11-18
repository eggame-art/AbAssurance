import logging
from datetime import UTC, date, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from abassurance.shared.environment import Environment
from abassurance.shared.spark_runtime import SparkBusy
from abassurance.systems.data_api.consolidated import committed_run
from abassurance.systems.data_integration.extraction.config import PROJECT_ROOT
from abassurance.systems.data_preparation.features import PreparedDataset
from abassurance.systems.prediction_api.routes import get_service
from abassurance.systems.prediction_api.schemas import PredictionRequest, PredictionResponse
from abassurance.systems.prediction_api.service import PredictionService
from abassurance.systems.spark_pipeline.hdfs import HdfsState, read_snapshot

LOGGER = logging.getLogger(__name__)

router = APIRouter(tags=["prediction"])


class ContractInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: UUID
    contract_id: UUID
    integration_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    observation_date: date


class ContractResult(BaseModel):
    customer_id: UUID
    contract_id: UUID
    integration_id: str
    snapshot: str
    observation_date: date
    features: PredictionRequest
    prediction: PredictionResponse


class PersonalContractInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    contract_id: UUID
    integration_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    observation_date: date


def predict_contract(
    spark, body: ContractInput, service: PredictionService, *, owner: tuple[str, str] | None = None
) -> ContractResult:
    from pyspark.sql import functions as F

    run = committed_run(body.integration_id)
    snapshot = run.snapshot

    if not snapshot:
        raise ValueError("Committed run has no snapshot")

    env = Environment.from_file(PROJECT_ROOT / ".env")
    root = f"{env.get('INTEGRATION_HDFS_ROOT', 'hdfs://namenode:9000/abassurance-web').rstrip('/')}/{run.id}"

    manifest = HdfsState(spark, root).read_json("CURRENT.json")

    if manifest.get("snapshot") != snapshot:
        raise ValueError("Snapshot does not match the committed integration")

    frames = {
        entity: read_snapshot(spark, root, entity, manifest=manifest).drop("_kafka")
        for entity in ("customers", "contracts", "claims", "payments")
    }

    customer_id = str(body.customer_id)
    contract_id = str(body.contract_id)

    contracts = frames["contracts"].filter(
        (F.col("contract_id") == contract_id) & (F.col("customer_id") == customer_id)
    )
    if owner is not None:
        from abassurance.systems.data_integration.mapping.identifiers import make_id

        if customer_id != make_id(owner[0], owner[1], "customers"):
            raise HTTPException(404, "Contract not found for this customer.")
        contracts = contracts.filter(F.col("source_system") == owner[0])

    selected = contracts.limit(2).collect()

    if len(selected) != 1:
        raise HTTPException(status_code=404, detail="Contract not found for this customer.")

    source = selected[0].source_system

    frames["customers"] = frames["customers"].filter(
        (F.col("customer_id") == customer_id) & (F.col("source_system") == source)
    )

    frames["contracts"] = contracts

    for entity in ("claims", "payments"):
        frames[entity] = frames[entity].filter(
            (F.col("contract_id") == contract_id) & (F.col("source_system") == source)
        )

    prepared = PreparedDataset(frames, body.observation_date.isoformat())

    try:
        row = prepared.dataset.first()

        if row is None:
            raise HTTPException(status_code=422, detail="Contract data is invalid or has not started at this date.")

        values = row.asDict()

        features = PredictionRequest.model_validate(
            {
                name: float(values[name])
                if name == "payment_amount_known_total" and values[name] is not None
                else values[name]
                for name in service.info.feature_columns
            }
        )

        prediction = service.predict(features)

        return ContractResult(
            customer_id=body.customer_id,
            contract_id=body.contract_id,
            integration_id=run.id,
            snapshot=snapshot,
            observation_date=body.observation_date,
            features=features,
            prediction=prediction,
        )
    finally:
        prepared.close()


@router.post(
    "/predict/contract",
    response_model=ContractResult,
    summary="Prédire pour un contrat",
    description="Administrateur. Calcule les caractéristiques du contrat à la date choisie depuis "
    "les données consolidées, puis renvoie la prédiction. Nécessite une intégration réussie et un modèle disponible.",
)
async def contract_prediction(
    body: ContractInput, request: Request, service: Annotated[PredictionService, Depends(get_service)]
) -> ContractResult:
    return await execute_prediction(body, request, service)


@router.post("/me/predict/contract", response_model=ContractResult, summary="Prédire sur un de ses contrats")
async def personal_contract_prediction(
    body: PersonalContractInput, request: Request, service: Annotated[PredictionService, Depends(get_service)]
) -> ContractResult:
    from abassurance.systems.data_integration.mapping.identifiers import make_id

    user = request.state.user
    if not user.get("source") or not user.get("source_id"):
        raise HTTPException(403, "No customer is associated with this account.")
    selected = ContractInput(
        customer_id=UUID(make_id(user["source"], user["source_id"], "customers")), **body.model_dump()
    )
    return await execute_prediction(selected, request, service, owner=(user["source"], user["source_id"]))


async def execute_prediction(
    body: ContractInput, request: Request, service: PredictionService, *, owner: tuple[str, str] | None = None
) -> ContractResult:
    if body.observation_date > datetime.now(UTC).date():
        raise HTTPException(status_code=422, detail="Observation date cannot be in the future.")

    try:
        return await request.app.state.spark_runtime.run(
            lambda spark: (
                predict_contract(spark, body, service, owner=owner)
                if owner is not None
                else predict_contract(spark, body, service)
            ),
            timeout=90,
        )
    except SparkBusy as exc:
        raise HTTPException(429, "Prediction service is busy. Try again later.", headers={"Retry-After": "3"}) from exc
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Consolidated snapshot unavailable.") from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail="Contract prediction timed out.") from exc
    except Exception as exc:
        LOGGER.exception("Contract prediction failed")
        raise HTTPException(status_code=503, detail="Contract prediction unavailable.") from exc
