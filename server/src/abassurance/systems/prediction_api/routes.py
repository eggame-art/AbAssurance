import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from abassurance.systems.prediction_api.schemas import HealthResponse, ModelInfo, PredictionRequest, PredictionResponse
from abassurance.systems.prediction_api.service import PredictionService, PredictionUnavailable, UnexpectedFeatures

LOGGER = logging.getLogger(__name__)
router = APIRouter()


def get_service(request: Request) -> PredictionService:
    service = getattr(request.app.state, "prediction_service", None)

    if service is None or not service.available:
        raise HTTPException(status_code=503, detail="Model runtime is unavailable")

    return service


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Vérifier le moteur de prédiction",
    description="Administrateur. Indique si le modèle est chargé et disponible. "
    "Ne charge pas le modèle ; répond 503 tant qu'il n'est pas disponible.",
)
def health(request: Request, response: Response) -> HealthResponse:
    service = getattr(request.app.state, "prediction_service", None)
    available = service is not None and service.available

    if not available:
        response.status_code = 503

    return HealthResponse(
        status="ok" if available else "unavailable",
        model_loaded=service is not None,
        spark_available=available,
        model_id=service.info.model_id if service is not None else None,
    )


@router.get(
    "/model",
    response_model=ModelInfo,
    summary="Consulter le modèle",
    description="Administrateur. Renvoie l'algorithme, les variables et les métriques du modèle. "
    "Répond 503 si le modèle est absent ou indisponible.",
)
def model_info(service: Annotated[PredictionService, Depends(get_service)]) -> ModelInfo:
    return service.info


@router.post(
    "/predict",
    response_model=PredictionResponse,
    summary="Prédire un risque de résiliation",
    description="Administrateur. Reçoit les caractéristiques d'un contrat en JSON et renvoie "
    "la probabilité de résiliation, la classe prédite et l'identifiant du modèle.",
)
def predict(
    contract: PredictionRequest, service: Annotated[PredictionService, Depends(get_service)]
) -> PredictionResponse:
    try:
        return service.predict(contract)

    except UnexpectedFeatures as exc:
        raise HTTPException(
            status_code=422,
            detail=[
                {"loc": ["body", name], "type": "extra_forbidden", "msg": "Feature is not used by the loaded model"}
                for name in exc.fields
            ],
        ) from exc

    except PredictionUnavailable as exc:
        raise HTTPException(status_code=503, detail="Model runtime is unavailable") from exc

    except Exception as exc:
        LOGGER.exception("Prediction failed for model %s", service.info.model_id)

        raise HTTPException(status_code=500, detail="Prediction failed") from exc
