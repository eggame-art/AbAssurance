import secrets
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request, Response

from abassurance.shared.environment import Environment
from abassurance.shared.errors import ConfigurationError
from abassurance.systems.data_integration.extraction.config import PROJECT_ROOT
from abassurance.systems.integration_api.schemas import IntegrationRun
from abassurance.systems.integration_api.service import IntegrationBusy, create_run, execute_run, latest_run

router = APIRouter(prefix="/pipeline/integrations", tags=["pipeline"])


def require_admin(request: Request, authorization: Annotated[str | None, Header()] = None) -> None:
    if getattr(request.state, "user", {}).get("role") == "admin":
        return
    expected = Environment.from_file(PROJECT_ROOT / ".env").get("PIPELINE_ADMIN_TOKEN")

    if len(expected) < 32:
        raise HTTPException(status_code=503, detail="La clé administrateur n’est pas configurée sur le serveur.")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401, detail="Clé administrateur requise.", headers={"WWW-Authenticate": "Bearer"}
        )

    if not secrets.compare_digest(authorization[7:].encode(), expected.encode()):
        raise HTTPException(status_code=403, detail="Accès administrateur refusé.")


@router.get(
    "/latest",
    response_model=IntegrationRun | None,
    summary="Voir la dernière intégration",
    description="Administrateur. Renvoie l'état, les dates et le résultat du dernier traitement, "
    "ou null s'il n'y en a pas.",
)
def get_latest(response: Response) -> IntegrationRun | None:
    response.headers["Cache-Control"] = "no-store"
    return latest_run()


@router.post(
    "",
    response_model=IntegrationRun,
    status_code=202,
    dependencies=[Depends(require_admin)],
    summary="Lancer une intégration",
    description="Administrateur. Démarre le traitement Hop, Kafka, Spark et HDFS en arrière-plan. "
    "Répond 409 si un traitement est déjà en attente ou en cours.",
)
def start_integration(tasks: BackgroundTasks, response: Response) -> IntegrationRun:
    try:
        run = create_run()
    except IntegrationBusy as exc:
        raise HTTPException(status_code=409, detail="Une intégration est déjà en attente ou en cours.") from exc
    except (FileNotFoundError, ImportError, ConfigurationError) as exc:
        raise HTTPException(status_code=503, detail="Le runtime d’intégration est absent ou mal configuré.") from exc

    tasks.add_task(execute_run, run.id)
    response.headers["Cache-Control"] = "no-store"

    return run
