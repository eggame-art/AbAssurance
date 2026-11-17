from fastapi import APIRouter, Response

from abassurance.systems.monitoring.schemas import ServiceStatus
from abassurance.systems.monitoring.service import get_services_status

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.get(
    "/services",
    response_model=list[ServiceStatus],
    summary="Vérifier les services",
    description="Administrateur. Renvoie l'état d'Oracle, SQL Server, Kafka, Spark, HDFS et Hop.",
)
async def services_status(response: Response) -> list[ServiceStatus]:
    response.headers["Cache-Control"] = "no-store"
    return await get_services_status()
