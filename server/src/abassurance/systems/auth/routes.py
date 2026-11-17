from fastapi import APIRouter, HTTPException, Query, Request

from abassurance.systems.data_api.consolidated import get_consolidated
from abassurance.systems.data_api.schemas import ConsolidatedResponse, DataEntity

router = APIRouter(prefix="/me/data", tags=["personal-data"])


@router.get(
    "/{entity}",
    response_model=ConsolidatedResponse,
    summary="Consulter ses données",
    description="Renvoie une page des données du client lié au compte connecté. Le client est déterminé par la session.",
)
async def personal_data(
    entity: DataEntity, request: Request, offset: int = Query(0, ge=0), limit: int = Query(25, ge=1, le=100)
) -> ConsolidatedResponse:
    user = request.state.user

    if not user["source"] or not user["source_id"]:
        raise HTTPException(status_code=403, detail="No customer is associated with this account.")

    owner = (user["source"], user["source_id"])

    try:
        return await request.app.state.spark_runtime.run(
            lambda spark: get_consolidated(spark, entity, offset, limit, None, owner=owner)
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="No consolidated data available.") from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail="Personal data retrieval timed out.") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Personal data unavailable.") from exc
