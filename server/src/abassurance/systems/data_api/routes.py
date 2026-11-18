import asyncio
from collections.abc import Callable
from typing import Annotated, TypeVar
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response

from abassurance.shared.errors import ConfigurationError
from abassurance.shared.spark_runtime import SparkBusy
from abassurance.systems.data_api.schemas import (
    ClaimsResponse,
    ClientsResponse,
    ConsolidatedResponse,
    ContractsResponse,
    DataEntity,
    DataSource,
    PaymentsResponse,
)
from abassurance.systems.data_api.service import READ_TIMEOUT, get_claims, get_clients, get_contracts, get_payments

router = APIRouter(prefix="/data", tags=["data"])

T = TypeVar("T")


async def read_source(source: DataSource, entity: str, fetch: Callable[[DataSource], T]) -> T:
    name = "Oracle" if source == "oracle" else "SQL Server"

    try:
        return await asyncio.wait_for(asyncio.to_thread(fetch, source), timeout=READ_TIMEOUT)
    except (ConfigurationError, ImportError) as exc:
        raise HTTPException(status_code=503, detail=f"{name} configuration or driver is unavailable.") from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=f"{name} {entity} data retrieval timed out.") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to retrieve {entity} data from {name}.") from exc


@router.get(
    "/clients",
    response_model=ClientsResponse,
    summary="Lister les clients",
    description="Administrateur. Lit les clients dans la source choisie : oracle ou sqlserver.",
)
async def clients(source: DataSource, response: Response) -> ClientsResponse:
    response.headers["Cache-Control"] = "no-store"
    return await read_source(source, "client", get_clients)


@router.get(
    "/contracts",
    response_model=ContractsResponse,
    summary="Lister les contrats",
    description="Administrateur. Lit les contrats dans la source choisie : oracle ou sqlserver.",
)
async def contracts(source: DataSource, response: Response) -> ContractsResponse:
    response.headers["Cache-Control"] = "no-store"
    return await read_source(source, "contract", get_contracts)


@router.get(
    "/claims",
    response_model=ClaimsResponse,
    summary="Lister les sinistres",
    description="Administrateur. Lit les sinistres dans la source choisie : oracle ou sqlserver.",
)
async def claims(source: DataSource, response: Response) -> ClaimsResponse:
    response.headers["Cache-Control"] = "no-store"
    return await read_source(source, "claim", get_claims)


@router.get(
    "/payments",
    response_model=PaymentsResponse,
    summary="Lister les paiements",
    description="Administrateur. Lit les paiements dans la source choisie : oracle ou sqlserver.",
)
async def payments(source: DataSource, response: Response) -> PaymentsResponse:
    response.headers["Cache-Control"] = "no-store"
    return await read_source(source, "payment", get_payments)


@router.get(
    "/consolidated/{entity}",
    response_model=ConsolidatedResponse,
    summary="Consulter les données consolidées",
    description="Administrateur. Renvoie une page des données intégrées dans HDFS. "
    "integration_id sélectionne une intégration ; par défaut, la dernière réussie. "
    "customer_id permet de filtrer les contrats d'un client.",
)
async def consolidated(
    entity: DataEntity,
    request: Request,
    response: Response,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    integration_id: Annotated[str | None, Query(pattern=r"^[0-9a-f]{32}$")] = None,
    customer_id: UUID | None = None,
) -> ConsolidatedResponse:
    from abassurance.systems.data_api.consolidated import committed_run, get_consolidated

    response.headers["Cache-Control"] = "no-store"

    if customer_id is not None and entity != "contracts":
        raise HTTPException(422, "Customer filtering requires contracts.")
    try:
        selected = committed_run(integration_id)

        return await request.app.state.spark_runtime.run(
            lambda spark: (
                get_consolidated(spark, entity, offset, limit, selected.id, customer_filter=str(customer_id))
                if customer_id is not None
                else get_consolidated(spark, entity, offset, limit, selected.id)
            )
        )
    except SparkBusy as exc:
        raise HTTPException(429, "Data service is busy. Try again later.", headers={"Retry-After": "3"}) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="No completed consolidated snapshot is available.") from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail="Consolidated data retrieval timed out.") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="The consolidated snapshot is unavailable.") from exc
