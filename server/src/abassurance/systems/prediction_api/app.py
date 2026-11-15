from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from abassurance.systems.prediction_api.config import APIConfig
from abassurance.systems.prediction_api.loader import load_service
from abassurance.systems.prediction_api.routes import router


def create_app(config: APIConfig | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        service = await run_in_threadpool(load_service, config or APIConfig.from_environment())
        app.state.prediction_service = service
        try:
            yield
        finally:
            app.state.prediction_service = None
            await run_in_threadpool(service.close)

    app = FastAPI(title="AbAssurance prediction API", version="0.1.0", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"detail": [{key: error[key] for key in ("loc", "msg", "type")} for error in exc.errors()]},
        )

    app.include_router(router)
    return app
