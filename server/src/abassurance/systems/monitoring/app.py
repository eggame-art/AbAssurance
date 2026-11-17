import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from abassurance.shared.spark_runtime import SparkRuntime
from abassurance.systems.auth.routes import router as personal_router
from abassurance.systems.auth.service import protect
from abassurance.systems.auth.service import router as auth_router
from abassurance.systems.data_api.routes import router as data_router
from abassurance.systems.integration_api.routes import router as integration_router
from abassurance.systems.integration_api.service import recover_interrupted
from abassurance.systems.monitoring.routes import router
from abassurance.systems.prediction_api.config import APIConfig
from abassurance.systems.prediction_api.contracts import router as contract_prediction_router
from abassurance.systems.prediction_api.loader import load_service
from abassurance.systems.prediction_api.routes import get_service
from abassurance.systems.prediction_api.routes import router as prediction_router


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        recover_interrupted()
        try:
            yield
        finally:
            service = getattr(app.state, "prediction_service", None)
            if service is not None:
                await asyncio.to_thread(service.close)
            await asyncio.to_thread(app.state.spark_runtime.close)

    app = FastAPI(
        title="AbAssurance API",
        description="Connexion par cookie de session. Les routes métier sont réservées aux administrateurs, "
        "sauf /me/data qui donne accès aux données du compte connecté. "
        "Les requêtes POST doivent inclure l'en-tête X-Requested-With: AbAssurance.",
        lifespan=lifespan,
        root_path=os.environ.get("API_ROOT_PATH", "/api"),
    )
    app.middleware("http")(protect)
    app.include_router(auth_router)
    app.include_router(personal_router)
    app.state.spark_runtime = SparkRuntime()
    app.state.prediction_service = None

    async def web_prediction_service(request: Request):
        def load(spark):
            if request.app.state.prediction_service is None:
                request.app.state.prediction_service = load_service(APIConfig.from_environment(), spark=spark)
            service = request.app.state.prediction_service
            if not service.available:
                raise RuntimeError("Prediction runtime is unavailable")
            return service

        try:
            return await request.app.state.spark_runtime.run(load)
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail="Model loading timed out.") from exc
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Model runtime is unavailable.") from exc

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError):
        if request.url.path != "/predict":
            return await request_validation_exception_handler(request, exc)
        return JSONResponse(
            status_code=422,
            content={"detail": [{key: error[key] for key in ("loc", "msg", "type")} for error in exc.errors()]},
        )

    app.dependency_overrides[get_service] = web_prediction_service
    app.include_router(router)
    app.include_router(integration_router)
    app.include_router(data_router)
    app.include_router(prediction_router)
    app.include_router(contract_prediction_router)

    return app
