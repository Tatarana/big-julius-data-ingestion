"""FastAPI application entrypoint for big-julius-data-ingestion."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.core.logging import configure_logging, get_logger
from app.routers.ingestion import router as ingestion_router

configure_logging("INFO")
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown lifecycle.

    Args:
        app: The FastAPI application instance.
    """
    logger.info("big-julius-data-ingestion service starting up.")
    try:
        from app.core.config import settings  # noqa: PLC0415
        from app.core.logging import configure_logging  # noqa: PLC0415

        configure_logging(settings.log_level)
    except Exception:  # noqa: BLE001 — missing env vars in test/dev
        pass
    yield
    logger.info("big-julius-data-ingestion service shutting down.")


app = FastAPI(
    title="big-julius-data-ingestion",
    description=(
        "A microservice for ingesting financial transaction records from AWS S3 "
        "CSV files into Google Cloud Firestore, with deduplication logic."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(ingestion_router, prefix="", tags=["Ingestion"])


@app.get(
    "/health",
    summary="Health check",
    description="Returns service health status. Used by Docker healthcheck.",
    tags=["Health"],
)
async def health() -> JSONResponse:
    """Return a simple health check payload.

    Returns:
        JSON response with status 'ok'.
    """
    return JSONResponse(content={"status": "ok"})
