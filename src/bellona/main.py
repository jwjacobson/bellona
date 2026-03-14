from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from bellona.api.ui import router as ui_router
from bellona.api.v1 import router as v1_router
from bellona.core.config import get_settings
from bellona.core.logging import setup_logging

settings = get_settings()
setup_logging(level="DEBUG" if settings.debug else "INFO")

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("bellona starting up", debug=settings.debug)
    yield


app = FastAPI(title="Bellona", version="0.1.0", lifespan=lifespan)
app.include_router(v1_router)
app.include_router(ui_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
