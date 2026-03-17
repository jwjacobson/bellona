import uuid
from pathlib import Path

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, Form, Request, UploadFile
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.api.ui.templates import templates
from bellona.core.config import get_settings
from bellona.db.session import get_db
from bellona.models.system import IngestionJob
from bellona.services.ingestion import (
    create_connector,
    create_ingestion_job,
    get_connector,
    list_connectors,
    run_ingestion_job,
)

logger = structlog.get_logger()
router = APIRouter(prefix="/connectors")


@router.get("")
async def connectors_index(request: Request, db: AsyncSession = Depends(get_db)):
    connectors = await list_connectors(db)
    return templates.TemplateResponse(
        request, "connectors/index.html", {"connectors": connectors}
    )


@router.post("")
async def create_rest_connector(
    request: Request,
    name: str = Form(...),
    type: str = Form("rest_api"),
    base_url: str = Form(...),
    auth_type: str = Form("none"),
    pagination_strategy: str = Form("none"),
    record_path: str = Form("$.data"),
    db: AsyncSession = Depends(get_db),
):
    config = {
        "base_url": base_url,
        "auth_type": auth_type,
        "pagination_strategy": pagination_strategy,
        "record_path": record_path,
    }
    connector = await create_connector(db, type, name, config)
    await db.commit()
    return RedirectResponse(url=f"/ui/connectors/{connector.id}", status_code=303)


@router.post("/csv")
async def upload_csv_connector(
    request: Request,
    name: str = Form(...),
    file: UploadFile = None,  # type: ignore[assignment]
    db: AsyncSession = Depends(get_db),
):
    settings = get_settings()
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"{uuid.uuid4().hex}_{file.filename or 'upload.csv'}"
    file_path = data_dir / safe_name
    content = await file.read()
    file_path.write_bytes(content)

    connector = await create_connector(db, "csv", name, {"file_path": str(file_path)})
    await db.commit()
    return RedirectResponse(url=f"/ui/connectors/{connector.id}", status_code=303)


@router.get("/{connector_id}")
async def connector_detail(
    request: Request,
    connector_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    connector = await get_connector(db, connector_id)
    if connector is None:
        return templates.TemplateResponse(request, "404.html", {}, status_code=404)

    result = await db.execute(
        select(IngestionJob)
        .where(IngestionJob.connector_id == connector_id)
        .order_by(IngestionJob.started_at.desc())
        .limit(20)
    )
    jobs = list(result.scalars().all())

    return templates.TemplateResponse(
        request, "connectors/detail.html", {"connector": connector, "jobs": jobs}
    )


@router.post("/{connector_id}/sync")
async def trigger_sync(
    request: Request,
    connector_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    connector = await get_connector(db, connector_id)
    if connector is None:
        return templates.TemplateResponse(request, "404.html", {}, status_code=404)
    job = await create_ingestion_job(db, connector_id)
    await db.commit()
    background_tasks.add_task(run_ingestion_job, job.id)
    return RedirectResponse(url=f"/ui/connectors/{connector_id}", status_code=303)
