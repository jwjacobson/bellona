import json
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
from bellona.models.ontology import EntityType
from bellona.models.system import IngestionJob
from bellona.schemas.connectors import ConnectorPatch
from bellona.services.agent_service import propose_mapping, propose_schema, ProposalError
from bellona.services.ingestion import (
    create_connector,
    create_ingestion_job,
    get_connector,
    list_connectors,
    patch_connector,
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
    endpoint: str = Form(""),
    auth_type: str = Form("none"),
    pagination_strategy: str = Form("none"),
    record_path: str = Form("$.data"),
    db: AsyncSession = Depends(get_db),
):
    config = {
    "base_url": base_url,
    "endpoint": endpoint,
    "auth": {"type": auth_type},
    "records_jsonpath": record_path,
    "pagination": {
        "strategy": pagination_strategy,
    },
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


# @router.post("/discover")
# async def discover_api_ui(
#     request: Request,
#     base_url: str = Form(...),
#     db: AsyncSession = Depends(get_db),
# ):
#     from bellona.services.agent_service import discover_api

#     try:
#         proposal = await discover_api(db, base_url)
#         await db.commit()
#     except ProposalError as exc:
#         logger.warning("discovery failed", base_url=base_url, error=str(exc))
#         return RedirectResponse(url="/ui/connectors", status_code=303)
#     return RedirectResponse(url=f"/ui/proposals/{proposal.id}", status_code=303)

@router.post("/discover")
async def discover_api_ui(
    request: Request,
    base_url: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    from bellona.services.agent_service import discover_api

    try:
        proposal = await discover_api(db, base_url)
        await db.commit()
        return templates.TemplateResponse(
            request,
            "connectors/_discover_result.html",
            {"proposal": proposal, "base_url": base_url},
        )
    except ProposalError as exc:
        logger.warning("discovery failed", base_url=base_url, error=str(exc))
        return templates.TemplateResponse(
            request,
            "connectors/_discover_result.html",
            {"error": str(exc)},
        )

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
 
    # Load entity types for the mapping proposal dropdown
    et_result = await db.execute(select(EntityType).order_by(EntityType.name))
    entity_types = list(et_result.scalars().all())
 
    return templates.TemplateResponse(
        request,
        "connectors/detail.html",
        {"connector": connector, "jobs": jobs, "entity_types": entity_types},
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


@router.post("/{connector_id}/propose-schema")
async def propose_schema_ui(
    request: Request,
    connector_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    connector = await get_connector(db, connector_id)
    if connector is None:
        return templates.TemplateResponse(request, "404.html", {}, status_code=404)
    try:
        await propose_schema(db, connector_id)
        await db.commit()
    except ProposalError as exc:
        logger.warning("schema proposal failed", connector_id=str(connector_id), error=str(exc))
    return RedirectResponse(url=f"/ui/connectors/{connector_id}", status_code=303)


@router.post("/{connector_id}/propose-mapping")
async def propose_mapping_ui(
    request: Request,
    connector_id: uuid.UUID,
    entity_type_id: uuid.UUID = Form(...),
    db: AsyncSession = Depends(get_db),
):
    connector = await get_connector(db, connector_id)
    if connector is None:
        return templates.TemplateResponse(request, "404.html", {}, status_code=404)
    try:
        await propose_mapping(db, connector_id, entity_type_id)
        await db.commit()
    except ProposalError as exc:
        logger.warning(
            "mapping proposal failed",
            connector_id=str(connector_id),
            error=str(exc),
        )
    return RedirectResponse(url=f"/ui/connectors/{connector_id}", status_code=303)


@router.post("/{connector_id}/edit")
async def edit_connector(
    request: Request,
    connector_id: uuid.UUID,
    name: str = Form(...),
    base_url: str = Form(...),
    endpoint: str = Form(""),
    auth_type: str = Form("none"),
    pagination_strategy: str = Form("none"),
    page_size: str = Form(""),
    page_param: str = Form("page"),
    records_jsonpath: str = Form("$.data"),
    db: AsyncSession = Depends(get_db),
):
    connector = await get_connector(db, connector_id)
    if connector is None:
        return templates.TemplateResponse(request, "404.html", {}, status_code=404)
 
    pagination = {"strategy": pagination_strategy}
    if page_size:
        pagination["page_size"] = int(page_size)
    if pagination_strategy == "offset":
        pagination["page_param"] = page_param
 
    config = {
        "base_url": base_url,
        "endpoint": endpoint,
        "auth": {"type": auth_type},
        "records_jsonpath": records_jsonpath,
        "pagination": pagination,
    }
 
    patch_data = ConnectorPatch(name=name, config=config)
    await patch_connector(db, connector, patch_data)
    await db.commit()
    return RedirectResponse(url=f"/ui/connectors/{connector_id}", status_code=303)