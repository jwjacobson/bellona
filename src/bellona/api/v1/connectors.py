import uuid
from pathlib import Path

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, UploadFile, status
from fastapi import Form
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.core.config import get_settings
from bellona.db.session import get_db
from bellona.schemas.connectors import (
    ConnectorCreate,
    ConnectorRead,
    FieldMappingCreate,
    FieldMappingRead,
    IngestionJobRead,
    SchemaDiscoveryRead,
    SchemaFieldRead,
)
from bellona.services.ingestion import (
    create_connector,
    create_field_mapping,
    create_ingestion_job,
    delete_connector,
    get_connector,
    get_ingestion_job,
    list_connectors,
    run_ingestion_job,
    _create_connector_instance,
)

logger = structlog.get_logger()
router = APIRouter(tags=["connectors"])


# ── Connectors ────────────────────────────────────────────────────────────────


@router.post(
    "/connectors",
    response_model=ConnectorRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_connector_endpoint(
    data: ConnectorCreate, db: AsyncSession = Depends(get_db)
) -> ConnectorRead:
    connector = await create_connector(db, data.type, data.name, data.config)
    await db.commit()
    return connector  # type: ignore[return-value]


@router.get("/connectors", response_model=list[ConnectorRead])
async def list_connectors_endpoint(db: AsyncSession = Depends(get_db)) -> list[ConnectorRead]:
    return await list_connectors(db)  # type: ignore[return-value]


@router.get("/connectors/{connector_id}", response_model=ConnectorRead)
async def get_connector_endpoint(
    connector_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> ConnectorRead:
    connector = await get_connector(db, connector_id)
    if connector is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connector not found")
    return connector  # type: ignore[return-value]


@router.post(
    "/connectors/csv/upload",
    response_model=ConnectorRead,
    status_code=status.HTTP_201_CREATED,
)
async def upload_csv(
    file: UploadFile,
    name: str = Form(...),
    db: AsyncSession = Depends(get_db),
) -> ConnectorRead:
    settings = get_settings()
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"{uuid.uuid4().hex}_{file.filename or 'upload.csv'}"
    file_path = data_dir / safe_name
    content = await file.read()
    file_path.write_bytes(content)

    logger.info(
        "csv file uploaded",
        original_filename=file.filename,
        saved_path=str(file_path),
        size_bytes=len(content),
        connector_name=name,
    )

    connector = await create_connector(db, "csv", name, {"file_path": str(file_path)})
    await db.commit()
    return connector  # type: ignore[return-value]


@router.get("/connectors/{connector_id}/schema", response_model=SchemaDiscoveryRead)
async def discover_schema_endpoint(
    connector_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> SchemaDiscoveryRead:
    connector = await get_connector(db, connector_id)
    if connector is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connector not found")

    instance = _create_connector_instance(connector)
    schema = await instance.discover_schema()

    logger.info(
        "schema discovered",
        connector_id=str(connector_id),
        connector_type=connector.type,
        field_count=len(schema.fields),
        record_count_estimate=schema.record_count_estimate,
    )

    return SchemaDiscoveryRead(
        fields=[
            SchemaFieldRead(
                name=f.name,
                inferred_type=f.inferred_type,
                nullable=f.nullable,
                sample_values=f.sample_values,
            )
            for f in schema.fields
        ],
        record_count_estimate=schema.record_count_estimate,
    )


@router.post(
    "/connectors/{connector_id}/sync",
    response_model=IngestionJobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def trigger_sync(
    connector_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> IngestionJobRead:
    connector = await get_connector(db, connector_id)
    if connector is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connector not found")

    job = await create_ingestion_job(db, connector_id)
    await db.commit()

    logger.info("sync triggered", connector_id=str(connector_id), job_id=str(job.id))
    background_tasks.add_task(run_ingestion_job, job.id)
    return job  # type: ignore[return-value]


@router.delete(
    "/connectors/{connector_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_connector_endpoint(
    connector_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    try:
        await delete_connector(db, connector_id)
        await db.commit()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Connector not found"
        )

# ── Field Mappings ────────────────────────────────────────────────────────────


@router.post(
    "/mappings",
    response_model=FieldMappingRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_mapping_endpoint(
    data: FieldMappingCreate, db: AsyncSession = Depends(get_db)
) -> FieldMappingRead:
    mapping = await create_field_mapping(
        db,
        connector_id=data.connector_id,
        entity_type_id=data.entity_type_id,
        mapping_config=data.mapping_config.model_dump(),
        proposed_by=data.proposed_by,
    )
    await db.commit()
    return mapping  # type: ignore[return-value]


# ── Ingestion Jobs ────────────────────────────────────────────────────────────


@router.get("/ingestion-jobs/{job_id}", response_model=IngestionJobRead)
async def get_ingestion_job_endpoint(
    job_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> IngestionJobRead:
    job = await get_ingestion_job(db, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Ingestion job not found"
        )
    return job  # type: ignore[return-value]
