import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from bellona.connectors.base import BaseConnector
from bellona.connectors.csv_connector import CSVConnector
from bellona.connectors.rest_connector import RESTConnector
from bellona.core.logging import bind_job_context
from bellona.db.session import AsyncSessionLocal
from bellona.models.entities import Entity, Relationship
from bellona.models.ontology import EntityType, RelationshipType
from bellona.models.system import Connector, FieldMapping, IngestionJob
from bellona.ontology.validator import validate_record
from bellona.schemas.connectors import ConnectorPatch
from bellona.schemas.ontology import PropertyDefinitionCreate

logger = structlog.get_logger()


# ── Connector CRUD ────────────────────────────────────────────────────────────


async def create_connector(
    db: AsyncSession,
    connector_type: str,
    name: str,
    config: dict[str, Any],
) -> Connector:
    connector = Connector(type=connector_type, name=name, config=config)
    db.add(connector)
    await db.flush()
    logger.info(
        "connector created",
        connector_id=str(connector.id),
        type=connector_type,
        name=name,
    )
    return connector


async def get_connector(db: AsyncSession, connector_id: uuid.UUID) -> Connector | None:
    return await db.get(Connector, connector_id)


async def list_connectors(db: AsyncSession) -> list[Connector]:
    result = await db.execute(select(Connector).order_by(Connector.name))
    return list(result.scalars().all())


async def create_field_mapping(
    db: AsyncSession,
    connector_id: uuid.UUID,
    entity_type_id: uuid.UUID,
    mapping_config: dict[str, Any],
    proposed_by: str = "user",
) -> FieldMapping:
    mapping = FieldMapping(
        connector_id=connector_id,
        entity_type_id=entity_type_id,
        mapping_config=mapping_config,
        status="confirmed",
        proposed_by=proposed_by,
    )
    db.add(mapping)
    await db.flush()
    logger.info(
        "field mapping created",
        mapping_id=str(mapping.id),
        connector_id=str(connector_id),
        entity_type_id=str(entity_type_id),
        field_count=len(mapping_config.get("mappings", [])),
    )
    return mapping


async def get_field_mapping(
    db: AsyncSession, mapping_id: uuid.UUID
) -> FieldMapping | None:
    return await db.get(FieldMapping, mapping_id)


async def create_ingestion_job(
    db: AsyncSession, connector_id: uuid.UUID
) -> IngestionJob:
    job = IngestionJob(connector_id=connector_id, status="pending")
    db.add(job)
    await db.flush()
    logger.info(
        "ingestion job queued", job_id=str(job.id), connector_id=str(connector_id)
    )
    return job


async def get_ingestion_job(db: AsyncSession, job_id: uuid.UUID) -> IngestionJob | None:
    return await db.get(IngestionJob, job_id)


def _create_connector_instance(connector: Connector) -> BaseConnector:
    if connector.type == "csv":
        return CSVConnector(
            connector_id=connector.id,
            file_path=connector.config["file_path"],
            name=connector.name,
        )
    if connector.type == "rest_api":
        return RESTConnector(
            connector_id=connector.id,
            config=connector.config,
            name=connector.name,
        )
    raise ValueError(f"Unknown connector type: {connector.type!r}")


def _apply_mapping(
    data: dict[str, Any],
    mapping_config: dict[str, Any],
) -> dict[str, Any]:
    mappings = mapping_config.get("mappings", [])
    if not mappings:
        return data

    result: dict[str, Any] = {}
    for m in mappings:
        source = m["source_field"]
        target = m["target_property"]
        if source in data:
            result[target] = data[source]
        else:
            logger.warning(
                "mapped source field missing from record",
                source_field=source,
                target_property=target,
            )
    return result


def _serialize_for_json(props: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for k, v in props.items():
        if isinstance(v, datetime):
            result[k] = v.isoformat()
        elif isinstance(v, date):
            result[k] = v.isoformat()
        elif isinstance(v, Decimal):
            result[k] = float(v)
        elif isinstance(v, (str, int, float, bool, type(None), list, dict)):
            result[k] = v
        else:
            raise TypeError(
                f"Cannot serialize property {k!r}: unsupported type {type(v).__name__}"
            )
    return result


async def _resolve_relationships(
    db: AsyncSession,
    source_entity_type_id: uuid.UUID,
    source_entities: list[Entity],
) -> int:
    """Create Relationship rows for confirmed RelationshipTypes whose source
    entity type matches the entity type just ingested. Skip when the source
    property is null, or when no target entity with the matching value exists
    — Issue #2 covers retroactive resolution for later-arriving targets."""
    if not source_entities:
        return 0

    rt_result = await db.execute(
        select(RelationshipType).where(
            RelationshipType.source_entity_type_id == source_entity_type_id
        )
    )
    rel_types = list(rt_result.scalars().all())

    created = 0
    for rt in rel_types:
        src_prop = rt.source_property
        tgt_prop = rt.target_property
        if not src_prop or not tgt_prop:
            logger.debug(
                "skipping relationship type without source/target property",
                relationship_type_id=str(rt.id),
                name=rt.name,
            )
            continue

        refs: list[tuple[Entity, Any]] = []
        for entity in source_entities:
            val = entity.properties.get(src_prop)
            if val is None:
                continue
            refs.append((entity, val))
        if not refs:
            continue

        unique_values = list({str(v) for _, v in refs})
        target_result = await db.execute(
            select(Entity).where(
                Entity.entity_type_id == rt.target_entity_type_id,
                Entity.properties[tgt_prop].astext.in_(unique_values),
            )
        )
        by_value: dict[str, Entity] = {
            str(t.properties.get(tgt_prop)): t
            for t in target_result.scalars().all()
        }

        rt_created = 0
        for entity, val in refs:
            target = by_value.get(str(val))
            if target is None:
                continue
            db.add(
                Relationship(
                    relationship_type_id=rt.id,
                    source_entity_id=entity.id,
                    target_entity_id=target.id,
                )
            )
            rt_created += 1

        created += rt_created
        logger.info(
            "relationships resolved for type",
            relationship_type_id=str(rt.id),
            name=rt.name,
            created=rt_created,
            unresolved=len(refs) - rt_created,
        )

    if created:
        await db.flush()
    return created


async def _load_entity_type(
    db: AsyncSession, entity_type_id: uuid.UUID
) -> EntityType | None:
    res = await db.execute(
        select(EntityType)
        .where(EntityType.id == entity_type_id)
        .options(selectinload(EntityType.property_definitions))
        .execution_options(populate_existing=True)
    )
    return res.scalar_one_or_none()


async def _execute_ingestion_job(job_id: uuid.UUID, db: AsyncSession) -> None:
    """Core ingestion logic. Accepts an existing session for testability."""
    # Bind job_id to contextvars so all logger calls in this task include it,
    # including calls in helper functions like _apply_mapping.
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(job_id=str(job_id))

    job = await db.get(IngestionJob, job_id)
    if job is None:
        logger.warning("ingestion job not found")
        return

    job.status = "running"
    job.started_at = datetime.now(UTC)
    await db.flush()

    try:
        connector_model = await db.get(Connector, job.connector_id)
        if connector_model is None:
            raise ValueError(f"Connector {job.connector_id} not found")

        structlog.contextvars.bind_contextvars(
            connector_id=str(connector_model.id),
            connector_type=connector_model.type,
        )
        logger.info("ingestion job started", connector_name=connector_model.name)

        mapping_result = await db.execute(
            select(FieldMapping)
            .where(
                FieldMapping.connector_id == job.connector_id,
                FieldMapping.status == "confirmed",
            )
            .order_by(FieldMapping.id.desc())
            .limit(1)
        )
        mapping = mapping_result.scalar_one_or_none()
        if mapping is None:
            raise ValueError("No confirmed field mapping found for this connector")

        entity_type = await _load_entity_type(db, mapping.entity_type_id)
        if entity_type is None:
            raise ValueError(f"Entity type {mapping.entity_type_id} not found")

        logger.info(
            "ingestion setup complete",
            entity_type=entity_type.name,
            mapping_id=str(mapping.id),
            mapped_fields=len(mapping.mapping_config.get("mappings", [])),
        )

        prop_defs = [
            PropertyDefinitionCreate(
                name=p.name,
                data_type=p.data_type,
                required=p.required,
                constraints=p.constraints,
                description=p.description,
            )
            for p in entity_type.property_definitions
        ]

        connector_instance = _create_connector_instance(connector_model)

        records_processed = 0
        records_failed = 0
        error_entries: list[dict[str, Any]] = []
        new_entities: list[Entity] = []

        async for source_record in connector_instance.fetch_records():
            mapped = _apply_mapping(source_record.data, mapping.mapping_config)
            result = validate_record(mapped, prop_defs)

            if result.valid:
                props = _serialize_for_json(result.coerced)
                entity = Entity(
                    entity_type_id=entity_type.id,
                    properties=props,
                    schema_version=entity_type.schema_version,
                    source_connector_id=connector_model.id,
                    source_record_id=source_record.source_metadata.get(
                        "source_identifier"
                    ),
                )
                db.add(entity)
                new_entities.append(entity)
                records_processed += 1
            else:
                records_failed += 1
                error_entries.append(
                    {
                        "record_index": records_processed + records_failed - 1,
                        "errors": [
                            {"field": e.field, "message": e.message}
                            for e in result.errors
                        ],
                    }
                )
                logger.debug(
                    "record validation failed",
                    record_index=records_processed + records_failed - 1,
                    errors=[
                        {"field": e.field, "message": e.message} for e in result.errors
                    ],
                )

        await db.flush()

        relationships_created = 0
        try:
            relationships_created = await _resolve_relationships(
                db, entity_type.id, new_entities
            )
        except Exception as exc:
            logger.warning(
                "relationship resolution failed; entities were ingested",
                error=str(exc),
                exc_info=True,
            )

        job.status = "completed"
        job.records_processed = records_processed
        job.records_failed = records_failed
        job.error_log = {"errors": error_entries} if error_entries else None
        job.completed_at = datetime.now(UTC)
        connector_model.last_sync_at = datetime.now(UTC)
        await db.flush()

        log_fn = logger.warning if records_failed else logger.info
        log_fn(
            "ingestion job completed",
            records_processed=records_processed,
            records_failed=records_failed,
            relationships_created=relationships_created,
        )

    except Exception as exc:
        logger.error("ingestion job failed", error=str(exc), exc_info=True)
        job.status = "failed"
        job.error_log = {"error": str(exc)}
        job.completed_at = datetime.now(UTC)
        await db.flush()


async def run_ingestion_job(job_id: uuid.UUID) -> None:
    """Background task entry point. Creates its own DB session."""
    logger.info("ingestion background task started", job_id=str(job_id))
    async with AsyncSessionLocal() as db:
        try:
            await _execute_ingestion_job(job_id, db)
            await db.commit()
        except Exception:
            await db.rollback()
            logger.error(
                "ingestion background task failed", job_id=str(job_id), exc_info=True
            )
            raise


async def delete_connector(db: AsyncSession, connector_id: uuid.UUID) -> None:
    """Delete a connector. CASCADE handles ingestion jobs and field mappings.
    Entity source_connector_id is SET NULL."""
    connector = await db.get(Connector, connector_id)
    if connector is None:
        raise ValueError("Connector not found")

    name = connector.name
    await db.delete(connector)
    await db.flush()

    logger.info("connector deleted", connector_id=str(connector_id), name=name)


async def patch_connector(
    db: AsyncSession, connector: Connector, data: ConnectorPatch
) -> Connector:
    if data.name is not None:
        connector.name = data.name
    if data.config is not None:
        connector.config = data.config
    await db.flush()
    logger.info(
        "connector patched",
        connector_id=str(connector.id),
        name=connector.name,
    )
    return connector
