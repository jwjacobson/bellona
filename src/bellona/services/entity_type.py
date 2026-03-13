import uuid

import structlog
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from bellona.models.ontology import EntityType, PropertyDefinition
from bellona.schemas.ontology import EntityTypeCreate, EntityTypePatch

logger = structlog.get_logger()


async def _load_entity_type(
    db: AsyncSession, entity_type_id: uuid.UUID
) -> EntityType | None:
    result = await db.execute(
        select(EntityType)
        .where(EntityType.id == entity_type_id)
        .options(selectinload(EntityType.property_definitions))
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def create_entity_type(db: AsyncSession, data: EntityTypeCreate) -> EntityType:
    entity_type = EntityType(name=data.name, description=data.description)
    db.add(entity_type)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        raise

    for prop in data.properties:
        prop_def = PropertyDefinition(
            entity_type_id=entity_type.id,
            name=prop.name,
            data_type=prop.data_type,
            required=prop.required,
            constraints=prop.constraints,
            description=prop.description,
            schema_version=entity_type.schema_version,
        )
        db.add(prop_def)

    await db.flush()
    loaded = await _load_entity_type(db, entity_type.id)
    assert loaded is not None
    logger.info(
        "entity type created",
        entity_type_id=str(loaded.id),
        name=loaded.name,
        property_count=len(loaded.property_definitions),
    )
    return loaded


async def get_entity_type(
    db: AsyncSession, entity_type_id: uuid.UUID
) -> EntityType | None:
    return await _load_entity_type(db, entity_type_id)


async def list_entity_types(db: AsyncSession) -> list[EntityType]:
    result = await db.execute(select(EntityType).order_by(EntityType.name))
    return list(result.scalars().all())


async def create_entity_type_gin_index(entity_type_id: uuid.UUID) -> None:
    """Background task: create a GIN index on entities.properties for this entity type."""
    from bellona.db.session import engine

    index_name = f"idx_entities_props_gin_{entity_type_id.hex}"
    ddl = text(
        f"CREATE INDEX IF NOT EXISTS {index_name} "
        f"ON entities USING gin (properties) "
        f"WHERE entity_type_id = '{entity_type_id}'"
    )
    try:
        async with engine.begin() as conn:
            await conn.execute(ddl)
        logger.info("GIN index created", entity_type_id=str(entity_type_id), index_name=index_name)
    except Exception:
        logger.warning("GIN index creation failed", entity_type_id=str(entity_type_id), exc_info=True)


async def patch_entity_type(
    db: AsyncSession, entity_type: EntityType, data: EntityTypePatch
) -> EntityType:
    if data.description is not None:
        entity_type.description = data.description

    if data.add_properties:
        entity_type.schema_version += 1
        for prop in data.add_properties:
            prop_def = PropertyDefinition(
                entity_type_id=entity_type.id,
                name=prop.name,
                data_type=prop.data_type,
                required=prop.required,
                constraints=prop.constraints,
                description=prop.description,
                schema_version=entity_type.schema_version,
            )
            db.add(prop_def)

    await db.flush()
    loaded = await _load_entity_type(db, entity_type.id)
    assert loaded is not None
    logger.info(
        "entity type patched",
        entity_type_id=str(loaded.id),
        name=loaded.name,
        schema_version=loaded.schema_version,
        properties_added=len(data.add_properties),
    )
    return loaded
