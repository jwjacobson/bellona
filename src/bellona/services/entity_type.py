import uuid

import structlog
from sqlalchemy import delete as sa_delete, func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from bellona.models.entities import Entity, Relationship
from bellona.models.ontology import EntityType, PropertyDefinition, RelationshipType
from bellona.schemas.ontology import EntityTypeCreate, EntityTypePatch
from bellona.models.system import FieldMapping

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
        logger.info(
            "GIN index created",
            entity_type_id=str(entity_type_id),
            index_name=index_name,
        )
    except Exception:
        logger.warning(
            "GIN index creation failed",
            entity_type_id=str(entity_type_id),
            exc_info=True,
        )


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


async def delete_entity_type(db: AsyncSession, entity_type_id: uuid.UUID) -> None:
    """Delete an entity type. Raises IntegrityError if entities or relationship types
    still reference it (due to RESTRICT foreign keys)."""
    entity_type = await db.get(EntityType, entity_type_id)
    if entity_type is None:
        raise ValueError("Entity type not found")

    name = entity_type.name
    await db.delete(entity_type)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        raise

    logger.info("entity type deleted", entity_type_id=str(entity_type_id), name=name)


async def delete_entity_type_cascade(
    db: AsyncSession, entity_type_id: uuid.UUID
) -> dict:
    """Delete an entity type and everything underneath it:
    relationships → entities → relationship types → field mappings → entity type.

    Returns counts of what was deleted.
    """
    entity_type = await db.get(EntityType, entity_type_id)
    if entity_type is None:
        raise ValueError("Entity type not found")

    name = entity_type.name

    # 1. Get all entity IDs for this type
    entity_ids_result = await db.execute(
        select(Entity.id).where(Entity.entity_type_id == entity_type_id)
    )
    entity_ids = list(entity_ids_result.scalars().all())

    # 2. Delete relationships involving these entities
    relationships_deleted = 0
    if entity_ids:
        result = await db.execute(
            sa_delete(Relationship).where(
                or_(
                    Relationship.source_entity_id.in_(entity_ids),
                    Relationship.target_entity_id.in_(entity_ids),
                )
            )
        )
        relationships_deleted = result.rowcount

    # 3. Delete entities of this type
    result = await db.execute(
        sa_delete(Entity).where(Entity.entity_type_id == entity_type_id)
    )
    entities_deleted = result.rowcount

    # 4. Delete relationship types that reference this entity type (as source or target)
    result = await db.execute(
        sa_delete(RelationshipType).where(
            or_(
                RelationshipType.source_entity_type_id == entity_type_id,
                RelationshipType.target_entity_type_id == entity_type_id,
            )
        )
    )
    relationship_types_deleted = result.rowcount

    # 5. Delete field mappings for this entity type
    await db.execute(
        sa_delete(FieldMapping).where(FieldMapping.entity_type_id == entity_type_id)
    )

    # 6. Delete property definitions and entity type
    # (property_definitions cascade via ORM, but being explicit)
    await db.execute(
        sa_delete(PropertyDefinition).where(
            PropertyDefinition.entity_type_id == entity_type_id
        )
    )
    await db.execute(sa_delete(EntityType).where(EntityType.id == entity_type_id))
    await db.flush()

    logger.info(
        "entity type cascade deleted",
        entity_type_id=str(entity_type_id),
        name=name,
        relationships_deleted=relationships_deleted,
        entities_deleted=entities_deleted,
        relationship_types_deleted=relationship_types_deleted,
    )

    return {
        "relationships_deleted": relationships_deleted,
        "entities_deleted": entities_deleted,
        "relationship_types_deleted": relationship_types_deleted,
        "entity_type_deleted": name,
    }
