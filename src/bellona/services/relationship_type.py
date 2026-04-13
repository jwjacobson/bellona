import uuid
import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.ontology import EntityType, RelationshipType
from bellona.schemas.ontology import RelationshipTypeCreate

logger = structlog.get_logger()


async def create_relationship_type(
    db: AsyncSession, data: RelationshipTypeCreate
) -> RelationshipType:
    source = await db.get(EntityType, data.source_entity_type_id)
    target = await db.get(EntityType, data.target_entity_type_id)
    if source is None or target is None:
        raise ValueError("Source or target entity type not found")

    rel_type = RelationshipType(
        name=data.name,
        source_entity_type_id=data.source_entity_type_id,
        target_entity_type_id=data.target_entity_type_id,
        cardinality=data.cardinality,
        properties=data.properties,
    )
    db.add(rel_type)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        raise

    logger.info(
        "relationship type created",
        relationship_type_id=str(rel_type.id),
        name=rel_type.name,
        source=source.name,
        target=target.name,
        cardinality=rel_type.cardinality,
    )
    return rel_type


async def list_relationship_types(db: AsyncSession) -> list[RelationshipType]:
    result = await db.execute(select(RelationshipType).order_by(RelationshipType.name))
    return list(result.scalars().all())


async def delete_relationship_type(
    db: AsyncSession, relationship_type_id: uuid.UUID
) -> None:
    """Delete a relationship type. Raises IntegrityError if relationships
    still reference it (due to RESTRICT foreign key)."""
    rel_type = await db.get(RelationshipType, relationship_type_id)
    if rel_type is None:
        raise ValueError("Relationship type not found")

    name = rel_type.name
    await db.delete(rel_type)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        raise

    logger.info(
        "relationship type deleted",
        relationship_type_id=str(relationship_type_id),
        name=name,
    )
