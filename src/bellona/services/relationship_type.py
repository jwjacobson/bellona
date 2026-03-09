from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.ontology import EntityType, RelationshipType
from bellona.schemas.ontology import RelationshipTypeCreate


async def create_relationship_type(
    db: AsyncSession, data: RelationshipTypeCreate
) -> RelationshipType:
    # Verify both entity types exist
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
    return rel_type


async def list_relationship_types(db: AsyncSession) -> list[RelationshipType]:
    result = await db.execute(select(RelationshipType).order_by(RelationshipType.name))
    return list(result.scalars().all())
