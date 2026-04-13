"""
New service: services/entity.py

Handles entity and relationship delete operations.
"""

import uuid

import structlog
from sqlalchemy import delete as sa_delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.models.entities import Entity, Relationship

logger = structlog.get_logger()


# ── Single Entity Delete ──────────────────────────────────────────────────────


async def delete_entity(db: AsyncSession, entity_id: uuid.UUID) -> None:
    """Delete an entity. CASCADE handles its relationships automatically."""
    entity = await db.get(Entity, entity_id)
    if entity is None:
        raise ValueError("Entity not found")

    await db.delete(entity)
    await db.flush()

    logger.info("entity deleted", entity_id=str(entity_id))


# ── Bulk Entity Delete ────────────────────────────────────────────────────────


async def delete_entities_bulk(db: AsyncSession, entity_ids: list[uuid.UUID]) -> int:
    """Delete specific entities by ID. Returns count of deleted entities."""
    # Delete relationships involving these entities first
    await db.execute(
        sa_delete(Relationship).where(
            or_(
                Relationship.source_entity_id.in_(entity_ids),
                Relationship.target_entity_id.in_(entity_ids),
            )
        )
    )

    result = await db.execute(sa_delete(Entity).where(Entity.id.in_(entity_ids)))
    await db.flush()

    logger.info("entities bulk deleted", count=result.rowcount)
    return result.rowcount


async def delete_entities_by_type(db: AsyncSession, entity_type_id: uuid.UUID) -> int:
    """Delete all entities of a given type. Returns count of deleted entities."""
    # Get entity IDs first for relationship cleanup
    entity_ids_result = await db.execute(
        select(Entity.id).where(Entity.entity_type_id == entity_type_id)
    )
    entity_ids = list(entity_ids_result.scalars().all())

    if not entity_ids:
        return 0

    # Delete relationships involving these entities
    await db.execute(
        sa_delete(Relationship).where(
            or_(
                Relationship.source_entity_id.in_(entity_ids),
                Relationship.target_entity_id.in_(entity_ids),
            )
        )
    )

    result = await db.execute(
        sa_delete(Entity).where(Entity.entity_type_id == entity_type_id)
    )
    await db.flush()

    logger.info(
        "entities deleted by type",
        entity_type_id=str(entity_type_id),
        count=result.rowcount,
    )
    return result.rowcount


# ── Single Relationship Delete ────────────────────────────────────────────────


async def delete_relationship(db: AsyncSession, relationship_id: uuid.UUID) -> None:
    """Delete a single relationship."""
    rel = await db.get(Relationship, relationship_id)
    if rel is None:
        raise ValueError("Relationship not found")

    await db.delete(rel)
    await db.flush()

    logger.info("relationship deleted", relationship_id=str(relationship_id))
