"""Entity query endpoints."""
import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.db.session import get_db
from bellona.schemas.query import EntityPage, EntityQuery, EntityRead, RelationshipRead
from bellona.services.query import get_entity, get_entity_relationships, query_entities

logger = structlog.get_logger()
router = APIRouter(prefix="/entities", tags=["entities"])


@router.get("", response_model=EntityPage)
async def list_entities(
    entity_type_id: uuid.UUID | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
) -> EntityPage:
    return await query_entities(
        db,
        EntityQuery(entity_type_id=entity_type_id, page=page, page_size=page_size),
    )


@router.post("/query", response_model=EntityPage)
async def query_entities_endpoint(
    query: EntityQuery,
    db: AsyncSession = Depends(get_db),
) -> EntityPage:
    return await query_entities(db, query)


@router.get("/{entity_id}", response_model=EntityRead)
async def get_entity_endpoint(
    entity_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> EntityRead:
    entity = await get_entity(db, entity_id)
    if entity is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity not found")
    return EntityRead.model_validate(entity)


@router.get("/{entity_id}/relationships", response_model=list[RelationshipRead])
async def get_entity_relationships_endpoint(
    entity_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> list[RelationshipRead]:
    entity = await get_entity(db, entity_id)
    if entity is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity not found")
    relationships = await get_entity_relationships(db, entity_id)
    return [RelationshipRead.model_validate(r) for r in relationships]
