import uuid

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.db.session import get_db
from bellona.schemas.delete import CascadeDeleteResult
from bellona.schemas.ontology import (
    EntityTypeCreate,
    EntityTypePatch,
    EntityTypeList,
    EntityTypeRead,
)
from bellona.services.entity_type import (
    create_entity_type,
    create_entity_type_gin_index,
    delete_entity_type,
    delete_entity_type_cascade,
    get_entity_type,
    list_entity_types,
    patch_entity_type,
)

logger = structlog.get_logger()
router = APIRouter(prefix="/entity-types", tags=["entity-types"])


@router.post("", response_model=EntityTypeRead, status_code=status.HTTP_201_CREATED)
async def create(
    data: EntityTypeCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    try:
        entity_type = await create_entity_type(db, data)
        await db.commit()
        background_tasks.add_task(create_entity_type_gin_index, entity_type.id)
        return entity_type
    except IntegrityError:
        logger.warning("entity type already exists", name=data.name)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Entity type '{data.name}' already exists",
        )


@router.get("", response_model=list[EntityTypeList])
async def list_all(db: AsyncSession = Depends(get_db)):
    return await list_entity_types(db)


@router.get("/{entity_type_id}", response_model=EntityTypeRead)
async def get_one(entity_type_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    entity_type = await get_entity_type(db, entity_type_id)
    if entity_type is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Entity type not found"
        )
    return entity_type


@router.patch("/{entity_type_id}", response_model=EntityTypeRead)
async def patch(
    entity_type_id: uuid.UUID,
    data: EntityTypePatch,
    db: AsyncSession = Depends(get_db),
):
    entity_type = await get_entity_type(db, entity_type_id)
    if entity_type is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Entity type not found"
        )
    entity_type = await patch_entity_type(db, entity_type, data)
    await db.commit()
    return entity_type


@router.delete("/{entity_type_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(
    entity_type_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    try:
        await delete_entity_type(db, entity_type_id)
        await db.commit()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Entity type not found"
        )
    except IntegrityError:
        logger.warning(
            "cannot delete entity type: dependents exist",
            entity_type_id=str(entity_type_id),
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot delete entity type: entities or relationship types still reference it. "
            "Delete them first, or use the cascade-delete endpoint.",
        )


@router.post("/{entity_type_id}/cascade-delete", response_model=CascadeDeleteResult)
async def cascade_delete(
    entity_type_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await delete_entity_type_cascade(db, entity_type_id)
        await db.commit()
        return result
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Entity type not found",
        )
