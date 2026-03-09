import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.db.session import get_db
from bellona.schemas.ontology import EntityTypeCreate, EntityTypePatch, EntityTypeList, EntityTypeRead
from bellona.services.entity_type import (
    create_entity_type,
    get_entity_type,
    list_entity_types,
    patch_entity_type,
)

router = APIRouter(prefix="/entity-types", tags=["entity-types"])


@router.post("", response_model=EntityTypeRead, status_code=status.HTTP_201_CREATED)
async def create(data: EntityTypeCreate, db: AsyncSession = Depends(get_db)):
    try:
        entity_type = await create_entity_type(db, data)
        await db.commit()
        return entity_type
    except IntegrityError:
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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity type not found")
    return entity_type


@router.patch("/{entity_type_id}", response_model=EntityTypeRead)
async def patch(
    entity_type_id: uuid.UUID,
    data: EntityTypePatch,
    db: AsyncSession = Depends(get_db),
):
    entity_type = await get_entity_type(db, entity_type_id)
    if entity_type is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Entity type not found")
    entity_type = await patch_entity_type(db, entity_type, data)
    await db.commit()
    return entity_type
