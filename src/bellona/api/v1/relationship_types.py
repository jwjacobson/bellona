from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.db.session import get_db
from bellona.schemas.ontology import RelationshipTypeCreate, RelationshipTypeRead
from bellona.services.relationship_type import (
    create_relationship_type,
    list_relationship_types,
)

router = APIRouter(prefix="/relationship-types", tags=["relationship-types"])


@router.post(
    "", response_model=RelationshipTypeRead, status_code=status.HTTP_201_CREATED
)
async def create(data: RelationshipTypeCreate, db: AsyncSession = Depends(get_db)):
    try:
        rel_type = await create_relationship_type(db, data)
        await db.commit()
        return rel_type
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Relationship type '{data.name}' already exists",
        )


@router.get("", response_model=list[RelationshipTypeRead])
async def list_all(db: AsyncSession = Depends(get_db)):
    return await list_relationship_types(db)
