import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
 
from bellona.db.session import get_db
from bellona.services.entity import delete_relationship
 
router = APIRouter(prefix="/relationships", tags=["relationships"])
 
 
@router.delete("/{relationship_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_relationship_endpoint(
    relationship_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    try:
        await delete_relationship(db, relationship_id)
        await db.commit()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Relationship not found"
        )