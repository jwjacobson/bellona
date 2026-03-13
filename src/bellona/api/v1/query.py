"""Natural language query endpoint."""
import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.db.session import get_db
from bellona.schemas.agents import (
    NaturalLanguageQueryRequest,
    NaturalLanguageQueryResponse,
)
from bellona.services.agent_service import ProposalError, run_nl_query

logger = structlog.get_logger()
router = APIRouter(tags=["query"])


@router.post("/query/natural", response_model=NaturalLanguageQueryResponse)
async def natural_language_query(
    data: NaturalLanguageQueryRequest,
    db: AsyncSession = Depends(get_db),
) -> NaturalLanguageQueryResponse:
    try:
        return await run_nl_query(db, data.question, data.entity_type_id)
    except ProposalError as exc:
        msg = str(exc)
        if "not found" in msg:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=msg)
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=msg)
