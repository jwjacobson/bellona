import structlog
from fastapi import APIRouter, Depends, Form, Request
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.api.ui.templates import templates
from bellona.db.session import get_db
from bellona.services.agent_service import ProposalError, run_nl_query

logger = structlog.get_logger()
router = APIRouter(prefix="/query")


@router.get("")
async def query_index(request: Request):
    return templates.TemplateResponse(
        request, "query/index.html", {"result": None, "question": ""}
    )


@router.post("")
async def run_query(
    request: Request,
    question: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    error = None
    result = None
    try:
        result = await run_nl_query(db, question)
    except (ProposalError, Exception) as exc:
        error = str(exc)
        logger.warning("nl query failed", error=error)

    return templates.TemplateResponse(
        request,
        "query/index.html",
        {"result": result, "question": question, "error": error},
    )
