import structlog
from fastapi import APIRouter, Depends, Form, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.api.ui.templates import templates
from bellona.core.config import get_settings
from bellona.db.session import get_db
from bellona.services.agent_service import ProposalError, run_nl_query

logger = structlog.get_logger()
router = APIRouter(prefix="/query")
limiter = Limiter(key_func=get_remote_address)
settings = get_settings()


@router.get("")
async def query_index(request: Request):
    return templates.TemplateResponse(
        request, "query/index.html", {"result": None, "question": ""}
    )


@router.post("")

@limiter.limit(
    f"{settings.demo_max_agent_calls}/hour",
    exempt_when=lambda: not settings.demo_mode
)
async def run_query(
    request: Request,
    question: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    error = None
    result = None

    if settings.demo_mode and len(question) > settings.demo_max_query_length:
        return templates.TemplateResponse(
            request,
            "query/index.html",
            {
                "result": None,
                "question": question,
                "error": f"Query too long. Please keep questions under {settings.demo_max_query_length} characters.",
            },
        )

    try:
        result = await run_nl_query(db, question)
    except ProposalError as exc:
        error = str(exc)
        logger.warning("nl query failed", question=question, error=error)
    except Exception:
        logger.exception("nl query unexpected failure", question=question)
        error = "The query agent encountered an error. Please try again."

    return templates.TemplateResponse(
        request,
        "query/index.html",
        {"result": result, "question": question, "error": error},
    )