import uuid

import structlog
from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.api.ui.templates import templates
from bellona.db.session import get_db
from bellona.models.system import AgentProposal
from bellona.services.agent_service import (
    ProposalError,
    confirm_mapping_proposal,
    confirm_schema_proposal,
    list_proposals,
    reject_proposal,
)

logger = structlog.get_logger()
router = APIRouter(prefix="/proposals")


@router.get("")
async def proposals_index(request: Request, db: AsyncSession = Depends(get_db)):
    proposals = await list_proposals(db)
    return templates.TemplateResponse(
        request, "proposals/index.html", {"proposals": proposals}
    )


@router.post("/{proposal_id}/confirm")
async def confirm(
    request: Request,
    proposal_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    try:
        proposal = await db.get(AgentProposal, proposal_id)
        if proposal is None:
            raise ProposalError(f"Proposal {proposal_id} not found")

        if proposal.proposal_type == "mapping":
            await confirm_mapping_proposal(db, proposal_id)
        elif proposal.proposal_type == "entity_type":
            await confirm_schema_proposal(db, proposal_id)
        await db.commit()
    except ProposalError as exc:
        proposals = await list_proposals(db)
        return templates.TemplateResponse(
            request,
            "proposals/index.html",
            {"proposals": proposals, "error": str(exc)},
            status_code=422,
        )
    return RedirectResponse(url="/ui/proposals", status_code=303)


@router.post("/{proposal_id}/reject")
async def reject(
    request: Request,
    proposal_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    try:
        await reject_proposal(db, proposal_id)
        await db.commit()
    except ProposalError as exc:
        proposals = await list_proposals(db)
        return templates.TemplateResponse(
            request,
            "proposals/index.html",
            {"proposals": proposals, "error": str(exc)},
            status_code=422,
        )
    return RedirectResponse(url="/ui/proposals", status_code=303)
