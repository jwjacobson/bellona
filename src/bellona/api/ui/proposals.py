import uuid

import structlog
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from bellona.api.ui.templates import templates
from bellona.db.session import get_db
from bellona.models.ontology import EntityType
from bellona.models.system import AgentProposal, Connector
from bellona.services.agent_service import (
    ProposalError,
    confirm_discovery_proposal,
    confirm_mapping_proposal,
    confirm_schema_proposal,
    list_proposals,
    propose_mapping,
    reject_proposal,
)

logger = structlog.get_logger()
router = APIRouter(prefix="/proposals")


async def _proposal_context(db: AsyncSession) -> dict:
    """Build shared template context for the proposals page."""
    proposals = await list_proposals(db)

    # Load entity types for the mapping proposal dropdown
    et_result = await db.execute(select(EntityType).order_by(EntityType.name))
    entity_types = list(et_result.scalars().all())

    # Load connectors so we can show connector names on proposals
    conn_result = await db.execute(select(Connector))
    connectors = {c.id: c for c in conn_result.scalars().all()}

    return {
        "proposals": proposals,
        "entity_types": entity_types,
        "connectors": connectors,
    }


@router.get("")
async def proposals_index(request: Request, db: AsyncSession = Depends(get_db)):
    ctx = await _proposal_context(db)
    return templates.TemplateResponse(request, "proposals/index.html", ctx)


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
        elif proposal.proposal_type == "discovery":
            form = await request.form()
            selected = [int(v) for v in form.getlist("resources")]
            await confirm_discovery_proposal(
                db, proposal_id, selected_resources=selected or None
            )
        await db.commit()
    except ProposalError as exc:
        ctx = await _proposal_context(db)
        ctx["error"] = str(exc)
        return templates.TemplateResponse(
            request, "proposals/index.html", ctx, status_code=422,
        )
    if proposal.proposal_type == "discovery":
        return RedirectResponse(url="/ui/connectors", status_code=303)

    if proposal.connector_id:
        return RedirectResponse(url=f"/ui/connectors/{proposal.connector_id}", status_code=303)
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
        ctx = await _proposal_context(db)
        ctx["error"] = str(exc)
        return templates.TemplateResponse(
            request, "proposals/index.html", ctx, status_code=422,
        )
    return RedirectResponse(url="/ui/proposals", status_code=303)


@router.post("/propose-mapping")
async def propose_mapping_ui(
    request: Request,
    connector_id: uuid.UUID = Form(...),
    entity_type_id: uuid.UUID = Form(...),
    db: AsyncSession = Depends(get_db),
):
    try:
        await propose_mapping(db, connector_id, entity_type_id)
        await db.commit()
    except ProposalError as exc:
        logger.warning(
            "mapping proposal failed",
            connector_id=str(connector_id),
            entity_type_id=str(entity_type_id),
            error=str(exc),
        )
        ctx = await _proposal_context(db)
        ctx["error"] = str(exc)
        return templates.TemplateResponse(
            request, "proposals/index.html", ctx, status_code=422,
        )
    return RedirectResponse(url="/ui/proposals", status_code=303)
