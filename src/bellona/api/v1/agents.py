import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.db.session import get_db
from bellona.schemas.agents import (
    AgentProposalRead,
    MappingProposeRequest,
    QualityReport,
    SchemaProposeRequest,
)
from bellona.schemas.connectors import FieldMappingRead
from bellona.schemas.ontology import EntityTypeRead
from bellona.services.agent_service import (
    ProposalError,
    check_quality,
    confirm_mapping_proposal,
    confirm_schema_proposal,
    list_proposals,
    propose_mapping,
    propose_schema,
    reject_proposal,
)

logger = structlog.get_logger()
router = APIRouter(tags=["agents"])


def _not_found(msg: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=msg)


def _unprocessable(msg: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=msg)


# ── Mapper Agent ──────────────────────────────────────────────────────────────


@router.post(
    "/mappings/propose",
    response_model=AgentProposalRead,
    status_code=status.HTTP_201_CREATED,
)
async def propose_mapping_endpoint(
    data: MappingProposeRequest,
    db: AsyncSession = Depends(get_db),
) -> AgentProposalRead:
    try:
        proposal = await propose_mapping(db, data.connector_id, data.entity_type_id)
    except ProposalError as exc:
        raise _not_found(str(exc))
    await db.commit()
    return proposal  # type: ignore[return-value]


@router.post("/mappings/{proposal_id}/confirm", response_model=FieldMappingRead)
async def confirm_mapping_endpoint(
    proposal_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> FieldMappingRead:
    try:
        field_mapping = await confirm_mapping_proposal(db, proposal_id)
    except ProposalError as exc:
        msg = str(exc)
        if "not found" in msg:
            raise _not_found(msg)
        raise _unprocessable(msg)
    await db.commit()
    return field_mapping  # type: ignore[return-value]


# ── Schema Agent ──────────────────────────────────────────────────────────────


@router.post(
    "/schema/propose",
    response_model=AgentProposalRead,
    status_code=status.HTTP_201_CREATED,
)
async def propose_schema_endpoint(
    data: SchemaProposeRequest,
    db: AsyncSession = Depends(get_db),
) -> AgentProposalRead:
    try:
        proposal = await propose_schema(db, data.connector_id)
    except ProposalError as exc:
        raise _not_found(str(exc))
    await db.commit()
    return proposal  # type: ignore[return-value]


# ── Generic Proposal Actions ──────────────────────────────────────────────────


@router.post("/proposals/{proposal_id}/confirm", response_model=EntityTypeRead)
async def confirm_proposal_endpoint(
    proposal_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> EntityTypeRead:
    try:
        entity_type = await confirm_schema_proposal(db, proposal_id)
    except ProposalError as exc:
        msg = str(exc)
        if "not found" in msg:
            raise _not_found(msg)
        raise _unprocessable(msg)
    await db.commit()
    return entity_type  # type: ignore[return-value]


@router.post("/proposals/{proposal_id}/reject", response_model=AgentProposalRead)
async def reject_proposal_endpoint(
    proposal_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> AgentProposalRead:
    try:
        proposal = await reject_proposal(db, proposal_id)
    except ProposalError as exc:
        raise _not_found(str(exc))
    await db.commit()
    return proposal  # type: ignore[return-value]


@router.get("/proposals", response_model=list[AgentProposalRead])
async def list_proposals_endpoint(
    db: AsyncSession = Depends(get_db),
) -> list[AgentProposalRead]:
    return await list_proposals(db)  # type: ignore[return-value]


# ── Quality Agent ─────────────────────────────────────────────────────────────


@router.post("/quality/check/{entity_type_id}", response_model=QualityReport)
async def quality_check_endpoint(
    entity_type_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> QualityReport:
    try:
        return await check_quality(db, entity_type_id)
    except ProposalError as exc:
        raise _not_found(str(exc))
