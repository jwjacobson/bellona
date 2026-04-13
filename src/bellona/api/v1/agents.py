import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.db.session import get_db
from bellona.models.system import AgentProposal
from bellona.schemas.agents import (
    AgentProposalRead,
    ConfirmDiscoveryRequest,
    DiscoveryRequest,
    MappingProposeRequest,
    QualityReport,
    SchemaProposeRequest,
)
from bellona.schemas.connectors import ConnectorRead, FieldMappingRead
from bellona.schemas.ontology import EntityTypeRead
from bellona.services.agent_service import (
    ProposalError,
    check_quality,
    confirm_discovery_proposal,
    confirm_mapping_proposal,
    confirm_schema_proposal,
    discover_api,
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


# ── Discovery Agent ───────────────────────────────────────────────────────────


@router.post(
    "/discovery/discover",
    response_model=AgentProposalRead,
    status_code=status.HTTP_201_CREATED,
)
async def discover_api_endpoint(
    data: DiscoveryRequest,
    db: AsyncSession = Depends(get_db),
) -> AgentProposalRead:
    try:
        proposal = await discover_api(db, data.base_url, data.auth_config)
    except ProposalError as exc:
        raise _unprocessable(str(exc))
    await db.commit()
    return proposal  # type: ignore[return-value]


@router.post(
    "/discovery/{proposal_id}/confirm",
    response_model=list[ConnectorRead],
)
async def confirm_discovery_endpoint(
    proposal_id: uuid.UUID,
    data: ConfirmDiscoveryRequest | None = None,
    db: AsyncSession = Depends(get_db),
) -> list[ConnectorRead]:
    selected = data.selected_resources if data else None
    try:
        connectors = await confirm_discovery_proposal(db, proposal_id, selected)
    except ProposalError as exc:
        msg = str(exc)
        if "not found" in msg:
            raise _not_found(msg)
        raise _unprocessable(msg)
    await db.commit()
    return [ConnectorRead.model_validate(c) for c in connectors]


# ── Generic Proposal Actions ──────────────────────────────────────────────────


@router.post("/proposals/{proposal_id}/confirm")
async def confirm_proposal_endpoint(
    proposal_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    try:
        proposal = await db.get(AgentProposal, proposal_id)
        if proposal is None:
            raise ProposalError(f"Proposal {proposal_id} not found")

        if proposal.proposal_type == "mapping":
            result = await confirm_mapping_proposal(db, proposal_id)
            await db.commit()
            return FieldMappingRead.model_validate(result)
        elif proposal.proposal_type == "entity_type":
            result = await confirm_schema_proposal(db, proposal_id)
            await db.commit()
            return EntityTypeRead.model_validate(result)
        elif proposal.proposal_type == "discovery":
            connectors = await confirm_discovery_proposal(db, proposal_id)
            await db.commit()
            return [ConnectorRead.model_validate(c) for c in connectors]
        else:
            raise ProposalError(f"Unknown proposal type: {proposal.proposal_type}")
    except ProposalError as exc:
        msg = str(exc)
        if "not found" in msg:
            raise _not_found(msg)
        raise _unprocessable(msg)


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
