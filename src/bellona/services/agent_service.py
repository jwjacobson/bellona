"""Agent service layer: orchestrates agent calls and persists proposals."""
import uuid
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from bellona.agents.mapper_agent import MapperAgent
from bellona.agents.quality_agent import QualityAgent
from bellona.agents.schema_agent import SchemaAgent
from bellona.core.config import get_settings
from bellona.models.entities import Entity
from bellona.models.ontology import EntityType
from bellona.models.system import AgentProposal, Connector, FieldMapping
from bellona.schemas.agents import (
    EntityTypeProposalContent,
    MappingProposalContent,
    ProposedPropertyDefinition,
    QualityReport,
)
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import _create_connector_instance, _load_entity_type

logger = structlog.get_logger()


class ProposalError(Exception):
    """Raised when a proposal operation cannot be completed."""


def _get_api_key() -> str:
    return get_settings().claude_api_key

def _get_model() -> str:
    return get_settings().claude_model

async def _get_connector_or_raise(db: AsyncSession, connector_id: uuid.UUID) -> Connector:
    connector = await db.get(Connector, connector_id)
    if connector is None:
        raise ProposalError(f"Connector {connector_id} not found")
    return connector


async def _get_entity_type_or_raise(
    db: AsyncSession, entity_type_id: uuid.UUID
) -> EntityType:
    et = await _load_entity_type(db, entity_type_id)
    if et is None:
        raise ProposalError(f"Entity type {entity_type_id} not found")
    return et


async def _get_proposal_or_raise(
    db: AsyncSession, proposal_id: uuid.UUID
) -> AgentProposal:
    proposal = await db.get(AgentProposal, proposal_id)
    if proposal is None:
        raise ProposalError(f"Proposal {proposal_id} not found")
    return proposal


def _entity_types_to_context(entity_types: list[EntityType]) -> list[dict[str, Any]]:
    return [
        {
            "name": et.name,
            "description": et.description or "",
            "properties": [
                {
                    "name": p.name,
                    "data_type": p.data_type,
                    "required": p.required,
                    "description": p.description or "",
                }
                for p in et.property_definitions
            ],
        }
        for et in entity_types
    ]


async def propose_mapping(
    db: AsyncSession,
    connector_id: uuid.UUID,
    entity_type_id: uuid.UUID,
    *,
    _mock_result: MappingProposalContent | None = None,
) -> AgentProposal:
    """Run the Mapper Agent and persist the proposal as an AgentProposal."""
    connector = await _get_connector_or_raise(db, connector_id)
    entity_type = await _get_entity_type_or_raise(db, entity_type_id)

    connector_instance = _create_connector_instance(connector)
    schema = await connector_instance.discover_schema()

    et_context = _entity_types_to_context([entity_type])

    if _mock_result is not None:
        proposal_content = _mock_result
    else:
        agent = MapperAgent(api_key=_get_api_key(), model=_get_model())
        proposal_content = await agent.propose(schema, et_context)

    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content=proposal_content.model_dump(),
        confidence=proposal_content.overall_confidence,
        connector_id=connector_id,
        entity_type_id=entity_type_id,
    )
    db.add(proposal)
    await db.flush()
    logger.info(
        "mapping proposal created",
        proposal_id=str(proposal.id),
        connector_id=str(connector_id),
        entity_type_id=str(entity_type_id),
        confidence=proposal_content.overall_confidence,
    )
    return proposal


async def propose_schema(
    db: AsyncSession,
    connector_id: uuid.UUID,
    *,
    _mock_result: EntityTypeProposalContent | None = None,
) -> AgentProposal:
    """Run the Schema Agent and persist the proposal as an AgentProposal."""
    connector = await _get_connector_or_raise(db, connector_id)

    connector_instance = _create_connector_instance(connector)
    schema = await connector_instance.discover_schema()

    # Provide all existing entity types as context so the agent avoids duplicates.
    all_et_result = await db.execute(
        select(EntityType).options(selectinload(EntityType.property_definitions))
    )
    all_entity_types = list(all_et_result.scalars().all())
    et_context = _entity_types_to_context(all_entity_types)

    if _mock_result is not None:
        proposal_content = _mock_result
    else:
        agent = SchemaAgent(api_key=_get_api_key(), model=_get_model())
        proposal_content = await agent.propose(schema, et_context)

    proposal = AgentProposal(
        proposal_type="entity_type",
        status="proposed",
        content=proposal_content.model_dump(),
        confidence=proposal_content.confidence,
        connector_id=connector_id,
    )
    db.add(proposal)
    await db.flush()
    logger.info(
        "schema proposal created",
        proposal_id=str(proposal.id),
        connector_id=str(connector_id),
        entity_type_name=proposal_content.entity_type_name,
        confidence=proposal_content.confidence,
    )
    return proposal


async def confirm_mapping_proposal(
    db: AsyncSession, proposal_id: uuid.UUID
) -> FieldMapping:
    """Convert an approved mapping AgentProposal into a confirmed FieldMapping."""
    proposal = await _get_proposal_or_raise(db, proposal_id)
    if proposal.proposal_type != "mapping":
        raise ProposalError(
            f"Proposal {proposal_id} is type '{proposal.proposal_type}', not 'mapping'"
        )
    if proposal.status != "proposed":
        raise ProposalError(
            f"Proposal {proposal_id} is already '{proposal.status}'"
        )

    # Extract mapping entries (strip agent-only fields like reasoning/confidence).
    raw_mappings = proposal.content.get("mappings", [])
    clean_mappings = [
        {"source_field": m["source_field"], "target_property": m["target_property"]}
        for m in raw_mappings
    ]

    field_mapping = FieldMapping(
        connector_id=proposal.connector_id,
        entity_type_id=proposal.entity_type_id,
        mapping_config={"mappings": clean_mappings},
        status="confirmed",
        proposed_by="agent",
    )
    db.add(field_mapping)

    proposal.status = "approved"
    await db.flush()

    logger.info(
        "mapping proposal confirmed",
        proposal_id=str(proposal_id),
        field_mapping_id=str(field_mapping.id),
        field_count=len(clean_mappings),
    )
    return field_mapping


async def confirm_schema_proposal(
    db: AsyncSession, proposal_id: uuid.UUID
) -> EntityType:
    """Execute an approved schema AgentProposal by creating the entity type."""
    proposal = await _get_proposal_or_raise(db, proposal_id)
    if proposal.proposal_type != "entity_type":
        raise ProposalError(
            f"Proposal {proposal_id} is type '{proposal.proposal_type}', not 'entity_type'"
        )
    if proposal.status != "proposed":
        raise ProposalError(
            f"Proposal {proposal_id} is already '{proposal.status}'"
        )

    content = EntityTypeProposalContent.model_validate(proposal.content)
    entity_type = await create_entity_type(
        db,
        EntityTypeCreate(
            name=content.entity_type_name,
            description=content.description or None,
            properties=[
                PropertyDefinitionCreate(
                    name=p.name,
                    data_type=p.data_type,
                    required=p.required,
                    description=p.description or None,
                )
                for p in content.properties
            ],
        ),
    )

    proposal.status = "approved"
    await db.flush()

    logger.info(
        "schema proposal confirmed",
        proposal_id=str(proposal_id),
        entity_type_id=str(entity_type.id),
        entity_type_name=entity_type.name,
    )
    return entity_type


async def reject_proposal(db: AsyncSession, proposal_id: uuid.UUID) -> AgentProposal:
    """Mark any proposal as rejected."""
    proposal = await _get_proposal_or_raise(db, proposal_id)
    if proposal.status != "proposed":
        raise ProposalError(
            f"Proposal {proposal_id} is already '{proposal.status}'"
        )
    proposal.status = "rejected"
    await db.flush()
    logger.info("proposal rejected", proposal_id=str(proposal_id))
    return proposal


async def list_proposals(db: AsyncSession) -> list[AgentProposal]:
    """Return all proposals currently in 'proposed' status."""
    result = await db.execute(
        select(AgentProposal)
        .where(AgentProposal.status == "proposed")
        .order_by(AgentProposal.created_at.desc())
    )
    return list(result.scalars().all())


async def check_quality(
    db: AsyncSession,
    entity_type_id: uuid.UUID,
    *,
    _mock_result: QualityReport | None = None,
) -> QualityReport:
    """Run the Quality Agent against entities of the given type. Returns report (not persisted)."""
    entity_type = await _get_entity_type_or_raise(db, entity_type_id)

    entities_result = await db.execute(
        select(Entity).where(Entity.entity_type_id == entity_type_id).limit(100)
    )
    entities = list(entities_result.scalars().all())

    entity_type_context: dict[str, Any] = {
        "name": entity_type.name,
        "properties": [
            {
                "name": p.name,
                "data_type": p.data_type,
                "required": p.required,
            }
            for p in entity_type.property_definitions
        ],
    }
    entities_context = [
        {"id": str(e.id), "properties": e.properties}
        for e in entities
    ]

    if _mock_result is not None:
        return _mock_result

    agent = QualityAgent(api_key=_get_api_key(), model=_get_model())
    return await agent.check(entity_type_context, entities_context)
